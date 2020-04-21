import logging
import os
import random
import threading
import time
import traceback

import rpyc
import sys


class NodeRef:
    def __init__(self, name, hostAddr, port):
        self.name = name
        self.host = hostAddr
        self.port = port


'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''


class RaftNode(rpyc.Service):
    ELECTION_TIMEOUT_BASELINE = 0.150 * 25  # seconds to wait before calling an election
    HEARTBEAT_INTERVAL = ELECTION_TIMEOUT_BASELINE * 0.75
    NODE_STATE_FOLDER = "node_states"
    NODE_LOGS_FOLDER = "node_logs"

    BACKUP_SEPARATOR = ":"
    TERM_BACKUP_KEY = "term"
    VOTE_BACKUP_KEY = "vote"
    LEADER_BACKUP_KEY = "isLeader"
    BACKUP_TRUE_VALUE = "true"
    BACKUP_FALSE_VALUE = "false"

    def _construct_node_state_file_path(self):
        pathStr = os.path.join(RaftNode.NODE_STATE_FOLDER, "node" + str(self.identityIndex) + ".txt")
        return pathStr

    def _save_node_state(self):
        if not os.path.exists(RaftNode.NODE_STATE_FOLDER):
            os.makedirs(RaftNode.NODE_STATE_FOLDER)

        nodeStateStorageFilePath = self._construct_node_state_file_path()
        with open(nodeStateStorageFilePath, mode="w") as nodeStateStorageFile:
            termLine = RaftNode.TERM_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + str(self.currTerm) + "\n"
            nodeStateStorageFile.write(termLine)
            voteTargetIndex = self.voteTarget if self.voteTarget is not None else -1
            voteLine = RaftNode.VOTE_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + str(voteTargetIndex) + "\n"
            nodeStateStorageFile.write(voteLine)
            isLeaderVal = RaftNode.BACKUP_TRUE_VALUE if self.exposed_is_leader() else RaftNode.BACKUP_FALSE_VALUE
            leaderLine = RaftNode.LEADER_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + isLeaderVal + "\n"
            nodeStateStorageFile.write(leaderLine)

            nodeStateStorageFile.flush()
            os.fsync(nodeStateStorageFile.fileno())

    def _load_node_backup(self, backupFile):
        backupDict = {}

        for backupLine in backupFile:
            if backupLine != "":
                lineTokens = backupLine.split(RaftNode.BACKUP_SEPARATOR)
                if len(lineTokens) == 2:
                    currKey = lineTokens[0].strip()
                    currVal = lineTokens[1].strip()

                    backupDict[currKey] = currVal
                else:
                    self.nodeLogger.error("malformed line in node backup file: %s", backupLine)

        return backupDict

    def _restart_timeout(self):
        self.lastContactTimestamp = time.time()
        electionTimer = threading.Timer(self.electionTimeout, self.check_for_election_timeout)
        electionTimer.start()

    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, configFilePath, nodeIdentityIndex):
        self.identityIndex = nodeIdentityIndex

        self.isCandidate = False
        self._leaderStatus = False
        self.currTerm = 0
        self.voteTarget = None  # who the node is voting for in the current term

        # set up logging
        nodeName = "raftNode_" + str(nodeIdentityIndex)
        self.nodeLogger = logging.getLogger(nodeName)
        self.nodeLogger.setLevel(logging.DEBUG)

        if not os.path.exists(RaftNode.NODE_LOGS_FOLDER):
            os.makedirs(RaftNode.NODE_LOGS_FOLDER)

        logFilePath = os.path.join(RaftNode.NODE_LOGS_FOLDER, nodeName + ".log")
        logFileHandler = logging.FileHandler(logFilePath)
        logFileHandler.setLevel(logging.DEBUG)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logFileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)
        self.nodeLogger.addHandler(logFileHandler)
        self.nodeLogger.addHandler(consoleHandler)

        nodeStateBackupFilePath = self._construct_node_state_file_path()
        if os.path.exists(nodeStateBackupFilePath):
            with open(nodeStateBackupFilePath, mode="r") as nodeBackup:
                nodeStateBackup = self._load_node_backup(nodeBackup)

                storedTermStr = nodeStateBackup.get(RaftNode.TERM_BACKUP_KEY)
                if storedTermStr is not None:
                    storedTermVal = int(storedTermStr)
                    self.currTerm = storedTermVal

                storedVoteStr = nodeStateBackup.get(RaftNode.VOTE_BACKUP_KEY)
                if storedVoteStr is not None:
                    storedVoteVal = int(storedVoteStr)
                    if storedVoteVal >= 0:
                        self.voteTarget = storedVoteVal

                storedLeaderStr = nodeStateBackup.get(RaftNode.LEADER_BACKUP_KEY)
                if storedLeaderStr is not None and storedLeaderStr == RaftNode.BACKUP_TRUE_VALUE:
                    self._leaderStatus = True
            self.nodeLogger.info("loading backup of prior node state from disk:\n"
                                 "term %d; voteTarget %d (-1 standing for None); and leader status %s", self.currTerm,
                                 self.voteTarget if self.voteTarget is not None else -1, self._leaderStatus)

        self.otherNodes = []
        with open(configFilePath) as nodesConfigFile:
            nodesConfigFile.readline()  # ignore first line with node count

            for nodeInd, nodeLine in enumerate(nodesConfigFile):
                if nodeInd != nodeIdentityIndex:
                    otherNodeTerms = nodeLine.split(":")
                    otherNodeName = otherNodeTerms[0].strip()
                    otherNodeHost = otherNodeTerms[1].strip()
                    otherNodePort = otherNodeTerms[2].strip()
                    otherNodePort = int(otherNodePort)
                    otherNode = NodeRef(otherNodeName, otherNodeHost, otherNodePort)
                    self.otherNodes.append(otherNode)

        self.electionTimeout = (1 + random.random()) * RaftNode.ELECTION_TIMEOUT_BASELINE
        self._restart_timeout()

        self.nodeLogger.critical("I am node %d and I just finished being constructed, with %d fellow nodes",
                                 self.identityIndex, len(self.otherNodes))

    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader
    
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    
        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''

    def exposed_is_leader(self):
        return self._leaderStatus

    def exposed_append_entries(self, leaderTerm, leaderIndex):
        willAppendEntries = False
        if leaderTerm < self.currTerm:
            self.nodeLogger.info(
                "while in term %d, received append_entries() from stale leader %d which thought it was in term %d",
                self.currTerm, leaderIndex, leaderTerm)
        else:
            self.nodeLogger.debug(
                "while in term %d, executing append_entries on behalf of node %d, the leader in term %d",
                self.currTerm, leaderIndex, leaderTerm)

            self._restart_timeout()

            if leaderTerm > self.currTerm:
                if self.voteTarget is not None:
                    self.nodeLogger.warning("was in election for term %d, voting for candidate node %d, "
                                            "when received request to append entries in later term %d", self.currTerm,
                                            self.voteTarget, leaderTerm)
                    self.voteTarget = None

                self.nodeLogger.critical("was in term %d with candidate status %s and leader status %s when "
                                         "received heartbeat from leader node %d in higher term %d", self.currTerm,
                                         self.isCandidate, self._leaderStatus, leaderIndex, leaderTerm)
                self.isCandidate = False
                self._leaderStatus = False
                self.currTerm = leaderTerm
                self._save_node_state()

            self.voteTarget = None
            willAppendEntries = True

        return (self.currTerm, willAppendEntries)

    def call_append_entries(self, otherNodeDesc):
        assert self.exposed_is_leader()
        appendEntriesRetVal = None

        try:
            nodeConn = rpyc.connect(otherNodeDesc.host, otherNodeDesc.port)
            otherNodeRoot = nodeConn.root
            appendEntriesRetVal = otherNodeRoot.append_entries(self.currTerm, self.identityIndex)
        except ConnectionRefusedError:
            self.nodeLogger.info("leader node %d in term %d was unable to connect to another node with port %d",
                                 self.identityIndex, self.currTerm, otherNodeDesc.port)
        except EOFError:
            self.nodeLogger.info("leader node %d in term %d lost connection to another node with port %d",
                                 self.identityIndex, self.currTerm, otherNodeDesc.port)
        except Exception as e:
            self.nodeLogger.error("Exception for leader node %d in term %d: %s\n%s\n%s",
                                  self.identityIndex, self.currTerm, e.__doc__, str(e), traceback.format_exc())
        return appendEntriesRetVal

    def exposed_request_vote(self, candidateTerm, candidateIndex):
        willVote = False

        if candidateTerm < self.currTerm:
            self.nodeLogger.info("while in term %d, received request_vote() from stale leader %d "
                                 "which thought it was in term %d", self.currTerm, candidateIndex, candidateTerm)
        else:
            self.nodeLogger.debug(
                "while in term %d, executing request_vote on behalf of node %d, a candidate in term %d",
                self.currTerm, candidateIndex, candidateTerm)
            if candidateTerm > self.currTerm:
                if self.voteTarget is not None:
                    self.nodeLogger.warning("was in election for term %d, voting for candidate node %d, "
                                            "when received request for vote in later term %d", self.currTerm,
                                            self.voteTarget, candidateTerm)
                    self.voteTarget = None

                self.nodeLogger.critical("was in term %d with candidate status %s and leader status %s when "
                                         "received request for vote in higher term %d", self.currTerm, self.isCandidate,
                                         self._leaderStatus, candidateTerm)
                self.isCandidate = False
                self._leaderStatus = False
                self.currTerm = candidateTerm
                self._save_node_state()
            else:
                if self.exposed_is_leader():
                    self.nodeLogger.warning("elected leader %d received request_vote() from candidate %d "
                                            "when both are in term %d", self.identityIndex, candidateIndex,
                                            candidateTerm)

            if self.voteTarget is None:
                self.nodeLogger.critical("casting vote for candidate node %d", candidateIndex)
                self._restart_timeout()

                self.voteTarget = candidateIndex
                self._save_node_state()
                willVote = True

        return (self.currTerm, willVote)

    def call_request_vote(self, otherNodeDesc):
        assert self.isCandidate
        requestVoteRetVal = None

        try:
            nodeConn = rpyc.connect(otherNodeDesc.host, otherNodeDesc.port)
            otherNodeRoot = nodeConn.root
            requestVoteRetVal = otherNodeRoot.request_vote(self.currTerm, self.identityIndex)
        except ConnectionRefusedError as cre:
            self.nodeLogger.info("candidate node %d in term %d was unable to connect to another node with port %d",
                                 self.identityIndex, self.currTerm, otherNodeDesc.port)
        except EOFError:
            self.nodeLogger.info("candidate node %d in term %d lost connection to another node with port %d",
                                 self.identityIndex, self.currTerm, otherNodeDesc.port)
        except Exception as e:
            self.nodeLogger.error("Exception for candidate node %d in term %d: %s\n%s\n%s",
                                  self.identityIndex, self.currTerm, e.__doc__, str(e), traceback.format_exc())
        return requestVoteRetVal

    def check_for_election_timeout(self):
        if self.exposed_is_leader():
            self.nodeLogger.info("this node is ignoring an election timeout because it's the leader")
        else:
            self.nodeLogger.debug("checking whether election should be started")
            if (time.time() - self.lastContactTimestamp) > self.electionTimeout:
                self.isCandidate = True
                self.currTerm += 1
                self._save_node_state()
                self._restart_timeout()

                self.nodeLogger.critical("starting election for the new term %d", self.currTerm)
                electionTerm = self.currTerm
                numVotes = 1
                numNodes = 1 + len(self.otherNodes)

                nodesToContact = self.otherNodes.copy()

                while len(nodesToContact) > 0 and self.isCandidate and electionTerm == self.currTerm:
                    currOtherNode = nodesToContact.pop(0)
                    nodeVoteResponse = self.call_request_vote(currOtherNode)
                    

                    if nodeVoteResponse is None:
                        nodesToContact.append(currOtherNode)
                    elif nodeVoteResponse[1]:
                        numVotes += 1
                    else:
                        responderTerm = nodeVoteResponse[0]
                        if responderTerm > self.currTerm:
                            self.nodeLogger.critical("terminating election for term %d because a vote request response"
                                                 " informed this node of higher term %d", self.currTerm, responderTerm)
                            self.isCandidate = False
                            self.currTerm = responderTerm
                            self._save_node_state()
                            self._restart_timeout()

                    # possible race condition with _leaderStatus?
                    if numVotes > numNodes / 2.0:
                        self.nodeLogger.critical("becoming the leader for the term %d with %d votes!!!",
                                                 self.currTerm, numVotes)
                        self._leaderStatus = True
                        self.isCandidate = False
                        self._save_node_state()

                        self.send_heartbeats()

    def send_heartbeats(self):
        if not self.exposed_is_leader():
            self.nodeLogger.warning("in term %d, node attempted to send heartbeats out despite not being the leader",
                                    self.currTerm)
        else:
            heartbeatTimer = threading.Timer(RaftNode.HEARTBEAT_INTERVAL, self.send_heartbeats)
            heartbeatTimer.start()

            nodesToContact = self.otherNodes.copy()
            while len(nodesToContact) > 0 and self.exposed_is_leader():
                currOtherNode = nodesToContact.pop(0)
                nodeHeartbeatResponse = self.call_append_entries(currOtherNode)
                

                if nodeHeartbeatResponse is None:
                    nodesToContact.append(currOtherNode)
                else:
                    responderTerm = nodeHeartbeatResponse[0]
                    if responderTerm > self.currTerm:
                        self.nodeLogger.critical("this node was leader in term %d but is abandoning that status because"
                                             "a heartbeat response informed it of a higher term %d",
                                             self.currTerm, responderTerm)
                        self._leaderStatus = False
                        self.currTerm = responderTerm
                        self._restart_timeout()


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer

    nodeNum = -1
    currNodePort = -1

    configFileName = sys.argv[1]

    currNodeIndexStr = sys.argv[2]
    currNodeIndex = int(currNodeIndexStr)

    with open(configFileName) as configFile:
        nodeNumLine = configFile.readline()
        if nodeNumLine[:2] == "N:":
            nodeNumStr = nodeNumLine[2:]
            nodeNumStr = nodeNumStr.strip()
            nodeNum = int(nodeNumStr)
        else:
            print("invalid config file- bad initial node count line: %s" % nodeNumLine)
            raise Exception("bad config file")

        if currNodeIndex < nodeNum:
            nodeDescriptions = configFile.readlines()
            if len(nodeDescriptions) == nodeNum:
                currNodeLine = nodeDescriptions[currNodeIndex]
                nodeTerms = currNodeLine.split(":")
                nodePortStr = nodeTerms[2].strip()
                currNodePort = int(nodePortStr)
            else:
                print("invalid config file- wrong number of lines of node descriptions %s" % nodeNumLine)
                raise Exception("bad config file")
        else:
            print("unacceptably high index %d for node system which only has %d nodes" % (currNodeIndex, nodeNum))
            raise Exception("bad node index")

    if currNodePort > 0:
        server = ThreadPoolServer(RaftNode(configFileName, currNodeIndex), port=currNodePort)
        server.start()
