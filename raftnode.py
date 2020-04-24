import logging
import os
import random
import threading
import time
import traceback
import math

import rpyc
from rpyc.utils import helpers
from rpyc.core import AsyncResultTimeout
import socket

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
    ELECTION_TIMEOUT_BASELINE = 0.3   #used to calculate the seconds to wait before calling an election, based on communication delay for contacting 1 active node
    CONNECTION_TIMEOUT = 0.35

    NODE_STATE_FOLDER = "node_states"
    NODE_LOGS_FOLDER = "node_logs"

    BACKUP_SEPARATOR = ":"
    TERM_BACKUP_KEY = "term"
    VOTE_BACKUP_KEY = "vote"
    CURR_LEADER_BACKUP_KEY = "currLeader"

    LEADER_STATUS_BACKUP_KEY = "isLeader"
    BACKUP_TRUE_VALUE = "true"
    BACKUP_FALSE_VALUE = "false"

    #todo convert this to instance-level constant defined in constructor
    def _construct_node_state_file_path(self):
        pathStr = os.path.join(RaftNode.NODE_STATE_FOLDER, "node" + str(self.identityIndex) + ".txt")
        return pathStr

    def _save_node_state(self):
        # saveStartTime = time.time()
        if not os.path.exists(RaftNode.NODE_STATE_FOLDER):
            os.makedirs(RaftNode.NODE_STATE_FOLDER)

        nodeStateStorageFilePath = self._construct_node_state_file_path()

        self.stateFileLock.acquire()

        with open(nodeStateStorageFilePath, mode="w") as nodeStateStorageFile:
            termLine = RaftNode.TERM_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + str(self.currTerm) + "\n"
            nodeStateStorageFile.write(termLine)
            voteTargetIndex = self.voteTarget if self.voteTarget is not None else -1
            voteLine = RaftNode.VOTE_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + str(voteTargetIndex) + "\n"
            nodeStateStorageFile.write(voteLine)

            currLeaderIndex = self.currLeader if self.currLeader is not None else -1
            currLeaderLine = RaftNode.CURR_LEADER_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + str(currLeaderIndex) + "\n"
            nodeStateStorageFile.write(currLeaderLine)

            isLeaderVal = RaftNode.BACKUP_TRUE_VALUE if self.exposed_is_leader() else RaftNode.BACKUP_FALSE_VALUE
            leaderLine = RaftNode.LEADER_STATUS_BACKUP_KEY + RaftNode.BACKUP_SEPARATOR + isLeaderVal + "\n"
            nodeStateStorageFile.write(leaderLine)

            nodeStateStorageFile.flush()
            os.fsync(nodeStateStorageFile.fileno())

        self.stateFileLock.release()

        # saveDuration = time.time() - saveStartTime
        # self.nodeLogger.debug("saving node state took %f seconds", saveDuration)

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
        # electionTimerStartupStartTime = time.time()
        electionTimer = threading.Timer(self.electionTimeout, self.check_for_election_timeout)
        electionTimer.start()

        # electionTimerStartupDuration = time.time() - electionTimerStartupStartTime
        # self.nodeLogger.debug("starting up an election timer took %f seconds", electionTimerStartupDuration)

    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, configFilePath, nodeIdentityIndex):
        self.identityIndex = nodeIdentityIndex

        self.isCandidate = False
        #todo eliminate _leaderStatus in favor of using currLeader only
        self._leaderStatus = False
        self.currTerm = 0
        self.voteTarget = None  # who the node is voting for in the current term
        self.currLeader = None # who has been elected leader in the current term

        # should these not be reentrant?
        self.stateFileLock = threading.RLock()
        self.stateLock = threading.RLock()


        # set up logging
        nodeName = "raftNode" + str(nodeIdentityIndex)
        self.nodeLogger = logging.getLogger(nodeName)
        self.nodeLogger.setLevel(logging.DEBUG)

        if not os.path.exists(RaftNode.NODE_LOGS_FOLDER):
            os.makedirs(RaftNode.NODE_LOGS_FOLDER)

        logFilePath = os.path.join(RaftNode.NODE_LOGS_FOLDER, nodeName + ".log")
        logFileHandler = logging.FileHandler(logFilePath)
        logFileHandler.setLevel(logging.DEBUG)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
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

                storedCurrLeaderStr = nodeStateBackup.get(RaftNode.CURR_LEADER_BACKUP_KEY)
                if storedCurrLeaderStr is not None:
                    storedCurrLeaderVal = int(storedCurrLeaderStr)
                    if storedCurrLeaderVal >= 0:
                        self.currLeader = storedCurrLeaderVal

                storedLeaderStatusStr = nodeStateBackup.get(RaftNode.LEADER_STATUS_BACKUP_KEY)
                if storedLeaderStatusStr is not None and storedLeaderStatusStr == RaftNode.BACKUP_TRUE_VALUE:
                    self._leaderStatus = True

            self.nodeLogger.info("loading backup of prior node state from disk:\n"
                                 "term %d; voteTarget %d (-1 standing for None); "
                                 "current leader %d (-1 standing for None); and leader status %s", self.currTerm,
                                 self.voteTarget if self.voteTarget is not None else -1,
                                 self.currLeader if self.currLeader is not None else -1, self._leaderStatus)

            if self.exposed_is_leader() and self.currLeader != self.identityIndex:
                self.nodeLogger.error("INVALID NODE STATE LOADED FROM DISK")

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

        numOtherNodes = len(self.otherNodes)
        numNodes = 1+ numOtherNodes


        #subtracting 1 because this node already provides itself with 1 vote when it's a candidate
        if numNodes % 2 == 0:
            numVotesNeeded = numNodes/2 + 1 -1
        else:
            numVotesNeeded = math.ceil(numNodes/2) -1
        # based on worst-case where only a bare majority of nodes are still alive & one or more of those live nodes
        #  is after all of the dead ones in the list
        minimumElectionTimeout = (numOtherNodes-numVotesNeeded)*RaftNode.CONNECTION_TIMEOUT + \
                               RaftNode.ELECTION_TIMEOUT_BASELINE*numVotesNeeded
        #todo try 0.5-1.5 rather than 1-2 or 0.75-1.75
        self.electionTimeout = (1 + random.random())*minimumElectionTimeout
        self._restart_timeout()

        self.heartbeatInterval = 0.5*minimumElectionTimeout

        self.nodeLogger.critical("I am node %d (election timeout %f) and I just finished being constructed, with %d fellow nodes",
                                 self.identityIndex, self.electionTimeout, len(self.otherNodes))
        for otherNodeDesc in self.otherNodes:
            self.nodeLogger.debug("other node %s is at host %s and port %d", otherNodeDesc.name, otherNodeDesc.host,
                                  otherNodeDesc.port)

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

        appendEntriesStartTime = time.time()

        self.nodeLogger.debug("about to acquire LOCK to execute append_entries RPC for leader node %d "
                              "which was in term %d", leaderIndex, leaderTerm)
        self.stateLock.acquire()
        self.nodeLogger.debug("successfully acquired LOCK to execute append_entries RPC for leader node %d "
                              "which was in term %d", leaderIndex, leaderTerm)


        termAtStartOfAppendEntries = self.currTerm

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
                self.currLeader = None
                self.currTerm = leaderTerm

            if self.currLeader != leaderIndex:
                self.nodeLogger.critical("acknowledging node %d as the leader for term %d", leaderIndex, self.currTerm)

            self.voteTarget = None
            self.currLeader = leaderIndex
            self._save_node_state()
            willAppendEntries = True

        self.nodeLogger.debug("releasing LOCK after executing append_entries RPC for leader node %d "
                              "which was in term %d", leaderIndex, leaderTerm)
        self.stateLock.release()

        appendEntriesDuration = time.time() - appendEntriesStartTime
        self.nodeLogger.debug("while starting in term %d, executing append_entries for leader node %d "
                              "which was in term %d took %f seconds", termAtStartOfAppendEntries, leaderIndex,
                              leaderTerm, appendEntriesDuration)

        return (self.currTerm, willAppendEntries)

    def call_append_entries(self, otherNodeDesc):
        assert self.exposed_is_leader()
        appendEntriesRetVal = None

        heartbeatRpcStartTime = time.time()

        try:
            nodeConnStream = rpyc.SocketStream.connect(otherNodeDesc.host, otherNodeDesc.port,
                                                       timeout= RaftNode.CONNECTION_TIMEOUT, attempts= 1)
            nodeConn = rpyc.connect_stream(nodeConnStream)
            otherNodeRoot = nodeConn.root
            timedAppendEntriesProxy = helpers.timed(otherNodeRoot.append_entries, RaftNode.CONNECTION_TIMEOUT)
            appendEntriesPromise = timedAppendEntriesProxy(self.currTerm, self.identityIndex)
            appendEntriesRetVal = appendEntriesPromise.value
        except AsyncResultTimeout:
            self.nodeLogger.info("connection timed out while leader node %d in term %d tried to send append_entries "
                                 "to node %s", self.identityIndex, self.currTerm, otherNodeDesc.name)
        except (socket.timeout, ConnectionRefusedError):
            self.nodeLogger.info("leader node %d in term %d was unable to connect to another node %s",
                                 self.identityIndex, self.currTerm, otherNodeDesc.name)
        except EOFError:
            self.nodeLogger.info("leader node %d in term %d lost connection to another node %s",
                                 self.identityIndex, self.currTerm, otherNodeDesc.name)
        except Exception as e:
            self.nodeLogger.error("Exception for leader node %d in term %d: %s\n%s\n%s",
                                  self.identityIndex, self.currTerm, e.__doc__, str(e), traceback.format_exc())

        heartbeatRpcDuration = time.time() - heartbeatRpcStartTime
        self.nodeLogger.debug("sending append_entries to other node %s took %f seconds", otherNodeDesc.name,
                              heartbeatRpcDuration)


        return appendEntriesRetVal

    def exposed_request_vote(self, candidateTerm, candidateIndex):
        willVote = False

        voteRequestStartTime = time.time()

        self.nodeLogger.debug("about to acquire LOCK to execute request_vote RPC for candidate node %d "
                              "which was in term %d", candidateIndex, candidateTerm)
        self.stateLock.acquire()
        self.nodeLogger.debug("successfully acquired LOCK to execute request_vote RPC for candidate node %d "
                              "which was in term %d", candidateIndex, candidateTerm)

        termAtStartOfVoteRequest = self.currTerm

        if candidateTerm < self.currTerm:
            self.nodeLogger.info("while in term %d, received request_vote() from stale candidate %d "
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
                self.currLeader = None
                self.currTerm = candidateTerm
                self._save_node_state()
                self._restart_timeout()
            else:
                if self.exposed_is_leader():
                    self.nodeLogger.warning("elected leader %d received request_vote() from candidate %d "
                                            "when both are in term %d", self.identityIndex, candidateIndex,
                                            self.currTerm)
                elif self.isCandidate:
                    self.nodeLogger.warning("candidate node %d received request_vote() from other candidate %d "
                                            "when both are in term %d", self.identityIndex, candidateIndex,
                                            self.currTerm)
                elif self.currLeader is not None:
                    self.nodeLogger.warning("follower node %d received request_vote() from candidate node %d when both "
                                            "are in term %d but another node %d has already been elected leader",
                                            self.identityIndex, candidateIndex, self.currTerm, self.currLeader)

            if not self.exposed_is_leader() and not self.isCandidate \
                    and self.currLeader is None and self.voteTarget is None:
                self.nodeLogger.critical("casting vote for candidate node %d in term %d", candidateIndex, self.currTerm)
                self._restart_timeout()

                self.voteTarget = candidateIndex
                self._save_node_state()
                willVote = True

        self.nodeLogger.debug("releasing LOCK after executing request_vote RPC for candidate node %d "
                              "which was in term %d", candidateIndex, candidateTerm)
        self.stateLock.release()

        voteRequestDuration = time.time() - voteRequestStartTime
        self.nodeLogger.debug("while starting in term %d, executing request_vote for candidate node %d which was in term %d "
                              "took %f seconds", termAtStartOfVoteRequest, candidateIndex, candidateTerm, voteRequestDuration)

        return (self.currTerm, willVote)

    def call_request_vote(self, otherNodeDesc):
        assert self.isCandidate
        requestVoteRetVal = None

        voteRequestRpcStartTime = time.time()

        try:
            nodeConnStream = rpyc.SocketStream.connect(otherNodeDesc.host, otherNodeDesc.port,
                                    timeout=RaftNode.CONNECTION_TIMEOUT, attempts=1)
            nodeConn = rpyc.connect_stream(nodeConnStream)
            otherNodeRoot = nodeConn.root
            timedRequestVoteProxy = helpers.timed(otherNodeRoot.request_vote, RaftNode.CONNECTION_TIMEOUT)
            voteRequestPromise = timedRequestVoteProxy(self.currTerm, self.identityIndex)
            requestVoteRetVal = voteRequestPromise.value
        except AsyncResultTimeout:
            self.nodeLogger.info("connection timed out while candidate node %d in term %d tried to send request_vote "
                                 "to node %s", self.identityIndex, self.currTerm, otherNodeDesc.name)
        except (socket.timeout, ConnectionRefusedError):
            self.nodeLogger.info("candidate node %d in term %d was unable to connect to another node %s",
                                 self.identityIndex, self.currTerm, otherNodeDesc.name)
        except EOFError:
            self.nodeLogger.info("candidate node %d in term %d lost connection to another node %s",
                                 self.identityIndex, self.currTerm, otherNodeDesc.name)
        except Exception as e:
            self.nodeLogger.error("Exception for candidate node %d in term %d: %s\n%s\n%s",
                                  self.identityIndex, self.currTerm, e.__doc__, str(e), traceback.format_exc())

        voteRequestRpcDuration = time.time() - voteRequestRpcStartTime
        self.nodeLogger.debug("sending vote request to node %s took %f seconds", otherNodeDesc.name,
                              voteRequestRpcDuration)

        return requestVoteRetVal

    def check_for_election_timeout(self):
        self.nodeLogger.debug("about to acquire LOCK to check for election timeout")
        self.stateLock.acquire()
        self.nodeLogger.debug("successfully acquired LOCK to check for election timeout")

        if self.exposed_is_leader():
            self.nodeLogger.info("this node is ignoring an election timeout because it's the leader and so releases the LOCK")
            self.stateLock.release()
        else:
            self.nodeLogger.debug("checking whether election should be started")
            if (time.time() - self.lastContactTimestamp) > self.electionTimeout:
                self.isCandidate = True
                self.currLeader = None
                self.voteTarget = self.identityIndex
                self.currTerm += 1
                self._save_node_state()
                self._restart_timeout()

                self.nodeLogger.critical("starting election for the new term %d", self.currTerm)
                electionTerm = self.currTerm


                numVotes = 1
                numNodes = 1 + len(self.otherNodes)

                nodesToContact = self.otherNodes.copy()

                self.nodeLogger.debug("about to contact the %d other nodes", len(nodesToContact))

                self.nodeLogger.debug("releasing the LOCK after starting election for term %d", self.currTerm)
                self.stateLock.release()

                while len(nodesToContact) > 0:
                    currOtherNode = nodesToContact.pop(0)

                    self.nodeLogger.debug("about to acquire LOCK to send vote request to node %s", currOtherNode.name)
                    self.stateLock.acquire()
                    self.nodeLogger.debug("successfully acquired LOCK to send vote request to node %s", currOtherNode.name)

                    if not self.isCandidate or self.currLeader is not None or electionTerm != self.currTerm:
                        self.nodeLogger.debug("releasing LOCK (before contacting node %s) as part of terminating the "
                                              "election which was running for term %d", currOtherNode.name, electionTerm)
                        self.stateLock.release()
                        break

                    self.nodeLogger.debug("releasing LOCK just before requesting vote from node %s", currOtherNode.name)
                    self.stateLock.release()


                    self.nodeLogger.debug("sending vote request to node %s, with %d more nodes "
                                          "to be contacted afterwards", currOtherNode.name, len(nodesToContact))
                    nodeVoteResponse = self.call_request_vote(currOtherNode)


                    self.nodeLogger.debug("acquiring LOCK in order to process results of requesting vote from node %s",
                                          currOtherNode.name)
                    self.stateLock.acquire()
                    self.nodeLogger.debug("successfully acquired LOCK in order to process results of requesting vote from node %s",
                                          currOtherNode.name)

                    if not self.isCandidate or self.currLeader is not None or electionTerm != self.currTerm:
                        self.nodeLogger.debug("releasing LOCK (after contacting node %s) as part of terminating the "
                                              "election which was running for term %d", currOtherNode.name, electionTerm)
                        self.stateLock.release()
                        break


                    if nodeVoteResponse is None:
                        nodesToContact.append(currOtherNode)
                    elif nodeVoteResponse[1]:
                        numVotes += 1
                        self.nodeLogger.critical("received vote from other node %s in term %d", currOtherNode.name,
                                             self.currTerm)
                    else:
                        responderTerm = nodeVoteResponse[0]
                        if responderTerm > self.currTerm:
                            self.nodeLogger.critical("terminating election for term %d because a vote request response"
                                                 " informed this node of higher term %d", self.currTerm, responderTerm)
                            self.isCandidate = False
                            self.voteTarget = None
                            self.currLeader = None
                            self.currTerm = responderTerm
                            self._save_node_state()
                            self._restart_timeout()

                    # possible race condition with _leaderStatus?
                    if numVotes > numNodes / 2.0:
                        self.nodeLogger.critical("becoming the leader for the term %d with %d votes!!!",
                                                 self.currTerm, numVotes)
                        self._leaderStatus = True
                        self.isCandidate = False
                        self.voteTarget = None
                        self.currLeader = self.identityIndex
                        self._save_node_state()

                        #releases lock before control flow leaves this function
                        self.nodeLogger.debug("releasing the LOCK after winning election for term %d", self.currTerm)
                        self.stateLock.release()

                        self.send_heartbeats()

                    #handles lock releasing in all cases except the one where this node just won an election
                    if numVotes <= numNodes / 2.0:
                        self.nodeLogger.debug("releasing LOCK after contacting a node %s", currOtherNode.name)
                        self.stateLock.release()
            else:
                self.nodeLogger.debug("releasing LOCK after finding that it isn't time for an election yet")
                self.stateLock.release()

    def send_heartbeats(self):

        self.stateLock.acquire()

        if not self.exposed_is_leader():
            self.nodeLogger.warning("in term %d, node attempted to send heartbeats out despite not being the leader (and releases the LOCK)",
                                    self.currTerm)
            self.stateLock.release()
        else:
            heartbeatTimer = threading.Timer(self.heartbeatInterval, self.send_heartbeats)
            heartbeatTimer.start()

            leaderTerm = self.currTerm

            nodesToContact = self.otherNodes.copy()

            self.nodeLogger.debug("release the LOCK just before contacting the %d other nodes", len(nodesToContact))

            self.stateLock.release()

            while len(nodesToContact) > 0:
                currOtherNode = nodesToContact.pop(0)

                self.nodeLogger.debug("acquiring the LOCK to send heartbeat to node %s", currOtherNode.name)
                self.stateLock.acquire()
                self.nodeLogger.debug("successfully acquired the LOCK to send heartbeat to node %s", currOtherNode.name)

                if not self.exposed_is_leader():
                    self.nodeLogger.info("former leader (from term %d) is releasing the LOCK rather than try to send any more heartbeats (before trying to contact node %s)", leaderTerm, currOtherNode.name)
                    self.stateLock.release()
                    break

                self.nodeLogger.debug("releasing LOCK just before sending heartbeat to node %s", currOtherNode.name)
                self.stateLock.release()


                self.nodeLogger.debug("sending heartbeat to node %s, with %d more nodes to be contacted "
                                      "afterwards", currOtherNode.name, len(nodesToContact))
                nodeHeartbeatResponse = self.call_append_entries(currOtherNode)


                self.nodeLogger.debug("acquiring the LOCK to process node %s 's response to a heartbeat", currOtherNode.name)
                self.stateLock.acquire()
                self.nodeLogger.debug("successfully acquired the LOCK to process node %s 's response to a heartbeat", currOtherNode.name)

                if not self.exposed_is_leader():
                    self.nodeLogger.info("former leader (from term %d) is releasing the LOCK rather than try to send any more heartbeats (after trying to contact node %s)", leaderTerm, currOtherNode.name)
                    self.stateLock.release()
                    break


                if nodeHeartbeatResponse is None:
                    nodesToContact.append(currOtherNode)
                else:
                    responderTerm = nodeHeartbeatResponse[0]
                    if responderTerm > self.currTerm:
                        self.nodeLogger.critical("this node was leader in term %d but is abandoning that status because "
                                             "a heartbeat response informed it of a higher term %d",
                                             self.currTerm, responderTerm)
                        self._leaderStatus = False
                        self.currLeader = None
                        self.currTerm = responderTerm
                        self._restart_timeout()

                self.nodeLogger.debug("releasing the LOCK after sending heartbeat to node %s", currOtherNode.name)
                self.stateLock.release()


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
