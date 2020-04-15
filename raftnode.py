import logging
import os
import random
import threading
import time

import rpyc
import sys




class NodeRef:
    def __init__(self, name, hostAddr, port):
        self.name = name
        self.host = hostAddr
        self.port = port

class RpcReturn:
    def __init__(self, termNum, outcomeBoolean):
        self.term = termNum
        self.boolResult = outcomeBoolean


'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''


class RaftNode(rpyc.Service):
    ELECTION_TIMEOUT_BASELINE=0.150 #seconds to wait before calling an election
    HEARTBEAT_INTERVAL= ELECTION_TIMEOUT_BASELINE/5
    NODE_STATE_FOLDER = "node_states"

    def _constructNodeStateFilePath(self):
        pathStr = os.path.join(RaftNode.NODE_STATE_FOLDER, "node" + self.identityIndex + ".txt")
        return pathStr

    def _saveNodeState(self):
        nodeStateStorageFilePath = self._constructNodeStateFilePath()
        with open(nodeStateStorageFilePath, mode="w") as nodeStateStorageFile:
            nodeState = "term= %d\n" % self.currTerm
            if self.voteTarget is not None:
                nodeState += "vote=%d\n" % self.voteTarget
            nodeStateStorageFile.write(nodeState)
            nodeStateStorageFile.flush()
            os.fsync(nodeStateStorageFile.fileno())



    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, configFilePath, nodeIdentityIndex):
        self.identityIndex = nodeIdentityIndex

        #set up logging
        nodeName = "raftNode_" + str(nodeIdentityIndex)
        self.nodeLogger = logging.getLogger(nodeName)
        self.nodeLogger.setLevel(logging.DEBUG)
        logFilePath = os.path.join("node_logs", nodeName + ".log")
        logFileHandler = logging.FileHandler(logFilePath)
        logFileHandler.setLevel(logging.DEBUG)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logFileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)
        self.nodeLogger.addHandler(logFileHandler)
        self.nodeLogger.addHandler(consoleHandler)

        #todo check for stored node state that can be recovered
        #todo? do I need to restore leader status?

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

        self.isCandidate = False
        self._leaderStatus = False
        self.currTerm = 0
        self.voteTarget = None # who the node is voting for in the current term

        self.electionTimeout = (1+random.random())*RaftNode.ELECTION_TIMEOUT_BASELINE
        self.lastContactTimestamp = time.time()
        self._lastContactTimestampLock = threading.Lock()

        electionTimer = threading.Timer(self.electionTimeout, self.check_for_election_timeout)
        electionTimer.start()

        self.nodeLogger.debug("I am node %d and I just finished being constructed, with %d fellow nodes",
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

    def exposed_append_entries(self, requesterTerm, leaderIndex):
        if(requesterTerm < self.currTerm):
            self.nodeLogger.info("in term %d, received append_entries() from stale leader %d which thought it was in term %d", self.currTerm, leaderIndex, requesterTerm)
            return RpcReturn(self.currTerm, False)

        #todo update if term >

        self.nodeLogger.debug("in term %d, executing append_entries on behalf of node %d, the leader in term %d",
                              self.currTerm, leaderIndex, requesterTerm)
        with self._lastContactTimestampLock:
            self.lastContactTimestamp = time.time()

        #todo finish?

    def call_append_entries(self, otherNode):
        assert self.exposed_is_leader()
        nodeConn = rpyc.connect(otherNode.host, otherNode.port)
        appendEntriesRetVal = nodeConn.append_entries(self.currTerm, self.identityIndex)
        return appendEntriesRetVal




    def exposed_request_vote(self, requesterTerm, candidateIndex):
        willVote = False

        if requesterTerm < self.currTerm:
            self.nodeLogger.info(
                "in term %d, received request_vote() from stale leader %d which thought it was in term %d",
                self.currTerm, candidateIndex, requesterTerm)
        else:
            with self._lastContactTimestampLock:
                self.lastContactTimestamp = time.time()

            if requesterTerm > self.currTerm:
                if self.voteTarget is not None:
                    self.nodeLogger.warning("was in election for term %d, voting for candidate node %d, "
                                            "when received request for vote in later term %d", self.currTerm,
                                            self.voteTarget, requesterTerm)
                    self.voteTarget = None
                # cast vote here?

                self.isCandidate = False
                self._leaderStatus = False
                self.currTerm = requesterTerm


            else:
                assert not self.exposed_is_leader()

            if  self.voteTarget is None:
                self.voteTarget = candidateIndex

                self._saveNodeState()





                # todo store vote choice on disk
                willVote = True



        return RpcReturn(self.currTerm, willVote)

    def call_request_vote(self, otherNode):
        assert self.isCandidate
        nodeConn = rpyc.connect(otherNode.host, otherNode.port)
        requestVoteRetVal = nodeConn.request_vote(self.currTerm, self.identityIndex)
        return requestVoteRetVal



    def check_for_election_timeout(self):
        self.nodeLogger.debug("checking whether election should be started")
        #todo check if already leader (edge case)
        with self._lastContactTimestampLock:
            if (time.time() - self.lastContactTimestamp) > self.electionTimeout:
                self.isCandidate = True
                self.currTerm += 1
                self.lastContactTimestamp = time.time()

        if self.isCandidate:
            self.nodeLogger.debug("starting election!")
            # todo start election
            numVotes = 1
            numNodes = 1 + len(self.otherNodes)

            for otherNode in self.otherNodes:


                if not self.isCandidate:
                    break

                #possible race condition with _leaderStatus?
                if numVotes > numNodes/2:
                    self._leaderStatus = True
                    self.isCandidate = False

                    heartbeatTimer = threading.Timer(RaftNode.HEARTBEAT_INTERVAL, self.send_heartbeats)
                    heartbeatTimer.start()

                    break



    def send_heartbeats(self):
        assert self.exposed_is_leader()


        pass
        #todo also reset leader's election timer?


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
