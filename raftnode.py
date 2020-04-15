import logging

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
        logFileHandler = logging.FileHandler(nodeName + ".log")
        logFileHandler.setLevel(logging.DEBUG)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logFileHandler.setFormatter(formatter)
        consoleHandler.setFormatter(formatter)
        self.nodeLogger.addHandler(logFileHandler)
        self.nodeLogger.addHandler(consoleHandler)


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
        return False


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
