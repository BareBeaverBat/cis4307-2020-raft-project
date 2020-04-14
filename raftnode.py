import logging

import rpyc
import sys


class NodeRef:
	def __init__(self, name, hostAddr, port):
		self.name=  name
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
	def __init__(self, configFileName, nodeIdentityIndex):
		self.identityIndex = nodeIdentityIndex
		self.otherNodes = []
		with open(configFileName) as nodesConfigFile:
			nodesConfigFile.readline() #ignore first line with node count

			for nodeInd, nodeLine in enumerate(nodesConfigFile):
				if nodeInd != nodeIdentityIndex:
					nodeTerms = nodeLine.split(":")
					otherNodeName = nodeTerms[0].strip()
					otherNodeHost = nodeTerms[1].strip()
					otherNodePort = nodeTerms[2].strip()
					otherNodePort = int(otherNodePort)
					otherNode = NodeRef(otherNodeName, otherNodeHost, otherNodePort)
					self.otherNodes.append(otherNode)


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

	nodeNum =-1
	nodePorts= []

	configFileName = sys.argv[1]
	with open(configFileName) as configFile:
		nodeNumLine = configFile.readline()
		if nodeNumLine[:2] == "N:":
			nodeNumStr = nodeNumLine[2:]
			nodeNumStr = nodeNumStr.strip()
			nodeNum= int(nodeNumStr)
		else:
			logging.critical("invalid config file- bad initial node count line: ", nodeNumLine)
			raise Exception("bad config file")

		for nodeLine in configFile:
			nodeTerms = nodeLine.split(":")
			nodePortStr = nodeTerms[2].strip()
			nodePortVal = int(nodePortStr)
			nodePorts.append(nodePortVal)


	if(nodeNum > 0):
		for nodeInd, nodePort in enumerate(nodePorts):
			server = ThreadPoolServer(RaftNode(configFileName, nodeInd), port=nodePort)
			server.start()



