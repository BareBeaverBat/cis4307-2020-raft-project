import rpyc
import sys

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
	def __init__(self, config):
		pass

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
	server = ThreadPoolServer(RaftNode(sys.argv[1]), port = 6000)
	server.start()

