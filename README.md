Raft Leader Election
by Scott Salisbury


Node State
Nodes are identified by their index (in the list of nodes specified by the config file). Nodes keep track of their own index value. 
They also store the index of the leader in the current term, and so one of them can check whether it's the leader.
A node also tracks when it was last contacted by another node, and whether it's a candidate for an election in the current term.

All of this state is backed up to a file whenever any piece of a node's state changes.




Timing
I kept track of the most recent time when the election timer was reset using a field with the exact time of that most recent resetting (receiving a heartbeat, casting a vote in an election, or moving to a new term). Meanwhile, a new thread was spun up with each resetting to check, after the election timeout interval had passed, whether the election timer hadn't been reset in that interval & so whether an election should be started. In that case, that thread would move to a new term and start an election, contacting the other nodes sequentially to ask for their votes.
Also, heartbeats were sent out by another thread, which first set up yet another thread to contacted the other nodes sequentially. 




Concurrency
There were serious risks of race conditions because rpyc executes RPC's from other nodes on separate threads, an election is started on a separate thread, and a round of heartbeats is sent out by its own thread. Therefore, one mutex controls access to a node's mutable fields and another controls access to the node state backup file.
I chose to have only one mutex for all of the node's fields because having multiple locks needing to be acquired at once increased the risk of deadlock (the state backup file lock could only be acquired once a thread had the node state lock). Also, there were few or no cases where an operation could be safely performed with exclusive access to only part of a node's state.

One major consideration with this is that the state lock had to be released before calling an RPC on another node and reacquired after getting a response from that other node. Otherwise, the RPC-calling node would be unresponsive to RPC's from other nodes while it was waiting for the RPC it called to return because RPC functions need access to the node state in order to execute. You'd end up with the nodes deadlocked, each waiting for another to respond to its RPC.
Also, once the node state lock was reacquired after an RPC on another node returned, all of the node state checks which had been done before calling the RPC (like whether the node was still a candidate in the same term's election, or whether it was still the leader in the same term) would need to be checked again in case the first node's state had been changed because it executed an RPC from a third node while waiting for a second node to finish executing an RPC.


