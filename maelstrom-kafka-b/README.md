# Idea

Idea is to shard the keys into nodes.

For each key, take `hash(key) % num_nodes` to decide which node should process that key. Then, a non-owner would just forward the message for that key to an owner. An owner would then append that message to its log. 

For this to work, the call chain needs to be blocking until either a time out, or until the owner returns a response. 

So a node only keeps the logs for its "owned" keys. 

The main advantage of this approach is avoiding a centralized node/storage and all the lock contentions and CAS (Compare And Swap) that come with it. So a significantly fewer network messages per op count.

Of course a drawback is if a network partition happens and a non-owner cannot reach the owner, then the client would have to continuously retry.




