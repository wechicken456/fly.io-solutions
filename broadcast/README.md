# Broadcast idea

At first this reminded me of Raft, but then I realized there was no leader in this scenario... 

Sending your entire broadcast log to every single neighbor will create a huge volume of traffic in real-time systems, so I pivoted to keeping track of what to send to each neighbor separately. 

To do this we have 2 arrays for *each* neighbor: 

- `pending`: all messages that haven't been sent to this neighbor.
- `inflight`: all messages that have been sent to this neighbor, but haven't been acknowledged by the neighbor yet. 

Then, whenever we need to send to this neighbor, we take everything from the `pending` and move it to the `inflight` array then reset (empty) the `pending` array.

So... when to send to neighbor?


# When to send?

I default to the simplest idea:

1. Send when you just received a broadcast (from the external system). 
2. Send *every* `100ms`, using the 2 arrays above. If the `inflight` array is NOT empty AND we have NOT timed out `100ms` for this neighbor, that means we're sitll waiting for an ack, so don't send. Otherwise, send everything in the `pending` queue for this neighbor. 



