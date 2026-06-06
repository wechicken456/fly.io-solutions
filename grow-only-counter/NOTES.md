# 1st approach: spam compare-and-swap


This doesn't work. Consider an example scenario:
```txt
INFO [2026-06-04 20:34:50,716] jepsen worker 2 - jepsen.util 2	:ok	:read	1425
INFO [2026-06-04 20:34:50,730] jepsen worker 0 - jepsen.util 0	:invoke	:read	nil
INFO [2026-06-04 20:34:50,731] jepsen worker 0 - jepsen.util 0	:ok	:read	1429
INFO [2026-06-04 20:34:50,747] jepsen worker 0 - jepsen.util 0	:invoke	:read	nil
INFO [2026-06-04 20:34:50,747] jepsen worker 1 - jepsen.util 1	:invoke	:add	1
INFO [2026-06-04 20:34:50,748] jepsen worker 1 - jepsen.util 1	:ok	:add	1
INFO [2026-06-04 20:34:50,748] jepsen worker 0 - jepsen.util 0	:ok	:read	1429
INFO [2026-06-04 20:34:50,763] jepsen worker 2 - jepsen.util 2	:invoke	:read	nil
INFO [2026-06-04 20:34:50,763] jepsen worker 2 - jepsen.util 2	:ok	:read	1429
INFO [2026-06-04 20:34:50,764] jepsen worker nemesis - jepsen.util :nemesis	:info	:stop-partition	nil
INFO [2026-06-04 20:34:50,764] jepsen worker nemesis - jepsen.util :nemesis	:info	:stop-partition	:network-healed
INFO [2026-06-04 20:34:50,764] jepsen worker 0 - jepsen.generator.interpreter Waiting for recovery...
INFO [2026-06-04 20:35:00,766] jepsen worker 0 - jepsen.util 0	:invoke	:read	nil
INFO [2026-06-04 20:35:00,766] jepsen worker 2 - jepsen.util 2	:invoke	:read	nil
INFO [2026-06-04 20:35:00,766] jepsen worker 1 - jepsen.util 1	:invoke	:read	nil
INFO [2026-06-04 20:35:00,767] jepsen worker 2 - jepsen.util 2	:ok	:read	1429
INFO [2026-06-04 20:35:00,767] jepsen worker 0 - jepsen.util 0	:ok	:read	1430
INFO [2026-06-04 20:35:00,767] jepsen worker 1 - jepsen.util 1	:ok	:read	1430
INFO [2026-06-04 20:35:00,781] jepsen test runner - jepsen.core Run complete, writing
INFO [2026-06-04 20:35:00,833] jepsen node n0 - maelstrom.db Tearing down n0
INFO [2026-06-04 20:35:00,833] jepsen node n1 - maelstrom.db Tearing down n1
INFO [2026-06-04 20:35:00,833] jepsen node n2 - maelstrom.db Tearing down n2
INFO [2026-06-04 20:35:02,768] jepsen node n0 - maelstrom.net Shutting down Maelstrom network
INFO [2026-06-04 20:35:02,769] jepsen test runner - jepsen.core Analyzing...
INFO [2026-06-04 20:35:03,065] jepsen test runner - jepsen.core Analysis complete
INFO [2026-06-04 20:35:03,070] jepsen results - jepsen.store Wrote /home/pwnphofun/Code/programming/Go/fly.io/maelstrom/store/g-counter/20260604T203429.825-0700/results.edn
INFO [2026-06-04 20:35:03,083] jepsen test runner - jepsen.core {:perf {:latency-graph {:valid? true},
        :rate-graph {:valid? true},
        :valid? true},
 :timeline {:valid? true},
 :exceptions {:valid? true},
 :stats {:valid? true,
         :count 1956,
         :ok-count 1956,
         :fail-count 0,
         :info-count 0,
         :by-f {:add {:valid? true,
                      :count 683,
                      :ok-count 683,
                      :fail-count 0,
                      :info-count 0},
                :read {:valid? true,
                       :count 1273,
                       :ok-count 1273,
                       :fail-count 0,
                       :info-count 0}}},
 :availability {:valid? true, :ok-fraction 1.0},
 :net {:all {:send-count 10186,
             :recv-count 10186,
             :msg-count 10186,
             :msgs-per-op 5.2075663},
       :clients {:send-count 3918, :recv-count 3918, :msg-count 3918},
       :servers {:send-count 6268,
                 :recv-count 6268,
                 :msg-count 6268,
                 :msgs-per-op 3.204499},
       :valid? true},
 :workload {:valid? false,
            :errors (#jepsen.history.Op{:index 3915,
                                        :time 30006406713,
                                        :type :ok,
                                        :process 2,
                                        :f :read,
                                        :value 1429,
                                        :final? true}),
            :final-reads (1429 1430 1430),
            :acceptable ([1430 1430])},
 :valid? false}


Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻

```


For each worker node, it has its own replica of the `seq-kv` storage node. And the syncing between `seq-kv` nodes happen in the background not visible to us.

The issue arised because we are using a *[sequentially consistent](https://en.wikipedia.org/wiki/Sequential_consistency)* key-value store. Under SEQ-KV, *physical* time does NOT dictate when a write becomes visible across nodes. When worker 1 executed the `add` operation, it performed a `CompareAndSwap` on its local replica or primary coordinator for that key.

1. The Add: The value successfully updated from 1429 to 1430 within worker 1's accessible network segment.

2. The Partition: The network was partitioned. Worker 2 was isolated from the updates happening on worker 1.

3. The Final Reads: Jepsen queued up three concurrent read invocations at 20:35:00,766 across all workers right as the network healed.

4. The Replication Lag: When worker 2 processed its read, the underlying `seq-kv` replication layer had not yet propagated the new state (`1430`) to worker 2's local store. Under sequential consistency, a node is perfectly permitted to serve reads from its stale local snapshot. It is not forced to block or fetch a synchronous global quorum.



# SOLUTION

We will NOT use any `seq-kv` here. Only use local data structures + gossiping for synchronization:

1. Each node keeps a local `MaxAdd` map, where the key is `nodeId`, and value is the accumulated `delta` for that `nodeId`.
2. So each node will always have its `MaxAdd` up-to-date, obviously.
3. Then, every fixed interval, send/gossip its `MaxAdd` map to other nodes. 
4. Each node receiving a gossip will update a `MaxAdd` pair as `n.MaxAdd[nodeId] = max(n.MaxAdd[nodeId], gossip.MaxAdd[nodeId])`

So in this solution, we priority local first, then merge later. This is possible, *in this problem*, because of:

1. **Idempotency (repetition is harmless)**: The `max` operation applied multiple times will yield the same results. e.g. `x = max(x, y) == max(max(x, y), y) == max(x, max(x, y))` will always yield the same results.
2. **Commutativity (order of operation doesn't matter)**: since there's only the `add` operation. If there is also the subtract operation, then it would NOT work:

- Node A and Node B both start at 0.
- Node A receives a subtract 5 operation. Its local state becomes -5.
- Node A gossips -5 to Node B.
- Node B runs the merge function: max(0, -5).
- The result is 0. The subtraction is completely erased.

