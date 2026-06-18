

Here are my results:
```
{:perf {:latency-graph {:valid? true},
        :rate-graph {:valid? true},
        :valid? true},
 :timeline {:valid? true},
 :exceptions {:valid? true},
 :stats {:valid? true,
         :count 17124,
         :ok-count 17116,
         :fail-count 0,
         :info-count 8,
         :by-f {:assign {:valid? true,
                         :count 262,
                         :ok-count 262,
                         :fail-count 0,
                         :info-count 0},
                :crash {:valid? false,
                        :count 8,
                        :ok-count 0,
                        :fail-count 0,
                        :info-count 8},
                :poll {:valid? true,
                       :count 8699,
                       :ok-count 8699,
                       :fail-count 0,
                       :info-count 0},
                :send {:valid? true,
                       :count 8155,
                       :ok-count 8155,
                       :fail-count 0,
                       :info-count 0}}},
 :availability {:valid? true, :ok-fraction 0.9995328},
 :net {:all {:send-count 356140,
             :recv-count 356140,
             :msg-count 356140,
             :msgs-per-op 20.79771},
       :clients {:send-count 40906,
                 :recv-count 40906,
                 :msg-count 40906},
       :servers {:send-count 315234,
                 :recv-count 315234,
                 :msg-count 315234,
                 :msgs-per-op 18.4089},
       :valid? true},
 :workload {:valid? true,
            :worst-realtime-lag {:time 33.336593208,
                                 :process 5,
                                 :key "9",
                                 :lag 33.247011583},
            :bad-error-types (),
            :error-types (),
            :info-txn-causes ()},
 :valid? true}
```


My only storage are lin-kv and seq-kv. No local storage or gossiping. I took the hint from part B which is to think about when I need linearizability versus sequential consistency. Main ideas are:

The logs must be linearizable. This is quite obvious since the append operation is not commutative. 

For each key key, we need to keep track of the next available offset to append a new message to. I'll call this counter_{key} . Again, since the append operation is not commutative, these counters must also be linearizable. 

For the commit offset commit_{key} for each key, this can be sequentially consistent. Why? Because a commit offset is monotonic. You never commit an offset smaller than previously committed. i.e. max(comOff1, max(comOff2, comOff3)) == max(comOff2, max(comOff1, comOff3)) and so on and so forth. Since the max operation is commutative and idempotent, the real-time ordering of the commit operations doesn't matter, even during a network partition. Eventually, all orderings will arrive at the same highest value of commit offset. 






