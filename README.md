# go-memcached-server

The Memcached server is written in Go for education goals a protocol-compatible implementation

Base text protocol implemented, tested by: `memcapable -h 127.0.0.1 -p 11211 -a`

Base binary protocol - in progress

# Performance

Main performance issue is with `net.Conn.Write` is too slow on small requests.
net.(*conn).Write -> net.(*netFD).Write -> internal/poll.(*FD).Write -> internal/poll.ignoringEINTRIO -> syscall.write ...

`go-memcached-server -m 2048`
```
~ docker run --network=host --rm redislabs/memtier_benchmark:latest -h ::1 -p 11211 -P memcache_binary --test-time 50 --hide-histogram
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  50 secs]  0 threads:    23708960 ops,  446697 (avg:  474144) ops/sec, 18.12MB/sec (avg: 19.23MB/sec),  0.45 (avg:  0.42) msec latency

4         Threads
50        Connections per thread
50        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        43106.18          ---          ---         0.42322         0.40700         0.87900         3.87100      3320.78
Gets       431042.64         0.00    431042.64         0.42162         0.40700         0.87100         3.75900     16369.31
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     474148.82         0.00    431042.64         0.42176         0.40700         0.87100         3.77500     19690.09
```

`memcached -m 2048 -p 11211`
```
~ docker run --network=host --rm redislabs/memtier_benchmark:latest -h ::1 -p 11211 -P memcache_binary --test-time 50 --hide-histogram
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  50 secs]  0 threads:    24933893 ops,  476625 (avg:  498648) ops/sec, 19.33MB/sec (avg: 20.22MB/sec),  0.42 (avg:  0.40) msec latency

4         Threads
50        Connections per thread
50        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        45332.80          ---          ---         0.40548         0.38300         0.66300         1.81500      3492.28
Gets       453306.53       242.02    453064.51         0.40057         0.38300         0.65500         1.68700     17214.82
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     498639.33       242.02    453064.51         0.40102         0.38300         0.66300         1.69500     20707.10
```
