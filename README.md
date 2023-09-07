# go-memcached-server

The Memcached server is written in Go for education goals a protocol-compatible implementation

Base text protocol implemented, tested by: `memcapable -h 127.0.0.1 -p 11211 -a`

Base binary protocol - in progress

# Performance

Main performance issue is with `net.Conn.Write` is too slow on small requests.

`go-memcached-server -m 2048`
```
[RUN #1 100%,  30 secs]  0 threads:     1070830 ops,   35289 (avg:   35692) ops/sec, 3.13GB/sec (avg: 3.17GB/sec),  0.45 (avg:  0.45) msec latency

4         Threads
4         Connections per thread
30        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets         3245.19          ---          ---         3.58197         3.51900         5.79100         6.81500   3323209.16
Gets        32447.19         2.40     32444.79         0.13386         0.07900         0.86300         1.71900      3277.86
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals      35692.38         2.40     32444.79         0.44737         0.08700         4.51100         5.75900   3326487.03
```

`memcached -m 2048 -p 11211`
```
~ docker run --network=host --rm redislabs/memtier_benchmark:latest -h ::1 -p 11211 -P memcache_binary -c4 -t4 --test-time 30 --hide-histogram -d $((1024*1024))
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  30 secs]  0 threads:     1706678 ops,   55580 (avg:   56888) ops/sec, 4.94GB/sec (avg: 5.05GB/sec),  0.29 (avg:  0.28) msec latency

4         Threads
4         Connections per thread
30        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets         5172.01          ---          ---         2.36731         2.39900         3.79100         4.35100   5296371.00
Gets        51716.23         0.00     51716.23         0.07231         0.06300         0.35900         0.61500      1963.99
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals      56888.23         0.00     51716.23         0.28096         0.06300         2.89500         3.77500   5298334.99
```
