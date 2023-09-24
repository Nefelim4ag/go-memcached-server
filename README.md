# go-memcached-server

The Memcached server is written in Go for education goals a protocol-compatible implementation

Base text protocol implemented
```
memcapable -h 127.0.0.1 -p 11211 -a |& grep pass
ascii version                           [pass]
ascii quit                              [pass]
ascii verbosity                         [pass]
ascii set                               [pass]
ascii set noreply                       [pass]
ascii get                               [pass]
ascii gets                              [pass]
ascii mget                              [pass]
ascii flush                             [pass]
ascii flush noreply                     [pass]
ascii add                               [pass]
ascii add noreply                       [pass]
ascii replace                           [pass]
ascii replace noreply                   [pass]
ascii cas                               [pass]
ascii cas noreply                       [pass]
ascii delete                            [pass]
ascii delete noreply                    [pass]
ascii incr                              [pass]
ascii incr noreply                      [pass]
ascii decr                              [pass]
ascii decr noreply                      [pass]
ascii append                            [pass]
ascii append noreply                    [pass]
ascii prepend                           [pass]
ascii prepend noreply                   [pass]
```

Base binary protocol
```
memcapable -h 127.0.0.1 -p 11211 -b |& grep pass
binary noop                             [pass]
binary quit                             [pass]
binary quitq                            [pass]
binary set                              [pass]
binary setq                             [pass]
binary flush                            [pass]
binary flushq                           [pass]
binary add                              [pass]
binary addq                             [pass]
binary get                              [pass]
binary getq                             [pass]
```
# Performance

Main performance issue is with `net.Conn.Write` is too slow on small requests.
net.(*conn).Write -> net.(*netFD).Write -> internal/poll.(*FD).Write -> internal/poll.ignoringEINTRIO -> syscall.write ...

`env GOMAXPROCS=4 ./memcached -m 2048 -pprof=true -loglevel 3`
```
~ docker run --network=host --rm redislabs/memtier_benchmark:latest -h ::1 -p 11211 -P memcache_binary --test-time 50 --hide-histogram
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  50 secs]  0 threads:    30283428 ops,  593644 (avg:  605641) ops/sec, 24.08MB/sec (avg: 24.56MB/sec),  0.34 (avg:  0.33) msec latency

4         Threads
50        Connections per thread
50        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        55060.20          ---          ---         0.33358         0.32700         0.86300         1.19100      4241.75
Gets       550582.15       342.67    550239.49         0.32999         0.32700         0.86300         1.16700     20909.16
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     605642.35       342.67    550239.49         0.33032         0.32700         0.86300         1.16700     25150.92
```

`memcached -m 2048 -p 11211 -t4`
```
~ docker run --network=host --rm redislabs/memtier_benchmark:latest -h ::1 -p 11211 -P memcache_binary --test-time 50 --hide-histogram
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%,  50 secs]  0 threads:    30101914 ops,  542152 (avg:  602001) ops/sec, 21.98MB/sec (avg: 24.41MB/sec),  0.37 (avg:  0.33) msec latency

4         Threads
50        Connections per thread
50        Seconds


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        54728.23          ---          ---         0.33573         0.34300         0.63100         1.05500      4216.18
Gets       547262.39       336.61    546925.77         0.33194         0.33500         0.63100         0.99900     20783.12
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     601990.61       336.61    546925.77         0.33229         0.34300         0.63100         0.99900     24999.30
```
