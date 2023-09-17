## Basic Idea
Just use prefix tree, dinamically grown from start of the hash, to avoid overload slice filtering.

Hash 0x0b3f of `str`
```
Root
[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [a] [b] [c] [d] [e] [f]
 |
\ /
First Child
[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [a] [b] [c] [d] [e] [f]
                                             |
                                            \ /
                                        child second
[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [a] [b] [c] [d] [e] [f]
             |
            \ /
        child third
[0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [a] [b] [c] [d] [e] [f]
                                                             |
                                                            \ /
                                                            leaf
                                                            {key: str, value: ...}
```


This is not space efficient or fastest, it just stupid simple and fast enough
Allow to make RCU read by atomic pointers and *simple locks for writing*

## Benchmarks

```
# With RWLock N Readers, 2 Writers
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkRuntimeMapWithWrites-12    	   10000	   1389688 ns/op	     497 B/op	       0 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	13.929s

# RCU/Mutex N Readers, 2 Writer
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkCustomMapWithWrites-12    	   17192	    343816 ns/op	  442306 B/op	   18771 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	6.226s

# SyncMap N Readers, 1 Writer
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkSyncMapWithWrites-12    	   31922	    255321 ns/op	   65265 B/op	    4060 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	8.385s

# Single thread
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkStringMaps/n=524288/runtime_map-12         	10337182	       100.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkStringMaps/n=524288/Recurse.Map-12         	 3844785	       312.9 ns/op	      16 B/op	       1 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	5.165s

```
