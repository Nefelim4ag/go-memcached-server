## Benchmarks

```
# With RWLock
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkRuntimeMapWithWrites-12    	   10000	   1105099 ns/op	     126 B/op	       0 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	11.062s

# RCU N Readers, 1 Writer
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkCustomMapWithWrites-12    	   19226	    293950 ns/op	   10029 B/op	     622 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	6.291s

# SyncMap N Readers, 1 Writer
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/recursemap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkSyncMapWithWrites-12    	   18363	    313556 ns/op	   16824 B/op	    1044 allocs/op
PASS
ok  	nefelim4ag/go-memcached-server/recursemap	6.428s

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
