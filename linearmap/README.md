## Benchmark
```
goos: linux
goarch: amd64
pkg: nefelim4ag/go-memcached-server/linearmap
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkStringMaps/n=262144/runtime_map-12             14216937                90.17 ns/op            0 B/op          0 allocs/op
BenchmarkStringMaps/n=262144/Linear.Map-12              10099183               119.0 ns/op             0 B/op          0 allocs/op
PASS
ok      nefelim4ag/go-memcached-server/linearmap        3.723s
```
