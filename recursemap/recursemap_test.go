package recursemap

// run with: $ go test --bench=. -test.benchmem .
// @see https://twitter.com/karlseguin/status/524452778093977600
import (
	"math/bits"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alphadose/haxmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkStringMaps(b *testing.B) {
	const keySz = 8
	sizes := []int{32768*16}
	for _, n := range sizes {
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap(b, genStringData(keySz, n))
			})
			b.Run("Recurse.Map", func(b *testing.B) {
				benchmarkCustomMap(b, genStringData(keySz, n))
			})
			// b.Run("Recurse.Map 2W MR", func(b *testing.B) {
			// 	BenchmarkCustomMapWithWrites(b)
			// })
			// b.Run("haxmap 1W MR", func(b *testing.B) {
			// 	BenchmarkHaxMapWithWrites(b)
			// })
			// b.Run("haxmap", func(b *testing.B) {
			//     benchmarkHaxMap(b, genStringData(keySz, n))
			// })
		})
	}
}

type fairLock struct {
	sync.RWMutex
}

func benchmarkRuntimeMap(b *testing.B, keys []string) {
	n := len(keys)
	m := make(map[string]string)
	for _, k := range keys {
		m[string(k)] = string(k)
	}

	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		k := keys[i%n]
		m[k] = k
		_, ok = m[k]
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func benchmarkHaxMap(b *testing.B, keys []string) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := haxmap.New[string, string]()
	for _, k := range keys {
		m.Set(string(k), string(k))
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m.Get(string(keys[uint32(i)&mod]))
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func benchmarkCustomMap(b *testing.B, keys []string) {
	n := len(keys)
	m := NewRecurseMap[string]()
	for _, k := range keys {
		v := string(k)
		m.Set(string(k), &v)
	}

	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		k := keys[i%n]
		m.Set(k, &k)
		_, ok = m.Get(k)
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func BenchmarkRuntimeMapWithWrites(b *testing.B) {
	keys := genStringData(8, 8192)

	m := make(map[string]string)

	var writer atomic.Int32
	l := &fairLock{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// use 1 thread as writer
		if writer.Load() < 1 {
			writer.Add(1)
			for pb.Next() {
				for _, k := range keys {
					l.Lock()
					m[k] = k
					l.Unlock()
				}
			}
		} else {
			for pb.Next() {
				// var ok bool
				for i := 0; i < b.N; i++ {
					l.RLock()
					_, ok := m[keys[i % len(keys)]]
					l.RUnlock()
					_ = ok
				}
			}
		}
	})
	b.ReportAllocs()
}

func BenchmarkSyncMapWithWrites(b *testing.B) {
	keys := genStringData(8, 8192)

	m := sync.Map{}

	var writer atomic.Int32

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// use 1 thread as writer
		if writer.Load() < 1 {
			writer.Add(1)
			for pb.Next() {
				for _, k := range keys {
					m.Store(k, k)
				}
			}
		} else {
			for pb.Next() {
				// var ok bool
				for i := 0; i < b.N; i++ {
					_, ok := m.Load(keys[i % len(keys)])
					_ = ok
				}
			}
		}
	})
	b.ReportAllocs()
}

func BenchmarkCustomMapWithWrites(b *testing.B) {
	keys := genStringData(8, 8192)
	// n := uint32(len(keys))
	// mod := n - 1 // power of 2 fast modulus
	// require.Equal(b, 1, bits.OnesCount32(n))

	m := NewRecurseMap[string]()
	// for _, k := range keys {
	// 	v := string(k)
	// 	m.Set(string(k), &v)
	// }
	var writer atomic.Int32
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// use 1 thread as writer
		if writer.Load() < 1 {
			writer.Add(1)
			for pb.Next() {
				for _, k := range keys {
					v := k
					m.Set(k, &v)
				}
			}
		} else {
			for pb.Next() {
				// var ok bool
				for i := 0; i < b.N; i++ {
					m.Get(keys[i % len(keys)])
				}
			}
		}
	})
	b.ReportAllocs()
}

func BenchmarkHaxMapWithWrites(b *testing.B) {
	keys := genStringData(8, 1024)
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))

	m := haxmap.New[string, string]()
	for _, k := range keys {
		v := string(k)
		m.Set(string(k), v)
	}
	var writer uintptr
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// use 1 thread as writer
		if atomic.CompareAndSwapUintptr(&writer, 0, 1) {
			for pb.Next() {
				for _, k := range keys {
					v := string(k)
					m.Set(string(k), v)
				}
			}
		} else {
			for pb.Next() {
				// var ok bool
				for i := 0; i < b.N; i++ {
					m.Get(string(keys[uint32(i)&mod]))
				}
			}
		}
	})
	b.ReportAllocs()
}

// func TestMemoryFootprint(t *testing.T) {
// 	t.Skip("unskip for memory footprint stats")
// 	var samples []float64
// 	for n := 10; n <= 10_000; n += 10 {
// 		b1 := testing.Benchmark(func(b *testing.B) {
// 			// max load factor 7/8
// 			m := NewMap[int, int](uint32(n))
// 			require.NotNil(b, m)
// 		})
// 		b2 := testing.Benchmark(func(b *testing.B) {
// 			// max load factor 6.5/8
// 			m := make(map[int]int, n)
// 			require.NotNil(b, m)
// 		})
// 		x := float64(b1.MemBytes) / float64(b2.MemBytes)
// 		samples = append(samples, x)
// 	}
// 	t.Logf("mean size ratio: %.3f", mean(samples))
// }

func genStringData(size, count int) (keys []string) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]rune, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([]string, count)
	for i := range keys {
		keys[i] = string(r[:size])
		r = r[size:]
	}
	return
}

// func mean(samples []float64) (m float64) {
// 	for _, s := range samples {
// 		m += s
// 	}
// 	return m / float64(len(samples))
// }

func TestSet(t *testing.T) {
	usefulStrings := []string{
		"foo",
		"114",
		"7855",
		"165",
		"374",
		"foo",
		"631",
		"1134",
		"1278",
		"1523",
		"1631",
		"2345",
		"foo",
		"2532",
		"2992",
		"3075",
		"3184",
		"3620",
		"3621",
		"3654",
		"3761",
		"3871",
		"4078",
		"4466",
		"4758",
		"4993",
		"5520",
		"6136",
		"6366",
		"6477",
		"6737",
		"6890",
		"7187",
		"7257",
		"7407",
		"7461",
		"8421",
		"8450",
		"8457",
		"8621",
		"8809",
		"9591",
		"9602",
		"9634",
	}
	m := NewRecurseMap[string]()
	if v, ok := m.Get("notExist"); ok {
		t.Fatal("Expected not found", v)
	}
	a := "value"
	for _, v := range usefulStrings {
		m.Set(v, &a)
	}
	v := "bar"
	m.Set("foo", &v)
	m.Set("foo", &v)
	if v, ok := m.Get("foo"); ok {
		if *v != "bar" {
			t.Fatalf("Expected bar, got %v", v)
		}
	} else {
		t.Fatal("Key foo not found")
	}
}
