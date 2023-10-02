package linearmap

import (
	"math/bits"
	"unsafe"

	"github.com/zeebo/xxh3"
	"golang.org/x/exp/slices"
)

const (
	slots     = 16
	bucketLen = 30
)

type (
	LinearMap[V any] struct {
		prefix        []mapBucket[V]
		generationOld uint8
		generation    uint8
		notMigrated   int
	}

	mapBucket[V any] struct {
		used          int
		list          *mapNode[V]
		minGeneration uint8
	}

	mapNode[V any] struct {
		bloom uint64
		// Must be ordered - filled first
		nextEmpty int
		entries   [slots]mapEntry[V]
		next      *mapNode[V]
	}

	mapEntry[V any] struct {
		hash  uint64
		key   string
		value *V
	}
)

func (n *mapNode[V]) bloomInsert(h uint64) {
	n.bloom = n.bloom | (1 << (h % 64))
}

func (n *mapNode[V]) bloomReset() {
	n.bloom = 0
}

func (n *mapNode[V]) bloomLookup(h uint64) bool {
	found := n.bloom&(1<<(h%64)) != 0
	return found
}

func (n *mapNode[V]) swap(i int, j int) {
	n.entries[i], n.entries[j] = n.entries[j], n.entries[i]
}

// NewLinearMap create empty map
func NewLinearMap[V any]() *LinearMap[V] {
	r := &LinearMap[V]{
		prefix: make([]mapBucket[V], 4),
	}
	return r
}

func log2(v int) int {
	return 63 - bits.LeadingZeros64(uint64(v))
}

func bitMask(v int) uint64 {
	var mask uint64
	mask = uint64(1<<log2(v)) - 1
	return mask
}

func (bucket *mapBucket[V]) merge(a *mapNode[V], b *mapNode[V]) {
	if a == nil || b == nil {
		return
	}

	// moved := 0
	for b.nextEmpty > 0 && a.nextEmpty < slots {
		b.nextEmpty--
		hash := b.entries[b.nextEmpty].hash
		a.entries[a.nextEmpty] = b.entries[b.nextEmpty]
		a.bloomInsert(hash)
		// moved++
		for k := a.nextEmpty; k > 0; k-- {
			if a.entries[k-1].hash > a.entries[k].hash {
				a.swap(k-1, k)
			}
		}
		a.nextEmpty++
	}

	// fmt.Printf("Merge moved: %d\n", moved)
	b.bloomReset()
	for k := 0; k < b.nextEmpty; k++ {
		b.bloomInsert(b.entries[k].hash)
	}

	if b.next == nil {
		if b.nextEmpty == 0 {
			a.next = nil
		}
		return
	}

	bucket.merge(b, b.next)
}

// Some bitwise magic with prefixes
// Initial all values distributed between
// 0 0b0 bucket
// 1 0b1 bucket
// After grow base prefix, now we have
// 0 0b00  bucket
// 1 0b01  bucket
// 2 0b10 bucket
// 3 0b11 bucket
// Assume hashes
// 0b0010 - was in zero, still in zero
// 0b0110 - was in zero, now in one
// 0b1010 - was in one,  now in third
// 0b1100 - was in one,  now in fourth

func (LM *LinearMap[V]) Set(key string, value *V) {
	h := xxh3.HashString(key)
	prefix := h & bitMask(len(LM.prefix))

	if LM.generationOld < LM.generation {
		prefixOld := h & bitMask(len(LM.prefix)>>1)
		bucketOld := &LM.prefix[prefixOld]
		moved := 0
		inplace := 0
		if bucketOld.minGeneration < LM.generation {
			// Rebalance bucket
			node := bucketOld.list
			bucketOld.minGeneration = LM.generation
			emptyEntry := mapEntry[V]{}

			for ; node != nil; node = node.next {
				node.bloomReset()
				for k := 0; k < node.nextEmpty; k++ {
					p := node.entries[k].hash & bitMask(len(LM.prefix))
					if p == prefixOld {
						inplace++
						node.bloomInsert(node.entries[k].hash)
						continue
					} else {
						moved++
						bucketOld.used--
						LM.unsafeSet(node.entries[k].key, node.entries[k].hash, p, node.entries[k].value)
						node.nextEmpty--
						node.entries[k] = node.entries[node.nextEmpty]
						node.entries[node.nextEmpty] = emptyEntry
						k--
					}
				}
			}
			// fmt.Printf("Migrate bucket[%0x] moved=%d inplace=%d\n", prefixOld, moved, inplace)

			bucketOld.merge(bucketOld.list, bucketOld.list.next)
			LM.notMigrated--
			if LM.notMigrated == 0 {
				newMinGen := LM.generation
				for k := 0; k < (len(LM.prefix) >> 1); k++ {
					if LM.prefix[k].minGeneration < newMinGen {
						// We have at least one not migrated bucket
						newMinGen = LM.prefix[k].minGeneration
						LM.notMigrated++
						break
					}
				}

				LM.generationOld = newMinGen
				if LM.generationOld == LM.generation {
					// fmt.Printf("Generation %d, migration completed\n", LM.generationOld)
				}
			}
		}
	}

	LM.unsafeSet(key, h, prefix, value)

	return
}

func (n *mapNode[V]) unsafeUpdate(key string, h uint64, value *V) (*V, bool) {
	e := mapEntry[V]{
		hash: h,
	}

	s := unsafe.Slice(&n.entries[0], n.nextEmpty)
	i, ok := slices.BinarySearchFunc(s, e, func(a mapEntry[V], b mapEntry[V]) int {
		if a.hash > b.hash {
			return 1
		}
		if a.hash == b.hash {
			return 0
		}
		return -1
	})

	if ok {
		old := n.entries[i].value
		n.entries[i].value = value
		return old, true
	}

	return nil, false
}

func (LM *LinearMap[V]) unsafeSet(key string, h uint64, prefix uint64, value *V) {
	if LM.prefix[prefix].list == nil {
		n := &mapNode[V]{}
		n.bloomInsert(h)
		n.entries[0].hash = h
		n.entries[0].key = key
		n.entries[0].value = value
		n.nextEmpty++
		LM.prefix[prefix].used++
		LM.prefix[prefix].list = n
	}

	bucket := &LM.prefix[prefix]
	node := bucket.list
	for ; node != nil; node = node.next {
		if node.nextEmpty == slots {
			if !node.bloomLookup(h) {
				if node.next == nil {
					// this is last node and it is full
					break
				}
				continue
			}

			_, ok := node.unsafeUpdate(key, h, value)
			if ok {
				return
			}
		}

		if node.bloomLookup(h) && node.nextEmpty < slots {
			for k := 0; k < node.nextEmpty; k++ {
				if node.entries[k].hash == h {
					if node.entries[k].key == key {
						node.entries[k].value = value
						return
					}
				}
			}
		}

		if node.next == nil {
			if node.nextEmpty < slots {
				node.bloomInsert(h)
				node.entries[node.nextEmpty].hash = h
				node.entries[node.nextEmpty].key = key
				node.entries[node.nextEmpty].value = value

				// Sorting
				for k := node.nextEmpty; k > 0; k-- {
					if node.entries[k-1].hash > node.entries[k].hash {
						node.swap(k-1, k)
					}
				}

				node.nextEmpty++
				bucket.used++

				return
			}
			break
		}
	}

	// Unsuccessful finding hash in range -> add new, empty bucket
	if node.next != nil {
		panic("Something goes wrong & tail is not nil")
	}

	n := &mapNode[V]{}

	n.bloomReset()
	n.bloomInsert(h)
	n.entries[n.nextEmpty].hash = h
	n.entries[n.nextEmpty].key = key
	n.entries[n.nextEmpty].value = value
	n.nextEmpty++
	bucket.used++
	node.next = n

	if bucket.used > bucketLen && LM.generationOld == LM.generation {
		// Run ammortized generation grow
		LM.notMigrated = len(LM.prefix)
		LM.prefix = append(LM.prefix, make([]mapBucket[V], len(LM.prefix))...)
		LM.generation++
		for k := range LM.prefix {
			if LM.prefix[k].list == nil {
				LM.prefix[k].minGeneration = LM.generation
				// Bucket is empty, reset generation
			}
		}
	}
}

func (LM *LinearMap[V]) Get(key string) (*V, bool) {
	h := xxh3.HashString(key)
	prefix := h & bitMask(len(LM.prefix))

	if LM.generationOld < LM.generation {
		prefixOld := h & bitMask(len(LM.prefix)>>1)
		bucketOld := &LM.prefix[prefixOld]
		if bucketOld.minGeneration < LM.generation {
			if prefixOld != prefix {
				if bucketOld.list == nil {
					bucketOld.minGeneration = LM.generation
				}
				// search old bucket
				for node := bucketOld.list; node != nil; node = node.next {
					if !node.bloomLookup(h) {
						continue
					}
					// Key can exists in Node
					for k := 0; k < node.nextEmpty; k++ {
						if node.entries[k].hash == h {
							if node.entries[k].key == key {
								return node.entries[k].value, true
							}
						}
					}
				}
			}
		}
	}

	bucket := &LM.prefix[prefix]
	if bucket.list == nil {
		return nil, false
	}

	for node := bucket.list; node != nil; node = node.next {
		if !node.bloomLookup(h) {
			continue
		}
		// Key can exists in Node
		for k := 0; k < node.nextEmpty; k++ {
			if node.entries[k].hash == h {
				if node.entries[k].key == key {
					return node.entries[k].value, true
				}
			}
		}
	}

	return nil, false
}
