package linearmap

import (
	"math/bits"

	"github.com/zeebo/xxh3"
)

const (
	slots = 16
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
		bloom      uint64
		emptySlots int
		entries    [slots]mapEntry[V]
		next       *mapNode[V]
	}

	mapEntry[V any] struct {
		hash   uint64
		key    string
		value  *V
		filled bool
	}
)

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
	mask = uint64(1 << log2(v)) - 1
	return mask
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
		prefixOld := h & bitMask(len(LM.prefix) >> 1)
		bucketOld := &LM.prefix[prefixOld]
		moved := 0
		inplace := 0
		if bucketOld.minGeneration < LM.generation {
			// Rebalance bucket
			node := bucketOld.list
			bucketOld.minGeneration = LM.generation

			for ; node != nil; node = node.next {
				var newBloom uint64
				for k := range node.entries {
					if node.entries[k].filled {
						p := node.entries[k].hash & bitMask(len(LM.prefix))
						if p == prefixOld {
							inplace++
							newBloom = newBloom | node.entries[k].hash
							continue
						} else {
							moved++
							bucketOld.used--
							LM.unsafeSet(node.entries[k].key, node.entries[k].hash, p, node.entries[k].value)
							node.emptySlots++
							node.entries[k].filled = false
						}
					}
				}
				node.bloom = newBloom
			}
			// fmt.Printf("Migrate bucket[%0x] moved=%d inplace=%d\n", prefixOld, moved, inplace)

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

func (LM *LinearMap[V]) unsafeSet(key string, h uint64, prefix uint64, value *V) {
	bucket := &LM.prefix[prefix]

	if bucket.list == nil {
		n := &mapNode[V]{
			emptySlots: slots,
		}
		LM.prefix[prefix].list = n
	}

	var lastNode *mapNode[V]
	var lastEmpty *mapEntry[V]
	node := bucket.list
	for ; node != nil; node = node.next {
		if node.bloom&h == 0 && node.emptySlots == 0 {
			continue
		}

		if node.bloom&h > 0 && node.emptySlots == 0 {
			for k := range node.entries {
				if node.entries[k].hash == h {
					if node.entries[k].key == key {
						node.entries[k].value = value
						return
					}
				}
			}
		}

		if node.bloom&h > 0 && node.emptySlots > 0 {
			for k := range node.entries {
				if lastEmpty == nil {
					if !node.entries[k].filled {
						lastNode = node
						lastEmpty = &node.entries[k]
					}
				}

				if node.entries[k].hash == h {
					if node.entries[k].key == key {
						node.entries[k].value = value
						return
					}
				}
			}
		}
	}

	if lastEmpty != nil {
		lastNode.bloom = lastNode.bloom | h
		lastEmpty.filled = true
		lastEmpty.hash = h
		lastEmpty.key = key
		lastEmpty.value = value

		bucket.used++

		return
	}

	// Unsuccessful finding hash in range -> add new, empty bucket
	if node != nil {
		panic("Something goes wrong & tail is not nil")
	}

	n := &mapNode[V]{
		bloom: h,
	}
	n.emptySlots = slots - 1
	n.entries[0].filled = true
	n.entries[0].hash = h
	n.entries[0].key = key
	n.entries[0].value = value

	n.next = bucket.list
	bucket.list = n

	bucket.used++

	if bucket.used > 128 && LM.generationOld == LM.generation {
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
	bucket := &LM.prefix[prefix]

	if LM.generationOld < LM.generation {
		prefixOld := h & bitMask(len(LM.prefix) >> 1)
		bucketOld := &LM.prefix[prefixOld]
		if bucketOld.minGeneration < LM.generation {
			if prefixOld != prefix {
				if bucketOld.list == nil {
					bucketOld.minGeneration = LM.generation
				}
				// search old bucket
				for node := bucketOld.list; node != nil; node = node.next {
					if node.bloom&h == 0 {
						continue
					}
					// Key can exists in Node
					for k := range node.entries {
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

	if bucket.list == nil {
		return nil, false
	}

	for node := bucket.list; node != nil; node = node.next {
		if node.bloom&h == 0 {
			continue
		}
		// Key can exists in Node
		for k := range node.entries {
			if node.entries[k].hash == h {
				if node.entries[k].key == key {
					return node.entries[k].value, true
				}
			}
		}
	}

	return nil, false
}
