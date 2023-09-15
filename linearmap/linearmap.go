package linearmap

import (
	"math/bits"

	"github.com/zeebo/xxh3"
)

type (
	LinearMap[V any] struct {
		prefix        []mapBucket[V]
		generationOld uint8
		generation    uint8
	}

	mapBucket[V any] struct {
		used          int
		vector        []mapEntry[V]
		minGeneration uint8
	}

	mapEntry[V any] struct {
		hash       uint64
		key        string
		value      V
		filled     bool
	}
)

// NewLinearMap create empty map
func NewLinearMap[V any]() *LinearMap[V] {
	r := &LinearMap[V]{
		prefix: make([]mapBucket[V], 128),
	}
	return r
}

func log2(v int) int {
	return 63 - bits.LeadingZeros64(uint64(v))
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
	prefix := h % uint64(len(LM.prefix))

	if LM.generationOld < LM.generation {
		prefixOld := h % uint64(len(LM.prefix) / 2)
		bucketOld := &LM.prefix[prefixOld]
		moved := 0
		inplace := 0
		if bucketOld.minGeneration < LM.generation {
			newVector := make([]mapEntry[V], 0, len(bucketOld.vector))
			for k, v := range bucketOld.vector {
				if !v.filled {
					break
				}
				vPrefixNew := v.hash % uint64(len(LM.prefix))
				if vPrefixNew != prefixOld {
					moved++
                    bucketOld.vector[k].filled = false
					bucketOld.used--
					LM.unsafeSet(key, v.hash, vPrefixNew, &v.value)
                } else {
					inplace++
					newVector = append(newVector, bucketOld.vector[k])
				}
			}
			bucketOld.vector = newVector
			bucketOld.minGeneration = LM.generation
			//fmt.Printf("Migrate bucket[%0x] moved=%d inplace=%d\n", prefixOld, moved, inplace)
		}
		newMinGen := LM.generation
		for _, v := range LM.prefix {
			if v.minGeneration < newMinGen {
				newMinGen = v.minGeneration
            }
		}
		LM.generationOld = newMinGen
		if LM.generationOld == LM.generation {
			//fmt.Printf("Generation %d, migration completed\n", LM.generationOld)
		}

	}

	LM.unsafeSet(key, h, prefix, value)

	return
}

func (LM *LinearMap[V]) unsafeSet(key string, h uint64, prefix uint64, value *V) {
	bucket := &LM.prefix[prefix]

	if bucket.vector == nil {
		LM.prefix[prefix].vector = make([]mapEntry[V], 0, len(LM.prefix)*2)
	}

	for k, v := range bucket.vector {
		if v.hash == h {
			if bucket.vector[k].key == key {
				bucket.vector[k].value = *value
				return
			}
		}
	}

	newMapEntry := mapEntry[V]{
		filled: true,
		hash:   h,
        key:    key,
        value:  *value,
	}
	bucket.vector = append(bucket.vector, newMapEntry)
	bucket.used++

	if bucket.used > len(LM.prefix)*2 && LM.generationOld == LM.generation {
		// Run ammortized generation grow
		LM.prefix = append(LM.prefix, make([]mapBucket[V], len(LM.prefix))...)
		LM.generation++
	}
}

func (LM *LinearMap[V]) Get(key string) (*V, bool) {
	h := xxh3.HashString(key)
	prefix := h % uint64(len(LM.prefix))
	bucket := &LM.prefix[prefix]

	if LM.generationOld < LM.generation {
		prefixOld := h % uint64(len(LM.prefix) / 2)
		bucketOld := &LM.prefix[prefixOld]
		if bucketOld.minGeneration < LM.generation {
			if prefixOld != prefix {
				if bucketOld.vector == nil {
					bucketOld.minGeneration = LM.generation
				}
				// search old bucket
				for k, v := range bucketOld.vector {
					if !v.filled {
						break
					}
					if v.hash == h {
						return &bucketOld.vector[k].value, true
					}
				}
			}
		}
	}

	if bucket.vector == nil {
		return nil, false
	}

	for _, v := range bucket.vector {
		if v.hash == h {
			if v.key == key {
                return &v.value, true
            }
		}
	}

	return nil, false
}
