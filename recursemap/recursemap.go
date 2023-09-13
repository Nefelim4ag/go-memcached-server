package recursemap

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
)

type nodeType uint64

const (
	empty      nodeType = iota
	dEntryType nodeType = 1 // This is bare entry value
	lNodeType  nodeType = 2 // This is a lead node with all children as slice of entries
	rNodeType  nodeType = 3 // This is a recurse node with all children as other nodes
)

type (
	RootMap[V any] struct {
		// Stupid as shit! Shitty! Fast and furious!
		// 256 * 8byte = 2048 byte of crazy pointers!
		nodes [256]atomic.Pointer[tNodeType[V]]
	}

	// Transition node type
	tNodeType[V any] struct {
		nodes [16]atomic.Pointer[tNodeType[V]]
	}

	// Will be direct converted ... on condition? Not sure
	// from tNodeType, because I'm fucking stupid
	leafNodeType[V any] struct {
		sync.RWMutex
		entries  [16][8]entryType[V]
	}

	entryType[V any] struct {
		key string
		// Racy of course
		value *V
	}
)

func NewRecurseMap[V any]() *RootMap[V] {
	RR := RootMap[V]{}

	// Preallocation
	// for k, _ := range RR.nodes {
	// 	RR.nodes[k] = &atomic.Pointer[tNodeType]{}
	// }
	return &RR
}

func (RR *RootMap[V]) travelTree(key string) (uint8, *leafNodeType[V]) {
	var hashRaw [8]byte
	h := xxhash.Sum64String(key)
	binary.LittleEndian.PutUint64(unsafe.Slice(&hashRaw[0], 8), h)

	Raddress := hashRaw[0]

	for RR.nodes[Raddress].Load() == nil {
		newNode := new(tNodeType[V])
		RR.nodes[Raddress].CompareAndSwap(nil, newNode)
	}

	Rnode := RR.nodes[Raddress].Load()
	hashOffset := 1
	for hashOffset < len(hashRaw)-1 {
		upper := hashRaw[hashOffset] >> 4
		lower := hashRaw[hashOffset] & 15
		for Rnode.nodes[upper].Load() == nil {
			newNode := new(tNodeType[V])
			Rnode.nodes[upper].CompareAndSwap(nil, newNode)
		}
		Rnode = Rnode.nodes[upper].Load()

		for Rnode.nodes[lower].Load() == nil {
			newNode := new(tNodeType[V])
			Rnode.nodes[lower].CompareAndSwap(nil, newNode)
		}
		Rnode = Rnode.nodes[lower].Load()
		hashOffset++
	}

	// Hash tail handle manually
	// Now we have last node in tree, this is leafNodeType
	upper := hashRaw[hashOffset] >> 4
	lower := hashRaw[hashOffset] & 15
	for Rnode.nodes[upper].Load() == nil {
		newLNode := new(leafNodeType[V])
		newNode := (*tNodeType[V])(unsafe.Pointer(newLNode))
		Rnode.nodes[upper].CompareAndSwap(nil, newNode)
	}

	Rnode = Rnode.nodes[upper].Load()
	Lnode := (*leafNodeType[V])(unsafe.Pointer(Rnode))

	return lower, Lnode
}

func (RR *RootMap[V]) Set(key string, value *V) {
	offset, Lnode := RR.travelTree(key)
	// Spinlock
	Lnode.Lock()

	// Assume collisions are rare, fast path
	if Lnode.entries[offset][0].key == "" {
		Lnode.entries[offset][0].key = key
		Lnode.entries[offset][0].value = value
		Lnode.Unlock()
		return
	}

	for k, v := range Lnode.entries[offset] {
		if v.key == key {
			Lnode.entries[offset][k].value = value
			Lnode.Unlock()
			return
		}
	}

	for k, v := range Lnode.entries[offset] {
		if v.key == "" {
			Lnode.entries[offset][k].key = key
			Lnode.entries[offset][k].value = value
			Lnode.Unlock()
			return
		}
	}
}

func (RR *RootMap[V]) Get(key string) (*V, bool) {
	var hashRaw [8]byte
	h := xxhash.Sum64String(key)
	binary.LittleEndian.PutUint64(unsafe.Slice(&hashRaw[0], 8), h)

	Raddress := hashRaw[0]
	Rnode := RR.nodes[Raddress].Load()
	if Rnode == nil {
		return nil, false
	}

	hashOffset := 1
	for hashOffset < len(hashRaw)-1 {
		upper := hashRaw[hashOffset] >> 4
		lower := hashRaw[hashOffset] & 15
		Rnode = Rnode.nodes[upper].Load()
		if Rnode == nil {
			return nil, false
		}

		Rnode = Rnode.nodes[lower].Load()
		if Rnode == nil {
			return nil, false
		}
		hashOffset++
	}

	// Hash tail handle manually
	// Now we have last node in tree, this is leafNodeType
	upper := hashRaw[hashOffset] >> 4
	lower := hashRaw[hashOffset] & 15
	for Rnode.nodes[upper].Load() == nil {
		return nil, false
	}

	Rnode = Rnode.nodes[upper].Load()
	Lnode := (*leafNodeType[V])(unsafe.Pointer(Rnode))

	if Lnode.entries[lower][0].key == "" {
		return nil, false
	}

	for _, v := range Lnode.entries[lower] {
		if v.key == key {
			return v.value, true
		}
	}

	return nil, false
}
