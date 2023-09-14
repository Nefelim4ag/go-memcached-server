package recursemap

import (
	"encoding/binary"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
)

type containerType int32

const (
	stemNode  containerType = 0
	leafNode  containerType = 0
	petalNode containerType = 1

	leafNodeAdequateSize = 1<<31 - 1
)

type (
	// NodeType is thread-safe entrypoint
	NodeType[V any] struct {
		container containerType                   // Type read-only defined before first use
		insiders  atomic.Int32                    // Not used
		nodes     [16]atomic.Pointer[NodeType[V]] // Stupid as shit! Shitty! Fast and furious!
	}

	// Will be direct converted ... on condition? Not sure
	// from NodeType, because I'm fucking stupid
	leafNodeType[V any] struct {
		container containerType // Type read-only defined before first use
		insiders  atomic.Int32
		entries   [16]atomic.Pointer[petalNodeType[V]]
	}

	petalNodeType[V any] struct {
		container containerType
		entries   atomic.Pointer[[]entryType[V]]
	}

	entryType[V any] struct {
		key string
		// Racy of course
		value atomic.Pointer[V]
	}
)

// NewRecurseMap create empty recurse map
func NewRecurseMap[V any]() *NodeType[V] {
	r := &NodeType[V]{
		container: stemNode,
	}
	return r
}

// grow is real function which does tree grow
func (Node *NodeType[V]) grow(offset uint8, lvl uint8) *NodeType[V] {
	// Read all values in slice, create new node and replace old pointer with new
	// Rebalance
	// fmt.Printf("Split node\n")
	var hashRaw [8]byte

	Lnode := (*leafNodeType[V])(unsafe.Pointer(Node))
	Lnode.container = leafNode
	// Spinlock =(
	for Lnode.insiders.Load() > 0 {
		// fmt.Printf("Insiders? %d, data: %+v\n", Lnode.insiders.Load(), Lnode)
		runtime.Gosched()
	}

	newNode := leafNodeType[V]{}
	// Child is petal node now, we want convert it to new leaf node
	child := Node.nodes[offset].Load()
	pNode := (*petalNodeType[V])(unsafe.Pointer(child))
	list := *pNode.entries.Load()
	for i := 0; i < len(list); i++ {
		var newOffset uint8
		key := list[i].key
		value := list[i].value.Load()
		h := xxhash.Sum64String(key)
		binary.BigEndian.PutUint64(unsafe.Slice(&hashRaw[0], 8), h)
		if (lvl+1)%2 == 0 {
			newOffset = hashRaw[(lvl+1)/2] >> 4
		} else {
			newOffset = hashRaw[(lvl+1)/2] & 15
		}
		newNode.set(newOffset, key, value)
	}

	newChild := (*NodeType[V])(unsafe.Pointer(&newNode))
	// fmt.Printf("Replace Petal with Leaf: %p -> %p\n", child, newChild)
	if !Node.nodes[offset].CompareAndSwap(child, newChild) {
		// Test that node has been grown by other thread
		c := Node.nodes[offset].Load()
		if c.container == petalNode {
			panic("Race condition & no one resize node!")
		}

	}

	return Node.nodes[offset].Load()
}

// travelTree create offset list and go deeper till the leafNode
func (Node *NodeType[V]) travelTree(key string) (uint8, *leafNodeType[V]) {
	var hashRaw [8]byte
	h := xxhash.Sum64String(key)
	binary.BigEndian.PutUint64(unsafe.Slice(&hashRaw[0], 8), h)
	// fmt.Printf("%s: set hash %016x\n", key, hashRaw)

	retNode := Node
	offset := uint8(0)
	for i := uint8(0); i < 16; i++ {
		if i%2 == 0 {
			offset = hashRaw[i/2] >> 4
		} else {
			offset = hashRaw[i/2] & 15
		}
		// fmt.Printf("%s: set ~ lvl %d offset %d, retNode %p, data: %+v\n", key, i, offset, retNode, retNode)
		nextNode := retNode.nodes[offset].Load()
		// Next node is not exists, so current is LeafNode, value will be created below in "set"
		if nextNode == nil {
			break
		}

		if nextNode.container == stemNode {
            retNode = retNode.nodes[offset].Load()
            continue
        }

		// Next node is petalNode, so current is LeafNode

		pNode := (*petalNodeType[V])(unsafe.Pointer(nextNode))
		plist := pNode.entries.Load()
		list := *plist
		if plist != nil {
			if len(list) > 15 {
				retNode = retNode.grow(offset, i)
				continue
			}
		}
		break
	}

	Lnode := (*leafNodeType[V])(unsafe.Pointer(retNode))
	// fmt.Printf("%s: set offset %d, lNode %p, data: %+v\n", key, offset, Lnode, Lnode)
	return offset, Lnode
}

func (Node *NodeType[V]) Set(key string, value *V) (*V, bool) {
	offset, Lnode := Node.travelTree(key)
	v, ok := Lnode.set(offset, key, value)
	// fmt.Printf("\n")
	return v, ok
}

// set Does internal set stuff like thread counting, grow, replace
func (Node *leafNodeType[V]) set(offset uint8, key string, value *V) (*V, bool) {
	Node.insiders.Add(1)
	defer Node.insiders.Add(-1)
	for Node.entries[offset].Load() == nil {
		// fmt.Printf("%s: set offset %d - new petalNode\n", key, offset)
		list := make([]entryType[V], 1)
		list[0].key = key
		list[0].value.Store(value)
		petalN := &petalNodeType[V]{
			container: petalNode,
		}
		petalN.entries.Store(&list)

		// fmt.Printf("%s: set offset %d, nil -> %p\n", key, offset, &list)
		if Node.entries[offset].CompareAndSwap(nil, petalN) {
			return nil, false
		}
	}

	// Go deeper

	pNode := Node.entries[offset].Load()
	if pNode.container != petalNode {
		panic("last node in tree is not petal")
	}
	plist := pNode.entries.Load()
	list := *plist
	for i := 0; i < len(list); i++ {
		// fmt.Printf("%s: set offset %d - filter list index: %d\n", key, offset, i)
		if list[i].key == key {
			old := list[i].value.Load()
			list[i].value.Store(value)
			// Check list is not appended, or retry
			if pNode.entries.Load() != plist {
				plist = pNode.entries.Load()
				i = 0 // retry
				continue
			} else {
				return old, false
			}
		}
	}

	// Value not exists in list, RCU append
	for {
		// fmt.Printf("%s: set offset %d - list append\n", key, offset)
		plist := pNode.entries.Load()
		list := *plist
		newList := make([]entryType[V], len(list)+1)
		copy(newList, list)
		newList[len(list)].key = key
		newList[len(list)].value.Store(value)
		if pNode.entries.CompareAndSwap(plist, &newList) {
			return nil, false
		}
	}
}

func (Node *NodeType[V]) Get(key string) (*V, bool) {
	var hashRaw [8]byte
	h := xxhash.Sum64String(key)
	binary.BigEndian.PutUint64(unsafe.Slice(&hashRaw[0], 8), h)
	// fmt.Printf("%s: get hash %016x\n", key, hashRaw)

	retNode := Node
	offset := uint8(0)
	for i := 0; i < 16; i++ {
		if i%2 == 0 {
			offset = hashRaw[i/2] >> 4
		} else {
			offset = hashRaw[i/2] & 15
		}
		if retNode == nil {
			return nil, false
		}
		container := retNode.container

		if container != petalNode{
			// fmt.Printf("%s: set ~ lvl %d offset %d, retNode %p, data: %+v\n", key, i, offset, retNode, retNode)
			retNode = retNode.nodes[offset].Load()
		} else {
			break
		}
	}

	pNode := (*petalNodeType[V])(unsafe.Pointer(retNode))
	// fmt.Printf("%s: get offset %d, petalNode %p, data: %+v\n", key, offset, pNode, pNode)
	plist := pNode.entries.Load()
	if plist == nil {
		return nil, false
	}
	list := *plist
	for i := 0; i < len(list); i++ {
		if list[i].key == key {
			return list[i].value.Load(), true
		}
	}

	return nil, false
}
