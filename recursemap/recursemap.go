package recursemap

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/xxh3"
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
		writeLock sync.Mutex                      // Not used
		nodes     [16]atomic.Pointer[NodeType[V]] // Stupid as shit! Shitty! Fast and furious!
	}

	// Will be direct converted ... on condition? Not sure
	// from NodeType, because I'm fucking stupid
	leafNodeType[V any] struct {
		container containerType // Type read-only defined before first use
		writeLock sync.Mutex
		entries   [16]atomic.Pointer[petalNodeType[V]]
	}

	petalNodeType[V any] struct {
		container containerType
		size      int
		entries   [16]atomic.Pointer[[]entryType[V]]
	}

	entryType[V any] struct {
		key string
		value atomic.Pointer[V]
	}
)

// NewRecurseMap create empty recurse map, uses RCU, safe for 1 Writer only =(
func NewRecurseMap[V any]() *NodeType[V] {
	r := &NodeType[V]{
		container: stemNode,
	}
	return r
}

// Just get 4 bits no matter in which order we use hash, while we steel use it
func getOffset(h uint64, lvl uint) uint {
	return uint(h >> (60 - 4*lvl)) & 15
}

// grow is real function which does tree grow
func (Node *leafNodeType[V]) splitChild(offset uint, lvl uint) {
	// Child is petal node now, we want convert it to new leaf node
	if lvl == 15 {
		// Hash too short... really?!
		return
	}
	child := Node.entries[offset].Load()
	pNode := (*petalNodeType[V])(unsafe.Pointer(child))
	newNode := NodeType[V]{}
	for _, v := range pNode.entries {
		if v.Load() != nil {
			for _, value := range *v.Load() {
				h := xxh3.HashString(value.key)
				newNode.rSet(h, lvl+1, value.key, value.value.Load())
			}
		}
	}
	cNode := (*NodeType[V])(unsafe.Pointer(Node))
	// fmt.Printf("Replace Petal with Leaf: %p -> %p\n", child, newChild)
	cNode.nodes[offset].Store(&newNode)
}

func (Node *NodeType[V]) rSet(h uint64, lvl uint, key string, value *V) (*V, bool) {
	offset := getOffset(h, lvl)
	Node.writeLock.Lock()

	nextNode := Node.nodes[offset].Load()
	if  nextNode == nil {
		Lnode := (*leafNodeType[V])(unsafe.Pointer(Node))
		Lnode.createSet(offset, h, lvl, key, value)
		Node.writeLock.Unlock()
		return nil, false
	}

	if nextNode.container == petalNode {
		Lnode := (*leafNodeType[V])(unsafe.Pointer(Node))
		v, ok := Lnode.updateSet(offset, h, lvl, key, value)
		Node.writeLock.Unlock()
		return v, ok
	}

	Node.writeLock.Unlock()
	return nextNode.rSet(h, lvl+1, key, value)
}

// Set returns old value or nil
func (Node *NodeType[V]) Set(key string, value *V) (*V, bool) {
	h := xxh3.HashString(key)
	return Node.rSet(h, 0, key, value)
}

func (Node *petalNodeType[V]) createList(h uint64, lvl uint, key string, value *V) {
	offset := getOffset(h, lvl)
    list := make([]entryType[V], 1, 8)
	list[0].key = key
	list[0].value.Store(value)
    Node.entries[offset].Store(&list)
}

func (Node *leafNodeType[V]) createSet(offset uint, h uint64, lvl uint, key string, value *V) {
		// fmt.Printf("%s: set offset %d - new petalNode\n", key, offset)
		petalN := &petalNodeType[V]{
			container: petalNode,
			size: 1,
		}
		petalN.createList(h, lvl+1, key, value)
		// fmt.Printf("%s: set offset %d, nil -> %p\n", key, offset, &list)
		Node.entries[offset].Store(petalN)
}

func (Node *petalNodeType[V]) updateList(h uint64, lvl uint, key string, value *V) (*V, bool) {
	offset := getOffset(h, lvl)
	list := Node.entries[offset].Load()
	if list == nil {
		list := make([]entryType[V], 1, 8)
		list[0].key = key
		list[0].value.Store(value)
		Node.entries[offset].Store(&list)
		Node.size++
		return nil, false
	}

	for k, v := range *list {
		// fmt.Printf("%s: set offset %d - filter list index: %d\n", key, offset, i)
		if v.key == key {
			old := v.value.Load()
			(*list)[k].value.Store(value)
			return old, true
		}
	}

	// fmt.Printf("%s: set offset %d - list append\n", key, offset)
	newEntry := entryType[V]{
		key: key,
	}
	newEntry.value.Store(value)
	newList := append(*list, newEntry)
	Node.entries[offset].Store(&newList)
	Node.size++

	return nil, false
}

func (Node *leafNodeType[V]) updateSet(offset uint, h uint64, lvl uint, key string, value *V) (*V, bool) {
	pNode := Node.entries[offset].Load()
	if pNode.container != petalNode {
		panic("last node in tree is not petal")
	}
	v, ok := pNode.updateList(h, lvl+1, key, value)
	if pNode.size > len(pNode.entries) * 6 {
		// fmt.Printf("%s: set offset %d - grow\n", key, offset)
        Node.splitChild(offset, lvl)
	}
	return v, ok
}

func (Node *NodeType[V]) rGet(key string, h uint64, lvl uint) (*V, bool) {
	offset := getOffset(h, lvl)

	if Node.container == petalNode {
		pNode := (*petalNodeType[V])(unsafe.Pointer(Node))
        list := pNode.entries[offset].Load()
		if list == nil {
			return nil, false
		}
        for _, v := range *list {
            // fmt.Printf("%s: set offset %d - filter list index: %d\n", key, offset, i)
            if v.key == key {
                return v.value.Load(), true
            }
        }
        return nil, false
	}

	retNode := Node.nodes[offset].Load()
	if retNode == nil {
        return nil, false
    }

	return retNode.rGet(key, h, lvl+1)
}

func (Node *NodeType[V]) Get(key string) (*V, bool) {
	h := xxh3.HashString(key)
	offset := getOffset(h, 0)
	retNode := Node.nodes[offset].Load()
	if retNode == nil {
        return nil, false
    }

	return (*retNode).rGet(key, h, 1)
}

// Delete returns old value or nil
func (Node *NodeType[V]) Delete(key string) (*V, bool) {
	h := xxh3.HashString(key)
	return Node.rDelete(h, 0, key)
}

func (Node *NodeType[V]) rDelete(h uint64, lvl uint, key string) (*V, bool) {
	offset := getOffset(h, lvl)
	Node.writeLock.Lock()

	nextNode := Node.nodes[offset].Load()
	if  nextNode == nil {
		return nil, false
	}

	if nextNode.container == petalNode {
		Lnode := (*leafNodeType[V])(unsafe.Pointer(Node))
		v, ok := Lnode.filterSet(offset, h, lvl, key)
		Node.writeLock.Unlock()
		return v, ok
	}

	Node.writeLock.Unlock()
	return nextNode.rDelete(h, lvl+1, key)
}

func (Node *leafNodeType[V]) filterSet(offset uint, h uint64, lvl uint, key string) (*V, bool) {
	pNode := Node.entries[offset].Load()
	if pNode.container != petalNode {
		panic("last node in tree is not petal")
	}
	v, ok := pNode.filterList(h, lvl+1, key)
	return v, ok
}

func (Node *petalNodeType[V]) filterList(h uint64, lvl uint, key string) (*V, bool) {
	offset := getOffset(h, lvl)
	list := Node.entries[offset].Load()
	if list == nil {
		return nil, false
	}

	for k, v := range *list {
		// fmt.Printf("%s: set offset %d - filter list index: %d\n", key, offset, i)
		if v.key == key {
			old := v.value.Load()
			newList := append((*list)[:k], (*list)[k+1:]...)
			Node.entries[offset].Store(&newList)
			Node.size--
			return old, true
		}
	}

	return nil, false
}
