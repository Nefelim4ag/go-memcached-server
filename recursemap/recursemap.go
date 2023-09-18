package recursemap

import (
	"fmt"
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
		// Iterator data
		iter      *iteratorState[V]
	}

	iteratorState[V any] struct {
		offset    [16]int
		lastLN    *listNodeType[V]
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
		entries   [16]atomic.Pointer[listNodeType[V]]
	}

	entryType[V any] struct {
		key   string
		value atomic.Pointer[V]
	}

	listNodeType[V any] struct {
		record entryType[V]
		next   atomic.Pointer[listNodeType[V]]
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
	return uint(h>>(60-4*lvl)) & 15
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
			for ln := v.Load(); ln != nil; ln = ln.next.Load() {
				key := ln.record.key
				h := xxh3.HashString(key)
				newNode.rSet(h, lvl+1, key, ln.record.value.Load())
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
	if nextNode == nil {
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
	ln := listNodeType[V]{
		record: entryType[V]{
			key: key,
		},
	}
	ln.record.value.Store(value)
	Node.entries[offset].Store(&ln)
}

func (Node *leafNodeType[V]) createSet(offset uint, h uint64, lvl uint, key string, value *V) {
	// fmt.Printf("%s: set offset %d - new petalNode\n", key, offset)
	petalN := &petalNodeType[V]{
		container: petalNode,
		size:      1,
	}
	petalN.createList(h, lvl+1, key, value)
	// fmt.Printf("%s: set offset %d, nil -> %p\n", key, offset, &list)
	Node.entries[offset].Store(petalN)
}

func (Node *petalNodeType[V]) updateList(h uint64, lvl uint, key string, value *V) (*V, bool) {
	offset := getOffset(h, lvl)
	list := Node.entries[offset].Load()
	if list == nil {
		Node.createList(h, lvl, key, value)
		Node.size++
		return nil, false
	}

	last := list
	for ln := list; ln != nil; ln = ln.next.Load() {
		if ln.record.key == key {
			old := ln.record.value.Load()
			ln.record.value.Store(value)
			return old, true
		}
		last = ln
	}

	for last.next.Load() != nil {
		last = last.next.Load()
	}

	ln := listNodeType[V]{
		record: entryType[V]{
			key: key,
		},
	}
	ln.record.value.Store(value)

	last.next.Store(&ln)
	Node.size++

	return nil, false
}

func (Node *leafNodeType[V]) updateSet(offset uint, h uint64, lvl uint, key string, value *V) (*V, bool) {
	pNode := Node.entries[offset].Load()
	if pNode.container != petalNode {
		panic("last node in tree is not petal")
	}
	v, ok := pNode.updateList(h, lvl+1, key, value)
	if pNode.size > len(pNode.entries)*6 {
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
		for ln := list; ln != nil; ln = ln.next.Load() {
			if ln.record.key == key {
				return ln.record.value.Load(), true
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
	if nextNode == nil {
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

	var old *V
	ok := false
	prevNode := list
	ln := list
	for ; ln != nil; ln = ln.next.Load() {
		if ln.record.key == key {
			old = ln.record.value.Load()
			ok = true
			Node.size--
			break
		}
		prevNode = ln
	}

	// Fisrt node
	if Node.entries[offset].Load() == ln {
		Node.entries[offset].Store(ln.next.Load())
	} else {
		// Remove for middle or tail
		prevNode.next.Store(ln.next.Load())
	}

	return old, ok
}

func (Node *NodeType[V]) ForEach() (*string, *V){
	if Node.iter == nil {
		Node.iter = &iteratorState[V]{}
	}
	offset := &Node.iter.offset
	lastLN := &Node.iter.lastLN

	// Finish current tail
	if *lastLN != nil {
		key, value := (*lastLN).record.key, (*lastLN).record.value.Load()
		*lastLN = (*lastLN).next.Load()
		return &key, value
	}

    var fullLoops [16]int

	dLvl := 0
	cNode := Node
	for dLvl = 0; dLvl < len(offset); dLvl++ {
		if fullLoops[dLvl] > 0 {
			return nil, nil // trap infinite loop
		}

		if dLvl == 0 {
			fmt.Printf("lvl: %d, offset: %d\n", dLvl, offset[dLvl])
		} else {
			fmt.Printf("lvl: %d, offset: %d, parent offset: %d, fullLoops: %d\n", dLvl, offset[dLvl], offset[dLvl-1])
		}

		if cNode == nil {
			if dLvl > 0 {
				dLvl--
				offset[dLvl] = (offset[dLvl] + 1)
				if offset[dLvl] >= len(cNode.nodes) {
					offset[dLvl] = 0
					fullLoops[dLvl]++
                }
				continue
			}
		}

		nextNode := cNode.nodes[offset[dLvl]].Load()
		for ; nextNode == nil && offset[dLvl] < 16 ; {
			nextNode = cNode.nodes[offset[dLvl]].Load()
			offset[dLvl]++
        }

		if offset[dLvl] >= len(cNode.nodes) {
			fullLoops[dLvl]++
			offset[dLvl] = 0
			if dLvl > 0 {
				dLvl--
				// Shift upper offset
				offset[dLvl] = (offset[dLvl] + 1)
				if offset[dLvl] >= len(cNode.nodes) {
					offset[dLvl] = 0
					fullLoops[dLvl]++
                }
				fmt.Printf("Wrap on lvl: %d, parent offset: %d, fullLoops: %d\n", dLvl, offset[dLvl], fullLoops[dLvl])
				continue
			} else {
				return nil, nil
			}
		}

		if cNode.container == petalNode {
			pNode := (*petalNodeType[V])(unsafe.Pointer(cNode))
            list := pNode.entries[offset[dLvl]].Load()
			if list == nil {
				offset[dLvl] = (offset[dLvl] + 1) % 16
                continue
            }
			*lastLN = list.next.Load()
			offset[dLvl]++
			if offset[dLvl] >= len(pNode.entries) {
				offset[dLvl] = 0
				if dLvl > 0 {
					offset[dLvl-1] = (offset[dLvl-1] + 1) % len(pNode.entries)
				} else {
					return nil, nil
				}
			}
			return &list.record.key, list.record.value.Load()
		}

		cNode = cNode.nodes[offset[dLvl]].Load()
	}

	return nil, nil
}
