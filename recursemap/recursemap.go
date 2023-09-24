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
		iter *iteratorState[V]
	}

	iteratorState[V any] struct {
		writeLock sync.Mutex
		vhash  uint64
		lastLN *listNodeType[V]
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
	// fmt.Printf("hash: 0x%016x, key: %s\n", h, key)
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

func incVHash(h uint64, lvl uint) uint64 {
	//fmt.Printf("vhash: 0x%016x\n", h)
	// if h&0xffff000000000000 == 0xffff000000000000 {
	// 	fmt.Printf("vhash: 0x%016x -> ", h)
	// }
	switch lvl {
	case 0:
		h = h & 0xf000000000000000
		h = h + 0x1000000000000000
	case 1:
		h = h & 0xff00000000000000
		h = h + 0x0100000000000000
	case 2:
		h = h & 0xfff0000000000000
		h = h + 0x0010000000000000
	case 3:
		h = h & 0xffff000000000000
		h = h + 0x0001000000000000
	case 4:
		h = h & 0xfffff00000000000
		h = h + 0x0000100000000000
	case 5:
		h = h & 0xffffff0000000000
		h = h + 0x0000010000000000
	case 6:
		h = h & 0xfffffff000000000
		h = h + 0x0000001000000000
	case 7:
		h = h & 0xffffffff00000000
		h = h + 0x0000000100000000
	case 8:
		h = h & 0xfffffffff0000000
		h = h + 0x0000000010000000
	case 9:
		h = h & 0xffffffffff000000
		h = h + 0x0000000001000000
	case 10:
		h = h & 0xfffffffffff00000
		h = h + 0x0000000000100000
	case 11:
		h = h & 0xffffffffffff0000
		h = h + 0x0000000000010000
	case 12:
		h = h & 0xfffffffffffff000
		h = h + 0x0000000000001000
	case 13:
		h = h & 0xffffffffffffff00
		h = h + 0x0000000000000100
	case 14:
		h = h & 0xfffffffffffffff0
		h = h + 0x0000000000000010
	case 15:
		h = h + 1
	default:
		panic("Wrong lvl")
	}

	// if h&0xffff000000000000 == 0xffff000000000000 {
	// 	fmt.Printf("0x%016x\n", h)
	// }

	return h
}

func (Node *petalNodeType[V]) rForEachList(lastLN **listNodeType[V]) (*string, *V) {
	for k := range Node.entries {
		lNode := Node.entries[k].Load()
		if lNode == nil {
			continue
		}

		newlNode := new(listNodeType[V])
		newlNode.record.key = lNode.record.key
		newlNode.record.value.Store(lNode.record.value.Load())

		newlNode.next.Store(*lastLN)
		*lastLN = newlNode

		for ln := lNode.next.Load(); ln != nil; ln = ln.next.Load() {
			// fmt.Printf("k: %d, perLNode %p copy, lvl: %d\n", k, ln, dLvl)
			newlNode := new(listNodeType[V])
			newlNode.record.key = ln.record.key
			newlNode.record.value.Store(ln.record.value.Load())
			newlNode.next.Store(*lastLN)
			*lastLN = newlNode
		}
	}

	if lastLN != nil {
		lNode := *lastLN
		*lastLN = lNode.next.Load()
		return &lNode.record.key, lNode.record.value.Load()
	}

	fmt.Printf("Empty petal node %p\n", Node)
	return nil, nil
}

func (Node *NodeType[V]) rForEach(vhash *uint64, lastLN **listNodeType[V], dLvl uint) (*string, *V) {
	if dLvl == 15 {
		panic("It is not supposed to go so deep! lvl 15 must be always petalNode")
	}
	if Node.container == petalNode {
		panic("It is not supposed to go here")
	}

	// map is empty
	allNil := true
	for k := range Node.nodes {
		if Node.nodes[k].Load() != nil {
			allNil = false
			break
		}
	}
	if allNil {
		*vhash = incVHash(*vhash, dLvl)
		return nil, nil
	}

	// if nextNode != nil {
	// 	fmt.Printf("vhash: 0x%016x, dlvl: %d\n", *vhash, dLvl)
	// }

	offset := getOffset(*vhash, dLvl)
	nextNode := Node.nodes[offset].Load()
	// Decrease recursion level manualy
	for i := offset; i < 16 && nextNode == nil; i++ {
		*vhash = incVHash(*vhash, dLvl)
		offset = getOffset(*vhash, dLvl)
		nextNode = Node.nodes[offset].Load()
	}

	if nextNode.container != petalNode {
		return nextNode.rForEach(vhash, lastLN, dLvl+1)
	}

	pNode := (*petalNodeType[V])(unsafe.Pointer(nextNode))
	k, v := pNode.rForEachList(lastLN)
	*vhash = incVHash(*vhash, dLvl)
	return k, v
}

func (Node *NodeType[V]) ForEach() (*string, *V) {
	if Node.iter == nil {
		Node.iter = &iteratorState[V]{}
	}
	vhash := &Node.iter.vhash
	lastLN := &Node.iter.lastLN

	Node.iter.writeLock.Lock()
	defer Node.iter.writeLock.Unlock()
	// Finish current tail
	if *lastLN != nil {
		key, value := (*lastLN).record.key, (*lastLN).record.value.Load()
		*lastLN = (*lastLN).next.Load()
		return &key, value
	}

	return Node.rForEach(vhash, lastLN, 0)
}

// Debug only
func (Node *NodeType[V]) rGetDebug(key string, h uint64, lvl uint) (*V, bool) {
	offset := getOffset(h, lvl)

	if Node.container == petalNode {
		pNode := (*petalNodeType[V])(unsafe.Pointer(Node))
		list := pNode.entries[offset].Load()
		fmt.Printf("pNode[%x] %p (l) -> ", offset, list)
		if list == nil {
			return nil, false
		}

		depth := 0
		for ln := list; ln != nil; ln = ln.next.Load() {
			fmt.Printf("lNode[%d] %p (l)", depth, ln)
			if ln.record.key == key {
				return ln.record.value.Load(), true
			}
			fmt.Printf(" -> ")
			depth++
		}
		return nil, false
	}

	retNode := Node.nodes[offset].Load()
	if retNode == nil {
		return nil, false
	}

	fmt.Printf("Node[%x] %p -> ", offset, retNode)
	return retNode.rGetDebug(key, h, lvl+1)
}

func (Node *NodeType[V]) GetDebug(key string) (*V, bool) {
	h := xxh3.HashString(key)
	fmt.Printf("hash: 0x%016x, key: %s\n", h, key)
	offset := getOffset(h, 0)
	retNode := Node.nodes[offset].Load()
	if retNode == nil {
		return nil, false
	}
	fmt.Printf("Root[%x] %p -> ", offset, retNode)
	v, ok := (*retNode).rGetDebug(key, h, 1)
	fmt.Println()
	return v, ok
}
