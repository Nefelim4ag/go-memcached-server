package memstore

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"time"
)

type SharedStore struct {
	sync.RWMutex
	size_limit uint64
	item_size_limit uint64
	current_size uint64
	flush time.Time
	internal map[string] entry
}

type entry struct {
	atime time.Time
	size uint64
	item any
}

type sortedEntry struct {
	key string
	e entry
}

type byATime []sortedEntry

func (a byATime) Len() int           { return len(a) }
func (a byATime) Less(i, j int) bool {
	l := a[i].e.atime
	r := a[i].e.atime

	return l.Before(r)
}
func (a byATime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func NewSharedStore() *SharedStore {
	return &SharedStore{
		flush: time.Now(),
		internal: make(map[string] entry),
	}
}

type Manager interface {
	Set(key string, value struct{})
	Get(key string) struct{}
	Delete(key string)
	SetMemoryLimit(limit uint64)
	SetItemSizeLimit(limit uint64)
}

func (s *SharedStore) Set(key string, value any, size uint64) error {
	if s.item_size_limit > 0 && s.item_size_limit < size {
		return fmt.Errorf("SERVER_ERROR object too large for cache")
	}
	s.Lock()
	defer s.Unlock()

	value_old, ok := s.internal[key]
	if ok {
		s.current_size -= value_old.size
	}

	s.internal[key] = entry{
		atime: time.Now(),
		size: size,
        item: value,
	}

	s.current_size += size

	if s.size_limit > 0 && s.current_size > s.size_limit {
		go s.LRU()
    }

	return nil
}

func (s *SharedStore) LRU() {
	s.Lock()
    defer s.Unlock()

	items_count := len(s.internal)
	if items_count < 2 {
		log.Fatalf("ENOMEM, how to free? Items count is less than 2: %d ", items_count)
	}

	items_to_cmp := max(2, items_count / 128)

	for s.current_size > s.size_limit {
		// Get items_to_cmp and remove oldest
		items_sort_buffer := make([]sortedEntry, 0, items_to_cmp)
		i := 0
		for k, v := range s.internal {
			if i == items_to_cmp {
				break
            }
			i++
			items_sort_buffer = append(items_sort_buffer, sortedEntry{
                key: k,
                e: v,
            })
		}

		if len(items_sort_buffer) == 0 {
			log.Fatalf("LRU item sort buffer is empty")
		}

		sort.Sort(byATime(items_sort_buffer))
		first := items_sort_buffer[0]
		s.deleteNoLock(first.key)
	}

	go runtime.GC()
}

func(s *SharedStore) Get(key string) (any, bool) {
	s.RLock()
	defer s.RUnlock()
	value, ok := s.internal[key]
	if s.flush.After(value.atime) {
		ok = false
	}
	return value.item, ok
}

func (s *SharedStore) deleteNoLock(key string) {
	value, ok := s.internal[key]
	if ok {
		s.current_size -= value.size
		delete(s.internal, key)
	}
}

func (s *SharedStore) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	s.deleteNoLock(key)
}

func (s *SharedStore) Flush() {
    s.flush = time.Now()
}

func (s *SharedStore) SetMemoryLimit(limit uint64) {
	s.size_limit = limit
}

func (s *SharedStore) SetItemSizeLimit(limit uint64) {
	s.item_size_limit = limit
}
