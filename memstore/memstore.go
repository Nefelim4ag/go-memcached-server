package memstore

import (
	"fmt"
	"runtime"
	"time"

	"github.com/alphadose/haxmap"
)

type lruState[V any] struct {
	items_to_cmp uint
	// items_sort_buffer []sortedMEntry
}

type (
	SharedStore[K string, V any] struct {
		size_limit uint64
		item_size_limit uint64
		flush time.Time
		internal *haxmap.Map[K, MEntry[V]]

		m runtime.MemStats
		lru lruState[V]
	}

	MEntry[V any] struct {
		atime time.Time
		size uint64
		item V
	}
)

func NewSharedStore[K string, V any]() *SharedStore[K, V] {
	S := SharedStore[K, V]{
		internal: haxmap.New[K, MEntry[V]](1024),
		flush: time.Now(),
	}

	go S.LRUCrawler()

	return &S
}

func (s *SharedStore[K, V]) Set(key K, value V, size uint64) error {
	if s.item_size_limit > 0 && s.item_size_limit < size {
		return fmt.Errorf("SERVER_ERROR object too large for cache")
	}

	e := MEntry[V]{
		atime: time.Now(),
		size: size,
        item: value,
	}
	s.internal.Set(key, e)

	return nil
}

// func evict(haxmap.Map K, haxmap.Map V) bool {
// 	e := V.(MEntry)
//     _ := K
// 	// if e.atime.Before()
// 	// e.atime = time.Now()
// }

func (s *SharedStore[K, V]) LRUCrawler(){
	last_flush := s.flush
	for {
		runtime.ReadMemStats(&s.m)
		for s.m.Sys > s.size_limit || last_flush.Before(s.flush) {
			items_count := uint(s.internal.Len())
			if s.m.Sys > s.size_limit && items_count < 2 {
				panic(fmt.Sprintf("ENOMEM, how to free? Items count is less than 2: %d ", items_count))
			}

			s.lru.items_to_cmp = max(2, items_count / 128)
			// s.lru.items_sort_buffer = make([]sortedMEntry, 0, s.lru.items_to_cmp)

			i := uint(0)
			for false { //k, v := range s.internal
				if i == s.lru.items_to_cmp {
					break
				}
				i++
				// s.lru.items_sort_buffer = append(s.lru.items_sort_buffer, sortedMEntry{
				// 	key: k,
				// 	e: v,
				// })
			}

			// 		if len(items_sort_buffer) == 0 {
			// 			log.Fatalf("LRU item sort buffer is empty")
			// 		}

			// 		sort.Sort(byATime(items_sort_buffer))
			// 		first := items_sort_buffer[0]
			// 		s.deleteNoLock(first.key)


			last_flush = s.flush
			// s.internal.ForEach(evict)
			runtime.GC()
		}

		time.Sleep(time.Second)
    }
}

func(s *SharedStore[K, V]) Get(key K) (value *V, ok bool) {
	e, ok := s.internal.Get(key)
	if ok {
		if s.flush.After(e.atime) {
			return nil, false
		}
		updated := e
		updated.atime = time.Now()
		go s.internal.CompareAndSwap(key, e, updated)
		value := e.item
		return &value, ok
	}

	return nil, false
}

func (s *SharedStore[K, V]) Delete(key K) {
	s.internal.Del(key)
}

func (s *SharedStore[K, V]) Flush() {
    s.flush = time.Now()
}

func (s *SharedStore[K, V]) SetMemoryLimit(limit uint64) {
	s.size_limit = limit
}

func (s *SharedStore[K, V]) SetItemSizeLimit(limit uint64) {
	s.item_size_limit = limit
}
