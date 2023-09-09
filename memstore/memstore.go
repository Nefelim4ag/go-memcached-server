package memstore

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	log "github.com/sirupsen/logrus"
)

type (
	SharedStore[V any] struct {
		size_limit uint64
		item_size_limit uint64
		flush time.Time
		storeShards []StoreShard[V]
		shardsCount uint
	}

	StoreShard[V any] struct {
		_ [64 - 40]byte
		// 8 mutex + 16 rw mutex
		sync.RWMutex
		count uint64 // count of items in shard
		size  uint64 // total size of items in shard
		// 8 ? byte map pointer
		h map[string]MEntry[V] // map in shard
	}

	MEntry[V any] struct {
		atime time.Time
		size uint64
		item *V
	}

	SMEntry struct {
		key string
		atime time.Time
		size uint64
	}
)

func NextPowOf2(n int) uint{
   k := 1
   for ;k < n; {
      k = k << 1
   }
   return uint(k)
}

func NewSharedStore[V any]() *SharedStore[V] {
	numCpu := NextPowOf2(runtime.NumCPU())
	S := SharedStore[V]{
		storeShards: make([]StoreShard[V], numCpu),
		shardsCount: uint(numCpu),
		flush: time.Now(),
	}

	for k, _ := range S.storeShards {
		S.storeShards[k].h = make(map[string]MEntry[V])
	}

	go S.LRUCrawler()

	return &S
}

func (s *SharedStore[V]) getShard(key string) *StoreShard[V] {
	xxhash.New()
	hShort := uint64(0)
	h := xxhash.Sum64String(string(key))
	hShort = h % uint64(s.shardsCount)

	return &s.storeShards[hShort]
}

func (s *SharedStore[V]) Set(key string, value V, size uint64) error {
	if s.item_size_limit > 0 && s.item_size_limit < size {
		return fmt.Errorf("SERVER_ERROR object too large for cache")
	}

	e := MEntry[V]{
		atime: time.Now(),
		size: size,
        item: &value,
	}

	shard := s.getShard(key)
	if shard.size > s.size_limit / uint64(s.shardsCount) {
		shard.evictItem()
	}

	shard.Lock()
	old, ok := shard.h[key]
	if !ok {
		shard.count++
		shard.size += size
	} else {
		if old.size != size {
			shard.size -= old.size
			shard.size += size
		}
	}
	shard.h[key] = e
	shard.Unlock()

	return nil
}

func(s *SharedStore[V]) Get(key string) (value *V, ok bool) {
	shard := s.getShard(key)
	shard.RLock()
	defer shard.RUnlock()

	e, ok := shard.h[key]
	if ok {
		if s.flush.After(e.atime) {
			return nil, false
		}
		// updated := e
		// updated.atime = time.Now()
		value := e.item
		return value, ok
	}

	return nil, false
}

func (s *SharedStore[V]) Delete(key string) {
	shard := s.getShard(key)
	shard.Lock()
	old, ok := shard.h[key]
	if ok {
        shard.count--
		shard.size -= old.size
    }
	delete(shard.h, key)
	shard.Unlock()
}

func (s *SharedStore[V]) Flush() {
    s.flush = time.Now()
}

func (s *SharedStore[V]) SetMemoryLimit(limit uint64) {
	s.size_limit = limit
}

func (s *SharedStore[V]) SetItemSizeLimit(limit uint64) {
	s.item_size_limit = limit
}

func (shard *StoreShard[V]) tryExpireRandItem(flush time.Time) (expired bool) {
	deleted := false

	for k, v := range shard.h {
		if flush.After(v.atime) {
			shard.count--
			shard.size -= v.size
			delete(shard.h, k)
			deleted = true
		}
		break
	}

	return deleted
}

func getOldest(smel []SMEntry) SMEntry {
	if len(smel) == 0 {
		panic("Empty candidate list for deletion from LRU!")
	}

	oldest := smel[0]
	for _, v := range smel {
		if v.atime.Before(oldest.atime) {
			oldest = v
		}
	}

	return oldest
}

func (shard *StoreShard[V]) evictItem() {
	if shard.count == 0 {
		shard.Lock()
		shard.h = make(map[string]MEntry[V])
		shard.Unlock()
		return
	}

	if shard.count < 1024 {
		shard.Lock()
		var oldest_k string
		var oldest_v MEntry[V]
		for k, v := range shard.h {
			oldest_k = k
			oldest_v = v
			break
		}

		for k, v := range shard.h {
			if v.atime.Before(oldest_v.atime) {
                oldest_k = k
                oldest_v = v
            }
        }

		shard.count--
		shard.size -= oldest_v.size
		delete(shard.h, oldest_k)

		shard.Unlock()
		return
	}

	batch_size := min(128, shard.count / 64)
	items := make([]SMEntry, 0, batch_size)

	shard.Lock()
	for k, v := range shard.h {
		if batch_size == 0 {
			break
		}
		sme := SMEntry{
			key: k,
			atime: v.atime,
			size: v.size,
		}

		items = append(items, sme)
		batch_size--
	}

	oldest := getOldest(items)
	shard.count--
	shard.size -= oldest.size
	delete(shard.h, oldest.key)

	shard.Unlock()
}

func (s *SharedStore[V]) LRUCrawler(){
	last_flush := s.flush

	for {
		if last_flush.Before(s.flush) {
			for k, _ := range s.storeShards {
				shard := &s.storeShards[k]
				items_count := shard.count

				flush_expired := uint(0)
				for items_count > 0 {
					shard.Lock()
					batch_size := min(1024, max(1, items_count / 1024))
					for i := uint64(0); i < batch_size; i++ {
						if shard.tryExpireRandItem(s.flush) {
							flush_expired++
						}
						items_count--
                    }
					shard.Unlock()
				}

				log.Infof("Flushed from shard %d: %d\n", k, flush_expired)
			}
			last_flush = s.flush
			runtime.GC()
		}

		for k, _ := range s.storeShards {
			shard := &s.storeShards[k]
			if shard.size > s.size_limit / uint64(s.shardsCount) {
				shard.evictItem()
			}
		}
		runtime.GC()

		time.Sleep(time.Second)
    }
}
