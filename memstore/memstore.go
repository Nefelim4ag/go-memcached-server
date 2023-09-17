package memstore

import (
	"fmt"
	"nefelim4ag/go-memcached-server/recursemap"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/cespare/xxhash"
)

// Overhead size cost accounting for values
const mEntrySize = 44

type (
	// SharedStore is
	SharedStore struct {
		storeSizeLimit int64
		itemSizeLimit  int32

		count  atomic.Int64
		size   atomic.Int64
		casSrc atomic.Uint64 // cas source monotonically increasing

		flush     int64
		ctime     int64
		ValuePool sync.Pool

		coolmap *recursemap.NodeType[MEntry]
	}

	// MEntry is base memcached record
	MEntry struct {
		Flags   [4]byte
		ExpTime uint32
		Size    uint32
		Cas     uint64
		Key     string
		Value   []byte

		atime int64
	}
)

// NewSharedStore init a new SharedStore
func NewSharedStore() *SharedStore {
	S := SharedStore{
		flush: time.Now().UnixMicro(),
		ctime: time.Now().Unix(),
		ValuePool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 8)
				return &b
			},
		},
		coolmap: recursemap.NewRecurseMap[MEntry](),
	}

	// go S.LRUCrawler()

	return &S
}

// Set set or update value in shared store
func (s *SharedStore) Set(key string, entry *MEntry) error {
	if s.itemSizeLimit > 0 && s.itemSizeLimit < int32(entry.Size) {
		return fmt.Errorf("SERVER_ERROR object too large for cache")
	}

	entry.atime = time.Now().UnixMicro()

	if s.size.Load() > s.storeSizeLimit {
		s.unsafeEvictItem()
	}
	s.casSrc.Add(1)
	entry.Cas = s.casSrc.Load()
	old, ok := s.coolmap.Set(key, entry)
	if !ok {
		s.count.Add(1)
		s.size.Add(int64(entry.Size) + mEntrySize)
	} else {
		s.ValuePool.Put(&old.Value)
		s.size.Add(int64(entry.Size) - int64(old.Size))
	}

	return nil
}

// Get return current value from store
func (s *SharedStore) Get(key string) (value *MEntry, ok bool) {
	e, ok := s.coolmap.Get(key)
	if ok {
		if s.flush > e.atime {
			return nil, false
		}
		if e.ExpTime > 0 && s.ctime > int64(e.ExpTime) {
			return nil, false
		}

		// Dirty hacky test of update items concurently =(
		// fatal error: concurrent map read and map write
		// updated := e
		// updated.atime = time.Now().UnixMicro()
		// shard.h[key] = updated
		value := e
		return value, ok
	}

	return nil, false
}

func (s *SharedStore) Delete(key string) {
	s.unsafeDelete(key)
}

func (s *SharedStore) Flush() {
	s.flush = time.Now().UnixMicro()
}

func (s *SharedStore) SetMemoryLimit(limit int64) {
	s.storeSizeLimit = limit
}

func (s *SharedStore) SetItemSizeLimit(limit int32) {
	s.itemSizeLimit = limit
}

func (s *SharedStore) unsafeDelete(k string) {
	v, ok := s.coolmap.Delete(k)
	if ok {
		s.count.Add(-1)
		s.size.Add(-(int64(v.Size) + mEntrySize))
	}
}

func (s *SharedStore) tryExpireRandItem(flush int64) (expired bool) {
	deleted := false

	// for k, v := range shard.h {
	// 	if flush > v.atime {
	// 		shard.unsafeDelete(k, &v)
	// 		deleted = true
	// 	}
	// 	break
	// }

	return deleted
}

func (s *SharedStore) unsafeEvictItem() {
	// if shard.count == 0 {
	// 	shard.h = make(map[string]MEntry)
	// 	return
	// }

	// batch_size := 1024
	// var oldest MEntry
	// for _, v := range shard.h {
	// 	if batch_size == 0 {
	// 		break
	// 	}

	// 	if v.atime < oldest.atime {
	// 		oldest = v
	// 	}

	// 	batch_size--
	// }

	// s.unsafeDelete(oldest.Key)
}

func (s *SharedStore) LRUCrawler() {
	// last_flush := s.flush

	// for {
	// 	s.ctime = time.Now().Unix()

	// 	if last_flush < s.flush {
	// 		for k, _ := range s.storeShards {
	// 			shard := &s.storeShards[k]
	// 			items_count := shard.count

	// 			flush_expired := uint(0)
	// 			for items_count > 0 {
	// 				shard.Lock()
	// 				batch_size := min(1024, max(1, items_count/1024))
	// 				for i := uint64(0); i < batch_size; i++ {
	// 					if shard.tryExpireRandItem(s.flush) {
	// 						flush_expired++
	// 					}
	// 					items_count--
	// 				}
	// 				shard.Unlock()
	// 			}

<<<<<<< HEAD
				slog.Info("memstore - flushed shard", "shard", k, slog.Uint64("time", uint64(flush_expired)))
			}
			last_flush = s.flush
			runtime.GC()
		}
=======
	// 			log.Infof("Flushed from shard %d: %d\n", k, flush_expired)
	// 		}
	// 		last_flush = s.flush
	// 		runtime.GC()
	// 	}
>>>>>>> 6a73cbd (feat(server/newmap): It works1)

	// 	for k, _ := range s.storeShards {
	// 		shard := &s.storeShards[k]
	// 		size_limit := atomic.LoadUint64(&s.size_limit)
	// 		if size_limit > 0 && shard.size > size_limit/uint64(s.shardsCount) {
	// 			shard.Lock()
	// 			shard.unsafeEvictItem()
	// 			shard.Unlock()
	// 		}
	// 	}

	// 	time.Sleep(time.Second)
	// }
}
