package memstore

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
	log "github.com/sirupsen/logrus"
)

// Overhead size cost accounting for values
const mEntrySize = 44

type (
	SharedStore struct {
		size_limit      uint64
		item_size_limit uint32
		shardsCount     uint32
		flush           int64
		ctime           int64
		ValuePool       sync.Pool
		storeShards     []StoreShard
	}

	StoreShard struct {
		// 8 mutex + 16 rw mutex
		sync.RWMutex
		count uint64 // count of items in shard
		size  uint64 // total size of items in shard
		cas_s uint64 // cas source monotonically increasing
		// 8 ? byte map pointer
		h map[string]MEntry // map in shard
		_ [8]byte
	}

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

func NextPowOf2(n int) uint {
	k := 1
	for k < n {
		k = k << 1
	}
	return uint(k)
}

func NewSharedStore() *SharedStore {
	numCpu := uint32(NextPowOf2(runtime.NumCPU()))
	S := SharedStore{
		storeShards: make([]StoreShard, numCpu),
		shardsCount: numCpu,
		flush:       time.Now().UnixMicro(),
		ctime:       time.Now().Unix(),
		ValuePool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 8)
				return &b
			},
		},
	}

	for k, _ := range S.storeShards {
		S.storeShards[k].h = make(map[string]MEntry)
	}

	go S.LRUCrawler()

	return &S
}

func (s *SharedStore) getShard(key string) *StoreShard {
	xxhash.New()
	hShort := uint64(0)
	h := xxhash.Sum64String(string(key))
	hShort = h % uint64(s.shardsCount)

	return &s.storeShards[hShort]
}

func (s *SharedStore) Set(key string, entry *MEntry) error {
	if s.item_size_limit > 0 && s.item_size_limit < entry.Size {
		return fmt.Errorf("SERVER_ERROR object too large for cache")
	}

	entry.atime = time.Now().UnixMicro()
	shard := s.getShard(key)

	shard.Lock()
	if shard.size > s.size_limit/uint64(s.shardsCount) {
		shard.unsafeEvictItem()
	}
	shard.cas_s++
	entry.Cas = shard.cas_s
	old, ok := shard.h[key]
	if !ok {
		shard.count++
		shard.size += uint64(entry.Size) + mEntrySize
	} else {
		s.ValuePool.Put(&old.Value)
		shard.size -= uint64(old.Size)
		shard.size += uint64(entry.Size)
	}
	shard.h[key] = *entry

	shard.Unlock()

	return nil
}

func (s *SharedStore) Get(key string) (value *MEntry, ok bool) {
	shard := s.getShard(key)
	shard.RLock()
	defer shard.RUnlock()

	e, ok := shard.h[key]
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
		return &value, ok
	}

	return nil, false
}

func (s *SharedStore) Delete(key string) {
	shard := s.getShard(key)
	shard.Lock()
	old, ok := shard.h[key]
	if ok {
		shard.unsafeDelete(old.Key, &old)
	}
	shard.Unlock()
}

func (s *SharedStore) Flush() {
	s.flush = time.Now().UnixMicro()
}

func (s *SharedStore) SetMemoryLimit(limit uint64) {
	atomic.StoreUint64(&s.size_limit, limit)
}

func (s *SharedStore) SetItemSizeLimit(limit uint32) {
	s.item_size_limit = limit
}

func (shard *StoreShard) unsafeDelete(k string, v *MEntry) {
	shard.count--
	shard.size -= uint64(v.Size) - mEntrySize
	delete(shard.h, k)
}

func (shard *StoreShard) tryExpireRandItem(flush int64) (expired bool) {
	deleted := false

	for k, v := range shard.h {
		if flush > v.atime {
			shard.unsafeDelete(k, &v)
			deleted = true
		}
		break
	}

	return deleted
}

func (shard *StoreShard) unsafeEvictItem() {
	if shard.count == 0 {
		shard.h = make(map[string]MEntry)
		return
	}

	batch_size := 1024
	var oldest MEntry
	for _, v := range shard.h {
		if batch_size == 0 {
			break
		}

		if v.atime < oldest.atime {
			oldest = v
		}

		batch_size--
	}

	shard.unsafeDelete(oldest.Key, &oldest)
}

func (s *SharedStore) LRUCrawler() {
	last_flush := s.flush

	for {
		s.ctime = time.Now().Unix()

		if last_flush < s.flush {
			for k, _ := range s.storeShards {
				shard := &s.storeShards[k]
				items_count := shard.count

				flush_expired := uint(0)
				for items_count > 0 {
					shard.Lock()
					batch_size := min(1024, max(1, items_count/1024))
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
			size_limit := atomic.LoadUint64(&s.size_limit)
			if size_limit > 0 && shard.size > size_limit/uint64(s.shardsCount) {
				shard.Lock()
				shard.unsafeEvictItem()
				shard.Unlock()
			}
		}

		time.Sleep(time.Second)
	}
}
