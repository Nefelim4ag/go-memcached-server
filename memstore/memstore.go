package memstore

import (
	"fmt"
	"nefelim4ag/go-memcached-server/recursemap"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"
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

	go S.LRUCrawler()

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
		e.atime = time.Now().UnixMicro()
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

	k, v := s.coolmap.ForEach()
	if k != nil {
		if flush > v.atime {
			s.unsafeDelete(*k)
			deleted = true
		}
	}

	return deleted
}

func (s *SharedStore) unsafeEvictItem() {
	_, v := s.coolmap.ForEach()
	oldest := *v
	for i := 0; i < 1024; i++ {
		_, v = s.coolmap.ForEach()
		if v.atime < oldest.atime {
			oldest = *v
		}
	}

	s.unsafeDelete(oldest.Key)
}

func (s *SharedStore) LRUCrawler() {
	last_flush := s.flush

	for {
		s.ctime = time.Now().Unix()

		if last_flush < s.flush {
			flushExpired := 0
			for i := 0; i < int(s.count.Load()); i++ {
				if s.tryExpireRandItem(s.flush) {
					flushExpired++
				}
			}

			slog.Info("memstore - flushed", "expired", flushExpired, "total", s.count.Load())
			last_flush = s.flush
			runtime.GC()
		}

		sizeLimit := s.storeSizeLimit
		if sizeLimit > 0 && s.size.Load() > sizeLimit {
			s.unsafeEvictItem()
		}

		time.Sleep(time.Second)
	}
}
