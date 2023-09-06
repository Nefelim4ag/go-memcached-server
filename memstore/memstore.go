package memstore

import (
	"sync"
)

type SharedStore struct {
	sync.RWMutex
	internal map[string] any
}

func NewSharedStore() *SharedStore {
	return &SharedStore{
		internal: make(map[string] any),
	}
}

type Manager interface {
	Set(key string, value struct{})
	Get(key string) struct{}
}

func (s *SharedStore) Set(key string, value any) {
	s.Lock()
	defer s.Unlock()
	s.internal[key] = value
}

func(s *SharedStore) Get(key string) (any, bool) {
	s.RLock()
	defer s.RUnlock()
	value, ok := s.internal[key]
	return value, ok
}
