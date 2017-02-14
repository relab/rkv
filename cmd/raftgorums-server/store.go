package main

import "sync"

// Store is a key-value store backed by a map.
type Store struct {
	slock sync.RWMutex
	store map[string]string
}

// NewStore returns a newly initialized store.
func NewStore() *Store {
	return &Store{
		store: make(map[string]string),
	}
}

// Lookup gets a value from the map given a key. It returns a bool indicating if
// the value was found or not.
func (s *Store) Lookup(key string) (string, bool) {
	s.slock.RLock()
	defer s.slock.RUnlock()

	value, ok := s.store[key]

	return value, ok
}

// Insert inserts value in map given a key. It returns true if the value was
// successfully inserted. False indicates that the client should retry, as we
// cannot know if the value was inserted successfully or not.
func (s *Store) Insert(key, value string) bool {
	s.slock.Lock()
	defer s.slock.Unlock()

	s.store[key] = value

	return true
}
