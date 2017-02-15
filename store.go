package rkv

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/relab/raft"
)

// Store is a key-value store backed by a map.
type Store struct {
	slock sync.RWMutex
	store map[string]string

	raft raft.Raft
}

// NewStore returns a newly initialized store.
func NewStore(raft raft.Raft) *Store {
	s := &Store{
		store: make(map[string]string),
		raft:  raft,
	}

	return s
}

// Lookup gets a value from the map given a key. It returns a bool indicating if
// the value was found or not.
func (s *Store) Lookup(key string) (string, error) {
	s.slock.RLock()
	defer s.slock.RUnlock()

	// TODO Buffer reads to amortize cost.
	err := s.raft.Read(context.TODO())

	if err != nil {
		return "", err
	}

	// Empty string if not set. This depends on the application I think. We
	// don't really care about the key being set or not.
	value, _ := s.store[key]

	return value, nil
}

// Insert inserts value in map given a key. It returns true if the value was
// successfully inserted. False indicates that the client should retry, as we
// cannot know if the value was inserted successfully or not.
func (s *Store) Insert(key, value string) error {
	s.slock.Lock()
	defer s.slock.Unlock()

	err := s.raft.ProposeCmd(context.TODO(), []byte(fmt.Sprintf("%s=%s", key, value)))

	if err != nil {
		return err
	}

	// TODO Assumes successful for now.
	s.store[key] = value

	return nil
}
