package rkv

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	commonpb "github.com/relab/raft/raftpb"
	cmdpb "github.com/relab/rkv/cmdpb"
)

// TODO Context timeout should depend on heartbeat setting.

// ErrSessionExpired indicates that the client session doesn't exist or has
// already expired.
var ErrSessionExpired = errors.New("session expired")

// Store is a key-value store backed by a map.
type Store struct {
	sync.RWMutex

	store       map[string]string
	clients     map[string]uint64
	waitCmd     map[string]chan interface{}
	waitApplied map[uint64][]chan struct{}

	applied uint64

	raft raft.Raft
}

type readRequest struct {
	key   string
	value chan<- string
}

// NewStore returns a newly initialized store.
func NewStore(raft raft.Raft) *Store {
	s := &Store{
		store:       make(map[string]string),
		clients:     make(map[string]uint64),
		waitCmd:     make(map[string]chan interface{}),
		waitApplied: make(map[uint64][]chan struct{}),
		raft:        raft,
	}

	go s.run()

	return s
}

func (s *Store) run() {
	for {
		select {
		case entries := <-s.raft.Committed():
			for _, entry := range entries {
				s.handleEntry(&entry)
			}
		}
	}
}

func (s *Store) handleEntry(entry *commonpb.Entry) {
	s.Lock()
	defer s.Unlock()

	switch entry.EntryType {
	case commonpb.EntryNormal:
		if bytes.Equal(raft.NOOP, entry.Data) {
			return
		}

		var cmd cmdpb.Cmd
		err := cmd.Unmarshal(entry.Data)

		if err != nil {
			// TODO Ignore malformed requests. Log?
			return
		}

		var res interface{}

		switch cmd.CmdType {
		case cmdpb.Register:
			id := fmt.Sprintf("%d", entry.Index)
			res = id
			if _, ok := s.clients[id]; ok {
				// Don't reset sequence number if we receive a
				// duplicate.
				break
			}
			s.clients[id] = 0
		case cmdpb.Insert:
			// TODO We need to return ErrSessionExpired, when the
			// session is expired. TODO If we see seq > oldseq + 1,
			// we need to deal with that, i.e, we might have stale
			// data?
			if oldseq := s.clients[cmd.ClientID]; oldseq >= cmd.Seq {
				// Already applied.
				break
			}

			s.clients[cmd.ClientID] = cmd.Seq
			s.store[cmd.Key] = cmd.Value
		}

		cmdID := cmdUID(&cmd)
		if ch, ok := s.waitCmd[cmdID]; ok {
			ch <- res
			delete(s.waitCmd, cmdID)
		}
	}

	if waiting, ok := s.waitApplied[entry.Index]; ok {
		for _, ch := range waiting {
			close(ch)
		}

		delete(s.waitApplied, entry.Index)
	}

	s.applied = entry.Index
}

// randSeq is used to uniquely identify register command. There is a chance of a
// collision, but I'll take those odds any day.
func randSeq() uint64 {
	return uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
}

func cmdUID(cmd *cmdpb.Cmd) string {
	if cmd.CmdType == cmdpb.Insert {
		return fmt.Sprintf("INSERT:%s%d", cmd.ClientID, cmd.Seq)
	}

	return fmt.Sprintf("REGISTER:%d", cmd.Seq)
}

func (s *Store) Register() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := cmdpb.Cmd{CmdType: cmdpb.Register, Seq: randSeq()}
	b, err := cmd.Marshal()

	if err != nil {
		return "", err
	}

	done := make(chan interface{}, 1)
	// TODO Store cmds under clientID, so they can be expired.
	s.Lock()
	s.waitCmd[cmdUID(&cmd)] = done
	s.Unlock()

	if err := s.raft.ProposeCmd(ctx, b); err != nil {
		return "", err
	}

	select {
	case res := <-done:
		return res.(string), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Lookup gets a value from the map given a key. If an error is returned, the
// client should retry the same request. Note that if the key doesn't exist, the
// empty string is returned.
func (s *Store) Lookup(key string, allowStale bool) (string, error) {
	// TODO Buffer reads to amortize cost.

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if !allowStale {
		readIndex, err := s.raft.Read(ctx)

		if err != nil {
			return "", err
		}

		if err := s.waitUntilApplied(ctx, readIndex); err != nil {
			return "", err
		}
	}

	value := make(chan string, 1)

	go func() {
		s.RLock()
		defer s.RUnlock()
		value <- s.store[key]
	}()

	select {
	case v := <-value:
		return v, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Insert inserts value in map given a key. If an error is returned, the client
// should retry the same request.
func (s *Store) Insert(key, value, id string, seq uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := s.validate(ctx, id, seq)

	if err != nil {
		return err
	}

	cmd := cmdpb.Cmd{
		CmdType:  cmdpb.Insert,
		ClientID: id,
		Seq:      seq,
		Key:      key,
		Value:    value,
	}
	b, err := cmd.Marshal()

	if err != nil {
		return err
	}

	done := make(chan interface{}, 1)
	s.Lock()
	s.waitCmd[cmdUID(&cmd)] = done
	s.Unlock()

	if err := s.raft.ProposeCmd(ctx, b); err != nil {
		return err
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) validate(ctx context.Context, id string, seq uint64) error {
	minIndex, err := strconv.ParseUint(id, 10, 64)

	if err != nil {
		return err
	}

	if err := s.waitUntilApplied(ctx, minIndex); err != nil {
		return err
	}

	s.RLock()
	defer s.RUnlock()

	if oldSeq, ok := s.clients[id]; !ok || oldSeq != seq-1 {
		return ErrSessionExpired
	}

	return nil
}

func (s *Store) waitUntilApplied(ctx context.Context, i uint64) error {
	s.Lock()
	ci := s.applied

	if ci < i {
		done := make(chan struct{})

		// Important that we add done to waitApplied before unlocking.
		// Otherwise we might leak channels in the map.
		s.waitApplied[i] = append(s.waitApplied[i], done)
		s.Unlock()

		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.Unlock()
	return nil
}
