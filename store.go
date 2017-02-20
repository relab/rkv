package rkv

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	commonpb "github.com/relab/raft/raftpb"
	cmdpb "github.com/relab/rkv/cmdpb"
)

// Store is a key-value store backed by a map.
type Store struct {
	store map[string]string

	nextID  uint64
	clients map[string]uint64

	waiting map[string]chan interface{}

	raft raft.Raft
}

// NewStore returns a newly initialized store.
func NewStore(raft raft.Raft) *Store {
	s := &Store{
		store:   make(map[string]string),
		clients: make(map[string]uint64),
		waiting: make(map[string]chan interface{}),
		raft:    raft,
	}

	go s.run()

	return s
}

func (s *Store) run() {
	for {
		select {
		case entries := <-s.raft.Committed():
			for _, entry := range entries {
				switch entry.EntryType {
				case commonpb.EntryNormal:
					var cmd cmdpb.Cmd
					err := cmd.Unmarshal(entry.Data)

					if err != nil {
						// TODO Ignore malformed requests. Log?
						continue
					}

					var res interface{}

					switch cmd.CmdType {
					case cmdpb.Register:
						id := fmt.Sprintf("%d", entry.Index)
						res = id
						if _, ok := s.clients[id]; ok {
							// Don't reset sequence
							// number if we receive
							// a duplicate.
							break
						}
						s.clients[id] = 0
					case cmdpb.Insert:
						// TODO We need to return
						// ErrSessionExpired, when the
						// session is expired. TODO If
						// we see seq > oldseq + 1, we
						// need to deal with that, i.e,
						// we might have stale data?
						if oldseq := s.clients[cmd.ClientID]; oldseq >= cmd.Seq {
							// Already applied.
							break
						}

						s.clients[cmd.ClientID] = cmd.Seq
						s.store[cmd.Key] = cmd.Value
					}

					cmdID := cmdUID(&cmd)
					if ch, ok := s.waiting[cmdID]; ok {
						ch <- res
						delete(s.waiting, cmdID)
					}
				}
			}
		}
	}
}

// randSeq is used to uniquely identify register command. There is a chance of a
// collision, but I'll take those odds any day.
func randSeq() uint64 {
	return uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
}

func cmdUID(cmd *cmdpb.Cmd) string {
	switch cmd.CmdType {
	case cmdpb.Insert:
		return fmt.Sprintf("INSERT:%s%d", cmd.ClientID, cmd.Seq)
	case cmdpb.Lookup:
		return fmt.Sprintf("LOOKUP:%s%d", cmd.ClientID, cmd.Seq)
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
	s.waiting[cmdUID(&cmd)] = done

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

// Lookup gets a value from the map given a key. It returns a bool indicating if
// the value was found or not. TODO We need to query the state machine. It's not
// safe to just read the value.
func (s *Store) Lookup(key string) (string, error) {
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
func (s *Store) Insert(id string, seq uint64, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// TODO Check if session is valid. Make it a function.

	cmd := cmdpb.Cmd{
		CmdType:  cmdpb.Register,
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
	s.waiting[cmdUID(&cmd)] = done

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
