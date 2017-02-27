package main

import (
	"container/heap"
	"errors"
	"fmt"

	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

// Store is an implementation of raft.StateMachine. It holds client session
// information, and a key-value store.
type Store struct {
	store    map[string]string
	sessions map[uint64]*Session
}

// NewStore initializes and returns a *Store.
func NewStore() *Store {
	return &Store{
		store:    make(map[string]string),
		sessions: make(map[uint64]*Session),
	}
}

// Apply implements raft.StateMachine.
func (s *Store) Apply(entry *commonpb.Entry) interface{} {
	switch entry.EntryType {
	case commonpb.EntryNormal:
		var cmd rkvpb.Cmd
		err := cmd.Unmarshal(entry.Data)

		if err != nil {
			panic(err)
		}

		return s.applyStore(entry.Index, &cmd)
	case commonpb.EntryConfChange:
		panic("not implemented yet")
	default:
		panic(fmt.Sprintf("got unknown entry type: %v", entry.EntryType))
	}
}

func (s *Store) applyStore(i uint64, cmd *rkvpb.Cmd) interface{} {
	switch cmd.CmdType {
	case rkvpb.Register:
		if _, ok := s.sessions[i]; !ok {
			var pending Cmds
			heap.Init(&pending)
			s.sessions[i] = &Session{pending: &pending}
		}

		return i
	case rkvpb.Insert:
		var req rkvpb.InsertRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		session, ok := s.sessions[req.ClientID]

		if !ok {
			return errors.New("session expired")
		}

		session.Push(&req)

		for session.HasNext() {
			toApply := session.Pop()
			s.store[toApply.Key] = toApply.Value
		}

		return true
	case rkvpb.Lookup:
		var req rkvpb.LookupRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		return s.store[req.Key]
	default:
		panic(fmt.Sprintf("got unknown cmd type: %v", cmd.CmdType))
	}
}
