package main

import (
	"container/heap"
	"fmt"

	memdb "github.com/hashicorp/go-memdb"
	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

type KeyValue struct {
	Key   string
	Value string
}

type Client struct {
	ClientID string
	Seq      uint64
}

// Store is an implementation of raft.StateMachine. It holds client session
// information, and a key-value store.
type Store struct {
	db *memdb.MemDB

	pendingCmds map[uint64]*Cmds
	pendingSeqs map[uint64]uint64
}

// NewStore initializes and returns a *Store.
func NewStore() *Store {
	db, err := memdb.NewMemDB(schema)

	if err != nil {
		panic("Could not create db: " + err.Error())
	}

	return &Store{
		db:          db,
		pendingCmds: make(map[uint64]*Cmds),
		pendingSeqs: make(map[uint64]uint64),
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
		if _, ok := s.pendingCmds[i]; !ok {
			var pending Cmds
			heap.Init(&pending)
			s.pendingCmds[i] = &pending
			s.pendingSeqs[i] = 0
		}

		return i
	case rkvpb.Insert:
		var req rkvpb.InsertRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		s.Push(req.ClientID, &req)
		oldSeq := s.pendingSeqs[req.ClientID]

		var toApply []*rkvpb.InsertRequest
		for s.HasNext(req.ClientID) {
			toApply = append(toApply, s.Pop(req.ClientID))
		}

		if len(toApply) > 0 {
			txn := s.db.Txn(true)
			for _, r := range toApply {
				if err := txn.Insert("kvs", &KeyValue{
					r.Key,
					r.Value,
				}); err != nil {
					panic("Could not insert key-value: " + err.Error())
				}
			}

			newSeq := oldSeq + uint64(len(toApply))
			s.pendingSeqs[req.ClientID] = newSeq

			if err := txn.Insert("sessions", &Client{
				ClientID: fmt.Sprintf("%d", req.ClientID),
				Seq:      newSeq,
			}); err != nil {
				panic("Could not insert updated sequence: " + err.Error())
			}
			txn.Commit()
		}

		return true
	case rkvpb.Lookup:
		var req rkvpb.LookupRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		txn := s.db.Txn(false)
		defer txn.Abort()

		raw, err := txn.First("kvs", "id", req.Key)

		if err != nil {
			panic("Could not lookup '" + req.Key + "': " + err.Error())
		}

		return raw.(*KeyValue).Value
	default:
		panic(fmt.Sprintf("got unknown cmd type: %v", cmd.CmdType))
	}
}

// Push command onto queue.
func (s *Store) Push(clientID uint64, req *rkvpb.InsertRequest) {
	heap.Push(s.pendingCmds[clientID], req)
}

// HasNext returns true if the next command in the queue is the next entry in
// the sequence.
func (s *Store) HasNext(clientID uint64) bool {
	pending := s.pendingCmds[clientID]

	if len(*pending) == 0 {
		return false
	}

	if (*pending)[0].ClientSeq != s.pendingSeqs[clientID]+1 {
		return false
	}

	return true
}

// Pop removes the next command from the queue and returns it. Only call Pop
// after a successful call to HasNext.
func (s *Store) Pop(clientID uint64) *rkvpb.InsertRequest {
	s.pendingSeqs[clientID]++
	return heap.Pop(s.pendingCmds[clientID]).(*rkvpb.InsertRequest)
}
