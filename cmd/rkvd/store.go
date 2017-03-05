package main

import (
	"container/heap"
	"fmt"
	"reflect"
	"strconv"

	"github.com/hashicorp/go-immutable-radix"
	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

// Store is an implementation of raft.StateMachine. It holds client session
// information, and a key-value store.
type Store struct {
	kvs         *iradix.Tree
	sessions    *iradix.Tree
	pendingCmds map[uint64]*Cmds
}

// NewStore initializes and returns a *Store.
func NewStore() *Store {
	return &Store{
		kvs:         iradix.New(),
		sessions:    iradix.New(),
		pendingCmds: make(map[uint64]*Cmds),
	}
}

// Apply implements raft.StateMachine.
func (s *Store) Apply(entry *commonpb.Entry) interface{} {
	switch entry.EntryType {
	case commonpb.EntryNormal:
		var cmd rkvpb.Cmd
		err := cmd.Unmarshal(entry.Data)

		if err != nil {
			panic(fmt.Sprintf("could not unmarshal %v: %v", entry.Data, err))
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
		id := []byte(strconv.FormatUint(i, 10))
		s.sessions, _, _ = s.sessions.Insert(id, uint64(0))

		var pending Cmds
		heap.Init(&pending)
		s.pendingCmds[i] = &pending

		return i
	case rkvpb.Insert:
		var req rkvpb.InsertRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(fmt.Sprintf("could not unmarshal %v: %v", cmd.Data, err))
		}

		id := []byte(strconv.FormatUint(req.ClientID, 10))
		raw, found := s.sessions.Get(id)

		if !found {
			panic(fmt.Sprintf("clientID %v not found", req.ClientID))
		}

		oldSeq, ok := raw.(uint64)

		if !ok {
			panic(fmt.Sprintf("expected uint64 got %s", reflect.TypeOf(raw)))
		}

		nextSeq := oldSeq + 1

		txn := s.kvs.Txn()

		if req.ClientSeq != nextSeq {
			s.PushCmd(req.ClientID, &req)
		} else {
			txn.Insert([]byte(req.Key), req.Value)
			nextSeq++
		}

		for s.HasCmd(req.ClientID, nextSeq) {
			nextReq := s.PopCmd(req.ClientID)
			txn.Insert([]byte(nextReq.Key), nextReq.Value)
			nextSeq++
		}

		s.kvs = txn.CommitOnly()

		if oldSeq != nextSeq {
			s.sessions, _, _ = s.sessions.Insert(id, nextSeq)
		}

		return true
	case rkvpb.Lookup:
		var req rkvpb.LookupRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		raw, found := s.kvs.Get([]byte(req.Key))

		if !found {
			return "(none)"
		}

		val, ok := raw.(string)

		if !ok {
			panic(fmt.Sprintf("expected string got %s", reflect.TypeOf(raw)))
		}

		return val
	default:
		panic(fmt.Sprintf("got unknown cmd type: %v", cmd.CmdType))
	}
}

// Cmds implements container/heap.
type Cmds []*rkvpb.InsertRequest

func (c Cmds) Len() int {
	return len(c)
}

func (c Cmds) Less(i, j int) bool {
	return c[i].ClientSeq < c[j].ClientSeq
}

func (c Cmds) Swap(i, j int) {
	c[i].ClientSeq, c[j].ClientSeq = c[j].ClientSeq, c[i].ClientSeq
}

// Pop implements container/heap.
func (c *Cmds) Pop() interface{} {
	n := len(*c)
	x := (*c)[n-1]
	*c = (*c)[:n-1]
	return x
}

// Push implements container/heap.
func (c *Cmds) Push(x interface{}) {
	*c = append(*c, x.(*rkvpb.InsertRequest))
}

// PushCmd command onto queue.
func (s *Store) PushCmd(clientID uint64, req *rkvpb.InsertRequest) {
	heap.Push(s.pendingCmds[clientID], req)
}

// HasCmd returns true if the next command in the queue is the next entry in
// the sequence.
func (s *Store) HasCmd(clientID, clientSeq uint64) bool {
	pending := s.pendingCmds[clientID]

	if len(*pending) == 0 {
		return false
	}

	if (*pending)[0].ClientSeq != clientSeq {
		return false
	}

	return true
}

// PopCmd removes the next command from the queue and returns it. Only call
// PopCmd after a successful call to HasCmd.
func (s *Store) PopCmd(clientID uint64) *rkvpb.InsertRequest {
	return heap.Pop(s.pendingCmds[clientID]).(*rkvpb.InsertRequest)
}
