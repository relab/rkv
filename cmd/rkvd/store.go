package main

import (
	"container/heap"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

// SnapTick is the time between finishing taking a snapshot and starting to take
// a new one. TODO Config.
const SnapTick = 20 * time.Second

// Store is an implementation of raft.StateMachine. It holds client session
// information, and a key-value store.
type Store struct {
	kvs         *iradix.Tree
	sessions    *iradix.Tree
	pendingCmds map[uint64]*Cmds

	snapTimer *time.Timer
	snapshot  unsafe.Pointer // *commonpb.Snapshot
}

// NewStore initializes and returns a *Store.
func NewStore() *Store {
	return &Store{
		kvs:         iradix.New(),
		sessions:    iradix.New(),
		pendingCmds: make(map[uint64]*Cmds),
		snapTimer:   time.NewTimer(SnapTick),
		snapshot:    unsafe.Pointer(&commonpb.Snapshot{}),
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

		res := s.applyStore(entry.Index, &cmd)

		select {
		case <-s.snapTimer.C:
			go s.takeSnapshot(
				entry.Term, entry.Index,
				s.kvs.Root().Iterator(),
				s.sessions.Root().Iterator(),
			)
		default:
		}

		return res
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

		nextSeq := oldSeq

		txn := s.kvs.Txn()

		if req.ClientSeq != nextSeq+1 {
			s.PushCmd(req.ClientID, &req)
		} else {
			txn.Insert([]byte(req.Key), req.Value)
			nextSeq++
		}

		for s.HasCmd(req.ClientID, nextSeq+1) {
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

// Snapshot implements raft.StateMachine.
func (s *Store) Snapshot() *commonpb.Snapshot {
	snapshot := (*commonpb.Snapshot)(atomic.LoadPointer(&s.snapshot))
	return snapshot
}

func (s *Store) takeSnapshot(term, index uint64, iterKvs, iterSessions *iradix.Iterator) {
	start := time.Now()
	defer func() {
		fmt.Println("Snapshot took:", time.Now().Sub(start))
	}()

	var kvs []*rkvpb.KeyValue
	k, v, more := iterKvs.Next()

	for more {
		kvs = append(kvs, &rkvpb.KeyValue{
			Key:   k,
			Value: v.(string),
		})
		k, v, more = iterKvs.Next()
	}

	var sessions []*rkvpb.Session
	k, v, more = iterSessions.Next()

	for more {
		sessions = append(sessions, &rkvpb.Session{
			ClientID:  k,
			ClientSeq: v.(uint64),
		})
		k, v, more = iterSessions.Next()
	}

	snap := &rkvpb.Snapshot{
		Kvs:      kvs,
		Sessions: sessions,
	}

	b, err := snap.Marshal()
	if err != nil {
		panic(fmt.Sprintf("could not marshal %v: %v", snap, err))
	}

	fmt.Println("Snapshot size:", len(b), "bytes")

	snapshot := &commonpb.Snapshot{
		Term:  term,
		Index: index,
		Data:  b,
	}

	atomic.StorePointer(&s.snapshot, unsafe.Pointer(snapshot))
	s.snapTimer = time.NewTimer(SnapTick)
}

// TODO Implement.
func (s *Store) Restore(snapshot *commonpb.Snapshot) {

}
