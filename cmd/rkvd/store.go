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
	"github.com/relab/raft/commonpb"
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

	snapTimer  *time.Timer
	snapshot   unsafe.Pointer // *commonpb.Snapshot
	snapshotCh chan *commonpb.Snapshot
}

// NewStore initializes and returns a *Store.
func NewStore() *Store {
	return &Store{
		kvs:         iradix.New(),
		sessions:    iradix.New(),
		pendingCmds: make(map[uint64]*Cmds),
		snapTimer:   time.NewTimer(SnapTick),
		snapshot:    unsafe.Pointer(&commonpb.Snapshot{}),
		snapshotCh:  make(chan *commonpb.Snapshot),
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

		res, mutated := s.applyStore(entry.Index, &cmd)

		// We can only snapshot if the state was mutated, i.e., on write
		// requests. This is because we currently don't set entry.Index
		// for read requests.
		if mutated {
			s.maybeSnapshot(entry)
		}

		return res
	case commonpb.EntryConfChange:
		// TODO s.snapshotMaybe(entry)?
		panic("not implemented yet")
	default:
		panic(fmt.Sprintf("got unknown entry type: %v", entry.EntryType))
	}
}

func (s *Store) maybeSnapshot(entry *commonpb.Entry) {
	select {
	case <-s.snapTimer.C:
		go s.takeSnapshot(
			atomic.LoadPointer(&s.snapshot),
			entry.Term, entry.Index,
			s.kvs.Root().Iterator(),
			s.sessions.Root().Iterator(),
		)
	default:
	}
}

func (s *Store) applyStore(i uint64, cmd *rkvpb.Cmd) (interface{}, bool) {
	switch cmd.CmdType {
	case rkvpb.Register:
		id := []byte(strconv.FormatUint(i, 10))
		s.sessions, _, _ = s.sessions.Insert(id, uint64(0))

		var pending Cmds
		heap.Init(&pending)
		s.pendingCmds[i] = &pending

		return i, true
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

		switch {
		// Request is next to be applied.
		case req.ClientSeq == nextSeq+1:
			txn.Insert([]byte(req.Key), req.Value)
			nextSeq++

		// Already applied, ignore.
		case req.ClientSeq < nextSeq+1:
			// We could return here, but we would need to commit the
			// txn anyways. And we should return true, even though
			// the state machine was not actually mutated, the
			// requests index is set.

		// Future request, queue it.
		default:
			s.PushCmd(req.ClientID, &req)
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

		return true, true
	case rkvpb.Lookup:
		var req rkvpb.LookupRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		raw, found := s.kvs.Get([]byte(req.Key))

		if !found {
			return "(none)", false
		}

		val, ok := raw.(string)

		if !ok {
			panic(fmt.Sprintf("expected string got %s", reflect.TypeOf(raw)))
		}

		return val, false
	default:
		panic(fmt.Sprintf("got unknown cmd type: %v", cmd.CmdType))
	}
}

// Snapshot implements raft.StateMachine.
func (s *Store) Snapshot() <-chan *commonpb.Snapshot {
	return s.snapshotCh
}

func (s *Store) takeSnapshot(old unsafe.Pointer, term, index uint64, iterKvs, iterSessions *iradix.Iterator) {
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
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              b,
	}

	// This is supposed to prevent a snapshot in progress from overwriting a
	// call to Restore.
	ok := atomic.CompareAndSwapPointer(&s.snapshot, old, unsafe.Pointer(snapshot))

	if ok {
		s.snapshotCh <- snapshot
	}

	s.snapTimer = time.NewTimer(SnapTick)
}

// Restore implements raft.StateMachine.
func (s *Store) Restore(snapshot *commonpb.Snapshot) {
	// Replace current snapshot.
	atomic.StorePointer(&s.snapshot, unsafe.Pointer(snapshot))

	s.pendingCmds = make(map[uint64]*Cmds)

	// Return if we received the empty snapshot.
	if snapshot.Data == nil {
		return
	}

	var snap rkvpb.Snapshot
	err := snap.Unmarshal(snapshot.Data)
	if err != nil {
		panic(fmt.Sprintf("could not unmarshal %v: %v", snapshot.Data, err))
	}

	txn := s.kvs.Txn()
	for _, kv := range snap.Kvs {
		txn.Insert(kv.Key, kv.Value)
	}
	s.kvs = txn.CommitOnly()

	txn = s.sessions.Txn()
	for _, session := range snap.Sessions {
		txn.Insert(session.ClientID, session.ClientSeq)
		var pending Cmds
		heap.Init(&pending)
		id, _ := strconv.Atoi(string(session.ClientID))
		s.pendingCmds[uint64(id)] = &pending
	}
	s.sessions = txn.CommitOnly()
}
