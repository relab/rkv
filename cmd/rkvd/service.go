package main

import (
	"container/heap"
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

type Session struct {
	seq     uint64
	pending *Cmds
}

func (s *Session) Push(req *rkvpb.InsertRequest) {
	heap.Push(s.pending, req)
}

func (s *Session) HasNext() bool {
	if len(*s.pending) == 0 {
		return false
	}

	if (*s.pending)[0].ClientSeq != s.seq+1 {
		return false
	}

	return true
}

func (s *Session) Pop() *rkvpb.InsertRequest {
	s.seq++
	return heap.Pop(s.pending).(*rkvpb.InsertRequest)
}

// Service exposes a key-value store as a gRPC service.
type Service struct {
	store    map[string]string
	sessions map[uint64]*Session

	raft raft.Raft
}

// NewService initializes and returns a new Service.
func NewService() *Service {
	return &Service{
		store:    make(map[string]string),
		sessions: make(map[uint64]*Session),
	}
}

func (s *Service) SetRaft(raft raft.Raft) {
	s.raft = raft
}

func (s *Service) Apply(entry *commonpb.Entry) interface{} {
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

func (s *Service) applyStore(i uint64, cmd *rkvpb.Cmd) interface{} {
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

func (s *Service) Register(ctx context.Context, req *rkvpb.RegisterRequest) (*rkvpb.RegisterResponse, error) {
	cmd := &rkvpb.Cmd{
		CmdType: rkvpb.Register,
	}

	b, err := cmd.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := s.raft.ProposeCmd(ctx, b)

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.Result():
		return &rkvpb.RegisterResponse{ClientID: res.(uint64)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) Insert(ctx context.Context, req *rkvpb.InsertRequest) (*rkvpb.InsertResponse, error) {
	reqb, err := req.Marshal()

	if err != nil {
		return nil, err
	}

	cmd := &rkvpb.Cmd{
		CmdType: rkvpb.Insert,
		Data:    reqb,
	}

	b, err := cmd.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := s.raft.ProposeCmd(ctx, b)

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.Result():
		err, ok := res.(error)

		if ok {
			return nil, err
		}

		return &rkvpb.InsertResponse{Ok: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) Lookup(ctx context.Context, req *rkvpb.LookupRequest) (*rkvpb.LookupResponse, error) {
	reqb, err := req.Marshal()

	if err != nil {
		return nil, err
	}

	cmd := &rkvpb.Cmd{
		CmdType: rkvpb.Lookup,
		Data:    reqb,
	}

	b, err := cmd.Marshal()

	if err != nil {
		return nil, err
	}

	future, err := s.raft.ProposeCmd(ctx, b)

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.Result():
		return &rkvpb.LookupResponse{Value: res.(string)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

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

func (c *Cmds) Pop() interface{} {
	n := len(*c)
	x := (*c)[n-1]
	*c = (*c)[:n-1]
	return x
}

func (c *Cmds) Push(x interface{}) {
	*c = append(*c, x.(*rkvpb.InsertRequest))
}
