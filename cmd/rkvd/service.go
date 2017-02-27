package main

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	commonpb "github.com/relab/raft/raftpb"
	"github.com/relab/rkv/rkvpb"
)

// Service exposes the Store api as a http service.
type Service struct {
	store map[string]string

	raft raft.Raft
}

// NewService creates a new Service backed by store.
func NewService() *Service {
	return &Service{
		store: make(map[string]string),
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

		return s.applyStore(&cmd)
	case commonpb.EntryConfChange:
		panic("not implemented yet")
	default:
		panic(fmt.Sprintf("got unknown entry type: %v", entry.EntryType))
	}
}

func (s *Service) applyStore(cmd *rkvpb.Cmd) interface{} {
	switch cmd.CmdType {
	case rkvpb.Register:
		panic("not implemented yet")
	case rkvpb.Insert:
		var req rkvpb.InsertRequest
		err := req.Unmarshal(cmd.Data)
		if err != nil {
			panic(err)
		}

		s.store[req.Key] = req.Value
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
	return &rkvpb.RegisterResponse{ClientID: 5}, nil
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
	case <-future.Result():
		return &rkvpb.InsertResponse{Ok: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) Lookup(ctx context.Context, req *rkvpb.LookupRequest) (*rkvpb.LookupResponse, error) {
	return &rkvpb.LookupResponse{Value: s.store[req.Key]}, nil
}
