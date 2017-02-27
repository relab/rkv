package main

import (
	"golang.org/x/net/context"

	"github.com/relab/raft"
	"github.com/relab/rkv/rkvpb"
)

// Service exposes a key-value store as a gRPC service.
type Service struct {
	raft raft.Raft
}

// NewService initializes and returns a new Service.
func NewService(raft raft.Raft) *Service {
	return &Service{
		raft: raft,
	}
}

// Register implements RKVServer.
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

// Insert implements RKVServer.
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

// Lookup implements RKVServer.
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

	future, err := s.raft.ReadCmd(ctx, b)

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
