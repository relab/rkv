package main

import (
	"golang.org/x/net/context"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
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
	case res := <-future.ResultCh():
		return &rkvpb.RegisterResponse{ClientID: res.Value.(uint64)}, nil
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
	case <-future.ResultCh():
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
	case res := <-future.ResultCh():
		return &rkvpb.LookupResponse{Value: res.Value.(string)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Reconf implements RKVServer.
func (s *Service) Reconf(ctx context.Context, req *commonpb.ReconfRequest) (*commonpb.ReconfResponse, error) {
	future, err := s.raft.ProposeConf(ctx, req)

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		return res.Value.(*commonpb.ReconfResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
