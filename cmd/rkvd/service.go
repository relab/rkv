package main

import (
	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	"github.com/relab/rkv/rkvpb"
)

// Service exposes a key-value store as a gRPC service.
type Service struct {
	raft    raft.Raft
	sem     chan struct{} // Forces requests to be handled in order. len(sema) = concurrent requests.
	leader  chan struct{}
	replace chan struct{}
	logger  logrus.FieldLogger
}

// NewService initializes and returns a new Service.
func NewService(logger logrus.FieldLogger, raft raft.Raft, leader chan struct{}) *Service {
	return &Service{
		raft:    raft,
		sem:     make(chan struct{}, 50000),
		leader:  leader,
		replace: make(chan struct{}, 1),
		logger:  logger,
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

	s.sem <- struct{}{}
	future, err := s.raft.ProposeCmd(ctx, b)
	<-s.sem

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		if err, ok := res.Value.(error); ok {
			return nil, err
		}
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

	s.sem <- struct{}{}
	future, err := s.raft.ProposeCmd(ctx, b)
	<-s.sem

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		if err, ok := res.Value.(error); ok {
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

	s.sem <- struct{}{}
	future, err := s.raft.ReadCmd(ctx, b)
	<-s.sem

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		if err, ok := res.Value.(error); ok {
			return nil, err
		}
		return &rkvpb.LookupResponse{Value: res.Value.(string)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Reconf implements RKVServer.
func (s *Service) Reconf(ctx context.Context, req *commonpb.ReconfRequest) (*commonpb.ReconfResponse, error) {
	s.sem <- struct{}{}
	future, err := s.raft.ProposeConf(ctx, req)
	<-s.sem

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		if err, ok := res.Value.(error); ok {
			return nil, err
		}
		return res.Value.(*commonpb.ReconfResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReconfOnBecome implements RKVServer.
func (s *Service) ReconfOnBecome(ctx context.Context, req *commonpb.ReconfRequest) (*commonpb.ReconfResponse, error) {
	s.sem <- struct{}{}
	s.logger.Warnln("Waiting on becoming leader")
	select {
	case <-s.leader:
		s.logger.Warnln("Became leader, proceeding...")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	future, err := s.raft.ProposeConf(ctx, req)
	<-s.sem

	if err != nil {
		return nil, err
	}

	select {
	case res := <-future.ResultCh():
		if err, ok := res.Value.(error); ok {
			return nil, err
		}
		go func() {
			select {
			case s.replace <- struct{}{}:
			default:
				return
			}
			req.ReconfType = commonpb.ReconfRemove
			req.ServerID = 1
			s.Reconf(context.Background(), req)
			<-s.replace
		}()
		return res.Value.(*commonpb.ReconfResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
