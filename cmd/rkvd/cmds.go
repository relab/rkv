package main

import (
	"container/heap"

	"github.com/relab/rkv/rkvpb"
)

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
