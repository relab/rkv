package main

import (
	"container/heap"

	"github.com/relab/rkv/rkvpb"
)

// Session represents a client session. It holds the next sequence number that
// can be applied, and all pending commands in a priority queue.
type Session struct {
	seq     uint64
	pending *Cmds
}

// Push command onto queue.
func (s *Session) Push(req *rkvpb.InsertRequest) {
	heap.Push(s.pending, req)
}

// HasNext returns true if the next command in the queue is the next entry in
// the sequence.
func (s *Session) HasNext() bool {
	if len(*s.pending) == 0 {
		return false
	}

	if (*s.pending)[0].ClientSeq != s.seq+1 {
		return false
	}

	return true
}

// Pop removes the next command from the queue and returns it. Only call Pop
// after a successful call to HasNext.
func (s *Session) Pop() *rkvpb.InsertRequest {
	s.seq++
	return heap.Pop(s.pending).(*rkvpb.InsertRequest)
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
