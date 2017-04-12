package raftgorums

import (
	"container/list"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
	pb "github.com/relab/raft/raftgorums/raftpb"
)

// RequestVote implements gorums.RaftServer.
func (r *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return r.HandleRequestVoteRequest(req), nil
}

// AppendEntries implements gorums.RaftServer.
func (r *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return r.HandleAppendEntriesRequest(req), nil
}

// InstallSnapshot implements gorums.RaftServer.
func (r *Raft) InstallSnapshot(ctx context.Context, snapshot *commonpb.Snapshot) (*pb.InstallSnapshotResponse, error) {
	return r.HandleInstallSnapshotRequest(snapshot), nil
}

// CatchMeUp implements gorums.RaftServer.
func (r *Raft) CatchMeUp(ctx context.Context, req *pb.CatchMeUpRequest) (res *pb.Empty, err error) {
	res = &pb.Empty{}
	r.match[r.mem.getNodeID(req.FollowerID)] <- req.NextIndex
	return
}

// HandleRequestVoteRequest must be called when receiving a RequestVoteRequest,
// the return value must be delivered to the requester.
func (r *Raft) HandleRequestVoteRequest(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.Lock()
	defer r.Unlock()
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.rvreq)
		defer timer.ObserveDuration()
	}

	var voteGranted bool
	defer func() {
		r.logger.WithFields(logrus.Fields{
			"currentterm": r.currentTerm,
			"requestterm": req.Term,
			"prevote":     req.PreVote,
			"candidateid": req.CandidateID,
			"votegranted": voteGranted,
		}).Infoln("Got vote request")
	}()

	// #RV1 Reply false if term < currentTerm.
	if req.Term < r.currentTerm {
		return &pb.RequestVoteResponse{Term: r.currentTerm}
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if req.Term > r.currentTerm && !req.PreVote {
		r.becomeFollower(req.Term)
	}

	voted := r.votedFor != None

	if req.PreVote && (r.heardFromLeader || (voted && req.Term == r.currentTerm)) {
		// We don't grant pre-votes if we have recently heard from a
		// leader or already voted in the pre-term.
		return &pb.RequestVoteResponse{Term: r.currentTerm}
	}

	lastIndex := r.storage.NextIndex() - 1
	lastLogTerm := r.logTerm(lastIndex)

	// We can grant a vote in the same term, as long as it's to the same
	// candidate. This is useful if the response was lost, and the candidate
	// sends another request.
	alreadyVotedForCandidate := r.votedFor == req.CandidateID

	// If the logs have last entries with different terms, the log with the
	// later term is more up-to-date.
	laterTerm := req.LastLogTerm > lastLogTerm

	// If the logs end with the same term, whichever log is longer is more
	// up-to-date.
	longEnough := req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastIndex

	// We can only grant a vote if: we have not voted yet, we vote for the
	// same candidate again, or this is a pre-vote.
	canGrantVote := !voted || alreadyVotedForCandidate || req.PreVote

	// #RV2 If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote.
	voteGranted = canGrantVote && (laterTerm || longEnough)

	if voteGranted {
		if req.PreVote {
			return &pb.RequestVoteResponse{VoteGranted: true, Term: req.Term}
		}

		r.votedFor = req.CandidateID
		r.storage.Set(KeyVotedFor, req.CandidateID)

		// #F2 If election timeout elapses without receiving
		// AppendEntries RPC from current leader or granting a vote to
		// candidate: convert to candidate. Here we are granting a vote
		// to a candidate so we reset the election timeout.
		r.resetElection = true
		r.resetBaseline = true

		return &pb.RequestVoteResponse{VoteGranted: true, Term: r.currentTerm}
	}

	// #RV2 The candidate's log was not up-to-date
	return &pb.RequestVoteResponse{Term: r.currentTerm}
}

// HandleAppendEntriesRequest must be called when receiving a
// AppendEntriesRequest, the return value must be delivered to the requester.
func (r *Raft) HandleAppendEntriesRequest(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.Lock()
	defer r.Unlock()
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.aereq)
		defer timer.ObserveDuration()
	}

	reqLogger := r.logger.WithFields(logrus.Fields{
		"currentterm":  r.currentTerm,
		"requestterm":  req.Term,
		"leaderid":     req.LeaderID,
		"prevlogindex": req.PrevLogIndex,
		"prevlogterm":  req.PrevLogTerm,
		"commitindex":  req.CommitIndex,
		"lenentries":   len(req.Entries),
	})
	reqLogger.Infoln("Got AppendEntries")

	// #AE1 Reply false if term < currentTerm.
	if req.Term < r.currentTerm {
		return &pb.AppendEntriesResponse{
			Success: false,
			Term:    r.currentTerm,
		}
	}

	logLen := r.storage.NextIndex() - 1
	prevTerm := r.logTerm(req.PrevLogIndex)

	// An AppendEntries request is always successful for the first index. A
	// leader can only be elected leader if its log matches that of a
	// majority and our log is guaranteed to be at least 0 in length.
	firstIndex := req.PrevLogIndex == 0

	// The index preceding the entries we are going to replicate must be in our log.
	gotPrevIndex := req.PrevLogIndex <= logLen
	// The term must match to satisfy the log matching property.
	sameTerm := req.PrevLogTerm == prevTerm

	// If the previous entry is in our log, then our log matches the leaders
	// up till and including the previous entry. And we can safely replicate
	// next new entries.
	gotPrevEntry := gotPrevIndex && sameTerm

	success := firstIndex || gotPrevEntry

	// #A2 If RPC request or response contains term T > currentTerm: set
	// currentTerm = T, convert to follower.
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term)
	} else if r.id != req.LeaderID {
		r.becomeFollower(r.currentTerm)
	}

	if r.metricsEnabled {
		rmetrics.leader.Set(float64(req.LeaderID))
	}

	// We acknowledge this server as the leader as it's has the highest term
	// we have seen, and there can only be one leader per term.
	r.leader = req.LeaderID
	r.heardFromLeader = true
	r.seenLeader = true

	if !success {
		// TODO This is potentially wrong, we cannot send match index
		// for entries that are not committed? Also use logLen...
		r.cureqout <- &catchUpReq{
			leaderID:   req.LeaderID,
			matchIndex: r.storage.NextIndex() - 1,
		}

		return &pb.AppendEntriesResponse{
			Term:       req.Term,
			MatchIndex: r.storage.NextIndex() - 1,
		}
	}

	// If we already know that the entries we are receiving are committed in
	// our log, we can return early.
	if req.CommitIndex < r.commitIndex {
		return &pb.AppendEntriesResponse{
			Term:       req.Term,
			MatchIndex: r.storage.NextIndex() - 1,
			Success:    success,
		}
	}

	var toSave []*commonpb.Entry
	index := req.PrevLogIndex

	for _, entry := range req.Entries {
		// Increment first so we start at previous index + 1.
		index++

		// If the terms don't match, our logs conflict at this index. On
		// the first conflict this will truncate the log to the lowest
		// common matching index. After that it will fill the log with
		// the new entries from the leader. This is because entry.Term
		// will always conflict with term 0, which will be returned for
		// indexes outside our log.
		if entry.Term != r.logTerm(index) {
			logLen = r.storage.NextIndex() - 1
			for logLen > index-1 {
				r.storage.RemoveEntries(logLen, logLen)
				logLen = r.storage.NextIndex() - 1
			}
			toSave = append(toSave, entry)
		}
	}

	if len(toSave) > 0 {
		r.storage.StoreEntries(toSave)
	}
	logLen = r.storage.NextIndex() - 1

	old := r.commitIndex
	// Commit index can not exceed the length of our log.
	r.commitIndex = min(req.CommitIndex, logLen)

	if r.metricsEnabled {
		rmetrics.commitIndex.Set(float64(r.commitIndex))
	}

	if r.commitIndex > old {
		r.logger.WithFields(logrus.Fields{
			"oldcommitindex": old,
			"commitindex":    r.commitIndex,
		}).Infoln("Set commit index")

		r.newCommit(old)
	}

	reqLogger.WithFields(logrus.Fields{
		"lensaved":   len(toSave),
		"lenlog":     logLen,
		"matchindex": index,
		"success":    success,
	}).Infoln("Saved entries to stable storage")

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		MatchIndex: index,
		Success:    success,
	}
}

func (r *Raft) HandleInstallSnapshotRequest(snapshot *commonpb.Snapshot) (res *pb.InstallSnapshotResponse) {
	r.Lock()
	defer r.Unlock()

	res = &pb.InstallSnapshotResponse{
		Term: r.currentTerm,
	}

	return
}

// HandleRequestVoteResponse must be invoked when receiving a
// RequestVoteResponse.
func (r *Raft) HandleRequestVoteResponse(response *pb.RequestVoteResponse) {
	r.Lock()
	defer r.Unlock()
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.rvres)
		defer timer.ObserveDuration()
	}

	term := r.currentTerm

	if r.preElection {
		term++
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	if response.Term > term {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < term {
		return
	}

	// Cont. from startElection(). We have now received a response from Gorums.

	// #C5 If votes received from majority of server: become leader.
	// Make sure we have not stepped down while waiting for replies.
	if r.state == Candidate && response.VoteGranted {
		if r.preElection {
			r.preElection = false
			r.startElectionNow <- struct{}{}
			return
		}

		// We have received at least a quorum of votes.
		// We are the leader for this term. See Raft Paper Figure 2 -> Rules for Servers -> Leaders.

		if r.metricsEnabled {
			rmetrics.leader.Set(float64(r.id))
		}

		r.logger.WithFields(logrus.Fields{
			"currentterm": r.currentTerm,
		}).Infoln("Elected leader")

		logLen := r.storage.NextIndex() - 1

		r.state = Leader
		r.leader = r.id
		r.seenLeader = true
		r.heardFromLeader = true
		r.nextIndex = logLen + 1
		r.pending = list.New()
		r.pendingReads = nil

		// Empty queue.
	EMPTYCH:
		for {
			select {
			case <-r.queue:
			default:
				// Paper §8: We add a no-op, so that the leader
				// commits an entry from its own term. This
				// ensures that the leader knows which entries
				// are committed.
				r.queue <- raft.NewFuture(&commonpb.Entry{
					EntryType: commonpb.EntryInternal,
					Term:      r.currentTerm,
					Data:      raft.NOOP,
				})
				break EMPTYCH
			}
		}

		// TODO r.sendAppendEntries()?

		return
	}

	r.preElection = true

	// #C7 If election timeout elapses: start new election.
	// This will happened if we don't receive enough replies in time. Or we lose the election but don't see a higher term number.
}

// HandleAppendEntriesResponse must be invoked when receiving an
// AppendEntriesResponse.
func (r *Raft) HandleAppendEntriesResponse(response *pb.AppendEntriesQFResponse, replies uint64) {
	r.Lock()
	defer func() {
		r.Unlock()
		r.advanceCommitIndex()
	}()
	if r.metricsEnabled {
		timer := metrics.NewTimer(rmetrics.aeres)
		defer timer.ObserveDuration()
	}

	// #A2 If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.
	// If we didn't get a response from a majority (excluding self) step down.
	if response.Term > r.currentTerm || replies < uint64(len(r.addrs)/2) {
		r.becomeFollower(response.Term)

		return
	}

	// Ignore late response
	if response.Term < r.currentTerm {
		return
	}

	if r.state == Leader {
		if response.Success {
			// Successful heartbeat to a majority.
			r.resetElection = true

			r.matchIndex = response.MatchIndex
			r.nextIndex = r.matchIndex + 1

			return
		}

		// If AppendEntries was not successful lower match index.
		r.nextIndex = max(1, response.MatchIndex)
	}
}

func (r *Raft) HandleInstallSnapshotResponse(res *pb.InstallSnapshotResponse) bool {
	r.Lock()
	defer r.Unlock()

	if res.Term > r.currentTerm {
		r.becomeFollower(res.Term)

		return false
	}

	return true
}