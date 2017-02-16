package rkv

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/relab/raft"
	commonpb "github.com/relab/raft/raftpb"
)

// Command types.
const (
	CmdRegister = "REG"
	CmdInsert   = "INS"
)

// Store is a key-value store backed by a map.
type Store struct {
	store map[string]string

	nextID  uint64
	clients map[string]uint64

	waiting map[string]chan interface{}

	raft raft.Raft
}

// NewStore returns a newly initialized store.
func NewStore(raft raft.Raft) *Store {
	s := &Store{
		store:   make(map[string]string),
		clients: make(map[string]uint64),
		waiting: make(map[string]chan interface{}),
		raft:    raft,
	}

	go s.run()

	return s
}

func (s *Store) run() {
	for {
		select {
		case entries := <-s.raft.Committed():
			for _, entry := range entries {
				switch entry.EntryType {
				case commonpb.EntryNormal:
					cmd := string(entry.Data)
					split := strings.Split(cmd, ":")
					cmdType := split[0]
					var res interface{}

					switch cmdType {
					case CmdRegister:
						id := fmt.Sprintf("%d", entry.Index)
						res = id
						if _, ok := s.clients[id]; ok {
							// Don't reset sequence
							// number if we receive
							// a duplicate.
							break
						}
						s.clients[id] = 0
					case CmdInsert:
						split = strings.Split(split[1], ",")

						id := split[0]
						seq, _ := strconv.Atoi(split[1])

						// TODO We need to return
						// ErrSessionExpired, when the
						// session is expired. TODO If
						// we see seq > oldseq + 1, we
						// need to deal with that, i.e,
						// we might have stale data?
						if oldseq := s.clients[id]; oldseq >= uint64(seq) {
							// Already applied.
							break
						}

						s.clients[id] = uint64(seq)

						key := split[2]
						value := split[3]

						s.store[key] = value
					}

					if ch, ok := s.waiting[cmd]; ok {
						ch <- res
						delete(s.waiting, cmd)
					}
				}
			}
		}
	}
}

func (s *Store) waitOnResponse(ctx context.Context, cmd string) (interface{}, error) {
	done := make(chan interface{}, 1)
	s.waiting[cmd] = done

	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Store) Register() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Random int used to uniquely identify command. There is a chance of a
	// collision, but I'll take those odds any day.
	cmd := fmt.Sprintf("%s:%d", CmdRegister, rand.Int63())

	if err := s.raft.ProposeCmd(ctx, []byte(cmd)); err != nil {
		return "", err
	}

	// There is a race condition on the command being committed before we
	// actually start waiting. If that happens, the wait will timeout, and
	// the client will retry.
	res, err := s.waitOnResponse(ctx, cmd)

	if err != nil {
		return "", err
	}

	return res.(string), nil
}

// Lookup gets a value from the map given a key. It returns a bool indicating if
// the value was found or not. TODO We need to query the state machine. It's not
// safe to just read the value.
func (s *Store) Lookup(key string) (string, error) {
	// TODO Buffer reads to amortize cost.
	err := s.raft.Read(context.TODO())

	if err != nil {
		return "", err
	}

	// Empty string if not set. This depends on the application I think. We
	// don't really care about the key being set or not.
	value, _ := s.store[key]

	return value, nil
}

// Insert inserts value in map given a key. It returns true if the value was
// successfully inserted. False indicates that the client should retry, as we
// cannot know if the value was inserted successfully or not.
func (s *Store) Insert(id, seq, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// id and seq uniquely identifies this command.
	cmd := fmt.Sprintf("%s:%s,%s,%s,%s", CmdInsert, id, seq, key, value)

	if err := s.raft.ProposeCmd(ctx, []byte(cmd)); err != nil {
		return err
	}

	// There is a race condition on the command being committed before we
	// actually start waiting. If that happens, the wait will timeout, and
	// the client will retry.
	if _, err := s.waitOnResponse(ctx, cmd); err != nil {
		return err
	}

	return nil
}
