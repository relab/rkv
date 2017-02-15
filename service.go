package rkv

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"

	"github.com/relab/raft"
)

// Service exposes the Store api as a http service.
type Service struct {
	store *Store
}

// NewService creates a new Service backed by store.
func NewService(store *Store) *Service {
	return &Service{
		store: store,
	}
}

// ServeHTTP implements the http.Handler interface.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]

	if len(key) < 1 {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := s.store.Lookup(key)

		if err != nil {
			raftError(w, r, err)
			return
		}

		fmt.Fprintf(w, "%s", value)

	case http.MethodPut:
		value, err := ioutil.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		err = s.store.Insert(key, string(value))

		if err != nil {
			raftError(w, r, err)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func raftError(w http.ResponseWriter, r *http.Request, err error) {
	switch err := err.(type) {
	case raft.ErrNotLeader:
		// TODO Assumes a valid addr is returned.
		host, port, _ := net.SplitHostPort(err.LeaderAddr)

		if host == "" {
			host = "localhost"
		}

		// TODO Hack. Since LeaderAddr is the Raft port, we just assume
		// the application is using Raft port - 100. Fix means changing
		// Raft to put the application port into LeaderAddr, however we
		// don't have a way of knowing the application ports, as they
		// are set locally.
		p, _ := strconv.Atoi(port)
		port = strconv.Itoa(p - 100)

		addr := net.JoinHostPort(host, port)

		http.Redirect(w, r, "http://"+addr+r.URL.Path, http.StatusTemporaryRedirect)
	default:
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
	}
}
