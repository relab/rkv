package rkv

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"

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
	path := strings.Split(r.URL.Path, "/")

	if len(path) < 2 {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		return
	}

	switch path[1] {
	case "register":
		id, err := s.store.Register()

		if err != nil {
			raftError(w, r, err)
			return
		}

		fmt.Fprintln(w, id)
		return
	case "store":
		if len(path) != 3 {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		key := path[2]

		if len(key) < 1 {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		s.handleStore(w, r, key)

	default:
		http.NotFound(w, r)
		return
	}
}

func (s *Service) handleStore(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		value, err := s.store.Lookup(key, false)

		if err != nil {
			raftError(w, r, err)
			return
		}

		fmt.Fprintln(w, value)

	case http.MethodPut:
		query := r.URL.Query()
		idq := query["id"]
		seqq := query["seq"]

		if len(idq) != 1 || len(seqq) != 1 {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		id := idq[0]
		seq, err := strconv.ParseUint(seqq[0], 10, 64)

		if err != nil {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		value, err := ioutil.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}

		err = s.store.Insert(key, string(value), id, seq)

		if err != nil {
			raftError(w, r, err)
			return
		}

		// TODO Change to StatusOK when we actually verify commitment.
		w.WriteHeader(http.StatusAccepted)
	}
}

func raftError(w http.ResponseWriter, r *http.Request, err error) {
	switch err := err.(type) {
	case raft.ErrNotLeader:
		if err.LeaderAddr == "" {
			// TODO Document that this means the client should
			// change to a random server.
			w.Header().Set("Retry-After", "-1")
			http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
		}

		host, port, erri := net.SplitHostPort(err.LeaderAddr)

		if erri != nil {
			http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
		}

		if host == "" {
			host = "localhost"
		}

		// TODO Document that this service always uses Raft port - 100.
		p, erri := strconv.Atoi(port)

		if erri != nil {
			http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
		}

		port = strconv.Itoa(p - 100)
		addr := net.JoinHostPort(host, port)

		http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	default:
		if err == ErrSessionExpired {
			// TODO Document that this means the session didn't
			// exist or expired.
			http.Error(w, "410 Gone", http.StatusGone)
			return
		}

		// TODO Document that this means the client should retry with
		// the same server in 1s. We could probably do exponential
		// back-off here.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
	}
}
