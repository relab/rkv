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

		switch r.Method {
		case http.MethodGet:
			value, err := s.store.Lookup(key, false)

			if err != nil {
				raftError(w, r, err)
				return
			}

			fmt.Fprintln(w, value)

		case http.MethodPut:
			value, err := ioutil.ReadAll(r.Body)

			if err != nil {
				http.Error(w, "400 Bad Request", http.StatusBadRequest)
				return
			}

			query := r.URL.Query()
			// TODO Bound check.
			id := query["id"][0]
			seq := query["seq"][0]
			sequ, err := strconv.ParseUint(seq, 10, 64)

			if err != nil {
				http.Error(w, "400 Bad Request", http.StatusBadRequest)
				return
			}

			err = s.store.Insert(id, sequ, key, string(value))

			if err != nil {
				raftError(w, r, err)
				return
			}

			// TODO Change to StatusOK when we actually verify commitment.
			w.WriteHeader(http.StatusAccepted)
		}
	default:
		http.NotFound(w, r)
		return
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

		// TODO Hack. Since LeaderAddr is the Raft port, we just assume
		// the application is using Raft port - 100. Fix means changing
		// Raft to put the application port into LeaderAddr, however we
		// don't have a way of knowing the application ports, as they
		// are set locally.
		p, _ := strconv.Atoi(port)
		port = strconv.Itoa(p - 100)

		addr := net.JoinHostPort(host, port)

		http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	default:
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
	}
}
