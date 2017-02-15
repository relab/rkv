package rkv

import (
	"fmt"
	"io/ioutil"
	"net/http"
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
		value, ok := s.store.Lookup(key)

		if !ok {
			http.NotFound(w, r)
			break
		}

		fmt.Fprintf(w, "%s", value)

	case http.MethodPut:
		value, err := ioutil.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			break
		}

		if ok := s.store.Insert(key, string(value)); !ok {
			http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
			break
		}

		w.WriteHeader(http.StatusAccepted)
	}
}
