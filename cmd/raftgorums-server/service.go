package main

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
	key := r.RequestURI

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
		}

		if ok := s.store.Insert(key, string(value)); !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			break
		}

		w.WriteHeader(http.StatusAccepted)
	}
}
