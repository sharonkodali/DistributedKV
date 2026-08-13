// Package store implements a simple, thread-safe in-memory key-value store.
//
// This is the "database" part of the project. It knows nothing about
// networking, replication, or Raft -- it just holds data in memory and
// makes sure concurrent reads/writes don't corrupt each other.
//
// Keeping this isolated matters: later, when we add Raft, the Raft layer
// will call into this store's Apply-style methods as its "state machine."
// Separating storage from consensus is a real architectural pattern used
// in etcd, CockroachDB, and similar systems.
package store

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when a Get is called on a key that doesn't exist.
var ErrKeyNotFound = errors.New("key not found")

// Store is a thread-safe in-memory key-value store.
//
// sync.RWMutex allows multiple concurrent readers OR one exclusive writer,
// which is important once many client requests hit the server at the same
// time -- without it, two goroutines writing to the map at once would
// crash the program (Go's map type is not safe for concurrent writes).
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates an empty, ready-to-use Store.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get returns the value for a key, or ErrKeyNotFound if it doesn't exist.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return val, nil
}

// Set stores a value for a key, overwriting any existing value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete removes a key. It is not an error to delete a key that doesn't exist.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}

// Len returns the number of keys currently stored. Useful for status/debug endpoints.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

// Snapshot returns a copy of all current data. Used by the Raft layer to
// persist the store's state to disk for fast recovery. Returns a copy
// (not the live map) so the snapshot can't be mutated by concurrent writes
// while it's being written out.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copyData := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copyData[k] = v
	}
	return copyData
}

// Restore replaces the store's entire contents with the given data. Used
// by the Raft layer when a node needs to catch up from a snapshot instead
// of replaying every historical log entry.
func (s *Store) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
}
