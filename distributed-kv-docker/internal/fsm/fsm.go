// Package fsm implements the glue between Raft and our key-value store.
//
// Raft (the hashicorp/raft library) knows nothing about "keys" or "values" --
// it only knows how to safely replicate an ordered log of opaque byte commands
// across servers. It expects whoever's using it to provide an "FSM"
// (Finite State Machine): something that knows how to take a committed log
// entry and actually apply it to real data.
//
// This file is that bridge. It does NOT reimplement storage -- it wraps the
// existing store.Store and translates Raft log entries into calls like
// store.Set(key, value).
package fsm

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"distributed-kv/internal/store"
)

// CommandType distinguishes the kinds of operations we can replicate.
type CommandType string

const (
	CommandSet    CommandType = "set"
	CommandDelete CommandType = "delete"
)

// Command is what gets serialized into the Raft log. Every write to the
// cluster becomes one of these, encoded as JSON bytes, before Raft ever
// sees it. Raft replicates these bytes; it has no idea what's inside them.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

// FSM implements raft.FSM by wrapping our existing store.Store.
type FSM struct {
	store *store.Store
}

// New creates an FSM backed by the given store.
func New(s *store.Store) *FSM {
	return &FSM{store: s}
}

// Apply is called by Raft once a log entry has been committed by a majority
// of the cluster. This is the ONLY place actual writes should happen --
// nothing else should call store.Set/Delete directly, or nodes could drift
// out of sync with each other.
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal raft log entry: %w", err)
	}

	switch cmd.Type {
	case CommandSet:
		f.store.Set(cmd.Key, cmd.Value)
		return nil
	case CommandDelete:
		f.store.Delete(cmd.Key)
		return nil
	default:
		return fmt.Errorf("unknown command type: %q", cmd.Type)
	}
}

// Snapshot captures the current state of the store so Raft can compact its
// log (instead of keeping every write ever made, forever) and so restarted
// nodes can catch up quickly instead of replaying the entire history.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// We hold no lock here beyond what store.Snapshot() itself does --
	// see store.Snapshot() for how a consistent copy is taken.
	data := f.store.Snapshot()
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the store's entire contents with what's in the snapshot.
// Called when a node starts up and needs to catch up to the cluster's
// current state without replaying every historical log entry.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string]string
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.store.Restore(data)
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot -- the interface Raft uses to
// actually persist the snapshot bytes to disk.
type fsmSnapshot struct {
	data map[string]string
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		encoder := json.NewEncoder(sink)
		if err := encoder.Encode(s.data); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s *fsmSnapshot) Release() {}
