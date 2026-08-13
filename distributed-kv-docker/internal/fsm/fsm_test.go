package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"

	"distributed-kv/internal/store"
)

func TestApplySet(t *testing.T) {
	s := store.New()
	f := New(s)

	cmd := Command{Type: CommandSet, Key: "username", Value: "John"}
	data, _ := json.Marshal(cmd)

	result := f.Apply(&raft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("unexpected error applying set: %v", err)
	}

	val, err := s.Get("username")
	if err != nil || val != "John" {
		t.Fatalf("expected 'John', got %q, err %v", val, err)
	}
}

func TestApplyDelete(t *testing.T) {
	s := store.New()
	s.Set("username", "John")
	f := New(s)

	cmd := Command{Type: CommandDelete, Key: "username"}
	data, _ := json.Marshal(cmd)

	f.Apply(&raft.Log{Data: data})

	_, err := s.Get("username")
	if err != store.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestApplyUnknownCommand(t *testing.T) {
	s := store.New()
	f := New(s)

	cmd := Command{Type: "bogus", Key: "x"}
	data, _ := json.Marshal(cmd)

	result := f.Apply(&raft.Log{Data: data})
	if _, ok := result.(error); !ok {
		t.Fatalf("expected an error for unknown command type, got %v", result)
	}
}

// TestSnapshotRestoreRoundTrip verifies that taking a snapshot and restoring
// it into a fresh FSM reproduces the same data -- this is the mechanism
// nodes use to catch up quickly instead of replaying the whole log.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := store.New()
	s.Set("a", "1")
	s.Set("b", "2")
	f := New(s)

	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatalf("unexpected error taking snapshot: %v", err)
	}

	sink := newFakeSnapshotSink()
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("unexpected error persisting snapshot: %v", err)
	}

	freshStore := store.New()
	freshFSM := New(freshStore)
	if err := freshFSM.Restore(sink.reader()); err != nil {
		t.Fatalf("unexpected error restoring snapshot: %v", err)
	}

	val, err := freshStore.Get("a")
	if err != nil || val != "1" {
		t.Fatalf("expected restored key 'a' to be '1', got %q, err %v", val, err)
	}
}
