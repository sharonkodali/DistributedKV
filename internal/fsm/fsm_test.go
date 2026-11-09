package fsm

import (
	"encoding/json"
	"io"
	"strings"
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

func applyCmd(t *testing.T, f *FSM, cmd Command) interface{} {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return f.Apply(&raft.Log{Data: data})
}

// A corrupted log entry must produce an error, not a panic or a silent no-op.
func TestApplyMalformedJSON(t *testing.T) {
	s := store.New()
	f := New(s)

	result := f.Apply(&raft.Log{Data: []byte("{not json")})
	if err, ok := result.(error); !ok || err == nil {
		t.Fatalf("expected an error for malformed log data, got %v", result)
	}
	if s.Len() != 0 {
		t.Fatalf("malformed entry must not change the store, got %d keys", s.Len())
	}
}

func TestApplySetOverwrites(t *testing.T) {
	s := store.New()
	f := New(s)

	applyCmd(t, f, Command{Type: CommandSet, Key: "k", Value: "first"})
	applyCmd(t, f, Command{Type: CommandSet, Key: "k", Value: "second"})

	if val, _ := s.Get("k"); val != "second" {
		t.Fatalf("expected 'second', got %q", val)
	}
}

// Value has `omitempty`, so an empty value is absent from the JSON entirely.
// It must still round-trip as "" rather than failing.
func TestApplySetEmptyValue(t *testing.T) {
	s := store.New()
	f := New(s)

	if res := applyCmd(t, f, Command{Type: CommandSet, Key: "k", Value: ""}); res != nil {
		t.Fatalf("unexpected result: %v", res)
	}
	val, err := s.Get("k")
	if err != nil || val != "" {
		t.Fatalf("expected empty value, got %q, %v", val, err)
	}
}

func TestApplyDeleteMissingKey(t *testing.T) {
	f := New(store.New())
	if res := applyCmd(t, f, Command{Type: CommandDelete, Key: "nope"}); res != nil {
		t.Fatalf("deleting a missing key should succeed, got %v", res)
	}
}

// Replaying the same sequence of log entries on two fresh FSMs must give
// identical state -- this determinism is what keeps replicas in sync.
func TestApplyIsDeterministic(t *testing.T) {
	cmds := []Command{
		{Type: CommandSet, Key: "a", Value: "1"},
		{Type: CommandSet, Key: "b", Value: "2"},
		{Type: CommandDelete, Key: "a"},
		{Type: CommandSet, Key: "c", Value: "3"},
		{Type: CommandSet, Key: "b", Value: "22"},
	}
	s1, s2 := store.New(), store.New()
	f1, f2 := New(s1), New(s2)
	for _, c := range cmds {
		applyCmd(t, f1, c)
		applyCmd(t, f2, c)
	}

	snap1, snap2 := s1.Snapshot(), s2.Snapshot()
	if len(snap1) != len(snap2) {
		t.Fatalf("replicas diverged: %v vs %v", snap1, snap2)
	}
	for k, v := range snap1 {
		if snap2[k] != v {
			t.Fatalf("replicas diverged on %q: %q vs %q", k, v, snap2[k])
		}
	}
}

// A snapshot must capture state at the moment it was taken, even if writes
// keep arriving while Raft is persisting it.
func TestSnapshotUnaffectedByLaterWrites(t *testing.T) {
	s := store.New()
	s.Set("a", "1")
	f := New(s)

	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	applyCmd(t, f, Command{Type: CommandSet, Key: "a", Value: "changed"})
	applyCmd(t, f, Command{Type: CommandSet, Key: "b", Value: "2"})

	sink := newFakeSnapshotSink()
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}
	restored := store.New()
	if err := New(restored).Restore(sink.reader()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if val, _ := restored.Get("a"); val != "1" {
		t.Fatalf("snapshot picked up a later write: got %q", val)
	}
	if restored.Len() != 1 {
		t.Fatalf("expected 1 key in snapshot, got %d", restored.Len())
	}
	snapshot.Release() // no-op, but part of the raft.FSMSnapshot contract
}

// Restore must replace state, not merge into it.
func TestRestoreReplacesExistingState(t *testing.T) {
	s := store.New()
	s.Set("stale", "x")
	f := New(s)

	if err := f.Restore(io.NopCloser(strings.NewReader(`{"fresh":"y"}`))); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := s.Get("stale"); err != store.ErrKeyNotFound {
		t.Fatalf("expected stale key removed, got %v", err)
	}
	if val, _ := s.Get("fresh"); val != "y" {
		t.Fatalf("expected fresh key, got %q", val)
	}
}

func TestRestoreMalformedSnapshot(t *testing.T) {
	s := store.New()
	s.Set("keep", "1")
	f := New(s)

	if err := f.Restore(io.NopCloser(strings.NewReader("garbage"))); err == nil {
		t.Fatal("expected an error restoring a malformed snapshot")
	}
	if val, _ := s.Get("keep"); val != "1" {
		t.Fatalf("failed restore must leave existing data alone, got %q", val)
	}
}

// A snapshot of "null" decodes to a nil map; the node must stay writable.
func TestRestoreNullSnapshotStaysWritable(t *testing.T) {
	s := store.New()
	f := New(s)

	if err := f.Restore(io.NopCloser(strings.NewReader("null"))); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if res := applyCmd(t, f, Command{Type: CommandSet, Key: "k", Value: "v"}); res != nil {
		t.Fatalf("apply after null restore failed: %v", res)
	}
}

func TestRestoreClosesReader(t *testing.T) {
	rc := &trackingReadCloser{Reader: strings.NewReader(`{}`)}
	if err := New(store.New()).Restore(rc); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !rc.closed {
		t.Fatal("Restore must close the snapshot reader it is given")
	}
}

// If writing the snapshot fails, Persist must Cancel the sink (so Raft
// discards the partial file) and report the error.
func TestPersistCancelsSinkOnWriteError(t *testing.T) {
	s := store.New()
	s.Set("a", "1")
	snapshot, _ := New(s).Snapshot()

	sink := &failingSink{}
	if err := snapshot.Persist(sink); err == nil {
		t.Fatal("expected Persist to return the write error")
	}
	if !sink.cancelled {
		t.Fatal("expected Persist to cancel the sink after a write error")
	}
}
