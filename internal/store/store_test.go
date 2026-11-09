package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	s := New()
	s.Set("name", "John")

	val, err := s.Get("name")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "John" {
		t.Fatalf("expected 'John', got %q", val)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := New()

	_, err := s.Get("does-not-exist")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Set("name", "John")
	s.Delete("name")

	_, err := s.Get("name")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	s := New()
	s.Set("name", "John")
	s.Set("name", "Jane")

	val, _ := s.Get("name")
	if val != "Jane" {
		t.Fatalf("expected 'Jane', got %q", val)
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected snapshot of 2 keys, got %d", len(snap))
	}

	// Mutating the original store after taking the snapshot should NOT
	// affect the snapshot -- it must be an independent copy.
	s.Set("c", "3")
	if len(snap) != 2 {
		t.Fatalf("snapshot should not change after original store mutates, got %d keys", len(snap))
	}

	// Restoring into a fresh store should reproduce the snapshotted state.
	fresh := New()
	fresh.Restore(snap)

	val, err := fresh.Get("a")
	if err != nil || val != "1" {
		t.Fatalf("expected restored key 'a' to be '1', got %q, err %v", val, err)
	}
	if fresh.Len() != 2 {
		t.Fatalf("expected restored store to have 2 keys, got %d", fresh.Len())
	}
}

// TestConcurrentAccess simulates many goroutines reading and writing at the
// same time. Run with `go test -race` -- if the mutex were missing or wrong,
// this test would crash or the race detector would flag it.
func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			s.Set("key", "value")
		}(i)
		go func(n int) {
			defer wg.Done()
			_, _ = s.Get("key")
		}(i)
	}

	wg.Wait()
}

func TestNewStoreIsEmpty(t *testing.T) {
	s := New()
	if s.Len() != 0 {
		t.Fatalf("expected empty store, got %d keys", s.Len())
	}
}

func TestDeleteMissingKeyIsNoOp(t *testing.T) {
	s := New()
	s.Set("keep", "1")
	s.Delete("never-existed")

	if s.Len() != 1 {
		t.Fatalf("expected 1 key after deleting a missing key, got %d", s.Len())
	}
}

func TestLenTracksSetsAndDeletes(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("a", "overwrite") // overwriting must not add a key
	if s.Len() != 2 {
		t.Fatalf("expected 2 keys, got %d", s.Len())
	}
	s.Delete("a")
	if s.Len() != 1 {
		t.Fatalf("expected 1 key after delete, got %d", s.Len())
	}
}

// Empty keys and values are legal at the store level -- the HTTP layer is
// what rejects empty keys, so the store itself shouldn't special-case them.
func TestEmptyKeyAndValue(t *testing.T) {
	s := New()
	s.Set("", "")

	val, err := s.Get("")
	if err != nil || val != "" {
		t.Fatalf("expected empty value with no error, got %q, %v", val, err)
	}
}

func TestSnapshotOfEmptyStore(t *testing.T) {
	snap := New().Snapshot()
	if snap == nil || len(snap) != 0 {
		t.Fatalf("expected non-nil empty snapshot, got %#v", snap)
	}
}

// Writing to the snapshot map must not leak back into the live store.
func TestSnapshotIsIndependentCopy(t *testing.T) {
	s := New()
	s.Set("a", "1")

	snap := s.Snapshot()
	snap["a"] = "tampered"
	snap["new"] = "x"

	if val, _ := s.Get("a"); val != "1" {
		t.Fatalf("mutating snapshot changed store: got %q", val)
	}
	if s.Len() != 1 {
		t.Fatalf("mutating snapshot added keys to store: got %d", s.Len())
	}
}

func TestRestoreReplacesExistingData(t *testing.T) {
	s := New()
	s.Set("old", "gone")

	s.Restore(map[string]string{"new": "here"})

	if _, err := s.Get("old"); err != ErrKeyNotFound {
		t.Fatalf("expected old key to be wiped by Restore, got err %v", err)
	}
	if val, _ := s.Get("new"); val != "here" {
		t.Fatalf("expected restored key, got %q", val)
	}
}

// Regression test: Restore used to store the caller's map directly, so the
// caller could change the store's contents without taking the lock.
func TestRestoreCopiesInput(t *testing.T) {
	input := map[string]string{"a": "1"}
	s := New()
	s.Restore(input)

	input["a"] = "tampered"
	input["b"] = "2"

	if val, _ := s.Get("a"); val != "1" {
		t.Fatalf("mutating Restore's input changed the store: got %q", val)
	}
	if s.Len() != 1 {
		t.Fatalf("expected 1 key, got %d", s.Len())
	}
}

// Regression test: Restore(nil) used to leave a nil map behind, so the next
// Set panicked with "assignment to entry in nil map".
func TestRestoreNilThenSet(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Restore(nil)

	if s.Len() != 0 {
		t.Fatalf("expected empty store after Restore(nil), got %d", s.Len())
	}
	s.Set("b", "2") // must not panic
	if val, _ := s.Get("b"); val != "2" {
		t.Fatalf("expected 'b'='2', got %q", val)
	}
}

// TestConcurrentMixedOperations hammers every method at once, including
// Snapshot and Restore, so `go test -race` can catch any unguarded access.
func TestConcurrentMixedOperations(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(5)
		key := fmt.Sprintf("k%d", i)
		go func() { defer wg.Done(); s.Set(key, "v") }()
		go func() { defer wg.Done(); _, _ = s.Get(key) }()
		go func() { defer wg.Done(); s.Delete(key) }()
		go func() { defer wg.Done(); _ = s.Len(); _ = s.Snapshot() }()
		go func() { defer wg.Done(); s.Restore(map[string]string{key: "r"}) }()
	}
	wg.Wait()
}

// After concurrent writes to distinct keys finish, every write must be visible.
func TestConcurrentWritesAllLand(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	const n = 500

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		}(i)
	}
	wg.Wait()

	if s.Len() != n {
		t.Fatalf("expected %d keys, got %d", n, s.Len())
	}
	for i := 0; i < n; i++ {
		if val, _ := s.Get(fmt.Sprintf("k%d", i)); val != fmt.Sprintf("v%d", i) {
			t.Fatalf("key k%d: expected v%d, got %q", i, i, val)
		}
	}
}
