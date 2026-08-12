package store

import (
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
