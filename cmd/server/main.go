// Command server runs a single-node HTTP key-value server.
//
// This is Day 1 of the project: one server, no replication yet. It exists
// so we have a correct, tested foundation before adding the distributed
// parts (Raft, replication, leader election) in later stages.
//
// API:
//
//	GET    /kv/{key}   -> 200 {"key":..,"value":..}   or 404 if missing
//	PUT    /kv/{key}   -> body is the raw value        -> 200 on success
//	DELETE /kv/{key}   -> 200 on success (idempotent)
//	GET    /status     -> basic info about this node (used later for leader info)
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"strings"

	"distributed-kv/internal/store"
)

// server bundles the store with HTTP handlers. Using a struct (instead of
// package-level functions) keeps this testable and avoids global state --
// each server instance owns its own store.
type server struct {
	store *store.Store
}

func main() {
	addr := flag.String("addr", ":8080", "address for the HTTP server to listen on")
	flag.Parse()

	s := &server{store: store.New()}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)

	log.Printf("kv server listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// handleKV dispatches GET/PUT/DELETE on /kv/{key} to the right store method.
func (s *server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key is required, e.g. /kv/username", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleGet(w http.ResponseWriter, key string) {
	val, err := s.store.Get(key)
	if err != nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": val,
	})
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	s.store.Set(key, string(body))
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleDelete(w http.ResponseWriter, key string) {
	s.store.Delete(key)
	w.WriteHeader(http.StatusOK)
}

// handleStatus reports basic node info. Right now it's just a key count,
// but this is the same endpoint we'll extend later to show "am I the
// leader?", current Raft term, etc. -- so build the habit of checking it now.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"key_count": s.store.Len(),
	})
}
