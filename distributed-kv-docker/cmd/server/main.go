// Command server runs one node of a distributed-kv cluster.
//
// Each node runs both an HTTP API (for clients) and a Raft instance (for
// talking to its peers). Writes are only ever applied on the leader --
// if this node isn't the leader, it transparently proxies the write to
// whichever node currently is, so clients don't need to know or care
// which node is in charge at any given moment.
//
// Example: starting a 3-node cluster locally.
//
//	# node1 bootstraps a brand-new, single-member cluster
//	go run ./cmd/server \
//	  -id node1 -http-addr :8081 -raft-addr :9001 \
//	  -data-dir /tmp/kv/node1 -bootstrap \
//	  -peers 127.0.0.1:9001=127.0.0.1:8081,127.0.0.1:9002=127.0.0.1:8082,127.0.0.1:9003=127.0.0.1:8083
//
//	# node2 and node3 join through node1's HTTP API
//	go run ./cmd/server \
//	  -id node2 -http-addr :8082 -raft-addr :9002 \
//	  -data-dir /tmp/kv/node2 -join 127.0.0.1:8081 \
//	  -peers 127.0.0.1:9001=127.0.0.1:8081,127.0.0.1:9002=127.0.0.1:8082,127.0.0.1:9003=127.0.0.1:8083
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"distributed-kv/internal/raftnode"
)

func main() {
	id := flag.String("id", "", "unique node ID, e.g. node1 (required)")
	httpAddr := flag.String("http-addr", ":8080", "address for the client-facing HTTP API")
	raftAddr := flag.String("raft-addr", ":9000", "address for Raft's internal node-to-node traffic")
	dataDir := flag.String("data-dir", "/tmp/kv-data", "directory for this node's Raft log/snapshots")
	bootstrap := flag.Bool("bootstrap", false, "true ONLY for the very first node of a brand-new cluster")
	join := flag.String("join", "", "HTTP address of an existing cluster member to join through, e.g. 127.0.0.1:8081")
	peers := flag.String("peers", "", "comma-separated raftAddr=httpAddr map, used to forward writes to the leader")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required")
	}

	peerMap, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}

	node, err := raftnode.Start(raftnode.Config{
		NodeID:    *id,
		RaftAddr:  *raftAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
	})
	if err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}

	if *join != "" {
		go joinCluster(*join, *id, *raftAddr)
	}

	s := &server{node: node, peers: peerMap}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/join", s.handleJoin)

	log.Printf("node %s: http on %s, raft on %s", *id, *httpAddr, *raftAddr)
	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// parsePeers turns "raftAddr1=httpAddr1,raftAddr2=httpAddr2" into a map.
// This map is how a follower figures out WHICH HTTP ADDRESS to forward a
// write to once it knows the leader's Raft address (Raft only speaks in
// terms of its own transport addresses, not our HTTP API addresses).
func parsePeers(s string) (map[string]string, error) {
	m := make(map[string]string)
	if s == "" {
		return m, nil
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad peer entry %q, expected raftAddr=httpAddr", pair)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}

// joinCluster asks an existing node (reached via its HTTP API) to add us
// as a new Raft voter. It retries for a while because the target node may
// not have finished electing a leader yet when we first try.
func joinCluster(joinHTTPAddr, nodeID, raftAddr string) {
	body, _ := json.Marshal(map[string]string{"node_id": nodeID, "raft_addr": raftAddr})

	for attempt := 0; attempt < 10; attempt++ {
		resp, err := http.Post("http://"+joinHTTPAddr+"/join", "application/json", strings.NewReader(string(body)))
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("successfully joined cluster via %s", joinHTTPAddr)
				return
			}
			respBody, _ := io.ReadAll(resp.Body)
			log.Printf("join attempt %d failed: %s", attempt+1, string(respBody))
		} else {
			log.Printf("join attempt %d failed: %v", attempt+1, err)
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("giving up joining cluster after repeated failures")
}

type server struct {
	node  *raftnode.Node
	peers map[string]string // raftAddr -> httpAddr
}

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
		s.handleWrite(w, r, key, "PUT")
	case http.MethodDelete:
		s.handleWrite(w, r, key, "DELETE")
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet always reads from THIS node's local store. This is a
// deliberate simplicity/consistency tradeoff worth being able to explain:
// a follower's local copy could theoretically be a few milliseconds
// behind the leader if a write is still being replicated when the read
// happens ("eventually consistent" reads). The alternative -- forcing
// every read through the leader like writes -- would be "strongly
// consistent" but slower and puts more load on a single node.
func (s *server) handleGet(w http.ResponseWriter, key string) {
	val, err := s.node.Store.Get(key)
	if err != nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

// handleWrite is the core of the "only the leader writes" rule. If this
// node is the leader, it applies the write through Raft directly. If not,
// it transparently proxies the request to whichever node IS the leader,
// so the client never needs to know or retry manually.
func (s *server) handleWrite(w http.ResponseWriter, r *http.Request, key, method string) {
	if !s.node.IsLeader() {
		s.forwardToLeader(w, r, key)
		return
	}

	var err error
	if method == "PUT" {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		err = s.node.Set(key, string(body))
	} else {
		err = s.node.Delete(key)
	}

	if err != nil {
		http.Error(w, fmt.Sprintf("write failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// forwardToLeader proxies a write request to the current leader's HTTP
// API and relays its response back to the original client.
func (s *server) forwardToLeader(w http.ResponseWriter, r *http.Request, key string) {
	leaderRaftAddr := s.node.LeaderAddr()
	if leaderRaftAddr == "" {
		http.Error(w, "no leader currently elected, try again shortly", http.StatusServiceUnavailable)
		return
	}

	leaderHTTPAddr, ok := s.peers[leaderRaftAddr]
	if !ok {
		http.Error(w, fmt.Sprintf("don't know the HTTP address for leader %s", leaderRaftAddr), http.StatusBadGateway)
		return
	}

	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	req, err := http.NewRequest(r.Method, "http://"+leaderHTTPAddr+"/kv/"+key, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, "failed to build forward request", http.StatusInternalServerError)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to reach leader: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleJoin lets another node ask to be added to the cluster as a voter.
// Only the leader is allowed to change cluster membership, so this
// rejects the request (with the same "not leader" pattern as writes) if
// called against a follower.
func (s *server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if !s.node.IsLeader() {
		http.Error(w, "not the leader, retry against the current leader", http.StatusServiceUnavailable)
		return
	}

	if err := s.node.Join(req.NodeID, req.RaftAddr); err != nil {
		http.Error(w, fmt.Sprintf("failed to join: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleStatus reports this node's view of the cluster -- crucial for the
// "demo moment" of showing who the current leader is and watching it
// change when you kill a process.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"state":       s.node.Raft.State().String(),
		"is_leader":   s.node.IsLeader(),
		"leader_addr": s.node.LeaderAddr(),
		"key_count":   s.node.Store.Len(),
	})
}
