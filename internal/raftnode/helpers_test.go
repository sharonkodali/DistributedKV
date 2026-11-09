package raftnode

import (
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// testNode remembers the Config a node was started with, so a test can shut
// it down ("crash" it) and later start it again with the same identity and
// data directory ("recover" it).
type testNode struct {
	*Node
	cfg  Config
	down atomic.Bool // atomic: read by writer goroutines while a test crashes the node
}

// freeAddr asks the OS for an unused localhost TCP port. There is a tiny
// window where another process could grab it before Raft binds, but on a
// test machine that's not a practical concern.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startCluster starts n real Raft nodes on localhost, each with its own
// on-disk data directory. node1 bootstraps the cluster; the rest join it
// through the leader, exactly like the Docker Compose setup does.
func startCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	root := t.TempDir()
	nodes := make([]*testNode, 0, n)

	for i := 1; i <= n; i++ {
		cfg := Config{
			NodeID:    fmt.Sprintf("node%d", i),
			RaftAddr:  freeAddr(t),
			DataDir:   filepath.Join(root, fmt.Sprintf("node%d", i)),
			Bootstrap: i == 1,
		}
		node, err := Start(cfg)
		if err != nil {
			t.Fatalf("failed to start %s: %v", cfg.NodeID, err)
		}
		nodes = append(nodes, &testNode{Node: node, cfg: cfg})
	}

	t.Cleanup(func() {
		for _, tn := range nodes {
			if !tn.down.Load() {
				_ = tn.Shutdown()
			}
		}
	})

	leader := waitForLeader(t, nodes[:1], 10*time.Second)
	for _, tn := range nodes[1:] {
		if err := leader.Join(tn.cfg.NodeID, tn.cfg.RaftAddr); err != nil {
			t.Fatalf("failed to join %s: %v", tn.cfg.NodeID, err)
		}
	}

	// Wait until every node agrees on who the leader is, so tests start
	// from a fully formed cluster.
	waitFor(t, 10*time.Second, "all nodes to see the leader", func() bool {
		for _, tn := range nodes {
			if tn.LeaderAddr() == "" {
				return false
			}
		}
		return true
	})
	return nodes
}

// waitForLeader returns the single running node that considers itself
// leader, failing the test if none appears before the timeout.
func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	var leader *testNode
	waitFor(t, timeout, "a leader to be elected", func() bool {
		for _, tn := range nodes {
			if !tn.down.Load() && tn.IsLeader() {
				leader = tn
				return true
			}
		}
		return false
	})
	return leader
}

// followers returns every running node that isn't the given leader.
func followers(nodes []*testNode, leader *testNode) []*testNode {
	var out []*testNode
	for _, tn := range nodes {
		if tn != leader && !tn.down.Load() {
			out = append(out, tn)
		}
	}
	return out
}

// crash shuts a node down abruptly, as if its process were killed.
func crash(t *testing.T, tn *testNode) {
	t.Helper()
	if err := tn.Shutdown(); err != nil {
		t.Fatalf("failed to shut down %s: %v", tn.cfg.NodeID, err)
	}
	tn.down.Store(true)
}

// restart brings a crashed node back with the same ID, address and data
// directory. Bootstrap is always off: the node already belongs to a cluster.
func restart(t *testing.T, tn *testNode) {
	t.Helper()
	cfg := tn.cfg
	cfg.Bootstrap = false
	node, err := Start(cfg)
	if err != nil {
		t.Fatalf("failed to restart %s: %v", cfg.NodeID, err)
	}
	tn.Node = node
	tn.down.Store(false)
}

// waitFor polls cond every 20ms until it returns true or the timeout passes.
// Distributed systems converge "eventually", so tests must wait for a state
// rather than assume it's reached instantly.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// waitForValue waits until the node's local store holds key=want.
func waitForValue(t *testing.T, tn *testNode, key, want string) {
	t.Helper()
	waitFor(t, 5*time.Second, fmt.Sprintf("%s to have %s=%q", tn.cfg.NodeID, key, want), func() bool {
		got, err := tn.Store.Get(key)
		return err == nil && got == want
	})
}
