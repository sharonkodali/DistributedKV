package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"distributed-kv/internal/raftnode"
)

// ---------- helpers ----------

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// testCluster is n real Raft nodes, each fronted by a real HTTP server
// running the production routing table.
type testCluster struct {
	nodes   []*raftnode.Node
	servers []*server
	https   []*httptest.Server
}

func (c *testCluster) leaderIndex(t *testing.T) int {
	t.Helper()
	idx := -1
	waitFor(t, 10*time.Second, "a leader", func() bool {
		for i, n := range c.nodes {
			if n.IsLeader() {
				idx = i
				return true
			}
		}
		return false
	})
	return idx
}

func (c *testCluster) followerIndex(t *testing.T) int {
	t.Helper()
	l := c.leaderIndex(t)
	return (l + 1) % len(c.nodes)
}

func (c *testCluster) url(i int, path string) string {
	return c.https[i].URL + path
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	root := t.TempDir()
	c := &testCluster{}
	raftAddrs := make([]string, n)

	for i := 0; i < n; i++ {
		raftAddrs[i] = freeAddr(t)
		node, err := raftnode.Start(raftnode.Config{
			NodeID:    fmt.Sprintf("node%d", i+1),
			RaftAddr:  raftAddrs[i],
			DataDir:   filepath.Join(root, fmt.Sprintf("node%d", i+1)),
			Bootstrap: i == 0,
		})
		if err != nil {
			t.Fatalf("start node%d: %v", i+1, err)
		}
		c.nodes = append(c.nodes, node)
	}

	// Every server shares one peers map (raftAddr -> httpAddr). It's filled
	// in after the HTTP servers exist, since httptest picks the ports.
	peers := map[string]string{}
	for _, node := range c.nodes {
		s := &server{node: node, peers: peers}
		c.servers = append(c.servers, s)
		c.https = append(c.https, httptest.NewServer(s.routes()))
	}
	for i, h := range c.https {
		peers[raftAddrs[i]] = strings.TrimPrefix(h.URL, "http://")
	}

	t.Cleanup(func() {
		for i := range c.nodes {
			c.https[i].Close()
			_ = c.nodes[i].Shutdown()
		}
	})

	waitFor(t, 10*time.Second, "node1 to become leader", c.nodes[0].IsLeader)
	for i := 1; i < n; i++ {
		if err := c.nodes[0].Join(fmt.Sprintf("node%d", i+1), raftAddrs[i]); err != nil {
			t.Fatalf("join node%d: %v", i+1, err)
		}
	}
	waitFor(t, 10*time.Second, "all nodes to know the leader", func() bool {
		for _, node := range c.nodes {
			if node.LeaderAddr() == "" {
				return false
			}
		}
		return true
	})
	return c
}

func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// ---------- parsePeers / resolveWithRetry ----------

func shortRetries(t *testing.T) {
	t.Helper()
	oldRA, oldRD, oldJA, oldJD := resolveAttempts, resolveDelay, joinAttempts, joinDelay
	resolveAttempts, resolveDelay = 2, 10*time.Millisecond
	joinAttempts, joinDelay = 2, 10*time.Millisecond
	t.Cleanup(func() {
		resolveAttempts, resolveDelay, joinAttempts, joinDelay = oldRA, oldRD, oldJA, oldJD
	})
}

func TestParsePeersEmpty(t *testing.T) {
	m, err := parsePeers("")
	if err != nil || len(m) != 0 {
		t.Fatalf("expected empty map, got %v, %v", m, err)
	}
}

func TestParsePeersValid(t *testing.T) {
	m, err := parsePeers("127.0.0.1:9001=127.0.0.1:8081,127.0.0.1:9002=127.0.0.1:8082")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m["127.0.0.1:9001"] != "127.0.0.1:8081" || m["127.0.0.1:9002"] != "127.0.0.1:8082" || len(m) != 2 {
		t.Fatalf("unexpected map: %v", m)
	}
}

// Regression test for the Docker bug: Raft reports leaders by IP, so the
// map must be keyed by the resolved IP even when configured by hostname.
func TestParsePeersResolvesHostnamesToIPs(t *testing.T) {
	m, err := parsePeers("localhost:9001=localhost:8081")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := m["localhost:9001"]; ok {
		t.Fatal("map must not be keyed by hostname; Raft never reports hostnames")
	}
	for k, v := range m {
		host, _, _ := net.SplitHostPort(k)
		if net.ParseIP(host) == nil {
			t.Fatalf("expected an IP key, got %q", k)
		}
		if v != "localhost:8081" {
			t.Fatalf("HTTP address should be kept as configured, got %q", v)
		}
	}
}

func TestParsePeersRejectsMalformedEntry(t *testing.T) {
	if _, err := parsePeers("127.0.0.1:9001"); err == nil {
		t.Fatal("expected an error for an entry without '='")
	}
}

func TestParsePeersFailsOnUnresolvableAddr(t *testing.T) {
	shortRetries(t)
	if _, err := parsePeers("127.0.0.1:not-a-port=127.0.0.1:8081"); err == nil {
		t.Fatal("expected an error for an address that can never resolve")
	}
}

func TestResolveWithRetryGivesUpAfterAttempts(t *testing.T) {
	shortRetries(t)
	start := time.Now()
	if _, err := resolveWithRetry("127.0.0.1:bogus"); err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("resolveWithRetry ignored the configured attempt count")
	}
}

// ---------- single-node HTTP API ----------

func TestPutThenGet(t *testing.T) {
	c := newTestCluster(t, 1)

	if code, _ := do(t, http.MethodPut, c.url(0, "/kv/username"), "John"); code != http.StatusOK {
		t.Fatalf("PUT: expected 200, got %d", code)
	}
	code, body := do(t, http.MethodGet, c.url(0, "/kv/username"), "")
	if code != http.StatusOK {
		t.Fatalf("GET: expected 200, got %d", code)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("GET returned invalid JSON %q: %v", body, err)
	}
	if got["key"] != "username" || got["value"] != "John" {
		t.Fatalf("unexpected body: %v", got)
	}
}

func TestGetMissingKeyReturns404(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodGet, c.url(0, "/kv/nope"), ""); code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", code)
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	c := newTestCluster(t, 1)
	do(t, http.MethodPut, c.url(0, "/kv/k"), "v")

	if code, _ := do(t, http.MethodDelete, c.url(0, "/kv/k"), ""); code != http.StatusOK {
		t.Fatalf("DELETE: expected 200, got %d", code)
	}
	if code, _ := do(t, http.MethodGet, c.url(0, "/kv/k"), ""); code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", code)
	}
}

func TestDeleteMissingKeyIsIdempotent(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodDelete, c.url(0, "/kv/never"), ""); code != http.StatusOK {
		t.Fatalf("expected 200 deleting a missing key, got %d", code)
	}
}

// Values are raw bytes, so spaces, newlines, JSON and unicode must survive.
func TestPutPreservesValueExactly(t *testing.T) {
	c := newTestCluster(t, 1)
	value := "line one\nline two {\"json\": true} ünïcode ✓"
	do(t, http.MethodPut, c.url(0, "/kv/raw"), value)

	_, body := do(t, http.MethodGet, c.url(0, "/kv/raw"), "")
	var got map[string]string
	_ = json.Unmarshal([]byte(body), &got)
	if got["value"] != value {
		t.Fatalf("value changed in transit: got %q, want %q", got["value"], value)
	}
}

func TestEmptyKeyReturns400(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodGet, c.url(0, "/kv/"), ""); code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty key, got %d", code)
	}
}

func TestUnsupportedMethodReturns405(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodPost, c.url(0, "/kv/k"), "v"); code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", code)
	}
}

func TestStatusReportsLeaderAndKeyCount(t *testing.T) {
	c := newTestCluster(t, 1)
	do(t, http.MethodPut, c.url(0, "/kv/a"), "1")
	do(t, http.MethodPut, c.url(0, "/kv/b"), "2")

	code, body := do(t, http.MethodGet, c.url(0, "/status"), "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	var status struct {
		State      string `json:"state"`
		IsLeader   bool   `json:"is_leader"`
		LeaderAddr string `json:"leader_addr"`
		KeyCount   int    `json:"key_count"`
	}
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatalf("invalid JSON %q: %v", body, err)
	}
	if status.State != "Leader" || !status.IsLeader || status.LeaderAddr == "" || status.KeyCount != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

// ---------- /join ----------

func TestJoinRejectsNonPost(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodGet, c.url(0, "/join"), ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", code)
	}
}

func TestJoinRejectsBadBody(t *testing.T) {
	c := newTestCluster(t, 1)
	if code, _ := do(t, http.MethodPost, c.url(0, "/join"), "not json"); code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
}

func TestJoinOnFollowerReturns503(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.followerIndex(t)
	body := `{"node_id":"node9","raft_addr":"127.0.0.1:1"}`
	if code, _ := do(t, http.MethodPost, c.url(f, "/join"), body); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from a follower, got %d", code)
	}
}

// The real join path: a fresh node calls joinCluster against the leader's
// HTTP API and becomes a voting member that receives replicated data.
func TestJoinClusterAddsNodeOverHTTP(t *testing.T) {
	c := newTestCluster(t, 1)
	do(t, http.MethodPut, c.url(0, "/kv/existing"), "data")

	raftAddr := freeAddr(t)
	newNode, err := raftnode.Start(raftnode.Config{NodeID: "node2", RaftAddr: raftAddr, DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer newNode.Shutdown()

	if ok := joinCluster(strings.TrimPrefix(c.https[0].URL, "http://"), "node2", raftAddr); !ok {
		t.Fatal("joinCluster reported failure")
	}
	waitFor(t, 5*time.Second, "new node to receive existing data", func() bool {
		v, err := newNode.Store.Get("existing")
		return err == nil && v == "data"
	})
}

func TestJoinClusterGivesUpOnPersistentFailure(t *testing.T) {
	shortRetries(t)
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer failing.Close()

	if joinCluster(strings.TrimPrefix(failing.URL, "http://"), "n", "127.0.0.1:1") {
		t.Fatal("expected joinCluster to give up against a server that always fails")
	}
}

func TestJoinClusterGivesUpWhenUnreachable(t *testing.T) {
	shortRetries(t)
	if joinCluster(freeAddr(t), "n", "127.0.0.1:1") { // nothing listens there
		t.Fatal("expected joinCluster to give up when no server is listening")
	}
}

func TestJoinWithDuplicateAddressFails(t *testing.T) {
	c := newTestCluster(t, 1)
	// Adding a voter with an ID that already exists but a different address
	// is a conflicting membership change; Raft must refuse it.
	leaderAddr := c.nodes[0].LeaderAddr()
	body := fmt.Sprintf(`{"node_id":"other","raft_addr":%q}`, leaderAddr)
	if code, _ := do(t, http.MethodPost, c.url(0, "/join"), body); code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for a conflicting join, got %d", code)
	}
}

// ---------- multi-node: forwarding ----------

// Clients can write to ANY node. A follower must proxy the write to the
// leader, and the value must then be readable from every node.
func TestWriteToFollowerIsForwardedToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.followerIndex(t)

	if code, body := do(t, http.MethodPut, c.url(f, "/kv/fwd"), "ok"); code != http.StatusOK {
		t.Fatalf("PUT via follower: expected 200, got %d (%s)", code, body)
	}
	for i := range c.nodes {
		i := i
		waitFor(t, 5*time.Second, fmt.Sprintf("node%d to have the forwarded write", i+1), func() bool {
			v, err := c.nodes[i].Store.Get("fwd")
			return err == nil && v == "ok"
		})
	}
}

func TestDeleteToFollowerIsForwardedToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	l, f := c.leaderIndex(t), c.followerIndex(t)
	do(t, http.MethodPut, c.url(l, "/kv/gone"), "x")

	if code, _ := do(t, http.MethodDelete, c.url(f, "/kv/gone"), ""); code != http.StatusOK {
		t.Fatalf("DELETE via follower: expected 200, got %d", code)
	}
	if _, err := c.nodes[l].Store.Get("gone"); err == nil {
		t.Fatal("leader should no longer have the key")
	}
}

func TestStatusOnFollower(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.followerIndex(t)

	_, body := do(t, http.MethodGet, c.url(f, "/status"), "")
	var status map[string]interface{}
	_ = json.Unmarshal([]byte(body), &status)
	if status["state"] != "Follower" || status["is_leader"] != false || status["leader_addr"] == "" {
		t.Fatalf("unexpected follower status: %v", status)
	}
}

func TestForwardFailsWhenLeaderHTTPUnknown(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.followerIndex(t)
	c.servers[f].peers = map[string]string{} // follower has no address book

	if code, _ := do(t, http.MethodPut, c.url(f, "/kv/k"), "v"); code != http.StatusBadGateway {
		t.Fatalf("expected 502 when the leader's HTTP address is unknown, got %d", code)
	}
}

func TestForwardFailsWhenLeaderUnreachable(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.followerIndex(t)
	leaderRaft := c.nodes[f].LeaderAddr()
	c.servers[f].peers = map[string]string{leaderRaft: freeAddr(t)} // nothing listening

	if code, _ := do(t, http.MethodPut, c.url(f, "/kv/k"), "v"); code != http.StatusBadGateway {
		t.Fatalf("expected 502 when the leader can't be reached, got %d", code)
	}
}

// A node with no cluster has no leader to forward to: 503, "try again".
func TestWriteWithNoLeaderReturns503(t *testing.T) {
	node, err := raftnode.Start(raftnode.Config{NodeID: "lonely", RaftAddr: freeAddr(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer node.Shutdown()
	h := httptest.NewServer((&server{node: node, peers: map[string]string{}}).routes())
	defer h.Close()

	if code, _ := do(t, http.MethodPut, h.URL+"/kv/k", "v"); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 with no leader, got %d", code)
	}
}

// Writes fail with 500 (not a false 200) when the leader has lost quorum.
func TestWriteWithoutQuorumReturns500(t *testing.T) {
	c := newTestCluster(t, 3)
	l := c.leaderIndex(t)
	for i := range c.nodes {
		if i != l {
			_ = c.nodes[i].Shutdown()
		}
	}

	code, _ := do(t, http.MethodPut, c.url(l, "/kv/k"), "v")
	// The leader either still thinks it leads (Raft apply fails: 500) or has
	// already stepped down and has no leader to forward to (503). Either is
	// a correct refusal; a 200 would mean an unsafe, unreplicated write.
	if code != http.StatusInternalServerError && code != http.StatusServiceUnavailable {
		t.Fatalf("expected 500 or 503 without quorum, got %d", code)
	}
}

// ---------- setup (flag handling) ----------

func TestSetupRequiresID(t *testing.T) {
	if _, _, err := setup([]string{"-raft-addr", freeAddr(t)}); err == nil {
		t.Fatal("expected an error when -id is missing")
	}
}

func TestSetupRejectsUnknownFlag(t *testing.T) {
	if _, _, err := setup([]string{"-id", "n", "-no-such-flag"}); err == nil {
		t.Fatal("expected an error for an unknown flag")
	}
}

func TestSetupRejectsBadPeers(t *testing.T) {
	if _, _, err := setup([]string{"-id", "n", "-peers", "missing-equals-sign"}); err == nil {
		t.Fatal("expected an error for a malformed -peers value")
	}
}

func TestSetupFailsWhenRaftCantStart(t *testing.T) {
	if _, _, err := setup([]string{"-id", "n", "-raft-addr", "127.0.0.1:bad", "-data-dir", t.TempDir()}); err == nil {
		t.Fatal("expected an error when the Raft node can't start")
	}
}

// End to end through setup(): a bootstrapped node, then a second node that
// joins it using only command-line flags, the way Docker Compose runs them.
func TestSetupBootstrapAndJoinViaFlags(t *testing.T) {
	raft1, raft2 := freeAddr(t), freeAddr(t)
	s1, httpAddr, err := setup([]string{"-id", "node1", "-raft-addr", raft1, "-http-addr", ":0",
		"-data-dir", t.TempDir(), "-bootstrap"})
	if err != nil {
		t.Fatalf("setup node1: %v", err)
	}
	defer s1.node.Shutdown()
	if httpAddr != ":0" {
		t.Fatalf("expected http addr to be passed through, got %q", httpAddr)
	}
	h1 := httptest.NewServer(s1.routes())
	defer h1.Close()
	waitFor(t, 10*time.Second, "node1 to lead", s1.node.IsLeader)

	s2, _, err := setup([]string{"-id", "node2", "-raft-addr", raft2, "-data-dir", t.TempDir(),
		"-join", strings.TrimPrefix(h1.URL, "http://")})
	if err != nil {
		t.Fatalf("setup node2: %v", err)
	}
	defer s2.node.Shutdown()

	waitFor(t, 10*time.Second, "node2 to join and see the leader", func() bool {
		return s2.node.LeaderAddr() != ""
	})
}
