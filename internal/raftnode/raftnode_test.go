package raftnode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"distributed-kv/internal/store"
)

func TestSingleNodeBootstrapBecomesLeader(t *testing.T) {
	nodes := startCluster(t, 1)
	n := nodes[0]

	if !n.IsLeader() {
		t.Fatal("a bootstrapped single-node cluster should elect itself leader")
	}
	if n.LeaderAddr() == "" {
		t.Fatal("leader should know its own address")
	}
}

func TestSingleNodeSetGetDelete(t *testing.T) {
	n := startCluster(t, 1)[0]

	if err := n.Set("username", "John"); err != nil {
		t.Fatalf("set: %v", err)
	}
	// Set only returns after the FSM applied the entry, so the value must
	// already be in the local store -- no waiting needed on the leader.
	if val, err := n.Store.Get("username"); err != nil || val != "John" {
		t.Fatalf("expected John, got %q, %v", val, err)
	}

	if err := n.Delete("username"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := n.Store.Get("username"); err != store.ErrKeyNotFound {
		t.Fatalf("expected key deleted, got %v", err)
	}
}

func TestStartFailsOnInvalidRaftAddr(t *testing.T) {
	_, err := Start(Config{NodeID: "n", RaftAddr: "127.0.0.1:not-a-port", DataDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected an error for an unparseable raft address")
	}
}

func TestStartFailsWhenPortInUse(t *testing.T) {
	n := startCluster(t, 1)[0]

	_, err := Start(Config{NodeID: "dup", RaftAddr: n.cfg.RaftAddr, DataDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected an error when the raft port is already taken")
	}
}

func TestStartFailsWhenDataDirIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Start(Config{NodeID: "n", RaftAddr: freeAddr(t), DataDir: filepath.Join(file, "data")})
	if err == nil {
		t.Fatal("expected an error when the data dir can't be created")
	}
}

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	nodes := startCluster(t, 3)

	leaders := 0
	for _, tn := range nodes {
		if tn.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
	if got := len(nodes[0].Raft.GetConfiguration().Configuration().Servers); got != 3 {
		t.Fatalf("expected 3 voters in the cluster configuration, got %d", got)
	}
}

func TestAllNodesAgreeOnLeaderAddress(t *testing.T) {
	nodes := startCluster(t, 3)
	want := nodes[0].LeaderAddr()

	for _, tn := range nodes {
		if got := tn.LeaderAddr(); got != want {
			t.Fatalf("%s thinks the leader is %q, %s thinks %q", tn.cfg.NodeID, got, nodes[0].cfg.NodeID, want)
		}
	}
}

func TestWriteReplicatesToAllNodes(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)

	if err := leader.Set("color", "blue"); err != nil {
		t.Fatalf("set: %v", err)
	}
	for _, tn := range nodes {
		waitForValue(t, tn, "color", "blue")
	}
}

func TestDeleteReplicatesToAllNodes(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)

	if err := leader.Set("temp", "1"); err != nil {
		t.Fatalf("set: %v", err)
	}
	for _, tn := range nodes {
		waitForValue(t, tn, "temp", "1")
	}
	if err := leader.Delete("temp"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for _, tn := range nodes {
		tn := tn
		waitFor(t, 5*time.Second, tn.cfg.NodeID+" to drop the key", func() bool {
			_, err := tn.Store.Get("temp")
			return err == store.ErrKeyNotFound
		})
	}
}

// Every replica must apply writes in the same order, so after a burst of
// overlapping writes all nodes end with identical data.
func TestReplicasConvergeAfterManyWrites(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)

	for i := 0; i < 200; i++ {
		// Only 20 distinct keys, so most writes overwrite an earlier one --
		// any reordering between replicas would show up as a different value.
		if err := leader.Set(fmt.Sprintf("k%d", i%20), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	want := leader.Store.Snapshot()
	for _, tn := range followers(nodes, leader) {
		tn := tn
		waitFor(t, 5*time.Second, tn.cfg.NodeID+" to converge", func() bool {
			got := tn.Store.Snapshot()
			if len(got) != len(want) {
				return false
			}
			for k, v := range want {
				if got[k] != v {
					return false
				}
			}
			return true
		})
	}
}

// Raft only accepts writes on the leader. A follower must refuse rather than
// apply the write locally, or replicas would diverge.
func TestWriteOnFollowerIsRejected(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)
	follower := followers(nodes, leader)[0]

	err := follower.Set("k", "v")
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader from a follower, got %v", err)
	}
	if _, err := follower.Store.Get("k"); err != store.ErrKeyNotFound {
		t.Fatal("a rejected write must not appear in the follower's store")
	}
}

func TestJoinOnFollowerIsRejected(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)
	follower := followers(nodes, leader)[0]

	if err := follower.Join("node99", freeAddr(t)); err == nil {
		t.Fatal("expected a follower to reject a membership change")
	}
}

// TestLeaderFailoverNoDataLoss is the headline guarantee: while writes are
// in flight, the leader is killed. A new leader must be elected, and every
// write that was acknowledged as successful must survive on every remaining
// node. Writes that failed during the election are allowed -- clients were
// told they failed -- but an acknowledged write must never disappear.
func TestLeaderFailoverNoDataLoss(t *testing.T) {
	nodes := startCluster(t, 5)
	oldLeader := waitForLeader(t, nodes, 5*time.Second)

	var (
		mu    sync.Mutex
		acked = map[string]string{}
		stop  atomic.Bool
		wg    sync.WaitGroup
	)

	// 10 concurrent writers keep sending writes to whichever node is
	// currently the leader, for the whole test.
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				var target *testNode
				for _, tn := range nodes {
					if !tn.down.Load() && tn.IsLeader() {
						target = tn
						break
					}
				}
				if target == nil {
					time.Sleep(10 * time.Millisecond) // election in progress
					continue
				}
				key, val := fmt.Sprintf("w%d-%d", w, i), fmt.Sprintf("v%d", i)
				if err := target.Set(key, val); err == nil {
					mu.Lock()
					acked[key] = val
					mu.Unlock()
				}
			}
		}(w)
	}

	time.Sleep(500 * time.Millisecond) // let writes build up
	killedAt := time.Now()
	crash(t, oldLeader)

	newLeader := waitForLeader(t, nodes, 10*time.Second)
	electionTime := time.Since(killedAt)
	if newLeader == oldLeader {
		t.Fatal("the crashed node can't still be leader")
	}
	t.Logf("new leader %s elected %v after the old leader was killed", newLeader.cfg.NodeID, electionTime.Round(time.Millisecond))

	time.Sleep(500 * time.Millisecond) // keep writing to the new leader
	stop.Store(true)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) == 0 {
		t.Fatal("no writes were acknowledged; the test didn't exercise anything")
	}
	t.Logf("%d writes acknowledged across the failover", len(acked))

	for _, tn := range nodes {
		if tn.down.Load() {
			continue
		}
		tn := tn
		waitFor(t, 10*time.Second, tn.cfg.NodeID+" to hold every acknowledged write", func() bool {
			for k, v := range acked {
				if got, err := tn.Store.Get(k); err != nil || got != v {
					return false
				}
			}
			return true
		})
	}
}

// Raft needs a majority. With 2 of 3 nodes gone, the survivor can't commit
// anything -- the write must fail rather than be acknowledged unsafely.
func TestWriteFailsWithoutQuorum(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)
	for _, f := range followers(nodes, leader) {
		crash(t, f)
	}

	if err := leader.Set("k", "v"); err == nil {
		t.Fatal("a write must not succeed without a majority of nodes")
	}
}

// A 5-node cluster tolerates 2 failures: writes still commit with 3 nodes up.
func TestFiveNodeClusterToleratesTwoFailures(t *testing.T) {
	nodes := startCluster(t, 5)
	leader := waitForLeader(t, nodes, 5*time.Second)
	fs := followers(nodes, leader)
	crash(t, fs[0])
	crash(t, fs[1])

	if err := leader.Set("still", "works"); err != nil {
		t.Fatalf("write should succeed with 3 of 5 nodes up: %v", err)
	}
	for _, tn := range nodes {
		if !tn.down.Load() {
			waitForValue(t, tn, "still", "works")
		}
	}
}

// A follower that was down while writes happened must catch up when it
// comes back, using the log and data directory it persisted to disk.
func TestRestartedFollowerCatchesUp(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := waitForLeader(t, nodes, 5*time.Second)
	follower := followers(nodes, leader)[0]

	if err := leader.Set("before", "1"); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitForValue(t, follower, "before", "1")

	crash(t, follower)
	if err := leader.Set("during", "2"); err != nil {
		t.Fatalf("set while follower down: %v", err)
	}

	restart(t, follower)
	waitForValue(t, follower, "before", "1")
	waitForValue(t, follower, "during", "2")
}

// After a snapshot, a restarted node rebuilds its state from the snapshot
// file. Restarting with Bootstrap=true also checks that an already-
// initialised node treats ErrCantBootstrap as harmless instead of failing.
func TestRestartRestoresFromSnapshot(t *testing.T) {
	tn := startCluster(t, 1)[0]
	for i := 0; i < 10; i++ {
		if err := tn.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := tn.Raft.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	crash(t, tn)

	node, err := Start(tn.cfg) // cfg.Bootstrap is still true here
	if err != nil {
		t.Fatalf("restart with existing data should succeed, got %v", err)
	}
	tn.Node = node
	tn.down.Store(false)

	waitForLeader(t, []*testNode{tn}, 10*time.Second)
	for i := 0; i < 10; i++ {
		waitForValue(t, tn, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
}

func TestLeaderAddrEmptyWithoutCluster(t *testing.T) {
	// Not bootstrapped and never joined: this node has no cluster and so
	// no leader. The HTTP layer relies on "" here to return 503.
	n, err := Start(Config{NodeID: "lonely", RaftAddr: freeAddr(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.Shutdown()

	if n.IsLeader() || n.LeaderAddr() != "" {
		t.Fatal("an unjoined node should have no leader")
	}
}

// If the snapshot directory can't be created (here: a file is in the way),
// Start must fail loudly rather than run a node that can't snapshot.
func TestStartFailsWhenSnapshotStoreUnavailable(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snapshots"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(Config{NodeID: "n", RaftAddr: freeAddr(t), DataDir: dir}); err == nil {
		t.Fatal("expected an error when the snapshot store can't be created")
	}
}

// If the on-disk Raft log can't be opened (here: a directory sits where the
// BoltDB file should be), Start must fail rather than run without durability.
func TestStartFailsWhenLogStoreUnavailable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "raft.db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(Config{NodeID: "n", RaftAddr: freeAddr(t), DataDir: dir}); err == nil {
		t.Fatal("expected an error when the raft log file can't be opened")
	}
}

// Regression test: a failed Start used to leave its TCP port bound, so
// retrying on the same address failed with "address already in use".
func TestFailedStartReleasesPort(t *testing.T) {
	addr := freeAddr(t)
	badDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(badDir, "raft.db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(Config{NodeID: "n", RaftAddr: addr, DataDir: badDir}); err == nil {
		t.Fatal("expected the first Start to fail")
	}

	n, err := Start(Config{NodeID: "n", RaftAddr: addr, DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("retry on the same port should succeed, got %v", err)
	}
	n.Shutdown()
}
