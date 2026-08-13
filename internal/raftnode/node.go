// Package raftnode wires together everything a single cluster member needs
// to participate in Raft: the FSM (our store), the log/stable storage, the
// snapshot store, and the network transport used to talk to peer nodes.
//
// Nothing in here implements consensus itself -- that's entirely the
// hashicorp/raft library's job. This file's only responsibility is
// configuration: telling the library WHERE to store its data and HOW to
// reach other nodes.
package raftnode

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"distributed-kv/internal/fsm"
	"distributed-kv/internal/store"
)

// Config holds everything needed to start one Raft node.
type Config struct {
	// NodeID must be unique across the cluster (e.g. "node1", "node2").
	NodeID string

	// RaftAddr is the address this node's Raft transport listens on for
	// traffic FROM OTHER RAFT NODES (heartbeats, log replication, votes).
	// This is separate from the HTTP address clients use.
	RaftAddr string

	// DataDir is where Raft persists its log, stable store, and snapshots
	// to disk, so a restarted node doesn't lose its history.
	DataDir string

	// Bootstrap should be true ONLY the very first time a brand-new
	// cluster starts, and only passed for the node that kicks things off.
	// It tells Raft "there is no existing cluster -- start a fresh one
	// with just me as the initial member." Never set this to true when
	// rejoining an already-running cluster, or you risk creating a second,
	// conflicting cluster (a real split-brain).
	Bootstrap bool
}

// Node bundles a running Raft instance with the FSM/store it manages.
type Node struct {
	Raft  *raft.Raft
	FSM   *fsm.FSM
	Store *store.Store
}

// Start creates and starts a Raft node according to cfg. It does not join
// or bootstrap a cluster by itself beyond the optional single-node
// bootstrap described above -- adding peers happens separately via
// AddVoter (see cluster.go).
func Start(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	// Transport is how this node sends/receives Raft protocol messages
	// (AppendEntries, RequestVote, etc.) to and from its peers over TCP.
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	// Snapshot store: where periodic full-state snapshots are written, so
	// a restarted node can catch up fast instead of replaying every log
	// entry since the beginning of time.
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Log store + stable store: BoltDB-backed, on-disk storage for the
	// replicated log itself and small pieces of Raft's persistent state
	// (like "who did I vote for in the last election").
	boltPath := filepath.Join(cfg.DataDir, "raft.db")
	boltStore, err := raftboltdb.New(raftboltdb.Options{Path: boltPath})
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}

	kvStore := store.New()
	f := fsm.New(kvStore)

	r, err := raft.NewRaft(raftConfig, f, boltStore, boltStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}

	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		// BootstrapCluster is a no-op (returns an error we can safely
		// ignore) if this node already has log entries -- e.g. on a
		// restart. It should only actually take effect the very first
		// time a cluster is created.
		r.BootstrapCluster(configuration)
	}

	return &Node{Raft: r, FSM: f, Store: kvStore}, nil
}
