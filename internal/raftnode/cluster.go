package raftnode

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Join adds a new node as a voting member of the cluster. This must only
// be called against the current LEADER -- Raft rejects AddVoter calls made
// against a follower, since only the leader is allowed to propose changes
// to cluster membership (the same rule that applies to normal writes).
func (n *Node) Join(nodeID, raftAddr string) error {
	future := n.Raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, // prevIndex: 0 means "don't check, just add" -- fine for this project's scope
		10*time.Second,
	)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s (%s): %w", nodeID, raftAddr, err)
	}
	return nil
}

// IsLeader reports whether this node currently believes it is the leader.
// Note this can theoretically be stale by the time the caller acts on it
// (the leader could step down a moment later) -- Raft's Apply() call
// itself is the real source of truth and will return ErrNotLeader if this
// node loses leadership before the write completes.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// LeaderAddr returns the Raft transport address of the current leader, or
// an empty string if no leader is currently known (e.g. an election is
// in progress).
func (n *Node) LeaderAddr() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}
