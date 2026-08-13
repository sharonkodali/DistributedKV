package raftnode

import (
	"encoding/json"
	"fmt"
	"time"

	"distributed-kv/internal/fsm"
)

// applyTimeout bounds how long we wait for a write to be committed by a
// majority of the cluster before giving up and reporting an error to the
// client. In a healthy cluster this completes in milliseconds; a longer
// wait usually means the cluster has no leader or is partitioned.
const applyTimeout = 5 * time.Second

// Set replicates a SET command through Raft. It only returns successfully
// once a MAJORITY of the cluster has durably stored the entry -- that's
// what "committed" means, and it's the guarantee that makes this safe
// even if this node crashes immediately afterward.
func (n *Node) Set(key, value string) error {
	return n.apply(fsm.Command{Type: fsm.CommandSet, Key: key, Value: value})
}

// Delete replicates a DELETE command through Raft, with the same
// majority-commit guarantee as Set.
func (n *Node) Delete(key string) error {
	return n.apply(fsm.Command{Type: fsm.CommandDelete, Key: key})
}

func (n *Node) apply(cmd fsm.Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Apply() only succeeds on the leader. If this node isn't the leader,
	// it returns raft.ErrNotLeader -- callers (the HTTP layer) are
	// responsible for checking IsLeader() first and forwarding elsewhere,
	// but this check here is a correctness backstop either way.
	future := n.Raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// future.Response() returns whatever FSM.Apply() returned -- in our
	// case that's either nil or an error (e.g. "unknown command type").
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok && err != nil {
			return fmt.Errorf("fsm apply failed: %w", err)
		}
	}

	return nil
}
