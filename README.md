# distributed-kv

A distributed key-value store implementing the Raft consensus algorithm
for leader election, log replication, and automatic failover — built to
understand (and demonstrate) how systems like etcd, CockroachDB, and
Kubernetes' control plane stay correct across multiple machines even when
individual nodes crash.

Runs as a 5-node cluster in Docker, with one node acting as leader and
the rest as replicated followers. Writes are only accepted once a
majority of nodes have durably committed them; if the leader dies, the
remaining nodes automatically elect a new one within seconds, with zero
data loss and zero manual intervention.

## What it demonstrates

- **Leader election** via the Raft consensus algorithm (using
  `hashicorp/raft`, a production-grade implementation used by etcd and
  Consul)
- **Log replication** with majority-commit semantics — a write only
  succeeds once it's durably stored on more than half the cluster
- **Automatic failover** — killing the leader process triggers a new
  election with no manual intervention, verified with zero data loss
- **Transparent write forwarding** — clients can write to any node; a
  follower silently proxies the request to the current leader
- **Containerized deployment** — a 5-node cluster via Docker Compose,
  each node in its own container, communicating over a real network

## Quick demo

```bash
docker compose up --build

# in another terminal
curl -s localhost:8081/status
go run ./cmd/cli -addr localhost:8082 set username John
go run ./cmd/cli -addr localhost:8081 get username

# kill the leader and watch a new one take over
docker stop kv-node1
sleep 5
curl -s localhost:8082/status   # a different node is now leader
go run ./cmd/cli -addr localhost:8082 get username   # data survived
```

## Architecture

```
cmd/server/        -- cluster-aware HTTP + Raft node (entry point)
cmd/cli/            -- CLI client for talking to any node
internal/store/     -- thread-safe in-memory key-value store
internal/fsm/        -- bridges Raft and the store (Apply/Snapshot/Restore)
internal/raftnode/   -- configures and runs the Raft instance, handles joins/writes
scripts/run-cluster.sh -- local 3-node cluster without Docker, for quick iteration
Dockerfile           -- multi-stage build producing a minimal server image
docker-compose.yml    -- 5-node cluster, one container per node
```

The storage layer (`store`) has no knowledge of Raft. `fsm` translates
committed Raft log entries into calls on `store`. `raftnode` owns the
actual `*raft.Raft` instance and exposes simple `Set`/`Delete`/`Join`
methods. This separation is what let consensus get added on top of the
storage layer without rewriting it — the same pattern used in real
systems like etcd.

## How a write actually flows

1. A client sends `PUT /kv/{key}` to any node.
2. If that node isn't the leader, it transparently proxies the request
   to whichever node is (clients never need to track leadership
   themselves).
3. The leader appends the write to its local Raft log and replicates it
   to all followers.
4. Once a **majority** of nodes have durably stored the entry, it's
   considered committed — only then is it applied to the actual
   key-value store and the client gets a success response.

This majority-commit rule is what makes the system tolerate node
failures without losing data: as long as a majority of nodes are up, at
least one of them has every committed write.

## Two real bugs found and fixed along the way

- **IP vs. hostname mismatch**: Raft reports its own network address in
  resolved-IP form (`172.19.0.2:9001`), never as the original hostname
  (`node1:9001`) it was configured with — because Go's networking types
  can only represent IPs. The write-forwarding logic originally matched
  on hostnames and silently failed against real leader addresses in
  Docker. Fixed by resolving every peer's hostname to its IP once at
  startup, so lookups match what Raft actually reports at runtime.
- **Docker Compose startup race**: with 5 containers starting near-
  simultaneously, a node could try to resolve a peer's hostname before
  that peer's container had registered with Docker's internal DNS.
  Fixed with a retry loop around DNS resolution instead of failing on
  the first miss.

## Tests

```bash
go test -race ./...
```

## Possible extensions

- Simulate network partitions (not just node crashes) and verify quorum
  behavior
- Add Prometheus metrics (replication lag, leader changes over time)
- Snapshot-based log compaction tuning for large datasets
