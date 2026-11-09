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
  `hashicorp/raft`, the production-grade implementation behind Consul,
  Nomad and Vault)
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
cmd/loadtest/       -- concurrent load generator + "no acknowledged write lost" verifier
internal/store/     -- thread-safe in-memory key-value store
internal/fsm/        -- bridges Raft and the store (Apply/Snapshot/Restore)
internal/raftnode/   -- configures and runs the Raft instance, handles joins/writes
scripts/run-cluster.sh -- local cluster without Docker (NODES=3 by default)
scripts/benchmark.sh   -- throughput/latency benchmark against a fresh local cluster
scripts/failover-test.sh -- kills the leader mid-load, times re-election, verifies no data loss
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
go test -race ./...                         # ~108 tests, about 80s
go test -race -cover ./...                  # per-package coverage
```

Every test runs under Go's race detector. Coverage is 89% of statements
overall (store 100%, fsm 100%, cli 98.5%, server 93%, loadtest 82%,
raftnode 80%).

What's covered:

- **store**: CRUD, snapshot/restore independence, and concurrent use of
  every method at once under `-race`.
- **fsm**: applying set/delete/malformed/unknown log entries, determinism
  (two replicas replaying the same log end up identical), snapshot
  persist/restore, and the snapshot-write failure path.
- **raftnode**: real multi-node Raft clusters on localhost, covering:
  - leader election and log replication
  - followers rejecting writes and membership changes
  - **killing the leader while 10 clients are writing and checking that every
    acknowledged write survives on every remaining node**
  - writes refused without a majority (quorum)
  - a 5-node cluster surviving 2 failures
  - a crashed follower catching up after restart
  - recovery from a snapshot
- **server**: the HTTP API end to end (status codes, JSON, raw values),
  forwarding writes from a follower to the leader, and the error paths: no
  leader (503), unknown or unreachable leader (502), lost quorum, joining
  over HTTP, and flag handling.
- **cli**: every command against a fake server, argument validation, and
  unreachable-server errors.

### Bugs the tests found

- `store.Restore(nil)` left a nil map behind, so the next write panicked
  (`assignment to entry in nil map`). A snapshot that decodes from JSON
  `null` would trigger it. `Restore` also kept the caller's map instead of
  copying it, so outside code could mutate the store without the lock.
- A failed `raftnode.Start` (e.g. the log file couldn't be opened) left its
  TCP port bound, so retrying on the same address failed with "address
  already in use".

Each fix has a regression test that fails on the old code.

## Load testing and failover

`cmd/loadtest` runs N concurrent clients for a fixed time and prints
throughput plus p50/p95/p99 latency. Afterwards it reads every
**acknowledged** write back from every live node and exits non-zero if any
are missing.

```bash
./scripts/benchmark.sh                     # 5 nodes, 50 clients, 10s per scenario
NODES=3 CLIENTS=200 ./scripts/benchmark.sh
./scripts/failover-test.sh                 # 5 nodes, 50 clients, kill -9 the leader at t=5s
```

Results on an Apple M5 laptop (10 cores), 5 local nodes, 64-byte values:

| Scenario | Clients | Throughput | p50 latency | p99 latency |
|---|---|---|---|---|
| Writes, any node (followers forward) | 50 | ~640/s | 77 ms | 116 ms |
| Writes, leader only | 50 | ~690/s | 70 ms | 150 ms |
| Writes, any node | 200 | ~2,000/s | 98 ms | 155 ms |
| Writes, any node | 500 | ~3,200/s | 149 ms | 254 ms |
| Reads, all nodes (local reads) | 50 | ~82,600/s | 0.5 ms | 2.4 ms |

Every write run verified zero acknowledged writes lost. Write throughput
grows with concurrency because Raft batches concurrent entries into one
replication round and one disk sync, so each client waits about the same
time while the batch gets bigger. Reads are served from each node's local
copy, so they skip consensus entirely (see the consistency note in
`cmd/server/main.go`).

**Failover** (`scripts/failover-test.sh`, 5 nodes, 50 writing clients,
leader killed with `kill -9` mid-write): a new leader was elected in
1.5–2.3s across runs, and every acknowledged write was present on all 4
surviving nodes. The load test counts thousands of failed requests in
this run. That's expected: about a fifth of requests target the dead
node, and writes sent during the election are refused. A refused write was
never acknowledged, so it doesn't count as lost.

## Possible extensions

- Simulate network partitions (not just node crashes) and verify quorum
  behavior
- Add Prometheus metrics (replication lag, leader changes over time)
- Snapshot-based log compaction tuning for large datasets
