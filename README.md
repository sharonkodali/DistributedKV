# distributed-kv

A distributed key-value store built to learn (and demonstrate) core
distributed systems concepts: replication, leader election (Raft), and
failure handling.

## Status: Day 1 complete

A single-node HTTP key-value server. No replication yet — this is the
foundation the distributed parts get built on top of.

## Run it

**Single node (Day 1 style, no Raft):**
```bash
go run ./cmd/server -addr :8080
go run ./cmd/cli set username John
go run ./cmd/cli get username
```

**3-node Raft cluster (Day 3-4):**
```bash
chmod +x scripts/run-cluster.sh
./scripts/run-cluster.sh start

# check who the leader is
curl -s localhost:8081/status

# write through any node -- it'll auto-forward to the leader if needed
go run ./cmd/cli -addr localhost:8082 set username John
go run ./cmd/cli -addr localhost:8081 get username

./scripts/run-cluster.sh stop
./scripts/run-cluster.sh clean   # stop + wipe data for a fresh start
```

**Test leader failover:**
```bash
./scripts/run-cluster.sh start
curl -s localhost:8081/status          # note who's leader
# find and kill that node's process (check /tmp/kv-cluster.pids or `ps aux | grep kv-server`)
sleep 3
curl -s localhost:8082/status          # a new leader should now be elected
```

## API

| Method | Path        | Description                          |
|--------|-------------|---------------------------------------|
| GET    | /kv/{key}   | Fetch a value. 404 if not found.      |
| PUT    | /kv/{key}   | Set a value (body = raw value).       |
| DELETE | /kv/{key}   | Delete a key. Safe if key is missing. |
| GET    | /status     | Basic node info (key count, etc).     |

## Project layout

```
cmd/server/       -- cluster-aware HTTP + Raft node (entry point: main.go)
cmd/cli/          -- CLI client for talking to any node
internal/store/   -- the actual key-value store (thread-safe in-memory map)
internal/fsm/     -- bridges Raft and the store (implements Apply/Snapshot/Restore)
internal/raftnode/-- starts/configures a Raft instance, handles joins and writes
scripts/          -- run-cluster.sh: start/stop/clean a local 3-node cluster
```

`store` knows nothing about Raft. `fsm` translates committed Raft log
entries into calls on `store`. `raftnode` owns the actual `*raft.Raft`
instance and exposes `Set`/`Delete`/`Join` as simple Go methods. This
layering is what let Raft get added without rewriting the store or CLI.

## A note on dependency resolution

This project depends on `github.com/hashicorp/raft`, which has some
legacy transitive dependencies. On a normal machine with unrestricted
internet, `go mod tidy` resolves this in seconds -- this is one of the
most widely used Go libraries for distributed consensus. If you're
working in a network-restricted environment and `go mod tidy` fails on a
host like `gopkg.in` or `google.golang.org`, that's an environment
network policy issue, not a problem with the code.

## Tests

```bash
go test -race ./...
```

## Roadmap

- [x] Day 1-2: single-node server + CLI
- [x] Day 3-4: wrap store as a Raft state machine, elect a leader across 3 nodes
- [ ] Day 5-7: (mostly done above) test leader-crash recovery thoroughly, add more failure-mode tests
- [ ] Day 8-10: Docker Compose for 5 nodes, polish /status
- [ ] Day 11-14: buffer + simulate network partitions
