# distributed-kv

A distributed key-value store built to learn (and demonstrate) core
distributed systems concepts: replication, leader election (Raft), and
failure handling.

## Status: Day 1 complete

A single-node HTTP key-value server. No replication yet — this is the
foundation the distributed parts get built on top of.

## Run it

```bash
# start the server
go run ./cmd/server -addr :8080

# in another terminal, use the CLI
go run ./cmd/cli set username John
go run ./cmd/cli get username
go run ./cmd/cli delete username
go run ./cmd/cli status
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
cmd/server/    -- the HTTP server (entry point: main.go)
cmd/cli/       -- CLI client for talking to the server
internal/store/-- the actual key-value store (thread-safe in-memory map)
```

`store` is deliberately isolated from networking. Later, when Raft is
added, the Raft layer will call into store's methods as its "state
machine" -- this separation is what makes that possible without a rewrite.

## Tests

```bash
go test -race ./...
```

## Roadmap

- [x] Day 1-2: single-node server + CLI
- [ ] Day 3-4: wrap store as a Raft state machine, elect a leader across 3 nodes
- [ ] Day 5-7: replicate writes through Raft, test leader-crash recovery
- [ ] Day 8-10: Docker Compose for 5 nodes, /status shows leader info
- [ ] Day 11-14: buffer + simulate network partitions
