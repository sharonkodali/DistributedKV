#!/usr/bin/env bash
# Starts a 3-node distributed-kv cluster on your local machine.
#
# node1 bootstraps a brand-new cluster; node2 and node3 join through it.
# Logs go to /tmp/kv-logs/*.log so you can watch each node's Raft state
# transitions (elections, heartbeats, etc.) while the cluster runs.
#
# NOTE: this builds a real binary first and runs THAT, rather than using
# `go run`. `go run` compiles a temp binary and launches it as a CHILD
# process of the `go run` wrapper -- killing the wrapper's PID does not
# reliably kill the child, which makes it impossible to cleanly simulate
# a node crashing. Running the compiled binary directly avoids that.
#
# Usage:
#   ./scripts/run-cluster.sh start   # start all 3 nodes in the background
#   ./scripts/run-cluster.sh stop    # stop all 3 nodes
#   ./scripts/run-cluster.sh clean   # stop + wipe all data/logs for a fresh start

set -euo pipefail

DATA_ROOT=/tmp/kv-data
LOG_ROOT=/tmp/kv-logs
PID_FILE=/tmp/kv-cluster.pids
BIN=/tmp/kv-server-bin

PEERS="127.0.0.1:9101=127.0.0.1:8081,127.0.0.1:9102=127.0.0.1:8082,127.0.0.1:9103=127.0.0.1:8083"

build() {
  echo "Building server binary..."
  go build -o "$BIN" ./cmd/server
}

start() {
  build
  mkdir -p "$DATA_ROOT" "$LOG_ROOT"
  rm -f "$PID_FILE"

  echo "Starting node1 (bootstrap)..."
  "$BIN" \
    -id node1 -http-addr :8081 -raft-addr 127.0.0.1:9101 \
    -data-dir "$DATA_ROOT/node1" -bootstrap \
    -peers "$PEERS" > "$LOG_ROOT/node1.log" 2>&1 &
  echo $! >> "$PID_FILE"

  sleep 2 # give node1 time to bootstrap and become leader

  echo "Starting node2 (joining via node1)..."
  "$BIN" \
    -id node2 -http-addr :8082 -raft-addr 127.0.0.1:9102 \
    -data-dir "$DATA_ROOT/node2" -join 127.0.0.1:8081 \
    -peers "$PEERS" > "$LOG_ROOT/node2.log" 2>&1 &
  echo $! >> "$PID_FILE"

  echo "Starting node3 (joining via node1)..."
  "$BIN" \
    -id node3 -http-addr :8083 -raft-addr 127.0.0.1:9103 \
    -data-dir "$DATA_ROOT/node3" -join 127.0.0.1:8081 \
    -peers "$PEERS" > "$LOG_ROOT/node3.log" 2>&1 &
  echo $! >> "$PID_FILE"

  sleep 2
  echo ""
  echo "Cluster started. HTTP APIs: node1=:8081 node2=:8082 node3=:8083"
  echo "Logs: $LOG_ROOT/node{1,2,3}.log"
  echo "PIDs (these are the REAL server processes now): $(cat "$PID_FILE" | tr '\n' ' ')"
  echo "Check status with: curl -s localhost:8081/status"
  echo "Kill the leader to test failover with: kill -9 <pid>"
  echo "Stop with: ./scripts/run-cluster.sh stop"
}

stop() {
  if [ -f "$PID_FILE" ]; then
    echo "Stopping cluster..."
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
    echo "Stopped."
  else
    echo "No running cluster found (no $PID_FILE)."
  fi
}

clean() {
  stop
  echo "Wiping data and logs..."
  rm -rf "$DATA_ROOT" "$LOG_ROOT"
  echo "Clean."
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  clean) clean ;;
  *)
    echo "Usage: $0 {start|stop|clean}"
    exit 1
    ;;
esac
