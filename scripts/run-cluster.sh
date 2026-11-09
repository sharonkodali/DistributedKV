#!/usr/bin/env bash
# Starts a distributed-kv cluster on your local machine (3 nodes by
# default; set NODES=5 for a 5-node cluster).
#
# node1 bootstraps a brand-new cluster; the others join through it.
# Logs go to /tmp/kv-logs/*.log so you can watch each node's Raft state
# transitions (elections, heartbeats, etc.) while the cluster runs.
#
# NOTE: this builds a real binary first and runs THAT, rather than using
# `go run`. `go run` compiles a temp binary and launches it as a CHILD
# process of the `go run` wrapper -- killing the wrapper's PID does not
# reliably kill the child, which makes it impossible to cleanly simulate
# a node crashing. Running the compiled binary directly avoids that.
#
# Node i listens on HTTP port 808i and Raft port 910i. The PID file lists
# PIDs in node order (line 1 = node1), which scripts/failover-test.sh uses
# to kill whichever node is the leader.
#
# Usage:
#   ./scripts/run-cluster.sh start          # start 3 nodes in the background
#   NODES=5 ./scripts/run-cluster.sh start  # start 5 nodes
#   ./scripts/run-cluster.sh stop           # stop all nodes
#   ./scripts/run-cluster.sh clean          # stop + wipe all data/logs for a fresh start

set -euo pipefail

NODES=${NODES:-3}
DATA_ROOT=/tmp/kv-data
LOG_ROOT=/tmp/kv-logs
PID_FILE=/tmp/kv-cluster.pids
BIN=/tmp/kv-server-bin

if [ "$NODES" -lt 1 ] || [ "$NODES" -gt 9 ]; then
  echo "NODES must be between 1 and 9 (got $NODES)"
  exit 1
fi

PEERS=""
for i in $(seq 1 "$NODES"); do
  PEERS="${PEERS:+$PEERS,}127.0.0.1:910$i=127.0.0.1:808$i"
done

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

  for i in $(seq 2 "$NODES"); do
    echo "Starting node$i (joining via node1)..."
    "$BIN" \
      -id "node$i" -http-addr ":808$i" -raft-addr "127.0.0.1:910$i" \
      -data-dir "$DATA_ROOT/node$i" -join 127.0.0.1:8081 \
      -peers "$PEERS" > "$LOG_ROOT/node$i.log" 2>&1 &
    echo $! >> "$PID_FILE"
  done

  sleep 2
  echo ""
  echo "Cluster of $NODES started. HTTP APIs on ports 8081-808$NODES"
  echo "Logs: $LOG_ROOT/node*.log"
  echo "PIDs (these are the REAL server processes now): $(tr '\n' ' ' < "$PID_FILE")"
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
