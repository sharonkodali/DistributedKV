#!/usr/bin/env bash
# Starts a fresh local cluster and measures write and read performance with
# cmd/loadtest, then stops the cluster.
#
# Runs three scenarios:
#   - writes to any node (followers forward to the leader: an extra hop)
#   - writes straight to the leader
#   - reads spread across all nodes (served from each node's local copy)
#
# Usage:
#   ./scripts/benchmark.sh
#   NODES=3 CLIENTS=20 DURATION=5 ./scripts/benchmark.sh
#
# Numbers depend heavily on the machine (CPU, disk fsync speed), so record
# the hardware alongside any result you quote.

set -euo pipefail

NODES=${NODES:-5}
CLIENTS=${CLIENTS:-50}
DURATION=${DURATION:-10}
LOADTEST_BIN=/tmp/kv-loadtest-bin

cd "$(dirname "$0")/.."
trap 'NODES=$NODES ./scripts/run-cluster.sh stop >/dev/null 2>&1' EXIT

NODES=$NODES ./scripts/run-cluster.sh clean >/dev/null
NODES=$NODES ./scripts/run-cluster.sh start >/dev/null
go build -o "$LOADTEST_BIN" ./cmd/loadtest

ADDRS=""
for i in $(seq 1 "$NODES"); do ADDRS="${ADDRS:+$ADDRS,}localhost:808$i"; done

echo "== writes, any node (with forwarding) =="
"$LOADTEST_BIN" -addrs "$ADDRS" -clients "$CLIENTS" -duration "${DURATION}s" -mode write -target any
echo ""
echo "== writes, leader only =="
"$LOADTEST_BIN" -addrs "$ADDRS" -clients "$CLIENTS" -duration "${DURATION}s" -mode write -target leader
echo ""
echo "== reads, all nodes =="
"$LOADTEST_BIN" -addrs "$ADDRS" -clients "$CLIENTS" -duration "${DURATION}s" -mode read
