#!/usr/bin/env bash
# Kills the Raft leader in the middle of a write-heavy load test, then
# reports how long re-election took and whether any acknowledged write was
# lost.
#
# Steps:
#   1. start a fresh local cluster (5 nodes by default)
#   2. run cmd/loadtest with concurrent clients writing for DURATION seconds
#   3. after KILL_AFTER seconds, find the leader and kill -9 its process
#   4. time how long until another node reports itself leader
#   5. when the load test finishes, it reads every acknowledged write back
#      from every surviving node (cmd/loadtest -verify)
#
# Writes sent during the election may fail -- those clients were told
# "error", which is correct. The guarantee being tested is that a write the
# cluster said "OK" to is never lost.
#
# Usage:
#   ./scripts/failover-test.sh
#   NODES=3 CLIENTS=20 DURATION=20 KILL_AFTER=8 ./scripts/failover-test.sh
#
# Exit code is 0 only if a new leader was elected AND verification passed.

set -uo pipefail

NODES=${NODES:-5}
CLIENTS=${CLIENTS:-50}
DURATION=${DURATION:-15}
KILL_AFTER=${KILL_AFTER:-5}
PID_FILE=/tmp/kv-cluster.pids
LOADTEST_BIN=/tmp/kv-loadtest-bin
LOADTEST_OUT=/tmp/kv-logs/loadtest.out

cd "$(dirname "$0")/.."

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

is_leader() { curl -s -m 0.5 "localhost:808$1/status" 2>/dev/null | grep -q '"is_leader":true'; }

trap 'NODES=$NODES ./scripts/run-cluster.sh stop >/dev/null 2>&1' EXIT

NODES=$NODES ./scripts/run-cluster.sh clean >/dev/null
NODES=$NODES ./scripts/run-cluster.sh start || exit 1
go build -o "$LOADTEST_BIN" ./cmd/loadtest || exit 1

ADDRS=""
for i in $(seq 1 "$NODES"); do ADDRS="${ADDRS:+$ADDRS,}localhost:808$i"; done

echo ""
echo "Running load: $CLIENTS clients writing for ${DURATION}s across $NODES nodes..."
"$LOADTEST_BIN" -addrs "$ADDRS" -clients "$CLIENTS" -duration "${DURATION}s" -mode write > "$LOADTEST_OUT" 2>&1 &
LOAD_PID=$!

sleep "$KILL_AFTER"

LEADER=""
for i in $(seq 1 "$NODES"); do
  if is_leader "$i"; then LEADER=$i; break; fi
done
if [ -z "$LEADER" ]; then
  echo "FAIL: no leader found before the kill"
  exit 1
fi

LEADER_PID=$(sed -n "${LEADER}p" "$PID_FILE")
echo "Killing leader node$LEADER (pid $LEADER_PID) with kill -9, mid-write..."
KILLED_AT=$(now_ms)
kill -9 "$LEADER_PID"

NEW_LEADER=""
DEADLINE=$(( KILLED_AT + 30000 ))
while [ -z "$NEW_LEADER" ] && [ "$(now_ms)" -lt "$DEADLINE" ]; do
  for i in $(seq 1 "$NODES"); do
    [ "$i" = "$LEADER" ] && continue
    if is_leader "$i"; then NEW_LEADER=$i; break; fi
  done
  [ -z "$NEW_LEADER" ] && sleep 0.05
done

if [ -z "$NEW_LEADER" ]; then
  echo "FAIL: no new leader elected within 30s"
  wait "$LOAD_PID"
  exit 1
fi
ELECTED_AT=$(now_ms)
echo "New leader: node$NEW_LEADER, elected $(( ELECTED_AT - KILLED_AT ))ms after the kill"

echo "Waiting for the load test to finish and verify..."
wait "$LOAD_PID"
LOAD_RC=$?

echo ""
cat "$LOADTEST_OUT"
echo ""
if [ "$LOAD_RC" -eq 0 ]; then
  echo "RESULT: PASS - re-election in $(( ELECTED_AT - KILLED_AT ))ms, no acknowledged writes lost"
else
  echo "RESULT: FAIL - see verification output above"
fi
exit "$LOAD_RC"
