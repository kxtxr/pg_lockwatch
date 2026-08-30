#!/usr/bin/env bash
#
# Drives a real multi-session lock-contention scenario against a running
# pgrx dev instance and asserts on pg_lockwatch's output.
#
# This is a shell script rather than a `cargo pgrx test` or
# `cargo pgrx regress` test because those run one psql session per test.
# This scenario needs one session to hold a lock while others genuinely
# block behind it. The script drives real concurrent psql processes.
#
# CONFIRMED (cargo-pgrx 0.19.2): `cargo pgrx connect <pgver>` execs psql
# and forwards heredoc stdin exactly like a plain `psql` call. What it
# does NOT do is forward extra CLI flags (e.g. `-tA`) to that psql
# process. It is a strict clap parser and errors on anything it does not
# recognize. Tuples-only/unaligned output is requested with psql's
# own `\t` / `\a` meta-commands inside the heredoc instead of `-tA`.
#
# USAGE:
#   cargo pgrx run pg17          # in one terminal, leave it running
#   ./tests/lock_contention.sh pg17   # in another terminal
#
set -euo pipefail

PGVER="${1:-pg17}"
CONNECT=(cargo pgrx connect "$PGVER")
FAIL=0

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAIL=1; }

echo "== Setup =="
"${CONNECT[@]}" <<'SQL'
DROP EXTENSION IF EXISTS pg_lockwatch;
DROP TABLE IF EXISTS lockwatch_test_t;
CREATE EXTENSION pg_lockwatch;
CREATE TABLE lockwatch_test_t (id int primary key, val int);
INSERT INTO lockwatch_test_t VALUES (1, 100);
SQL
echo "  extension and test table created"

echo "== Scenario: 1 blocker (AccessExclusiveLock), 3 waiters =="

# Use a table-level lock so all waiters queue behind the same lock object.
# Row-level UPDATE contention can form a transactionid wait chain, which is
# useful for graph traversal but less direct for this waiter-count assertion.
# AccessExclusiveLock also exercises the highest lock-mode severity.

# Session A: hold the table with the most severe lock mode via an open
# transaction. The 20s hold gives `cargo pgrx connect` enough time to spawn a
# fresh cargo process and backend per call (~0.3-0.5s each, observed),
# and 3 waiters plus a polling sampler below all pay that cost against
# this same window.
"${CONNECT[@]}" <<'SQL' &
BEGIN;
LOCK TABLE lockwatch_test_t IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(20);
COMMIT;
SQL
BLOCKER_JOB=$!

# Give the blocker time to actually acquire the lock before the waiters
# race it.
sleep 1

# Sessions B, C, D: each blocks immediately behind the table lock. A
# plain SELECT takes AccessShareLock, which conflicts with the blocker's
# AccessExclusiveLock.
WAITER_JOBS=()
for i in 1 2 3; do
    "${CONNECT[@]}" <<'SQL' &
    SELECT * FROM lockwatch_test_t;
SQL
    WAITER_JOBS+=($!)
done

echo "== Sampling while contention is live =="
# cargo pgrx connect is a strict CLI parser -- it does not forward psql
# flags like -tA to the underlying psql process, so tuples-only/unaligned
# output is requested via psql's own meta-commands in the script instead.
#
# Poll rather than sleep-and-hope: `cargo pgrx connect` itself has real
# per-invocation startup overhead, so 3 backgrounded waiters aren't
# guaranteed to have all reached the lock wait within any single fixed
# sleep. This was observed to flake with waiter_count == 2, then later 0
# once the blocker's short hold expired mid-poll) with a flat sleep.
# Bounded by wall-clock, not attempt count, since per-attempt latency
# varies; 15s leaves margin under the blocker's 20s hold.
POLL_TIMEOUT=15
POLL_START=$SECONDS
RESULT=""
WAITER_COUNT=0
while (( SECONDS - POLL_START < POLL_TIMEOUT )); do
    RESULT=$("${CONNECT[@]}" <<'SQL'
\t
\a
SELECT lockwatch_sample_now();
SELECT waiter_count, relation_name, risk_score > 0 AS scored
FROM lockwatch_risks
LIMIT 1;
SQL
)
    WAITER_COUNT=$(echo "$RESULT" | tail -1 | cut -d'|' -f1)
    [[ "$WAITER_COUNT" == "3" ]] && break
    sleep 0.3
done
echo "  raw output (after $((SECONDS - POLL_START))s polling): $RESULT"

RELATION=$(echo "$RESULT" | tail -1 | cut -d'|' -f2)
SCORED=$(echo "$RESULT" | tail -1 | cut -d'|' -f3)

[[ "$WAITER_COUNT" == "3" ]] && pass "waiter_count == 3" || fail "waiter_count was '$WAITER_COUNT', expected 3"
[[ "$RELATION" == "lockwatch_test_t" ]] && pass "relation_name resolved correctly" || fail "relation_name was '$RELATION'"
[[ "$SCORED" == "t" ]] && pass "risk_score > 0" || fail "risk_score was not positive"

echo "== Waiting for blocker to commit and waiters to drain =="
wait "$BLOCKER_JOB" || true
for j in "${WAITER_JOBS[@]}"; do wait "$j" || true; done

echo "== Sampling after contention clears (eviction check) =="
sleep 0.5
AFTER=$("${CONNECT[@]}" <<'SQL'
\t
\a
SELECT lockwatch_sample_now();
SELECT count(*) FROM lockwatch_risks;
SQL
)
AFTER_COUNT=$(echo "$AFTER" | tail -1)
[[ "$AFTER_COUNT" == "0" ]] && pass "stale blocker evicted after commit" || fail "expected 0 rows after commit, got $AFTER_COUNT"

echo "== Cleanup =="
"${CONNECT[@]}" <<'SQL'
DROP TABLE IF EXISTS lockwatch_test_t;
SQL

echo
if [[ "$FAIL" == "0" ]]; then
    echo "ALL CHECKS PASSED"
    exit 0
else
    echo "ONE OR MORE CHECKS FAILED"
    exit 1
fi
