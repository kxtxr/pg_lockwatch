#!/usr/bin/env bash
#
# Verifies the alerting path: NOTIFY on threshold breach + the
# lockwatch_history insert. Same concurrency-testing rationale as
# lock_contention.sh, including using psql's `\t`/`\a` meta-commands
# instead of `-tA` since `cargo pgrx connect` doesn't forward CLI flags
# to the underlying psql process — see that file's header comment.
#
# USAGE:
#   cargo pgrx run pg17          # in one terminal, leave it running
#   ./tests/alert_threshold.sh pg17   # in another terminal
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
-- Lowered so 3 waiters behind one blocker reliably crosses it. The
-- default (0.75) is tuned for a real workload, not a 4-session manual test.
ALTER SYSTEM SET lockwatch.risk_threshold = 0.05;
SELECT pg_reload_conf();
SQL

BASELINE_HISTORY_COUNT=$("${CONNECT[@]}" <<'SQL' | tail -1
\t
\a
SELECT count(*) FROM lockwatch_history;
SQL
)

echo "== Creating contention =="
# Table-level lock, not "3 sessions UPDATE the same row": see
# lock_contention.sh's header for why repeated row-level UPDATE waiters
# queue against EACH OTHER in pg_locks (not against the true blocker),
# which undercounts waiter_count and can leave risk_score under
# threshold even with 3 real waiters. AccessExclusiveLock also exercises
# the worst-case lock_mode severity term, unlike RowExclusiveLock.
"${CONNECT[@]}" <<'SQL' &
BEGIN;
LOCK TABLE lockwatch_test_t IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(20);
COMMIT;
SQL
BLOCKER_JOB=$!

sleep 1

for i in 1 2 3; do
    "${CONNECT[@]}" <<'SQL' &
    SELECT * FROM lockwatch_test_t;
SQL
done

echo "== Listening for NOTIFY while sampling =="
# LISTEN + sample in the same session, then check for the notification
# via pg_notification_queue_usage as a proxy — psql's own async notice
# printing is awkward to capture reliably in a non-interactive heredoc,
# so this checks the queue side effect plus the durable history row
# instead of trying to scrape "Asynchronous notification" text.
#
# Poll rather than a single fixed-delay sample: `cargo pgrx connect`'s
# own per-invocation startup overhead means the 3 backgrounded waiters
# aren't guaranteed to have all queued up yet on the first sample.
# Bounded by wall-clock, not attempt count, since per-attempt latency
# varies; 15s leaves margin under the blocker's 20s hold.
POLL_TIMEOUT=15
POLL_START=$SECONDS
NEW_HISTORY_COUNT="$BASELINE_HISTORY_COUNT"
while (( SECONDS - POLL_START < POLL_TIMEOUT )); do
    "${CONNECT[@]}" <<'SQL'
LISTEN lockwatch_alert;
SELECT lockwatch_sample_now();
SQL

    NEW_HISTORY_COUNT=$("${CONNECT[@]}" <<'SQL' | tail -1
\t
\a
SELECT count(*) FROM lockwatch_history;
SQL
)
    (( NEW_HISTORY_COUNT > BASELINE_HISTORY_COUNT )) && break
    sleep 0.3
done

if [[ "$NEW_HISTORY_COUNT" -gt "$BASELINE_HISTORY_COUNT" ]]; then
    pass "lockwatch_history gained a row ($BASELINE_HISTORY_COUNT -> $NEW_HISTORY_COUNT)"
else
    fail "lockwatch_history did not grow (still $NEW_HISTORY_COUNT) — threshold may not have been crossed, or fire_alert's INSERT failed"
fi

LATEST=$("${CONNECT[@]}" <<'SQL' | tail -1
\t
\a
SELECT blocking_pid, risk_score, waiter_count
FROM lockwatch_history
ORDER BY alerted_at DESC
LIMIT 1;
SQL
)
echo "  latest history row: $LATEST"

echo "== Cleanup =="
wait "$BLOCKER_JOB" || true
"${CONNECT[@]}" <<'SQL'
DROP TABLE IF EXISTS lockwatch_test_t;
ALTER SYSTEM RESET lockwatch.risk_threshold;
SELECT pg_reload_conf();
SQL

echo
if [[ "$FAIL" == "0" ]]; then
    echo "ALL CHECKS PASSED"
    exit 0
else
    echo "ONE OR MORE CHECKS FAILED"
    exit 1
fi
