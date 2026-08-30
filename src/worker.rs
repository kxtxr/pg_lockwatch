use crate::shmem::{self, LockMode};
use crate::{RISK_THRESHOLD, SAMPLE_INTERVAL_MS};
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Relation and tuple locks currently held by each backend.
///
/// This is separate from the wait graph. In a concurrent UPDATE chain, a root
/// blocker may be waited on through a transactionid lock while its relation
/// lock is self-compatible. Reading held locks by pid still gives useful
/// relation metadata for that root blocker.
const HELD_LOCKS_QUERY: &str = r#"
    SELECT
        l.pid                AS pid,
        l.relation            AS relation_oid,
        l.mode                AS lock_mode,
        act.query_start       AS held_since,
        act.query             AS blocking_query
    FROM pg_locks l
    JOIN pg_stat_activity act ON act.pid = l.pid
    WHERE l.granted AND l.locktype IN ('relation', 'tuple');
"#;

/// Direct wait-for edges (waiter pid -> its immediate blocker pids), via
/// `pg_blocking_pids()` rather than a hand-rolled pg_locks self-join.
/// That builtin handles FIFO and tuple-lock queueing details that a naive
/// pg_locks self-join can misattribute.
/// Walking this graph below is what turns that per-hop edge list into
/// "who's the true root, and how many total waiters sit behind it."
const WAIT_GRAPH_QUERY: &str = r#"
    SELECT pid, pg_blocking_pids(pid) AS blockers
    FROM pg_stat_activity
    WHERE wait_event_type = 'Lock' AND pid IS NOT NULL;
"#;

struct BlockerMeta {
    relation_oid: pg_sys::Oid,
    lock_mode: LockMode,
    held_since: Option<TimestampWithTimeZone>,
    blocking_query: Option<String>,
}

pub fn main_loop(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Without this the worker has no database connection and the first
    // transaction() trips an assertion that takes the whole cluster down.
    let db = crate::WORKER_DATABASE
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "postgres".to_string());
    BackgroundWorker::connect_worker_to_spi(Some(&db), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(
        SAMPLE_INTERVAL_MS.get().max(10) as u64,
    ))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        if BackgroundWorker::sighup_received() {
            // pgrx's handler only flags ConfigReloadPending. This process
            // still needs to reload postgresql.conf to pick up GUC changes.
            unsafe {
                pg_sys::ConfigReloadPending = 0;
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }

        // transaction() gives the closure a valid SPI/transaction context;
        // a panic inside sample_and_analyze() aborts this transaction and
        // gets logged, but the worker itself keeps running for the next
        // tick rather than taking the whole loop down.
        BackgroundWorker::transaction(|| {
            sample_and_analyze();
        });
    }
}

/// One sample-and-score cycle. Public and callable directly via
/// `lockwatch_sample_now()` so it can be exercised from SQL/tests
/// without waiting on the worker's timer.
pub fn sample_and_analyze() {
    let mut root_pids: Vec<i32> = Vec::new();

    let result = Spi::connect(|client| {
        // A session can hold several relation/tuple locks at once. Keep the
        // most severe one as the representative lock for scoring.
        let mut held: HashMap<i32, BlockerMeta> = HashMap::new();
        for row in client.select(HELD_LOCKS_QUERY, None, &[])? {
            let pid: i32 = row["pid"].value()?.unwrap_or(0);
            if pid == 0 {
                continue;
            }
            let relation_oid: pg_sys::Oid =
                row["relation_oid"].value()?.unwrap_or(pg_sys::InvalidOid);
            let lock_mode_str: String = row["lock_mode"].value()?.unwrap_or_default();
            let lock_mode = LockMode::from_pg_str(&lock_mode_str);

            let is_more_severe = match held.get(&pid) {
                None => true,
                Some(existing) => lock_mode.severity() > existing.lock_mode.severity(),
            };
            if is_more_severe {
                held.insert(
                    pid,
                    BlockerMeta {
                        relation_oid,
                        lock_mode,
                        held_since: row["held_since"].value()?,
                        blocking_query: row["blocking_query"].value()?,
                    },
                );
            }
        }

        let mut direct_blockers: HashMap<i32, Vec<i32>> = HashMap::new();
        for row in client.select(WAIT_GRAPH_QUERY, None, &[])? {
            let pid: i32 = row["pid"].value()?.unwrap_or(0);
            if pid == 0 {
                continue;
            }
            let blockers: Vec<i32> = row["blockers"].value()?.unwrap_or_default();
            direct_blockers.insert(pid, blockers);
        }

        // Walk each waiter up to its root blocker, a pid that
        // holds a lock but is not itself waiting on anything, rather
        // than crediting whichever pid happens to be directly ahead of it
        // in the queue. `visited` guards against cycles: a live deadlock
        // would show up as one, and Postgres's own deadlock detector
        // resolves those on its own timeline, not ours.
        let mut transitive_waiters: HashMap<i32, HashSet<i32>> = HashMap::new();
        let mut max_depth: HashMap<i32, u16> = HashMap::new();

        for (&waiter, first_hop) in &direct_blockers {
            let mut frontier: Vec<(i32, u16)> = first_hop.iter().map(|&pid| (pid, 1)).collect();
            let mut visited: HashSet<i32> = first_hop.iter().copied().collect();
            visited.insert(waiter);

            while let Some((pid, depth)) = frontier.pop() {
                match direct_blockers.get(&pid) {
                    Some(next_hop) => {
                        for &next in next_hop {
                            if visited.insert(next) {
                                frontier.push((next, depth + 1));
                            }
                        }
                    }
                    None => {
                        // pid isn't waiting on anyone -- it's a root.
                        transitive_waiters.entry(pid).or_default().insert(waiter);
                        let d = max_depth.entry(pid).or_insert(0);
                        if depth > *d {
                            *d = depth;
                        }
                    }
                }
            }
        }

        for (&root_pid, waiters) in &transitive_waiters {
            root_pids.push(root_pid);
            let waiter_count = waiters.len().min(u16::MAX as usize) as u16;
            let depth = *max_depth.get(&root_pid).unwrap_or(&0);
            let meta = held.get(&root_pid);

            let inserted = shmem::upsert_blocker(root_pid, |b| {
                if let Some(m) = meta {
                    b.relation_oid = m.relation_oid;
                    b.lock_mode = m.lock_mode;
                    b.query_fingerprint = m
                        .blocking_query
                        .as_deref()
                        .map(fingerprint_query)
                        .unwrap_or(0);
                    if b.lock_acquired_at.is_none() {
                        b.lock_acquired_at = m.held_since;
                        // Cold-start baseline: seed from current hold time so
                        // a long-already-running query does not look like an
                        // instant outlier on first observation. This
                        // converges to something more meaningful over
                        // repeated sightings of the same query_fingerprint.
                        b.baseline_hold_seconds = b
                            .hold_seconds(pgrx::datum::datetime_support::clock_timestamp())
                            .max(0.1);
                    }
                }
                b.cascade_depth = depth;
                b.push_waiter_count(waiter_count);
            });

            if !inserted {
                warning!(
                    "pg_lockwatch: tracking table full (lockwatch.max_tracked_blockers), \
                     dropping pid {root_pid}"
                );
            }
        }

        Ok::<(), pgrx::spi::Error>(())
    });

    if let Err(e) = result {
        warning!("pg_lockwatch: sample query failed: {e}");
        return;
    }

    shmem::evict_stale(&root_pids);
    check_thresholds();
}

fn check_thresholds() {
    let threshold = RISK_THRESHOLD.get();
    for blocker in shmem::snapshot() {
        let score = blocker.risk_score();
        if score >= threshold {
            fire_alert(&blocker, score);
        }
    }
}

fn fire_alert(blocker: &shmem::BlockerState, score: f64) {
    let payload = serde_json::json!({
        "blocking_pid": blocker.blocking_pid,
        "risk_score": score,
        "waiter_count": blocker.current_waiter_count(),
        "query_fingerprint": blocker.query_fingerprint,
    })
    .to_string();

    // Best-effort: a failed NOTIFY or history insert shouldn't crash the
    // sampling loop. Logged and moved on.
    let notify_result =
        Spi::run_with_args("SELECT pg_notify('lockwatch_alert', $1)", &[payload.into()]);
    if let Err(e) = notify_result {
        warning!("pg_lockwatch: NOTIFY failed: {e}");
    }

    let history_result = Spi::run_with_args(
        "INSERT INTO lockwatch_history (blocking_pid, risk_score, waiter_count, \
         query_fingerprint, alerted_at) VALUES ($1, $2, $3, $4, clock_timestamp())",
        &[
            blocker.blocking_pid.into(),
            score.into(),
            blocker.current_waiter_count().into(),
            blocker.query_fingerprint.into(),
        ],
    );
    if let Err(e) = history_result {
        warning!("pg_lockwatch: history insert failed: {e}");
    }
}

fn fingerprint_query(query: &str) -> i64 {
    // A production fingerprint should normalize literals and whitespace first,
    // similar to pg_stat_statements. This placeholder hashes the raw text.
    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    hasher.finish() as i64
}
