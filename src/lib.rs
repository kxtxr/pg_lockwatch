use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;
use std::time::Duration;

mod scoring;
mod shmem;
mod worker;

use shmem::LOCKWATCH_STATE;

pgrx::pg_module_magic!();

// ---------------------------------------------------------------------
// GUCs for runtime tuning.
// ---------------------------------------------------------------------

pub static SAMPLE_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static RISK_THRESHOLD: GucSetting<f64> = GucSetting::<f64>::new(0.75);
pub static MAX_TRACKED_BLOCKERS: GucSetting<i32> = GucSetting::<i32>::new(64);
pub static HISTORY_WINDOW: GucSetting<i32> = GucSetting::<i32>::new(8);

// A background worker connects to exactly one database for its whole life.
// pg_locks is cluster-wide so sampling works from anywhere, but
// lockwatch_history lives in whichever database the extension was created
// in. Point this at that database or history inserts fail.
pub static WORKER_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

// Scoring weights are separate GUCs so each one can be inspected and tuned via
// `ALTER SYSTEM SET lockwatch.weight_velocity = ...`.
pub static WEIGHT_VELOCITY: GucSetting<f64> = GucSetting::<f64>::new(0.35);
pub static WEIGHT_DURATION: GucSetting<f64> = GucSetting::<f64>::new(0.30);
pub static WEIGHT_LOCK_MODE: GucSetting<f64> = GucSetting::<f64>::new(0.20);
pub static WEIGHT_CASCADE_DEPTH: GucSetting<f64> = GucSetting::<f64>::new(0.15);

// ---------------------------------------------------------------------
// Shared memory registration. Sizes are fixed at postmaster start,
// so MAX_TRACKED_BLOCKERS / HISTORY_WINDOW above are read-only after boot
// (PGC_POSTMASTER context) even though they're declared as regular GUCs.
// ---------------------------------------------------------------------

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_int_guc(
        c"lockwatch.sample_interval_ms",
        c"How often the background worker samples pg_locks / pg_stat_activity, in milliseconds.",
        c"Lower values catch faster-forming cascades but increase SPI overhead. Start at 100ms.",
        &SAMPLE_INTERVAL_MS,
        10,
        10_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lockwatch.risk_threshold",
        c"Risk score (0.0-1.0) above which a NOTIFY is fired on the lockwatch_alert channel.",
        c"Tune against incident history rather than relying on the default.",
        &RISK_THRESHOLD,
        0.0,
        1.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lockwatch.max_tracked_blockers",
        c"Maximum number of distinct blocking PIDs tracked in shared memory.",
        c"Fixed at postmaster start. Shared memory cannot grow at runtime. Requires restart to change.",
        &MAX_TRACKED_BLOCKERS,
        8,
        512,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lockwatch.history_window",
        c"Number of samples kept per tracked blocker for velocity calculation.",
        c"Fixed at postmaster start, same constraint as max_tracked_blockers.",
        &HISTORY_WINDOW,
        4,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"lockwatch.database",
        c"Database the sampler background worker connects to.",
        c"Must be the database pg_lockwatch was CREATE EXTENSION'd into, or history inserts fail. Requires restart.",
        &WORKER_DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lockwatch.weight_velocity",
        c"Scoring weight: rate of growth of the waiter queue behind a blocker.",
        c"",
        &WEIGHT_VELOCITY,
        0.0,
        1.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lockwatch.weight_duration",
        c"Scoring weight: how far a blocker's current hold time exceeds that query's historical baseline.",
        c"",
        &WEIGHT_DURATION,
        0.0,
        1.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lockwatch.weight_lock_mode",
        c"Scoring weight: severity of the lock mode held (AccessExclusive > RowExclusive, etc).",
        c"",
        &WEIGHT_LOCK_MODE,
        0.0,
        1.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lockwatch.weight_cascade_depth",
        c"Scoring weight: how many transitive levels deep this blocker's own wait chain goes.",
        c"",
        &WEIGHT_CASCADE_DEPTH,
        0.0,
        1.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    // Shared memory must be requested in _PG_init, before shared_preload_libraries
    // finishes loading, or the PgLwLock will not be initialized.
    // Default is only derived for arrays up to 32 elements, so seed explicitly.
    pgrx::pg_shmem_init!(LOCKWATCH_STATE = [shmem::BlockerState::default(); shmem::MAX_BLOCKERS]);

    // Register the sampler as a dynamic background worker that starts with the
    // postmaster and restarts automatically after a crash.
    BackgroundWorkerBuilder::new("pg_lockwatch sampler")
        .set_function("lockwatch_worker_main")
        .set_library("pg_lockwatch")
        .set_argument(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn lockwatch_worker_main(arg: pg_sys::Datum) {
    worker::main_loop(arg);
}

// ---------------------------------------------------------------------
// SQL-facing functions and view. pgrx generates the CREATE FUNCTION and
// CREATE VIEW DDL via `cargo pgrx schema`.
// ---------------------------------------------------------------------

/// Current risk-scored snapshot of all tracked blockers, freshest first.
/// This reads directly from shared memory with no SPI and only the LWLock
/// already guarding the state.
#[pg_extern]
fn lockwatch_current_state() -> TableIterator<
    'static,
    (
        name!(blocking_pid, i32),
        name!(waiter_count, i32),
        name!(risk_score, f64),
        name!(relation_name, Option<String>),
        name!(held_since, Option<TimestampWithTimeZone>),
        name!(query_fingerprint, i64),
        name!(cascade_depth, i32),
    ),
> {
    let snapshot = shmem::snapshot();
    TableIterator::new(snapshot.into_iter().map(|b| {
        // Relation names are not stored in shared memory; resolve them here
        // through Postgres's regclass lookup.
        let relation_name: Option<String> = if b.relation_oid != pg_sys::InvalidOid {
            Spi::get_one_with_args("SELECT $1::regclass::text", &[b.relation_oid.into()])
                .ok()
                .flatten()
        } else {
            None
        };

        (
            b.blocking_pid,
            b.current_waiter_count(),
            b.risk_score(),
            relation_name,
            b.lock_acquired_at,
            b.query_fingerprint,
            b.cascade_depth as i32,
        )
    }))
}

/// Manually trigger one sample-and-score cycle, useful for testing
/// and for `cargo pgrx test` without waiting on the worker's timer.
#[pg_extern]
fn lockwatch_sample_now() {
    worker::sample_and_analyze();
}

extension_sql!(
    r#"
    CREATE VIEW lockwatch_risks AS
    SELECT blocking_pid, waiter_count, risk_score, relation_name,
           held_since, query_fingerprint, cascade_depth
    FROM lockwatch_current_state()
    ORDER BY risk_score DESC;
    "#,
    name = "lockwatch_risks_view",
    requires = [lockwatch_current_state]
);

// Outcome log for the feedback loop: every time the worker fires an
// alert, it inserts a row here. `resolved_as` starts NULL and can be
// back-filled later by a process that correlates alerts with deadlocks,
// timeouts, or clean resolution.
extension_sql!(
    r#"
    CREATE TABLE lockwatch_history (
        id                BIGSERIAL PRIMARY KEY,
        blocking_pid      INT NOT NULL,
        risk_score        DOUBLE PRECISION NOT NULL,
        waiter_count      INT NOT NULL,
        query_fingerprint BIGINT NOT NULL,
        alerted_at        TIMESTAMPTZ NOT NULL,
        resolved_as       TEXT
            CHECK (resolved_as IN ('deadlock', 'timeout', 'resolved_clean', 'unknown'))
    );

    CREATE INDEX lockwatch_history_alerted_at_idx ON lockwatch_history (alerted_at);
    "#,
    name = "lockwatch_history_table"
);

// ---------------------------------------------------------------------
// pgrx test harness boilerplate
// ---------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_sample_now_does_not_panic() {
        Spi::run("SELECT lockwatch_sample_now();").unwrap();
    }

    #[pg_test]
    fn test_current_state_view_exists() {
        let result = Spi::get_one::<i64>("SELECT count(*) FROM lockwatch_risks;");
        assert!(result.is_ok());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_lockwatch'"]
    }
}
