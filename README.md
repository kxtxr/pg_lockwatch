# pg_lockwatch

Predictive lock-contention monitoring for Postgres.

`pg_lockwatch` runs a pgrx background worker that samples `pg_locks` and
`pg_stat_activity`, tracks blockers in shared memory, and scores the risk of a
lock queue turning into a wider cascade.

## Build

Requires Rust, Postgres, and `cargo-pgrx` 0.19.2.

```bash
cargo install cargo-pgrx --version 0.19.2 --locked
cargo pgrx init
cargo pgrx run pg17
```

In the psql session started by `cargo pgrx run`:

```sql
CREATE EXTENSION pg_lockwatch;

SELECT lockwatch_sample_now();
SELECT * FROM lockwatch_risks;
LISTEN lockwatch_alert;
```

For a real cluster, add `pg_lockwatch` to `shared_preload_libraries` and
restart Postgres so the background worker can start.

## Configuration

Runtime settings are exposed as GUCs:

- `lockwatch.sample_interval_ms`: sampler interval in milliseconds
- `lockwatch.risk_threshold`: alert threshold from `0.0` to `1.0`
- `lockwatch.database`: database the worker connects to
- `lockwatch.weight_velocity`: waiter queue growth weight
- `lockwatch.weight_duration`: hold-time overrun weight
- `lockwatch.weight_lock_mode`: lock severity weight
- `lockwatch.weight_cascade_depth`: wait-chain depth weight

Shared memory capacity is fixed at postmaster start. Raising
`lockwatch.max_tracked_blockers` or `lockwatch.history_window` cannot grow the
compiled shared-memory arrays.

## Testing

Run the pgrx tests:

```bash
cargo pgrx test pg17
```

Run the multi-session integration scripts against a live `cargo pgrx run pg17`
instance:

```bash
./tests/lock_contention.sh pg17
./tests/alert_threshold.sh pg17
```

The scripts use separate `cargo pgrx connect` sessions so one backend can hold a
lock while other backends wait behind it.

## Architecture

- `src/lib.rs`: GUCs, shared-memory registration, background-worker startup,
  SQL functions, and the `lockwatch_risks` view
- `src/worker.rs`: sampling loop, wait-for graph traversal, alerting, and
  history writes
- `src/shmem.rs`: fixed-size shared-memory state guarded by `PgLwLock`
- `src/scoring.rs`: transparent weighted risk score

Alerts are sent with `NOTIFY lockwatch_alert` and also written to
`lockwatch_history`.

## Known Limits

- Query fingerprinting hashes raw query text. It does not normalize literals
  like `pg_stat_statements`.
- Hold-time baselines are seeded from the first observed blocker, not learned
  from long-term history.
- Shared memory uses one global lock. That is simple and acceptable for normal
  sampling intervals, but high-frequency sampling would need sharding.
- `lockwatch_history.resolved_as` is not back-filled yet. Outcome tracking needs
  a later process that correlates alerts with deadlocks, timeouts, or clean
  resolution.
- The extension builds with `cargo check`; validate it with `cargo pgrx run` and
  the integration scripts before using it on a real workload.
