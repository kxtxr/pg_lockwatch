# pg_lockwatch

Predictive lock-contention monitoring for Postgres. A background worker
samples `pg_locks` / `pg_stat_activity` on a timer, tracks each blocking
backend's waiter-queue growth in shared memory, and scores blockers by
how likely they are to be the start of a cascade — before Postgres's
own deadlock detector would ever notice (it only fires on true cycles,
after `deadlock_timeout` has already elapsed).

This was scaffolded outside a build environment (no Rust/pgrx toolchain
in the scaffolding sandbox), so **it has not been compiled**. Treat this
as a structurally-complete first draft to build and iterate on, not
tested code. See "Known gaps" below before you trust any of it.

## Build

Targets **pgrx 0.19.2** (bumped from the 0.12.9 this was originally scaffolded
against — see "Porting notes" below for exactly what changed and what's still
unverified).

```bash
# one-time setup, if you haven't already
cargo install cargo-pgrx --version 0.19.2 --locked
cargo pgrx init          # downloads + builds the PG versions in Cargo.toml features

cd pg_lockwatch
cargo pgrx run pg17      # compiles, launches a local PG17, drops you into psql
```

Then in that psql session:

```sql
CREATE EXTENSION pg_lockwatch;
-- shared_preload_libraries must include 'pg_lockwatch' for the
-- background worker to start — cargo pgrx run's test instance sets
-- this automatically via pg_test::postgresql_conf_options() in lib.rs;
-- for a real cluster, add it to postgresql.conf and restart.

SELECT lockwatch_sample_now();   -- manual one-shot, doesn't wait for the timer
SELECT * FROM lockwatch_risks;
LISTEN lockwatch_alert;
```

To run the (currently thin) test suite:

```bash
cargo pgrx test pg17
```

## Testing

Three layers, in order of how much they actually prove:

1. **`cargo pgrx test pg17`** — the unit tests in `lib.rs` and
   `scoring.rs`. Fast, isolated, but can't touch real cross-session
   locking (a `#[pg_test]` runs inside one transaction on one connection).

2. **`tests/lock_contention.sh pg17`** and **`tests/alert_threshold.sh
   pg17`** — real multi-session scenarios. Start `cargo pgrx run pg17`
   in one terminal and leave it running, then run these against it from
   another. They open one blocking session and three waiters via `psql`
   (through `cargo pgrx connect`), sample mid-contention, and assert on
   `waiter_count`, `relation_name` resolution, a positive `risk_score`,
   post-commit eviction, and (in the second script) that a threshold
   breach produces a `lockwatch_history` row.

   **Why shell scripts and not `cargo pgrx regress`:** that tool runs one
   psql session per test file — it can't express "session A holds a lock
   while session B blocks," which is the entire scenario. Postgres has a
   dedicated tool for exactly this shape of test, `pg_isolationtester`
   (`.spec` files, defined sessions that interleave), but I could not
   confirm `cargo-pgrx` wraps that specific tool rather than plain
   `pg_regress` — the changelog only describes `regress` in single-session
   terms (`--resetdb` running `setup.sql`, etc). Rather than write tests
   against an unconfirmed API, these scripts drive real `psql` processes
   directly.

   **One flagged assumption in both scripts:** they call `cargo pgrx
   connect pg17` and pipe a heredoc to it, assuming it forwards to `psql`
   the way plain `psql` accepts stdin. If that's wrong for your installed
   version, swap `CONNECT=(cargo pgrx connect "$PGVER")` for `CONNECT=(psql
   "$CONNINFO")` using whatever connection string `cargo pgrx status
   pg17` reports.

3. **Manual exploration** — worth doing at least once by hand before
   trusting the scripts: open 4 `cargo pgrx connect pg17` sessions
   yourself, watch `lockwatch_risks` change in real time as you commit/
   rollback the blocker, and eyeball whether the risk score's behavior
   matches your intuition. The scripts encode fixed assertions; your
   eyes will catch things like "the score should probably be higher
   here" that a boolean check won't.

## Porting notes (0.12.9 → 0.19.2)

Checked against pgrx's own release notes and the official v18.0 migration
guide rather than assumed. Already applied in this scaffold:

- **`crate-type = ["cdylib"]`**, not `["cdylib", "lib"]`. v0.18.0 removed the
  old two-pass schema build (extension + a separate `pgrx_embed` binary);
  metadata is now embedded in the shared library at normal build time, and
  the `"lib"` target is no longer needed. This project never had a
  `pgrx_embed` bin target to delete, so that part of the migration is moot.
- **`edition = "2024"`, `resolver = "3"`** — recommended as of v0.17.0.
- **`heapless` dropped from Cargo.toml.** It was a leftover from an earlier
  design draft — the final `shmem.rs` deliberately never uses it (see the
  POD note on `BlockerState`). Good timing anyway: v0.16.0 removed pgrx's
  direct support for `heapless` inside shared memory over unsoundness
  concerns, so this validates the design rather than requiring a change.
- **`pgrx`/`pgrx-tests` pinned with `=0.19.2`.** Standard practice pgrx
  itself recommends for every release, since it's pre-1.0 and point
  releases aren't guaranteed non-breaking.

**Not independently verified — check these first if the build fails:**

- **GUC registration signatures** (`GucRegistry::define_int_guc` /
  `define_float_guc` calls in `_PG_init`). v0.15.0 shipped "fix GUC" and
  "refactor GUC" PRs; `GucRegistry`, `GucSetting`, and `GucContext` all
  still exist under those names in 0.19.2, so the overall shape is likely
  close, but I could not confirm the exact parameter order/types didn't
  shift. This is the single most likely source of compile errors — the
  compiler will point at it directly, and the fix is almost certainly
  mechanical (reorder/retype arguments), not structural.
- **Shared memory internals** (v0.16.0, "improve shmem api"). The public
  surface used here — `PgLwLock`, `pg_shmem_init!`, `PGRXSharedMemory` —
  still exists by name in 0.19.2's docs, so `shmem.rs` is probably close to
  right as written, but the exact trait bounds pgrx expects on
  `PGRXSharedMemory` impls may have changed shape.
- **`BackgroundWorkerBuilder`** — unverified against 0.19.2's exact method
  set, though the `bgworkers` module is old and stable enough that I'd
  expect this to need the least fixing of the three.

Not applicable to this project (mentioned in pgrx's changelogs between
these versions, but nothing here triggers them): the `SqlTranslatable`
associated-const migration (only matters if you hand-write custom SQL
types — this project has none), the removed `pgrx::hooks` module (unused
here).

## Architecture

- **`lib.rs`** — GUC registration, shmem registration, `_PG_init`,
  SQL-facing `#[pg_extern]` functions, the `lockwatch_risks` view.
- **`shmem.rs`** — `BlockerState`: one fixed-size record per tracked
  blocking pid, held in a `PgLwLock`-guarded array in shared memory.
  Deliberately POD (no heap-backed fields) because Postgres shared
  memory is a flat region sized once at postmaster start.
- **`worker.rs`** — the background worker's sample loop. Runs the
  classic pg_locks self-join to find blocked/blocking pid pairs, updates
  shmem state, checks risk scores against the threshold, and fires
  `NOTIFY lockwatch_alert` + a `lockwatch_history` row when crossed.
- **`scoring.rs`** — turns one `BlockerState` into a single risk score
  via a transparent, GUC-weighted sum (queue velocity, hold-time
  overrun vs. baseline, lock mode severity, a cascade-depth proxy).

## Why SPI instead of `GetLockStatusData()`

Discussed and decided deliberately: `GetLockStatusData()` is faster but
touches unstable internal structs and requires taking
`LockHashPartitionLock` manually across partitions — get that wrong and
you either corrupt bookkeeping or introduce contention while detecting
someone else's. SPI against `pg_locks` (the *view*, which Postgres
commits to as a stable interface) stays inside pgrx's safe wrapper
surface and inherits Postgres's own correct locking. The direct-struct
path is the documented v2 optimization once the scoring model is
validated and profiling shows SPI overhead actually matters at your
target sample frequency — not before.

## Known gaps — read before treating any of this as production-ready

1. **Shared memory capacity is a compile-time constant** (`MAX_BLOCKERS
   = 128`, `HISTORY_LEN = 16` in `shmem.rs`), not truly driven by the
   `lockwatch.max_tracked_blockers` / `lockwatch.history_window` GUCs.
   Making those GUCs actually resize shared memory requires a hand-rolled
   `shmem_startup_hook` calling `pg_sys::RequestAddinShmemSpace()` with
   the GUC's value read before `pg_shmem_init!` runs — pgrx's macro
   doesn't expose that directly. Right now those GUCs are soft caps
   within the fixed capacity; raising them past the consts does nothing
   until that hook is written.

2. **Cascade depth isn't a real transitive graph walk.** `scoring.rs`
   uses waiter count as a cheap proxy. The honest version walks the
   full wait-for graph (is this blocker itself blocked by someone else,
   how deep) — that needs the sampling query extended to capture
   blocker-of-blocker edges, and a graph traversal over the shmem table.

3. **Query fingerprinting hashes raw query text**, so the same query
   shape called with different literal values won't fingerprint
   identically. Swap in normalization (strip literals the way
   `pg_stat_statements` does) before trusting baseline-vs-fingerprint
   comparisons.

4. **Baseline hold-time is cold-start seeded, not learned.** First
   sighting of a blocker seeds `baseline_hold_seconds` from its current
   hold time rather than an actual historical distribution for that
   query fingerprint. A real baseline needs a small stats table keyed
   by fingerprint, updated across many observations (e.g. an EMA or a
   proper running p95), which isn't built yet.

5. **One global `PgLwLock`** serializes all shmem reads/writes. Fine at
   sub-second sampling intervals; would need per-bucket sharding if
   ever pushed to much higher frequency.

6. **`lockwatch_history.resolved_as` is never back-filled** by any code
   here — there's no second worker correlating alerts against actual
   deadlock/timeout outcomes yet. That correlation is what would let
   you validate (and eventually tune) the scoring weights against
   reality instead of intuition; right now it's just a log.

7. **Not tested against a real cluster.** SPI column-access syntax
   (`row["col"].value()?`) and a couple of macro call shapes
   (`Spi::connect`, `BackgroundWorkerBuilder`) are written against pgrx
   0.12.9's API as documented; pgrx pre-1.0 has had breaking API
   changes between minor versions before, so expect to fix a handful of
   compile errors on first build rather than a clean `cargo pgrx run`.

## Suggested order of attack

1. Get it compiling against pgrx 0.12.9 — expect SPI row-access and
   background-worker registration to need the most fixing.
2. Validate the sampling query against a manually-constructed lock
   contention scenario (two `psql` sessions, one holding a row lock,
   several more queued behind it) and confirm `lockwatch_risks` reflects
   it.
3. Tune scoring weights against that same manual scenario before
   anything resembling production traffic.
4. Only then: real transitive cascade-depth walk, query normalization,
   learned baselines, the `shmem_startup_hook` for actual runtime-sized
   shmem.
