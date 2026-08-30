use pgrx::PgLwLock;
use pgrx::prelude::*;

// -----------------------------------------------------------------------
// IMPORTANT CAVEAT, read before changing these:
//
// Postgres shared memory is sized once at postmaster start and cannot
// grow at runtime. The clean way to do that from a *runtime-read* GUC
// value is to hand-roll a shmem_startup_hook that calls
// pg_sys::RequestAddinShmemSpace() using the GUC's already-parsed value —
// pgrx's `pg_shmem_init!` macro doesn't expose that hook directly, and
// wiring it by hand needs raw pg_sys calls.
//
// For v1, capacity is a compile-time constant instead. lockwatch.max_
// tracked_blockers and lockwatch.history_window (declared in lib.rs)
// are honored as *soft caps within this capacity* — raise them past
// these consts and they're clamped, not actually expanded. If you need
// this to be genuinely runtime-configurable, that's the v2 shmem_startup_
// hook work, not a lib.rs change.
// -----------------------------------------------------------------------

pub const MAX_BLOCKERS: usize = 128;
pub const HISTORY_LEN: usize = 16;

// Deliberately POD (plain-old-data): no heap-backed fields (String, Vec,
// etc). Shared memory in Postgres is a flat mmap'd region, not something
// the allocator manages — every field here needs a size known at compile
// time. Relation *names* are resolved from relation_oid at read time
// (see lib.rs), not stored here, specifically to keep this true.
#[derive(Copy, Clone, Debug)]
pub struct BlockerState {
    pub blocking_pid: i32,
    pub relation_oid: pg_sys::Oid,
    pub lock_mode: LockMode,
    pub lock_acquired_at: Option<TimestampWithTimeZone>,
    pub query_fingerprint: i64,
    pub waiter_history: [u16; HISTORY_LEN],
    pub history_head: usize,
    pub history_len: usize,
    pub baseline_hold_seconds: f64,
    /// Longest transitive wait-chain hanging off this blocker (0 = every
    /// waiter is queued directly on it). Computed each tick from a real
    /// wait-for graph walk (see worker.rs) -- not a proxy.
    pub cascade_depth: u16,
    pub in_use: bool,
}

impl Default for BlockerState {
    fn default() -> Self {
        Self {
            blocking_pid: 0,
            relation_oid: pg_sys::InvalidOid,
            lock_mode: LockMode::AccessShare,
            lock_acquired_at: None,
            query_fingerprint: 0,
            waiter_history: [0; HISTORY_LEN],
            history_head: 0,
            history_len: 0,
            baseline_hold_seconds: 0.0,
            cascade_depth: 0,
            in_use: false,
        }
    }
}

// SAFETY: BlockerState is Copy, contains no pointers/heap allocations,
// and every field is either a primitive or another PGRXSharedMemory-safe
// type (LockMode is a plain fieldless enum; TimestampWithTimeZone is
// pgrx's own Copy timestamp type). Safe to place directly in a
// PgLwLock-guarded shared memory region.
unsafe impl pgrx::PGRXSharedMemory for BlockerState {}

unsafe impl pgrx::PGRXSharedMemory for LockMode {}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

impl LockMode {
    /// Rough severity weight — how much damage this mode does to
    /// concurrent throughput if held for a while. Tune against your own
    /// workload; these are starting points, not physics.
    pub fn severity(&self) -> f64 {
        match self {
            LockMode::AccessShare => 0.05,
            LockMode::RowShare => 0.10,
            LockMode::RowExclusive => 0.25,
            LockMode::ShareUpdateExclusive => 0.40,
            LockMode::Share => 0.45,
            LockMode::ShareRowExclusive => 0.65,
            LockMode::Exclusive => 0.80,
            LockMode::AccessExclusive => 1.0,
        }
    }

    pub fn from_pg_str(s: &str) -> Self {
        match s {
            "AccessShareLock" => LockMode::AccessShare,
            "RowShareLock" => LockMode::RowShare,
            "RowExclusiveLock" => LockMode::RowExclusive,
            "ShareUpdateExclusiveLock" => LockMode::ShareUpdateExclusive,
            "ShareLock" => LockMode::Share,
            "ShareRowExclusiveLock" => LockMode::ShareRowExclusive,
            "ExclusiveLock" => LockMode::Exclusive,
            "AccessExclusiveLock" => LockMode::AccessExclusive,
            _ => LockMode::RowExclusive, // conservative default, not silently AccessShare
        }
    }
}

impl BlockerState {
    pub fn push_waiter_count(&mut self, count: u16) {
        self.waiter_history[self.history_head] = count;
        self.history_head = (self.history_head + 1) % HISTORY_LEN;
        if self.history_len < HISTORY_LEN {
            self.history_len += 1;
        }
    }

    pub fn current_waiter_count(&self) -> i32 {
        if self.history_len == 0 {
            return 0;
        }
        let last_idx = (self.history_head + HISTORY_LEN - 1) % HISTORY_LEN;
        self.waiter_history[last_idx] as i32
    }

    /// Waiters gained per sample tick, averaged over the recent window.
    /// This is the actual predictive signal — a steady queue isn't the
    /// same risk as one that's visibly accelerating.
    pub fn waiter_velocity(&self) -> f64 {
        if self.history_len < 2 {
            return 0.0;
        }
        let oldest_idx = (self.history_head + HISTORY_LEN - self.history_len) % HISTORY_LEN;
        let newest_idx = (self.history_head + HISTORY_LEN - 1) % HISTORY_LEN;
        let delta = self.waiter_history[newest_idx] as f64 - self.waiter_history[oldest_idx] as f64;
        delta / (self.history_len as f64 - 1.0)
    }

    pub fn hold_seconds(&self, now: TimestampWithTimeZone) -> f64 {
        match self.lock_acquired_at {
            Some(started) => ((now - started).as_micros() as f64 / 1_000_000.0).max(0.0),
            None => 0.0,
        }
    }

    /// Composite risk score in [0.0, 1.0], computed from GUC-tunable
    /// weights. Kept as a plain weighted sum on purpose — a scoring
    /// model ops can audit beats one that's marginally more accurate
    /// and completely opaque.
    pub fn risk_score(&self) -> f64 {
        crate::scoring::score(self)
    }
}

// A single PgLwLock guarding a fixed-size array is the simplest correct
// approach for v1. It means every read/write serializes on one lock,
// which is fine at our sampling frequency (10ms-10s ticks) but would
// need sharding (e.g. lock-per-bucket by pid hash) if this were ever
// pushed to sub-millisecond sampling.
// SAFETY: unique name within this extension.
pub static LOCKWATCH_STATE: PgLwLock<[BlockerState; MAX_BLOCKERS]> =
    unsafe { PgLwLock::new(c"pg_lockwatch_state") };

/// Take a consistent snapshot for SQL-facing reads. Cloning out of the
/// lock (rather than holding it while formatting rows) keeps the
/// critical section short.
pub fn snapshot() -> Vec<BlockerState> {
    let guard = LOCKWATCH_STATE.share();
    guard.iter().filter(|b| b.in_use).cloned().collect()
}

/// Find-or-allocate a slot for a given blocking pid. Returns None if
/// the table is full — callers should log-and-skip rather than panic;
/// a saturated tracking table means "raise lockwatch.max_tracked_blockers
/// and restart," not a crash.
pub fn upsert_blocker(pid: i32, f: impl FnOnce(&mut BlockerState)) -> bool {
    let mut guard = LOCKWATCH_STATE.exclusive();

    if let Some(existing) = guard.iter_mut().find(|b| b.in_use && b.blocking_pid == pid) {
        f(existing);
        return true;
    }

    if let Some(slot) = guard.iter_mut().find(|b| !b.in_use) {
        *slot = BlockerState {
            blocking_pid: pid,
            in_use: true,
            ..Default::default()
        };
        f(slot);
        return true;
    }

    false // table full
}

/// Clear entries for pids that are no longer blocking anyone — called
/// each tick before the new sample is written, so resolved contention
/// doesn't linger in the view forever.
pub fn evict_stale(still_blocking_pids: &[i32]) {
    let mut guard = LOCKWATCH_STATE.exclusive();
    for slot in guard.iter_mut() {
        if slot.in_use && !still_blocking_pids.contains(&slot.blocking_pid) {
            *slot = BlockerState::default();
        }
    }
}
