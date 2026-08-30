use pgrx::PgLwLock;
use pgrx::prelude::*;

// -----------------------------------------------------------------------
// IMPORTANT CAVEAT, read before changing these:
//
// Postgres shared memory is sized once at postmaster start and cannot
// grow at runtime. Runtime-sized shared memory would need a
// shmem_startup_hook that calls pg_sys::RequestAddinShmemSpace() with
// the parsed GUC value. pgrx's `pg_shmem_init!` macro does not expose
// that hook directly.
//
// For v1, capacity is a compile-time constant instead. lockwatch.max_
// tracked_blockers and lockwatch.history_window (declared in lib.rs)
// are soft caps within this capacity. Raising them past these consts
// clamps the value instead of expanding shared memory.
// -----------------------------------------------------------------------

pub const MAX_BLOCKERS: usize = 128;
pub const HISTORY_LEN: usize = 16;

/// lockwatch.max_tracked_blockers, clamped to the physical array size --
/// the GUC can be registered up to 512 (see lib.rs) but the backing array
/// is a fixed MAX_BLOCKERS regardless of what the GUC claims.
fn effective_max_tracked_blockers() -> usize {
    (crate::MAX_TRACKED_BLOCKERS.get().max(1) as usize).min(MAX_BLOCKERS)
}

/// lockwatch.history_window, clamped to the physical ring buffer size --
/// same reasoning as effective_max_tracked_blockers.
fn effective_history_window() -> usize {
    (crate::HISTORY_WINDOW.get().max(1) as usize).min(HISTORY_LEN)
}

// Plain-old-data only: no heap-backed fields such as String or Vec. Shared
// memory in Postgres is a flat mmap'd region, not allocator-managed storage.
// Every field here needs a size known at compile time. Relation names are
// resolved from relation_oid at read time (see lib.rs).
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
    /// Set once an alert has fired for the current above-threshold
    /// episode; cleared as soon as the score drops back below threshold.
    /// Makes alerting edge-triggered (fires once per crossing) instead of
    /// once per sample tick for as long as the blocker stays hot.
    pub alerted: bool,
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
            alerted: false,
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
    /// Approximate severity weight for the lock mode's effect on
    /// concurrent throughput. Tune against the target workload.
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
        // Capped at the configured history window, not the full physical
        // buffer -- this is what actually enforces lockwatch.history_window
        // as a soft cap; the array itself always stays HISTORY_LEN long.
        if self.history_len < effective_history_window() {
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
    /// This separates a steady queue from one that is still accelerating.
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
    /// weights. The implementation stays auditable by using a plain
    /// weighted sum.
    pub fn risk_score(&self) -> f64 {
        crate::scoring::score(self)
    }
}

// A single PgLwLock guarding a fixed-size array is the simplest correct
// approach for v1. It means every read/write serializes on one lock,
// which is fine at the configured sampling frequency (10ms-10s ticks) but would
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
/// the table is full. Callers should log and skip rather than panic;
/// a saturated tracking table means "raise lockwatch.max_tracked_blockers
/// and restart," not a crash.
pub fn upsert_blocker(pid: i32, f: impl FnOnce(&mut BlockerState)) -> bool {
    let mut guard = LOCKWATCH_STATE.exclusive();

    if let Some(existing) = guard.iter_mut().find(|b| b.in_use && b.blocking_pid == pid) {
        f(existing);
        return true;
    }

    // Enforced against the configured soft cap, not just physical
    // capacity -- lockwatch.max_tracked_blockers otherwise has no effect
    // at all, since every slot up to MAX_BLOCKERS would stay available
    // regardless of what the GUC says.
    let in_use_count = guard.iter().filter(|b| b.in_use).count();
    if in_use_count >= effective_max_tracked_blockers() {
        return false;
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

/// Blockers that just crossed above `threshold` this tick -- i.e.
/// weren't already flagged as alerted for the current episode. Marks
/// them alerted (so an ongoing incident doesn't re-fire every sample
/// tick) and clears the flag for anything that dropped back below
/// threshold, so a later re-escalation still fires.
pub fn take_alert_transitions(threshold: f64) -> Vec<(BlockerState, f64)> {
    let mut guard = LOCKWATCH_STATE.exclusive();
    let mut newly_alerted = Vec::new();
    for slot in guard.iter_mut() {
        if !slot.in_use {
            continue;
        }
        let score = slot.risk_score();
        if score >= threshold {
            if !slot.alerted {
                slot.alerted = true;
                newly_alerted.push((*slot, score));
            }
        } else {
            slot.alerted = false;
        }
    }
    newly_alerted
}

/// Clear entries for pids that are no longer blocking anyone.
pub fn evict_stale(still_blocking_pids: &[i32]) {
    let mut guard = LOCKWATCH_STATE.exclusive();
    for slot in guard.iter_mut() {
        if slot.in_use && !still_blocking_pids.contains(&slot.blocking_pid) {
            *slot = BlockerState::default();
        }
    }
}
