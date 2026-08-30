use crate::shmem::BlockerState;
use crate::{WEIGHT_CASCADE_DEPTH, WEIGHT_DURATION, WEIGHT_LOCK_MODE, WEIGHT_VELOCITY};

/// Squash an unbounded positive value into [0, 1] without a hard cutoff.
/// Used for velocity and duration-overrun, where "how much" matters but
/// one pathological outlier should not dominate the whole score.
fn normalize(x: f64, half_point: f64) -> f64 {
    if half_point <= 0.0 {
        return 0.0;
    }
    let x = x.max(0.0);
    x / (x + half_point)
}

/// Composite risk score in [0.0, 1.0].
///
/// Uses a transparent weighted sum rather than a learned model. Every
/// term is inspectable and every weight is a GUC.
pub fn score(state: &BlockerState) -> f64 {
    let now = pgrx::datum::datetime_support::clock_timestamp();

    let velocity = state.waiter_velocity();
    // half_point = 1.0 waiter/tick means "gaining one queued backend
    // per sample" already counts as meaningfully risky. Tune against
    // the configured sample_interval_ms.
    let velocity_term = normalize(velocity, 1.0);

    let hold_seconds = state.hold_seconds(now);
    let baseline = state.baseline_hold_seconds.max(0.1); // avoid div-by-zero on cold baselines
    let overrun_ratio = (hold_seconds / baseline) - 1.0; // 0 = at baseline, 1 = 2x baseline
    let duration_term = normalize(overrun_ratio.max(0.0), 1.0);

    let lock_mode_term = state.lock_mode.severity();

    // Real transitive wait-chain depth (see worker.rs's graph walk over
    // pg_blocking_pids), not a waiter-count proxy: a blocker with waiters
    // queued several links deep takes longer to fully drain even after it
    // itself resolves, since each link still has to wait for the one
    // ahead of it. half_point = 1 means "waiters queued behind other
    // waiters, not just directly on this blocker" already counts as
    // meaningfully risky.
    let cascade_term = normalize(state.cascade_depth as f64, 1.0);

    let wv = WEIGHT_VELOCITY.get();
    let wd = WEIGHT_DURATION.get();
    let wl = WEIGHT_LOCK_MODE.get();
    let wc = WEIGHT_CASCADE_DEPTH.get();
    let weight_sum = (wv + wd + wl + wc).max(0.0001);

    let raw = wv * velocity_term + wd * duration_term + wl * lock_mode_term + wc * cascade_term;

    (raw / weight_sum).clamp(0.0, 1.0)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::shmem::{BlockerState, LockMode};

    #[pgrx::pg_test]
    fn idle_blocker_scores_low() {
        let state = BlockerState {
            lock_mode: LockMode::AccessShare,
            ..Default::default()
        };
        assert!(score(&state) < 0.2);
    }

    #[pgrx::pg_test]
    fn access_exclusive_with_growing_queue_scores_high() {
        let mut state = BlockerState {
            lock_mode: LockMode::AccessExclusive,
            baseline_hold_seconds: 0.5,
            // cascade_depth is measured independently from waiter count, so
            // this fixture sets it explicitly for a high-risk queue.
            cascade_depth: 2,
            ..Default::default()
        };
        for w in [0u16, 1, 2, 4, 6, 9] {
            state.push_waiter_count(w);
        }
        assert!(score(&state) > 0.5);
    }
}
