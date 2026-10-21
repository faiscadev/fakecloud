use tokio::time::Instant;

pub(crate) const THROUGHPUT_ERROR_CODE: &str = "ServiceUnavailableException";
pub(crate) const THROUGHPUT_ERROR_MESSAGE: &str = "Slow down.";

#[derive(Clone, Copy, Default)]
pub(crate) struct ThroughputLimits {
    pub requests: Option<f64>,
    pub records: Option<f64>,
    pub mib: Option<f64>,
}

/// Stream-local admission policy, not a model of AWS's unpublished algorithm.
/// Request capacity resets each second. Record/byte excess carries forward:
/// a positive remaining allowance admits one whole record, even if it crosses
/// the boundary. Elapsed whole seconds repay debt but never accrue idle credit.
pub(crate) struct ThroughputWindow {
    started_at: Instant,
    active: bool,
    window: u64,
    requests: u64,
    records: Debt,
    bytes: Debt,
}

impl ThroughputWindow {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            active: false,
            window: 0,
            requests: 0,
            records: Debt::default(),
            bytes: Debt::default(),
        }
    }

    /// Charge the whole call before considering individual records.
    pub(crate) fn try_request(&mut self, now: Instant, limits: ThroughputLimits) -> bool {
        if self.active {
            let window = now.duration_since(self.started_at).as_secs();
            if window > self.window {
                self.window = window;
                self.requests = 0;
            }
        }
        let Some(requests) = self.requests.checked_add(1) else {
            return false;
        };
        if limits
            .requests
            .is_some_and(|limit| count_as_f64_ceil(requests as u128) > limit)
        {
            return false;
        }
        if !self.active {
            self.started_at = now;
            self.active = true;
        }
        self.requests = requests;
        true
    }

    /// Rejected entries debit neither dimension, leaving room for later entries
    /// (notably empty records when the byte allowance is exhausted).
    pub(crate) fn try_record(&mut self, limits: ThroughputLimits, bytes: usize) -> bool {
        if !self.records.has_room(self.window, limits.records, 1.0)
            || (bytes > 0 && !self.bytes.has_room(self.window, limits.mib, 1_048_576.0))
        {
            return false;
        }
        // Keep accounting even for currently ignored dimensions when another
        // quota is enabled, so a live quota change does not erase usage.
        self.records.charge(self.window, limits.records, 1.0, 1.0);
        if bytes > 0 {
            self.bytes.charge(
                self.window,
                limits.mib,
                1_048_576.0,
                count_as_f64_ceil(bytes as u128),
            );
        }
        true
    }
}

/// Keep the last charge as an immutable repayment anchor. Rejected retries and
/// zero-byte records must not repeatedly round byte debt. Compare total usage
/// with the cumulative allowance before subtracting: e.g. 0.5 MiB against five
/// 0.1 MiB allowances is exactly full, not a tiny positive burst credit.
#[derive(Default)]
struct Debt {
    used: f64,
    window: u64,
}

impl Debt {
    fn has_room(&self, window: u64, limit: Option<f64>, scale: f64) -> bool {
        limit.is_none_or(|limit| self.used / scale < limit * ((window - self.window) as f64 + 1.0))
    }

    fn charge(&mut self, window: u64, limit: Option<f64>, scale: f64, cost: f64) {
        let seconds = window - self.window;
        let used = if seconds == 0 {
            self.used
        } else if let Some(limit) = limit {
            let allowance = limit * seconds as f64;
            // Divide before multiplying to avoid overflow at huge MiB limits.
            if self.used / scale <= allowance {
                0.0
            } else {
                self.used - allowance * scale
            }
        } else {
            0.0
        };
        self.used = add_ceil(used, cost);
        self.window = window;
    }
}

/// Round an addition upward when its exact result lies between floats. Usage
/// must not round down and accidentally create room above large finite quotas.
fn add_ceil(a: f64, b: f64) -> f64 {
    let sum = a + b;
    let b_rounded = sum - a;
    let error = (a - (sum - b_rounded)) + (b - b_rounded);
    if error > 0.0 {
        sum.next_up()
    } else {
        sum
    }
}

fn count_as_f64_ceil(count: u128) -> f64 {
    let rounded = count as f64;
    if (rounded as u128) < count {
        rounded.next_up()
    } else {
        rounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn request_limit_is_hard_and_first_success_anchors_window() {
        let now = Instant::now();
        let mut w = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            requests: Some(1.0),
            ..Default::default()
        };
        assert!(!w.try_request(
            now,
            ThroughputLimits {
                requests: Some(0.0),
                ..limits
            }
        ));
        assert!(w.try_request(now + Duration::from_millis(500), limits));
        assert!(!w.try_request(now + Duration::from_secs(1), limits));
        assert!(w.try_request(now + Duration::from_millis(1500), limits));
        assert!(!w.try_request(now + Duration::from_millis(2499), limits));
        assert!(w.try_request(now + Duration::from_millis(2500), limits));
    }

    #[test]
    fn large_record_burst_carries_debt_across_multiple_windows() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            mib: Some(2.0 / 1_048_576.0),
            ..Default::default()
        };
        let mut w = ThroughputWindow::new(now);
        assert!(w.try_request(now, limits));
        assert!(w.try_record(limits, 5));
        assert!(!w.try_record(limits, 1));
        assert!(w.try_record(limits, 0));
        assert!(w.try_request(now + Duration::from_secs(1), limits));
        assert!(!w.try_record(limits, 1));
        assert!(w.try_request(now + Duration::from_secs(2), limits));
        assert!(w.try_record(limits, 1));
        assert!(!w.try_record(limits, 1));
    }

    #[test]
    fn skipped_seconds_repay_debt_without_idle_credit() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(1.0),
            mib: Some(1.0 / 1_048_576.0),
            ..Default::default()
        };
        let mut w = ThroughputWindow::new(now);
        assert!(w.try_request(now, limits));
        assert!(w.try_record(limits, 5));
        assert!(w.try_request(now + Duration::from_secs(100), limits));
        assert!(w.try_record(limits, 1));
        assert!(!w.try_record(limits, 1));
    }

    #[test]
    fn fractional_positive_allowances_admit_one_record_then_repay() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(0.5),
            mib: Some(0.25 / 1_048_576.0),
            ..Default::default()
        };
        let mut w = ThroughputWindow::new(now);
        assert!(w.try_request(now, limits));
        assert!(w.try_record(limits, 1));
        for seconds in [1, 2, 3] {
            assert!(w.try_request(now + Duration::from_secs(seconds), limits));
            assert!(!w.try_record(limits, 1));
        }
        assert!(w.try_request(now + Duration::from_secs(4), limits));
        assert!(w.try_record(limits, 1));
    }

    #[test]
    fn decimal_debt_recovery_does_not_depend_on_rejected_retry_frequency() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            mib: Some(0.1),
            ..Default::default()
        };
        let mut frequent = ThroughputWindow::new(now);
        let mut delayed = ThroughputWindow::new(now);
        for window in [&mut frequent, &mut delayed] {
            assert!(window.try_request(now, limits));
            assert!(window.try_record(limits, 524_288));
        }
        for second in 1..4 {
            assert!(frequent.try_request(now + Duration::from_secs(second), limits));
            assert!(!frequent.try_record(limits, 1));
        }
        for window in [&mut frequent, &mut delayed] {
            assert!(window.try_request(now + Duration::from_secs(4), limits));
            // The entire fifth allowance is still needed to repay the burst.
            assert!(!window.try_record(limits, 1));
            assert!(window.try_request(now + Duration::from_secs(5), limits));
            assert!(window.try_record(limits, 1));
        }
    }

    #[test]
    fn zero_limits_and_rejection_do_not_debit_other_dimensions() {
        let now = Instant::now();
        let mut w = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            records: Some(1.0),
            mib: Some(0.0),
            ..Default::default()
        };
        assert!(w.try_request(now, limits));
        assert!(!w.try_record(limits, 1));
        assert!(w.try_record(limits, 0));
        assert!(!w.try_record(limits, 0));
        let mut w = ThroughputWindow::new(now);
        assert!(!w.try_record(
            ThroughputLimits {
                records: Some(0.0),
                ..Default::default()
            },
            0
        ));
    }

    #[test]
    fn live_changes_preserve_usage_and_track_ignored_dimensions() {
        let now = Instant::now();
        let mut w = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            requests: Some(10.0),
            ..Default::default()
        };
        assert!(w.try_request(now, limits));
        assert!(w.try_record(limits, 5));
        assert!(!w.try_record(
            ThroughputLimits {
                mib: Some(5.0 / 1_048_576.0),
                ..limits
            },
            1
        ));
        assert!(!w.try_record(
            ThroughputLimits {
                records: Some(1.0),
                ..limits
            },
            0
        ));
        assert!(w.try_record(
            ThroughputLimits {
                records: Some(2.0),
                mib: Some(6.0 / 1_048_576.0),
                ..limits
            },
            1
        ));
    }

    #[test]
    fn rejected_requests_do_not_charge_records_or_move_boundary() {
        let now = Instant::now();
        let mut w = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            requests: Some(1.0),
            records: Some(1.0),
            ..Default::default()
        };
        assert!(w.try_request(now, limits));
        assert!(!w.try_request(now + Duration::from_millis(999), limits));
        assert!(w.try_record(limits, 0));
        assert!(w.try_request(now + Duration::from_secs(1), limits));
        assert!(w.try_record(limits, 0));
    }

    #[test]
    fn huge_finite_limits_and_large_counts_do_not_overflow_or_round_down() {
        let now = Instant::now();
        let mut w = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            requests: Some(f64::MAX),
            records: Some(f64::MAX),
            mib: Some(f64::MAX),
        };
        assert!(w.try_request(now, limits));
        assert!(w.try_record(limits, usize::MAX));
        assert!(w.try_request(now + Duration::from_secs(100), limits));
        assert!(w.try_record(limits, 1));
        w.requests = 9_007_199_254_740_992;
        assert!(!w.try_request(
            now + Duration::from_secs(100),
            ThroughputLimits {
                requests: Some(9_007_199_254_740_992.0),
                ..limits
            }
        ));
        w.requests = u64::MAX;
        assert!(!w.try_request(now + Duration::from_secs(100), limits));
        assert!(add_ceil(9_007_199_254_740_992.0, 1.0) > 9_007_199_254_740_992.0);
    }
}
