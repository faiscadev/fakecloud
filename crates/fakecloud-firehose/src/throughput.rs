use tokio::time::Instant;

pub(crate) const THROUGHPUT_ERROR_CODE: &str = "ServiceUnavailableException";
pub(crate) const THROUGHPUT_ERROR_MESSAGE: &str = "Slow down.";

const BYTES_PER_MIB: f64 = 1_048_576.0;

#[derive(Clone, Copy, Default)]
pub(crate) struct ThroughputLimits {
    pub requests: Option<f64>,
    pub records: Option<f64>,
    pub mib: Option<f64>,
}

/// Stream-local admission policy, not a model of AWS's unpublished algorithm.
/// Time is split into one-second windows from the first admitted request.
/// Request capacity resets each window. Record/byte excess carries forward:
/// a positive remaining allowance admits one whole record, even if it crosses
/// the boundary. Elapsed windows repay debt but never accrue idle credit.
#[derive(Default)]
pub(crate) struct ThroughputWindow {
    started_at: Option<Instant>,
    window: u64,
    requests: u64,
    records: Usage,
    bytes: Usage,
}

impl ThroughputWindow {
    /// Charge the whole call before considering individual records.
    pub(crate) fn try_request(&mut self, now: Instant, limits: ThroughputLimits) -> bool {
        if let Some(started_at) = self.started_at {
            let window = now.duration_since(started_at).as_secs();
            if window > self.window {
                self.window = window;
                self.requests = 0;
            }
        }
        if limits
            .requests
            .is_some_and(|limit| (self.requests + 1) as f64 > limit)
        {
            return false;
        }
        self.started_at.get_or_insert(now);
        self.requests += 1;
        true
    }

    /// Rejected entries debit neither dimension, leaving room for later entries
    /// (notably empty records when the byte allowance is exhausted).
    pub(crate) fn try_record(&mut self, limits: ThroughputLimits, bytes: usize) -> bool {
        let bytes_per_second = limits.mib.map(|mib| mib * BYTES_PER_MIB);
        if !self.records.has_room(self.window, limits.records)
            || (bytes > 0 && !self.bytes.has_room(self.window, bytes_per_second))
        {
            return false;
        }
        // Keep accounting even for currently ignored dimensions when another
        // quota is enabled, so a live quota change does not erase usage.
        self.records.charge(self.window, limits.records, 1.0);
        if bytes > 0 {
            self.bytes
                .charge(self.window, bytes_per_second, bytes as f64);
        }
        true
    }
}

/// Usage of one dimension since the window of its last charge. Only a charge
/// repays elapsed windows, so rejected retries cannot change recovery timing.
#[derive(Default)]
struct Usage {
    used: f64,
    window: u64,
}

impl Usage {
    /// Whether usage is below the allowance of every window since the last
    /// charge, up to and including `window`.
    fn has_room(&self, window: u64, per_second: Option<f64>) -> bool {
        per_second.is_none_or(|limit| self.used < limit * (window - self.window + 1) as f64)
    }

    fn charge(&mut self, window: u64, per_second: Option<f64>, cost: f64) {
        let elapsed = window - self.window;
        if elapsed > 0 {
            // Each elapsed window repays up to its allowance; an unlimited
            // dimension starts over. Unused allowance is not banked.
            let repaid = per_second.map_or(self.used, |limit| limit * elapsed as f64);
            self.used = (self.used - repaid).max(0.0);
            self.window = window;
        }
        self.used += cost;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn request_limit_is_hard_and_first_success_anchors_window() {
        let now = Instant::now();
        let mut w = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
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
        let mut frequent = ThroughputWindow::default();
        let mut delayed = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
        let limits = ThroughputLimits {
            records: Some(1.0),
            mib: Some(0.0),
            ..Default::default()
        };
        assert!(w.try_request(now, limits));
        assert!(!w.try_record(limits, 1));
        assert!(w.try_record(limits, 0));
        assert!(!w.try_record(limits, 0));
        let mut w = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
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
        let mut w = ThroughputWindow::default();
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
}
