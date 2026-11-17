//! Cross-service CloudWatch metrics adapter.
//!
//! Lets non-cloudwatch services (currently CloudWatch Logs metric
//! filters) publish metric data points without depending on this crate
//! directly.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};

use fakecloud_core::delivery::CloudwatchDelivery;

use crate::state::{MetricDatum, SharedCloudWatchState};

pub struct CloudwatchDeliveryImpl {
    state: SharedCloudWatchState,
}

impl CloudwatchDeliveryImpl {
    pub fn new(state: SharedCloudWatchState) -> Self {
        Self { state }
    }
}

impl CloudwatchDelivery for CloudwatchDeliveryImpl {
    fn put_metric(
        &self,
        account_id: &str,
        region: &str,
        namespace: &str,
        metric_name: &str,
        value: f64,
        unit: Option<&str>,
        dimensions: BTreeMap<String, String>,
        timestamp_ms: i64,
    ) {
        let timestamp = Utc
            .timestamp_millis_opt(timestamp_ms)
            .single()
            .unwrap_or_else(Utc::now);
        let mut state = self.state.write();
        let acct = state.get_or_create(account_id);
        let metrics_map = acct.metrics_in_mut(region);
        let bucket = metrics_map.entry(namespace.to_string()).or_default();
        let series = [(metric_name.to_string(), dimensions.clone())];
        bucket.push(MetricDatum {
            metric_name: metric_name.to_string(),
            dimensions,
            timestamp,
            value: Some(value),
            statistic_values: None,
            unit: unit.map(|s| s.to_string()),
            storage_resolution: None,
        });
        // Alarms on this metric evaluate now, as for PutMetricData; the
        // actions they owe are queued on the account for the CloudWatch
        // service to dispatch.
        crate::alarm_eval::evaluate_alarms_in(
            acct,
            region,
            Utc::now(),
            crate::alarm_eval::Scope::Series {
                namespace,
                series: &series,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CloudWatchAccounts;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[test]
    fn put_metric_appends_datum_to_namespace() {
        let state: SharedCloudWatchState = Arc::new(RwLock::new(CloudWatchAccounts::new()));
        let delivery = CloudwatchDeliveryImpl::new(state.clone());

        delivery.put_metric(
            "123456789012",
            "us-east-1",
            "MyApp",
            "ErrorCount",
            1.0,
            None,
            BTreeMap::new(),
            1_700_000_000_000,
        );

        let guard = state.read();
        let acct = guard.get("123456789012").unwrap();
        let metrics = acct.metrics_in("us-east-1").unwrap();
        let bucket = metrics.get("MyApp").unwrap();
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0].metric_name, "ErrorCount");
        assert_eq!(bucket[0].value, Some(1.0));
    }
}
