//! Concrete implementations of the Application Auto Scaling watcher
//! hooks. Wired by `main.rs` at startup so the watcher can read
//! CloudWatch metric samples and apply DynamoDB capacity changes
//! without those crates depending on `fakecloud-application-autoscaling`.

use std::collections::BTreeMap;

use fakecloud_application_autoscaling::hooks::{
    DynamoDbCapacityHook, EcsServiceHook, MetricReader,
};
use fakecloud_cloudwatch::SharedCloudWatchState;
use fakecloud_dynamodb::SharedDynamoDbState;
use fakecloud_ecs::SharedEcsState;

/// Reads from in-process CloudWatch metric and alarm state.
pub struct CloudwatchMetricReader {
    state: SharedCloudWatchState,
}

impl CloudwatchMetricReader {
    pub fn new(state: SharedCloudWatchState) -> Self {
        Self { state }
    }
}

impl MetricReader for CloudwatchMetricReader {
    fn latest_sample(
        &self,
        account_id: &str,
        region: &str,
        namespace: &str,
        metric_name: &str,
        dimensions: &BTreeMap<String, String>,
    ) -> Option<f64> {
        let guard = self.state.read();
        let acct = guard.get(account_id)?;
        let metrics_map = acct.metrics_in(region)?;
        let bucket = metrics_map.get(namespace)?;
        // Walk newest first so we surface the most recent matching
        // sample. CloudWatch state is append-only, so the latest entry
        // is at the tail.
        bucket
            .iter()
            .rev()
            .find(|d| d.metric_name == metric_name && &d.dimensions == dimensions)
            .and_then(|d| d.value)
    }

    fn alarm_state(&self, account_id: &str, region: &str, alarm_name: &str) -> Option<String> {
        let guard = self.state.read();
        let acct = guard.get(account_id)?;
        let alarms = acct.alarms_in(region)?;
        let alarm = alarms.get(alarm_name)?;
        Some(alarm.state_value.as_str().to_string())
    }

    fn alarms_firing_for_action(
        &self,
        account_id: &str,
        region: &str,
        policy_arn: &str,
    ) -> Vec<String> {
        let guard = self.state.read();
        let Some(acct) = guard.get(account_id) else {
            return Vec::new();
        };
        let Some(alarms) = acct.alarms_in(region) else {
            return Vec::new();
        };
        alarms
            .values()
            .filter(|a| a.state_value.as_str() == "ALARM")
            .filter(|a| a.alarm_actions.iter().any(|act| act == policy_arn))
            .map(|a| a.alarm_name.clone())
            .collect()
    }
}

/// Mutates DynamoDB table provisioned throughput. The watcher calls
/// `set_capacity` after computing a new desired capacity from
/// CloudWatch metrics; we update the table's
/// `ProvisionedThroughput.{ReadCapacityUnits,WriteCapacityUnits}` in
/// place so subsequent `DescribeTable` calls see the new value.
pub struct DynamoDbCapacityHookImpl {
    state: SharedDynamoDbState,
}

impl DynamoDbCapacityHookImpl {
    pub fn new(state: SharedDynamoDbState) -> Self {
        Self { state }
    }
}

impl DynamoDbCapacityHook for DynamoDbCapacityHookImpl {
    fn current_capacity(
        &self,
        account_id: &str,
        _region: &str,
        table_name: &str,
    ) -> Option<(i64, i64)> {
        let guard = self.state.read();
        let acct = guard.get(account_id)?;
        let table = acct.tables.get(table_name)?;
        if table.billing_mode != "PROVISIONED" {
            return None;
        }
        Some((
            table.provisioned_throughput.read_capacity_units,
            table.provisioned_throughput.write_capacity_units,
        ))
    }

    fn set_capacity(
        &self,
        account_id: &str,
        _region: &str,
        table_name: &str,
        read: Option<i64>,
        write: Option<i64>,
    ) -> Result<(), String> {
        let mut guard = self.state.write();
        let acct = guard
            .get_mut(account_id)
            .ok_or_else(|| format!("account {account_id} not found"))?;
        let table = acct
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("table {table_name} not found"))?;
        if table.billing_mode != "PROVISIONED" {
            return Err(format!(
                "table {table_name} is not on PROVISIONED billing mode"
            ));
        }
        if let Some(r) = read {
            if r <= 0 {
                return Err("ReadCapacityUnits must be > 0".to_string());
            }
            table.provisioned_throughput.read_capacity_units = r;
        }
        if let Some(w) = write {
            if w <= 0 {
                return Err("WriteCapacityUnits must be > 0".to_string());
            }
            table.provisioned_throughput.write_capacity_units = w;
        }
        Ok(())
    }
}

/// Mutates an ECS service's desiredCount. The watcher calls
/// `set_desired_count` after computing a new desired count from
/// CloudWatch metrics; we update the service's `desired_count` in
/// place and trigger task spawns/stops via the ECS service's
/// `UpdateService` path so subsequent `DescribeServices` calls see
/// the new value.
pub struct EcsServiceHookImpl {
    state: SharedEcsState,
}

impl EcsServiceHookImpl {
    pub fn new(state: SharedEcsState) -> Self {
        Self { state }
    }
}

impl EcsServiceHook for EcsServiceHookImpl {
    fn current_desired_count(
        &self,
        account_id: &str,
        _region: &str,
        cluster_name: &str,
        service_name: &str,
    ) -> Option<i32> {
        let guard = self.state.read();
        let acct = guard.get(account_id)?;
        let key = fakecloud_ecs::EcsState::service_key(cluster_name, service_name);
        acct.services.get(&key).map(|s| s.desired_count)
    }

    fn set_desired_count(
        &self,
        account_id: &str,
        _region: &str,
        cluster_name: &str,
        service_name: &str,
        desired_count: i32,
    ) -> Result<(), String> {
        let mut guard = self.state.write();
        let acct = guard
            .get_mut(account_id)
            .ok_or_else(|| format!("account {account_id} not found"))?;
        let key = fakecloud_ecs::EcsState::service_key(cluster_name, service_name);
        let service = acct
            .services
            .get_mut(&key)
            .ok_or_else(|| format!("service {service_name} not found in cluster {cluster_name}"))?;
        service.desired_count = desired_count;
        if let Some(d) = service
            .deployments
            .iter_mut()
            .find(|d| d.status == "PRIMARY")
        {
            d.desired_count = desired_count;
            d.updated_at = chrono::Utc::now();
        }
        Ok(())
    }
}
