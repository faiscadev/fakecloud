//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `cloudwatch`.

use super::*;

impl ResourceProvisioner {
    // --- CloudWatch ---

    pub(super) fn create_cloudwatch_alarm(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let alarm_name = props
            .get("AlarmName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let alarm_description = props
            .get("AlarmDescription")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let actions_enabled = props
            .get("ActionsEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let str_array = |key: &str| -> Vec<String> {
            props
                .get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let alarm_actions = str_array("AlarmActions");
        let ok_actions = str_array("OKActions");
        let insufficient_data_actions = str_array("InsufficientDataActions");

        let metric_name = props
            .get("MetricName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let namespace = props
            .get("Namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let statistic = props
            .get("Statistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let extended_statistic = props
            .get("ExtendedStatistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let unit = props
            .get("Unit")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let period = props.get("Period").and_then(|v| v.as_i64());
        let evaluation_periods = props
            .get("EvaluationPeriods")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let datapoints_to_alarm = props.get("DatapointsToAlarm").and_then(|v| v.as_i64());
        let threshold = props.get("Threshold").and_then(|v| v.as_f64());
        let comparison_operator = props
            .get("ComparisonOperator")
            .and_then(|v| v.as_str())
            .unwrap_or("GreaterThanThreshold")
            .to_string();
        let treat_missing_data = props
            .get("TreatMissingData")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let evaluate_low_sample_count_percentile = props
            .get("EvaluateLowSampleCountPercentile")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut dimensions: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Dimensions").and_then(|v| v.as_array()) {
            for d in arr {
                if let (Some(k), Some(v)) = (
                    d.get("Name").and_then(|x| x.as_str()),
                    d.get("Value").and_then(|x| x.as_str()),
                ) {
                    dimensions.insert(k.to_string(), v.to_string());
                }
            }
        }

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let alarm_arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            self.region, self.account_id, alarm_name
        );
        let now = Utc::now();
        let alarm = MetricAlarm {
            alarm_name: alarm_name.clone(),
            alarm_arn: alarm_arn.clone(),
            alarm_description,
            actions_enabled,
            ok_actions,
            alarm_actions,
            insufficient_data_actions,
            state_value: AlarmState::InsufficientData,
            state_reason: "Unchecked: Initial alarm creation".to_string(),
            state_updated_timestamp: now,
            metric_name,
            namespace,
            statistic,
            extended_statistic,
            dimensions,
            period,
            unit,
            evaluation_periods,
            datapoints_to_alarm,
            threshold,
            comparison_operator,
            treat_missing_data,
            evaluate_low_sample_count_percentile,
            threshold_metric_id: props
                .get("ThresholdMetricId")
                .and_then(|v| v.as_str())
                .map(String::from),
            configuration_updated_timestamp: now,
            alarm_configuration_updated_timestamp: now,
        };
        let region_alarms = state.alarms_in_mut(&self.region);
        if region_alarms.contains_key(&alarm_name) {
            return Err(format!("Alarm {alarm_name} already exists"));
        }
        region_alarms.insert(alarm_name.clone(), alarm);

        Ok(ProvisionResult::new(alarm_name).with("Arn", alarm_arn))
    }

    pub(super) fn delete_cloudwatch_alarm(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.alarms_in_mut(&self.region).remove(physical_id);
        Ok(())
    }

    pub(super) fn create_cloudwatch_dashboard(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let dashboard_name = props
            .get("DashboardName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        // CFN passes DashboardBody as a JSON string (Fn::Sub friendly).
        let body = props
            .get("DashboardBody")
            .ok_or("DashboardBody is required")?;
        let body_str = if let Some(s) = body.as_str() {
            s.to_string()
        } else {
            serde_json::to_string(body).map_err(|e| format!("invalid DashboardBody: {e}"))?
        };
        // Validate JSON syntax to mirror real PutDashboard behavior.
        serde_json::from_str::<serde_json::Value>(&body_str)
            .map_err(|e| format!("DashboardBody must be valid JSON: {e}"))?;

        let arn = format!(
            "arn:aws:cloudwatch::{}:dashboard/{dashboard_name}",
            self.account_id
        );
        let dashboard = Dashboard {
            name: dashboard_name.clone(),
            arn: arn.clone(),
            size_bytes: body_str.len() as i64,
            body: body_str,
            last_modified: Utc::now(),
        };

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dashboards.insert(dashboard_name.clone(), dashboard);

        Ok(ProvisionResult::new(dashboard_name).with("Arn", arn))
    }

    pub(super) fn delete_cloudwatch_dashboard(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dashboards.remove(physical_id);
        Ok(())
    }

    pub(super) fn update_cloudwatch_alarm(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        // Renaming AlarmName forces replacement in real CFN; mirror that.
        let new_alarm_name = props
            .get("AlarmName")
            .and_then(|v| v.as_str())
            .unwrap_or(&new_def.logical_id);
        if new_alarm_name != existing.physical_id {
            return Err(
                "AWS::CloudWatch::Alarm updates that change AlarmName require replacement"
                    .to_string(),
            );
        }

        let str_array = |key: &str| -> Vec<String> {
            props
                .get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let alarm_description = props
            .get("AlarmDescription")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let actions_enabled = props
            .get("ActionsEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let alarm_actions = str_array("AlarmActions");
        let ok_actions = str_array("OKActions");
        let insufficient_data_actions = str_array("InsufficientDataActions");
        let metric_name = props
            .get("MetricName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let namespace = props
            .get("Namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let statistic = props
            .get("Statistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let extended_statistic = props
            .get("ExtendedStatistic")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let unit = props
            .get("Unit")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let period = props.get("Period").and_then(|v| v.as_i64());
        let evaluation_periods = props
            .get("EvaluationPeriods")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let datapoints_to_alarm = props.get("DatapointsToAlarm").and_then(|v| v.as_i64());
        let threshold = props.get("Threshold").and_then(|v| v.as_f64());
        let comparison_operator = props
            .get("ComparisonOperator")
            .and_then(|v| v.as_str())
            .unwrap_or("GreaterThanThreshold")
            .to_string();
        let treat_missing_data = props
            .get("TreatMissingData")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let evaluate_low_sample_count_percentile = props
            .get("EvaluateLowSampleCountPercentile")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut dimensions: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Dimensions").and_then(|v| v.as_array()) {
            for d in arr {
                if let (Some(k), Some(v)) = (
                    d.get("Name").and_then(|x| x.as_str()),
                    d.get("Value").and_then(|x| x.as_str()),
                ) {
                    dimensions.insert(k.to_string(), v.to_string());
                }
            }
        }

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let region_alarms = state.alarms_in_mut(&self.region);
        let alarm = region_alarms
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Alarm {} not found", existing.physical_id))?;
        let now = Utc::now();
        alarm.alarm_description = alarm_description;
        alarm.actions_enabled = actions_enabled;
        alarm.ok_actions = ok_actions;
        alarm.alarm_actions = alarm_actions;
        alarm.insufficient_data_actions = insufficient_data_actions;
        alarm.metric_name = metric_name;
        alarm.namespace = namespace;
        alarm.statistic = statistic;
        alarm.extended_statistic = extended_statistic;
        alarm.dimensions = dimensions;
        alarm.period = period;
        alarm.unit = unit;
        alarm.evaluation_periods = evaluation_periods;
        alarm.datapoints_to_alarm = datapoints_to_alarm;
        alarm.threshold = threshold;
        alarm.comparison_operator = comparison_operator;
        alarm.treat_missing_data = treat_missing_data;
        alarm.evaluate_low_sample_count_percentile = evaluate_low_sample_count_percentile;
        alarm.configuration_updated_timestamp = now;
        alarm.alarm_configuration_updated_timestamp = now;

        let alarm_arn = alarm.alarm_arn.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", alarm_arn))
    }

    pub(super) fn update_cloudwatch_dashboard(
        &self,
        existing: &StackResource,
        new_def: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &new_def.properties;
        // Renaming DashboardName forces replacement.
        if let Some(new_name) = props.get("DashboardName").and_then(|v| v.as_str()) {
            if new_name != existing.physical_id {
                return Err(
                    "AWS::CloudWatch::Dashboard updates that change DashboardName require replacement"
                        .to_string(),
                );
            }
        }
        let body = props
            .get("DashboardBody")
            .ok_or("DashboardBody is required")?;
        let body_str = if let Some(s) = body.as_str() {
            s.to_string()
        } else {
            serde_json::to_string(body).map_err(|e| format!("invalid DashboardBody: {e}"))?
        };
        serde_json::from_str::<serde_json::Value>(&body_str)
            .map_err(|e| format!("DashboardBody must be valid JSON: {e}"))?;

        let mut accounts = self.cloudwatch_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let dashboard = state
            .dashboards
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Dashboard {} not found", existing.physical_id))?;
        dashboard.size_bytes = body_str.len() as i64;
        dashboard.body = body_str;
        dashboard.last_modified = Utc::now();
        let arn = dashboard.arn.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Arn", arn))
    }
}
