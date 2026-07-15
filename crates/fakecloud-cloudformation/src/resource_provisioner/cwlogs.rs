//! `AWS::CWLOGS::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

impl ResourceProvisioner {
    pub(crate) fn create_log_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let retention_in_days = props
            .get("RetentionInDays")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:*",
            state.region, state.account_id, log_group_name
        );

        let log_group = fakecloud_logs::LogGroup {
            name: log_group_name.to_string(),
            arn: arn.clone(),
            creation_time: Utc::now().timestamp_millis(),
            retention_in_days,
            kms_key_id: None,
            stored_bytes: 0,
            log_streams: std::collections::BTreeMap::new(),
            tags: std::collections::BTreeMap::new(),
            subscription_filters: Vec::new(),
            data_protection_policy: None,
            index_policies: Vec::new(),
            transformer: None,
            deletion_protection: false,
            log_group_class: Some("STANDARD".to_string()),
        };

        state
            .log_groups
            .insert(log_group_name.to_string(), log_group);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    /// Look up an S3 object's bytes from the in-process S3 state. Used by
    /// the Lambda function provisioner to hydrate `Code.S3Bucket` /
    /// `Code.S3Key` references into real ZIP content. Returns an error
    /// string when the bucket or key is missing so the CFN error
    /// surfaces back to the caller.
    pub(crate) fn delete_log_group(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.default_mut();
        // physical_id is the ARN; find the log group name
        let name = state
            .log_groups
            .iter()
            .find(|(_, g)| g.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = name {
            state.log_groups.remove(&name);
        }
        Ok(())
    }

    pub(crate) fn create_log_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let log_stream_name = props
            .get("LogStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let group = state
            .log_groups
            .get_mut(&log_group_name)
            .ok_or_else(|| format!("Log group {log_group_name} does not exist"))?;
        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:log-stream:{}",
            self.region, self.account_id, log_group_name, log_stream_name
        );
        if group.log_streams.contains_key(&log_stream_name) {
            return Err(format!(
                "Log stream {log_stream_name} already exists in {log_group_name}"
            ));
        }
        group.log_streams.insert(
            log_stream_name.clone(),
            LogStream {
                name: log_stream_name.clone(),
                arn,
                creation_time: Utc::now().timestamp_millis(),
                first_event_timestamp: None,
                last_event_timestamp: None,
                last_ingestion_time: None,
                upload_sequence_token: String::new(),
                events: Vec::new(),
            },
        );

        // Encode group + stream into the physical id so deletion can target both.
        let physical_id = format!("{log_group_name}|{log_stream_name}");
        Ok(ProvisionResult::new(physical_id))
    }

    pub(crate) fn delete_log_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, stream_name)) = physical_id.split_once('|') {
            if let Some(group) = state.log_groups.get_mut(group_name) {
                group.log_streams.remove(stream_name);
            }
        }
        Ok(())
    }

    pub(crate) fn create_metric_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let filter_name = props
            .get("FilterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let filter_pattern = props
            .get("FilterPattern")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut transformations: Vec<MetricTransformation> = Vec::new();
        if let Some(arr) = props
            .get("MetricTransformations")
            .and_then(|v| v.as_array())
        {
            for t in arr {
                let metric_name = t
                    .get("MetricName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let metric_namespace = t
                    .get("MetricNamespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let metric_value = t
                    .get("MetricValue")
                    .and_then(|v| v.as_str())
                    .unwrap_or("1")
                    .to_string();
                let default_value = t.get("DefaultValue").and_then(|v| v.as_f64());
                let unit = t
                    .get("Unit")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                transformations.push(MetricTransformation {
                    metric_name,
                    metric_namespace,
                    metric_value,
                    default_value,
                    unit,
                });
            }
        }

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if !state.log_groups.contains_key(&log_group_name) {
            return Err(format!("Log group {log_group_name} does not exist"));
        }
        state
            .metric_filters
            .retain(|f| !(f.log_group_name == log_group_name && f.filter_name == filter_name));
        state.metric_filters.push(MetricFilter {
            filter_name: filter_name.clone(),
            filter_pattern,
            log_group_name: log_group_name.clone(),
            metric_transformations: transformations,
            creation_time: Utc::now().timestamp_millis(),
        });

        Ok(ProvisionResult::new(format!(
            "{log_group_name}|{filter_name}"
        )))
    }

    pub(crate) fn delete_metric_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, filter_name)) = physical_id.split_once('|') {
            state
                .metric_filters
                .retain(|f| !(f.log_group_name == group_name && f.filter_name == filter_name));
        }
        Ok(())
    }

    pub(crate) fn create_subscription_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .map(parse_log_group_name)
            .ok_or_else(|| "LogGroupName is required".to_string())?;
        let filter_name = props
            .get("FilterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let filter_pattern = props
            .get("FilterPattern")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let destination_arn = props
            .get("DestinationArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "DestinationArn is required".to_string())?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let distribution = props
            .get("Distribution")
            .and_then(|v| v.as_str())
            .unwrap_or("ByLogStream")
            .to_string();

        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        let group = state
            .log_groups
            .get_mut(&log_group_name)
            .ok_or_else(|| format!("Log group {log_group_name} does not exist"))?;
        group
            .subscription_filters
            .retain(|f| f.filter_name != filter_name);
        group.subscription_filters.push(SubscriptionFilter {
            filter_name: filter_name.clone(),
            log_group_name: log_group_name.clone(),
            filter_pattern,
            destination_arn,
            role_arn,
            distribution,
            creation_time: Utc::now().timestamp_millis(),
        });

        Ok(ProvisionResult::new(format!(
            "{log_group_name}|{filter_name}"
        )))
    }

    pub(crate) fn delete_subscription_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut logs_accounts = self.logs_state.write();
        let state = logs_accounts.get_or_create(&self.account_id);
        if let Some((group_name, filter_name)) = physical_id.split_once('|') {
            if let Some(group) = state.log_groups.get_mut(group_name) {
                group
                    .subscription_filters
                    .retain(|f| f.filter_name != filter_name);
            }
        }
        Ok(())
    }
}
