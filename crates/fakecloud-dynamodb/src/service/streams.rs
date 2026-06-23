use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::KinesisDestination;

use super::{get_table, get_table_mut, require_str, DynamoDbService};

impl DynamoDbService {
    // ── Synthetic defaults ─────────────────────────────────────────────

    pub(super) fn describe_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::ok_json(json!({
            "Endpoints": [{
                "Address": format!("dynamodb.{}.amazonaws.com", req.region),
                "CachePeriodInMinutes": 1440
            }]
        }))
    }

    pub(super) fn describe_limits(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Smaller commercial regions cap per-account capacity at 40k while
        // the major ones (us-east-1 etc.) sit at 80k. Per-table caps stay
        // at 40k everywhere.
        let account_cap = match req.region.as_str() {
            "ap-south-1" | "ap-northeast-2" | "ap-southeast-1" | "ap-southeast-2"
            | "ca-central-1" | "eu-central-1" | "eu-north-1" | "sa-east-1" | "us-west-1" => 40_000,
            _ => 80_000,
        };
        Self::ok_json(json!({
            "AccountMaxReadCapacityUnits": account_cap,
            "AccountMaxWriteCapacityUnits": account_cap,
            "TableMaxReadCapacityUnits": 40_000,
            "TableMaxWriteCapacityUnits": 40_000
        }))
    }

    // ── Table Replica Auto Scaling ─────────────────────────────────────

    pub(super) fn describe_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;

        // Replicas come from the global-table replication group (created via
        // CreateGlobalTable / UpdateTable ReplicaUpdates). The global-table
        // name equals the table name in AWS.
        let replicas = state
            .global_tables
            .get(table_name)
            .map(|gt| replica_auto_scaling_list(&gt.replication_group))
            .unwrap_or_default();

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": table.name,
                "TableStatus": table.status,
                "Replicas": replicas
            }
        }))
    }

    pub(super) fn update_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Validate the table exists (returns ResourceNotFound otherwise).
        let (name, status) = {
            let table = get_table(&state.tables, table_name)?;
            (table.name.clone(), table.status.clone())
        };

        // Persist the supplied autoscaling settings onto the matching replicas
        // in the global-table replication group, then reflect them on read.
        if let Some(gt) = state.global_tables.get_mut(table_name) {
            if let Some(updates) = body["ReplicaUpdates"].as_array() {
                for update in updates {
                    let region = update["RegionName"].as_str().unwrap_or_default();
                    if let Some(replica) = gt
                        .replication_group
                        .iter_mut()
                        .find(|r| r.region_name == region)
                    {
                        if let Some(read) =
                            update.get("ReplicaProvisionedReadCapacityAutoScalingUpdate")
                        {
                            replica.read_capacity_auto_scaling =
                                Some(auto_scaling_description(read));
                        }
                    }
                }
            }
        }
        // A table-level write-capacity autoscaling update applies to every
        // replica per AWS semantics.
        if let Some(write) = body.get("ProvisionedWriteCapacityAutoScalingUpdate") {
            if let Some(gt) = state.global_tables.get_mut(table_name) {
                let desc = auto_scaling_description(write);
                for replica in gt.replication_group.iter_mut() {
                    replica.write_capacity_auto_scaling = Some(desc.clone());
                }
            }
        }

        let replicas = state
            .global_tables
            .get(table_name)
            .map(|gt| replica_auto_scaling_list(&gt.replication_group))
            .unwrap_or_default();

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": name,
                "TableStatus": status,
                "Replicas": replicas
            }
        }))
    }

    // ── Kinesis Streaming ──────────────────────────────────────────────

    pub(super) fn enable_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;
        // Precision is optional: real AWS only reports it once the caller set
        // it, so an unset precision must come back empty (not a synthesised
        // MILLISECOND default), which the Terraform resource asserts.
        let precision = body["EnableKinesisStreamingConfiguration"]
            ["ApproximateCreationDateTimePrecision"]
            .as_str()
            .unwrap_or("");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let table = get_table_mut(&mut state.tables, table_name)?;

        table.kinesis_destinations.push(KinesisDestination {
            stream_arn: stream_arn.to_string(),
            destination_status: "ACTIVE".to_string(),
            approximate_creation_date_time_precision: precision.to_string(),
        });

        let mut config = json!({});
        if !precision.is_empty() {
            config["ApproximateCreationDateTimePrecision"] = json!(precision);
        }
        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "EnableKinesisStreamingConfiguration": config
        }))
    }

    pub(super) fn disable_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let table = get_table_mut(&mut state.tables, table_name)?;

        if let Some(dest) = table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == stream_arn)
        {
            dest.destination_status = "DISABLED".to_string();
        }

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "DISABLED"
        }))
    }

    pub(super) fn describe_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;

        let destinations: Vec<Value> = table
            .kinesis_destinations
            .iter()
            .map(|d| {
                let mut o = json!({
                    "StreamArn": d.stream_arn,
                    "DestinationStatus": d.destination_status,
                });
                // Only report precision once it was actually set, so the
                // Terraform resource reads "" for an unset destination.
                if !d.approximate_creation_date_time_precision.is_empty() {
                    o["ApproximateCreationDateTimePrecision"] =
                        json!(d.approximate_creation_date_time_precision);
                }
                o
            })
            .collect();

        Self::ok_json(json!({
            "TableName": table_name,
            "KinesisDataStreamDestinations": destinations
        }))
    }

    pub(super) fn update_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;
        let precision = body["UpdateKinesisStreamingConfiguration"]
            ["ApproximateCreationDateTimePrecision"]
            .as_str()
            .unwrap_or("");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let table = get_table_mut(&mut state.tables, table_name)?;

        if let Some(dest) = table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == stream_arn)
        {
            dest.approximate_creation_date_time_precision = precision.to_string();
        }

        let mut config = json!({});
        if !precision.is_empty() {
            config["ApproximateCreationDateTimePrecision"] = json!(precision);
        }
        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "UpdateKinesisStreamingConfiguration": config
        }))
    }

    // ── Contributor Insights ───────────────────────────────────────────

    pub(super) fn describe_contributor_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let index_name = body["IndexName"].as_str();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;

        let top = table.top_contributors(10);
        let contributors: Vec<Value> = top
            .iter()
            .map(|(key, count)| {
                json!({
                    "Key": key,
                    "Count": count
                })
            })
            .collect();

        let mut result = json!({
            "TableName": table_name,
            "ContributorInsightsStatus": table.contributor_insights_status,
            "ContributorInsightsRuleList": ["DynamoDBContributorInsights"],
            "TopContributors": contributors
        });
        if let Some(idx) = index_name {
            result["IndexName"] = json!(idx);
        }

        Self::ok_json(result)
    }

    pub(super) fn update_contributor_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let action = require_str(&body, "ContributorInsightsAction")?;
        let index_name = body["IndexName"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let table = get_table_mut(&mut state.tables, table_name)?;

        let status = match action {
            "ENABLE" => "ENABLED",
            "DISABLE" => "DISABLED",
            _ => {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid ContributorInsightsAction: {action}"),
                ))
            }
        };
        table.contributor_insights_status = status.to_string();
        if status == "DISABLED" {
            table.contributor_insights_counters.clear();
        }

        let mut result = json!({
            "TableName": table_name,
            "ContributorInsightsStatus": status
        });
        if let Some(idx) = index_name {
            result["IndexName"] = json!(idx);
        }

        Self::ok_json(result)
    }

    pub(super) fn list_contributor_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableName", body["TableName"].as_str(), 1, 1024)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 0, 100)?;
        let table_name = body["TableName"].as_str();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let summaries: Vec<Value> = state
            .tables
            .values()
            .filter(|t| table_name.is_none() || table_name == Some(t.name.as_str()))
            .map(|t| {
                json!({
                    "TableName": t.name,
                    "ContributorInsightsStatus": t.contributor_insights_status
                })
            })
            .collect();

        Self::ok_json(json!({
            "ContributorInsightsSummaries": summaries
        }))
    }
}

/// Build the `Replicas` list (`ReplicaAutoScalingDescription[]`) from a
/// global-table replication group, emitting any persisted autoscaling
/// settings.
fn replica_auto_scaling_list(replicas: &[crate::state::ReplicaDescription]) -> Vec<Value> {
    replicas
        .iter()
        .map(|r| {
            let mut item = json!({
                "RegionName": r.region_name,
                "ReplicaStatus": r.replica_status,
            });
            let obj = item.as_object_mut().expect("json object");
            if let Some(read) = &r.read_capacity_auto_scaling {
                obj.insert(
                    "ReplicaProvisionedReadCapacityAutoScalingSettings".into(),
                    read.clone(),
                );
            }
            if let Some(write) = &r.write_capacity_auto_scaling {
                obj.insert(
                    "ReplicaProvisionedWriteCapacityAutoScalingSettings".into(),
                    write.clone(),
                );
            }
            item
        })
        .collect()
}

/// Convert an `AutoScalingSettingsUpdate` (from the request) into the
/// `AutoScalingSettingsDescription` shape returned on read.
fn auto_scaling_description(update: &Value) -> Value {
    let mut desc = serde_json::Map::new();
    if let Some(v) = update.get("MinimumUnits") {
        desc.insert("MinimumUnits".into(), v.clone());
    }
    if let Some(v) = update.get("MaximumUnits") {
        desc.insert("MaximumUnits".into(), v.clone());
    }
    if let Some(v) = update.get("AutoScalingDisabled") {
        desc.insert("AutoScalingDisabled".into(), v.clone());
    }
    if let Some(v) = update.get("AutoScalingRoleArn") {
        desc.insert("AutoScalingRoleArn".into(), v.clone());
    }
    // Echo the scaling policy configuration as a description entry.
    if let Some(policy) = update.get("ScalingPolicyUpdate") {
        let mut p = serde_json::Map::new();
        if let Some(name) = policy.get("PolicyName") {
            p.insert("PolicyName".into(), name.clone());
        }
        if let Some(cfg) = policy.get("TargetTrackingScalingPolicyConfiguration") {
            p.insert(
                "TargetTrackingScalingPolicyConfiguration".into(),
                cfg.clone(),
            );
        }
        desc.insert(
            "ScalingPolicies".into(),
            Value::Array(vec![Value::Object(p)]),
        );
    }
    Value::Object(desc)
}
