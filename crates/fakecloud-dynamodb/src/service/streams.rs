use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::KinesisDestination;

use super::{get_table, get_table_mut, require_str, DynamoDbService};

impl DynamoDbService {
    // ── Stubs ──────────────────────────────────────────────────────────

    pub(super) fn describe_endpoints(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::ok_json(json!({
            "Endpoints": [{
                "Address": "dynamodb.us-east-1.amazonaws.com",
                "CachePeriodInMinutes": 1440
            }]
        }))
    }

    pub(super) fn describe_limits(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::ok_json(json!({
            "AccountMaxReadCapacityUnits": 80000,
            "AccountMaxWriteCapacityUnits": 80000,
            "TableMaxReadCapacityUnits": 40000,
            "TableMaxWriteCapacityUnits": 40000
        }))
    }

    // ── Table Replica Auto Scaling ─────────────────────────────────────

    pub(super) fn describe_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": table.name,
                "TableStatus": table.status,
                "Replicas": []
            }
        }))
    }

    pub(super) fn update_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": table.name,
                "TableStatus": table.status,
                "Replicas": []
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
        let precision = body["EnableKinesisStreamingConfiguration"]
            ["ApproximateCreationDateTimePrecision"]
            .as_str()
            .unwrap_or("MILLISECOND");

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        table.kinesis_destinations.push(KinesisDestination {
            stream_arn: stream_arn.to_string(),
            destination_status: "ACTIVE".to_string(),
            approximate_creation_date_time_precision: precision.to_string(),
        });

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "EnableKinesisStreamingConfiguration": {
                "ApproximateCreationDateTimePrecision": precision
            }
        }))
    }

    pub(super) fn disable_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;

        let mut state = self.state.write();
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

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let destinations: Vec<Value> = table
            .kinesis_destinations
            .iter()
            .map(|d| {
                json!({
                    "StreamArn": d.stream_arn,
                    "DestinationStatus": d.destination_status,
                    "ApproximateCreationDateTimePrecision": d.approximate_creation_date_time_precision
                })
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
            .unwrap_or("MILLISECOND");

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        if let Some(dest) = table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == stream_arn)
        {
            dest.approximate_creation_date_time_precision = precision.to_string();
        }

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "UpdateKinesisStreamingConfiguration": {
                "ApproximateCreationDateTimePrecision": precision
            }
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

        let state = self.state.read();
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

        let mut state = self.state.write();
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

        let state = self.state.read();
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
