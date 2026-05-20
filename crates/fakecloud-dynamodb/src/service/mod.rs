mod batch;
#[cfg(test)]
mod expression_corpus_tests;
mod global_tables;
mod items;
mod queries;
mod streams;
mod tables;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use fakecloud_persistence::{S3Store, SnapshotStore};
use fakecloud_s3::SharedS3State;

use crate::state::{
    AttributeValue, DynamoDbSnapshot, DynamoTable, KinesisDestination, SharedDynamoDbState,
    DYNAMODB_SNAPSHOT_SCHEMA_VERSION,
};

/// Minimal subset of a ``DynamoTable`` that Kinesis streaming delivery needs.
///
/// A table can carry megabytes of items; cloning the whole table just to
/// release the write lock and deliver one change record is extremely wasteful.
/// Extracting only the fields the delivery path actually reads (destinations,
/// arn, name) keeps the clone small.
pub(super) struct KinesisDeliveryTarget {
    pub destinations: Vec<KinesisDestination>,
    pub arn: String,
    pub name: String,
}

/// Operation flavor for the per-item KMS audit-trail emitter. Reads
/// emit a paired `Decrypt` after `GenerateDataKey`; writes only emit
/// `GenerateDataKey`, mirroring AWS's audit shape.
pub(crate) enum TableKmsOp {
    Read,
    Write,
}

pub struct DynamoDbService {
    state: SharedDynamoDbState,
    pub(crate) s3_state: Option<SharedS3State>,
    pub(crate) s3_store: Option<Arc<dyn S3Store>>,
    delivery: Option<Arc<DeliveryBus>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    pub(crate) region: String,
    /// Serializes concurrent snapshot writes so the newest observed
    /// state always wins on disk. Without it, two tasks could race
    /// between state.read().clone() and store.save() and leave older
    /// bytes as the final on-disk state.
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DynamoDbService {
    pub fn new(state: SharedDynamoDbState) -> Self {
        Self {
            state,
            s3_state: None,
            s3_store: None,
            delivery: None,
            snapshot_store: None,
            kms_hook: None,
            region: "us-east-1".to_string(),
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn with_s3(mut self, s3_state: SharedS3State) -> Self {
        self.s3_state = Some(s3_state);
        self
    }

    pub fn with_s3_store(mut self, store: Arc<dyn S3Store>) -> Self {
        self.s3_store = Some(store);
        self
    }

    pub fn with_delivery(mut self, delivery: Arc<DeliveryBus>) -> Self {
        self.delivery = Some(delivery);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Record `GenerateDataKey` + `Decrypt` for an SSE-KMS table on a
    /// PutItem/UpdateItem (write) and GetItem/Query/Scan (read). DDB
    /// item bodies are nested attribute maps — encrypting them in
    /// fakecloud would balloon scope without adding test coverage that
    /// users actually want, so we just emit the audit-trail records the
    /// AWS API produces and let callers assert KMS usage via
    /// `/_fakecloud/kms/usage`.
    pub(crate) fn record_table_kms_usage(
        &self,
        account_id: &str,
        table_arn: &str,
        kms_key_arn: Option<&str>,
        operation: TableKmsOp,
    ) {
        let Some(hook) = &self.kms_hook else { return };
        let key = kms_key_arn
            .filter(|k| !k.is_empty())
            .unwrap_or("aws/dynamodb");
        // DynamoDB SSE-KMS uses the AWS-documented encryption context:
        // {aws:dynamodb:tableName: <name>, aws:dynamodb:subscriberId: <account>}
        // — see the AWS DynamoDB encryption-at-rest docs. The table arn
        // ends with `:table/<name>`, so derive the name from it.
        let table_name = table_arn.rsplit('/').next().unwrap_or(table_arn);
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:dynamodb:tableName".to_string(), table_name.to_string());
        ctx.insert(
            "aws:dynamodb:subscriberId".to_string(),
            account_id.to_string(),
        );
        let envelope = match hook.encrypt(
            account_id,
            &self.region,
            key,
            b"ddb-item",
            "dynamodb.amazonaws.com",
            ctx.clone(),
        ) {
            Ok(env) => env,
            Err(_) => return,
        };
        if matches!(operation, TableKmsOp::Read) {
            let _ = hook.decrypt(account_id, &envelope, "dynamodb.amazonaws.com", ctx);
        }
    }

    /// Persist the current in-memory state as a snapshot. Called after
    /// every state-mutating action. A noop when no snapshot store is
    /// configured (i.e. `StorageMode::Memory`).
    ///
    /// The snapshot lock serializes the full clone + serialize + write
    /// so concurrent mutators cannot leave older bytes on disk, and
    /// serialization + the blocking file write are offloaded to the
    /// blocking pool to keep Tokio workers responsive.
    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = DynamoDbSnapshot {
            schema_version: DYNAMODB_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(self.state.read().clone()),
            state: None,
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write dynamodb snapshot"),
            Err(err) => tracing::error!(%err, "dynamodb snapshot task panicked"),
        }
    }

    fn kinesis_target(table: &DynamoTable) -> Option<KinesisDeliveryTarget> {
        if table
            .kinesis_destinations
            .iter()
            .any(|d| d.destination_status == "ACTIVE")
        {
            Some(KinesisDeliveryTarget {
                destinations: table.kinesis_destinations.clone(),
                arn: table.arn.clone(),
                name: table.name.clone(),
            })
        } else {
            None
        }
    }

    /// Deliver a change record to all active Kinesis streaming destinations for a table.
    pub(super) fn deliver_to_kinesis_destinations(
        &self,
        target: &KinesisDeliveryTarget,
        event_name: &str,
        keys: &HashMap<String, AttributeValue>,
        old_image: Option<&HashMap<String, AttributeValue>>,
        new_image: Option<&HashMap<String, AttributeValue>>,
    ) {
        let delivery = match &self.delivery {
            Some(d) => d,
            None => return,
        };

        let active_destinations: Vec<_> = target
            .destinations
            .iter()
            .filter(|d| d.destination_status == "ACTIVE")
            .collect();

        if active_destinations.is_empty() {
            return;
        }

        let mut record = json!({
            "eventID": uuid::Uuid::new_v4().to_string(),
            "eventName": event_name,
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": target.arn.split(':').nth(3).unwrap_or("us-east-1"),
            "dynamodb": {
                "Keys": keys,
                "SequenceNumber": chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).to_string(),
                "SizeBytes": serde_json::to_string(keys).map(|s| s.len()).unwrap_or(0),
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
            "eventSourceARN": &target.arn,
            "tableName": &target.name,
        });

        if let Some(old) = old_image {
            record["dynamodb"]["OldImage"] = json!(old);
        }
        if let Some(new) = new_image {
            record["dynamodb"]["NewImage"] = json!(new);
        }

        let record_str = serde_json::to_string(&record).unwrap_or_default();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&record_str);
        let partition_key = serde_json::to_string(keys).unwrap_or_default();

        for dest in active_destinations {
            delivery.send_to_kinesis(&dest.stream_arn, &encoded, &partition_key);
        }
    }

    fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
        serde_json::from_slice(&req.body).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                format!("Invalid JSON: {e}"),
            )
        })
    }

    fn ok_json(body: Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(body))
    }
}

#[async_trait]
impl AwsService for DynamoDbService {
    fn service_name(&self) -> &str {
        "dynamodb"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_request(req.action.as_str(), &req.json_body());
        let result = match req.action.as_str() {
            "CreateTable" => self.create_table(&req),
            "DeleteTable" => self.delete_table(&req),
            "DescribeTable" => self.describe_table(&req),
            "ListTables" => self.list_tables(&req),
            "UpdateTable" => self.update_table(&req),
            "PutItem" => self.put_item(&req),
            "GetItem" => self.get_item(&req),
            "DeleteItem" => self.delete_item(&req),
            "UpdateItem" => self.update_item(&req),
            "Query" => self.query(&req),
            "Scan" => self.scan(&req),
            "BatchGetItem" => self.batch_get_item(&req),
            "BatchWriteItem" => self.batch_write_item(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsOfResource" => self.list_tags_of_resource(&req),
            "TransactGetItems" => self.transact_get_items(&req),
            "TransactWriteItems" => self.transact_write_items(&req),
            "ExecuteStatement" => self.execute_statement(&req),
            "BatchExecuteStatement" => self.batch_execute_statement(&req),
            "ExecuteTransaction" => self.execute_transaction(&req),
            "UpdateTimeToLive" => self.update_time_to_live(&req),
            "DescribeTimeToLive" => self.describe_time_to_live(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            // Synthetic defaults (no DAX endpoint discovery / no real per-account quotas tracked)
            "DescribeEndpoints" => self.describe_endpoints(&req),
            "DescribeLimits" => self.describe_limits(&req),
            // Backups
            "CreateBackup" => self.create_backup(&req),
            "DeleteBackup" => self.delete_backup(&req),
            "DescribeBackup" => self.describe_backup(&req),
            "ListBackups" => self.list_backups(&req),
            "RestoreTableFromBackup" => self.restore_table_from_backup(&req),
            "RestoreTableToPointInTime" => self.restore_table_to_point_in_time(&req),
            "UpdateContinuousBackups" => self.update_continuous_backups(&req),
            "DescribeContinuousBackups" => self.describe_continuous_backups(&req),
            // Global tables
            "CreateGlobalTable" => self.create_global_table(&req),
            "DescribeGlobalTable" => self.describe_global_table(&req),
            "DescribeGlobalTableSettings" => self.describe_global_table_settings(&req),
            "ListGlobalTables" => self.list_global_tables(&req),
            "UpdateGlobalTable" => self.update_global_table(&req),
            "UpdateGlobalTableSettings" => self.update_global_table_settings(&req),
            "DescribeTableReplicaAutoScaling" => self.describe_table_replica_auto_scaling(&req),
            "UpdateTableReplicaAutoScaling" => self.update_table_replica_auto_scaling(&req),
            // Kinesis streaming
            "EnableKinesisStreamingDestination" => self.enable_kinesis_streaming_destination(&req),
            "DisableKinesisStreamingDestination" => {
                self.disable_kinesis_streaming_destination(&req)
            }
            "DescribeKinesisStreamingDestination" => {
                self.describe_kinesis_streaming_destination(&req)
            }
            "UpdateKinesisStreamingDestination" => self.update_kinesis_streaming_destination(&req),
            // Contributor insights
            "DescribeContributorInsights" => self.describe_contributor_insights(&req),
            "UpdateContributorInsights" => self.update_contributor_insights(&req),
            "ListContributorInsights" => self.list_contributor_insights(&req),
            // Import/Export
            "ExportTableToPointInTime" => self.export_table_to_point_in_time(&req),
            "DescribeExport" => self.describe_export(&req),
            "ListExports" => self.list_exports(&req),
            "ImportTable" => self.import_table(&req),
            "DescribeImport" => self.describe_import(&req),
            "ListImports" => self.list_imports(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "dynamodb",
                &req.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateTable",
            "DeleteTable",
            "DescribeTable",
            "ListTables",
            "UpdateTable",
            "PutItem",
            "GetItem",
            "DeleteItem",
            "UpdateItem",
            "Query",
            "Scan",
            "BatchGetItem",
            "BatchWriteItem",
            "TagResource",
            "UntagResource",
            "ListTagsOfResource",
            "TransactGetItems",
            "TransactWriteItems",
            "ExecuteStatement",
            "BatchExecuteStatement",
            "ExecuteTransaction",
            "UpdateTimeToLive",
            "DescribeTimeToLive",
            "PutResourcePolicy",
            "GetResourcePolicy",
            "DeleteResourcePolicy",
            "DescribeEndpoints",
            "DescribeLimits",
            "CreateBackup",
            "DeleteBackup",
            "DescribeBackup",
            "ListBackups",
            "RestoreTableFromBackup",
            "RestoreTableToPointInTime",
            "UpdateContinuousBackups",
            "DescribeContinuousBackups",
            "CreateGlobalTable",
            "DescribeGlobalTable",
            "DescribeGlobalTableSettings",
            "ListGlobalTables",
            "UpdateGlobalTable",
            "UpdateGlobalTableSettings",
            "DescribeTableReplicaAutoScaling",
            "UpdateTableReplicaAutoScaling",
            "EnableKinesisStreamingDestination",
            "DisableKinesisStreamingDestination",
            "DescribeKinesisStreamingDestination",
            "UpdateKinesisStreamingDestination",
            "DescribeContributorInsights",
            "UpdateContributorInsights",
            "ListContributorInsights",
            "ExportTableToPointInTime",
            "DescribeExport",
            "ListExports",
            "ImportTable",
            "DescribeImport",
            "ListImports",
        ]
    }
}

mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod tests;
