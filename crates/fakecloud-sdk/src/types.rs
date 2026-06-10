use serde::{Deserialize, Serialize};

// ── Health ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub services: Vec<String>,
}

// ── Reset ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetServiceResponse {
    pub reset: String,
}

// ── RDS ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdsTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdsInstance {
    pub db_instance_identifier: String,
    pub db_instance_arn: String,
    pub db_instance_class: String,
    pub engine: String,
    pub engine_version: String,
    pub db_instance_status: String,
    pub master_username: String,
    pub db_name: Option<String>,
    pub endpoint_address: String,
    pub port: i32,
    pub allocated_storage: i32,
    pub publicly_accessible: bool,
    pub deletion_protection: bool,
    pub created_at: String,
    pub dbi_resource_id: String,
    pub container_id: String,
    pub host_port: u16,
    pub tags: Vec<RdsTag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RdsInstancesResponse {
    pub instances: Vec<RdsInstance>,
}

// ── Lambda ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub payload: String,
    pub source: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LambdaInvocationsResponse {
    pub invocations: Vec<LambdaInvocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WarmContainer {
    pub function_name: String,
    pub runtime: String,
    pub container_id: String,
    pub last_used_secs_ago: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WarmContainersResponse {
    pub containers: Vec<WarmContainer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvictContainerResponse {
    pub evicted: bool,
}

// ── SES ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentEmail {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    #[serde(default)]
    pub bcc: Vec<String>,
    pub subject: Option<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub raw_data: Option<String>,
    pub template_name: Option<String>,
    pub template_data: Option<String>,
    #[serde(default)]
    pub dkim_signature: Option<String>,
    /// Synthesized RFC 5322 headers stamped onto the message at send
    /// time. When DKIM is on, the first entry is `DKIM-Signature`. Each
    /// pair is `(name, value)`.
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesEmailsResponse {
    pub emails: Vec<SentEmail>,
}

/// Admin payload to flip an identity's `MailFromDomainStatus` for tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMailFromStatusRequest {
    pub status: String,
}

// ── SES introspection: bounces / insights / SMTP submissions / event-dest ──

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesBouncedRecipientInfo {
    pub recipient: String,
    pub bounce_type: String,
    pub action: String,
    pub status: String,
    pub diagnostic_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesBounce {
    pub message_id: String,
    pub bounce_type: String,
    pub bounce_sub_type: String,
    pub bounced_recipient_info: Vec<SesBouncedRecipientInfo>,
    pub explanation: Option<String>,
    pub timestamp: String,
    pub original_message_id: String,
    pub bounce_sender: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesBouncesResponse {
    pub bounces: Vec<SesBounce>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMessageInsightEvent {
    pub destination: String,
    pub timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounce_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounce_sub_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complaint_feedback_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMessageInsightsResponse {
    pub message_id: String,
    pub sends: Vec<SesMessageInsightEvent>,
    pub deliveries: Vec<SesMessageInsightEvent>,
    pub opens: Vec<SesMessageInsightEvent>,
    pub clicks: Vec<SesMessageInsightEvent>,
    pub bounces: Vec<SesMessageInsightEvent>,
    pub complaints: Vec<SesMessageInsightEvent>,
    pub rejects: Vec<SesMessageInsightEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesSmtpSubmission {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub subject: Option<String>,
    pub raw_size_bytes: usize,
    pub received_at: String,
    pub auth_user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesSmtpSubmissionsResponse {
    pub submissions: Vec<SesSmtpSubmission>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesEventDestinationDelivery {
    pub destination_name: String,
    pub destination_type: String,
    pub event_type: String,
    pub message_id: String,
    pub dispatched_at: String,
    pub target_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesEventDestinationDeliveriesResponse {
    pub deliveries: Vec<SesEventDestinationDelivery>,
}

/// Admin payload to flip the SES account-level `production_access_enabled`
/// flag. fakecloud defaults to `production_access_enabled=true` so users
/// don't have to verify recipients to send mail; flip this to `false` to
/// exercise sandbox-mode semantics (verified-recipient gate, send quotas,
/// etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesSandboxRequest {
    /// `true` puts the account back into sandbox mode (production access
    /// disabled); `false` re-enables production access.
    pub sandbox: bool,
}

/// Admin payload for `/_fakecloud/logs/anomalies/inject`. Lets tests
/// seed synthetic CloudWatch Logs anomalies so they can exercise
/// `ListAnomalies` / `UpdateAnomaly` deterministically.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsAnomalyInjectRequest {
    pub anomaly_detector_arn: String,
    #[serde(default)]
    pub log_group_arns: Vec<String>,
    pub pattern_string: String,
    #[serde(default)]
    pub priority: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsAnomalyInjectResponse {
    pub anomaly_id: String,
}

/// One entry in the `/_fakecloud/logs/delivery-config` introspection
/// response. Combines a `Delivery` with the `log_type` from its
/// associated `DeliverySource` so test code can assert end-to-end
/// configuration without joining state manually.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsDeliveryConfiguration {
    pub id: String,
    /// Delivery identifier (same value as `id`; mirrors AWS naming).
    pub name: String,
    pub delivery_destination_arn: String,
    pub delivery_source_name: String,
    /// Log type from the associated `DeliverySource` (e.g. `ACCESS_LOGS`).
    /// Empty string when the source has been deleted out from under the
    /// delivery.
    pub log_type: String,
    #[serde(default)]
    pub record_fields: Vec<String>,
    #[serde(default)]
    pub field_delimiter: Option<String>,
    #[serde(default)]
    pub s3_delivery_configuration: Option<serde_json::Value>,
    /// Unix-ms timestamp of `CreateDelivery`. `0` for deliveries
    /// recovered from older snapshots without this field.
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsDeliveryConfigResponse {
    pub configurations: Vec<LogsDeliveryConfiguration>,
}

/// One `Fields` entry parsed from a log group's index policy document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsFieldIndex {
    /// Fields list from `IndexPolicy.policy_document.Fields`.
    pub fields: Vec<String>,
    /// Unix-ms when the policy was created. Mirrors `last_updated_time`
    /// since fakecloud doesn't track a separate creation timestamp.
    pub created_at: i64,
    /// Unix-ms when the policy was last touched
    /// (PutIndexPolicy updates).
    pub last_used_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsFieldIndexesResponse {
    pub log_group_name: String,
    pub indexes: Vec<LogsFieldIndex>,
}

/// Admin payload for `/_fakecloud/cognito/compromised-passwords`.
/// Plaintext passwords are hashed (sha256) and added to the
/// compromised-credentials set consulted by `InitiateAuth` /
/// `AdminInitiateAuth` when the pool's
/// `CompromisedCredentialsRiskConfiguration.Actions.EventAction` is
/// `BLOCK`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CognitoCompromisedPasswordsRequest {
    pub passwords: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundEmailRequest {
    pub from: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundActionExecuted {
    pub rule: String,
    pub action_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundEmailResponse {
    pub message_id: String,
    pub matched_rules: Vec<String>,
    pub actions_executed: Vec<InboundActionExecuted>,
}

// ── SNS ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnsMessage {
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub subject: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnsMessagesResponse {
    pub messages: Vec<SnsMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnsSmsMessage {
    pub phone_number: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnsSmsResponse {
    pub messages: Vec<SnsSmsMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingConfirmation {
    pub subscription_arn: String,
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingConfirmationsResponse {
    pub pending_confirmations: Vec<PendingConfirmation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmSubscriptionRequest {
    pub subscription_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmSubscriptionResponse {
    pub confirmed: bool,
}

// ── SQS ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsMessageInfo {
    pub message_id: String,
    pub body: String,
    pub receive_count: u64,
    pub in_flight: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsQueueMessages {
    pub queue_url: String,
    pub queue_name: String,
    pub messages: Vec<SqsMessageInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsMessagesResponse {
    pub queues: Vec<SqsQueueMessages>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpirationTickResponse {
    pub expired_messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceDlqResponse {
    pub moved_messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppAsTickResponse {
    /// Number of scaling decisions that were applied this tick.
    pub applied: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppAsScheduledTickResponse {
    /// Number of scheduled actions that fired this tick.
    pub fired: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetSsmCommandStatusRequest {
    pub account_id: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetSsmCommandStatusResponse {
    pub updated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailSsmCommandRequest {
    pub account_id: Option<String>,
    /// Optional: target a single invocation. When `None`, every
    /// invocation on the command is flipped to `Failed`.
    pub instance_id: Option<String>,
    /// Optional friendly status detail (e.g. "Script exited with code 7").
    /// Defaults to "Failed".
    pub status_details: Option<String>,
    /// Optional captured stderr to expose via `GetCommandInvocation`.
    pub standard_error_content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailSsmCommandResponse {
    pub updated_invocations: usize,
}

/// One emitted parameter-policy event, as recorded by the
/// `/_fakecloud/ssm/parameter-policy-events` admin endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SsmParameterPolicyEvent {
    pub parameter_name: String,
    pub parameter_arn: String,
    /// One of `ExpirationRegistered`, `ExpirationNotificationRegistered`,
    /// `NoChangeNotificationRegistered`, `Expiration`,
    /// `ExpirationNotification`, `NoChangeNotification`.
    pub event_type: String,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SsmParameterPolicyEventsResponse {
    pub events: Vec<SsmParameterPolicyEvent>,
}

/// Body shape for `POST /_fakecloud/ssm/sessions/inject`. Drops a fake
/// session record into state without going through StartSession (which
/// returns the Smithy-declared `TargetNotConnected` unless
/// `FAKECLOUD_SSM_SESSION_ECHO=1`). Lets tests assert
/// `DescribeSessions`/`TerminateSession` paths work end-to-end.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectSsmSessionRequest {
    pub account_id: Option<String>,
    pub target: String,
    /// Defaults to `Connected`; pass `Terminated` to seed a finished
    /// session.
    pub status: Option<String>,
    /// Defaults to the account-root IAM ARN.
    pub owner: Option<String>,
    pub reason: Option<String>,
    /// Optional explicit session ID. Falls back to the autogenerated
    /// `session-XXXXXXXXXXXX` form when omitted or empty.
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectSsmSessionResponse {
    pub session_id: String,
}

// ── EventBridge ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventBridgeEvent {
    pub event_id: String,
    pub source: String,
    pub detail_type: String,
    pub detail: String,
    pub bus_name: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventBridgeLambdaDelivery {
    pub function_arn: String,
    pub payload: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventBridgeLogDelivery {
    pub log_group_arn: String,
    pub payload: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventBridgeDeliveries {
    pub lambda: Vec<EventBridgeLambdaDelivery>,
    pub logs: Vec<EventBridgeLogDelivery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventHistoryResponse {
    pub events: Vec<EventBridgeEvent>,
    pub deliveries: EventBridgeDeliveries,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireRuleRequest {
    pub bus_name: Option<String>,
    pub rule_name: String,
}

// ── RDS aws_lambda extension bridge ─────────────────────────────────

/// Request body for `POST /_fakecloud/rds/lambda-invoke`. The endpoint is
/// the bridge that the PostgreSQL `aws_lambda` extension calls into from
/// inside an RDS DB instance container — it's normally not driven by
/// user code directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsLambdaInvokeRequest {
    pub function_name: String,
    pub payload: Option<serde_json::Value>,
    pub invocation_type: Option<String>,
    pub region: Option<String>,
}

/// Shape returned by the bridge — mirrors what `aws_lambda.invoke()`
/// returns to SQL callers (RDS/Aurora-compatible).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsLambdaInvokeResponse {
    pub status_code: i32,
    pub payload: Option<serde_json::Value>,
    pub executed_version: Option<String>,
    pub log_result: Option<String>,
}

// ── RDS aws_s3 extension bridge ─────────────────────────────────────

/// Request body for `POST /_fakecloud/rds/s3-import`. The endpoint is
/// the bridge that the PostgreSQL `aws_s3` extension calls into to
/// fetch an object from a fakecloud bucket. Body is returned base64
/// encoded so JSON transport stays text-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsS3ImportRequest {
    pub bucket: String,
    pub key: String,
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsS3ImportResponse {
    pub bucket: String,
    pub key: String,
    pub body_b64: String,
    pub bytes_processed: i64,
}

/// Request body for `POST /_fakecloud/rds/s3-export`. Bridge equivalent
/// of an S3 PutObject driven from inside the DB container.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsS3ExportRequest {
    pub bucket: String,
    pub key: String,
    pub region: Option<String>,
    pub body_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RdsS3ExportResponse {
    pub bucket: String,
    pub key: String,
    pub bytes_uploaded: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireRuleTarget {
    #[serde(rename = "type")]
    pub target_type: String,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireRuleResponse {
    pub targets: Vec<FireRuleTarget>,
}

// ── Scheduler (EventBridge Scheduler) ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerSchedule {
    pub account_id: String,
    pub group_name: String,
    pub name: String,
    pub arn: String,
    pub state: String,
    pub schedule_expression: String,
    pub target_arn: String,
    pub last_fired: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerSchedulesResponse {
    pub schedules: Vec<SchedulerSchedule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireScheduleResponse {
    pub schedule_arn: String,
    pub target_arn: String,
}

// ── S3 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Notification {
    pub bucket: String,
    pub key: String,
    pub event_type: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3NotificationsResponse {
    pub notifications: Vec<S3Notification>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleTickResponse {
    pub processed_buckets: u64,
    pub expired_objects: u64,
    pub transitioned_objects: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3AccessPointEntry {
    pub name: String,
    pub alias: String,
    pub bucket: String,
    pub account_id: String,
    pub network_origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vpc_configuration: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_access_block: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3AccessPointsResponse {
    pub access_points: Vec<S3AccessPointEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3ObjectLambdaResponse {
    pub request_token: String,
    pub request_route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    pub body_base64: String,
    pub body_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3ObjectLambdaResponsesResponse {
    pub responses: Vec<S3ObjectLambdaResponse>,
}

// ── DynamoDB ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtlTickResponse {
    pub expired_items: u64,
}

// ── SecretsManager ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RotationTickResponse {
    pub rotated_secrets: Vec<String>,
}

// ── ElastiCache ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheCluster {
    pub cache_cluster_id: String,
    pub cache_cluster_status: String,
    pub engine: String,
    pub engine_version: String,
    pub cache_node_type: String,
    pub num_cache_nodes: i32,
    pub replication_group_id: Option<String>,
    pub port: Option<i32>,
    pub host_port: Option<u16>,
    pub container_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheClustersResponse {
    pub clusters: Vec<ElastiCacheCluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheReplicationGroupIntrospection {
    pub replication_group_id: String,
    pub status: String,
    pub description: String,
    pub member_clusters: Vec<String>,
    pub automatic_failover: bool,
    pub multi_az: bool,
    pub engine: String,
    pub engine_version: String,
    pub cache_node_type: String,
    pub num_cache_clusters: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheReplicationGroupsResponse {
    pub replication_groups: Vec<ElastiCacheReplicationGroupIntrospection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheServerlessCacheIntrospection {
    pub serverless_cache_name: String,
    pub status: String,
    pub engine: String,
    pub engine_version: String,
    pub cache_node_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheServerlessCachesResponse {
    pub serverless_caches: Vec<ElastiCacheServerlessCacheIntrospection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheAclUser {
    pub name: String,
    pub status: String,
    pub access_string: String,
    pub no_password_required: bool,
    pub password_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheAclGroup {
    pub name: String,
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheAclCluster {
    pub cluster_id: String,
    pub engine: String,
    pub users: Vec<ElastiCacheAclUser>,
    pub groups: Vec<ElastiCacheAclGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElastiCacheAclsResponse {
    pub acls: Vec<ElastiCacheAclCluster>,
}

// ── Step Functions ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsExecution {
    pub execution_arn: String,
    pub state_machine_arn: String,
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    pub start_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsExecutionsResponse {
    pub executions: Vec<StepFunctionsExecution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsSyncBillingDetails {
    pub billed_duration_in_milliseconds: i64,
    pub billed_memory_used_in_mb: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsSyncExecution {
    pub execution_arn: String,
    pub state_machine_arn: String,
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<String>,
    pub duration_ms: i64,
    pub billing_details: StepFunctionsSyncBillingDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsSyncExecutionsResponse {
    pub executions: Vec<StepFunctionsSyncExecution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsExecutionTreeNode {
    pub arn: String,
    pub state_machine_arn: String,
    pub status: String,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<String>,
    pub children: Vec<StepFunctionsExecutionTreeNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepFunctionsExecutionTreeResponse {
    pub root_arn: String,
    pub tree: StepFunctionsExecutionTreeNode,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SfnEnqueueActivityTaskRequest {
    pub activity_arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SfnEnqueueActivityTaskResponse {
    pub task_token: String,
}

// ── Cognito ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserConfirmationCodes {
    pub confirmation_code: Option<String>,
    pub attribute_verification_codes: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmationCode {
    pub pool_id: String,
    pub username: String,
    pub code: String,
    #[serde(rename = "type")]
    pub code_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribute: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmationCodesResponse {
    pub codes: Vec<ConfirmationCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmUserRequest {
    pub user_pool_id: String,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmUserResponse {
    pub confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenInfo {
    #[serde(rename = "type")]
    pub token_type: String,
    pub username: String,
    pub pool_id: String,
    pub client_id: String,
    pub issued_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokensResponse {
    pub tokens: Vec<TokenInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpireTokensRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_pool_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpireTokensResponse {
    pub expired_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthEvent {
    pub event_type: String,
    pub username: String,
    pub user_pool_id: String,
    pub client_id: Option<String>,
    pub timestamp: f64,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthEventsResponse {
    pub events: Vec<AuthEvent>,
}

/// One PreTokenGeneration Lambda trigger invocation captured for
/// introspection at `/_fakecloud/cognito/pretokengen/invocations`.
/// `claims_added` / `claims_overridden` / `group_overrides` are
/// pre-parsed from the Lambda response so test callers don't have to
/// walk the raw `claimsAndScopeOverrideDetails` shape themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreTokenGenInvocation {
    pub pool_id: String,
    pub user_pool_arn: String,
    pub username: String,
    pub trigger_source: String,
    pub lambda_arn: String,
    pub request_payload: serde_json::Value,
    pub response_payload: Option<serde_json::Value>,
    pub claims_added: Vec<String>,
    pub claims_overridden: Vec<String>,
    pub group_overrides: Vec<String>,
    /// RFC3339 timestamp.
    pub invoked_at: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreTokenGenInvocationsResponse {
    pub invocations: Vec<PreTokenGenInvocation>,
}

/// Request body for the `/_fakecloud/cognito/authorization-codes` admin
/// mint endpoint. Lets test harnesses (and any caller that wants to
/// drive the `authorization_code` grant before the Y4 hosted-UI lands)
/// pre-allocate the same `(client_id, redirect_uri, scopes, PKCE)`
/// binding the real `/oauth2/authorize` endpoint will eventually
/// produce.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintAuthorizationCodeRequest {
    pub user_pool_id: String,
    pub client_id: String,
    pub username: String,
    pub redirect_uri: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_challenge: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_challenge_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintAuthorizationCodeResponse {
    pub code: String,
}

// ── API Gateway v2 ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayV2Request {
    pub request_id: String,
    pub api_id: String,
    pub stage: String,
    pub method: String,
    pub path: String,
    pub headers: std::collections::HashMap<String, String>,
    pub query_params: std::collections::HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    pub timestamp: String,
    pub status_code: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayV2RequestsResponse {
    pub requests: Vec<ApiGatewayV2Request>,
}

// ── Bedrock ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockInvocation {
    pub model_id: String,
    pub input: String,
    pub output: String,
    pub timestamp: String,
    /// Error detail for faulted calls, or `None` on success.
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockInvocationsResponse {
    pub invocations: Vec<BedrockInvocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockModelResponseConfig {
    pub status: String,
    pub model_id: String,
}

/// One rule in a per-model response rule list.
///
/// `prompt_contains` is a substring that must appear in the prompt for this
/// rule to match. `None` or an empty string matches any prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockResponseRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_contains: Option<String>,
    pub response: String,
}

/// Configuration for a fault to inject on Bedrock runtime calls.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BedrockFaultRule {
    pub error_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

/// Server-side view of a queued fault rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockFaultRuleState {
    pub error_type: String,
    pub message: String,
    pub http_status: u16,
    pub remaining: u32,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub operation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockFaultsResponse {
    pub faults: Vec<BedrockFaultRuleState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockStatusResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentAliasSummary {
    pub alias_id: String,
    pub alias_name: String,
    pub agent_version: String,
    pub alias_arn: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentVersionSummary {
    pub agent_version: String,
    pub created_at: String,
    pub instruction: Option<String>,
    pub foundation_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentKnowledgeBaseSummary {
    pub knowledge_base_id: String,
    pub state: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentCollaboratorSummary {
    pub collaborator_id: String,
    pub collaborator_name: String,
    pub collaborator_alias_arn: String,
    pub relay_conversation_history: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentRow {
    pub agent_id: String,
    pub agent_name: String,
    pub agent_arn: String,
    pub agent_status: String,
    pub foundation_model: Option<String>,
    pub instruction: Option<String>,
    pub knowledge_bases: Vec<BedrockAgentKnowledgeBaseSummary>,
    pub action_groups: Vec<serde_json::Value>,
    pub collaborators: Vec<BedrockAgentCollaboratorSummary>,
    pub aliases: Vec<BedrockAgentAliasSummary>,
    pub versions: Vec<BedrockAgentVersionSummary>,
    pub prompt_overrides: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentAgentsResponse {
    pub agents: Vec<BedrockAgentRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentRuntimeInvocation {
    pub invocation_id: String,
    pub op: String,
    pub agent_id: Option<String>,
    pub flow_id: Option<String>,
    pub session_id: Option<String>,
    pub input: String,
    pub output: String,
    pub output_chunks: u32,
    pub trace: Option<serde_json::Value>,
    #[serde(default)]
    pub citations: Vec<serde_json::Value>,
    pub invoked_at: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BedrockAgentRuntimeInvocationsResponse {
    pub invocations: Vec<BedrockAgentRuntimeInvocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrRepository {
    pub repository_name: String,
    pub repository_arn: String,
    pub registry_id: String,
    pub repository_uri: String,
    pub image_tag_mutability: String,
    pub scan_on_push: bool,
    pub created_at: String,
    pub tags: Vec<EcrTag>,
    pub has_policy: bool,
    pub has_lifecycle_policy: bool,
    pub image_count: u64,
    pub layer_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrImage {
    pub repository_name: String,
    pub image_digest: String,
    pub image_tags: Vec<String>,
    pub image_size_in_bytes: u64,
    pub image_manifest_media_type: String,
    pub image_pushed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrImagesResponse {
    pub images: Vec<EcrImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrPullThroughRule {
    pub ecr_repository_prefix: String,
    pub upstream_registry_url: String,
    pub upstream_registry: Option<String>,
    pub credential_arn: Option<String>,
    pub custom_role_arn: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrPullThroughRulesResponse {
    pub rules: Vec<EcrPullThroughRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcrRepositoriesResponse {
    pub repositories: Vec<EcrRepository>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsCluster {
    pub cluster_name: String,
    pub cluster_arn: String,
    pub status: String,
    pub running_tasks_count: i32,
    pub pending_tasks_count: i32,
    pub active_services_count: i32,
    pub registered_container_instances_count: i32,
    pub capacity_providers: Vec<String>,
    pub tags: Vec<EcsTag>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsClustersResponse {
    pub clusters: Vec<EcsCluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskContainer {
    pub name: String,
    pub image: String,
    pub last_status: String,
    pub exit_code: Option<i64>,
    pub runtime_id: Option<String>,
    pub essential: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTask {
    pub task_arn: String,
    pub task_id: String,
    pub cluster_arn: String,
    pub cluster_name: String,
    pub task_definition_arn: String,
    pub family: String,
    pub revision: i32,
    pub last_status: String,
    pub desired_status: String,
    pub launch_type: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub stopping_at: Option<String>,
    pub stopped_at: Option<String>,
    pub stop_code: Option<String>,
    pub stopped_reason: Option<String>,
    pub group: Option<String>,
    pub containers: Vec<EcsTaskContainer>,
    pub captured_log_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTasksResponse {
    pub tasks: Vec<EcsTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskLogsResponse {
    pub task_arn: String,
    pub logs: String,
    pub last_status: String,
    pub exit_code: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EcsMarkFailedRequest {
    pub exit_code: Option<i64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsLifecycleEvent {
    pub at: String,
    pub event_type: String,
    pub task_arn: Option<String>,
    pub cluster_arn: Option<String>,
    pub last_status: Option<String>,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsEventsResponse {
    pub events: Vec<EcsLifecycleEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskMetadataLimits {
    pub cpu: Option<f64>,
    pub memory: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskMetadataPort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_port: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_port: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskMetadataContainer {
    pub name: String,
    pub image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_id: Option<String>,
    pub ports: Vec<EcsTaskMetadataPort>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub desired_status: String,
    pub known_status: String,
    pub limits: EcsTaskMetadataLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskMetadata {
    pub cluster: String,
    pub task_arn: String,
    pub family: String,
    pub revision: i32,
    pub desired_status: String,
    pub known_status: String,
    pub containers: Vec<EcsTaskMetadataContainer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_stopped_at: Option<String>,
    pub availability_zone: String,
    pub launch_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vpc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eni_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EcsTaskMetadataResponse {
    pub task: EcsTaskMetadata,
}

// ── ELBv2 ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2LoadBalancer {
    pub arn: String,
    pub name: String,
    pub dns_name: String,
    pub scheme: String,
    pub vpc_id: String,
    pub state_code: String,
    pub state_reason: Option<String>,
    pub lb_type: String,
    pub ip_address_type: String,
    pub availability_zones: Vec<Elbv2AvailabilityZone>,
    pub security_groups: Vec<String>,
    pub created_time: String,
    pub tags: Vec<Elbv2Tag>,
    /// In-process data plane TCP port for ALBs. `None` for NLB/GWLB
    /// or when the data plane is disabled. Tests connect to
    /// `127.0.0.1:<bound_port>` to reach the routed targets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2AvailabilityZone {
    pub zone_name: String,
    pub subnet_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2LoadBalancersResponse {
    pub load_balancers: Vec<Elbv2LoadBalancer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2TargetGroup {
    pub arn: String,
    pub name: String,
    pub protocol: Option<String>,
    pub port: Option<i32>,
    pub vpc_id: Option<String>,
    pub target_type: String,
    pub load_balancer_arns: Vec<String>,
    pub targets: Vec<Elbv2Target>,
    pub health_check_protocol: Option<String>,
    pub health_check_port: Option<String>,
    pub health_check_path: Option<String>,
    pub healthy_threshold_count: i32,
    pub unhealthy_threshold_count: i32,
    pub created_time: String,
    pub tags: Vec<Elbv2Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2Target {
    pub id: String,
    pub port: Option<i32>,
    pub availability_zone: Option<String>,
    pub health_state: String,
    pub health_reason: Option<String>,
    pub health_description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2TargetGroupsResponse {
    pub target_groups: Vec<Elbv2TargetGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2Listener {
    pub arn: String,
    pub load_balancer_arn: String,
    pub port: Option<i32>,
    pub protocol: Option<String>,
    pub ssl_policy: Option<String>,
    pub certificate_arns: Vec<String>,
    pub default_action_type: Option<String>,
    pub default_target_group_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2ListenersResponse {
    pub listeners: Vec<Elbv2Listener>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2Rule {
    pub arn: String,
    pub listener_arn: String,
    pub priority: String,
    pub is_default: bool,
    pub condition_fields: Vec<String>,
    pub action_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2RulesResponse {
    pub rules: Vec<Elbv2Rule>,
}

/// Request to bootstrap an IAM admin user in a specific account.
/// Used by `/_fakecloud/iam/create-admin` to solve the multi-account
/// bootstrap problem: there's no per-account root credential, so this
/// endpoint creates a user with full admin access in any account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAdminRequest {
    pub account_id: String,
    pub user_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAdminResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub account_id: String,
    pub arn: String,
}

/// Body for `POST /_fakecloud/route53/health-checks/{id}/status`. The
/// admin endpoint flips a stored Route 53 health check's reported
/// status (and optionally the last-failure-reason observation) so
/// tests can simulate failover scenarios without a live checker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route53HealthCheckStatusRequest {
    /// New status reported by `GetHealthCheckStatus`. One of
    /// `"Success"`, `"Failure"`, `"Timeout"`, `"DnsError"`,
    /// `"InsufficientDataPoints"`, `"Unknown"`.
    pub status: Route53HealthCheckStatusValue,
    /// Optional last-failure observation surfaced by
    /// `GetHealthCheckLastFailureReason` and appended to the
    /// `<Status>` element for failure-flavoured statuses (`Failure`,
    /// `Timeout`, `DnsError`). Ignored when `status` is `Success`,
    /// `InsufficientDataPoints`, or `Unknown`. `None` leaves the prior
    /// value intact.
    #[serde(default)]
    pub reason: Option<String>,
}

/// Discriminator for the admin `status` field. Mirrors the variants of
/// `fakecloud_route53::HealthCheckStatus` without forcing the SDK crate
/// to depend on the route53 crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Route53HealthCheckStatusValue {
    Success,
    Failure,
    Timeout,
    DnsError,
    InsufficientDataPoints,
    Unknown,
}

/// Response body for `GET /_fakecloud/route53/zones/{id}/dnssec`. Surfaces
/// the deterministic ECDSA P-256 DNSSEC chain-of-trust material for a
/// hosted zone with at least one ACTIVE Key Signing Key. Real Route 53
/// keeps this material inside KMS; fakecloud derives it from the
/// `(zone_id, ksk_name)` pair so persistence reloads, multiple test
/// runs, and verifier code see stable values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route53DnssecMaterialResponse {
    /// Hosted zone the material belongs to (without the
    /// `/hostedzone/` prefix).
    pub hosted_zone_id: String,
    /// KSK name used to derive the keypair.
    pub key_signing_key_name: String,
    /// Algorithm number (always `13` for ECDSAP256SHA256).
    pub algorithm: u8,
    /// DNSKEY flags field (always `257` for a KSK).
    pub flags: u16,
    /// Standard DNSKEY key tag (RFC 4034 Appendix B).
    pub key_tag: u16,
    /// DNSKEY public-key wire bytes (`X || Y`, 64 bytes for P-256),
    /// base64-encoded — what would appear in the DNSKEY RDATA.
    pub dnskey_public_key_b64: String,
    /// SHA-256 DS digest hex over the canonical owner name + DNSKEY
    /// RDATA. Equivalent to what the parent zone publishes.
    pub ds_digest_sha256_hex: String,
}

/// Body for `POST /_fakecloud/route53/zones/{id}/dnssec/sign`. Signs an
/// RRset under the zone's first ACTIVE KSK and returns the raw RRSIG
/// fields so tests can verify the signature against
/// `dnskey_public_key_b64` from `Route53DnssecMaterialResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route53DnssecSignRequest {
    /// RRset owner name (e.g., `"www.example.com."`). Trailing dot
    /// optional — added if missing.
    pub name: String,
    /// Record type (`"A"`, `"AAAA"`, `"CNAME"`, `"TXT"`, ...).
    #[serde(rename = "type")]
    pub record_type: String,
    /// Original TTL field for the RRSIG.
    pub ttl: u32,
    /// One-or-more RDATA values matching what `ResourceRecord.Value`
    /// would carry on the wire.
    pub rdatas: Vec<String>,
}

/// Response from the DNSSEC sign admin endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route53DnssecSignResponse {
    /// Base64-encoded raw `r||s` ECDSA-P256 signature (64 bytes
    /// decoded).
    pub signature_b64: String,
    /// Algorithm number (always `13`).
    pub algorithm: u8,
    /// Key tag of the signing KSK.
    pub key_tag: u16,
    /// Owner name of the signer (the zone name).
    pub signer_name: String,
    /// Unix-time inception (signature validity start).
    pub inception: u32,
    /// Unix-time expiration (signature validity end).
    pub expiration: u32,
    /// Label count for the RRSIG `Labels` field.
    pub labels: u8,
    /// Original TTL echoed back from the request.
    pub original_ttl: u32,
    /// Record type echoed back from the request.
    #[serde(rename = "type")]
    pub rrset_type: String,
}

/// Body for `POST /_fakecloud/acm/certificates/{arn-or-id}/status`. The
/// admin endpoint flips a stored ACM certificate's status (and
/// optionally records a failure reason) so tests can synchronously
/// drive a cert to `ISSUED`, `FAILED`, or `VALIDATION_TIMED_OUT`
/// without waiting on the auto-issue tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcmCertificateStatusRequest {
    /// New certificate status. One of `"ISSUED"`, `"FAILED"`,
    /// `"VALIDATION_TIMED_OUT"`. Other ACM statuses are accepted as
    /// raw strings in case callers want to simulate a niche state.
    pub status: String,
    /// Optional failure reason surfaced as `FailureReason` in
    /// `DescribeCertificate`. Ignored when `status = ISSUED`. `None`
    /// leaves the prior value intact.
    #[serde(default)]
    pub reason: Option<String>,
}

// ── Glue ────────────────────────────────────────────────────────────

/// Curated row for `GET /_fakecloud/glue/jobs`. Mirrors the
/// configured Glue Job state so tests can assert what `CreateJob`
/// recorded without re-listing through the AWS surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueJob {
    pub account_id: String,
    pub name: String,
    pub role: String,
    pub command: serde_json::Value,
    pub default_arguments: std::collections::BTreeMap<String, String>,
    pub max_capacity: Option<f64>,
    pub max_retries: i64,
    pub timeout: Option<i64>,
    pub glue_version: Option<String>,
    pub worker_type: Option<String>,
    pub number_of_workers: Option<i64>,
    pub created_on: String,
    pub last_modified_on: String,
}

/// Curated row for `GET /_fakecloud/glue/crawlers`. One entry per
/// crawler across every account, mirroring what `CreateCrawler`
/// recorded plus its lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueCrawler {
    pub account_id: String,
    pub name: String,
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
    /// READY / RUNNING / STOPPING.
    pub state: String,
    /// Short summary of configured targets, e.g. "2 S3, 1 JDBC".
    pub target_summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    pub creation_time: String,
    pub last_updated: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueCrawlersResponse {
    pub crawlers: Vec<GlueCrawler>,
}

// ── CloudWatch ──────────────────────────────────────────────────────

/// A single metric dimension (name/value pair).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchDimension {
    pub name: String,
    pub value: String,
}

/// One alarm (metric or composite) as exposed by
/// `GET /_fakecloud/cloudwatch/alarms`. Metric-only fields
/// (`namespace`/`metricName`/`threshold`/`comparisonOperator`) are
/// omitted for composite alarms; `alarmRule` is present only for
/// composite alarms.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchAlarm {
    pub account_id: String,
    pub region: String,
    pub name: String,
    /// "metric" or "composite".
    #[serde(rename = "type")]
    pub alarm_type: String,
    /// OK / ALARM / INSUFFICIENT_DATA.
    pub state: String,
    pub state_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_updated_timestamp: Option<String>,
    pub actions_enabled: bool,
    pub alarm_actions: Vec<String>,
    pub ok_actions: Vec<String>,
    pub insufficient_data_actions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alarm_rule: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchAlarmsResponse {
    pub alarms: Vec<CloudWatchAlarm>,
}

/// Latest datapoint summary for a metric series.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchLatestDatapoint {
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

/// One unique metric series as exposed by
/// `GET /_fakecloud/cloudwatch/metrics`, keyed by
/// (account, region, namespace, metricName, dimensions).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchMetric {
    pub account_id: String,
    pub region: String,
    pub namespace: String,
    pub metric_name: String,
    pub dimensions: Vec<CloudWatchDimension>,
    pub datapoint_count: usize,
    /// Most-recent datapoint, or `null` if the series has none.
    pub latest: Option<CloudWatchLatestDatapoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudWatchMetricsResponse {
    pub metrics: Vec<CloudWatchMetric>,
}

// ── Firehose ────────────────────────────────────────────────────────

/// Server-side encryption summary for a Firehose delivery stream as
/// exposed by `GET /_fakecloud/firehose/delivery-streams`. `status` is
/// `ENABLED`/`DISABLED`; `keyType`/`keyArn` are present only when a
/// customer-managed key is configured.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirehoseEncryption {
    /// ENABLED / DISABLED.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_arn: Option<String>,
}

/// One delivery stream as exposed by
/// `GET /_fakecloud/firehose/delivery-streams`. One entry per stream
/// across every account, mirroring what `CreateDeliveryStream` recorded
/// plus its lifecycle and encryption state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirehoseDeliveryStream {
    pub account_id: String,
    pub name: String,
    pub arn: String,
    /// DirectPut / KinesisStreamAsSource.
    pub stream_type: String,
    /// CREATING / ACTIVE / ...
    pub status: String,
    pub encryption: FirehoseEncryption,
    pub destination_count: usize,
    pub create_timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_update_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirehoseDeliveryStreamsResponse {
    pub delivery_streams: Vec<FirehoseDeliveryStream>,
}

// ── Athena ──────────────────────────────────────────────────────────

/// One row in the Athena named-query introspection list returned by
/// `GET /_fakecloud/athena/named-queries`. Mirrors the underlying named
/// query record plus a `last_used_at` timestamp the server bumps every
/// time `StartQueryExecution` resolves the query by id.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AthenaNamedQuery {
    pub named_query_id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub database: String,
    pub query_string: String,
    pub workgroup: String,
    /// RFC3339 timestamp of the most recent `StartQueryExecution` that
    /// resolved its query string from this named query. `None` until the
    /// first such invocation.
    #[serde(default)]
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueJobsResponse {
    pub jobs: Vec<GlueJob>,
}

/// Curated row for `GET /_fakecloud/glue/job-runs`. Includes the
/// full state machine of a JobRun (StartJobRun ledger). Filter by
/// `?job_name=foo` to scope to a single job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueJobRun {
    pub account_id: String,
    pub id: String,
    pub job_name: String,
    pub attempt: i64,
    pub started_on: String,
    pub completed_on: Option<String>,
    pub job_run_state: String,
    pub arguments: std::collections::BTreeMap<String, String>,
    pub error_message: Option<String>,
    pub execution_time: i64,
}

// ── Organizations ───────────────────────────────────────────────────

/// A single member account as exposed by
/// `GET /_fakecloud/organizations/accounts`. Mirrors the AWS
/// Organizations `Account` shape but adds two fakecloud-only fields
/// useful for test assertions: `parentOuId` (resolved parent OU or
/// root) and `scpAttached` (the set of SCP IDs directly attached to
/// the account — does not walk up the hierarchy).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationsAccount {
    pub id: String,
    pub arn: String,
    pub email: String,
    pub name: String,
    /// AWS lifecycle state. One of `ACTIVE`, `SUSPENDED`,
    /// `PENDING_CLOSURE`.
    pub status: String,
    /// How the account entered the organization. One of `INVITED`,
    /// `CREATED`.
    pub joined_method: String,
    /// RFC3339 timestamp the account joined the org.
    pub joined_timestamp: String,
    /// Parent OU or root id. Always set for accounts attached to a
    /// live org; `None` only if the account record is mid-removal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_ou_id: Option<String>,
    /// Tags directly attached to the account (alphabetical by key).
    #[serde(default)]
    pub tags: Vec<OrganizationsTag>,
    /// SCP ids directly attached to the account (alphabetical).
    /// Does not include policies inherited from parent OUs or root.
    #[serde(default)]
    pub scp_attached: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlueJobRunsResponse {
    pub runs: Vec<GlueJobRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AthenaNamedQueriesResponse {
    pub queries: Vec<AthenaNamedQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationsTag {
    pub key: String,
    pub value: String,
}

/// Response body for `GET /_fakecloud/organizations/accounts`.
///
/// `managementAccountId` and `masterAccountId` are duplicates — AWS
/// renamed `Master` to `Management` in 2020 but kept the old field
/// around for back-compat. Both are returned here so SDKs in either
/// vintage match.
///
/// When no organization has been created yet, `accounts` is empty and
/// the account-id fields are `None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationsAccountsResponse {
    pub accounts: Vec<OrganizationsAccount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub management_account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_account_id: Option<String>,
}

/// One billing-responsibility transfer as exposed by
/// `GET /_fakecloud/organizations/responsibility-transfers`. Mirrors the
/// AWS `ResponsibilityTransfer` shape: `direction` is `INBOUND`/`OUTBOUND`,
/// `status` walks the transfer lifecycle, and `activeHandshakeId` points
/// at the handshake the invited org accepts/declines (or `null`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationsResponsibilityTransfer {
    pub id: String,
    pub arn: String,
    pub name: String,
    #[serde(rename = "type")]
    pub transfer_type: String,
    pub status: String,
    /// INBOUND / OUTBOUND.
    pub direction: String,
    pub source_management_account_id: String,
    pub source_management_account_email: String,
    pub target_management_account_id: String,
    pub target_management_account_email: String,
    /// RFC3339 timestamp the transfer was initiated.
    pub start_timestamp: String,
    /// RFC3339 timestamp the transfer concluded, or `null` while open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_handshake_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationsResponsibilityTransfersResponse {
    pub responsibility_transfers: Vec<OrganizationsResponsibilityTransfer>,
}

/// Body for `POST /_fakecloud/cloudfront/distributions/{id}/status`. The
/// admin endpoint flips a stored CloudFront Distribution's status so
/// tests can synchronously force it into `Deployed` or `InProgress`
/// without waiting on the propagation tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudFrontDistributionStatusRequest {
    /// New distribution status. Typically `"Deployed"` or `"InProgress"`.
    pub status: String,
}

// ── ACM (introspection) ─────────────────────────────────────────────

/// Response body for `GET /_fakecloud/acm/certificates/{arn-or-id}/chain-info`.
/// Reports PEM block/byte counts and a `status` / `cert_type` snapshot
/// so tests can verify that uploaded chains round-trip intact. The
/// `external_ca_validated` flag is always `false` to document that
/// fakecloud does not run real X.509 verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AcmCertificateChainInfo {
    pub certificate_arn: String,
    pub certificate_pem_bytes: u64,
    pub certificate_pem_blocks: u64,
    pub chain_pem_bytes: u64,
    pub chain_pem_blocks: u64,
    pub external_ca_validated: bool,
    pub status: String,
    pub cert_type: String,
}

// ── Cognito extras ──────────────────────────────────────────────────

/// Response from `POST /_fakecloud/cognito/compromised-passwords`. Echoes
/// the count of *new* password hashes added to the compromised-credentials
/// set on this call.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompromisedPasswordsResponse {
    pub added: u64,
}

/// One registered WebAuthn credential surfaced by
/// `GET /_fakecloud/cognito/webauthn-credentials`. `attestation_info` is
/// kept as raw JSON because its shape depends on the attestation format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnCredential {
    pub account_id: String,
    pub pool_user: String,
    pub credential_id: String,
    pub relying_party_id: String,
    pub attestation_info: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnCredentialsResponse {
    pub credentials: Vec<WebAuthnCredential>,
}

// ── SES extras (admin responses) ────────────────────────────────────

/// Response from `POST /_fakecloud/ses/identities/{name}/mail-from-status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMailFromStatusResponse {
    pub identity: String,
    pub mail_from_domain_status: String,
}

/// Response from `GET /_fakecloud/ses/identities/{name}/dkim-public-key`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesDkimPublicKeyResponse {
    pub identity: String,
    pub selector: String,
    pub public_key_base64: String,
    pub signing_enabled: bool,
}

/// Response from `POST /_fakecloud/ses/account/sandbox`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesSandboxResponse {
    pub sandbox: bool,
    pub production_access_enabled: bool,
}

/// Response from `GET /_fakecloud/ses/metrics`. fakecloud surfaces a
/// running `suppressedDropsTotal` counter so test code can verify that
/// the suppression list short-circuits sends.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMetricsResponse {
    pub suppressed_drops_total: u64,
}

// ── ELBv2 admin ─────────────────────────────────────────────────────

/// Response from `POST /_fakecloud/elbv2/access-logs/flush`. `flushed`
/// is the number of access-log records flushed to the configured S3
/// bucket on this call.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2AccessLogsFlushResponse {
    pub flushed: u64,
}

// ── API Gateway v2 WebSocket connections ────────────────────────────

/// Single active WebSocket connection tracked by the API Gateway v2
/// fake. Returned by `GET /_fakecloud/apigatewayv2/connections`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayV2Connection {
    pub connection_id: String,
    pub api_id: String,
    pub stage: String,
    pub connected_at: String,
    pub last_active_at: String,
    pub source_ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayV2ConnectionsResponse {
    pub connections: Vec<ApiGatewayV2Connection>,
}

// ── ECS task IAM credentials ────────────────────────────────────────

/// Response shape for `GET /_fakecloud/ecs/creds/{task_id}`. Matches the
/// real ECS task metadata credential endpoint field casing (PascalCase),
/// so this type is `Deserialize` only — fakecloud writes the keys
/// already capitalized.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcsTaskCredentialsResponse {
    #[serde(rename = "AccessKeyId")]
    pub access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    pub secret_access_key: String,
    #[serde(rename = "Token")]
    pub token: String,
    #[serde(rename = "Expiration")]
    pub expiration: String,
    #[serde(rename = "RoleArn")]
    pub role_arn: String,
}

// ── KMS usage (admin) ───────────────────────────────────────────────

/// One recorded KMS data-plane invocation, exposed by
/// `GET /_fakecloud/kms/usage`. Fields mirror the JSON payload emitted
/// by the server's usage recorder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsUsageRecord {
    pub timestamp: String,
    pub operation: String,
    pub service_principal: Option<String>,
    pub account_id: String,
    pub key_arn: String,
    pub encryption_context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsUsageResponse {
    pub records: Vec<KmsUsageRecord>,
}

// ── ELBv2 WAF counts (admin) ────────────────────────────────────────

/// Response body for `GET /_fakecloud/elbv2/waf-counts`. The exact
/// shape of `counts` is service-internal and intentionally left as
/// free-form JSON so we don't have to track every new dimension in
/// the SDK.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elbv2WafCountsResponse {
    pub counts: serde_json::Value,
}

// ── EC2 instances (introspection) ───────────────────────────────────

/// A single EC2 instance as surfaced by `GET /_fakecloud/ec2/instances`.
/// Instances are metadata-faithful today (Docker-backed execution is a
/// roadmap follow-up), so this mirrors the control-plane view without
/// leaking runtime-internal fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ec2Instance {
    pub instance_id: String,
    pub image_id: String,
    pub instance_type: String,
    /// EC2 state name: `pending` | `running` | `shutting-down` |
    /// `terminated` | `stopping` | `stopped`.
    pub state: String,
    pub private_ip: String,
    pub public_ip: Option<String>,
    pub subnet_id: Option<String>,
    pub vpc_id: Option<String>,
    pub key_name: Option<String>,
    pub security_group_ids: Vec<String>,
    pub availability_zone: String,
    pub launch_time: String,
}

/// Response body for `GET /_fakecloud/ec2/instances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ec2InstancesResponse {
    pub instances: Vec<Ec2Instance>,
}
