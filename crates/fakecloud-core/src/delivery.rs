use std::collections::HashMap;
use std::sync::Arc;

/// Cross-service message delivery.
///
/// Services use this to deliver messages to other services without
/// direct dependencies between service crates. The server wires up
/// the delivery functions at startup.
pub struct DeliveryBus {
    /// Deliver a message to an SQS queue by ARN.
    sqs_sender: Option<Arc<dyn SqsDelivery>>,
    /// Publish a message to an SNS topic by ARN.
    sns_sender: Option<Arc<dyn SnsDelivery>>,
    /// Put an event onto an EventBridge bus.
    eventbridge_sender: Option<Arc<dyn EventBridgeDelivery>>,
    /// Invoke a Lambda function by ARN.
    lambda_invoker: Option<Arc<dyn LambdaDelivery>>,
    /// Put records to a Kinesis Data Stream by ARN.
    kinesis_sender: Option<Arc<dyn KinesisDelivery>>,
    /// Start Step Functions executions.
    stepfunctions_starter: Option<Arc<dyn StepFunctionsDelivery>>,
    /// Start SageMaker pipeline executions (Scheduler sagemaker target).
    sagemaker_pipeline_starter: Option<Arc<dyn SageMakerPipelineDelivery>>,
    /// Write objects to S3 buckets.
    s3_writer: Option<Arc<dyn S3Delivery>>,
    /// Put records into Firehose delivery streams.
    firehose_sender: Option<Arc<dyn FirehoseDelivery>>,
    /// Send a high-level SES SendEmail call (cross-service universal target).
    ses_dispatcher: Option<Arc<dyn SesSendEmailDispatcher>>,
    /// Run an ECS task on a cluster (cross-service universal target).
    ecs_task_runner: Option<Arc<dyn EcsTaskRunner>>,
    /// Register/deregister ELBv2 targets from ECS runtime.
    elbv2_target_registration: Option<Arc<dyn Elbv2TargetRegistration>>,
    /// Publish CloudWatch metric data points (CloudWatch Logs metric
    /// filters extract these on PutLogEvents).
    cloudwatch_metrics: Option<Arc<dyn CloudwatchDelivery>>,
    /// Put log events into CloudWatch Logs log groups. Used by Step
    /// Functions Express execution logging and ECS awslogs driver.
    cloudwatch_logs: Option<Arc<dyn CloudwatchLogsDelivery>>,
    /// Verify a Cognito-issued JWT against the user pool that issued it.
    /// Used by API Gateway v1's `COGNITO_USER_POOLS` authorizer to
    /// validate signature/expiry/audience and extract claims.
    cognito_jwt_verifier: Option<Arc<dyn CognitoJwtVerifier>>,
    /// Encrypt/decrypt via KMS for cross-service SSE (S3 SSE-KMS, SES
    /// S3Action KmsKeyArn, etc.).
    kms_hook: Option<Arc<dyn KmsHook>>,
}

/// Message attribute for SQS delivery from SNS.
#[derive(Debug, Clone)]
pub struct SqsMessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<String>,
}

/// Error returned by fallible SQS delivery. Used by Scheduler's DLQ
/// routing, which must distinguish "target queue missing" from
/// "delivered successfully" to decide whether to send to the
/// `DeadLetterConfig.Arn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqsDeliveryError {
    /// The target queue ARN did not resolve to any existing queue.
    QueueNotFound(String),
    /// The ARN could not be parsed into a valid SQS queue identifier.
    InvalidArn(String),
    /// The message violated a constraint required by the target queue
    /// (e.g. FIFO send missing MessageDeduplicationId without
    /// content-based dedup enabled). Surfaces as a non-retriable
    /// failure so the upstream service can route to its configured DLQ.
    InvalidParameter(String),
    /// The target refused the delivery for lack of authorization (e.g. a
    /// Scheduler target bus in another account whose resource policy does
    /// not allow the schedule's role).
    AccessDenied(String),
    /// The target could not take the delivery right now (e.g. a service's
    /// delivery not wired yet at startup).
    TargetUnavailable(String),
}

impl std::fmt::Display for SqsDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueNotFound(arn) => write!(f, "queue not found: {arn}"),
            Self::InvalidArn(arn) => write!(f, "invalid queue ARN: {arn}"),
            Self::InvalidParameter(msg) => write!(f, "invalid parameter: {msg}"),
            Self::AccessDenied(msg) => write!(f, "access denied: {msg}"),
            Self::TargetUnavailable(msg) => write!(f, "target unavailable: {msg}"),
        }
    }
}

impl std::error::Error for SqsDeliveryError {}

/// Trait for delivering messages to SQS queues.
pub trait SqsDelivery: Send + Sync {
    fn deliver_to_queue(
        &self,
        queue_arn: &str,
        message_body: &str,
        attributes: &HashMap<String, String>,
    );

    /// Deliver with message attributes and FIFO fields
    fn deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        // Default implementation: fall back to simple delivery
        let _ = (message_attributes, message_group_id, message_dedup_id);
        self.deliver_to_queue(queue_arn, message_body, &HashMap::new());
    }

    /// The stored ARN of the queue a QueueUrl (`<endpoint>/<account>/<name>`)
    /// names in `region`, looked up by account and queue name. A QueueUrl
    /// carries no region, so the caller passes the region it is acting in
    /// (the region an SQS request would be signed for). `None` when no such
    /// queue exists there.
    fn queue_arn_for_url(&self, region: &str, queue_url: &str) -> Option<String> {
        let _ = (region, queue_url);
        None
    }

    /// Fallible variant used by Scheduler's DLQ routing. Default
    /// implementation assumes the queue exists (preserving the
    /// fire-and-forget semantics of `deliver_to_queue`); the real SQS
    /// impl overrides this to actually look up the queue and report
    /// `QueueNotFound` so the caller can route to a DLQ.
    fn try_deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) -> Result<(), SqsDeliveryError> {
        self.deliver_to_queue_with_attrs(
            queue_arn,
            message_body,
            message_attributes,
            message_group_id,
            message_dedup_id,
        );
        Ok(())
    }
}

/// Trait for publishing messages to SNS topics.
pub trait SnsDelivery: Send + Sync {
    fn publish_to_topic(&self, topic_arn: &str, message: &str, subject: Option<&str>);

    /// Publish to a FIFO SNS topic carrying the message group/dedup IDs
    /// that downstream SQS subscribers need for ordering. Default impl
    /// drops the IDs so non-FIFO callers don't have to override.
    fn publish_to_topic_fifo(
        &self,
        topic_arn: &str,
        message: &str,
        subject: Option<&str>,
        _message_group_id: Option<&str>,
        _message_dedup_id: Option<&str>,
    ) {
        self.publish_to_topic(topic_arn, message, subject);
    }
}

/// An event another service puts onto an EventBridge bus (S3 bucket
/// notifications, ECS task state changes, Scheduler/Pipes/Step Functions
/// targets, ...).
///
/// `account_id` and `region` name where the event *originates*: the owner
/// account and region of the emitting resource. They are stamped into the
/// event's `account` and `region` fields and are what rule patterns match on,
/// exactly as AWS does for service events. There is deliberately no
/// "default account" form: every caller knows the resource the event is
/// about, and an event that silently lands on another account's bus is
/// invisible to the rules that should have fired.
#[derive(Debug, Clone, Copy)]
pub struct CrossServiceEvent<'a> {
    pub source: &'a str,
    pub detail_type: &'a str,
    /// JSON detail document.
    pub detail: &'a str,
    /// Bus name (a bus owned by `account_id`) or a full event-bus ARN. An ARN
    /// routes the event to the bus owner's account (a cross-account target),
    /// while the event keeps its originating `account` / `region`.
    pub event_bus: &'a str,
    /// Account the event originates in.
    pub account_id: &'a str,
    /// Region the event originates in.
    pub region: &'a str,
    /// ARNs of the resources the event is about (`resources` field).
    pub resources: &'a [String],
    /// The IAM role the source acts as when it puts the event (a schedule's
    /// or pipe's role, a state machine's execution role). A bus in another
    /// account authorizes the put against this principal through its
    /// resource policy; `None` means the originating account's root.
    pub principal_arn: Option<&'a str>,
}

/// Why EventBridge refused a cross-service event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventBridgeDeliveryError {
    /// The target bus belongs to another account and its resource policy
    /// does not allow the source's principal to put events on it. The event
    /// is not stored, archived or delivered.
    #[error("AccessDeniedException: {0}")]
    AccessDenied(String),
    /// EventBridge could not take the event (e.g. delivery not wired yet at
    /// startup). The event is dropped; the source records a failed delivery.
    #[error("InternalException: {0}")]
    Unavailable(String),
}

/// Trait for putting events onto an EventBridge bus from cross-service integrations.
pub trait EventBridgeDelivery: Send + Sync {
    /// Put `event` onto the bus it names, scoped to the account that owns that
    /// bus. The implementation handles rule matching and target delivery.
    /// Returns the new event's ID, or why the put was refused.
    fn put_event(&self, event: &CrossServiceEvent<'_>) -> Result<String, EventBridgeDeliveryError>;
}

/// Trait for invoking Lambda functions from cross-service integrations.
pub trait LambdaDelivery: Send + Sync {
    /// Invoke a Lambda function with the given payload.
    /// The function is identified by ARN. Returns the response bytes on success.
    fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>;
}

/// Trait for putting records to Kinesis Data Streams.
pub trait KinesisDelivery: Send + Sync {
    /// Put a record to a Kinesis stream identified by ARN.
    /// The data should be base64-encoded. partition_key is used for shard distribution.
    fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str);
}

/// Trait for starting Step Functions executions from cross-service integrations.
pub trait StepFunctionsDelivery: Send + Sync {
    /// Start a state machine execution with the given input.
    /// The state machine is identified by ARN.
    fn start_execution(&self, state_machine_arn: &str, input: &str);
}

/// Cross-service SageMaker pipeline dispatch used by EventBridge Scheduler's
/// `sagemaker:pipeline` target. The pipeline is identified by ARN
/// (`arn:aws:sagemaker:<region>:<account>:pipeline/<name>`); `parameters` is the
/// target's `PipelineParameterList`.
pub trait SageMakerPipelineDelivery: Send + Sync {
    fn start_pipeline_execution(&self, pipeline_arn: &str, parameters: &serde_json::Value);
}

/// Cross-service Kinesis Data Firehose dispatch used by services
/// (CloudWatch Logs subscription filters, EventBridge targets) that
/// route records into a delivery stream without depending on the
/// firehose crate directly. ARN form is
/// `arn:aws:firehose:<region>:<account>:deliverystream/<name>`.
pub trait FirehoseDelivery: Send + Sync {
    fn put_record(&self, delivery_stream_arn: &str, data: &[u8]);
}

/// Cross-service S3 writer used by services that need to deliver
/// content to S3 buckets without taking a direct dep on the S3 crate
/// (CloudWatch Logs export tasks, Kinesis Firehose, ELB access logs).
pub trait S3Delivery: Send + Sync {
    /// Put an object to a bucket. Returns the bucket name on success
    /// or an error string the caller can surface in tests / logs.
    fn put_object(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), String>;

    /// Read an object's body. Returns Err when the bucket or key does
    /// not exist or when the body cannot be read. Used by RDS
    /// `RestoreDBInstanceFromS3` to ingest a backup blob without taking
    /// a direct dep on the S3 crate.
    fn get_object(&self, account_id: &str, bucket: &str, key: &str) -> Result<Vec<u8>, String>;
}

/// SES SendEmail dispatch for callers that already speak the AWS SES
/// SendEmail / SendEmailV2 shape (multiple to/cc/bcc, optional subject,
/// text/html bodies). Distinct from `EmailDispatcher`, which is the
/// single-recipient cross-service primitive used by Cognito.
pub trait SesSendEmailDispatcher: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn send_email(
        &self,
        account_id: &str,
        from: &str,
        to: Vec<String>,
        cc: Vec<String>,
        bcc: Vec<String>,
        subject: Option<&str>,
        text_body: Option<&str>,
        html_body: Option<&str>,
    ) -> Result<(), String>;
}

/// Synthesize an ECS RunTask call from outside the ECS crate. Used by
/// EventBridge Scheduler and EventBridge Rules to start tasks without
/// depending directly on `fakecloud_ecs`.
pub trait EcsTaskRunner: Send + Sync {
    fn run_task(
        &self,
        account_id: &str,
        cluster: &str,
        task_definition: &str,
        launch_type: Option<&str>,
        count: usize,
    ) -> Result<(), String>;
}

/// Register/deregister targets with ELBv2 target groups from outside
/// the elbv2 crate. Used by ECS runtime when tasks with load balancers
/// reach RUNNING or STOPPED.
pub trait Elbv2TargetRegistration: Send + Sync {
    fn register_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    );
    fn deregister_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    );
}

/// Publish CloudWatch metric data points from outside the cloudwatch
/// crate. Used by CloudWatch Logs metric filters when an incoming log
/// event matches their pattern.
pub trait CloudwatchDelivery: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn put_metric(
        &self,
        account_id: &str,
        region: &str,
        namespace: &str,
        metric_name: &str,
        value: f64,
        unit: Option<&str>,
        dimensions: std::collections::BTreeMap<String, String>,
        timestamp_ms: i64,
    );
}

/// Put log events into CloudWatch Logs log groups from outside the
/// logs crate. Used by Step Functions Express execution logging and
/// ECS awslogs driver so they can deliver without depending directly
/// on fakecloud_logs.
pub trait CloudwatchLogsDelivery: Send + Sync {
    fn put_log_events(
        &self,
        account_id: &str,
        log_group_name: &str,
        log_stream_name: &str,
        events: &[(i64, String)],
    );
}

/// Outbound email dispatch used by services that emulate AWS flows that
/// route through SES (Cognito verification, etc.) without taking a direct
/// dependency on the SES crate.
pub trait EmailDispatcher: Send + Sync {
    fn send_email(
        &self,
        account_id: &str,
        from: &str,
        to: &str,
        subject: &str,
        body_text: &str,
        body_html: Option<&str>,
    );
}

/// Outbound SMS dispatch used by services that emulate AWS flows that route
/// through SNS phone-number publish (Cognito SMS MFA, etc.).
pub trait SmsDispatcher: Send + Sync {
    fn send_sms(&self, account_id: &str, phone_number: &str, message: &str);
}

/// Cross-service KMS hook: services that accept a `KmsKeyId` (Secrets
/// Manager, SSM `SecureString`, S3 SSE-KMS, SQS / SNS / DynamoDB
/// encrypted resources) call this so that real KMS calls happen, the
/// invocation is recorded for introspection, and the returned blob is
/// decryptable by the public KMS API.
///
/// Encryption context is the AWS-defined per-service map (e.g.
/// `{aws:secretsmanager:secretArn: <arn>}` for Secrets Manager) and is
/// recorded with the call so test code can assert it.
pub trait KmsHook: Send + Sync {
    fn encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<String, String>;

    fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, String>;

    /// Resolve `key_id` (key id, key ARN, alias name or alias ARN) to the
    /// key's ARN, provisioning the AWS-managed `aws/<service>` key on first
    /// use. Services report the resolved ARN on their resource descriptions.
    fn resolve_key_arn(
        &self,
        _account_id: &str,
        _region: &str,
        _key_id: &str,
        _service_principal: &str,
    ) -> Result<String, String> {
        Err("key resolution is not supported by this hook".to_string())
    }

    /// The ARN of the AWS-managed key for `service` (the key behind
    /// `alias/aws/<service>`) in `account_id` and `region`, minted on first
    /// use. AWS-managed keys exist once per account AND region, so each
    /// region gets its own key in its own partition.
    fn aws_managed_key_arn(
        &self,
        _account_id: &str,
        _region: &str,
        _service: &str,
        _service_principal: &str,
    ) -> Result<String, String> {
        Err("AWS-managed key resolution is not supported by this hook".to_string())
    }
}

/// The ARN of the AWS-managed KMS key for `service` (the key behind
/// `alias/aws/<service>`) in `account_id` and `region`, which a service reports
/// for a resource encrypted at rest when the caller named no key. The key is
/// resolved through the KMS hook, which mints it on first use in that account
/// and region (and the region's partition), as real KMS does, so the ARN is a
/// real key `DescribeKey` / `Decrypt` accept. `None` when no hook is wired (a
/// service running outside the server) or resolution fails; a failure is
/// logged, never swallowed.
pub fn aws_managed_kms_key_arn(
    hook: Option<&dyn KmsHook>,
    account_id: &str,
    region: &str,
    service: &str,
) -> Option<String> {
    let hook = hook?;
    let alias = format!("alias/aws/{service}");
    let principal = format!("{service}.amazonaws.com");
    match hook.aws_managed_key_arn(account_id, region, service, &principal) {
        Ok(arn) => Some(arn),
        Err(err) => {
            tracing::warn!(
                %err,
                account_id,
                region,
                alias = %alias,
                "could not resolve the AWS-managed KMS key; reporting no key"
            );
            None
        }
    }
}

/// The KMS key a resource is encrypted with: the caller-named key (`named`,
/// ignored when empty) or, when none is named, the AWS-managed key for
/// `service` in `account_id` and `region` (see [`aws_managed_kms_key_arn`]).
pub fn kms_key_or_aws_managed(
    hook: Option<&dyn KmsHook>,
    named: Option<&str>,
    account_id: &str,
    region: &str,
    service: &str,
) -> Option<String> {
    match named.filter(|k| !k.is_empty()) {
        Some(key) => Some(key.to_string()),
        None => aws_managed_kms_key_arn(hook, account_id, region, service),
    }
}

/// [`kms_key_or_aws_managed`] for services that report the key as a key ARN
/// whatever form the caller named it in (RDS, DocumentDB, Neptune, EBS): a
/// named key is resolved with [`resolve_named_kms_key_arn`].
pub fn kms_key_arn_or_aws_managed(
    hook: Option<&dyn KmsHook>,
    named: Option<&str>,
    account_id: &str,
    region: &str,
    service: &str,
) -> Option<String> {
    match named.filter(|k| !k.is_empty()) {
        Some(key) => Some(resolve_named_kms_key_arn(
            hook, key, account_id, region, service,
        )),
        None => aws_managed_kms_key_arn(hook, account_id, region, service),
    }
}

/// The ARN of the key a caller named (key id, key ARN, alias name or alias
/// ARN) as KMS resolves it: an alias reports its target key's ARN, and an
/// AWS-managed alias (`alias/aws/<svc>`) that region's AWS-managed key, minted
/// on first use. A key KMS cannot resolve (one never created in fakecloud, one
/// in another region than `region`, or no hook wired) is reported as given.
pub fn resolve_named_kms_key_arn(
    hook: Option<&dyn KmsHook>,
    key: &str,
    account_id: &str,
    region: &str,
    service: &str,
) -> String {
    let Some(h) = hook else {
        return key.to_string();
    };
    // A KMS ARN naming another region is a key the resource's region cannot
    // use: report it as given rather than resolving (or minting) anything in
    // that other region.
    if let Some(rest) = key.strip_prefix("arn:") {
        if rest.split(':').nth(2).is_some_and(|r| r != region) {
            return key.to_string();
        }
    }
    // KMS resolves an alias name in `region` and an alias ARN in the region
    // it names; `alias/aws/<svc>` resolves to (minting on first use) that
    // region's AWS-managed key for `<svc>`, provisioned for that service.
    let resource = key.rsplit(':').next().unwrap_or(key);
    let owner = resource
        .strip_prefix("alias/aws/")
        .filter(|s| !s.is_empty())
        .unwrap_or(service);
    let principal = format!("{owner}.amazonaws.com");
    match h.resolve_key_arn(account_id, region, key, &principal) {
        Ok(arn) => arn,
        Err(err) => {
            tracing::debug!(%err, key, "KMS key not resolvable; reporting it as given");
            key.to_string()
        }
    }
}

/// Cognito-issued JWT verification hook. Implementations are wired by
/// fakecloud-server and back the `COGNITO_USER_POOLS` authorizer in
/// API Gateway v1. The verifier validates RS256 signature, exp/nbf,
/// `iss`, and `aud`/`client_id` against the user pool referenced by
/// the authorizer's `providerArns`. On success returns the decoded
/// claims as a JSON object; on failure returns an error string the
/// caller surfaces as `401 Unauthorized`.
pub trait CognitoJwtVerifier: Send + Sync {
    fn verify_token(
        &self,
        account_id: &str,
        user_pool_arn: &str,
        token: &str,
    ) -> Result<serde_json::Value, String>;
}

impl DeliveryBus {
    pub fn new() -> Self {
        Self {
            sqs_sender: None,
            sns_sender: None,
            eventbridge_sender: None,
            lambda_invoker: None,
            kinesis_sender: None,
            stepfunctions_starter: None,
            sagemaker_pipeline_starter: None,
            s3_writer: None,
            firehose_sender: None,
            ses_dispatcher: None,
            ecs_task_runner: None,
            elbv2_target_registration: None,
            cloudwatch_metrics: None,
            cloudwatch_logs: None,
            cognito_jwt_verifier: None,
            kms_hook: None,
        }
    }

    pub fn with_cognito_jwt_verifier(mut self, verifier: Arc<dyn CognitoJwtVerifier>) -> Self {
        self.cognito_jwt_verifier = Some(verifier);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    /// Encrypt plaintext with the supplied KMS key. Returns Err when no
    /// KMS hook is wired or the encryption fails.
    pub fn kms_encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<String, String> {
        match self.kms_hook {
            Some(ref h) => h.encrypt(
                account_id,
                region,
                key_id,
                plaintext,
                service_principal,
                encryption_context,
            ),
            None => Err("KMS hook not configured".to_string()),
        }
    }

    /// Decrypt a KMS ciphertext blob. Returns Err when no KMS hook is
    /// wired or the decryption fails.
    pub fn kms_decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        match self.kms_hook {
            Some(ref h) => h.decrypt(
                account_id,
                ciphertext_b64,
                service_principal,
                encryption_context,
            ),
            None => Err("KMS hook not configured".to_string()),
        }
    }

    /// Verify a Cognito JWT against a user pool. Returns `Err` when no
    /// verifier is wired or when the token fails validation.
    pub fn verify_cognito_jwt(
        &self,
        account_id: &str,
        user_pool_arn: &str,
        token: &str,
    ) -> Result<serde_json::Value, String> {
        match self.cognito_jwt_verifier {
            Some(ref v) => v.verify_token(account_id, user_pool_arn, token),
            None => Err("Cognito JWT verifier not configured".to_string()),
        }
    }

    pub fn with_cloudwatch_metrics(mut self, sender: Arc<dyn CloudwatchDelivery>) -> Self {
        self.cloudwatch_metrics = Some(sender);
        self
    }

    /// Publish a CloudWatch metric data point. Silently no-ops when no
    /// CloudWatch sender is wired (in-process tests not exercising the
    /// metrics path).
    #[allow(clippy::too_many_arguments)]
    pub fn put_cloudwatch_metric(
        &self,
        account_id: &str,
        region: &str,
        namespace: &str,
        metric_name: &str,
        value: f64,
        unit: Option<&str>,
        dimensions: std::collections::BTreeMap<String, String>,
        timestamp_ms: i64,
    ) {
        if let Some(ref sender) = self.cloudwatch_metrics {
            sender.put_metric(
                account_id,
                region,
                namespace,
                metric_name,
                value,
                unit,
                dimensions,
                timestamp_ms,
            );
        }
    }

    pub fn with_cloudwatch_logs(mut self, sender: Arc<dyn CloudwatchLogsDelivery>) -> Self {
        self.cloudwatch_logs = Some(sender);
        self
    }

    /// Put log events to a CloudWatch Logs log group / stream.
    /// Silently no-ops when no CloudWatch Logs sender is wired.
    pub fn put_log_events(
        &self,
        account_id: &str,
        log_group_name: &str,
        log_stream_name: &str,
        events: &[(i64, String)],
    ) {
        if let Some(ref sender) = self.cloudwatch_logs {
            sender.put_log_events(account_id, log_group_name, log_stream_name, events);
        }
    }

    pub fn with_ses_dispatcher(mut self, dispatcher: Arc<dyn SesSendEmailDispatcher>) -> Self {
        self.ses_dispatcher = Some(dispatcher);
        self
    }

    pub fn with_ecs_task_runner(mut self, runner: Arc<dyn EcsTaskRunner>) -> Self {
        self.ecs_task_runner = Some(runner);
        self
    }

    pub fn with_elbv2_target_registration(mut self, reg: Arc<dyn Elbv2TargetRegistration>) -> Self {
        self.elbv2_target_registration = Some(reg);
        self
    }

    /// Register targets with an ELBv2 target group. Silently no-ops when
    /// no ELBv2 target registration hook is wired.
    pub fn register_elbv2_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    ) {
        if let Some(ref reg) = self.elbv2_target_registration {
            reg.register_targets(account_id, target_group_arn, targets);
        }
    }

    /// Deregister targets from an ELBv2 target group. Silently no-ops
    /// when no ELBv2 target registration hook is wired.
    pub fn deregister_elbv2_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    ) {
        if let Some(ref reg) = self.elbv2_target_registration {
            reg.deregister_targets(account_id, target_group_arn, targets);
        }
    }

    /// Send an email via SES. Returns `Err` when no SES dispatcher is
    /// wired or the underlying impl rejects (bad source/dest).
    #[allow(clippy::too_many_arguments)]
    pub fn send_ses_email(
        &self,
        account_id: &str,
        from: &str,
        to: Vec<String>,
        cc: Vec<String>,
        bcc: Vec<String>,
        subject: Option<&str>,
        text_body: Option<&str>,
        html_body: Option<&str>,
    ) -> Result<(), String> {
        match self.ses_dispatcher {
            Some(ref d) => {
                d.send_email(account_id, from, to, cc, bcc, subject, text_body, html_body)
            }
            None => Err("SES dispatcher not configured".to_string()),
        }
    }

    /// Run an ECS task. Returns `Err` when no ECS runner is wired or
    /// the impl rejects (unknown cluster / task definition).
    pub fn run_ecs_task(
        &self,
        account_id: &str,
        cluster: &str,
        task_definition: &str,
        launch_type: Option<&str>,
        count: usize,
    ) -> Result<(), String> {
        match self.ecs_task_runner {
            Some(ref r) => r.run_task(account_id, cluster, task_definition, launch_type, count),
            None => Err("ECS task runner not configured".to_string()),
        }
    }

    pub fn with_s3(mut self, sender: Arc<dyn S3Delivery>) -> Self {
        self.s3_writer = Some(sender);
        self
    }

    pub fn with_firehose(mut self, sender: Arc<dyn FirehoseDelivery>) -> Self {
        self.firehose_sender = Some(sender);
        self
    }

    /// Send a single record to a Firehose delivery stream by ARN.
    /// Silently no-ops when no Firehose sender is wired (in-process
    /// tests). Production wiring goes through fakecloud_firehose.
    pub fn put_record_to_firehose(&self, delivery_stream_arn: &str, data: &[u8]) {
        if let Some(ref sender) = self.firehose_sender {
            sender.put_record(delivery_stream_arn, data);
        }
    }

    /// Write content to S3. Returns Err when no S3 writer is wired or
    /// when the underlying impl rejects the bucket / payload.
    pub fn put_object_to_s3(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), String> {
        match self.s3_writer {
            Some(ref sender) => sender.put_object(account_id, bucket, key, body, content_type),
            None => Err("S3 writer not configured".to_string()),
        }
    }

    /// Read content from S3. Returns Err when no S3 client is wired or
    /// when the underlying impl cannot resolve the object.
    pub fn get_object_from_s3(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<u8>, String> {
        match self.s3_writer {
            Some(ref sender) => sender.get_object(account_id, bucket, key),
            None => Err("S3 client not configured".to_string()),
        }
    }

    pub fn with_sqs(mut self, sender: Arc<dyn SqsDelivery>) -> Self {
        self.sqs_sender = Some(sender);
        self
    }

    pub fn with_sns(mut self, sender: Arc<dyn SnsDelivery>) -> Self {
        self.sns_sender = Some(sender);
        self
    }

    pub fn with_eventbridge(mut self, sender: Arc<dyn EventBridgeDelivery>) -> Self {
        self.eventbridge_sender = Some(sender);
        self
    }

    pub fn with_lambda(mut self, invoker: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_invoker = Some(invoker);
        self
    }

    pub fn with_kinesis(mut self, sender: Arc<dyn KinesisDelivery>) -> Self {
        self.kinesis_sender = Some(sender);
        self
    }

    /// Put a record to a Kinesis Data Stream identified by ARN.
    /// Silently no-ops when no Kinesis sender is wired.
    pub fn put_record_to_kinesis(&self, stream_arn: &str, data: &str, partition_key: &str) {
        if let Some(ref sender) = self.kinesis_sender {
            sender.put_record(stream_arn, data, partition_key);
        }
    }

    pub fn with_sagemaker_pipeline(mut self, starter: Arc<dyn SageMakerPipelineDelivery>) -> Self {
        self.sagemaker_pipeline_starter = Some(starter);
        self
    }

    /// Start a SageMaker pipeline execution if a starter is wired.
    pub fn start_sagemaker_pipeline(&self, pipeline_arn: &str, parameters: &serde_json::Value) {
        if let Some(ref starter) = self.sagemaker_pipeline_starter {
            starter.start_pipeline_execution(pipeline_arn, parameters);
        }
    }

    pub fn with_stepfunctions(mut self, starter: Arc<dyn StepFunctionsDelivery>) -> Self {
        self.stepfunctions_starter = Some(starter);
        self
    }

    /// The stored ARN of the SQS queue a QueueUrl names in `region`, if it
    /// exists.
    pub fn sqs_queue_arn_for_url(&self, region: &str, queue_url: &str) -> Option<String> {
        self.sqs_sender
            .as_ref()?
            .queue_arn_for_url(region, queue_url)
    }

    /// Send a message to an SQS queue identified by ARN.
    pub fn send_to_sqs(
        &self,
        queue_arn: &str,
        message_body: &str,
        attributes: &HashMap<String, String>,
    ) {
        if let Some(ref sender) = self.sqs_sender {
            sender.deliver_to_queue(queue_arn, message_body, attributes);
        }
    }

    /// Send a message to an SQS queue with message attributes and FIFO fields.
    pub fn send_to_sqs_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        if let Some(ref sender) = self.sqs_sender {
            sender.deliver_to_queue_with_attrs(
                queue_arn,
                message_body,
                message_attributes,
                message_group_id,
                message_dedup_id,
            );
        }
    }

    /// Fallible SQS send — returns `Err` when the target queue does not
    /// exist, so callers (Scheduler) can route to a DLQ. Returns
    /// `Err(QueueNotFound)` when no SQS sender is wired up at all,
    /// matching the "target unreachable" semantics Scheduler relies on.
    pub fn try_send_to_sqs_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) -> Result<(), SqsDeliveryError> {
        match self.sqs_sender {
            Some(ref sender) => sender.try_deliver_to_queue_with_attrs(
                queue_arn,
                message_body,
                message_attributes,
                message_group_id,
                message_dedup_id,
            ),
            None => Err(SqsDeliveryError::QueueNotFound(queue_arn.to_string())),
        }
    }

    /// Publish a message to an SNS topic identified by ARN.
    pub fn publish_to_sns(&self, topic_arn: &str, message: &str, subject: Option<&str>) {
        if let Some(ref sender) = self.sns_sender {
            sender.publish_to_topic(topic_arn, message, subject);
        }
    }

    /// Put a cross-service event onto an EventBridge bus (see
    /// [`CrossServiceEvent`] for how the bus and account are resolved).
    /// `Ok(None)` when no EventBridge sender is wired.
    pub fn put_event_to_eventbridge(
        &self,
        event: &CrossServiceEvent<'_>,
    ) -> Result<Option<String>, EventBridgeDeliveryError> {
        match self.eventbridge_sender {
            Some(ref sender) => sender.put_event(event).map(Some),
            None => Ok(None),
        }
    }

    /// Invoke a Lambda function identified by ARN.
    pub async fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> Option<Result<Vec<u8>, String>> {
        if let Some(ref invoker) = self.lambda_invoker {
            Some(invoker.invoke_lambda(function_arn, payload).await)
        } else {
            None
        }
    }

    /// Put a record to a Kinesis stream identified by ARN.
    pub fn send_to_kinesis(&self, stream_arn: &str, data: &str, partition_key: &str) {
        if let Some(ref sender) = self.kinesis_sender {
            sender.put_record(stream_arn, data, partition_key);
        }
    }

    /// Start a Step Functions execution identified by state machine ARN.
    pub fn start_stepfunctions_execution(&self, state_machine_arn: &str, input: &str) {
        if let Some(ref starter) = self.stepfunctions_starter {
            starter.start_execution(state_machine_arn, input);
        }
    }
}

impl Default for DeliveryBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Mock implementations
    struct MockSqs {
        call_count: AtomicUsize,
    }
    impl SqsDelivery for MockSqs {
        fn deliver_to_queue(
            &self,
            _queue_arn: &str,
            _message_body: &str,
            _attributes: &HashMap<String, String>,
        ) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct MockSns {
        call_count: AtomicUsize,
    }
    impl SnsDelivery for MockSns {
        fn publish_to_topic(&self, _topic_arn: &str, _message: &str, _subject: Option<&str>) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct MockEventBridge {
        call_count: AtomicUsize,
    }
    impl EventBridgeDelivery for MockEventBridge {
        fn put_event(
            &self,
            _event: &CrossServiceEvent<'_>,
        ) -> Result<String, EventBridgeDeliveryError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok("id".to_string())
        }
    }

    fn test_event<'a>(source: &'a str, bus: &'a str) -> CrossServiceEvent<'a> {
        CrossServiceEvent {
            source,
            detail_type: "type",
            detail: "{}",
            event_bus: bus,
            account_id: "111111111111",
            region: "eu-west-1",
            resources: &[],
            principal_arn: None,
        }
    }

    struct MockKinesis {
        call_count: AtomicUsize,
    }
    impl KinesisDelivery for MockKinesis {
        fn put_record(&self, _stream_arn: &str, _data: &str, _partition_key: &str) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct MockStepFunctions {
        call_count: AtomicUsize,
    }
    impl StepFunctionsDelivery for MockStepFunctions {
        fn start_execution(&self, _state_machine_arn: &str, _input: &str) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn delivery_bus_new_has_no_senders() {
        let bus = DeliveryBus::new();
        // Calling methods without senders should be no-ops
        bus.send_to_sqs("arn:queue", "body", &HashMap::new());
        bus.publish_to_sns("arn:topic", "msg", None);
        let _ = bus.put_event_to_eventbridge(&test_event("src", "default"));
        bus.send_to_kinesis("arn:stream", "data", "pk");
        bus.start_stepfunctions_execution("arn:sfn", "{}");
        // No panics = success
    }

    #[test]
    fn delivery_bus_default_is_same_as_new() {
        let bus = DeliveryBus::default();
        bus.send_to_sqs("arn:q", "b", &HashMap::new());
    }

    #[test]
    fn send_to_sqs_calls_sender() {
        let mock = Arc::new(MockSqs {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_sqs(mock.clone());

        bus.send_to_sqs("arn:queue", "msg", &HashMap::new());
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);

        bus.send_to_sqs("arn:queue2", "msg2", &HashMap::new());
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn send_to_sqs_with_attrs_calls_sender() {
        let mock = Arc::new(MockSqs {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_sqs(mock.clone());

        let mut attrs = HashMap::new();
        attrs.insert(
            "key".to_string(),
            SqsMessageAttribute {
                data_type: "String".to_string(),
                string_value: Some("val".to_string()),
                binary_value: None,
            },
        );
        bus.send_to_sqs_with_attrs("arn:q", "body", &attrs, Some("group"), Some("dedup"));
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn publish_to_sns_calls_sender() {
        let mock = Arc::new(MockSns {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_sns(mock.clone());

        bus.publish_to_sns("arn:topic", "message", Some("subject"));
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn put_event_to_eventbridge_calls_sender() {
        let mock = Arc::new(MockEventBridge {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_eventbridge(mock.clone());

        let _ = bus.put_event_to_eventbridge(&test_event("aws.s3", "default"));
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn send_to_kinesis_calls_sender() {
        let mock = Arc::new(MockKinesis {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_kinesis(mock.clone());

        bus.send_to_kinesis("arn:stream", "data", "partition-key");
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn start_stepfunctions_calls_sender() {
        let mock = Arc::new(MockStepFunctions {
            call_count: AtomicUsize::new(0),
        });
        let bus = DeliveryBus::new().with_stepfunctions(mock.clone());

        bus.start_stepfunctions_execution("arn:sfn:machine", r#"{"key":"val"}"#);
        assert_eq!(mock.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn builder_chaining_works() {
        let sqs = Arc::new(MockSqs {
            call_count: AtomicUsize::new(0),
        });
        let sns = Arc::new(MockSns {
            call_count: AtomicUsize::new(0),
        });
        let eb = Arc::new(MockEventBridge {
            call_count: AtomicUsize::new(0),
        });
        let kin = Arc::new(MockKinesis {
            call_count: AtomicUsize::new(0),
        });
        let sfn = Arc::new(MockStepFunctions {
            call_count: AtomicUsize::new(0),
        });

        let bus = DeliveryBus::new()
            .with_sqs(sqs.clone())
            .with_sns(sns.clone())
            .with_eventbridge(eb.clone())
            .with_kinesis(kin.clone())
            .with_stepfunctions(sfn.clone());

        bus.send_to_sqs("q", "m", &HashMap::new());
        bus.publish_to_sns("t", "m", None);
        let _ = bus.put_event_to_eventbridge(&test_event("s", "b"));
        bus.send_to_kinesis("s", "d", "k");
        bus.start_stepfunctions_execution("sm", "{}");

        assert_eq!(sqs.call_count.load(Ordering::SeqCst), 1);
        assert_eq!(sns.call_count.load(Ordering::SeqCst), 1);
        assert_eq!(eb.call_count.load(Ordering::SeqCst), 1);
        assert_eq!(kin.call_count.load(Ordering::SeqCst), 1);
        assert_eq!(sfn.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invoke_lambda_returns_none_without_invoker() {
        let bus = DeliveryBus::new();
        let result = bus.invoke_lambda("arn:lambda", "{}").await;
        assert!(result.is_none());
    }
}
