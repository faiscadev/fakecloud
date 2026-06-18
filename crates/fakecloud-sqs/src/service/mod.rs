use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use md5::Md5;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    MessageAttribute, MessageMoveTask, MessageMoveTaskStatus, RedrivePolicy, SharedSqsState,
    SqsMessage, SqsQueue, SqsSnapshot, SqsState, SQS_SNAPSHOT_SCHEMA_VERSION,
};

/// Names of queue attributes whose stored value is a JSON document.
/// We canonicalize these to compact JSON on write so the round-trip
/// through `GetQueueAttributes` matches what Terraform's `jsonencode`
/// emits — real AWS SQS canonicalizes the same way server-side.
const JSON_VALUED_ATTRS: &[&str] = &["Policy", "RedrivePolicy", "RedriveAllowPolicy"];

/// Per-queue configuration used while processing a SendMessageBatch,
/// read once outside the per-entry loop.
pub(crate) struct BatchSendConfig {
    is_fifo: bool,
    content_based_dedup: bool,
    max_message_size: usize,
    queue_delay: Option<i64>,
    now: chrono::DateTime<Utc>,
}

impl BatchSendConfig {
    fn from_queue(queue: &SqsQueue, now: chrono::DateTime<Utc>) -> Self {
        Self {
            is_fifo: queue.is_fifo,
            content_based_dedup: queue
                .attributes
                .get("ContentBasedDeduplication")
                .map(|v| v.as_str())
                == Some("true"),
            max_message_size: queue
                .attributes
                .get("MaximumMessageSize")
                .and_then(|s| s.parse().ok())
                .unwrap_or(262144),
            queue_delay: queue
                .attributes
                .get("DelaySeconds")
                .and_then(|s| s.parse().ok()),
            now,
        }
    }
}

pub(crate) enum BatchEntryOutcome {
    Success(Value),
    Failure(Value),
}

pub struct SqsService {
    state: SharedSqsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    /// Serializes concurrent snapshot writes so the newest observed
    /// state always wins on disk. Without it, two tasks could race
    /// between read-clone and save, leaving older bytes as the final
    /// on-disk state (P1 in Cubic review).
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    pub(crate) region: String,
}

mod auth;
mod dlq;
mod messages;
mod moves;
mod permissions;
mod queues;
mod tags;

impl SqsService {
    pub fn new(state: SharedSqsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            kms_hook: None,
            region: "us-east-1".to_string(),
        }
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

    /// Persist the current in-memory state as a snapshot. Called after
    /// every successful mutating action. Noop when no store is wired.
    ///
    /// The snapshot lock is held across the clone + serialize + write
    /// sequence so concurrent mutators cannot interleave and leave
    /// stale bytes on disk. Serialization and the blocking write are
    /// offloaded to the blocking pool to keep Tokio worker threads
    /// responsive under write-heavy load.
    async fn save_snapshot(&self) {
        save_sqs_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current SQS state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation
    /// provisioner mutates `state` directly and uses this to write a
    /// CFN-provisioned queue through to disk, the same way a direct mutating
    /// API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_sqs_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current SQS state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `SqsService::save_snapshot` and the CloudFormation
/// provisioner's post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_sqs_snapshot(
    state: &SharedSqsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = SqsSnapshot {
        schema_version: SQS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write sqs snapshot"),
        Err(err) => tracing::error!(%err, "sqs snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for SqsService {
    fn service_name(&self) -> &str {
        "sqs"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
            "CreateQueue" => self.create_queue(&req),
            "DeleteQueue" => self.delete_queue(&req),
            "ListQueues" => self.list_queues(&req),
            "GetQueueUrl" => self.get_queue_url(&req),
            "GetQueueAttributes" => self.get_queue_attributes(&req),
            "SetQueueAttributes" => self.set_queue_attributes(&req),
            "SendMessage" => self.send_message(&req),
            "SendMessageBatch" => self.send_message_batch(&req),
            "ReceiveMessage" => self.receive_message(&req).await,
            "DeleteMessage" => self.delete_message(&req),
            "DeleteMessageBatch" => self.delete_message_batch(&req),
            "PurgeQueue" => self.purge_queue(&req),
            "ChangeMessageVisibility" => self.change_message_visibility(&req),
            "ChangeMessageVisibilityBatch" => self.change_message_visibility_batch(&req),
            "ListQueueTags" => self.list_queue_tags(&req),
            "TagQueue" => self.tag_queue(&req),
            "UntagQueue" => self.untag_queue(&req),
            "AddPermission" => self.add_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            "ListDeadLetterSourceQueues" => self.list_dead_letter_source_queues(&req),
            "StartMessageMoveTask" => self.start_message_move_task(&req),
            "CancelMessageMoveTask" => self.cancel_message_move_task(&req),
            "ListMessageMoveTasks" => self.list_message_move_tasks(&req),
            _ => Err(AwsServiceError::action_not_implemented("sqs", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateQueue",
            "DeleteQueue",
            "ListQueues",
            "GetQueueUrl",
            "GetQueueAttributes",
            "SetQueueAttributes",
            "SendMessage",
            "SendMessageBatch",
            "ReceiveMessage",
            "DeleteMessage",
            "DeleteMessageBatch",
            "PurgeQueue",
            "ChangeMessageVisibility",
            "ChangeMessageVisibilityBatch",
            "ListQueueTags",
            "TagQueue",
            "UntagQueue",
            "AddPermission",
            "RemovePermission",
            "ListDeadLetterSourceQueues",
            "StartMessageMoveTask",
            "CancelMessageMoveTask",
            "ListMessageMoveTasks",
        ]
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    /// SQS resources are queue ARNs of the form
    /// `arn:{partition}:sqs:{region}:{account}:{queue-name}`. The queue
    /// name is carried differently depending on the action: queue-targeted
    /// calls pass a `QueueUrl` that we parse to extract the final path
    /// segment; `CreateQueue` and `GetQueueUrl` carry a `QueueName`
    /// parameter instead. Account-level actions (`ListQueues`) target
    /// `*`.
    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        static ACTIONS: &[&str] = &[
            "CreateQueue",
            "DeleteQueue",
            "ListQueues",
            "GetQueueUrl",
            "GetQueueAttributes",
            "SetQueueAttributes",
            "SendMessage",
            "SendMessageBatch",
            "ReceiveMessage",
            "DeleteMessage",
            "DeleteMessageBatch",
            "PurgeQueue",
            "ChangeMessageVisibility",
            "ChangeMessageVisibilityBatch",
            "ListQueueTags",
            "TagQueue",
            "UntagQueue",
            "AddPermission",
            "RemovePermission",
            "ListDeadLetterSourceQueues",
            "StartMessageMoveTask",
            "CancelMessageMoveTask",
            "ListMessageMoveTasks",
        ];
        let action = ACTIONS.iter().copied().find(|a| *a == request.action)?;
        let body = parse_body(request);
        let account = request
            .principal
            .as_ref()
            .map(|p| p.account_id.as_str())
            .unwrap_or(request.account_id.as_str());
        let resource = sqs_resource_for(action, account, &request.region, &body);
        Some(fakecloud_core::auth::IamAction {
            service: "sqs",
            action,
            resource,
        })
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        let mut out = std::collections::BTreeMap::new();
        if action.action == "SendMessage" {
            let body = parse_body(request);
            let attrs = parse_message_attributes(&body);
            for (name, attr) in &attrs {
                let key = format!("sqs:messageattribute.{name}");
                // String-typed attributes contribute their StringValue.
                // Binary / Number attributes fall back to the data type
                // so the key still shows up for existence-style
                // conditions; evaluators that look for a concrete
                // StringEquals value simply safe-fail on mismatch.
                let value = attr
                    .string_value
                    .clone()
                    .unwrap_or_else(|| attr.data_type.clone());
                out.insert(key, vec![value]);
            }
        }
        out
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        if resource_arn == "*" {
            return Some(std::collections::HashMap::new());
        }
        // SQS ARN: arn:{partition}:sqs:{region}:{account}:{queue-name}
        let parts: Vec<&str> = resource_arn.split(':').collect();
        if parts.len() < 6 {
            return None;
        }
        let queue_name = parts[5];
        let account_id = parts[4];
        let _accts = self.state.read();
        let state = _accts.get(account_id)?;
        let queue_url = state.name_to_url.get(queue_name)?;
        let queue = state.queues.get(queue_url)?;
        Some(
            queue
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        match action {
            "CreateQueue" | "TagQueue" => {
                let body = parse_body(request);
                let mut tags = std::collections::HashMap::new();
                // JSON-protocol clients send tags as objects.
                for field in &["tags", "Tags"] {
                    if let Some(obj) = body[field].as_object() {
                        for (k, v) in obj {
                            if let Some(val) = v.as_str() {
                                tags.insert(k.clone(), val.to_string());
                            }
                        }
                    }
                }
                // Query-protocol clients flatten tags into `Tag.N.Key` /
                // `Tag.N.Value` query params (CreateQueue) or
                // `Tags.member.N.Key` / `.Value` (TagQueue). Without
                // walking those, IAM tag conditions evaluate against
                // an empty tag set.
                for prefix in &["Tag.", "Tags.member.", "tags.member.", "tag."] {
                    let mut idx = 1usize;
                    loop {
                        let key_param = format!("{prefix}{idx}.Key");
                        let val_param = format!("{prefix}{idx}.Value");
                        match (
                            request.query_params.get(&key_param),
                            request.query_params.get(&val_param),
                        ) {
                            (Some(k), Some(v)) => {
                                tags.insert(k.clone(), v.clone());
                                idx += 1;
                            }
                            _ => break,
                        }
                    }
                }
                Some(tags)
            }
            _ => Some(std::collections::HashMap::new()),
        }
    }
}

/// Background message-move worker. Drains the source queue one message at a
/// time, sleeping `interval` between moves to enforce the requested rate.
/// Updates the matching task's `messages_moved` counter on each move and
/// flips status to `Completed` when the source drains, or `Cancelled` when
/// the cancel flag is set. Exits silently if the source/destination queue
/// or the task itself disappears (e.g. queue deleted while moving).
#[allow(clippy::too_many_arguments)]
async fn run_message_move_task(
    state_handle: SharedSqsState,
    account_id: String,
    region: String,
    task_handle: String,
    source_url: String,
    destination_arn: Option<String>,
    dlq_targets: Vec<String>,
    interval: std::time::Duration,
    cancel_flag: Arc<AtomicBool>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
) {
    // Persist current state through the same code path SqsService::save_snapshot
    // uses. The background loop mutates queues outside any handler, so callers
    // can't trigger a save on its behalf — failing to checkpoint here means
    // moved messages and task progress are silently rolled back after restart.
    async fn checkpoint(
        state_handle: &SharedSqsState,
        snapshot_store: &Option<Arc<dyn SnapshotStore>>,
        snapshot_lock: &Arc<AsyncMutex<()>>,
    ) {
        let Some(store) = snapshot_store.clone() else {
            return;
        };
        let _guard = snapshot_lock.lock().await;
        let snapshot = SqsSnapshot {
            schema_version: SQS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(state_handle.read().clone()),
            state: None,
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        if let Ok(Err(err)) = join {
            tracing::warn!(%err, "background message-move snapshot save failed");
        }
    }
    enum Step {
        Moved,
        SourceDrained,
        Failed,
    }

    let mut idx: usize = 0;
    loop {
        if cancel_flag.load(Ordering::SeqCst) {
            finalize_move_task(
                &state_handle,
                &account_id,
                &region,
                &task_handle,
                MessageMoveTaskStatus::Cancelled,
            );
            return;
        }

        // Each iteration acquires the write lock for exactly one move and
        // drops it before sleeping or finalizing — `finalize_move_task` also
        // acquires the lock, so we can't hold one when we call it.
        let step: Step = {
            let mut accounts = state_handle.write();
            let state = accounts.get_or_create(&account_id);

            // Source queue must exist (immutable check before any mutation).
            if !state.queues.contains_key(&source_url) {
                drop(accounts);
                finalize_move_task(
                    &state_handle,
                    &account_id,
                    &region,
                    &task_handle,
                    MessageMoveTaskStatus::Failed,
                );
                return;
            }

            // Resolve and verify the target before popping anything off the
            // source — AWS treats the source as the durable copy until the
            // destination commit, so we must never pop a message we can't
            // deliver. If the destination is gone, leave the source untouched
            // and finalize Failed.
            let target_url_opt = if let Some(ref dest_arn) = destination_arn {
                state
                    .queues
                    .values()
                    .find(|q| &q.arn == dest_arn)
                    .map(|q| q.queue_url.clone())
            } else if dlq_targets.is_empty() {
                None
            } else {
                let url = dlq_targets[idx % dlq_targets.len()].clone();
                Some(url)
            };
            match target_url_opt.filter(|u| state.queues.contains_key(u)) {
                None => Step::Failed,
                Some(target_url) => {
                    // Source is guaranteed to exist (checked above); pop one message.
                    let popped = state
                        .queues
                        .get_mut(&source_url)
                        .and_then(|q| q.messages.pop_front());
                    match popped {
                        None => Step::SourceDrained,
                        Some(mut msg) => {
                            msg.visible_at = None;
                            msg.receive_count = 0;
                            match state.queues.get_mut(&target_url) {
                                Some(target_queue) => {
                                    target_queue.messages.push_back(msg);
                                    if destination_arn.is_none() {
                                        idx += 1;
                                    }
                                    if let Some(task) = state
                                        .message_move_tasks
                                        .iter_mut()
                                        .find(|t| t.task_handle == task_handle)
                                    {
                                        task.messages_moved += 1;
                                    }
                                    Step::Moved
                                }
                                None => {
                                    // Target vanished between resolution and
                                    // the get_mut here. Push the message back
                                    // to the source so it isn't lost.
                                    if let Some(source_queue) = state.queues.get_mut(&source_url) {
                                        source_queue.messages.push_front(msg);
                                    }
                                    Step::Failed
                                }
                            }
                        }
                    }
                }
            }
        };

        match step {
            Step::Moved => {
                checkpoint(&state_handle, &snapshot_store, &snapshot_lock).await;
                tokio::time::sleep(interval).await;
            }
            Step::SourceDrained => {
                finalize_move_task(
                    &state_handle,
                    &account_id,
                    &region,
                    &task_handle,
                    MessageMoveTaskStatus::Completed,
                );
                checkpoint(&state_handle, &snapshot_store, &snapshot_lock).await;
                return;
            }
            Step::Failed => {
                finalize_move_task(
                    &state_handle,
                    &account_id,
                    &region,
                    &task_handle,
                    MessageMoveTaskStatus::Failed,
                );
                checkpoint(&state_handle, &snapshot_store, &snapshot_lock).await;
                return;
            }
        }
    }
}

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::query::{query_metadata_only_xml, query_response_xml};

const SQS_NS: &str = "http://queue.amazonaws.com/doc/2012-11-05/";

impl SqsService {}

#[path = "../helpers.rs"]
mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
