use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::Engine;
use chrono::Utc;

use fakecloud_core::delivery::{SqsDelivery, SqsDeliveryError, SqsMessageAttribute};

use crate::state::{MessageAttribute, SharedSqsState, SqsMessage};

/// Implements SqsDelivery so other services can push messages into SQS queues.
pub struct SqsDeliveryImpl {
    state: SharedSqsState,
    kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    /// Set whenever a cross-service delivery mutates queue state. The
    /// `SqsDelivery` trait is synchronous and has no access to the async
    /// snapshot writer the service handler uses, so messages injected by
    /// SNS/EventBridge/S3/Scheduler fan-out used to vanish on restart while
    /// direct SendMessage survived. A background flusher in the server polls
    /// this flag and persists, giving delivered messages durability with a
    /// small (sub-second) eventual-consistency window. bug-audit 2026-06-15,
    /// 4.8.
    dirty: Option<Arc<AtomicBool>>,
}

impl SqsDeliveryImpl {
    pub fn new(state: SharedSqsState) -> Self {
        Self {
            state,
            kms_hook: None,
            dirty: None,
        }
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    /// Wire a dirty flag that a background flusher watches to persist
    /// cross-service deliveries. See the `dirty` field. bug-audit 4.8.
    pub fn with_dirty_flag(mut self, dirty: Arc<AtomicBool>) -> Self {
        self.dirty = Some(dirty);
        self
    }

    fn mark_dirty(&self) {
        if let Some(flag) = &self.dirty {
            flag.store(true, Ordering::Release);
        }
    }
}

impl SqsDelivery for SqsDeliveryImpl {
    fn deliver_to_queue(
        &self,
        queue_arn: &str,
        message_body: &str,
        _attributes: &HashMap<String, String>,
    ) {
        self.deliver_to_queue_with_attrs(queue_arn, message_body, &HashMap::new(), None, None);
    }

    fn deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        if let Err(err) = self.try_deliver_to_queue_with_attrs(
            queue_arn,
            message_body,
            message_attributes,
            message_group_id,
            message_dedup_id,
        ) {
            tracing::warn!(%err, queue_arn, "SQS delivery failed");
        }
    }

    fn try_deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) -> Result<(), SqsDeliveryError> {
        let mut accounts = self.state.write();

        // Parse account from queue ARN (arn:aws:sqs:region:ACCOUNT:name)
        let default_id = accounts.default_account_id().to_string();
        let target_account = queue_arn
            .split(':')
            .nth(4)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| SqsDeliveryError::InvalidArn(queue_arn.to_string()))?
            .to_string();
        let target_account = if target_account.is_empty() {
            default_id
        } else {
            target_account
        };
        let region = accounts.region().to_string();
        let state = accounts.get_or_create(&target_account);

        // Find queue by ARN
        let queue = state
            .queues
            .values_mut()
            .find(|q| q.arn == queue_arn)
            .ok_or_else(|| SqsDeliveryError::QueueNotFound(queue_arn.to_string()))?;

        // If queue has SSE-KMS configured and a KMS hook is wired,
        // encrypt the body before storing it. Cross-service injections
        // (SNS fanout, EventBridge target, Scheduler) need the same
        // encryption guarantee as direct SendMessage callers; otherwise
        // SSE-KMS queues silently store plaintext from upstream services.
        let kms_key_id = queue.attributes.get("KmsMasterKeyId").cloned();
        let queue_arn_owned = queue.arn.clone();

        // For FIFO queues without content-based dedup, require explicit dedup ID
        if queue.is_fifo && message_dedup_id.is_none() {
            let content_based = queue
                .attributes
                .get("ContentBasedDeduplication")
                .map(|v| v.as_str())
                == Some("true");
            if !content_based {
                // Real AWS surfaces InvalidParameterValue for FIFO sends
                // missing a dedup ID; silently dropping defeats DLQ
                // routing on the upstream service (SNS / EventBridge /
                // Scheduler). Bubble the error so callers can route to
                // their configured failure target.
                return Err(SqsDeliveryError::InvalidParameter(format!(
                    "FIFO queue {queue_arn} requires MessageDeduplicationId or ContentBasedDeduplication=true"
                )));
            }
        }

        let now = Utc::now();

        let effective_dedup_id = if message_dedup_id.is_some() {
            message_dedup_id.map(|s| s.to_string())
        } else if queue.is_fifo {
            Some(crate::service::sha256_hex(message_body))
        } else {
            None
        };

        // FIFO queues stamp every accepted message with a monotonic
        // sequence_number — receivers depend on it for in-order delivery
        // and replay detection. The sync SendMessage path already does
        // this; cross-service deliveries used to leave it as None,
        // which broke FIFO consumers downstream.
        let sequence_number = if queue.is_fifo {
            let seq = queue.next_sequence_number;
            queue.next_sequence_number = queue.next_sequence_number.saturating_add(1);
            Some(seq.to_string())
        } else {
            None
        };

        let sqs_attrs: BTreeMap<String, MessageAttribute> = message_attributes
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    MessageAttribute {
                        data_type: v.data_type.clone(),
                        string_value: v.string_value.clone(),
                        binary_value: v
                            .binary_value
                            .as_ref()
                            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok()),
                    },
                )
            })
            .collect();

        let stored_body = match (kms_key_id.as_deref(), &self.kms_hook) {
            (Some(key), Some(hook)) if !key.is_empty() => {
                let mut ctx = HashMap::new();
                ctx.insert("aws:sqs:arn".to_string(), queue_arn_owned.clone());
                hook.encrypt(
                    &target_account,
                    &region,
                    key,
                    message_body.as_bytes(),
                    "sqs.amazonaws.com",
                    ctx,
                )
                .map_err(|e| {
                    SqsDeliveryError::InvalidParameter(format!(
                        "SQS SSE-KMS encrypt failed for {queue_arn_owned}: {e}"
                    ))
                })?
            }
            _ => message_body.to_string(),
        };
        // Reacquire mutable queue after the borrow drop above (hook call
        // used `&self.kms_hook` not `state`).
        let queue = accounts
            .get_or_create(&target_account)
            .queues
            .values_mut()
            .find(|q| q.arn == queue_arn_owned)
            .ok_or_else(|| SqsDeliveryError::QueueNotFound(queue_arn_owned.clone()))?;

        let msg = SqsMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            receipt_handle: None,
            md5_of_body: crate::service::md5_hex(message_body),
            body: stored_body,
            sent_timestamp: now.timestamp_millis(),
            attributes: BTreeMap::new(),
            message_attributes: sqs_attrs,
            visible_at: None,
            receive_count: 0,
            message_group_id: message_group_id.map(|s| s.to_string()),
            message_dedup_id: effective_dedup_id,
            created_at: now,
            sequence_number,
        };
        queue.messages.push_back(msg);
        drop(accounts);
        self.mark_dirty();
        tracing::debug!(queue_arn, "delivered message to SQS queue");
        Ok(())
    }
}

/// Persist `state` as an SQS snapshot if it differs from what is on disk.
///
/// Shared by the background flusher and exposed for tests so the durability of
/// cross-service deliveries can be verified with a real save+load round-trip.
/// Returns `Ok(())` even when no store is configured (memory mode). bug-audit
/// 2026-06-15, 4.8.
pub fn persist_sqs_state(
    state: &SharedSqsState,
    store: &Arc<dyn fakecloud_persistence::SnapshotStore>,
) -> std::io::Result<()> {
    let snapshot = crate::state::SqsSnapshot {
        schema_version: crate::state::SQS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    store.save(&bytes)
}

/// Background loop that persists cross-service SQS deliveries.
///
/// The `SqsDelivery` trait is synchronous and has no access to the async
/// snapshot writer the request handler uses, so fan-out messages from
/// SNS/EventBridge/S3/Scheduler were never checkpointed. This loop watches the
/// shared `dirty` flag the delivery impl sets and persists the state whenever
/// it flips, bounding the data-loss window to one poll interval. The flush is
/// offloaded to the blocking pool so the file write never stalls a Tokio
/// worker. bug-audit 2026-06-15, 4.8.
pub async fn run_delivery_flusher(
    state: SharedSqsState,
    store: Arc<dyn fakecloud_persistence::SnapshotStore>,
    dirty: Arc<AtomicBool>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        // swap(false): only persist when there was a delivery since the last
        // flush, and clear before persisting so a delivery racing the flush
        // re-arms the flag rather than being lost.
        if dirty.swap(false, Ordering::AcqRel) {
            let state = state.clone();
            let store = store.clone();
            let dirty = dirty.clone();
            let join = tokio::task::spawn_blocking(move || persist_sqs_state(&state, &store)).await;
            match join {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::warn!(%err, "sqs delivery flush failed; will retry");
                    dirty.store(true, Ordering::Release);
                }
                Err(err) => {
                    tracing::warn!(%err, "sqs delivery flush task panicked");
                    dirty.store(true, Ordering::Release);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{SharedSqsState, SqsQueue, SqsState};
    use chrono::Utc;
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::VecDeque;
    use std::sync::Arc;

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";
    const ENDPOINT: &str = "http://localhost:4566";

    fn make_queue(name: &str, is_fifo: bool, content_based_dedup: bool) -> SqsQueue {
        let mut attributes = BTreeMap::new();
        if content_based_dedup {
            attributes.insert("ContentBasedDeduplication".to_string(), "true".to_string());
        }
        SqsQueue {
            queue_name: name.to_string(),
            queue_url: format!("{ENDPOINT}/{ACCOUNT}/{name}"),
            arn: Arn::new("sqs", REGION, ACCOUNT, name).to_string(),
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
        }
    }

    fn make_state_with_queue(queue: SqsQueue) -> SharedSqsState {
        let mut multi: MultiAccountState<SqsState> =
            MultiAccountState::new(ACCOUNT, REGION, ENDPOINT);
        let state = multi.default_mut();
        state
            .name_to_url
            .insert(queue.queue_name.clone(), queue.queue_url.clone());
        state.queues.insert(queue.queue_url.clone(), queue);
        Arc::new(RwLock::new(multi))
    }

    #[test]
    fn deliver_to_queue_pushes_message() {
        let queue = make_queue("standard", false, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue(&arn, "hello", &HashMap::new());
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages.len(), 1);
        let msg = q.messages.front().unwrap();
        assert_eq!(msg.body, "hello");
        assert!(msg.message_group_id.is_none());
        assert!(msg.message_dedup_id.is_none());
    }

    #[test]
    fn deliver_fifo_without_dedup_id_is_dropped() {
        let queue = make_queue("fifo.fifo", true, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue_with_attrs(&arn, "body", &HashMap::new(), Some("g1"), None);
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert!(q.messages.is_empty());
    }

    #[test]
    fn deliver_fifo_content_based_dedup_generates_id() {
        let queue = make_queue("fifo.fifo", true, true);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue_with_attrs(&arn, "body", &HashMap::new(), Some("g1"), None);
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages.len(), 1);
        assert!(q.messages.front().unwrap().message_dedup_id.is_some());
    }

    #[test]
    fn deliver_fifo_assigns_sequence_number() {
        let queue = make_queue("fifo.fifo", true, true);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue_with_attrs(&arn, "first", &HashMap::new(), Some("g1"), None);
        delivery.deliver_to_queue_with_attrs(&arn, "second", &HashMap::new(), Some("g1"), None);
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages.len(), 2);
        let s0 = q.messages[0].sequence_number.as_deref().unwrap();
        let s1 = q.messages[1].sequence_number.as_deref().unwrap();
        let n0: u64 = s0.parse().expect("decimal sequence number");
        let n1: u64 = s1.parse().expect("decimal sequence number");
        assert_eq!(n1, n0 + 1, "sequence_number must be monotonic");
    }

    #[test]
    fn try_deliver_fifo_no_dedup_returns_invalid_parameter() {
        let queue = make_queue("fifo.fifo", true, false);
        let arn = queue.arn.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        let err = delivery
            .try_deliver_to_queue_with_attrs(&arn, "body", &HashMap::new(), Some("g1"), None)
            .expect_err("should reject FIFO send without dedup id");
        assert!(matches!(err, SqsDeliveryError::InvalidParameter(_)));
    }

    #[test]
    fn deliver_fifo_with_explicit_dedup_id_preserved() {
        let queue = make_queue("fifo.fifo", true, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue_with_attrs(
            &arn,
            "body",
            &HashMap::new(),
            Some("g1"),
            Some("dedup-123"),
        );
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages.len(), 1);
        let msg = q.messages.front().unwrap();
        assert_eq!(msg.message_dedup_id.as_deref(), Some("dedup-123"));
        assert_eq!(msg.message_group_id.as_deref(), Some("g1"));
    }

    #[test]
    fn deliver_includes_string_message_attribute() {
        let queue = make_queue("standard", false, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        let mut attrs = HashMap::new();
        attrs.insert(
            "TraceId".to_string(),
            SqsMessageAttribute {
                data_type: "String".to_string(),
                string_value: Some("abc".to_string()),
                binary_value: None,
            },
        );
        delivery.deliver_to_queue_with_attrs(&arn, "body", &attrs, None, None);
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        let msg = q.messages.front().unwrap();
        let trace = msg.message_attributes.get("TraceId").unwrap();
        assert_eq!(trace.data_type, "String");
        assert_eq!(trace.string_value.as_deref(), Some("abc"));
    }

    #[test]
    fn deliver_decodes_binary_message_attribute() {
        let queue = make_queue("standard", false, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        let encoded = base64::engine::general_purpose::STANDARD.encode([0x01, 0x02, 0x03]);
        let mut attrs = HashMap::new();
        attrs.insert(
            "Blob".to_string(),
            SqsMessageAttribute {
                data_type: "Binary".to_string(),
                string_value: None,
                binary_value: Some(encoded),
            },
        );
        delivery.deliver_to_queue_with_attrs(&arn, "body", &attrs, None, None);
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        let msg = q.messages.front().unwrap();
        let blob = msg.message_attributes.get("Blob").unwrap();
        assert_eq!(
            blob.binary_value.as_deref(),
            Some(&[0x01u8, 0x02, 0x03][..])
        );
    }

    struct StubKmsHook;
    impl fakecloud_core::delivery::KmsHook for StubKmsHook {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            _key_id: &str,
            plaintext: &[u8],
            _service: &str,
            _ctx: HashMap<String, String>,
        ) -> Result<String, String> {
            Ok(format!("ENC:{}", String::from_utf8_lossy(plaintext)))
        }
        fn decrypt(
            &self,
            _account_id: &str,
            _envelope: &str,
            _service: &str,
            _ctx: HashMap<String, String>,
        ) -> Result<Vec<u8>, String> {
            Err("unused".to_string())
        }
    }

    #[test]
    fn cross_service_delivery_encrypts_when_queue_has_kms_key() {
        let mut queue = make_queue("encrypted", false, false);
        queue
            .attributes
            .insert("KmsMasterKeyId".to_string(), "alias/aws/sqs".to_string());
        let url = queue.queue_url.clone();
        let arn = queue.arn.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone()).with_kms_hook(Arc::new(StubKmsHook));
        delivery.deliver_to_queue(&arn, "secret-body", &HashMap::new());
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages.len(), 1);
        assert_eq!(q.messages[0].body, "ENC:secret-body");
    }

    #[test]
    fn cross_service_delivery_skips_encryption_when_no_kms_key() {
        let queue = make_queue("plain", false, false);
        let url = queue.queue_url.clone();
        let arn = queue.arn.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone()).with_kms_hook(Arc::new(StubKmsHook));
        delivery.deliver_to_queue(&arn, "plain-body", &HashMap::new());
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert_eq!(q.messages[0].body, "plain-body");
    }

    // bug-audit 2026-06-15, 4.8: a cross-service delivery must survive a
    // save+load round-trip. Before the dirty-flag + flusher, only the service
    // handler persisted, so SNS/EventBridge fan-out messages vanished on
    // restart while direct SendMessage survived.
    #[test]
    fn delivered_message_survives_save_and_load() {
        use fakecloud_persistence::SnapshotStore;

        let queue = make_queue("durable", false, false);
        let arn = queue.arn.clone();
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);

        let dirty = Arc::new(AtomicBool::new(false));
        let delivery = SqsDeliveryImpl::new(state.clone()).with_dirty_flag(dirty.clone());
        delivery.deliver_to_queue(&arn, "fanned-out", &HashMap::new());
        assert!(
            dirty.load(Ordering::Acquire),
            "delivery must mark state dirty for the flusher"
        );

        // Flush exactly as the background flusher would, then drop in-memory
        // state and reload from the snapshot bytes.
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn SnapshotStore> = Arc::new(
            fakecloud_persistence::DiskSnapshotStore::new(dir.path().join("snapshot.json")),
        );
        persist_sqs_state(&state, &store).unwrap();

        let bytes = store.load().unwrap().expect("snapshot written");
        let snapshot: crate::state::SqsSnapshot = serde_json::from_slice(&bytes).unwrap();
        let accounts = snapshot.accounts.expect("multi-account snapshot");
        let restored = accounts.default_ref();
        let q = restored
            .queues
            .get(&url)
            .expect("queue present after reload");
        assert_eq!(
            q.messages.len(),
            1,
            "delivered message must survive restart"
        );
        assert_eq!(q.messages.front().unwrap().body, "fanned-out");
    }

    #[test]
    fn deliver_unknown_queue_is_noop() {
        let queue = make_queue("standard", false, false);
        let url = queue.queue_url.clone();
        let state = make_state_with_queue(queue);
        let delivery = SqsDeliveryImpl::new(state.clone());
        delivery.deliver_to_queue(
            &Arn::new("sqs", REGION, ACCOUNT, "missing").to_string(),
            "body",
            &HashMap::new(),
        );
        let guard = state.read();
        let q = guard.default_ref().queues.get(&url).unwrap();
        assert!(q.messages.is_empty());
    }
}
