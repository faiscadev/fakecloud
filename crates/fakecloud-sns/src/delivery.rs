use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::{DeliveryBus, SnsDelivery};

use crate::state::{PublishedMessage, SharedSnsState};

/// Implements SnsDelivery so other services (EventBridge, S3 events, ...)
/// can publish to SNS topics. Shares the per-protocol fan-out helpers
/// with the direct `Publish` op so filter policies, FIFO group/dedup
/// IDs, HTTP retries with DLQ redrive, and Lambda invocations behave
/// identically regardless of which entry point produced the message.
pub struct SnsDeliveryImpl {
    state: SharedSnsState,
    delivery: Arc<DeliveryBus>,
}

impl SnsDeliveryImpl {
    pub fn new(state: SharedSnsState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }
}

impl SnsDeliveryImpl {
    fn fan_out(
        &self,
        topic_arn: &str,
        message: &str,
        subject: Option<&str>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        // Resolve the target account from the topic ARN, record the
        // published message, and pull the per-topic subscriber list under
        // the write lock. We then drop the lock before doing any
        // blocking I/O (HTTP retries, lambda invocations, SQS push).
        let (subscribers, msg_id, endpoint) = {
            let mut accounts = self.state.write();
            let default_id = accounts.default_account_id().to_string();
            let target_account = topic_arn.split(':').nth(4).unwrap_or(&default_id);
            let state = accounts.get_or_create(target_account);

            if !state.topics.contains_key(topic_arn) {
                tracing::warn!(topic_arn, "SNS delivery target topic not found");
                return;
            }

            let msg_id = uuid::Uuid::new_v4().to_string();
            let attrs: BTreeMap<String, crate::state::MessageAttribute> = BTreeMap::new();
            state.published.push(PublishedMessage {
                message_id: msg_id.clone(),
                topic_arn: topic_arn.to_string(),
                message: message.to_string(),
                subject: subject.map(|s| s.to_string()),
                message_attributes: attrs.clone(),
                message_group_id: message_group_id.map(|s| s.to_string()),
                message_dedup_id: message_dedup_id.map(|s| s.to_string()),
                timestamp: Utc::now(),
            });

            let subscribers =
                crate::service::collect_topic_subscribers(state, topic_arn, &attrs, message);
            let endpoint = state.endpoint.clone();
            (subscribers, msg_id, endpoint)
        };

        // Cross-service publishes don't carry typed message attributes
        // (S3 events, EventBridge targets serialize the whole payload
        // into the Message body), so the envelope attributes map is
        // empty. The direct `Publish` op populates this from
        // `MessageAttributes.entry.N.*` query params.
        let empty_attrs: BTreeMap<String, crate::state::MessageAttribute> = BTreeMap::new();
        let envelope_attrs = serde_json::Map::new();

        let ctx = crate::service::TopicFanoutContext {
            msg_id: &msg_id,
            topic_arn,
            subject,
            endpoint: &endpoint,
            default_message: message,
            structure: None,
            envelope_attrs: &envelope_attrs,
            message_attributes: &empty_attrs,
            message_group_id,
            message_dedup_id,
        };

        crate::service::fan_out_to_subscribers(&self.state, &self.delivery, &subscribers, &ctx);
    }
}

impl SnsDelivery for SnsDeliveryImpl {
    fn publish_to_topic(&self, topic_arn: &str, message: &str, subject: Option<&str>) {
        self.fan_out(topic_arn, message, subject, None, None);
    }

    fn publish_to_topic_fifo(
        &self,
        topic_arn: &str,
        message: &str,
        subject: Option<&str>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        self.fan_out(
            topic_arn,
            message,
            subject,
            message_group_id,
            message_dedup_id,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{SharedSnsState, SnsState, SnsSubscription, SnsTopic};
    use chrono::Utc;
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::{BTreeMap, HashMap};

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";
    const ENDPOINT: &str = "http://localhost:4566";

    fn make_topic(name: &str) -> (SnsTopic, String) {
        let arn = Arn::new("sns", REGION, ACCOUNT, name).to_string();
        let topic = SnsTopic {
            topic_arn: arn.clone(),
            name: name.to_string(),
            attributes: BTreeMap::new(),
            tags: Vec::new(),
            is_fifo: false,
            created_at: Utc::now(),
            subscriptions_deleted: 0,
            fifo_sequence: 0,
            dedup_cache: std::collections::BTreeMap::new(),
        };
        (topic, arn)
    }

    fn make_subscription(
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        confirmed: bool,
    ) -> SnsSubscription {
        let sub_arn = format!("{topic_arn}:{}", uuid::Uuid::new_v4());
        SnsSubscription {
            subscription_arn: sub_arn,
            topic_arn: topic_arn.to_string(),
            protocol: protocol.to_string(),
            endpoint: endpoint.to_string(),
            owner: ACCOUNT.to_string(),
            attributes: BTreeMap::new(),
            confirmed,
            confirmation_token: None,
        }
    }

    fn make_state(topics: Vec<SnsTopic>, subs: Vec<SnsSubscription>) -> SharedSnsState {
        let mut multi: MultiAccountState<SnsState> =
            MultiAccountState::new(ACCOUNT, REGION, ENDPOINT);
        let state = multi.default_mut();
        for t in topics {
            state.topics.insert(t.topic_arn.clone(), t);
        }
        let mut sub_map: BTreeMap<String, SnsSubscription> = BTreeMap::new();
        for s in subs {
            sub_map.insert(s.subscription_arn.clone(), s);
        }
        state.subscriptions = sub_map;
        Arc::new(RwLock::new(multi))
    }

    #[test]
    fn publish_records_message_in_topic() {
        let (topic, arn) = make_topic("t1");
        let state = make_state(vec![topic], vec![]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "hello", Some("hi"));
        let guard = state.read();
        let s = guard.default_ref();
        assert_eq!(s.published.len(), 1);
        let msg = &s.published[0];
        assert_eq!(msg.message, "hello");
        assert_eq!(msg.subject.as_deref(), Some("hi"));
        assert_eq!(msg.topic_arn, arn);
    }

    #[test]
    fn publish_unknown_topic_noop() {
        let (topic, _arn) = make_topic("t1");
        let state = make_state(vec![topic], vec![]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(
            &Arn::new("sns", REGION, ACCOUNT, "unknown").to_string(),
            "body",
            None,
        );
        let guard = state.read();
        assert!(guard.default_ref().published.is_empty());
    }

    #[test]
    fn publish_records_email_delivery() {
        let (topic, arn) = make_topic("t1");
        let sub = make_subscription(&arn, "email", "user@example.com", true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "greetings", Some("subj"));
        let guard = state.read();
        let s = guard.default_ref();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].email_address, "user@example.com");
        assert_eq!(s.sent_emails[0].message, "greetings");
        assert_eq!(s.sent_emails[0].subject.as_deref(), Some("subj"));
    }

    #[test]
    fn publish_email_invokes_smtp_relay_when_env_set() {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut log = Vec::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            stream.write_all(b"220 stub\r\n").unwrap();
            let mut in_data = false;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let trimmed = line.trim_end().to_string();
                log.push(trimmed.clone());
                if in_data {
                    if trimmed == "." {
                        stream.write_all(b"250 ok\r\n").unwrap();
                        in_data = false;
                    }
                    continue;
                }
                let up = trimmed.to_uppercase();
                if up.starts_with("HELO") || up.starts_with("EHLO") {
                    stream.write_all(b"250 hello\r\n").unwrap();
                } else if up.starts_with("MAIL FROM") || up.starts_with("RCPT TO") {
                    stream.write_all(b"250 ok\r\n").unwrap();
                } else if up == "DATA" {
                    stream.write_all(b"354 send data\r\n").unwrap();
                    in_data = true;
                } else if up == "QUIT" {
                    stream.write_all(b"221 bye\r\n").unwrap();
                    break;
                } else {
                    stream.write_all(b"250 ok\r\n").unwrap();
                }
            }
            tx.send(log).ok();
        });
        // SAFETY: tests in the same process share env; setting and
        // unsetting around the call is OK because the publish path is
        // synchronous.
        std::env::set_var(
            "FAKECLOUD_SES_SMTP_RELAY",
            format!("smtp://127.0.0.1:{port}"),
        );
        let (topic, arn) = make_topic("t1");
        let sub = make_subscription(&arn, "email", "user@example.com", true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "greetings", Some("subj"));
        std::env::remove_var("FAKECLOUD_SES_SMTP_RELAY");
        let log = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("smtp stub did not receive mail");
        assert!(log
            .iter()
            .any(|l| l.starts_with("RCPT TO:<user@example.com>")));
        assert!(log.iter().any(|l| l == "Subject: subj"));
        assert!(log.iter().any(|l| l == "greetings"));
    }

    #[test]
    fn publish_records_sms_delivery() {
        let (topic, arn) = make_topic("t1");
        let sub = make_subscription(&arn, "sms", "+14155550199", true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "text-body", None);
        let guard = state.read();
        let s = guard.default_ref();
        assert_eq!(s.sms_messages.len(), 1);
        assert_eq!(s.sms_messages[0].0, "+14155550199");
        assert_eq!(s.sms_messages[0].1, "text-body");
    }

    #[test]
    fn publish_unconfirmed_subscription_skipped() {
        let (topic, arn) = make_topic("t1");
        let sub = make_subscription(&arn, "email", "user@example.com", false);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "msg", None);
        let guard = state.read();
        let s = guard.default_ref();
        assert!(s.sent_emails.is_empty());
        assert_eq!(s.published.len(), 1);
    }

    #[tokio::test]
    async fn publish_records_lambda_invocation() {
        let (topic, arn) = make_topic("t1");
        let fn_arn = Arn::new("lambda", REGION, ACCOUNT, "function:myFn").to_string();
        let sub = make_subscription(&arn, "lambda", &fn_arn, true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new());
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "payload", Some("s"));
        let guard = state.read();
        let s = guard.default_ref();
        assert_eq!(s.lambda_invocations.len(), 1);
        assert_eq!(s.lambda_invocations[0].function_arn, fn_arn);
        assert_eq!(s.lambda_invocations[0].message, "payload");
    }

    #[tokio::test]
    async fn publish_with_sqs_subscriber_calls_delivery_bus() {
        use fakecloud_core::delivery::SqsDelivery;
        use std::sync::Mutex;

        struct Call {
            queue_arn: String,
            body: String,
        }

        #[derive(Default)]
        struct Recorder {
            calls: Mutex<Vec<Call>>,
        }

        impl SqsDelivery for Recorder {
            fn deliver_to_queue(
                &self,
                queue_arn: &str,
                message_body: &str,
                _attrs: &HashMap<String, String>,
            ) {
                self.calls.lock().unwrap().push(Call {
                    queue_arn: queue_arn.to_string(),
                    body: message_body.to_string(),
                });
            }

            fn deliver_to_queue_with_attrs(
                &self,
                queue_arn: &str,
                message_body: &str,
                _attrs: &HashMap<String, fakecloud_core::delivery::SqsMessageAttribute>,
                _group: Option<&str>,
                _dedup: Option<&str>,
            ) {
                self.calls.lock().unwrap().push(Call {
                    queue_arn: queue_arn.to_string(),
                    body: message_body.to_string(),
                });
            }
        }

        let recorder = Arc::new(Recorder::default());
        let (topic, arn) = make_topic("t1");
        let q_arn = Arn::new("sqs", REGION, ACCOUNT, "q1").to_string();
        let sub = make_subscription(&arn, "sqs", &q_arn, true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic(&arn, "hello-sqs", Some("s"));
        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].queue_arn, q_arn);
        let envelope: serde_json::Value = serde_json::from_str(&calls[0].body).unwrap();
        assert_eq!(envelope["Type"], "Notification");
        assert_eq!(envelope["Message"], "hello-sqs");
        assert_eq!(envelope["Subject"], "s");
        assert_eq!(envelope["TopicArn"], arn);
    }

    /// Cross-service publish to a FIFO topic must forward MessageGroupId
    /// and MessageDeduplicationId to SQS subscribers, matching the
    /// direct `Publish` op.
    #[tokio::test]
    async fn fifo_publish_forwards_group_and_dedup_to_sqs() {
        use fakecloud_core::delivery::SqsDelivery;
        use std::sync::Mutex;

        #[derive(Default)]
        struct Recorder {
            calls: Mutex<Vec<(Option<String>, Option<String>)>>,
        }

        impl SqsDelivery for Recorder {
            fn deliver_to_queue(
                &self,
                _queue_arn: &str,
                _message_body: &str,
                _attrs: &HashMap<String, String>,
            ) {
                self.calls.lock().unwrap().push((None, None));
            }

            fn deliver_to_queue_with_attrs(
                &self,
                _queue_arn: &str,
                _message_body: &str,
                _attrs: &HashMap<String, fakecloud_core::delivery::SqsMessageAttribute>,
                group: Option<&str>,
                dedup: Option<&str>,
            ) {
                self.calls
                    .lock()
                    .unwrap()
                    .push((group.map(|s| s.into()), dedup.map(|s| s.into())));
            }
        }

        let recorder = Arc::new(Recorder::default());
        let (topic, arn) = make_topic("t1.fifo");
        let q_arn = Arn::new("sqs", REGION, ACCOUNT, "q1.fifo").to_string();
        let sub = make_subscription(&arn, "sqs", &q_arn, true);
        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);
        delivery.publish_to_topic_fifo(&arn, "fifo-body", None, Some("group-A"), Some("dedup-1"));
        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.as_deref(), Some("group-A"));
        assert_eq!(calls[0].1.as_deref(), Some("dedup-1"));
    }

    /// A subscription whose FilterPolicy doesn't match the message body
    /// is skipped on the cross-service publish path, just like the
    /// direct `Publish` op does.
    #[tokio::test]
    async fn filter_policy_drops_non_matching_subscriber() {
        use fakecloud_core::delivery::SqsDelivery;
        use std::sync::Mutex;

        #[derive(Default)]
        struct Recorder {
            count: Mutex<usize>,
        }

        impl SqsDelivery for Recorder {
            fn deliver_to_queue(
                &self,
                _queue_arn: &str,
                _message_body: &str,
                _attrs: &HashMap<String, String>,
            ) {
                *self.count.lock().unwrap() += 1;
            }

            fn deliver_to_queue_with_attrs(
                &self,
                _queue_arn: &str,
                _message_body: &str,
                _attrs: &HashMap<String, fakecloud_core::delivery::SqsMessageAttribute>,
                _group: Option<&str>,
                _dedup: Option<&str>,
            ) {
                *self.count.lock().unwrap() += 1;
            }
        }

        let recorder = Arc::new(Recorder::default());
        let (topic, arn) = make_topic("t1");
        let q_arn = Arn::new("sqs", REGION, ACCOUNT, "q1").to_string();
        let mut sub = make_subscription(&arn, "sqs", &q_arn, true);
        // FilterPolicyScope=MessageBody so the policy is matched against
        // the JSON message instead of message attributes.
        sub.attributes
            .insert("FilterPolicyScope".to_string(), "MessageBody".to_string());
        sub.attributes.insert(
            "FilterPolicy".to_string(),
            r#"{"event":["created"]}"#.to_string(),
        );

        let state = make_state(vec![topic], vec![sub]);
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = SnsDeliveryImpl::new(state.clone(), bus);

        // Non-matching event: should be dropped.
        delivery.publish_to_topic(&arn, r#"{"event":"deleted"}"#, None);
        assert_eq!(*recorder.count.lock().unwrap(), 0);

        // Matching event: should deliver.
        delivery.publish_to_topic(&arn, r#"{"event":"created"}"#, None);
        assert_eq!(*recorder.count.lock().unwrap(), 1);
    }
}
