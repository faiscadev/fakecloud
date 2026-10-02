//! Cross-service hook adapters extracted from main.rs by audit-2026-05-19.

use std::sync::Arc;

pub(crate) struct KmsHookAdapter {
    pub(crate) inner: fakecloud_kms::hook::KmsServiceHook,
    /// Shared KMS state, snapshotted after the hook mints an `aws/<service>`
    /// AWS-managed key on first use, so the key survives a restart and the
    /// corresponding ciphertext stays decryptable.
    pub(crate) state: fakecloud_kms::SharedKmsState,
    pub(crate) snapshot_store: std::sync::OnceLock<Arc<dyn fakecloud_persistence::SnapshotStore>>,
}

impl KmsHookAdapter {
    pub(crate) fn new(
        state: fakecloud_kms::SharedKmsState,
        usage: fakecloud_kms::hook::SharedKmsUsageState,
    ) -> Self {
        Self {
            inner: fakecloud_kms::hook::KmsServiceHook::new(state.clone(), usage),
            state,
            snapshot_store: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn set_snapshot_store(&self, store: Arc<dyn fakecloud_persistence::SnapshotStore>) {
        let _ = self.snapshot_store.set(store);
    }

    fn save_snapshot_blocking(&self, store: &dyn fakecloud_persistence::SnapshotStore) {
        let snapshot = fakecloud_kms::KmsSnapshot {
            schema_version: fakecloud_kms::KMS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(self.state.read().clone()),
            state: None,
        };
        match serde_json::to_vec(&snapshot) {
            Ok(bytes) => {
                if let Err(err) = store.save(&bytes) {
                    tracing::error!(%err, "kms hook snapshot save failed");
                }
            }
            Err(err) => tracing::error!(%err, "kms hook snapshot serialize failed"),
        }
    }

    /// Persist KMS state after a key was minted, durably, before the hook
    /// call returns: a restart right after must still find the key (and so
    /// decrypt what was encrypted under it). On a multi-thread Tokio runtime
    /// the blocking serialize + write runs inside `block_in_place`, so the
    /// worker's other tasks move to another thread instead of stalling;
    /// elsewhere it runs directly.
    fn persist_minted_key(&self) {
        let Some(store) = self.snapshot_store.get() else {
            return;
        };
        let multi_thread = tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
        if multi_thread {
            tokio::task::block_in_place(|| self.save_snapshot_blocking(store.as_ref()));
        } else {
            self.save_snapshot_blocking(store.as_ref());
        }
    }

    /// Resolve `key_id`, persisting KMS state when that minted a key.
    fn resolve_and_persist(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<String, String> {
        let (arn, minted) = self
            .inner
            .resolve_key_arn_tracked(account_id, region, key_id, service_principal)
            .map_err(|e| e.to_string())?;
        if minted {
            self.persist_minted_key();
        }
        Ok(arn)
    }
}

impl fakecloud_core::delivery::KmsHook for KmsHookAdapter {
    fn encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<String, String> {
        // Resolve first (minting and persisting an AWS-managed key on first
        // use), then encrypt under the resolved key.
        let key_arn = self.resolve_and_persist(account_id, region, key_id, service_principal)?;
        self.inner
            .encrypt(
                account_id,
                region,
                &key_arn,
                plaintext,
                service_principal,
                encryption_context,
            )
            .map_err(|e| e.to_string())
    }

    fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        service_principal: &str,
        encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        self.inner
            .decrypt(
                account_id,
                ciphertext_b64,
                service_principal,
                encryption_context,
            )
            .map_err(|e| e.to_string())
    }

    fn resolve_key_arn(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<String, String> {
        self.resolve_and_persist(account_id, region, key_id, service_principal)
    }

    fn aws_managed_key_arn(
        &self,
        account_id: &str,
        region: &str,
        service: &str,
        service_principal: &str,
    ) -> Result<String, String> {
        let (arn, minted) =
            self.inner
                .aws_managed_key_arn_tracked(account_id, region, service, service_principal);
        if minted {
            self.persist_minted_key();
        }
        Ok(arn)
    }
}

pub(crate) struct SesEmailDispatcher {
    pub(crate) state: fakecloud_ses::SharedSesState,
}

impl fakecloud_core::delivery::EmailDispatcher for SesEmailDispatcher {
    fn send_email(
        &self,
        account_id: &str,
        from: &str,
        to: &str,
        subject: &str,
        body_text: &str,
        body_html: Option<&str>,
    ) {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.sent_emails.push(fakecloud_ses::SentEmail {
            message_id: format!("cognito-{}", uuid::Uuid::new_v4()),
            from: from.to_string(),
            to: vec![to.to_string()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some(subject.to_string()),
            html_body: body_html.map(|s| s.to_string()),
            text_body: Some(body_text.to_string()),
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: chrono::Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        });
    }
}

/// SES SendEmail dispatcher for cross-service universal targets
/// (EventBridge Scheduler `arn:aws:scheduler:::aws-sdk:sesv2:sendEmail`,
/// EventBridge Rules with SES targets). Distinct from `SesEmailDispatcher`
/// which is the single-recipient primitive Cognito uses — this one
/// preserves multi-recipient + subject + html semantics that real callers
/// pass via the SES API shape.
pub(crate) struct SesSendEmailDispatcherImpl {
    pub(crate) state: fakecloud_ses::SharedSesState,
}

impl fakecloud_core::delivery::SesSendEmailDispatcher for SesSendEmailDispatcherImpl {
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
    ) -> Result<(), String> {
        if to.is_empty() && cc.is_empty() && bcc.is_empty() {
            return Err("at least one recipient required".to_string());
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.sent_emails.push(fakecloud_ses::SentEmail {
            message_id: format!("scheduler-{}", uuid::Uuid::new_v4()),
            from: from.to_string(),
            to,
            cc,
            bcc,
            subject: subject.map(String::from),
            html_body: html_body.map(String::from),
            text_body: text_body.map(String::from),
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: chrono::Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        });
        Ok(())
    }
}

/// ELBv2 target registration/deregistration from ECS runtime.
pub(crate) struct Elbv2TargetRegistrationImpl {
    pub(crate) state: fakecloud_elbv2::SharedElbv2State,
}

impl fakecloud_core::delivery::Elbv2TargetRegistration for Elbv2TargetRegistrationImpl {
    fn register_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    ) {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(account_id);
        let Some(tg) = st.target_groups.get_mut(target_group_arn) else {
            return;
        };
        for (id, port) in targets {
            tg.targets.retain(|t| t.id != id);
            tg.targets.push(fakecloud_elbv2::TargetDescription {
                id,
                port: port.map(|p| p as i32),
                availability_zone: None,
                health: fakecloud_elbv2::TargetHealth {
                    state: "initial".into(),
                    reason: None,
                    description: None,
                },
                consecutive_success: 0,
                consecutive_failure: 0,
                last_probe_at: None,
            });
        }
    }

    fn deregister_targets(
        &self,
        account_id: &str,
        target_group_arn: &str,
        targets: Vec<(String, Option<i64>)>,
    ) {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(account_id);
        let Some(tg) = st.target_groups.get_mut(target_group_arn) else {
            return;
        };
        for (id, _port) in targets {
            tg.targets.retain(|t| t.id != id);
        }
    }
}

/// EC2 subnet lookup for the ECS runtime (`awsvpc` ENI addressing).
pub(crate) struct Ec2NetworkLookupImpl {
    pub(crate) state: fakecloud_ec2::SharedEc2State,
}

impl fakecloud_core::delivery::Ec2NetworkLookup for Ec2NetworkLookupImpl {
    fn subnet_cidr(&self, account_id: &str, subnet_id: &str) -> Option<String> {
        let accounts = self.state.read();
        accounts
            .get(account_id)?
            .subnets
            .get(subnet_id)
            .map(|s| s.cidr_block.clone())
    }
}

/// ECS RunTask runner for cross-service universal targets. Wraps an
/// `Arc<EcsService>` so the call goes through the same validation +
/// runtime spawn path as a direct ECS RunTask request.
pub(crate) struct EcsTaskRunnerImpl {
    pub(crate) service: Arc<fakecloud_ecs::EcsService>,
}

impl fakecloud_core::delivery::EcsTaskRunner for EcsTaskRunnerImpl {
    fn run_task(
        &self,
        account_id: &str,
        cluster: &str,
        task_definition: &str,
        launch_type: Option<&str>,
        count: usize,
    ) -> Result<(), String> {
        self.service
            .run_task_external(account_id, cluster, task_definition, launch_type, count)
    }
}

/// SageMaker pipeline starter used by EventBridge Scheduler's
/// `sagemaker:pipeline` target: persists a PipelineExecution record so the
/// Describe/List pipeline-execution ops resolve it.
pub(crate) struct SageMakerPipelineDeliveryImpl {
    pub(crate) state: fakecloud_sagemaker::SharedSageMakerState,
}

impl fakecloud_core::delivery::SageMakerPipelineDelivery for SageMakerPipelineDeliveryImpl {
    fn start_pipeline_execution(&self, pipeline_arn: &str, parameters: &serde_json::Value) {
        fakecloud_sagemaker::start_pipeline_execution_from_delivery(
            &self.state,
            pipeline_arn,
            parameters,
        );
    }
}

/// SMS dispatcher used by Cognito's verification flow: append to the SNS
/// account's `sms_messages` so test code can assert on what landed.
pub(crate) struct SnsSmsDispatcher {
    pub(crate) state: fakecloud_sns::SharedSnsState,
}

impl fakecloud_core::delivery::SmsDispatcher for SnsSmsDispatcher {
    fn send_sms(&self, account_id: &str, phone_number: &str, message: &str) {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state
            .sms_messages
            .push((phone_number.to_string(), message.to_string()));
    }
}

#[cfg(test)]
mod kms_hook_adapter_tests {
    use super::*;
    use fakecloud_core::delivery::KmsHook;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts saves and records the number of keys in the last snapshot.
    #[derive(Default)]
    struct CountingStore {
        saves: AtomicUsize,
        last_keys: AtomicUsize,
    }

    impl fakecloud_persistence::SnapshotStore for CountingStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            let snap: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            let keys = snap["accounts"]["accounts"]["123456789012"]["keys"]
                .as_object()
                .map_or(0, |k| k.len());
            self.last_keys.store(keys, Ordering::SeqCst);
            self.saves.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn adapter() -> (KmsHookAdapter, Arc<CountingStore>) {
        let state: fakecloud_kms::SharedKmsState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let adapter = KmsHookAdapter::new(state, Default::default());
        let store = Arc::new(CountingStore::default());
        adapter.set_snapshot_store(store.clone());
        (adapter, store)
    }

    /// A minted key is saved before the hook call returns (no yield needed),
    /// on the multi-thread runtime the server runs; existing keys save nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minted_keys_are_saved_before_the_call_returns() {
        let (adapter, store) = adapter();
        let principal = "timestream.amazonaws.com";
        adapter
            .resolve_key_arn(
                "123456789012",
                "us-east-1",
                "alias/aws/timestream",
                principal,
            )
            .unwrap();
        assert_eq!(store.saves.load(Ordering::SeqCst), 1);
        assert_eq!(store.last_keys.load(Ordering::SeqCst), 1);
        adapter
            .encrypt(
                "123456789012",
                "us-east-1",
                "alias/aws/timestream",
                b"x",
                principal,
                Default::default(),
            )
            .unwrap();
        assert_eq!(store.saves.load(Ordering::SeqCst), 1, "no new key, no save");
        adapter
            .aws_managed_key_arn("123456789012", "eu-west-1", "timestream", principal)
            .unwrap();
        assert_eq!(store.saves.load(Ordering::SeqCst), 2);
        assert_eq!(store.last_keys.load(Ordering::SeqCst), 2);
    }

    /// Outside a multi-thread runtime the save runs directly, still before
    /// the call returns.
    #[test]
    fn minted_keys_are_saved_without_a_runtime() {
        let (adapter, store) = adapter();
        adapter
            .aws_managed_key_arn(
                "123456789012",
                "us-east-1",
                "dynamodb",
                "dynamodb.amazonaws.com",
            )
            .unwrap();
        assert_eq!(store.saves.load(Ordering::SeqCst), 1);
    }
}
