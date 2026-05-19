//! Cross-service hook adapters extracted from main.rs by audit-2026-05-19.

use std::sync::Arc;

pub(crate) struct KmsHookAdapter {
    pub(crate) inner: fakecloud_kms::hook::KmsServiceHook,
    /// Shared KMS state — used to snapshot after the hook auto-provisions
    /// an `aws/<service>` AWS-managed key on first use, so the new key
    /// survives a server restart and the corresponding ciphertext stays
    /// decryptable.
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

    pub(crate) fn key_count(&self) -> usize {
        self.state.read().iter().map(|(_, s)| s.keys.len()).sum()
    }

    pub(crate) fn save_snapshot_blocking(&self) {
        let Some(store) = self.snapshot_store.get() else {
            return;
        };
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
        let before = self.key_count();
        let result = self
            .inner
            .encrypt(
                account_id,
                region,
                key_id,
                plaintext,
                service_principal,
                encryption_context,
            )
            .map_err(|e| e.to_string());
        // Auto-provisioned a new AWS-managed key — persist immediately so
        // a restart can still decrypt its ciphertext.
        if result.is_ok() && self.key_count() > before {
            self.save_snapshot_blocking();
        }
        result
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
