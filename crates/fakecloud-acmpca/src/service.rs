//! ACM Private CA (`acm-pca`) awsJson1_1 service.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use http::StatusCode;
use parking_lot::{Mutex as SyncMutex, RwLock};
use serde_json::{json, Value};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;
use fakecloud_s3::{memory_body, S3Object, SharedS3State};

use crate::persistence::save_acmpca_snapshot;
use crate::provision::{build_pending_ca, CaCreateParams};
use crate::state::{
    AcmPcaAccounts, AuditReport, CertificateAuthority, IssuedCertificate, Permission,
    RevokedCertificate, SharedAcmPcaState, TagEntry,
};
use crate::validate::{self, ImportCheck};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateCertificateAuthority",
    "DescribeCertificateAuthority",
    "ListCertificateAuthorities",
    "UpdateCertificateAuthority",
    "DeleteCertificateAuthority",
    "RestoreCertificateAuthority",
    "GetCertificateAuthorityCertificate",
    "GetCertificateAuthorityCsr",
    "ImportCertificateAuthorityCertificate",
    "IssueCertificate",
    "GetCertificate",
    "RevokeCertificate",
    "ListTags",
    "TagCertificateAuthority",
    "UntagCertificateAuthority",
    "CreatePermission",
    "ListPermissions",
    "DeletePermission",
    "PutPolicy",
    "GetPolicy",
    "DeletePolicy",
    "CreateCertificateAuthorityAuditReport",
    "DescribeCertificateAuthorityAuditReport",
];

/// Actions that mutate persisted state and therefore trigger a snapshot write.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateCertificateAuthority",
    "UpdateCertificateAuthority",
    "DeleteCertificateAuthority",
    "RestoreCertificateAuthority",
    "ImportCertificateAuthorityCertificate",
    "IssueCertificate",
    "RevokeCertificate",
    "TagCertificateAuthority",
    "UntagCertificateAuthority",
    "CreatePermission",
    "DeletePermission",
    "PutPolicy",
    "DeletePolicy",
    "CreateCertificateAuthorityAuditReport",
];

/// Actions that need the CA's private key (or its CSR) to be present, so they
/// wait for background key generation to finish before running.
const KEY_DEPENDENT_ACTIONS: &[&str] = &[
    "GetCertificateAuthorityCsr",
    "IssueCertificate",
    "ImportCertificateAuthorityCertificate",
];

/// Upper bound on how long a key-dependent action waits for background key
/// generation. Real RSA-4096 generation in an unoptimized build can take tens of
/// seconds; past this the handler proceeds and returns its own not-ready error.
const KEYGEN_WAIT: Duration = Duration::from_secs(300);

pub struct AcmPcaService {
    state: SharedAcmPcaState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// Per-CA notifiers signalled when background key generation completes.
    /// Not persisted — rebuilt by `rearm_pending_creations` after a restart.
    keygen_waiters: Arc<SyncMutex<HashMap<String, Arc<Notify>>>>,
    /// Shared S3 state, when wired. `CreateCertificateAuthorityAuditReport`
    /// delivers the generated report object into the target bucket here so the
    /// documented create-report -> read-from-S3 flow actually works.
    s3: Option<SharedS3State>,
}

impl AcmPcaService {
    pub fn new(state: SharedAcmPcaState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            keygen_waiters: Arc::new(SyncMutex::new(HashMap::new())),
            s3: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Wire the shared S3 state so audit reports are delivered as real objects
    /// into the caller-specified bucket/key.
    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    pub fn shared_state(&self) -> SharedAcmPcaState {
        Arc::clone(&self.state)
    }

    async fn save_snapshot(&self) {
        save_acmpca_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists current state when invoked, or `None` in
    /// memory mode. The CloudFormation provisioner mutates `state` directly and
    /// uses this to write CFN-provisioned CAs through to disk.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_acmpca_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Generate a CA's key pair + CSR off the async runtime and install it. The
    /// CA is already `PENDING_CERTIFICATE`; this only fills in the private key +
    /// CSR (decoupled from status so the CA never wedges in `CREATING`). Real
    /// RSA-4096 generation can take tens of seconds, so it runs on
    /// `spawn_blocking` and never blocks the request path. On failure the CA is
    /// marked `FAILED`. A per-CA notifier wakes any handler waiting on the key,
    /// and the result is persisted so it survives a restart.
    fn spawn_ca_keygen(
        &self,
        account_id: String,
        arn: String,
        key_algorithm: String,
        subject: Value,
    ) {
        let state = Arc::clone(&self.state);
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        let waiters = Arc::clone(&self.keygen_waiters);
        // Register the readiness notifier before spawning so a handler that
        // starts waiting immediately can find it.
        let notify = Arc::new(Notify::new());
        waiters.lock().insert(arn.clone(), notify.clone());
        tokio::spawn(async move {
            let material = tokio::task::spawn_blocking(move || {
                crate::provision::generate_ca_material(&key_algorithm, &subject)
            })
            .await;
            {
                let mut accounts = state.write();
                if let Some(ca) = accounts
                    .accounts
                    .get_mut(&account_id)
                    .and_then(|a| a.authorities.get_mut(&arn))
                {
                    // Only fill if the key is still missing and the CA has not
                    // moved to a terminal state in the meantime.
                    if ca.ca_key_pem.is_empty() && ca.status != "FAILED" && ca.status != "DELETED" {
                        match material {
                            Ok(Ok((key_pem, csr_pem))) => {
                                crate::provision::fill_keygen(ca, key_pem, csr_pem);
                            }
                            Ok(Err(err)) => {
                                ca.status = "FAILED".to_string();
                                ca.failure_reason = Some("OTHER".to_string());
                                tracing::error!(%err, "acm-pca CA key generation failed");
                            }
                            Err(err) => {
                                ca.status = "FAILED".to_string();
                                ca.failure_reason = Some("OTHER".to_string());
                                tracing::error!(%err, "acm-pca CA keygen task panicked");
                            }
                        }
                    }
                }
            }
            // Wake waiters (removing the notifier first so a late waiter that
            // misses the wake re-checks the now-ready state instead).
            waiters.lock().remove(&arn);
            notify.notify_waiters();
            save_acmpca_snapshot(&state, store, &lock).await;
        });
    }

    /// Wait (bounded) for a CA's background key generation to finish before a
    /// key-dependent action uses it. Returns as soon as the key material is
    /// present, or the CA reaches a terminal/absent state, or [`KEYGEN_WAIT`]
    /// elapses — in which case the handler runs and returns its own not-ready
    /// error. A no-op for CAs whose key is already installed (e.g. CFN-created).
    async fn ensure_key_ready(&self, account: &str, arn: &str) {
        let deadline = Instant::now() + KEYGEN_WAIT;
        loop {
            {
                let st = self.state.read();
                match st
                    .accounts
                    .get(account)
                    .and_then(|a| a.authorities.get(arn))
                {
                    // Unknown CA: let the handler produce ResourceNotFound.
                    None => return,
                    Some(ca) => {
                        if !ca.ca_key_pem.is_empty() || ca.status == "FAILED" {
                            return;
                        }
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return;
            }
            let notify = self.keygen_waiters.lock().get(arn).cloned();
            match notify {
                Some(n) => {
                    let _ = tokio::time::timeout(remaining, n.notified()).await;
                }
                // Notifier already gone (keygen finished, or never armed): the
                // next loop re-reads state; a short sleep bounds the spin.
                None => {
                    tokio::time::sleep(Duration::from_millis(20).min(remaining)).await;
                }
            }
        }
    }

    /// Re-arm key generation for any CA restored with no key material (the key
    /// was never persisted because the previous process exited mid-keygen).
    /// Called by the server after loading the persistence snapshot. Also migrates
    /// any legacy `CREATING` state to `PENDING_CERTIFICATE`.
    pub fn rearm_pending_creations(&self) {
        let pending: Vec<(String, String, String, Value)> = {
            let mut state = self.state.write();
            let mut out = Vec::new();
            for (account_id, account) in state.accounts.iter_mut() {
                for (arn, ca) in account.authorities.iter_mut() {
                    let unfinished = ca.ca_key_pem.is_empty()
                        && (ca.status == "PENDING_CERTIFICATE" || ca.status == "CREATING");
                    if unfinished {
                        if ca.status == "CREATING" {
                            ca.status = "PENDING_CERTIFICATE".to_string();
                        }
                        out.push((
                            account_id.clone(),
                            arn.clone(),
                            ca.key_algorithm.clone(),
                            crate::provision::subject_of(ca),
                        ));
                    }
                }
            }
            out
        };
        for (account_id, arn, key_algorithm, subject) in pending {
            self.spawn_ca_keygen(account_id, arn, key_algorithm, subject);
        }
    }
}

impl Default for AcmPcaService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(AcmPcaAccounts::new())))
    }
}

#[async_trait]
impl AwsService for AcmPcaService {
    fn service_name(&self) -> &str {
        "acm-pca"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        // Key-dependent actions wait for background key generation to finish so a
        // client that creates a CA and immediately calls GetCsr / IssueCertificate
        // / Import does not race the keygen task.
        if KEY_DEPENDENT_ACTIONS.contains(&req.action.as_str()) {
            if let Some(arn) = req
                .json_body()
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
            {
                self.ensure_key_ready(&account_id(&req), arn).await;
            }
        }
        let result = match req.action.as_str() {
            "CreateCertificateAuthority" => self.create_certificate_authority(&req),
            "DescribeCertificateAuthority" => self.describe_certificate_authority(&req),
            "ListCertificateAuthorities" => self.list_certificate_authorities(&req),
            "UpdateCertificateAuthority" => self.update_certificate_authority(&req),
            "DeleteCertificateAuthority" => self.delete_certificate_authority(&req),
            "RestoreCertificateAuthority" => self.restore_certificate_authority(&req),
            "GetCertificateAuthorityCertificate" => {
                self.get_certificate_authority_certificate(&req)
            }
            "GetCertificateAuthorityCsr" => self.get_certificate_authority_csr(&req),
            "ImportCertificateAuthorityCertificate" => {
                self.import_certificate_authority_certificate(&req)
            }
            "IssueCertificate" => self.issue_certificate(&req),
            "GetCertificate" => self.get_certificate(&req),
            "RevokeCertificate" => self.revoke_certificate(&req),
            "ListTags" => self.list_tags(&req),
            "TagCertificateAuthority" => self.tag_certificate_authority(&req),
            "UntagCertificateAuthority" => self.untag_certificate_authority(&req),
            "CreatePermission" => self.create_permission(&req),
            "ListPermissions" => self.list_permissions(&req),
            "DeletePermission" => self.delete_permission(&req),
            "PutPolicy" => self.put_policy(&req),
            "GetPolicy" => self.get_policy(&req),
            "DeletePolicy" => self.delete_policy(&req),
            "CreateCertificateAuthorityAuditReport" => {
                self.create_certificate_authority_audit_report(&req)
            }
            "DescribeCertificateAuthorityAuditReport" => {
                self.describe_certificate_authority_audit_report(&req)
            }
            other => Err(AwsServiceError::action_not_implemented("acm-pca", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Handlers ────────────────────────────────────────────────────────

impl AcmPcaService {
    fn create_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let config = body
            .get("CertificateAuthorityConfiguration")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_args("CertificateAuthorityConfiguration is required"))?;
        let key_algorithm = config
            .get("KeyAlgorithm")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("KeyAlgorithm is required"))?
            .to_string();
        // AWS ACM PCA only accepts a subset of the model's `KeyAlgorithm` enum
        // as a CA key algorithm. Reject the rest with the same validation error
        // AWS returns rather than silently substituting a different key type.
        if !validate::SUPPORTED_CA_KEY_ALGORITHMS.contains(&key_algorithm.as_str()) {
            return Err(invalid_args(format!(
                "The certificate authority key algorithm {key_algorithm} is not supported"
            )));
        }
        let signing_algorithm = config
            .get("SigningAlgorithm")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("SigningAlgorithm is required"))?
            .to_string();
        if !VALID_SIGNING_ALGORITHMS.contains(&signing_algorithm.as_str()) {
            return Err(invalid_args(format!(
                "Invalid SigningAlgorithm: {signing_algorithm}"
            )));
        }
        let subject = config
            .get("Subject")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_args("Subject is required"))?
            .clone();

        let ca_type = body
            .get("CertificateAuthorityType")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("CertificateAuthorityType is required"))?
            .to_string();
        if ca_type != "ROOT" && ca_type != "SUBORDINATE" {
            return Err(invalid_args(format!(
                "Invalid CertificateAuthorityType: {ca_type}"
            )));
        }

        let usage_mode = body
            .get("UsageMode")
            .and_then(Value::as_str)
            .unwrap_or("GENERAL_PURPOSE")
            .to_string();
        if usage_mode != "GENERAL_PURPOSE" && usage_mode != "SHORT_LIVED_CERTIFICATE" {
            return Err(invalid_args(format!("Invalid UsageMode: {usage_mode}")));
        }
        let key_storage = body
            .get("KeyStorageSecurityStandard")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(k) = &key_storage {
            if !VALID_KEY_STORAGE_STANDARDS.contains(&k.as_str()) {
                return Err(invalid_args(format!(
                    "Invalid KeyStorageSecurityStandard: {k}"
                )));
            }
        }
        let idempotency_token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(t) = &idempotency_token {
            if t.is_empty() || t.len() > 36 {
                return Err(invalid_args(
                    "IdempotencyToken length must be between 1 and 36",
                ));
            }
        }
        // The builder applies AWS's default RevocationConfiguration (CRL + OCSP
        // disabled) when the caller omits one.
        let revocation_configuration = body.get("RevocationConfiguration").cloned();
        let tags = parse_tags(body.get("Tags"))?;

        let account = account_id(req);
        let region = region(req);

        // Idempotency: AWS collapses repeated CreateCertificateAuthority calls
        // that carry the same IdempotencyToken within a ~5-minute window into a
        // single CA, returning the ARN of the one already created rather than
        // minting a fresh CA/ARN on every retry. Match on (account, token).
        if let Some(token) = &idempotency_token {
            let existing = {
                let accounts = self.state.read();
                accounts.accounts.get(&account).and_then(|a| {
                    a.authorities
                        .values()
                        .find(|ca| {
                            ca.idempotency_token.as_deref() == Some(token.as_str())
                                && ca.status != "DELETED"
                                && within_idempotency_window(ca.created_at)
                        })
                        .map(|ca| ca.arn.clone())
                })
            };
            if let Some(arn) = existing {
                return Ok(AwsResponse::ok_json(
                    json!({ "CertificateAuthorityArn": arn }),
                ));
            }
        }

        let ca_id = Uuid::new_v4().to_string();
        let arn = format!("arn:aws:acm-pca:{region}:{account}:certificate-authority/{ca_id}");

        // Every CA — ROOT included — is reported PENDING_CERTIFICATE immediately
        // (matching AWS, which reaches that state within seconds), while its
        // real, requested-algorithm key pair is generated on a background task.
        // Decoupling status from keygen keeps the CA out of a prolonged CREATING
        // state that would trip clients' create waiters on slow RSA-4096 keys.
        // A ROOT CA is activated by the self-sign ceremony (GetCsr ->
        // IssueCertificate(RootCACertificate) -> ImportCertificateAuthorityCertificate).
        let params = CaCreateParams {
            arn: arn.clone(),
            account: account.clone(),
            ca_type,
            key_algorithm: key_algorithm.clone(),
            signing_algorithm,
            subject: subject.clone(),
            configuration: config.clone(),
            usage_mode,
            key_storage_security_standard: key_storage,
            revocation_configuration,
            idempotency_token,
            tags,
        };
        let ca = build_pending_ca(params);
        {
            let mut accounts = self.state.write();
            accounts
                .accounts
                .entry(account.clone())
                .or_default()
                .authorities
                .insert(arn.clone(), ca);
        }
        self.spawn_ca_keygen(account, arn.clone(), key_algorithm, subject);

        Ok(AwsResponse::ok_json(
            json!({ "CertificateAuthorityArn": arn }),
        ))
    }

    fn describe_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        Ok(AwsResponse::ok_json(
            json!({ "CertificateAuthority": ca_to_json(ca) }),
        ))
    }

    fn list_certificate_authorities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (max, start) = read_pagination(&body)?;
        if let Some(owner) = body.get("ResourceOwner").and_then(Value::as_str) {
            if owner != "SELF" && owner != "OTHER_ACCOUNTS" {
                return Err(invalid_args(format!("Invalid ResourceOwner: {owner}")));
            }
        }
        let accounts = self.state.read();
        let all: Vec<Value> = accounts
            .accounts
            .get(&account_id(req))
            .map(|a| a.authorities.values().map(ca_to_json).collect())
            .unwrap_or_default();
        let (page, next) = paginate(all, max, start);
        let mut resp = json!({ "CertificateAuthorities": page });
        if let Some(token) = next {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn update_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let new_status = body.get("Status").and_then(Value::as_str);
        if let Some(s) = new_status {
            if s != "ACTIVE" && s != "DISABLED" {
                return Err(invalid_args(format!("Invalid status transition: {s}")));
            }
        }
        let new_revocation = body.get("RevocationConfiguration").cloned();
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        if ca.status == "CREATING" || ca.status == "DELETED" {
            return Err(invalid_state(
                "The certificate authority cannot be updated in its current state",
            ));
        }
        if let Some(s) = new_status {
            // A CA can only be activated once it actually holds a certificate.
            // Setting DISABLED, by contrast, is permitted from ACTIVE, DISABLED
            // AND PENDING_CERTIFICATE: the terraform-provider-aws destroy path
            // disables a CA before deleting it even when the CA never had a
            // certificate installed, and real AWS accepts that transition. The
            // top-of-handler guard already rejects the genuinely terminal
            // CREATING/DELETED states.
            if s == "ACTIVE" && ca.ca_cert_pem.is_none() {
                return Err(invalid_state(
                    "A certificate authority without an installed certificate cannot be set to ACTIVE",
                ));
            }
            ca.status = s.to_string();
            ca.last_state_change_at = Some(Utc::now());
        }
        if new_revocation.is_some() {
            ca.revocation_configuration = new_revocation;
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let days = body
            .get("PermanentDeletionTimeInDays")
            .and_then(Value::as_i64)
            .unwrap_or(30);
        if !(7..=30).contains(&days) {
            return Err(invalid_args(
                "PermanentDeletionTimeInDays must be between 7 and 30",
            ));
        }
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        // ACTIVE CAs must be disabled before deletion.
        if ca.status == "ACTIVE" {
            return Err(invalid_state(
                "A certificate authority must be disabled before it can be deleted",
            ));
        }
        if ca.status == "DELETED" {
            return Err(invalid_state(
                "The certificate authority is already deleted",
            ));
        }
        ca.status = "DELETED".to_string();
        ca.restorable_until = Some(Utc::now() + chrono::Duration::days(days));
        ca.last_state_change_at = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn restore_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        if ca.status != "DELETED" {
            return Err(invalid_state(
                "Only a certificate authority in the DELETED state can be restored",
            ));
        }
        // Restore to DISABLED if it had a cert, else back to PENDING_CERTIFICATE.
        ca.status = if ca.ca_cert_pem.is_some() {
            "DISABLED".to_string()
        } else {
            "PENDING_CERTIFICATE".to_string()
        };
        ca.restorable_until = None;
        ca.last_state_change_at = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_certificate_authority_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let cert = ca.ca_cert_pem.as_ref().ok_or_else(|| {
            invalid_state("The certificate authority does not have a certificate installed")
        })?;
        let mut resp = json!({ "Certificate": cert });
        if let Some(chain) = &ca.ca_cert_chain_pem {
            resp["CertificateChain"] = json!(chain);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn get_certificate_authority_csr(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        if ca.status == "DELETED" {
            return Err(invalid_state("The certificate authority is deleted"));
        }
        if ca.status == "CREATING" || ca.csr_pem.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RequestInProgressException",
                "The certificate authority is still being created",
            ));
        }
        Ok(AwsResponse::ok_json(json!({ "Csr": ca.csr_pem })))
    }

    fn import_certificate_authority_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let cert_pem = decode_blob(body.get("Certificate"))
            .ok_or_else(|| malformed_certificate("Certificate is required"))?;
        let chain_pem = decode_blob(body.get("CertificateChain"));
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        if ca.status == "DELETED" {
            return Err(invalid_state("The certificate authority is deleted"));
        }
        if ca.status == "CREATING" || ca.ca_key_pem.is_empty() {
            return Err(invalid_state(
                "The certificate authority is still being created",
            ));
        }
        // For a subordinate CA a certificate chain is required.
        if ca.ca_type == "SUBORDINATE" && chain_pem.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "A certificate chain is required to import a subordinate CA certificate",
            ));
        }
        // The imported certificate must parse as real X.509 and certify this
        // CA's own key pair — not just contain a "BEGIN CERTIFICATE" marker.
        let ca_key = validate::load_key_pair(&ca.ca_key_pem)
            .map_err(|e| request_failed(format!("failed to load CA key: {e}")))?;
        match validate::verify_imported_cert(&cert_pem, &ca_key) {
            ImportCheck::Ok => {}
            ImportCheck::Malformed(reason) => {
                return Err(malformed_certificate(format!(
                    "The certificate is not valid: {reason}"
                )));
            }
            ImportCheck::Mismatch => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CertificateMismatchException",
                    "The certificate's public key does not match this certificate authority's key pair",
                ));
            }
        }
        ca.ca_cert_pem = Some(cert_pem);
        ca.ca_cert_chain_pem = chain_pem;
        ca.status = "ACTIVE".to_string();
        if ca.not_before.is_none() {
            ca.not_before = Some(Utc::now());
            ca.not_after = Some(Utc::now() + chrono::Duration::days(3650));
        }
        if ca.serial.is_none() {
            ca.serial = Some(format!("{:x}", Uuid::new_v4().as_u128()));
        }
        ca.last_state_change_at = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn issue_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let csr_pem =
            decode_blob(body.get("Csr")).ok_or_else(|| malformed_csr("Csr is required"))?;
        let signing_algorithm = body
            .get("SigningAlgorithm")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("SigningAlgorithm is required"))?
            .to_string();
        if !VALID_SIGNING_ALGORITHMS.contains(&signing_algorithm.as_str()) {
            return Err(invalid_args(format!(
                "Invalid SigningAlgorithm: {signing_algorithm}"
            )));
        }
        let validity = body
            .get("Validity")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_args("Validity is required"))?;
        let validity_value = validity
            .get("Value")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid_args("Validity.Value is required"))?;
        let validity_type = validity
            .get("Type")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("Validity.Type is required"))?;
        let template_arn = body
            .get("TemplateArn")
            .and_then(Value::as_str)
            .map(str::to_string);
        // ApiPassthrough (Subject + Extensions) is stamped into the issued
        // certificate so a caller that requests SANs / a subject / key-usage
        // actually gets them back from GetCertificate. Rejected outright if it
        // is present but not an object.
        let api_passthrough = match body.get("ApiPassthrough") {
            Some(v) if v.is_object() => Some(v.clone()),
            Some(Value::Null) | None => None,
            Some(_) => return Err(invalid_args("ApiPassthrough must be an object")),
        };
        let idempotency_token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(str::to_string);

        let is_ca_template = template_arn
            .as_deref()
            .map(|t| t.contains("CACertificate") || t.contains("RootCACertificate"))
            .unwrap_or(false);

        let now = Utc::now();
        // `ValidityNotBefore` (optional) sets the cert's start; default is now.
        let not_before = match body.get("ValidityNotBefore").filter(|v| v.is_object()) {
            Some(vnb) => {
                let v = vnb
                    .get("Value")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| invalid_args("ValidityNotBefore.Value is required"))?;
                let t = vnb
                    .get("Type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid_args("ValidityNotBefore.Type is required"))?;
                validate::resolve_validity(now, v, t).map_err(invalid_args)?
            }
            None => now,
        };
        // Checked resolution — a hostile Validity.Value can no longer overflow.
        let not_after =
            validate::resolve_validity(now, validity_value, validity_type).map_err(invalid_args)?;
        // The certificate's validity window must be well-ordered.
        if not_after <= not_before {
            return Err(invalid_args(
                "The requested validity period ends on or before it begins",
            ));
        }

        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;

        // Idempotency: a repeated IssueCertificate carrying the same
        // IdempotencyToken within the ~5-minute window returns the certificate
        // already issued for that token instead of signing (and charging for) a
        // second certificate with a fresh serial/ARN.
        if let Some(token) = &idempotency_token {
            if let Some(existing) = ca.issued.values().find(|c| {
                c.idempotency_token.as_deref() == Some(token.as_str())
                    && within_idempotency_window(c.issued_at)
            }) {
                return Ok(AwsResponse::ok_json(
                    json!({ "CertificateArn": existing.arn }),
                ));
            }
        }

        // A ROOT CA self-signs its own certificate during activation:
        // IssueCertificate(RootCACertificate template) is permitted while the CA
        // is still PENDING_CERTIFICATE. Every other issuance requires an ACTIVE CA.
        let root_self_sign =
            ca.status == "PENDING_CERTIFICATE" && ca.ca_type == "ROOT" && is_ca_template;
        if ca.status != "ACTIVE" && !root_self_sign {
            if ca.status == "CREATING" {
                return Err(invalid_state(
                    "The certificate authority is still being created",
                ));
            }
            return Err(invalid_state(format!(
                "The certificate authority is not in the ACTIVE state (current: {})",
                ca.status
            )));
        }

        // Load the CA key configured to sign with the requested algorithm's hash.
        let ca_key =
            validate::load_signing_key(&ca.ca_key_pem, &ca.key_algorithm, &signing_algorithm)
                .map_err(invalid_args)?;

        let (issuer, chain_base) = if root_self_sign {
            let issuer = validate::self_issuer(&crate::provision::subject_of(ca), &ca_key)
                .map_err(|e| request_failed(format!("failed to build root issuer: {e}")))?;
            (issuer, None)
        } else {
            let ca_cert_pem = ca
                .ca_cert_pem
                .clone()
                .ok_or_else(|| invalid_state("The certificate authority has no certificate"))?;
            let issuer = validate::issuer_from_ca_cert(&ca_cert_pem, &ca_key)
                .map_err(|e| request_failed(format!("failed to build issuer: {e}")))?;
            (issuer, Some(ca_cert_pem))
        };

        let (cert_pem, serial) = validate::issue_certificate(
            &issuer,
            &ca_key,
            &csr_pem,
            not_before,
            not_after,
            is_ca_template,
            api_passthrough.as_ref(),
        )
        .map_err(malformed_csr)?;

        let cert_arn = format!("{arn}/certificate/{serial}");
        // Chain = the CA's own cert (if installed) + any parent chain. A root
        // self-sign has no chain yet (it becomes the trust anchor on import).
        let chain = chain_base.map(|mut c| {
            if let Some(parent) = &ca.ca_cert_chain_pem {
                c.push_str(parent);
            }
            c
        });
        ca.issued.insert(
            cert_arn.clone(),
            IssuedCertificate {
                arn: cert_arn.clone(),
                serial,
                certificate_pem: cert_pem,
                chain_pem: chain,
                issued_at: now,
                not_before,
                not_after,
                template_arn,
                signing_algorithm,
                idempotency_token,
            },
        );

        Ok(AwsResponse::ok_json(json!({ "CertificateArn": cert_arn })))
    }

    fn get_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let cert_arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_arn("CertificateArn is required"))?
            .to_string();
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let cert = ca
            .issued
            .get(&cert_arn)
            .ok_or_else(|| not_found(&cert_arn))?;
        let mut resp = json!({ "Certificate": cert.certificate_pem });
        if let Some(chain) = &cert.chain_pem {
            resp["CertificateChain"] = json!(chain);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn revoke_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let raw_serial = body
            .get("CertificateSerial")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("CertificateSerial is required"))?;
        // AWS accepts the serial in several presentations (lowercase hex, upper
        // hex, colon-delimited); normalize before matching so a legitimate cert
        // can always be revoked regardless of the format the caller used.
        let serial = normalize_serial(raw_serial);
        let reason = body
            .get("RevocationReason")
            .and_then(Value::as_str)
            .unwrap_or("UNSPECIFIED")
            .to_string();
        if !VALID_REVOCATION_REASONS.contains(&reason.as_str()) {
            return Err(invalid_args(format!("Invalid RevocationReason: {reason}")));
        }
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        // Revocation is only meaningful against an active CA (a CRL/OCSP is
        // published from an ACTIVE authority).
        if ca.status != "ACTIVE" {
            return Err(invalid_state(format!(
                "The certificate authority is not in the ACTIVE state (current: {})",
                ca.status
            )));
        }
        if ca.revoked.contains_key(&serial) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RequestAlreadyProcessedException",
                "The certificate has already been revoked",
            ));
        }
        // The serial must belong to a certificate this CA issued.
        let known = ca
            .issued
            .values()
            .any(|c| normalize_serial(&c.serial) == serial);
        if !known {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "The certificate serial does not correspond to a certificate issued by this CA",
            ));
        }
        ca.revoked.insert(
            serial.clone(),
            RevokedCertificate {
                serial,
                revoked_at: Utc::now(),
                reason,
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let (max, start) = read_pagination(&body)?;
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let (page, next) = paginate(tags_to_json(&ca.tags), max, start);
        let mut resp = json!({ "Tags": page });
        if let Some(token) = next {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn tag_certificate_authority(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let new_tags = parse_tags(body.get("Tags"))?;
        if new_tags.is_empty() {
            return Err(invalid_tag("At least one tag is required"));
        }
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        // Compute the resulting tag count BEFORE mutating so an over-limit
        // request is rejected atomically with no state change (and no
        // over-limit snapshot written to disk).
        let added_keys = new_tags
            .iter()
            .filter(|t| !ca.tags.iter().any(|e| e.key == t.key))
            .count();
        if ca.tags.len() + added_keys > 50 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TooManyTagsException",
                "A certificate authority cannot have more than 50 tags",
            ));
        }
        for tag in new_tags {
            if let Some(existing) = ca.tags.iter_mut().find(|t| t.key == tag.key) {
                existing.value = tag.value;
            } else {
                ca.tags.push(tag);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_certificate_authority(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let remove = parse_tags(body.get("Tags"))?;
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        ca.tags.retain(|t| {
            !remove
                .iter()
                .any(|r| r.key == t.key && (r.value.is_none() || r.value == t.value))
        });
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let principal = body
            .get("Principal")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("Principal is required"))?
            .to_string();
        let actions: Vec<String> = body
            .get("Actions")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if actions.is_empty() {
            return Err(invalid_args("At least one action is required"));
        }
        let source_account = body
            .get("SourceAccount")
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        if ca.permissions.contains_key(&principal) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "PermissionAlreadyExistsException",
                "A permission for this principal already exists",
            ));
        }
        ca.permissions.insert(
            principal.clone(),
            Permission {
                certificate_authority_arn: arn.clone(),
                created_at: Utc::now(),
                principal,
                source_account,
                actions,
                policy: None,
            },
        );
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_permissions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let (max, start) = read_pagination(&body)?;
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let all: Vec<Value> = ca.permissions.values().map(permission_to_json).collect();
        let (page, next) = paginate(all, max, start);
        let mut resp = json!({ "Permissions": page });
        if let Some(token) = next {
            resp["NextToken"] = json!(token);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn delete_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let principal = body
            .get("Principal")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("Principal is required"))?
            .to_string();
        let mut accounts = self.state.write();
        let ca = accounts
            .accounts
            .get_mut(&account_id(req))
            .and_then(|a| a.authorities.get_mut(&arn))
            .ok_or_else(|| not_found(&arn))?;
        ca.permissions.remove(&principal);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = body
            .get("ResourceArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_arn("ResourceArn is required"))?
            .to_string();
        if !is_ca_arn(&resource_arn) {
            return Err(invalid_arn(format!("Invalid resource ARN: {resource_arn}")));
        }
        let policy = body
            .get("Policy")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_policy("Policy is required"))?
            .to_string();
        if serde_json::from_str::<Value>(&policy).is_err() {
            return Err(invalid_policy("The policy is not valid JSON"));
        }
        let mut accounts = self.state.write();
        let acct = accounts.accounts.entry(account_id(req)).or_default();
        if !acct.authorities.contains_key(&resource_arn) {
            return Err(not_found(&resource_arn));
        }
        acct.policies.insert(resource_arn, policy);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = body
            .get("ResourceArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_arn("ResourceArn is required"))?
            .to_string();
        if !is_ca_arn(&resource_arn) {
            return Err(invalid_arn(format!("Invalid resource ARN: {resource_arn}")));
        }
        let accounts = self.state.read();
        let acct = accounts
            .accounts
            .get(&account_id(req))
            .ok_or_else(|| not_found(&resource_arn))?;
        let policy = acct
            .policies
            .get(&resource_arn)
            .ok_or_else(|| resource_not_found("The resource policy does not exist"))?;
        Ok(AwsResponse::ok_json(json!({ "Policy": policy })))
    }

    fn delete_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = body
            .get("ResourceArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_arn("ResourceArn is required"))?
            .to_string();
        if !is_ca_arn(&resource_arn) {
            return Err(invalid_arn(format!("Invalid resource ARN: {resource_arn}")));
        }
        let mut accounts = self.state.write();
        let acct = accounts
            .accounts
            .get_mut(&account_id(req))
            .ok_or_else(|| not_found(&resource_arn))?;
        if !acct.authorities.contains_key(&resource_arn) {
            return Err(not_found(&resource_arn));
        }
        acct.policies.remove(&resource_arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_certificate_authority_audit_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let s3_bucket = body
            .get("S3BucketName")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("S3BucketName is required"))?
            .to_string();
        let format = body
            .get("AuditReportResponseFormat")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("AuditReportResponseFormat is required"))?
            .to_string();
        if format != "JSON" && format != "CSV" {
            return Err(invalid_args(format!(
                "Invalid AuditReportResponseFormat: {format}"
            )));
        }
        let report_id = Uuid::new_v4().to_string();
        let s3_key = format!(
            "audit-report/{}/{}.{}",
            ca_id_from_arn(&arn),
            report_id,
            format.to_lowercase()
        );

        let report_body = {
            let mut accounts = self.state.write();
            let ca = accounts
                .accounts
                .get_mut(&account_id(req))
                .and_then(|a| a.authorities.get_mut(&arn))
                .ok_or_else(|| not_found(&arn))?;
            if ca.ca_cert_pem.is_none() {
                return Err(invalid_state(
                    "The certificate authority does not have a certificate installed",
                ));
            }
            let report_body = build_audit_report(ca, &format);
            ca.audit_reports.insert(
                report_id.clone(),
                AuditReport {
                    id: report_id.clone(),
                    certificate_authority_arn: arn.clone(),
                    s3_bucket_name: s3_bucket.clone(),
                    s3_key: s3_key.clone(),
                    status: "SUCCESS".to_string(),
                    created_at: Utc::now(),
                    response_format: format.clone(),
                    body: report_body.clone(),
                },
            );
            report_body
        };

        // Deliver the report to S3 so the documented create-report -> read the
        // object from the bucket flow works. Skipped when S3 is not wired or the
        // target bucket does not exist (fakecloud has no way to reach a bucket in
        // an account it does not manage), leaving the report still describable
        // via DescribeCertificateAuthorityAuditReport.
        self.deliver_audit_report_to_s3(req, &s3_bucket, &s3_key, &format, report_body);

        Ok(AwsResponse::ok_json(
            json!({ "AuditReportId": report_id, "S3Key": s3_key }),
        ))
    }

    /// Put the generated audit report body into the target S3 bucket as a real
    /// object at `s3_key`. No-op when S3 is not wired or the bucket is absent.
    fn deliver_audit_report_to_s3(
        &self,
        req: &AwsRequest,
        bucket_name: &str,
        key: &str,
        format: &str,
        body: String,
    ) {
        let Some(s3) = &self.s3 else {
            return;
        };
        let content_type = if format == "CSV" {
            "text/csv"
        } else {
            "application/json"
        };
        let size = body.len() as u64;
        let etag = format!("\"{}\"", audit_report_etag(body.as_bytes()));
        let now = Utc::now();
        let object = S3Object {
            key: key.to_string(),
            body: memory_body(bytes::Bytes::from(body.into_bytes())),
            content_type: content_type.to_string(),
            etag,
            size,
            last_modified: now,
            storage_class: "STANDARD".to_string(),
            ..Default::default()
        };
        let mut s3_state = s3.write();
        let account = account_id(req);
        let state = s3_state.get_or_create(&account);
        if let Some(bucket) = state.buckets.get_mut(bucket_name) {
            bucket.objects.insert(key.to_string(), object);
        }
    }

    fn describe_certificate_authority_audit_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = required_ca_arn(&body)?;
        let report_id = body
            .get("AuditReportId")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_args("AuditReportId is required"))?
            .to_string();
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let report = ca
            .audit_reports
            .get(&report_id)
            .ok_or_else(|| resource_not_found("The audit report does not exist"))?;
        Ok(AwsResponse::ok_json(json!({
            "AuditReportStatus": report.status,
            "S3BucketName": report.s3_bucket_name,
            "S3Key": report.s3_key,
            "CreatedAt": report.created_at.timestamp(),
        })))
    }
}

// ─── Serialization helpers ──────────────────────────────────────────

fn ca_to_json(ca: &CertificateAuthority) -> Value {
    let mut v = json!({
        "Arn": ca.arn,
        "OwnerAccount": ca.owner_account,
        "CreatedAt": ca.created_at.timestamp(),
        "Type": ca.ca_type,
        "Status": ca.status,
        "CertificateAuthorityConfiguration": ca.configuration,
        "UsageMode": ca.usage_mode,
    });
    if let Some(s) = &ca.serial {
        v["Serial"] = json!(s);
    }
    if let Some(t) = ca.last_state_change_at {
        v["LastStateChangeAt"] = json!(t.timestamp());
    }
    if let Some(t) = ca.not_before {
        v["NotBefore"] = json!(t.timestamp());
    }
    if let Some(t) = ca.not_after {
        v["NotAfter"] = json!(t.timestamp());
    }
    if let Some(r) = &ca.failure_reason {
        v["FailureReason"] = json!(r);
    }
    if let Some(r) = &ca.revocation_configuration {
        v["RevocationConfiguration"] = r.clone();
    }
    if let Some(t) = ca.restorable_until {
        v["RestorableUntil"] = json!(t.timestamp());
    }
    if let Some(k) = &ca.key_storage_security_standard {
        v["KeyStorageSecurityStandard"] = json!(k);
    }
    v
}

fn tags_to_json(tags: &[TagEntry]) -> Vec<Value> {
    tags.iter()
        .map(|t| {
            let mut m = json!({ "Key": t.key });
            if let Some(v) = &t.value {
                m["Value"] = json!(v);
            }
            m
        })
        .collect()
}

fn permission_to_json(p: &Permission) -> Value {
    let mut v = json!({
        "CertificateAuthorityArn": p.certificate_authority_arn,
        "CreatedAt": p.created_at.timestamp(),
        "Principal": p.principal,
        "Actions": p.actions,
    });
    if let Some(s) = &p.source_account {
        v["SourceAccount"] = json!(s);
    }
    v
}

/// Build a real audit report body listing issued and revoked certificates.
fn build_audit_report(ca: &CertificateAuthority, format: &str) -> String {
    if format == "CSV" {
        let mut out = String::from(
            "awsAccountId,certificateArn,serial,notBefore,notAfter,issuedAt,revokedAt,revocationReason\n",
        );
        for cert in ca.issued.values() {
            let revoked = ca.revoked.get(&cert.serial);
            out.push_str(&format!(
                "{},{},{},{},{},{},{},{}\n",
                ca.owner_account,
                cert.arn,
                cert.serial,
                cert.not_before.to_rfc3339(),
                cert.not_after.to_rfc3339(),
                cert.issued_at.to_rfc3339(),
                revoked
                    .map(|r| r.revoked_at.to_rfc3339())
                    .unwrap_or_default(),
                revoked.map(|r| r.reason.clone()).unwrap_or_default(),
            ));
        }
        out
    } else {
        let entries: Vec<Value> = ca
            .issued
            .values()
            .map(|cert| {
                let revoked = ca.revoked.get(&cert.serial);
                let mut e = json!({
                    "awsAccountId": ca.owner_account,
                    "certificateArn": cert.arn,
                    "serial": cert.serial,
                    "notBefore": cert.not_before.to_rfc3339(),
                    "notAfter": cert.not_after.to_rfc3339(),
                    "issuedAt": cert.issued_at.to_rfc3339(),
                });
                if let Some(r) = revoked {
                    e["revokedAt"] = json!(r.revoked_at.to_rfc3339());
                    e["revocationReason"] = json!(r.reason);
                }
                e
            })
            .collect();
        serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
    }
}

// ─── Input helpers ───────────────────────────────────────────────────

fn account_id(req: &AwsRequest) -> String {
    if req.account_id.is_empty() {
        "000000000000".to_string()
    } else {
        req.account_id.clone()
    }
}

fn region(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

fn is_ca_arn(arn: &str) -> bool {
    arn.starts_with("arn:aws:acm-pca:") && arn.contains(":certificate-authority/")
}

/// AWS's IdempotencyToken de-duplication window for both
/// `CreateCertificateAuthority` and `IssueCertificate`: a repeated token within
/// this window returns the resource already created; past it the same token
/// mints a fresh resource.
const IDEMPOTENCY_WINDOW_MINUTES: i64 = 5;

/// True when `created` is recent enough that a repeated idempotency token should
/// still collapse onto the resource created at that time.
fn within_idempotency_window(created: DateTime<Utc>) -> bool {
    Utc::now().signed_duration_since(created)
        < chrono::Duration::minutes(IDEMPOTENCY_WINDOW_MINUTES)
}

// ─── Pagination ─────────────────────────────────────────────────────

fn invalid_next_token() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidNextTokenException",
        "The NextToken value is invalid",
    )
}

/// Opaque pagination token: base64 of the 0-based offset to resume from. Kept
/// server-side-decodable so a client that round-trips the token continues
/// exactly where the previous page ended.
fn encode_next_token(offset: usize) -> String {
    base64::engine::general_purpose::STANDARD.encode(offset.to_string())
}

fn decode_next_token(token: &str) -> Result<usize, AwsServiceError> {
    base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(invalid_next_token)
}

/// Read and validate the shared `MaxResults` (1..=1000) + `NextToken` paging
/// inputs, resolving them to `(max_results, start_offset)`.
fn read_pagination(body: &Value) -> Result<(Option<usize>, usize), AwsServiceError> {
    let max = match body.get("MaxResults") {
        Some(v) => match v.as_i64() {
            Some(n) if (1..=1000).contains(&n) => Some(n as usize),
            _ => return Err(invalid_args("MaxResults must be between 1 and 1000")),
        },
        None => None,
    };
    let start = match body.get("NextToken").and_then(Value::as_str) {
        Some(t) => decode_next_token(t)?,
        None => 0,
    };
    Ok((max, start))
}

/// Slice `items` for the requested page, returning the page plus the NextToken
/// to continue with (`None` once the page reaches the end of the list).
fn paginate<T>(items: Vec<T>, max: Option<usize>, start: usize) -> (Vec<T>, Option<String>) {
    let total = items.len();
    let start = start.min(total);
    let take = max.unwrap_or(usize::MAX);
    let end = start.saturating_add(take).min(total);
    let page: Vec<T> = items.into_iter().skip(start).take(end - start).collect();
    let next = if end < total {
        Some(encode_next_token(end))
    } else {
        None
    };
    (page, next)
}

/// A stable 32-hex-char ETag for a delivered audit report object (S3 ETags are
/// hex; the exact hash function is unobservable, so a SHA-256 prefix suffices).
fn audit_report_etag(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    hex::encode(&digest[..16])
}

fn ca_id_from_arn(arn: &str) -> &str {
    arn.rsplit_once("certificate-authority/")
        .map(|(_, id)| id)
        .unwrap_or(arn)
}

/// Normalize a certificate serial to fakecloud's stored form (lowercase hex,
/// no separators) so callers can pass any AWS presentation — plain lowercase
/// or uppercase hex, or colon-delimited (`1a:2b:...`), with an optional `0x`.
fn normalize_serial(s: &str) -> String {
    let s = s.trim();
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    s.chars()
        .filter(|c| !c.is_whitespace() && *c != ':')
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Extract + validate the `CertificateAuthorityArn` field.
fn required_ca_arn(body: &Value) -> Result<String, AwsServiceError> {
    let arn = body
        .get("CertificateAuthorityArn")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_arn("CertificateAuthorityArn is required"))?;
    if !is_ca_arn(arn) {
        return Err(invalid_arn(format!(
            "Malformed certificate authority ARN: {arn}"
        )));
    }
    Ok(arn.to_string())
}

/// Decode a Smithy blob field, which the SDK sends base64-encoded over
/// awsJson. Falls back to the raw string when it is already PEM text.
fn decode_blob(v: Option<&Value>) -> Option<String> {
    let s = v.and_then(Value::as_str)?;
    if s.contains("BEGIN ") {
        return Some(s.to_string());
    }
    match base64::engine::general_purpose::STANDARD.decode(s) {
        Ok(bytes) => String::from_utf8(bytes).ok(),
        Err(_) => Some(s.to_string()),
    }
}

fn parse_tags(v: Option<&Value>) -> Result<Vec<TagEntry>, AwsServiceError> {
    let Some(arr) = v.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for t in arr {
        let key = t
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_tag("Tag Key is required"))?
            .to_string();
        let value = t.get("Value").and_then(Value::as_str).map(str::to_string);
        out.push(TagEntry { key, value });
    }
    Ok(out)
}

// ─── Error constructors (declared shape names) ──────────────────────

fn invalid_args(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgsException", msg)
}
fn invalid_arn(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArnException", msg)
}
fn invalid_state(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidStateException", msg)
}
fn invalid_policy(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidPolicyException", msg)
}
fn invalid_tag(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidTagException", msg)
}
fn malformed_csr(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "MalformedCSRException", msg)
}
fn malformed_certificate(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedCertificateException",
        msg,
    )
}
fn request_failed(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "RequestFailedException", msg)
}
fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}
fn not_found(arn: &str) -> AwsServiceError {
    resource_not_found(format!(
        "The certificate authority {arn} was not found or is in an invalid state"
    ))
}

const VALID_SIGNING_ALGORITHMS: &[&str] = &[
    "SHA256WITHECDSA",
    "SHA384WITHECDSA",
    "SHA512WITHECDSA",
    "SHA256WITHRSA",
    "SHA384WITHRSA",
    "SHA512WITHRSA",
    "SM3WITHSM2",
    "ML_DSA_44",
    "ML_DSA_65",
    "ML_DSA_87",
];

const VALID_KEY_STORAGE_STANDARDS: &[&str] = &[
    "FIPS_140_2_LEVEL_2_OR_HIGHER",
    "FIPS_140_2_LEVEL_3_OR_HIGHER",
    "CCPC_LEVEL_1_OR_HIGHER",
];

const VALID_REVOCATION_REASONS: &[&str] = &[
    "UNSPECIFIED",
    "KEY_COMPROMISE",
    "CERTIFICATE_AUTHORITY_COMPROMISE",
    "AFFILIATION_CHANGED",
    "SUPERSEDED",
    "CESSATION_OF_OPERATION",
    "PRIVILEGE_WITHDRAWN",
    "A_A_COMPROMISE",
];

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::service::AwsRequest;

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "acm-pca".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    const ROOT_TEMPLATE: &str = "arn:aws:acm-pca:::template/RootCACertificate/V1";
    const TEST_ACCOUNT: &str = "123456789012";

    /// A freshly created CA is `PENDING_CERTIFICATE` immediately; its real key
    /// pair is generated in the background. Assert the status is `PENDING_CERTIFICATE`
    /// straight away, then wait for the key material to become available (the unit
    /// tests call handlers directly, bypassing the `handle` dispatch that awaits
    /// keygen for the HTTP path).
    async fn poll_pending(svc: &AcmPcaService, arn: &str) {
        let d = svc
            .describe_certificate_authority(&req(
                "DescribeCertificateAuthority",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap();
        assert_eq!(
            body_json(&d)["CertificateAuthority"]["Status"],
            "PENDING_CERTIFICATE",
            "CA should be PENDING_CERTIFICATE immediately after create"
        );
        svc.ensure_key_ready(TEST_ACCOUNT, arn).await;
        let st = svc.state.read();
        let ca = st
            .accounts
            .get(TEST_ACCOUNT)
            .and_then(|a| a.authorities.get(arn))
            .unwrap();
        assert_ne!(ca.status, "FAILED", "CA key generation FAILED");
        assert!(!ca.ca_key_pem.is_empty(), "CA key never became ready");
    }

    /// Run the full activation ceremony for a ROOT CA: wait for keygen, fetch the
    /// CA's own CSR, self-sign it with the `RootCACertificate` template, then
    /// import the resulting certificate to bring the CA to `ACTIVE`. Mirrors the
    /// real AWS flow where every CA starts `PENDING_CERTIFICATE`.
    async fn create_active_root(svc: &AcmPcaService, key_algo: &str, signing_algo: &str) -> String {
        let resp = svc
            .create_certificate_authority(&req(
                "CreateCertificateAuthority",
                json!({
                    "CertificateAuthorityConfiguration": {
                        "KeyAlgorithm": key_algo,
                        "SigningAlgorithm": signing_algo,
                        "Subject": { "CommonName": "root.example.com", "Organization": "Test" }
                    },
                    "CertificateAuthorityType": "ROOT"
                }),
            ))
            .unwrap();
        let arn = body_json(&resp)["CertificateAuthorityArn"]
            .as_str()
            .unwrap()
            .to_string();

        // A brand-new CA is PENDING_CERTIFICATE (not ACTIVE) after keygen.
        poll_pending(svc, &arn).await;

        let csr = body_json(
            &svc.get_certificate_authority_csr(&req(
                "GetCertificateAuthorityCsr",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap(),
        )["Csr"]
            .as_str()
            .unwrap()
            .to_string();

        // Self-sign the root certificate (permitted while PENDING_CERTIFICATE).
        let issued = svc
            .issue_certificate(&req(
                "IssueCertificate",
                json!({
                    "CertificateAuthorityArn": arn,
                    "Csr": csr,
                    "SigningAlgorithm": signing_algo,
                    "TemplateArn": ROOT_TEMPLATE,
                    "Validity": { "Value": 3650, "Type": "DAYS" }
                }),
            ))
            .unwrap();
        let cert_arn = body_json(&issued)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let root_cert = body_json(
            &svc.get_certificate(&req(
                "GetCertificate",
                json!({ "CertificateAuthorityArn": arn, "CertificateArn": cert_arn }),
            ))
            .unwrap(),
        )["Certificate"]
            .as_str()
            .unwrap()
            .to_string();

        // Import the self-signed root -> CA becomes ACTIVE.
        svc.import_certificate_authority_certificate(&req(
            "ImportCertificateAuthorityCertificate",
            json!({ "CertificateAuthorityArn": arn, "Certificate": root_cert }),
        ))
        .unwrap();

        let d = svc
            .describe_certificate_authority(&req(
                "DescribeCertificateAuthority",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap();
        assert_eq!(body_json(&d)["CertificateAuthority"]["Status"], "ACTIVE");
        arn
    }

    /// Build a real end-entity CSR for a leaf certificate request.
    fn leaf_csr() -> String {
        let client_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params =
            rcgen::CertificateParams::new(vec!["leaf.example.com".to_string()]).unwrap();
        params.distinguished_name = {
            let mut dn = rcgen::DistinguishedName::new();
            dn.push(rcgen::DnType::CommonName, "leaf.example.com");
            dn
        };
        params
            .serialize_request(&client_key)
            .unwrap()
            .pem()
            .unwrap()
    }

    /// Issue a leaf cert against `arn` and return the leaf PEM.
    fn issue_leaf(svc: &AcmPcaService, arn: &str, signing_algo: &str) -> String {
        let issued = svc
            .issue_certificate(&req(
                "IssueCertificate",
                json!({
                    "CertificateAuthorityArn": arn,
                    "Csr": leaf_csr(),
                    "SigningAlgorithm": signing_algo,
                    "Validity": { "Value": 365, "Type": "DAYS" }
                }),
            ))
            .unwrap();
        let cert_arn = body_json(&issued)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        body_json(
            &svc.get_certificate(&req(
                "GetCertificate",
                json!({ "CertificateAuthorityArn": arn, "CertificateArn": cert_arn }),
            ))
            .unwrap(),
        )["Certificate"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Assert that `leaf_pem` was really signed by the CA cert in `ca_cert_pem`.
    fn assert_leaf_verifies(leaf_pem: &str, ca_cert_pem: &str) {
        let (_, ca_pem) = x509_parser::pem::parse_x509_pem(ca_cert_pem.as_bytes()).unwrap();
        let ca_x509 = ca_pem.parse_x509().unwrap();
        let (_, leaf_der) = x509_parser::pem::parse_x509_pem(leaf_pem.as_bytes()).unwrap();
        let leaf_x509 = leaf_der.parse_x509().unwrap();
        assert_eq!(
            leaf_x509.issuer().to_string(),
            ca_x509.subject().to_string()
        );
        leaf_x509
            .verify_signature(Some(ca_x509.public_key()))
            .expect("issued certificate must verify against the CA");
    }

    #[tokio::test]
    async fn root_ca_issues_verifiable_certificate() {
        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;

        let ca_cert = body_json(
            &svc.get_certificate_authority_certificate(&req(
                "GetCertificateAuthorityCertificate",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap(),
        )["Certificate"]
            .as_str()
            .unwrap()
            .to_string();

        let leaf = issue_leaf(&svc, &arn, "SHA256WITHECDSA");
        assert_leaf_verifies(&leaf, &ca_cert);
    }

    /// Finding 2 regression: a real RSA CA's key material must survive a restart
    /// (serialize the snapshot, reload it into a fresh service) and still sign
    /// verifiable certificates. This exercises the `rsa`-crate keygen + PEM
    /// reload path end to end.
    #[tokio::test]
    async fn rsa_ca_survives_restart_and_keeps_issuing() {
        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "RSA_2048", "SHA256WITHRSA").await;

        let ca_cert = body_json(
            &svc.get_certificate_authority_certificate(&req(
                "GetCertificateAuthorityCertificate",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap(),
        )["Certificate"]
            .as_str()
            .unwrap()
            .to_string();

        // First issuance before the restart.
        let leaf_before = issue_leaf(&svc, &arn, "SHA256WITHRSA");
        assert_leaf_verifies(&leaf_before, &ca_cert);

        // Simulate a restart: serialize the snapshot and rehydrate a new service.
        let snapshot = crate::state::AcmPcaSnapshot {
            schema_version: crate::state::ACM_PCA_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(svc.state.read().clone()),
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let restored: crate::state::AcmPcaSnapshot = serde_json::from_slice(&bytes).unwrap();
        let svc2 = AcmPcaService::new(Arc::new(RwLock::new(restored.accounts.unwrap())));

        // The reloaded RSA CA must still sign verifiable certificates.
        let leaf_after = issue_leaf(&svc2, &arn, "SHA256WITHRSA");
        assert_leaf_verifies(&leaf_after, &ca_cert);
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// `AwsResponse` is not `Debug`, so `Result::unwrap_err` cannot be used;
    /// this pulls the error out (panicking if the call unexpectedly succeeded).
    fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error but the call succeeded"),
            Err(e) => e,
        }
    }

    fn create_ca(svc: &AcmPcaService, common_name: &str, extra: Value) -> String {
        let mut body = json!({
            "CertificateAuthorityConfiguration": {
                "KeyAlgorithm": "EC_prime256v1",
                "SigningAlgorithm": "SHA256WITHECDSA",
                "Subject": { "CommonName": common_name }
            },
            "CertificateAuthorityType": "ROOT"
        });
        if let Value::Object(map) = extra {
            for (k, v) in map {
                body[k] = v;
            }
        }
        body_json(
            &svc.create_certificate_authority(&req("CreateCertificateAuthority", body))
                .unwrap(),
        )["CertificateAuthorityArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Finding 1: repeated CreateCertificateAuthority carrying the same
    /// IdempotencyToken within the window collapses onto one CA/ARN.
    #[tokio::test]
    async fn create_certificate_authority_is_idempotent_within_window() {
        let svc = AcmPcaService::default();
        let first = create_ca(
            &svc,
            "idem.example.com",
            json!({ "IdempotencyToken": "tok-1" }),
        );
        let second = create_ca(
            &svc,
            "idem.example.com",
            json!({ "IdempotencyToken": "tok-1" }),
        );
        assert_eq!(
            first, second,
            "same idempotency token must return the same CA ARN"
        );
        let count = svc
            .state
            .read()
            .accounts
            .get(TEST_ACCOUNT)
            .map(|a| a.authorities.len())
            .unwrap_or(0);
        assert_eq!(count, 1, "repeated create must not mint a second CA");

        // A different token creates a distinct CA.
        let third = create_ca(
            &svc,
            "idem.example.com",
            json!({ "IdempotencyToken": "tok-2" }),
        );
        assert_ne!(first, third, "a different token must create a new CA");
    }

    /// Finding 1: repeated IssueCertificate with the same IdempotencyToken
    /// returns the certificate already issued instead of signing a second one.
    #[tokio::test]
    async fn issue_certificate_is_idempotent_within_window() {
        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;
        let issue_body = json!({
            "CertificateAuthorityArn": arn,
            "Csr": leaf_csr(),
            "SigningAlgorithm": "SHA256WITHECDSA",
            "Validity": { "Value": 365, "Type": "DAYS" },
            "IdempotencyToken": "issue-tok-1"
        });
        let first = body_json(
            &svc.issue_certificate(&req("IssueCertificate", issue_body.clone()))
                .unwrap(),
        )["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let second = body_json(
            &svc.issue_certificate(&req("IssueCertificate", issue_body))
                .unwrap(),
        )["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            first, second,
            "same idempotency token must return the same certificate ARN"
        );
        // Exactly one certificate carries the shared token (the other issued
        // cert is the root's own self-signed activation certificate).
        let with_token = svc
            .state
            .read()
            .accounts
            .get(TEST_ACCOUNT)
            .unwrap()
            .authorities
            .get(&arn)
            .unwrap()
            .issued
            .values()
            .filter(|c| c.idempotency_token.as_deref() == Some("issue-tok-1"))
            .count();
        assert_eq!(
            with_token, 1,
            "repeated issue must not sign a second certificate"
        );
    }

    /// Finding 2: ListCertificateAuthorities honours MaxResults and round-trips
    /// its NextToken so every CA is returned exactly once across pages.
    #[tokio::test]
    async fn list_certificate_authorities_paginates() {
        let svc = AcmPcaService::default();
        for i in 0..3 {
            create_ca(&svc, &format!("ca{i}.example.com"), json!({}));
        }
        let page1 = body_json(
            &svc.list_certificate_authorities(&req(
                "ListCertificateAuthorities",
                json!({ "MaxResults": 2 }),
            ))
            .unwrap(),
        );
        let list1 = page1["CertificateAuthorities"].as_array().unwrap().clone();
        assert_eq!(list1.len(), 2, "first page must respect MaxResults");
        let token = page1["NextToken"]
            .as_str()
            .expect("a truncated page must carry a NextToken")
            .to_string();

        let page2 = body_json(
            &svc.list_certificate_authorities(&req(
                "ListCertificateAuthorities",
                json!({ "MaxResults": 2, "NextToken": token }),
            ))
            .unwrap(),
        );
        let list2 = page2["CertificateAuthorities"].as_array().unwrap().clone();
        assert_eq!(list2.len(), 1, "second page must hold the remainder");
        assert!(
            page2.get("NextToken").is_none(),
            "the final page must not carry a NextToken"
        );

        let mut arns: Vec<String> = list1
            .iter()
            .chain(list2.iter())
            .map(|c| c["Arn"].as_str().unwrap().to_string())
            .collect();
        arns.sort();
        arns.dedup();
        assert_eq!(arns.len(), 3, "pagination must cover all three CAs once");

        // A garbage NextToken is rejected.
        let err = expect_err(svc.list_certificate_authorities(&req(
            "ListCertificateAuthorities",
            json!({ "NextToken": "not-base64-@@" }),
        )));
        assert_eq!(err.code(), "InvalidNextTokenException");
    }

    /// Finding 3: the audit report is delivered as a real object into the target
    /// S3 bucket, so the documented create-report -> read-from-S3 flow works.
    #[tokio::test]
    async fn audit_report_is_delivered_to_s3() {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_s3::{S3Bucket, S3State, SharedS3State};

        let s3: SharedS3State = Arc::new(RwLock::new(MultiAccountState::<S3State>::new(
            TEST_ACCOUNT,
            "us-east-1",
            "http://localhost",
        )));
        s3.write().get_or_create(TEST_ACCOUNT).buckets.insert(
            "audit-bucket".to_string(),
            S3Bucket::new("audit-bucket", "us-east-1", TEST_ACCOUNT),
        );

        let svc =
            AcmPcaService::new(Arc::new(RwLock::new(AcmPcaAccounts::new()))).with_s3(s3.clone());
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;
        // Issue a leaf so the report body has a certificate row.
        let leaf_arn = body_json(
            &svc.issue_certificate(&req(
                "IssueCertificate",
                json!({
                    "CertificateAuthorityArn": arn,
                    "Csr": leaf_csr(),
                    "SigningAlgorithm": "SHA256WITHECDSA",
                    "Validity": { "Value": 365, "Type": "DAYS" }
                }),
            ))
            .unwrap(),
        )["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();

        let created = body_json(
            &svc.create_certificate_authority_audit_report(&req(
                "CreateCertificateAuthorityAuditReport",
                json!({
                    "CertificateAuthorityArn": arn,
                    "S3BucketName": "audit-bucket",
                    "AuditReportResponseFormat": "JSON"
                }),
            ))
            .unwrap(),
        );
        let s3_key = created["S3Key"].as_str().unwrap().to_string();

        // The object really exists in the bucket and its body is the report.
        let bytes = {
            let st = s3.read();
            let bucket = st
                .get(TEST_ACCOUNT)
                .unwrap()
                .buckets
                .get("audit-bucket")
                .unwrap();
            let obj = bucket
                .objects
                .get(&s3_key)
                .expect("audit report object must be delivered to the bucket");
            assert_eq!(obj.content_type, "application/json");
            S3State::read_body_uncached(&obj.body).unwrap()
        };
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        let entries = parsed.as_array().unwrap();
        // The report lists every issued cert (the leaf plus the root's own
        // self-signed activation certificate).
        assert!(
            entries
                .iter()
                .any(|e| e["certificateArn"].as_str() == Some(leaf_arn.as_str())),
            "delivered report must include the issued leaf certificate"
        );
    }

    /// Finding 4: a validity past the representable certificate-expiry ceiling
    /// (year 9999) is rejected with a ValidationException, not silently clamped
    /// to the Unix epoch (which would produce a backwards not_after < not_before).
    #[tokio::test]
    async fn over_large_validity_is_rejected() {
        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;
        // END_DATE in year 10000 -> beyond the time crate's year-9999 ceiling.
        let err = expect_err(svc.issue_certificate(&req(
            "IssueCertificate",
            json!({
                "CertificateAuthorityArn": arn,
                "Csr": leaf_csr(),
                "SigningAlgorithm": "SHA256WITHECDSA",
                "Validity": { "Value": 100_000_101_000_000i64, "Type": "END_DATE" }
            }),
        )));
        assert_eq!(err.code(), "InvalidArgsException");
    }

    /// Bug-hunt regression: `IssueCertificate` `ApiPassthrough` (Subject +
    /// Extensions) is stamped into the issued certificate, so a caller that asks
    /// for subject-alternative-names, a subject override, key-usage and
    /// extended-key-usage actually gets them back from `GetCertificate` — rather
    /// than the field being silently accepted and dropped.
    #[tokio::test]
    async fn issue_certificate_applies_api_passthrough() {
        use x509_parser::extensions::GeneralName;

        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;

        let issued = svc
            .issue_certificate(&req(
                "IssueCertificate",
                json!({
                    "CertificateAuthorityArn": arn,
                    "Csr": leaf_csr(),
                    "SigningAlgorithm": "SHA256WITHECDSA",
                    "Validity": { "Value": 365, "Type": "DAYS" },
                    "ApiPassthrough": {
                        "Subject": { "CommonName": "passthrough.example.com" },
                        "Extensions": {
                            "SubjectAlternativeNames": [
                                { "DnsName": "alt.example.com" },
                                { "IpAddress": "10.0.0.1" }
                            ],
                            "KeyUsage": { "DigitalSignature": true, "KeyEncipherment": true },
                            "ExtendedKeyUsage": [
                                { "ExtendedKeyUsageType": "SERVER_AUTH" }
                            ]
                        }
                    }
                }),
            ))
            .unwrap();
        let cert_arn = body_json(&issued)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let leaf_pem = body_json(
            &svc.get_certificate(&req(
                "GetCertificate",
                json!({ "CertificateAuthorityArn": arn, "CertificateArn": cert_arn }),
            ))
            .unwrap(),
        )["Certificate"]
            .as_str()
            .unwrap()
            .to_string();

        let (_, pem) = x509_parser::pem::parse_x509_pem(leaf_pem.as_bytes()).unwrap();
        let cert = pem.parse_x509().unwrap();

        // Subject override applied.
        assert!(
            cert.subject()
                .to_string()
                .contains("passthrough.example.com"),
            "ApiPassthrough Subject must override the certificate subject, got: {}",
            cert.subject()
        );

        // Both SANs present.
        let san = cert
            .subject_alternative_name()
            .unwrap()
            .expect("issued cert must carry a SubjectAlternativeName extension");
        let has_dns = san
            .value
            .general_names
            .iter()
            .any(|g| matches!(g, GeneralName::DNSName("alt.example.com")));
        let has_ip = san
            .value
            .general_names
            .iter()
            .any(|g| matches!(g, GeneralName::IPAddress(&[10, 0, 0, 1])));
        assert!(
            has_dns,
            "ApiPassthrough DnsName SAN must appear in the cert"
        );
        assert!(
            has_ip,
            "ApiPassthrough IpAddress SAN must appear in the cert"
        );

        // KeyUsage applied.
        let ku = cert
            .key_usage()
            .unwrap()
            .expect("issued cert must carry a KeyUsage extension");
        assert!(ku.value.digital_signature());
        assert!(ku.value.key_encipherment());

        // ExtendedKeyUsage applied.
        let eku = cert
            .extended_key_usage()
            .unwrap()
            .expect("issued cert must carry an ExtendedKeyUsage extension");
        assert!(eku.value.server_auth, "SERVER_AUTH EKU must be present");
    }

    /// Bug-hunt regression: an unsupported `ApiPassthrough` extension
    /// (`CertificatePolicies`, which cannot be faithfully encoded) is rejected
    /// with an error rather than silently accepted and dropped.
    #[tokio::test]
    async fn issue_certificate_rejects_unsupported_passthrough() {
        let svc = AcmPcaService::default();
        let arn = create_active_root(&svc, "EC_prime256v1", "SHA256WITHECDSA").await;
        let err = expect_err(svc.issue_certificate(&req(
            "IssueCertificate",
            json!({
                "CertificateAuthorityArn": arn,
                "Csr": leaf_csr(),
                "SigningAlgorithm": "SHA256WITHECDSA",
                "Validity": { "Value": 365, "Type": "DAYS" },
                "ApiPassthrough": {
                    "Extensions": {
                        "CertificatePolicies": [ { "CertPolicyId": "1.2.3.4" } ]
                    }
                }
            }),
        )));
        assert_eq!(err.code(), "MalformedCSRException");
    }

    /// UpdateCertificateAuthority(Status): a PENDING_CERTIFICATE CA can be moved
    /// to DISABLED (the terraform destroy path relies on this — it disables a CA
    /// before deleting it even when no certificate was ever installed), but it
    /// cannot be set to ACTIVE without an installed certificate.
    #[tokio::test]
    async fn update_status_transitions_from_pending_ca() {
        let svc = AcmPcaService::default();
        let arn = create_ca(&svc, "pending.example.com", json!({}));

        // PENDING_CERTIFICATE -> DISABLED is allowed.
        svc.update_certificate_authority(&req(
            "UpdateCertificateAuthority",
            json!({ "CertificateAuthorityArn": arn, "Status": "DISABLED" }),
        ))
        .unwrap();
        let status = body_json(
            &svc.describe_certificate_authority(&req(
                "DescribeCertificateAuthority",
                json!({ "CertificateAuthorityArn": arn }),
            ))
            .unwrap(),
        )["CertificateAuthority"]["Status"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(status, "DISABLED");

        // But it cannot be set ACTIVE without an installed certificate.
        let err = expect_err(svc.update_certificate_authority(&req(
            "UpdateCertificateAuthority",
            json!({ "CertificateAuthorityArn": arn, "Status": "ACTIVE" }),
        )));
        assert_eq!(err.code(), "InvalidStateException");
    }
}
