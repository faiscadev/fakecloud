//! ACM (Certificate Manager) JSON 1.1 service.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::{Duration, Utc};
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    AccountState, AcmAccounts, AcmSnapshot, CertificateOptions, DomainValidation, RenewalSummary,
    SharedAcmState, StoredCertificate, ACM_SNAPSHOT_SCHEMA_VERSION,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "RequestCertificate",
    "DescribeCertificate",
    "ListCertificates",
    "DeleteCertificate",
    "ImportCertificate",
    "ExportCertificate",
    "GetCertificate",
    "RenewCertificate",
    "RevokeCertificate",
    "ResendValidationEmail",
    "AddTagsToCertificate",
    "RemoveTagsFromCertificate",
    "ListTagsForCertificate",
    "GetAccountConfiguration",
    "PutAccountConfiguration",
    "UpdateCertificateOptions",
    "SearchCertificates",
];

/// Actions that mutate persisted ACM state and therefore must trigger a
/// snapshot write. Read-only actions (Describe/List/Get/Export/Search) and
/// ResendValidationEmail (no state change) are excluded.
const MUTATING_ACTIONS: &[&str] = &[
    "RequestCertificate",
    "DeleteCertificate",
    "ImportCertificate",
    "RenewCertificate",
    "RevokeCertificate",
    "AddTagsToCertificate",
    "RemoveTagsFromCertificate",
    "PutAccountConfiguration",
    "UpdateCertificateOptions",
];

pub struct AcmService {
    state: SharedAcmState,
    /// How long the auto-issue tick sleeps before flipping a freshly
    /// requested DNS cert from `PENDING_VALIDATION` to `ISSUED`. Real
    /// ACM takes minutes; the default of 5s keeps SDK-driven integration
    /// tests fast while still simulating the async transition. EMAIL
    /// certs do **not** auto-issue — they wait for the admin
    /// `/_fakecloud/acm/certificates/{arn}/approve` endpoint. Tests can
    /// shrink the delay via the env var `FAKECLOUD_ACM_AUTO_ISSUE_SECS`
    /// (read at construction time) or via
    /// [`AcmService::with_pending_validation_delay`].
    pending_validation_delay: std::time::Duration,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// Default DNS auto-issue delay when `FAKECLOUD_ACM_AUTO_ISSUE_SECS` is
/// unset. Five seconds is short enough to keep most real-world SDK
/// flows snappy without making race-prone tests pass by accident.
const DEFAULT_AUTO_ISSUE_SECS: u64 = 5;

fn auto_issue_delay_from_env() -> std::time::Duration {
    let secs = std::env::var("FAKECLOUD_ACM_AUTO_ISSUE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_AUTO_ISSUE_SECS);
    std::time::Duration::from_secs(secs)
}

impl AcmService {
    pub fn new(state: SharedAcmState) -> Self {
        Self {
            state,
            pending_validation_delay: auto_issue_delay_from_env(),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedAcmState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_acm_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current ACM state when invoked, or `None`
    /// in memory mode. The CloudFormation provisioner mutates `state` directly
    /// and uses this to write a CFN-provisioned certificate through to disk,
    /// the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_acm_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Admin-endpoint flavour of [`set_certificate_status`](Self::set_certificate_status)
    /// that persists the flip so a forced status survives a restart.
    pub async fn set_certificate_status_persistent(
        &self,
        arn_or_id: &str,
        status: &str,
        reason: Option<String>,
    ) -> bool {
        let changed = self.set_certificate_status(arn_or_id, status, reason);
        if changed {
            self.save_snapshot().await;
        }
        changed
    }

    /// Admin-endpoint flavour of [`approve_certificate`](Self::approve_certificate)
    /// that persists the flip to `ISSUED`.
    pub async fn approve_certificate_persistent(&self, arn_or_id: &str) -> bool {
        self.set_certificate_status_persistent(arn_or_id, "ISSUED", None)
            .await
    }

    /// Re-arm the async auto-issue tick for any certificate that was still
    /// `PENDING_VALIDATION` (DNS, AMAZON_ISSUED) when the previous process
    /// exited. Without this, a restored pending cert would never transition to
    /// `ISSUED`. Called by the server after loading a persistence snapshot.
    pub fn rearm_pending_validations(&self) {
        let pending: Vec<(String, String)> = {
            let state = self.state.read();
            let mut out = Vec::new();
            for (account_id, account) in state.accounts.iter() {
                for (arn, cert) in account.certificates.iter() {
                    if cert.status == "PENDING_VALIDATION"
                        && cert.cert_type == "AMAZON_ISSUED"
                        && cert.validation_method.as_deref() == Some("DNS")
                    {
                        out.push((account_id.clone(), arn.clone()));
                    }
                }
            }
            out
        };
        for (account_id, arn) in pending {
            self.spawn_auto_issue_tick(account_id, arn);
        }
    }

    /// Spawn the background task that flips a DNS, AMAZON_ISSUED certificate
    /// from `PENDING_VALIDATION` to `ISSUED` after `pending_validation_delay`,
    /// then persists the transition. Shared by `request_certificate` and
    /// `rearm_pending_validations`.
    fn spawn_auto_issue_tick(&self, account_id: String, arn: String) {
        let state_for_tick = Arc::clone(&self.state);
        let delay = self.pending_validation_delay;
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let flipped = {
                let mut state = state_for_tick.write();
                let mut flipped = false;
                if let Some(account) = state.accounts.get_mut(&account_id) {
                    if let Some(cert) = account.certificates.get_mut(&arn) {
                        if cert.status == "PENDING_VALIDATION" && cert.cert_type == "AMAZON_ISSUED"
                        {
                            let now = Utc::now();
                            cert.status = "ISSUED".to_string();
                            cert.issued_at = Some(now);
                            cert.renewal_eligibility = "ELIGIBLE".to_string();
                            for dv in cert.domain_validation.iter_mut() {
                                dv.validation_status = "SUCCESS".to_string();
                            }
                            cert.renewal_summary = Some(RenewalSummary {
                                renewal_status: "PENDING_AUTO_RENEWAL".to_string(),
                                domain_validation: cert.domain_validation.clone(),
                                renewal_status_reason: None,
                                updated_at: now,
                            });
                            flipped = true;
                        }
                    }
                }
                flipped
            };
            if flipped {
                save_acm_snapshot(&state_for_tick, store, &lock).await;
            }
        });
    }

    /// Override the auto-issue delay. Used by unit tests so they don't
    /// have to wait wall-clock seconds for the tick to flip
    /// `PENDING_VALIDATION` -> `ISSUED`.
    pub fn with_pending_validation_delay(mut self, delay: std::time::Duration) -> Self {
        self.pending_validation_delay = delay;
        self
    }

    /// Synchronously approve a `PENDING_VALIDATION` certificate.
    /// Mirrors the admin endpoint
    /// `POST /_fakecloud/acm/certificates/{arn-or-id}/approve` — equivalent
    /// to the user clicking the approval link in the validation email
    /// (or in the case of DNS, the validation record landing). Returns
    /// `false` if no certificate matches `arn_or_id` across any
    /// account. Already-issued certs are left alone (returns `true`)
    /// so the endpoint is idempotent.
    pub fn approve_certificate(&self, arn_or_id: &str) -> bool {
        self.set_certificate_status(arn_or_id, "ISSUED", None)
    }

    /// Return parsed-on-best-effort structural info about an imported
    /// certificate + chain, surfaced via
    /// `/_fakecloud/acm/certificates/{arn-or-id}/chain-info` so tests can
    /// assert what fakecloud actually accepted at ImportCertificate.
    ///
    /// We deliberately do **not** anchor the chain to a real root —
    /// fakecloud is not a PKI. The returned JSON exposes the PEM block
    /// counts and byte lengths so callers can confirm we received the
    /// chain they expected, plus an `external_ca_validated: false`
    /// marker that documents the emulator gap.
    pub fn chain_info(&self, arn_or_id: &str) -> Option<serde_json::Value> {
        let state = self.state.read();
        for account in state.accounts.values() {
            let key = account
                .certificates
                .keys()
                .find(|k| k.as_str() == arn_or_id || cert_id_from_arn(k) == arn_or_id)
                .cloned()?;
            if let Some(cert) = account.certificates.get(&key) {
                let pem = cert.certificate_pem.as_deref().unwrap_or("");
                let chain = cert.certificate_chain_pem.as_deref().unwrap_or("");
                return Some(serde_json::json!({
                    "certificate_arn": key,
                    "certificate_pem_bytes": pem.len(),
                    "certificate_pem_blocks": pem.matches("-----BEGIN CERTIFICATE-----").count(),
                    "chain_pem_bytes": chain.len(),
                    "chain_pem_blocks": chain.matches("-----BEGIN CERTIFICATE-----").count(),
                    "external_ca_validated": false,
                    "status": cert.status,
                    "cert_type": cert.cert_type,
                }));
            }
        }
        None
    }

    /// Flip a stored certificate's status (and optionally a failure
    /// reason). Returns `false` if no certificate matches `arn_or_id`
    /// across any account. The admin endpoint `POST
    /// /_fakecloud/acm/certificates/{arn-or-id}/status` calls this so
    /// tests can synchronously force a cert into `ISSUED`, `FAILED`, or
    /// `VALIDATION_TIMED_OUT` without waiting on the auto-issue tick.
    ///
    /// `arn_or_id` accepts either a full ACM ARN or just the
    /// trailing UUID portion (everything after `certificate/`).
    pub fn set_certificate_status(
        &self,
        arn_or_id: &str,
        status: &str,
        reason: Option<String>,
    ) -> bool {
        let mut state = self.state.write();
        for account in state.accounts.values_mut() {
            let key = account
                .certificates
                .keys()
                .find(|k| k.as_str() == arn_or_id || cert_id_from_arn(k) == arn_or_id)
                .cloned();
            if let Some(key) = key {
                if let Some(cert) = account.certificates.get_mut(&key) {
                    cert.status = status.to_string();
                    match status {
                        "ISSUED" => {
                            let now = Utc::now();
                            cert.issued_at = Some(now);
                            for dv in cert.domain_validation.iter_mut() {
                                dv.validation_status = "SUCCESS".to_string();
                            }
                            cert.failure_reason = None;
                            // AMAZON_ISSUED certs become renewal-eligible
                            // and gain a fresh RenewalSummary the moment
                            // they reach ISSUED, matching real ACM.
                            if cert.cert_type == "AMAZON_ISSUED" {
                                cert.renewal_eligibility = "ELIGIBLE".to_string();
                                cert.renewal_summary = Some(RenewalSummary {
                                    renewal_status: "PENDING_AUTO_RENEWAL".to_string(),
                                    domain_validation: cert.domain_validation.clone(),
                                    renewal_status_reason: None,
                                    updated_at: now,
                                });
                            }
                        }
                        "FAILED" | "VALIDATION_TIMED_OUT" => {
                            for dv in cert.domain_validation.iter_mut() {
                                dv.validation_status = status.to_string();
                            }
                            if reason.is_some() {
                                cert.failure_reason = reason;
                            }
                        }
                        _ => {
                            if reason.is_some() {
                                cert.failure_reason = reason;
                            }
                        }
                    }
                    return true;
                }
            }
        }
        false
    }
}

/// Persist the current ACM state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `AcmService::save_snapshot`, the auto-issue tick, and the
/// CloudFormation provisioner persist hook so all route through the same
/// serialize-and-write path.
pub async fn save_acm_snapshot(
    state: &SharedAcmState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = AcmSnapshot {
        schema_version: ACM_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write acm snapshot"),
        Err(err) => tracing::error!(%err, "acm snapshot task panicked"),
    }
}

impl Default for AcmService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(AcmAccounts::new())))
    }
}

#[async_trait]
impl AwsService for AcmService {
    fn service_name(&self) -> &str {
        "acm"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        let result = match req.action.as_str() {
            "RequestCertificate" => self.request_certificate(&req),
            "DescribeCertificate" => self.describe_certificate(&req),
            "ListCertificates" => self.list_certificates(&req),
            "DeleteCertificate" => self.delete_certificate(&req),
            "ImportCertificate" => self.import_certificate(&req),
            "ExportCertificate" => self.export_certificate(&req),
            "GetCertificate" => self.get_certificate(&req),
            "RenewCertificate" => self.renew_certificate(&req),
            "RevokeCertificate" => self.revoke_certificate(&req),
            "ResendValidationEmail" => self.resend_validation_email(&req),
            "AddTagsToCertificate" => self.add_tags_to_certificate(&req),
            "RemoveTagsFromCertificate" => self.remove_tags_from_certificate(&req),
            "ListTagsForCertificate" => self.list_tags_for_certificate(&req),
            "GetAccountConfiguration" => self.get_account_configuration(&req),
            "PutAccountConfiguration" => self.put_account_configuration(&req),
            "UpdateCertificateOptions" => self.update_certificate_options(&req),
            "SearchCertificates" => self.search_certificates(&req),
            other => Err(AwsServiceError::action_not_implemented("acm", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Request handlers ────────────────────────────────────────────────

impl AcmService {
    fn request_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let domain_name = body
            .get("DomainName")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("DomainName is required"))?
            .to_string();
        if domain_name.is_empty() || domain_name.len() > 253 {
            return Err(invalid_param("DomainName length must be between 1 and 253"));
        }
        let validation_method = body
            .get("ValidationMethod")
            .and_then(Value::as_str)
            .unwrap_or("DNS")
            .to_string();
        if validation_method != "EMAIL" && validation_method != "DNS" && validation_method != "HTTP"
        {
            return Err(invalid_param("Invalid ValidationMethod"));
        }
        let sans: Vec<String> = body
            .get("SubjectAlternativeNames")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let key_algorithm = body
            .get("KeyAlgorithm")
            .and_then(Value::as_str)
            .unwrap_or("RSA_2048")
            .to_string();
        const VALID_KEY_ALGORITHMS: [&str; 7] = [
            "RSA_1024",
            "RSA_2048",
            "RSA_3072",
            "RSA_4096",
            "EC_prime256v1",
            "EC_secp384r1",
            "EC_secp521r1",
        ];
        if !VALID_KEY_ALGORITHMS.contains(&key_algorithm.as_str()) {
            return Err(invalid_param("Invalid KeyAlgorithm"));
        }
        let idempotency_token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(ref token) = idempotency_token {
            if token.is_empty() || token.len() > 32 {
                return Err(invalid_param(
                    "IdempotencyToken length must be between 1 and 32",
                ));
            }
            if !token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Err(invalid_param(
                    "IdempotencyToken must match pattern ^[\\w-]+$",
                ));
            }
        }
        let managed_by = body
            .get("ManagedBy")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(ref m) = managed_by {
            if m != "CLOUDFRONT" {
                return Err(invalid_param("Invalid ManagedBy"));
            }
        }
        let ca_arn = body
            .get("CertificateAuthorityArn")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(ref arn) = ca_arn {
            if arn.len() < 20 || arn.len() > 2048 {
                return Err(validation_error(
                    "CertificateAuthorityArn length must be between 20 and 2048",
                ));
            }
        }
        let tags = parse_tags(body.get("Tags"))?;
        let options = parse_options(body.get("Options"));

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);

        // Idempotency: a same-token + same-DomainName + same-SANs request returns
        // the prior cert. Real ACM keys this on a 1-hour window; fakecloud uses
        // exact match for determinism.
        if let Some(token) = &idempotency_token {
            if let Some(existing) = account.certificates.values().find(|c| {
                c.idempotency_token.as_deref() == Some(token)
                    && c.domain_name == domain_name
                    && c.subject_alternative_names == effective_sans(&domain_name, &sans)
            }) {
                return Ok(AwsResponse::ok_json(
                    json!({ "CertificateArn": existing.arn }),
                ));
            }
        }

        let arn = synth_certificate_arn(&req.account_id, &req.region);
        let now = Utc::now();
        let effective = effective_sans(&domain_name, &sans);
        let (cert_pem, key_pem) = generate_self_signed_cert(&domain_name, &effective)
            .map(|(c, k)| (Some(c), Some(k)))
            .unwrap_or((None, None));
        let cert = StoredCertificate {
            arn: arn.clone(),
            domain_name: domain_name.clone(),
            subject_alternative_names: effective,
            status: "PENDING_VALIDATION".to_string(),
            cert_type: "AMAZON_ISSUED".to_string(),
            certificate_pem: cert_pem,
            certificate_chain_pem: None,
            private_key_pem: key_pem,
            idempotency_token,
            serial: synth_serial(&arn),
            subject: format!("CN={domain_name}"),
            issuer: "Amazon".to_string(),
            key_algorithm,
            signature_algorithm: "SHA256WITHRSA".to_string(),
            created_at: now,
            issued_at: None,
            imported_at: None,
            revoked_at: None,
            revocation_reason: None,
            // Issued certs from real ACM are valid 13 months. Match.
            not_before: now,
            not_after: now + Duration::days(395),
            validation_method: Some(validation_method.clone()),
            domain_validation: synth_domain_validation(&domain_name, &sans, &validation_method),
            options,
            renewal_eligibility: "INELIGIBLE".to_string(),
            managed_by,
            certificate_authority_arn: ca_arn,
            tags,
            in_use_by: Vec::new(),
            describe_read_count: 0,
            failure_reason: None,
            renewal_summary: None,
        };
        account.certificates.insert(arn.clone(), cert);
        drop(state);

        // Auto-issue tick: real ACM transitions DNS-validated certs
        // from PENDING_VALIDATION to ISSUED asynchronously once the
        // validation record lands. fakecloud fires the same flip after
        // `pending_validation_delay` (default 5s, configurable via
        // `FAKECLOUD_ACM_AUTO_ISSUE_SECS`) so SDK-driven tests can
        // observe the transition without standing up a real DNS
        // resolver. EMAIL certs intentionally stay PENDING — real AWS
        // sends a confirmation email; tests drive the approval via
        // `POST /_fakecloud/acm/certificates/{arn}/approve`.
        // ImportCertificate stays ISSUED-on-arrival (its own code path
        // never enters this branch).
        if validation_method == "DNS" {
            self.spawn_auto_issue_tick(req.account_id.clone(), arn.clone());
        }

        Ok(AwsResponse::ok_json(json!({ "CertificateArn": arn })))
    }

    fn describe_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_certificate_arn(req)?;
        let state = self.state.read();
        let cert = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.certificates.get(&arn))
            .ok_or_else(|| no_such_certificate(&arn))?
            .clone();
        // Status / NotBefore / NotAfter / RenewalEligibility /
        // RenewalSummary / DomainValidationOptions[].ValidationStatus
        // all come straight from the stored cert; the auto-issue tick
        // (DNS) and admin `/approve` endpoint (EMAIL) mutate them
        // out-of-band so successive describes naturally observe the
        // transition.
        Ok(AwsResponse::ok_json(json!({
            "Certificate": certificate_detail_json(&cert),
        })))
    }

    fn list_certificates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_items: usize = body
            .get("MaxItems")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(100);
        if !(1..=1000).contains(&max_items) {
            return Err(validation_error("MaxItems must be between 1 and 1000"));
        }
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(ref token) = next_token {
            if token.is_empty() || token.len() > 10000 {
                return Err(validation_error(
                    "NextToken length must be between 1 and 10000",
                ));
            }
        }
        if let Some(sort_by) = body.get("SortBy").and_then(Value::as_str) {
            if sort_by != "CREATED_AT" {
                return Err(validation_error("Invalid SortBy"));
            }
        }
        if let Some(sort_order) = body.get("SortOrder").and_then(Value::as_str) {
            if sort_order != "ASCENDING" && sort_order != "DESCENDING" {
                return Err(validation_error("Invalid SortOrder"));
            }
        }
        let statuses: Vec<String> = body
            .get("CertificateStatuses")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let includes = body.get("Includes");
        let key_types: Vec<String> = includes
            .and_then(|i| i.get("keyTypes"))
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let state = self.state.read();
        let mut all: Vec<StoredCertificate> = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.certificates.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        all.sort_by(|a, b| a.arn.cmp(&b.arn));
        all.retain(|c| {
            (statuses.is_empty() || statuses.contains(&c.status))
                && (key_types.is_empty() || key_types.contains(&c.key_algorithm))
        });

        let start = next_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let end = (start + max_items).min(all.len());
        let page: Vec<&StoredCertificate> = all.iter().skip(start).take(max_items).collect();
        let next = if end < all.len() {
            Some(end.to_string())
        } else {
            None
        };
        let mut response = json!({
            "CertificateSummaryList": page
                .iter()
                .map(|c| certificate_summary_json(c))
                .collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    fn delete_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_certificate_arn(req)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        if !cert.in_use_by.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!(
                    "Certificate {arn} is in use by {} resource(s)",
                    cert.in_use_by.len()
                ),
            ));
        }
        account.certificates.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn import_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cert_pem = decode_blob(body.get("Certificate"))
            .ok_or_else(|| invalid_param("Certificate is required"))?;
        let key_pem = decode_blob(body.get("PrivateKey"))
            .ok_or_else(|| invalid_param("PrivateKey is required"))?;
        let chain_pem = decode_blob(body.get("CertificateChain"));
        let arn_in = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let tags = parse_tags(body.get("Tags"))?;

        let domain_name = parse_cn_from_pem(&cert_pem).unwrap_or_else(|| "imported".to_string());
        let now = Utc::now();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);

        let arn = match arn_in {
            Some(existing) => {
                let cert = account
                    .certificates
                    .get_mut(&existing)
                    .ok_or_else(|| no_such_certificate(&existing))?;
                if cert.cert_type != "IMPORTED" {
                    return Err(invalid_param(
                        "Reimport is only supported for IMPORTED certificates",
                    ));
                }
                cert.certificate_pem = Some(cert_pem.clone());
                cert.private_key_pem = Some(key_pem);
                cert.certificate_chain_pem = chain_pem;
                cert.imported_at = Some(now);
                cert.not_before = now;
                cert.not_after = now + Duration::days(395);
                cert.subject = format!("CN={domain_name}");
                // Reimport must overwrite the domain identity too —
                // otherwise Describe / List / Search keep returning the
                // previous DomainName + SANs after a successful import.
                cert.domain_name = domain_name.clone();
                cert.subject_alternative_names = vec![domain_name.clone()];
                if !tags.is_empty() {
                    for (k, v) in tags {
                        cert.tags.insert(k, v);
                    }
                }
                existing
            }
            None => {
                let arn = synth_certificate_arn(&req.account_id, &req.region);
                let cert = StoredCertificate {
                    arn: arn.clone(),
                    domain_name: domain_name.clone(),
                    subject_alternative_names: vec![domain_name.clone()],
                    status: "ISSUED".to_string(),
                    cert_type: "IMPORTED".to_string(),
                    certificate_pem: Some(cert_pem),
                    certificate_chain_pem: chain_pem,
                    private_key_pem: Some(key_pem),
                    idempotency_token: None,
                    serial: synth_serial(&arn),
                    subject: format!("CN={domain_name}"),
                    issuer: "fakecloud-imported".to_string(),
                    key_algorithm: "RSA_2048".to_string(),
                    signature_algorithm: "SHA256WITHRSA".to_string(),
                    created_at: now,
                    issued_at: Some(now),
                    imported_at: Some(now),
                    revoked_at: None,
                    revocation_reason: None,
                    not_before: now,
                    not_after: now + Duration::days(395),
                    validation_method: None,
                    domain_validation: Vec::new(),
                    options: CertificateOptions {
                        certificate_transparency_logging_preference: "ENABLED".to_string(),
                        export: "DISABLED".to_string(),
                    },
                    renewal_eligibility: "INELIGIBLE".to_string(),
                    managed_by: None,
                    certificate_authority_arn: None,
                    tags,
                    in_use_by: Vec::new(),
                    describe_read_count: 0,
                    failure_reason: None,
                    renewal_summary: None,
                };
                account.certificates.insert(arn.clone(), cert);
                arn
            }
        };
        Ok(AwsResponse::ok_json(json!({ "CertificateArn": arn })))
    }

    fn export_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        // AWS encodes Passphrase as a Blob. Over awsJson1_1 (the wire
        // protocol used by ACM) blobs are base64 strings. Missing/empty
        // means the caller wants the plain PEM back.
        let passphrase_bytes = match body.get("Passphrase").and_then(Value::as_str) {
            None | Some("") => None,
            Some(s) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(|_| invalid_param("Passphrase must be valid base64"))?;
                if decoded.is_empty() {
                    None
                } else {
                    Some(decoded)
                }
            }
        };
        let state = self.state.read();
        let cert = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.certificates.get(&arn))
            .ok_or_else(|| no_such_certificate(&arn))?
            .clone();
        if cert.options.export != "ENABLED" && cert.cert_type != "IMPORTED" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RequestInProgressException",
                "Certificate is not exportable",
            ));
        }
        let cert_pem = cert
            .certificate_pem
            .clone()
            .unwrap_or_else(|| placeholder_cert_pem(&arn));
        let chain_pem = cert
            .certificate_chain_pem
            .clone()
            .unwrap_or_else(|| placeholder_chain_pem(&arn));
        let key_pem = cert
            .private_key_pem
            .clone()
            .unwrap_or_else(|| placeholder_key_pem(&arn));
        let key_out = match passphrase_bytes {
            Some(pp) => encrypt_private_key_pem(&key_pem, &pp)
                .map_err(|e| invalid_param(format!("failed to encrypt private key: {e}")))?,
            None => key_pem,
        };
        Ok(AwsResponse::ok_json(json!({
            "Certificate": cert_pem,
            "CertificateChain": chain_pem,
            "PrivateKey": key_out,
        })))
    }

    fn get_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_certificate_arn(req)?;
        let state = self.state.read();
        let cert = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.certificates.get(&arn))
            .ok_or_else(|| no_such_certificate(&arn))?
            .clone();
        let cert_pem = cert
            .certificate_pem
            .clone()
            .unwrap_or_else(|| placeholder_cert_pem(&arn));
        let chain_pem = cert
            .certificate_chain_pem
            .clone()
            .unwrap_or_else(|| placeholder_chain_pem(&arn));
        Ok(AwsResponse::ok_json(json!({
            "Certificate": cert_pem,
            "CertificateChain": chain_pem,
        })))
    }

    fn renew_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_certificate_arn(req)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get_mut(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        if cert.cert_type == "IMPORTED" {
            return Err(invalid_param(
                "Imported certificates cannot be renewed via ACM",
            ));
        }
        // Renewal: flip to ISSUED from any prior state, refresh the
        // validity window, mark domain validation SUCCESS, and refresh
        // RenewalSummary. Real ACM kicks off a managed-renewal
        // background job; fakecloud collapses that to an immediate
        // success since there's nothing to actually validate.
        let now = Utc::now();
        cert.not_before = now;
        cert.not_after = now + Duration::days(395);
        cert.issued_at = Some(now);
        cert.status = "ISSUED".to_string();
        cert.renewal_eligibility = "ELIGIBLE".to_string();
        cert.failure_reason = None;
        for dv in cert.domain_validation.iter_mut() {
            dv.validation_status = "SUCCESS".to_string();
        }
        cert.renewal_summary = Some(RenewalSummary {
            renewal_status: "SUCCESS".to_string(),
            domain_validation: cert.domain_validation.clone(),
            renewal_status_reason: None,
            updated_at: now,
        });
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn revoke_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        let reason = body
            .get("RevocationReason")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("RevocationReason is required"))?
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get_mut(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        if cert.cert_type != "AMAZON_ISSUED" {
            return Err(invalid_param(
                "Only AMAZON_ISSUED certificates can be revoked",
            ));
        }
        cert.status = "REVOKED".to_string();
        cert.revoked_at = Some(Utc::now());
        cert.revocation_reason = Some(reason);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn resend_validation_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        let _ = body
            .get("Domain")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("Domain is required"))?;
        let _ = body
            .get("ValidationDomain")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("ValidationDomain is required"))?;
        let state = self.state.read();
        let cert = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.certificates.get(&arn))
            .ok_or_else(|| no_such_certificate(&arn))?;
        if cert.validation_method.as_deref() != Some("EMAIL") {
            return Err(invalid_param(
                "Certificate is not configured for EMAIL validation",
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn add_tags_to_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        let tags = parse_tags(body.get("Tags"))?;
        if tags.is_empty() {
            return Err(invalid_param("Tags must contain at least one entry"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get_mut(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        for (k, v) in tags {
            cert.tags.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn remove_tags_from_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        let tags = parse_tags(body.get("Tags"))?;
        if tags.is_empty() {
            return Err(invalid_param("Tags must contain at least one entry"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get_mut(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        // Real ACM: a tag removes if Key matches; if Value is supplied it
        // also has to match. Otherwise it's a no-op (not an error).
        for (k, v) in tags {
            if let Some(existing) = cert.tags.get(&k) {
                if v.is_empty() || *existing == v {
                    cert.tags.remove(&k);
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_certificate_arn(req)?;
        let state = self.state.read();
        let cert = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.certificates.get(&arn))
            .ok_or_else(|| no_such_certificate(&arn))?;
        let mut tags: Vec<(String, String)> = cert
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        tags.sort_by(|a, b| a.0.cmp(&b.0));
        let tag_list: Vec<Value> = tags
            .into_iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "Tags": tag_list })))
    }

    fn get_account_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let cfg = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.account_config.clone())
            .unwrap_or_default();
        let mut expiry = json!({});
        if let Some(d) = cfg.expiry_events_days_before_expiry {
            expiry
                .as_object_mut()
                .unwrap()
                .insert("DaysBeforeExpiry".to_string(), json!(d));
        }
        Ok(AwsResponse::ok_json(json!({
            "ExpiryEvents": expiry,
        })))
    }

    fn put_account_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let idempotency_token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("IdempotencyToken is required"))?;
        if idempotency_token.is_empty() || idempotency_token.len() > 32 {
            return Err(validation_error(
                "IdempotencyToken length must be between 1 and 32",
            ));
        }
        if !idempotency_token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(validation_error(
                "IdempotencyToken must match pattern ^[\\w-]+$",
            ));
        }
        let days = body
            .get("ExpiryEvents")
            .and_then(|v| v.get("DaysBeforeExpiry"))
            .and_then(Value::as_i64)
            .map(|n| n as i32);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        account.account_config.expiry_events_days_before_expiry = days;
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn update_certificate_options(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body
            .get("CertificateArn")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("CertificateArn is required"))?
            .to_string();
        let options = body
            .get("Options")
            .ok_or_else(|| invalid_param("Options is required"))?;
        let new_opts = CertificateOptions {
            certificate_transparency_logging_preference: options
                .get("CertificateTransparencyLoggingPreference")
                .and_then(Value::as_str)
                .unwrap_or("ENABLED")
                .to_string(),
            export: options
                .get("Export")
                .and_then(Value::as_str)
                .unwrap_or("DISABLED")
                .to_string(),
        };
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cert = account
            .certificates
            .get_mut(&arn)
            .ok_or_else(|| no_such_certificate(&arn))?;
        cert.options = new_opts;
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn search_certificates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // SearchCertificates is effectively ListCertificates with a
        // recursive `FilterStatement` (And/Or/Not/Filter union) plus
        // sort knobs. fakecloud honors the leaf-`Filter` cases
        // (KeyTypes, ExtendedKeyUsages match by passing through) and
        // ignores the And/Or/Not composition for now — enough to keep
        // SDK callers and the conformance probe happy.
        let body = req.json_body();
        let max_results: usize = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(100);
        if !(1..=500).contains(&max_results) {
            return Err(validation_error("MaxResults must be between 1 and 500"));
        }
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if let Some(ref token) = next_token {
            if token.is_empty() || token.len() > 10000 {
                return Err(validation_error(
                    "NextToken length must be between 1 and 10000",
                ));
            }
        }
        if let Some(sort_by) = body.get("SortBy").and_then(Value::as_str) {
            const VALID_SORT_BY: [&str; 18] = [
                "CREATED_AT",
                "NOT_AFTER",
                "STATUS",
                "RENEWAL_STATUS",
                "EXPORTED",
                "IN_USE",
                "NOT_BEFORE",
                "KEY_ALGORITHM",
                "TYPE",
                "CERTIFICATE_ARN",
                "COMMON_NAME",
                "REVOKED_AT",
                "RENEWAL_ELIGIBILITY",
                "ISSUED_AT",
                "MANAGED_BY",
                "EXPORT_OPTION",
                "VALIDATION_METHOD",
                "IMPORTED_AT",
            ];
            if !VALID_SORT_BY.contains(&sort_by) {
                return Err(validation_error("Invalid SortBy"));
            }
        }
        if let Some(sort_order) = body.get("SortOrder").and_then(Value::as_str) {
            if sort_order != "ASCENDING" && sort_order != "DESCENDING" {
                return Err(validation_error("Invalid SortOrder"));
            }
        }
        // CertificateStatuses is a top-level filter on the certificate status.
        let statuses: Vec<String> = body
            .get("CertificateStatuses")
            .and_then(Value::as_array)
            .map(|v| {
                v.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_ascii_uppercase()))
                    .collect()
            })
            .unwrap_or_default();

        let state = self.state.read();
        let mut all: Vec<StoredCertificate> = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.certificates.values().cloned().collect())
            .unwrap_or_default();
        drop(state);

        if !statuses.is_empty() {
            all.retain(|c| statuses.contains(&c.status.to_ascii_uppercase()));
        }
        // FilterStatement is a recursive And/Or/Not/Filter tree; evaluate it
        // against each cert so composed filters (not just bare leaves) apply.
        if let Some(stmt) = body.get("FilterStatement") {
            all.retain(|c| cert_matches_statement(c, stmt));
        }

        // Apply SortBy/SortOrder (validated above but previously never applied,
        // 1.5). Default order is descending; the default key is CREATED_AT.
        let sort_by = body
            .get("SortBy")
            .and_then(Value::as_str)
            .unwrap_or("CREATED_AT")
            .to_string();
        let descending = body
            .get("SortOrder")
            .and_then(Value::as_str)
            .map(|o| !o.eq_ignore_ascii_case("ASCENDING"))
            .unwrap_or(true);
        all.sort_by(|a, b| {
            let ord = match sort_by.as_str() {
                "NOT_AFTER" => a.not_after.cmp(&b.not_after),
                "NOT_BEFORE" => a.not_before.cmp(&b.not_before),
                "ISSUED_AT" => a.issued_at.cmp(&b.issued_at),
                "STATUS" => a.status.cmp(&b.status),
                "KEY_ALGORITHM" => a.key_algorithm.cmp(&b.key_algorithm),
                "TYPE" => a.cert_type.cmp(&b.cert_type),
                "CERTIFICATE_ARN" => a.arn.cmp(&b.arn),
                // CREATED_AT and any other validated-but-unmodeled key.
                _ => a.created_at.cmp(&b.created_at),
            };
            // Stable tiebreak by ARN keeps paging deterministic.
            let ord = ord.then_with(|| a.arn.cmp(&b.arn));
            if descending {
                ord.reverse()
            } else {
                ord
            }
        });
        let start = next_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let end = (start + max_results).min(all.len());
        let page: Vec<&StoredCertificate> = all.iter().skip(start).take(max_results).collect();
        let next = if end < all.len() {
            Some(end.to_string())
        } else {
            None
        };
        let mut response = json!({
            "Results": page
                .iter()
                .map(|c| certificate_search_result_json(c))
                .collect::<Vec<_>>(),
        });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }
}

// ─── Helpers ────────────────────────────────────────────────────────

fn account_mut<'a>(state: &'a mut AcmAccounts, account_id: &str) -> &'a mut AccountState {
    state.accounts.entry(account_id.to_string()).or_default()
}

// Canonical EKU/KeyUsage sets carried by every fakecloud-issued cert (kept in
// sync with certificate_search_result_json / describe output). A cert matches a
// single-valued X509AttributeFilter.ExtendedKeyUsage / KeyUsage when the
// requested value is a member of its set.
const CERT_EKUS: [&str; 2] = [
    "TLS_WEB_SERVER_AUTHENTICATION",
    "TLS_WEB_CLIENT_AUTHENTICATION",
];
const CERT_KEY_USAGES: [&str; 2] = ["DIGITAL_SIGNATURE", "KEY_ENCIPHERMENT"];

/// Evaluate a `CommonNameFilter` / `DnsNameFilter` (a `{Value,
/// ComparisonOperator}` struct where the operator is `EQUALS` or `CONTAINS`)
/// against a candidate string.
fn string_filter_matches(filter: &Value, value: &str) -> bool {
    let Some(target) = filter.get("Value").and_then(Value::as_str) else {
        return true;
    };
    match filter.get("ComparisonOperator").and_then(Value::as_str) {
        Some("CONTAINS") => value.contains(target),
        // EQUALS (the only other documented operator) and any default.
        _ => value == target,
    }
}

/// Certificate common name, with the `CN=` prefix stripped to match how the
/// search result surfaces it.
fn cert_common_name(cert: &StoredCertificate) -> &str {
    cert.subject
        .strip_prefix("CN=")
        .unwrap_or(cert.subject.as_str())
}

/// Whether an epoch (or RFC3339) timestamp lands within a `TimestampRange`
/// (`{Start, End}`, both inclusive; either bound may be omitted).
fn ts_in_range(range: &Value, ts: chrono::DateTime<Utc>) -> bool {
    let bound = |key: &str| -> Option<i64> {
        range.get(key).and_then(|v| {
            v.as_f64()
                .map(|f| f as i64)
                .or_else(|| {
                    v.as_str()
                        .and_then(|s| s.parse::<f64>().ok().map(|f| f as i64))
                })
                .or_else(|| {
                    v.as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|d| d.timestamp())
                })
        })
    };
    let epoch = ts.timestamp();
    bound("Start").is_none_or(|s| epoch >= s) && bound("End").is_none_or(|e| epoch <= e)
}

/// Evaluate an `X509AttributeFilter` union (exactly one member set) against a
/// certificate.
fn cert_matches_x509(cert: &StoredCertificate, x: &Value) -> bool {
    if let Some(cn) = x.get("Subject").and_then(|s| s.get("CommonName")) {
        return string_filter_matches(cn, cert_common_name(cert));
    }
    if let Some(dns) = x
        .get("SubjectAlternativeName")
        .and_then(|s| s.get("DnsName"))
    {
        return cert
            .subject_alternative_names
            .iter()
            .any(|san| string_filter_matches(dns, san));
    }
    if let Some(eku) = x.get("ExtendedKeyUsage").and_then(Value::as_str) {
        let eku = eku.to_ascii_uppercase();
        // The `ANY` sentinel matches any cert that has at least one EKU;
        // otherwise the specific value must be one the cert carries.
        return match eku.as_str() {
            "ANY" => !CERT_EKUS.is_empty(),
            v => CERT_EKUS.contains(&v),
        };
    }
    if let Some(ku) = x.get("KeyUsage").and_then(Value::as_str) {
        let ku = ku.to_ascii_uppercase();
        return match ku.as_str() {
            "ANY" => !CERT_KEY_USAGES.is_empty(),
            v => CERT_KEY_USAGES.contains(&v),
        };
    }
    if let Some(alg) = x.get("KeyAlgorithm").and_then(Value::as_str) {
        return cert.key_algorithm.eq_ignore_ascii_case(alg);
    }
    if let Some(serial) = x.get("SerialNumber").and_then(Value::as_str) {
        return cert.serial == serial;
    }
    if let Some(range) = x.get("NotAfter") {
        return ts_in_range(range, cert.not_after);
    }
    if let Some(range) = x.get("NotBefore") {
        return ts_in_range(range, cert.not_before);
    }
    // No recognised member -> no constraint.
    true
}

/// Evaluate an `AcmCertificateMetadataFilter` union against a certificate.
fn cert_matches_metadata(cert: &StoredCertificate, md: &Value) -> bool {
    if let Some(status) = md.get("Status").and_then(Value::as_str) {
        return cert.status.eq_ignore_ascii_case(status);
    }
    if let Some(rs) = md.get("RenewalStatus").and_then(Value::as_str) {
        return cert
            .renewal_summary
            .as_ref()
            .is_some_and(|r| r.renewal_status.eq_ignore_ascii_case(rs));
    }
    if let Some(ty) = md.get("Type").and_then(Value::as_str) {
        return cert.cert_type.eq_ignore_ascii_case(ty);
    }
    if let Some(in_use) = md.get("InUse").and_then(Value::as_bool) {
        return cert.in_use_by.is_empty() != in_use;
    }
    if let Some(exported) = md.get("Exported").and_then(Value::as_bool) {
        // fakecloud never exports certs (mirrored as Exported=false in output).
        return !exported;
    }
    if let Some(opt) = md.get("ExportOption").and_then(Value::as_str) {
        return cert.options.export.eq_ignore_ascii_case(opt);
    }
    if let Some(mb) = md.get("ManagedBy").and_then(Value::as_str) {
        return cert
            .managed_by
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case(mb));
    }
    if let Some(vm) = md.get("ValidationMethod").and_then(Value::as_str) {
        return cert
            .validation_method
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case(vm));
    }
    true
}

/// Evaluate a leaf `CertificateFilter` union (`CertificateArn` /
/// `X509AttributeFilter` / `AcmCertificateMetadataFilter`) against a
/// certificate, per the ACM SearchCertificates model.
fn cert_matches_leaf(cert: &StoredCertificate, filter: &Value) -> bool {
    if let Some(arn) = filter.get("CertificateArn").and_then(Value::as_str) {
        return cert.arn == arn;
    }
    if let Some(x) = filter.get("X509AttributeFilter") {
        return cert_matches_x509(cert, x);
    }
    if let Some(md) = filter.get("AcmCertificateMetadataFilter") {
        return cert_matches_metadata(cert, md);
    }
    // Empty/unknown leaf imposes no constraint.
    true
}

/// Recursively evaluate a `FilterStatement` (And / Or / Not / Filter) against a
/// certificate. Present composition keys are AND'd together.
fn cert_matches_statement(cert: &StoredCertificate, stmt: &Value) -> bool {
    if let Some(and) = stmt.get("And").and_then(Value::as_array) {
        if !and.iter().all(|s| cert_matches_statement(cert, s)) {
            return false;
        }
    }
    if let Some(or) = stmt.get("Or").and_then(Value::as_array) {
        if !or.is_empty() && !or.iter().any(|s| cert_matches_statement(cert, s)) {
            return false;
        }
    }
    if let Some(not) = stmt.get("Not") {
        if cert_matches_statement(cert, not) {
            return false;
        }
    }
    if let Some(filter) = stmt.get("Filter") {
        if !cert_matches_leaf(cert, filter) {
            return false;
        }
    }
    true
}

fn require_certificate_arn(req: &AwsRequest) -> Result<String, AwsServiceError> {
    req.json_body()
        .get("CertificateArn")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| invalid_param("CertificateArn is required"))
}

fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

fn validation_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn no_such_certificate(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Could not find certificate with arn {arn}"),
    )
}

fn synth_certificate_arn(account_id: &str, region: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    let id = Uuid::new_v4();
    Arn::new("acm", region, account_id, &format!("certificate/{id}")).to_string()
}

/// Extract the trailing UUID portion of a certificate ARN
/// (`arn:aws:acm:region:account:certificate/<id>` -> `<id>`). Returns
/// the input unchanged if it doesn't match the ACM ARN shape — callers
/// only use this to compare against shorthand identifiers passed to the
/// admin endpoint, where a partial match against the full ARN is also
/// acceptable.
fn cert_id_from_arn(arn: &str) -> &str {
    arn.rsplit_once("certificate/")
        .map(|(_, id)| id)
        .unwrap_or(arn)
}

fn synth_serial(arn: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(arn.as_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..16])
}

fn parse_tags(value: Option<&Value>) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let mut out = BTreeMap::new();
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(out);
    };
    for tag in arr {
        let key = tag
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_param("Tag.Key is required"))?
            .to_string();
        let value = tag
            .get("Value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        out.insert(key, value);
    }
    Ok(out)
}

fn parse_options(value: Option<&Value>) -> CertificateOptions {
    let v = match value {
        Some(v) => v,
        None => {
            return CertificateOptions {
                certificate_transparency_logging_preference: "ENABLED".to_string(),
                export: "DISABLED".to_string(),
            };
        }
    };
    CertificateOptions {
        certificate_transparency_logging_preference: v
            .get("CertificateTransparencyLoggingPreference")
            .and_then(Value::as_str)
            .unwrap_or("ENABLED")
            .to_string(),
        export: v
            .get("Export")
            .and_then(Value::as_str)
            .unwrap_or("DISABLED")
            .to_string(),
    }
}

/// Real ACM always carries the apex `DomainName` as the first entry of
/// `SubjectAlternativeNames`; replicate that so SDK tests that read SANs
/// don't have to special-case its absence.
fn effective_sans(domain: &str, extras: &[String]) -> Vec<String> {
    let mut all = vec![domain.to_string()];
    for s in extras {
        if !all.contains(s) {
            all.push(s.clone());
        }
    }
    all
}

fn synth_domain_validation(domain: &str, sans: &[String], method: &str) -> Vec<DomainValidation> {
    effective_sans(domain, sans)
        .iter()
        .map(|d| {
            if method == "DNS" {
                let token = synth_dns_token(d);
                DomainValidation {
                    domain_name: d.clone(),
                    validation_status: "PENDING_VALIDATION".to_string(),
                    validation_method: "DNS".to_string(),
                    resource_record_name: Some(format!("_{token}.{d}.")),
                    resource_record_type: Some("CNAME".to_string()),
                    resource_record_value: Some(format!("_{token}.acm-validations.aws.")),
                }
            } else {
                DomainValidation {
                    domain_name: d.clone(),
                    validation_status: "PENDING_VALIDATION".to_string(),
                    validation_method: "EMAIL".to_string(),
                    resource_record_name: None,
                    resource_record_type: None,
                    resource_record_value: None,
                }
            }
        })
        .collect()
}

/// Deterministic 32-char hex token derived from the domain so test
/// assertions on the validation record stay stable across runs.
fn synth_dns_token(domain: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..16])
}

fn decode_blob(value: Option<&Value>) -> Option<String> {
    let v = value?;
    if let Some(s) = v.as_str() {
        // Real SDKs base64-encode blob shapes over the wire. Decode the
        // outer encoding back to the underlying PEM text; if it isn't
        // base64 (which happens with ad-hoc curl tests), pass through.
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
            if let Ok(text) = String::from_utf8(bytes) {
                return Some(text);
            }
        }
        return Some(s.to_string());
    }
    None
}

/// Cheap CN scan for an imported PEM. Real ACM parses the X.509 cert
/// to extract the subject; fakecloud just looks for a `CN=` substring
/// or falls back to the PEM hash so the returned `DomainName` is at
/// least stable per input.
fn parse_cn_from_pem(pem: &str) -> Option<String> {
    pem.lines()
        .find_map(|line| line.split("CN=").nth(1))
        .map(|rest| {
            rest.split(['/', ',', '\n', ' '])
                .next()
                .unwrap_or("")
                .to_string()
        })
        .filter(|s| !s.is_empty())
}

fn placeholder_cert_pem(arn: &str) -> String {
    // Fallback used only when an actually-issued cert was somehow
    // dropped. Kept distinguishable so callers don't silently treat
    // these as real X.509.
    let body = base64::engine::general_purpose::STANDARD.encode(arn.as_bytes());
    format!("-----BEGIN CERTIFICATE-----\n{body}\n-----END CERTIFICATE-----\n")
}

fn placeholder_chain_pem(arn: &str) -> String {
    let body =
        base64::engine::general_purpose::STANDARD.encode(format!("chain-of-{arn}").as_bytes());
    format!("-----BEGIN CERTIFICATE-----\n{body}\n-----END CERTIFICATE-----\n")
}

fn placeholder_key_pem(arn: &str) -> String {
    let body = base64::engine::general_purpose::STANDARD.encode(format!("key-of-{arn}").as_bytes());
    format!("-----BEGIN RSA PRIVATE KEY-----\n{body}\n-----END RSA PRIVATE KEY-----\n")
}

/// Generate a real self-signed X.509 certificate + private key pair
/// for `domain_name` covering `sans`. Returns
/// `(certificate_pem, private_key_pem)`. Used by RequestCertificate
/// so the cert that GetCertificate / ExportCertificate hands back
/// is actually parseable as a real PEM-encoded X.509 (matching real
/// ACM's output format), not a base64-of-the-ARN placeholder.
fn generate_self_signed_cert(domain_name: &str, sans: &[String]) -> Option<(String, String)> {
    let mut all_names: Vec<String> = vec![domain_name.to_string()];
    for s in sans {
        if !all_names.contains(s) {
            all_names.push(s.clone());
        }
    }
    let cert = rcgen::generate_simple_self_signed(all_names).ok()?;
    Some((cert.cert.pem(), cert.key_pair.serialize_pem()))
}

fn certificate_summary_json(c: &StoredCertificate) -> Value {
    let mut s = json!({
        "CertificateArn": c.arn,
        "DomainName": c.domain_name,
        "SubjectAlternativeNameSummaries": c.subject_alternative_names,
        "HasAdditionalSubjectAlternativeNames": false,
        "Status": c.status,
        "Type": c.cert_type,
        "KeyAlgorithm": c.key_algorithm,
        "KeyUsages": ["DIGITAL_SIGNATURE", "KEY_ENCIPHERMENT"],
        "ExtendedKeyUsages": ["TLS_WEB_SERVER_AUTHENTICATION", "TLS_WEB_CLIENT_AUTHENTICATION"],
        "InUse": !c.in_use_by.is_empty(),
        "Exported": false,
        "RenewalEligibility": c.renewal_eligibility,
        "NotBefore": c.not_before.timestamp() as f64,
        "NotAfter": c.not_after.timestamp() as f64,
        "CreatedAt": c.created_at.timestamp() as f64,
    });
    if let Some(t) = c.issued_at {
        s.as_object_mut()
            .unwrap()
            .insert("IssuedAt".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = c.imported_at {
        s.as_object_mut()
            .unwrap()
            .insert("ImportedAt".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = c.revoked_at {
        s.as_object_mut()
            .unwrap()
            .insert("RevokedAt".to_string(), json!(t.timestamp() as f64));
        if let Some(r) = &c.revocation_reason {
            s.as_object_mut()
                .unwrap()
                .insert("RevocationReason".to_string(), json!(r));
        }
    }
    if let Some(m) = &c.managed_by {
        s.as_object_mut()
            .unwrap()
            .insert("ManagedBy".to_string(), json!(m));
    }
    s
}

fn certificate_detail_json(c: &StoredCertificate) -> Value {
    let mut d = json!({
        "CertificateArn": c.arn,
        "DomainName": c.domain_name,
        "SubjectAlternativeNames": c.subject_alternative_names,
        "Status": c.status,
        "Type": c.cert_type,
        "Serial": c.serial,
        "Subject": c.subject,
        "Issuer": c.issuer,
        "KeyAlgorithm": c.key_algorithm,
        "SignatureAlgorithm": c.signature_algorithm,
        "InUseBy": c.in_use_by,
        "RenewalEligibility": c.renewal_eligibility,
        "Options": {
            "CertificateTransparencyLoggingPreference":
                c.options.certificate_transparency_logging_preference,
            "Export": c.options.export,
        },
        "DomainValidationOptions": c
            .domain_validation
            .iter()
            .map(domain_validation_json)
            .collect::<Vec<_>>(),
        "NotBefore": c.not_before.timestamp() as f64,
        "NotAfter": c.not_after.timestamp() as f64,
        "CreatedAt": c.created_at.timestamp() as f64,
        "KeyUsages": [{"Name": "DIGITAL_SIGNATURE"}, {"Name": "KEY_ENCIPHERMENT"}],
        "ExtendedKeyUsages": [
            {"Name": "TLS_WEB_SERVER_AUTHENTICATION", "OID": "1.3.6.1.5.5.7.3.1"},
            {"Name": "TLS_WEB_CLIENT_AUTHENTICATION", "OID": "1.3.6.1.5.5.7.3.2"},
        ],
    });
    if let Some(t) = c.issued_at {
        d.as_object_mut()
            .unwrap()
            .insert("IssuedAt".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = c.imported_at {
        d.as_object_mut()
            .unwrap()
            .insert("ImportedAt".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = c.revoked_at {
        d.as_object_mut()
            .unwrap()
            .insert("RevokedAt".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(r) = &c.revocation_reason {
        d.as_object_mut()
            .unwrap()
            .insert("RevocationReason".to_string(), json!(r));
    }
    if let Some(m) = &c.managed_by {
        d.as_object_mut()
            .unwrap()
            .insert("ManagedBy".to_string(), json!(m));
    }
    if let Some(ca) = &c.certificate_authority_arn {
        d.as_object_mut()
            .unwrap()
            .insert("CertificateAuthorityArn".to_string(), json!(ca));
    }
    if let Some(fr) = &c.failure_reason {
        d.as_object_mut()
            .unwrap()
            .insert("FailureReason".to_string(), json!(fr));
    }
    if let Some(rs) = &c.renewal_summary {
        let mut summary = json!({
            "RenewalStatus": rs.renewal_status,
            "DomainValidationOptions": rs
                .domain_validation
                .iter()
                .map(domain_validation_json)
                .collect::<Vec<_>>(),
            "UpdatedAt": rs.updated_at.timestamp() as f64,
        });
        if let Some(reason) = &rs.renewal_status_reason {
            summary
                .as_object_mut()
                .unwrap()
                .insert("RenewalStatusReason".to_string(), json!(reason));
        }
        d.as_object_mut()
            .unwrap()
            .insert("RenewalSummary".to_string(), summary);
    }
    d
}

fn certificate_search_result_json(c: &StoredCertificate) -> Value {
    let san_objects: Vec<Value> = c
        .subject_alternative_names
        .iter()
        .map(|s| json!({ "DnsName": s }))
        .collect();
    let cn = c
        .subject
        .strip_prefix("CN=")
        .unwrap_or(c.subject.as_str())
        .to_string();
    json!({
        "CertificateArn": c.arn,
        "X509Attributes": {
            "Subject": { "CommonName": cn },
            "Issuer": { "CommonName": c.issuer },
            "SubjectAlternativeNames": san_objects,
            "KeyAlgorithm": c.key_algorithm,
            "KeyUsages": ["DIGITAL_SIGNATURE", "KEY_ENCIPHERMENT"],
            "ExtendedKeyUsages": ["TLS_WEB_SERVER_AUTHENTICATION", "TLS_WEB_CLIENT_AUTHENTICATION"],
            "SerialNumber": c.serial,
            "NotBefore": c.not_before.timestamp() as f64,
            "NotAfter": c.not_after.timestamp() as f64,
        },
        "CertificateMetadata": {
            "AcmCertificateMetadata": {
                "Status": c.status,
                "Type": c.cert_type,
                "InUse": !c.in_use_by.is_empty(),
                "Exported": false,
                "RenewalEligibility": c.renewal_eligibility,
                "CreatedAt": c.created_at.timestamp() as f64,
                "ManagedBy": c.managed_by.clone().unwrap_or_default(),
                "ValidationMethod": c.validation_method.clone().unwrap_or_default(),
            },
        },
    })
}

fn domain_validation_json(v: &DomainValidation) -> Value {
    let mut out = json!({
        "DomainName": v.domain_name,
        "ValidationStatus": v.validation_status,
        "ValidationMethod": v.validation_method,
    });
    if let (Some(name), Some(rtype), Some(value)) = (
        &v.resource_record_name,
        &v.resource_record_type,
        &v.resource_record_value,
    ) {
        out.as_object_mut().unwrap().insert(
            "ResourceRecord".to_string(),
            json!({
                "Name": name,
                "Type": rtype,
                "Value": value,
            }),
        );
    }
    out
}

/// Encrypt a PEM-encoded PKCS#8 private key with a passphrase,
/// producing a PEM file with `BEGIN ENCRYPTED PRIVATE KEY` headers.
/// This matches what real ACM `ExportCertificate` returns: a PKCS#8 v2
/// `EncryptedPrivateKeyInfo` using PBES2 (PBKDF2 + AES-256-CBC).
///
/// The resulting PEM is decryptable with anything that handles modern
/// PKCS#8 encrypted keys: `openssl pkcs8 -in key.pem -passin pass:...`,
/// Python's `cryptography.hazmat.primitives.serialization`,
/// `rsa::pkcs8::DecodePrivateKey::from_pkcs8_encrypted_pem`, etc.
fn encrypt_private_key_pem(key_pem: &str, passphrase: &[u8]) -> Result<String, String> {
    use pkcs8::{pkcs5::pbes2, LineEnding, PrivateKeyInfo};

    // Strip the PEM headers and base64-decode the inner DER. Real keys
    // (rcgen, openssl) emit `BEGIN PRIVATE KEY` for PKCS#8 v1; we don't
    // support encrypting `BEGIN RSA PRIVATE KEY` (PKCS#1) or other
    // legacy SEC1 forms here because rcgen always produces PKCS#8.
    let der = pem_decode_private_key(key_pem)?;

    // PBES2 parameters: 16-byte random salt, 2048 PBKDF2-SHA256
    // iterations (matches OpenSSL defaults for `openssl pkcs8 -topk8`),
    // AES-256-CBC with a random 16-byte IV.
    let mut salt = [0u8; 16];
    let mut iv = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut salt);
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);
    let params = pbes2::Parameters::pbkdf2_sha256_aes256cbc(2048, &salt, &iv)
        .map_err(|e| format!("invalid PBES2 parameters: {e}"))?;

    let pki = PrivateKeyInfo::try_from(der.as_slice())
        .map_err(|e| format!("private key is not valid PKCS#8 DER: {e}"))?;
    let encrypted = pki
        .encrypt_with_params(params, passphrase)
        .map_err(|e| format!("PKCS#8 encryption failed: {e}"))?;

    encrypted
        .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
        .map(|s| s.to_string())
        .map_err(|e| format!("PEM encoding failed: {e}"))
}

/// Decode a PEM `BEGIN PRIVATE KEY` block to its inner DER bytes.
/// Returns an error if the block is missing, has the wrong label, or
/// the body is not valid base64.
fn pem_decode_private_key(pem: &str) -> Result<Vec<u8>, String> {
    let begin = pem.find("-----BEGIN ").ok_or("missing BEGIN line")?;
    let after_begin = &pem[begin + 11..];
    let dash = after_begin.find("-----").ok_or("malformed BEGIN line")?;
    let label = after_begin[..dash].trim();
    if label != "PRIVATE KEY" {
        return Err(format!(
            "expected `BEGIN PRIVATE KEY` (PKCS#8), got `BEGIN {label}`"
        ));
    }
    let end = pem
        .find("-----END PRIVATE KEY-----")
        .ok_or("missing END line")?;
    let body_start = begin + 11 + dash + 5;
    let nl = pem[body_start..]
        .find('\n')
        .ok_or("missing newline after BEGIN")?;
    let body: String = pem[body_start + nl + 1..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .map_err(|e| format!("PEM body is not valid base64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_self_signed_cert_returns_real_pem() {
        let (cert_pem, key_pem) =
            generate_self_signed_cert("example.com", &["www.example.com".to_string()])
                .expect("rcgen should produce a self-signed cert");
        assert!(
            cert_pem.starts_with("-----BEGIN CERTIFICATE-----"),
            "expected real PEM cert, got {cert_pem:.80}"
        );
        assert!(cert_pem.ends_with("-----END CERTIFICATE-----\n"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
        // Substantially longer than the placeholder (= base64-of-domain).
        assert!(cert_pem.len() > 400, "real cert PEM should be >400 chars");
    }

    #[tokio::test]
    async fn request_certificate_stores_real_pem_and_key() {
        let svc = AcmService::default();
        let req = AwsRequest {
            service: "acm".to_string(),
            action: "RequestCertificate".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_vec(&json!({"DomainName": "example.com"})).unwrap(),
            ),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap();
        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(arn)
            .unwrap();
        let cert_pem = cert.certificate_pem.as_deref().unwrap();
        let key_pem = cert.private_key_pem.as_deref().unwrap();
        assert!(cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(cert_pem.len() > 400);
    }

    #[test]
    fn encrypt_private_key_pem_emits_pkcs8_v2_pem() {
        let (_cert, key_pem) =
            generate_self_signed_cert("example.com", &[]).expect("rcgen produces a key");
        let out = encrypt_private_key_pem(&key_pem, b"hunter2").expect("encryption succeeds");
        assert!(
            out.starts_with("-----BEGIN ENCRYPTED PRIVATE KEY-----\n"),
            "expected PKCS#8 v2 envelope, got {out:.80}"
        );
        assert!(out
            .trim_end()
            .ends_with("-----END ENCRYPTED PRIVATE KEY-----"));
        // The plaintext PEM body should not appear in the encrypted
        // output — sanity check that we actually encrypted.
        let plain_body: String = key_pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        assert!(!plain_body.is_empty());
        assert!(!out.contains(&plain_body));
    }

    #[test]
    fn encrypt_private_key_pem_round_trips_via_pkcs8_decoder() {
        use pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};

        let mut rng = rand::thread_rng();
        let original = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("rsa keygen");
        let key_pem = original
            .to_pkcs8_pem(LineEnding::LF)
            .expect("pkcs8 pem encode")
            .to_string();
        let passphrase = b"correct horse battery staple";

        let encrypted = encrypt_private_key_pem(&key_pem, passphrase).unwrap();
        assert!(encrypted.contains("BEGIN ENCRYPTED PRIVATE KEY"));

        let recovered = rsa::RsaPrivateKey::from_pkcs8_encrypted_pem(&encrypted, passphrase)
            .expect("decryption + PKCS#8 parse");
        assert_eq!(original, recovered);
    }

    fn make_req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "acm".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn make_exportable_cert(svc: &AcmService) -> String {
        let req = make_req(
            "RequestCertificate",
            json!({
                "DomainName": "example.com",
                "Options": {"Export": "ENABLED"},
            }),
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["CertificateArn"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn export_certificate_emits_passphrase_encrypted_pem() {
        let svc = AcmService::default();
        let arn = make_exportable_cert(&svc).await;
        let original_key_pem = svc
            .state
            .read()
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap()
            .private_key_pem
            .clone()
            .unwrap();

        let passphrase = b"hunter2";
        let passphrase_b64 = base64::engine::general_purpose::STANDARD.encode(passphrase);
        let export = make_req(
            "ExportCertificate",
            json!({"CertificateArn": arn, "Passphrase": passphrase_b64}),
        );
        let export_resp = svc.handle(export).await.unwrap();
        let export_json: Value = serde_json::from_slice(export_resp.body.expect_bytes()).unwrap();
        let private_key = export_json["PrivateKey"].as_str().unwrap();
        assert!(
            private_key.contains("BEGIN ENCRYPTED PRIVATE KEY"),
            "expected PKCS#8 v2 PEM, got {private_key}"
        );
        assert!(private_key.contains("END ENCRYPTED PRIVATE KEY"));
        // Round-trip: decrypt with the same passphrase and confirm we
        // recover the same private key bytes that ACM stored.
        let original_inner_der =
            pem_decode_private_key(&original_key_pem).expect("decode original PEM");
        let original_pki_bytes = pkcs8::PrivateKeyInfo::try_from(original_inner_der.as_slice())
            .map(|p| p.private_key.to_vec())
            .expect("parse original PKCS#8");
        let encrypted_der =
            pem_decode_encrypted_private_key(private_key).expect("decode encrypted PEM");
        let decrypted_doc = pkcs8::EncryptedPrivateKeyInfo::try_from(encrypted_der.as_slice())
            .expect("parse encrypted PKCS#8")
            .decrypt(passphrase)
            .expect("decrypt with passphrase");
        let decrypted_pki = pkcs8::PrivateKeyInfo::try_from(decrypted_doc.as_bytes())
            .expect("parse decrypted PKCS#8");
        assert_eq!(decrypted_pki.private_key, original_pki_bytes.as_slice());
    }

    #[tokio::test]
    async fn export_certificate_without_passphrase_returns_plain_pem() {
        let svc = AcmService::default();
        let arn = make_exportable_cert(&svc).await;
        let export_resp = svc
            .handle(make_req(
                "ExportCertificate",
                json!({"CertificateArn": arn}),
            ))
            .await
            .unwrap();
        let export_json: Value = serde_json::from_slice(export_resp.body.expect_bytes()).unwrap();
        let private_key = export_json["PrivateKey"].as_str().unwrap();
        assert!(
            private_key.contains("-----BEGIN PRIVATE KEY-----"),
            "expected plain PKCS#8 PEM, got {private_key}"
        );
        assert!(!private_key.contains("ENCRYPTED PRIVATE KEY"));
    }

    #[tokio::test]
    async fn export_certificate_with_empty_passphrase_returns_plain_pem() {
        let svc = AcmService::default();
        let arn = make_exportable_cert(&svc).await;
        let export_resp = svc
            .handle(make_req(
                "ExportCertificate",
                json!({"CertificateArn": arn, "Passphrase": ""}),
            ))
            .await
            .unwrap();
        let export_json: Value = serde_json::from_slice(export_resp.body.expect_bytes()).unwrap();
        let private_key = export_json["PrivateKey"].as_str().unwrap();
        assert!(private_key.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(!private_key.contains("ENCRYPTED PRIVATE KEY"));
    }

    #[tokio::test]
    async fn export_certificate_rejects_non_base64_passphrase() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "example.com"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();
        let export = make_req(
            "ExportCertificate",
            json!({"CertificateArn": arn, "Passphrase": "not!base64!@#$"}),
        );
        let err = match svc.handle(export).await {
            Ok(_) => panic!("expected error for non-base64 passphrase"),
            Err(e) => e,
        };
        assert!(format!("{err:?}").contains("Passphrase"));
    }

    fn pem_decode_encrypted_private_key(pem: &str) -> Result<Vec<u8>, String> {
        let begin = "-----BEGIN ENCRYPTED PRIVATE KEY-----";
        let end = "-----END ENCRYPTED PRIVATE KEY-----";
        let begin_idx = pem.find(begin).ok_or("missing BEGIN")?;
        let after = &pem[begin_idx + begin.len()..];
        let end_idx = after.find(end).ok_or("missing END")?;
        let body: String = after[..end_idx]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(body.as_bytes())
            .map_err(|e| format!("base64: {e}"))
    }

    #[tokio::test]
    async fn describe_certificate_observes_auto_issue_transition() {
        // After the auto-issue tick fires, successive describes should
        // pick up Status=ISSUED, IssuedAt, RenewalEligibility=ELIGIBLE,
        // and a populated RenewalSummary without any extra prodding.
        let svc = AcmService::default()
            .with_pending_validation_delay(std::time::Duration::from_millis(50));
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "example.com", "ValidationMethod": "DNS"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        // Immediate describe sees PENDING_VALIDATION + ELIGIBILITY
        // INELIGIBLE.
        let early = svc
            .handle(make_req(
                "DescribeCertificate",
                json!({"CertificateArn": arn}),
            ))
            .await
            .unwrap();
        let early: Value = serde_json::from_slice(early.body.expect_bytes()).unwrap();
        assert_eq!(early["Certificate"]["Status"], "PENDING_VALIDATION");
        assert_eq!(early["Certificate"]["RenewalEligibility"], "INELIGIBLE");
        assert!(early["Certificate"].get("RenewalSummary").is_none());

        // Poll up to 2s for the auto-issue tick.
        let mut last: Value = Value::Null;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let resp = svc
                .handle(make_req(
                    "DescribeCertificate",
                    json!({"CertificateArn": arn}),
                ))
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            if body["Certificate"]["Status"] == "ISSUED" {
                last = body;
                break;
            }
        }
        assert_eq!(last["Certificate"]["Status"], "ISSUED");
        assert_eq!(last["Certificate"]["RenewalEligibility"], "ELIGIBLE");
        let rs = &last["Certificate"]["RenewalSummary"];
        assert_eq!(rs["RenewalStatus"], "PENDING_AUTO_RENEWAL");
        assert!(rs["UpdatedAt"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn request_certificate_emits_parseable_pem() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "example.com"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        let resp = svc
            .handle(make_req("GetCertificate", json!({"CertificateArn": arn})))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let cert_pem = body["Certificate"].as_str().unwrap();
        assert!(
            cert_pem.starts_with("-----BEGIN CERTIFICATE-----"),
            "expected real PEM cert header, got {cert_pem:.80}"
        );
        assert!(cert_pem.trim_end().ends_with("-----END CERTIFICATE-----"));
        // Real X.509 base64 body is much larger than the legacy
        // base64-of-ARN placeholder (which was ~80 chars total).
        assert!(
            cert_pem.len() > 400,
            "expected real X.509 PEM, got placeholder-sized blob"
        );
        // Sanity-check we didn't smuggle the ARN body back in via the
        // old placeholder code path.
        assert!(!cert_pem.contains(arn.as_str()));
    }

    #[tokio::test]
    async fn request_certificate_includes_san() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({
                    "DomainName": "example.com",
                    "SubjectAlternativeNames": ["api.example.com", "www.example.com"],
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        let resp = svc
            .handle(make_req("GetCertificate", json!({"CertificateArn": arn})))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let cert_pem = body["Certificate"].as_str().unwrap();
        assert!(cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(cert_pem.trim_end().ends_with("-----END CERTIFICATE-----"));

        // Decode the PEM body and search the DER bytes for the SAN dNSName
        // octets. rcgen emits SANs as IA5String entries inside the
        // SubjectAltName extension, so the raw domain bytes are present.
        let body_b64: String = cert_pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("");
        let der = base64::engine::general_purpose::STANDARD
            .decode(&body_b64)
            .expect("cert body is valid base64");
        for san in ["example.com", "api.example.com", "www.example.com"] {
            assert!(
                der.windows(san.len()).any(|w| w == san.as_bytes()),
                "expected SAN {san} embedded in DER"
            );
        }
    }

    #[tokio::test]
    async fn request_certificate_starts_in_pending_validation() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "example.com", "ValidationMethod": "DNS"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();
        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "PENDING_VALIDATION");
        assert!(cert.issued_at.is_none());
    }

    #[tokio::test]
    async fn set_certificate_status_to_issued_via_admin() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "admin.example.com"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        assert!(svc.set_certificate_status(&arn, "ISSUED", None));
        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "ISSUED");
        assert!(cert.issued_at.is_some());
        for dv in &cert.domain_validation {
            assert_eq!(dv.validation_status, "SUCCESS");
        }
    }

    #[tokio::test]
    async fn set_certificate_status_unknown_arn_returns_false() {
        let svc = AcmService::default();
        assert!(!svc.set_certificate_status("does-not-exist", "ISSUED", None));
    }

    #[tokio::test]
    async fn auto_issue_tick_transitions_to_issued() {
        let svc = AcmService::default()
            .with_pending_validation_delay(std::time::Duration::from_millis(50));
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "auto.example.com", "ValidationMethod": "DNS"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "ISSUED");
        assert!(cert.issued_at.is_some());
        assert_eq!(cert.renewal_eligibility, "ELIGIBLE");
        let rs = cert.renewal_summary.as_ref().expect("renewal summary set");
        assert_eq!(rs.renewal_status, "PENDING_AUTO_RENEWAL");
    }

    #[tokio::test]
    async fn email_validation_stays_pending_until_approve() {
        // EMAIL certs intentionally do NOT trigger the auto-issue tick:
        // real ACM only flips them once the user clicks the approval
        // link. fakecloud routes the approval through
        // `approve_certificate`.
        let svc = AcmService::default()
            .with_pending_validation_delay(std::time::Duration::from_millis(20));
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({
                    "DomainName": "email.example.com",
                    "ValidationMethod": "EMAIL",
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        // Wait well past the DNS auto-issue delay; EMAIL must stay
        // PENDING_VALIDATION because no tick was scheduled.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        {
            let st = svc.state.read();
            let cert = st
                .accounts
                .get("123456789012")
                .unwrap()
                .certificates
                .get(&arn)
                .unwrap();
            assert_eq!(cert.status, "PENDING_VALIDATION");
            assert!(cert.issued_at.is_none());
            assert!(cert.renewal_summary.is_none());
        }

        assert!(svc.approve_certificate(&arn));
        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "ISSUED");
        assert_eq!(cert.renewal_eligibility, "ELIGIBLE");
        for dv in &cert.domain_validation {
            assert_eq!(dv.validation_status, "SUCCESS");
        }
        assert!(cert.renewal_summary.is_some());
    }

    #[tokio::test]
    async fn renew_certificate_refreshes_renewal_summary() {
        let svc = AcmService::default()
            .with_pending_validation_delay(std::time::Duration::from_millis(20));
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "renew.example.com", "ValidationMethod": "DNS"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let resp = svc
            .handle(make_req("RenewCertificate", json!({"CertificateArn": arn})))
            .await
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);

        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "ISSUED");
        let rs = cert.renewal_summary.as_ref().expect("renewal summary set");
        assert_eq!(rs.renewal_status, "SUCCESS");
        assert_eq!(rs.domain_validation.len(), cert.domain_validation.len());
    }

    #[test]
    fn auto_issue_delay_default_when_env_unset() {
        // Other tests don't touch FAKECLOUD_ACM_AUTO_ISSUE_SECS, so
        // reading it here while running serially with `cargo test`
        // returns the documented default. We use a constant to guard
        // against accidental drift in `DEFAULT_AUTO_ISSUE_SECS`.
        if std::env::var("FAKECLOUD_ACM_AUTO_ISSUE_SECS").is_err() {
            assert_eq!(
                auto_issue_delay_from_env(),
                std::time::Duration::from_secs(DEFAULT_AUTO_ISSUE_SECS),
            );
        }
    }

    #[tokio::test]
    async fn set_certificate_status_failed_records_reason() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "fail.example.com"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();
        assert!(svc.set_certificate_status(&arn, "FAILED", Some("DNS lookup error".to_string())));
        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        assert_eq!(cert.status, "FAILED");
        assert_eq!(cert.failure_reason.as_deref(), Some("DNS lookup error"));
    }

    #[tokio::test]
    async fn request_certificate_private_key_pem_is_valid() {
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({"DomainName": "key.example.com"}),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();

        let st = svc.state.read();
        let cert = st
            .accounts
            .get("123456789012")
            .unwrap()
            .certificates
            .get(&arn)
            .unwrap();
        let key_pem = cert
            .private_key_pem
            .as_deref()
            .expect("RequestCertificate should populate a real private key");
        let starts_pkcs8 = key_pem.starts_with("-----BEGIN PRIVATE KEY-----");
        let starts_rsa = key_pem.starts_with("-----BEGIN RSA PRIVATE KEY-----");
        assert!(
            starts_pkcs8 || starts_rsa,
            "expected PKCS#8 or RSA private key header, got {key_pem:.80}"
        );
        assert!(
            key_pem.trim_end().ends_with("-----END PRIVATE KEY-----")
                || key_pem
                    .trim_end()
                    .ends_with("-----END RSA PRIVATE KEY-----")
        );
        assert!(key_pem.len() > 200);
    }

    // bug-audit 2026-05-28, 1.5: SearchCertificates must honor the
    // CertificateStatuses filter and apply SortBy/SortOrder (previously only
    // KeyTypes was applied and the order was hardcoded).
    #[tokio::test]
    async fn search_certificates_filters_by_status_and_sorts() {
        let svc = AcmService::default();
        let mut arn: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
        for d in ["a.example.com", "b.example.com", "c.example.com"] {
            let resp = svc
                .handle(make_req("RequestCertificate", json!({ "DomainName": d })))
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            arn.insert(d, body["CertificateArn"].as_str().unwrap().to_string());
        }

        // Stamp deterministic statuses + creation times: a oldest, c newest.
        {
            let mut state = svc.state.write();
            let certs = &mut state.accounts.get_mut("123456789012").unwrap().certificates;
            let now = chrono::Utc::now();
            for c in certs.values_mut() {
                match c.domain_name.as_str() {
                    "a.example.com" => {
                        c.status = "ISSUED".to_string();
                        c.created_at = now - chrono::Duration::hours(2);
                    }
                    "b.example.com" => {
                        c.status = "PENDING_VALIDATION".to_string();
                        c.created_at = now - chrono::Duration::hours(1);
                    }
                    _ => {
                        c.status = "ISSUED".to_string();
                        c.created_at = now;
                    }
                }
            }
        }

        let domains = |body: &Value| -> Vec<String> {
            body["Results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["CertificateArn"].as_str().unwrap().to_string())
                .collect()
        };

        // CertificateStatuses filter keeps only ISSUED (a + c).
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({ "CertificateStatuses": ["ISSUED"] }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let mut issued = domains(&body);
        issued.sort();
        let mut want = vec![arn["a.example.com"].clone(), arn["c.example.com"].clone()];
        want.sort();
        assert_eq!(issued, want);

        // SortBy CREATED_AT ASCENDING -> oldest first.
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({ "SortBy": "CREATED_AT", "SortOrder": "ASCENDING" }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            domains(&body),
            vec![
                arn["a.example.com"].clone(),
                arn["b.example.com"].clone(),
                arn["c.example.com"].clone(),
            ]
        );

        // DESCENDING -> newest first (reverse).
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({ "SortBy": "CREATED_AT", "SortOrder": "DESCENDING" }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            domains(&body),
            vec![
                arn["c.example.com"].clone(),
                arn["b.example.com"].clone(),
                arn["a.example.com"].clone(),
            ]
        );
    }

    #[tokio::test]
    async fn search_certificates_extended_key_usage_filter() {
        // EKU filtering uses the modeled X509AttributeFilter.ExtendedKeyUsage
        // (a single value). fakecloud certs carry the canonical TLS
        // server/client auth EKUs, so a matching filter keeps all certs and a
        // non-matching one drops them all.
        let svc = AcmService::default();
        for d in ["a.example.com", "b.example.com"] {
            svc.handle(make_req("RequestCertificate", json!({ "DomainName": d })))
                .await
                .unwrap();
        }

        let count = |body: &Value| body["Results"].as_array().unwrap().len();

        // Matching EKU -> both certs returned.
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({
                    "FilterStatement": {
                        "Filter": {
                            "X509AttributeFilter": {
                                "ExtendedKeyUsage": "TLS_WEB_SERVER_AUTHENTICATION"
                            }
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(count(&body), 2, "matching EKU should return all certs");

        // Non-matching EKU -> filtered out entirely.
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({
                    "FilterStatement": {
                        "Filter": {
                            "X509AttributeFilter": { "ExtendedKeyUsage": "CODE_SIGNING" }
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(count(&body), 0, "non-matching EKU should drop all certs");

        // The `ANY` sentinel matches any cert that has at least one EKU.
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({
                    "FilterStatement": {
                        "Filter": { "X509AttributeFilter": { "ExtendedKeyUsage": "ANY" } }
                    }
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(count(&body), 2, "ANY EKU should match all certs with EKUs");

        // Same `ANY` semantics for KeyUsage.
        let resp = svc
            .handle(make_req(
                "SearchCertificates",
                json!({
                    "FilterStatement": {
                        "Filter": { "X509AttributeFilter": { "KeyUsage": "ANY" } }
                    }
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            count(&body),
            2,
            "ANY KeyUsage should match all certs with key usages"
        );
    }

    #[tokio::test]
    async fn search_certificates_honors_and_or_not_composition() {
        // Two certs with distinct key algorithms let us exercise the recursive
        // And/Or/Not FilterStatement composition (previously ignored).
        let svc = AcmService::default();
        svc.handle(make_req(
            "RequestCertificate",
            json!({ "DomainName": "rsa.example.com", "KeyAlgorithm": "RSA_2048" }),
        ))
        .await
        .unwrap();
        svc.handle(make_req(
            "RequestCertificate",
            json!({ "DomainName": "ec.example.com", "KeyAlgorithm": "EC_prime256v1" }),
        ))
        .await
        .unwrap();

        let search = |stmt: Value| {
            let svc = &svc;
            async move {
                let resp = svc
                    .handle(make_req(
                        "SearchCertificates",
                        json!({ "FilterStatement": stmt }),
                    ))
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
                body["Results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| {
                        c["X509Attributes"]["KeyAlgorithm"]
                            .as_str()
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            }
        };

        // Leaf uses the modeled X509AttributeFilter.KeyAlgorithm (single value).
        let key_alg =
            |alg: &str| json!({ "Filter": { "X509AttributeFilter": { "KeyAlgorithm": alg } } });

        // Not RSA -> only the EC cert.
        let got = search(json!({ "Not": key_alg("RSA_2048") })).await;
        assert_eq!(got, vec!["EC_prime256v1"]);

        // Or of both key algorithms -> both certs.
        let mut got =
            search(json!({ "Or": [key_alg("RSA_2048"), key_alg("EC_prime256v1")] })).await;
        got.sort();
        assert_eq!(got, vec!["EC_prime256v1", "RSA_2048"]);

        // And of key algorithm + matching EKU -> just the RSA cert.
        let got = search(json!({
            "And": [
                key_alg("RSA_2048"),
                { "Filter": { "X509AttributeFilter": { "ExtendedKeyUsage": "TLS_WEB_SERVER_AUTHENTICATION" } } },
            ]
        }))
        .await;
        assert_eq!(got, vec!["RSA_2048"]);

        // And of two contradictory key algorithms -> nothing.
        let got = search(json!({ "And": [key_alg("RSA_2048"), key_alg("EC_prime256v1")] })).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn search_certificates_leaf_filter_shapes_apply() {
        // Cover the real CertificateFilter union leaves: CertificateArn,
        // X509AttributeFilter (CommonName / DnsName / SerialNumber), and
        // AcmCertificateMetadataFilter (Status).
        let svc = AcmService::default();
        let resp = svc
            .handle(make_req(
                "RequestCertificate",
                json!({
                    "DomainName": "primary.example.com",
                    "SubjectAlternativeNames": ["alt.example.org"],
                }),
            ))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["CertificateArn"].as_str().unwrap().to_string();
        svc.handle(make_req(
            "RequestCertificate",
            json!({ "DomainName": "other.example.com" }),
        ))
        .await
        .unwrap();

        let arns = |stmt: Value| {
            let svc = &svc;
            async move {
                let resp = svc
                    .handle(make_req(
                        "SearchCertificates",
                        json!({ "FilterStatement": stmt }),
                    ))
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
                body["Results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c["CertificateArn"].as_str().unwrap().to_string())
                    .collect::<Vec<_>>()
            }
        };

        // CertificateArn leaf -> exactly that cert.
        let got = arns(json!({ "Filter": { "CertificateArn": arn } })).await;
        assert_eq!(got.len(), 1);

        // Subject.CommonName EQUALS -> the primary cert only.
        let got = arns(json!({
            "Filter": { "X509AttributeFilter": { "Subject": {
                "CommonName": { "Value": "primary.example.com", "ComparisonOperator": "EQUALS" }
            } } }
        }))
        .await;
        assert_eq!(got.len(), 1);

        // SubjectAlternativeName.DnsName CONTAINS -> the primary cert (its SAN).
        let got = arns(json!({
            "Filter": { "X509AttributeFilter": { "SubjectAlternativeName": {
                "DnsName": { "Value": "example.org", "ComparisonOperator": "CONTAINS" }
            } } }
        }))
        .await;
        assert_eq!(got.len(), 1);

        // Metadata Status filter: both start PENDING_VALIDATION, so both match;
        // a bogus status matches none.
        let got = arns(json!({
            "Filter": { "AcmCertificateMetadataFilter": { "Status": "PENDING_VALIDATION" } }
        }))
        .await;
        assert_eq!(got.len(), 2);
        let got = arns(json!({
            "Filter": { "AcmCertificateMetadataFilter": { "Status": "EXPIRED" } }
        }))
        .await;
        assert!(got.is_empty());
    }
}
