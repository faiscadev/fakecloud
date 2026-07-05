//! ACM Private CA (`acm-pca`) awsJson1_1 service.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_acmpca_snapshot;
use crate::provision::{build_creating_ca, CaCreateParams};
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

pub struct AcmPcaService {
    state: SharedAcmPcaState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl AcmPcaService {
    pub fn new(state: SharedAcmPcaState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
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

    /// Generate a CA's key pair + CSR off the async runtime and settle the CA
    /// from `CREATING` to `PENDING_CERTIFICATE`. Real RSA-4096 generation can
    /// take tens of seconds; running it on `spawn_blocking` keeps the create
    /// call fast and never blocks the request path. On failure the CA is marked
    /// `FAILED`. The transition is persisted so it survives a restart.
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
                    if ca.status == "CREATING" {
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
            save_acmpca_snapshot(&state, store, &lock).await;
        });
    }

    /// Re-arm key generation for any CA restored in the `CREATING` state (its
    /// key was never persisted because the previous process exited mid-keygen).
    /// Called by the server after loading the persistence snapshot.
    pub fn rearm_pending_creations(&self) {
        let pending: Vec<(String, String, String, Value)> = {
            let state = self.state.read();
            let mut out = Vec::new();
            for (account_id, account) in state.accounts.iter() {
                for (arn, ca) in account.authorities.iter() {
                    if ca.status == "CREATING" {
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
        let ca_id = Uuid::new_v4().to_string();
        let arn = format!("arn:aws:acm-pca:{region}:{account}:certificate-authority/{ca_id}");

        // Every CA — ROOT included — starts in CREATING and settles to
        // PENDING_CERTIFICATE once its (real, requested-algorithm) key pair has
        // been generated on a background task, matching AWS's lifecycle and
        // keeping CreateCertificateAuthority fast even for slow RSA-4096 keygen.
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
        let ca = build_creating_ca(params);
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
        if let Some(max) = body.get("MaxResults") {
            match max.as_i64() {
                Some(n) if (1..=1000).contains(&n) => {}
                _ => return Err(invalid_args("MaxResults must be between 1 and 1000")),
            }
        }
        if let Some(token) = body.get("NextToken") {
            let valid = token
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 43739);
            if !valid {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidNextTokenException",
                    "The NextToken value is invalid",
                ));
            }
        }
        if let Some(owner) = body.get("ResourceOwner").and_then(Value::as_str) {
            if owner != "SELF" && owner != "OTHER_ACCOUNTS" {
                return Err(invalid_args(format!("Invalid ResourceOwner: {owner}")));
            }
        }
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .accounts
            .get(&account_id(req))
            .map(|a| a.authorities.values().map(ca_to_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "CertificateAuthorities": list }),
        ))
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
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        Ok(AwsResponse::ok_json(
            json!({ "Tags": tags_to_json(&ca.tags) }),
        ))
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
        let accounts = self.state.read();
        let ca = accounts
            .accounts
            .get(&account_id(req))
            .and_then(|a| a.authorities.get(&arn))
            .ok_or_else(|| not_found(&arn))?;
        let perms: Vec<Value> = ca.permissions.values().map(permission_to_json).collect();
        Ok(AwsResponse::ok_json(json!({ "Permissions": perms })))
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
                s3_bucket_name: s3_bucket,
                s3_key: s3_key.clone(),
                status: "SUCCESS".to_string(),
                created_at: Utc::now(),
                response_format: format,
                body: report_body,
            },
        );
        Ok(AwsResponse::ok_json(
            json!({ "AuditReportId": report_id, "S3Key": s3_key }),
        ))
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

// ─── Timestamp ──────────────────────────────────────────────────────

#[allow(dead_code)]
fn epoch_f64(dt: DateTime<Utc>) -> f64 {
    dt.timestamp() as f64
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

    /// Poll `DescribeCertificateAuthority` until the CA leaves `CREATING`. Key
    /// generation now runs on a background task, so a freshly created CA starts
    /// in `CREATING` and settles to `PENDING_CERTIFICATE` once its key + CSR are
    /// ready.
    async fn poll_pending(svc: &AcmPcaService, arn: &str) {
        for _ in 0..400 {
            let d = svc
                .describe_certificate_authority(&req(
                    "DescribeCertificateAuthority",
                    json!({ "CertificateAuthorityArn": arn }),
                ))
                .unwrap();
            match body_json(&d)["CertificateAuthority"]["Status"]
                .as_str()
                .unwrap()
            {
                "PENDING_CERTIFICATE" => return,
                "FAILED" => panic!("CA key generation FAILED"),
                _ => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            }
        }
        panic!("CA never reached PENDING_CERTIFICATE");
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
}
