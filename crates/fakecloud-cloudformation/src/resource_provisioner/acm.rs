//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `acm`.

use super::*;

impl ResourceProvisioner {
    // --- ACM ---

    pub(super) fn create_acm_certificate(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain_name = props
            .get("DomainName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "DomainName is required".to_string())?
            .to_string();
        let sans: Vec<String> = props
            .get("SubjectAlternativeNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let key_algorithm = props
            .get("KeyAlgorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("RSA_2048")
            .to_string();
        let validation_method = props
            .get("ValidationMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("DNS")
            .to_string();
        let ca_arn = props
            .get("CertificateAuthorityArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_acm_tags(props.get("Tags"));
        let cert_transparency = props
            .get("CertificateTransparencyLoggingPreference")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLED")
            .to_string();

        // Mint a deterministic-ish ARN — ACM uses a UUID per certificate.
        let arn = format!(
            "arn:aws:acm:{}:{}:certificate/{}",
            self.region,
            self.account_id,
            Uuid::new_v4()
        );
        let now = Utc::now();

        // Build a real self-signed PEM via rcgen for the cert+SANs so
        // GetCertificate / DescribeCertificate round-trip parseable
        // X.509 (matches the runtime RequestCertificate path).
        let mut all_names = vec![domain_name.clone()];
        for s in &sans {
            if !all_names.contains(s) {
                all_names.push(s.clone());
            }
        }
        let (cert_pem, key_pem) = rcgen::generate_simple_self_signed(all_names.clone())
            .map(|c| (c.cert.pem(), c.key_pair.serialize_pem()))
            .map(|(c, k)| (Some(c), Some(k)))
            .unwrap_or((None, None));

        // CFN-provisioned certs land as ISSUED right away — real CFN
        // blocks until validation completes, but fakecloud doesn't run
        // a DNS server, so leaving the cert PENDING_VALIDATION would
        // wedge dependent resources (CloudFront/ELBv2) forever. The
        // runtime RequestCertificate path uses the async auto-issue
        // tick (DNS) or admin `/approve` endpoint (EMAIL) for parity
        // with ACM's async behaviour.
        let domain_validation: Vec<AcmDomainValidation> =
            synth_acm_domain_validation(&domain_name, &sans, &validation_method)
                .into_iter()
                .map(|mut dv| {
                    dv.validation_status = "SUCCESS".to_string();
                    dv
                })
                .collect();
        let renewal_summary = Some(AcmRenewalSummary {
            renewal_status: "PENDING_AUTO_RENEWAL".to_string(),
            domain_validation: domain_validation.clone(),
            renewal_status_reason: None,
            updated_at: now,
        });
        let cert = AcmStoredCertificate {
            arn: arn.clone(),
            domain_name: domain_name.clone(),
            subject_alternative_names: all_names,
            status: "ISSUED".to_string(),
            cert_type: "AMAZON_ISSUED".to_string(),
            certificate_pem: cert_pem,
            certificate_chain_pem: None,
            private_key_pem: key_pem,
            idempotency_token: None,
            serial: format!("{:032x}", Uuid::new_v4().as_u128()),
            subject: format!("CN={domain_name}"),
            issuer: "Amazon".to_string(),
            key_algorithm,
            signature_algorithm: "SHA256WITHRSA".to_string(),
            created_at: now,
            issued_at: Some(now),
            imported_at: None,
            revoked_at: None,
            revocation_reason: None,
            not_before: now,
            // 13 months (matches real ACM issued-cert lifetime).
            not_after: now + chrono::Duration::days(395),
            validation_method: Some(validation_method.clone()),
            domain_validation,
            options: AcmCertificateOptions {
                certificate_transparency_logging_preference: cert_transparency,
                export: "DISABLED".to_string(),
            },
            renewal_eligibility: "ELIGIBLE".to_string(),
            managed_by: None,
            certificate_authority_arn: ca_arn,
            tags,
            in_use_by: Vec::new(),
            describe_read_count: 0,
            failure_reason: None,
            renewal_summary,
        };

        let mut accounts = self.acm_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.certificates.insert(arn.clone(), cert);

        Ok(ProvisionResult::new(arn))
    }

    pub(super) fn delete_acm_certificate(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.acm_state.write();
        if let Some(account) = accounts.accounts.get_mut(&self.account_id) {
            account.certificates.remove(physical_id);
        }
        Ok(())
    }

    /// Provision the singleton `AWS::CertificateManager::Account` resource —
    /// stores `ExpiryEventsConfiguration.DaysBeforeExpiry` on the account
    /// config so `GetAccountConfiguration` reflects it.
    pub(super) fn create_acm_account(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let days = resource
            .properties
            .get("ExpiryEventsConfiguration")
            .and_then(|v| v.get("DaysBeforeExpiry"))
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let mut accounts = self.acm_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.account_config.expiry_events_days_before_expiry = days;
        Ok(ProvisionResult::new(format!(
            "acm-account-{}",
            self.account_id
        )))
    }

    /// Reset the account config back to the default (no expiry events).
    /// AWS keeps the account around — the CFN deletion just clears the
    /// per-account override.
    pub(super) fn delete_acm_account(&self) -> Result<(), String> {
        let mut accounts = self.acm_state.write();
        if let Some(account) = accounts.accounts.get_mut(&self.account_id) {
            account.account_config.expiry_events_days_before_expiry = None;
        }
        Ok(())
    }
}
