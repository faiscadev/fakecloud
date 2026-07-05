//! CloudFormation provisioner arms for `AWS::ACMPCA::*`.
//!
//! Without a provisioner arm a service with a registered snapshot hook yields
//! phantom `CREATE_COMPLETE` resources that vanish on restart (#1766). These
//! arms mutate the snapshot-backed `acm-pca` state directly, and the server's
//! `acm-pca` snapshot hook writes them through to disk.

use super::*;

use fakecloud_acmpca::{CertificateAuthority, IssuedCertificate, Permission};
use serde_json::json;

impl ResourceProvisioner {
    // --- AWS::ACMPCA::CertificateAuthority ---

    pub(super) fn create_acmpca_certificate_authority(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let ca_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("ROOT")
            .to_string();
        // ACM PCA nests the crypto config under `CertificateAuthorityConfiguration`
        // in the API, but flattens `KeyAlgorithm`/`SigningAlgorithm`/`Subject`
        // onto the resource in CloudFormation.
        let key_algorithm = props
            .get("KeyAlgorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("RSA_2048")
            .to_string();
        let signing_algorithm = props
            .get("SigningAlgorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("SHA256WITHRSA")
            .to_string();
        let subject = props.get("Subject").cloned().unwrap_or_else(|| json!({}));
        let usage_mode = props
            .get("UsageMode")
            .and_then(|v| v.as_str())
            .unwrap_or("GENERAL_PURPOSE")
            .to_string();
        let revocation_configuration = props.get("RevocationConfiguration").cloned();

        let configuration = json!({
            "KeyAlgorithm": key_algorithm,
            "SigningAlgorithm": signing_algorithm,
            "Subject": subject,
        });

        let ca_id = Uuid::new_v4();
        let arn = format!(
            "arn:aws:acm-pca:{}:{}:certificate-authority/{}",
            self.region, self.account_id, ca_id
        );
        let now = Utc::now();

        let key_pair = fakecloud_acmpca::generate_key_pair(&key_algorithm)
            .map_err(|e| format!("ACM PCA key generation failed: {e}"))?;
        let key_pem = key_pair.serialize_pem();
        let csr_pem = fakecloud_acmpca::generate_ca_csr(&subject, &key_pair)
            .map_err(|e| format!("ACM PCA CSR generation failed: {e}"))?;

        let (status, ca_cert_pem, serial, not_before, not_after) = if ca_type == "ROOT" {
            let nb = now;
            let na = now + chrono::Duration::days(3650);
            let (cert_pem, serial) =
                fakecloud_acmpca::generate_root_ca(&subject, &key_pair, nb, na)
                    .map_err(|e| format!("ACM PCA root CA generation failed: {e}"))?;
            (
                "ACTIVE".to_string(),
                Some(cert_pem),
                Some(serial),
                Some(nb),
                Some(na),
            )
        } else {
            ("PENDING_CERTIFICATE".to_string(), None, None, None, None)
        };

        let ca = CertificateAuthority {
            arn: arn.clone(),
            owner_account: self.account_id.clone(),
            created_at: now,
            last_state_change_at: Some(now),
            ca_type,
            serial,
            status,
            not_before,
            not_after,
            failure_reason: None,
            key_algorithm,
            signing_algorithm,
            configuration,
            revocation_configuration,
            usage_mode,
            key_storage_security_standard: None,
            restorable_until: None,
            idempotency_token: None,
            tags: parse_acmpca_tags(props.get("Tags")),
            ca_key_pem: key_pem,
            ca_cert_pem,
            ca_cert_chain_pem: None,
            csr_pem: csr_pem.clone(),
            issued: Default::default(),
            revoked: Default::default(),
            permissions: Default::default(),
            audit_reports: Default::default(),
        };

        let mut accounts = self.acmpca_state.write();
        accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default()
            .authorities
            .insert(arn.clone(), ca);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("CertificateSigningRequest", csr_pem))
    }

    pub(super) fn delete_acmpca_certificate_authority(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.acmpca_state.write();
        if let Some(account) = accounts.accounts.get_mut(&self.account_id) {
            account.authorities.remove(physical_id);
        }
        Ok(())
    }

    // --- AWS::ACMPCA::Certificate ---

    pub(super) fn create_acmpca_certificate(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let ca_arn = props
            .get("CertificateAuthorityArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CertificateAuthorityArn is required".to_string())?
            .to_string();
        let csr_pem = props
            .get("CertificateSigningRequest")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CertificateSigningRequest is required".to_string())?
            .to_string();
        let signing_algorithm = props
            .get("SigningAlgorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("SHA256WITHRSA")
            .to_string();
        let template_arn = props
            .get("TemplateArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let (validity_value, validity_type) = props
            .get("Validity")
            .map(|v| {
                (
                    v.get("Value").and_then(|x| x.as_i64()).unwrap_or(365),
                    v.get("Type")
                        .and_then(|x| x.as_str())
                        .unwrap_or("DAYS")
                        .to_string(),
                )
            })
            .unwrap_or((365, "DAYS".to_string()));

        let is_ca = template_arn
            .as_deref()
            .map(|t| t.contains("CACertificate") || t.contains("RootCACertificate"))
            .unwrap_or(false);
        let now = Utc::now();
        let not_after = fakecloud_acmpca::resolve_validity(now, validity_value, &validity_type);

        let mut accounts = self.acmpca_state.write();
        let ca = accounts
            .accounts
            .get_mut(&self.account_id)
            .and_then(|a| a.authorities.get_mut(&ca_arn))
            .ok_or_else(|| format!("certificate authority {ca_arn} not found"))?;
        let ca_cert_pem = ca
            .ca_cert_pem
            .clone()
            .ok_or_else(|| "certificate authority has no certificate".to_string())?;
        let ca_key = fakecloud_acmpca::load_key_pair(&ca.ca_key_pem)
            .map_err(|e| format!("failed to load CA key: {e}"))?;
        let (cert_pem, serial) = fakecloud_acmpca::issue_certificate(
            &ca_cert_pem,
            &ca_key,
            &csr_pem,
            now,
            not_after,
            is_ca,
        )
        .map_err(|e| format!("ACM PCA certificate issuance failed: {e}"))?;

        let cert_arn = format!("{ca_arn}/certificate/{serial}");
        let mut chain = ca_cert_pem;
        if let Some(parent) = &ca.ca_cert_chain_pem {
            chain.push_str(parent);
        }
        ca.issued.insert(
            cert_arn.clone(),
            IssuedCertificate {
                arn: cert_arn.clone(),
                serial,
                certificate_pem: cert_pem.clone(),
                chain_pem: Some(chain),
                issued_at: now,
                not_before: now,
                not_after,
                template_arn,
                signing_algorithm,
                idempotency_token: None,
            },
        );

        Ok(ProvisionResult::new(cert_arn.clone())
            .with("Arn", cert_arn)
            .with("Certificate", cert_pem))
    }

    // --- AWS::ACMPCA::CertificateAuthorityActivation ---

    pub(super) fn create_acmpca_certificate_authority_activation(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let ca_arn = props
            .get("CertificateAuthorityArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CertificateAuthorityArn is required".to_string())?
            .to_string();
        let cert_pem = props
            .get("Certificate")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Certificate is required".to_string())?
            .to_string();
        let chain_pem = props
            .get("CertificateChain")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let status = props
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_string();

        let mut complete_chain = cert_pem.clone();
        if let Some(chain) = &chain_pem {
            complete_chain.push_str(chain);
        }

        let mut accounts = self.acmpca_state.write();
        let ca = accounts
            .accounts
            .get_mut(&self.account_id)
            .and_then(|a| a.authorities.get_mut(&ca_arn))
            .ok_or_else(|| format!("certificate authority {ca_arn} not found"))?;
        ca.ca_cert_pem = Some(cert_pem);
        ca.ca_cert_chain_pem = chain_pem;
        ca.status = status;
        if ca.not_before.is_none() {
            ca.not_before = Some(Utc::now());
            ca.not_after = Some(Utc::now() + chrono::Duration::days(3650));
        }
        ca.last_state_change_at = Some(Utc::now());

        let physical_id = format!("{ca_arn}-activation");
        Ok(ProvisionResult::new(physical_id).with("CompleteCertificateChain", complete_chain))
    }

    // --- AWS::ACMPCA::Permission ---

    pub(super) fn create_acmpca_permission(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let ca_arn = props
            .get("CertificateAuthorityArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "CertificateAuthorityArn is required".to_string())?
            .to_string();
        let principal = props
            .get("Principal")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Principal is required".to_string())?
            .to_string();
        let actions: Vec<String> = props
            .get("Actions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let source_account = props
            .get("SourceAccount")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.acmpca_state.write();
        let ca = accounts
            .accounts
            .get_mut(&self.account_id)
            .and_then(|a| a.authorities.get_mut(&ca_arn))
            .ok_or_else(|| format!("certificate authority {ca_arn} not found"))?;
        ca.permissions.insert(
            principal.clone(),
            Permission {
                certificate_authority_arn: ca_arn.clone(),
                created_at: Utc::now(),
                principal: principal.clone(),
                source_account,
                actions,
                policy: None,
            },
        );

        Ok(ProvisionResult::new(format!("{ca_arn}#{principal}")))
    }

    pub(super) fn delete_acmpca_permission(&self, physical_id: &str) -> Result<(), String> {
        let Some((ca_arn, principal)) = physical_id.split_once('#') else {
            return Ok(());
        };
        let mut accounts = self.acmpca_state.write();
        if let Some(ca) = accounts
            .accounts
            .get_mut(&self.account_id)
            .and_then(|a| a.authorities.get_mut(ca_arn))
        {
            ca.permissions.remove(principal);
        }
        Ok(())
    }
}

/// Parse a CloudFormation `Tags` list into ACM PCA tag entries.
fn parse_acmpca_tags(value: Option<&serde_json::Value>) -> Vec<fakecloud_acmpca::TagEntry> {
    value
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let key = t.get("Key").and_then(|v| v.as_str())?.to_string();
                    let value = t
                        .get("Value")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    Some(fakecloud_acmpca::TagEntry { key, value })
                })
                .collect()
        })
        .unwrap_or_default()
}
