//! Route53 `dnssec` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    /// Sign the canonical RRset for `answers` with `ksk`. Returns
    /// base64-encoded raw `r||s` (64 bytes for ECDSA-P256). `None`
    /// when the key material is unavailable (e.g., legacy snapshots
    /// pre-dating the DNSSEC fields).
    pub(super) fn compute_rrsig_for_answers(
        &self,
        zone_name: &str,
        owner_name: &str,
        record_type: &str,
        ttl: u32,
        answers: &[String],
        ksk: &StoredKeySigningKey,
    ) -> Option<String> {
        if ksk.private_key_pem.is_empty() {
            return None;
        }
        let rtype_code = crate::dnssec::type_code(record_type)?;
        let rdatas: Vec<Vec<u8>> = answers
            .iter()
            .map(|v| crate::dnssec::encode_rdata(record_type, v))
            .collect();
        let canonical = crate::dnssec::canonical_rrset_bytes(
            owner_name,
            rtype_code,
            crate::dnssec::CLASS_IN,
            ttl,
            &rdatas,
        );
        let now = Utc::now().timestamp() as u32;
        let inception = now;
        let expiration = now.saturating_add(30 * 24 * 60 * 60); // 30 days
        let header = crate::dnssec::RrsigHeader {
            rtype: rtype_code,
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            labels: crate::dnssec::label_count(owner_name),
            original_ttl: ttl,
            sig_expiration: expiration,
            sig_inception: inception,
            key_tag: ksk.key_tag as u16,
            signer_name: zone_name,
        };
        let signed = crate::dnssec::rrsig_signed_data(&header, &canonical);
        let sig = crate::dnssec::sign_with_pkcs8_pem(&ksk.private_key_pem, &signed);
        Some(crate::dnssec::b64(&sig))
    }

    /// Look up the public DNSSEC material for a zone's first ACTIVE
    /// KSK (sorted by name). Returns the DNSKEY public key, computed
    /// key tag, DS digest hex, and the KSK record so admin endpoints
    /// can surface a stable DNSSEC chain-of-trust to test code.
    /// Returns `None` if the zone has no ACTIVE KSK.
    pub fn dnssec_material_for_zone(
        &self,
        zone_id: &str,
    ) -> Option<(StoredKeySigningKey, Vec<u8>, u16, String)> {
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT)?;
        let zone_id_clean = strip_zone_prefix(zone_id);
        // Existence check — return None when the zone is unknown
        // rather than synthesising bogus material.
        account.hosted_zones.get(&zone_id_clean)?;
        let ksk = account
            .key_signing_keys
            .values()
            .filter(|k| {
                k.hosted_zone_id == zone_id_clean && k.status.eq_ignore_ascii_case("ACTIVE")
            })
            .min_by(|a, b| a.name.cmp(&b.name))
            .cloned()?;
        let material = crate::dnssec::derive_keypair(&ksk.hosted_zone_id, &ksk.name);
        let key_tag = ksk.key_tag as u16;
        let ds_hex = ksk.ds_digest_hex.clone();
        Some((ksk, material.dnskey_public_key, key_tag, ds_hex))
    }

    /// Sign an arbitrary RRset with the zone's first ACTIVE KSK.
    /// Returns the base64 RRSIG bytes plus the tag/algorithm so the
    /// caller can construct a full RRSIG record. `None` when the zone
    /// or active KSK is missing or the record type isn't recognised.
    pub fn sign_rrset_with_zone_ksk(
        &self,
        zone_id: &str,
        owner_name: &str,
        record_type: &str,
        ttl: u32,
        rdata_values: &[String],
    ) -> Option<DnssecSignature> {
        let zone_id_clean = strip_zone_prefix(zone_id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT)?;
        let zone = account.hosted_zones.get(&zone_id_clean)?;
        let zone_name = zone.name.clone();
        let ksk = account
            .key_signing_keys
            .values()
            .filter(|k| {
                k.hosted_zone_id == zone_id_clean && k.status.eq_ignore_ascii_case("ACTIVE")
            })
            .min_by(|a, b| a.name.cmp(&b.name))
            .cloned()?;
        drop(state);
        let rtype_code = crate::dnssec::type_code(record_type)?;
        let normalized_owner = if owner_name.ends_with('.') {
            owner_name.to_string()
        } else {
            format!("{owner_name}.")
        };
        let rdatas: Vec<Vec<u8>> = rdata_values
            .iter()
            .map(|v| crate::dnssec::encode_rdata(record_type, v))
            .collect();
        let canonical = crate::dnssec::canonical_rrset_bytes(
            &normalized_owner,
            rtype_code,
            crate::dnssec::CLASS_IN,
            ttl,
            &rdatas,
        );
        let now = Utc::now().timestamp() as u32;
        let inception = now;
        let expiration = now.saturating_add(30 * 24 * 60 * 60);
        let labels = crate::dnssec::label_count(&normalized_owner);
        let header = crate::dnssec::RrsigHeader {
            rtype: rtype_code,
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            labels,
            original_ttl: ttl,
            sig_expiration: expiration,
            sig_inception: inception,
            key_tag: ksk.key_tag as u16,
            signer_name: &zone_name,
        };
        let signed = crate::dnssec::rrsig_signed_data(&header, &canonical);
        let sig = crate::dnssec::sign_with_pkcs8_pem(&ksk.private_key_pem, &signed);
        Some(DnssecSignature {
            signature_b64: crate::dnssec::b64(&sig),
            algorithm: crate::dnssec::DNSSEC_ALGORITHM,
            key_tag: ksk.key_tag as u16,
            signer_name: zone_name,
            inception,
            expiration,
            labels,
            original_ttl: ttl,
            rrset_type: record_type.to_string(),
        })
    }

    pub(super) fn get_dnssec(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        let status = account
            .dnssec_status
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(|| "NOT_SIGNING".to_string());
        let ksks: Vec<StoredKeySigningKey> = account
            .key_signing_keys
            .values()
            .filter(|k| k.hosted_zone_id == zone_id)
            .cloned()
            .collect();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetDNSSECResponse xmlns=\"{NS}\">"));
        body.push_str("<Status>");
        body.push_str(&format!(
            "<ServeSignature>{}</ServeSignature>",
            esc(&status)
        ));
        body.push_str("</Status>");
        body.push_str("<KeySigningKeys>");
        for k in &ksks {
            // KeySigningKeys list members lack `xmlName`, so the AWS SDK
            // expects the default `<member>` element name.
            body.push_str("<member>");
            push_key_signing_key_inner(&mut body, k);
            body.push_str("</member>");
        }
        body.push_str("</KeySigningKeys>");
        body.push_str("</GetDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn enable_hosted_zone_dnssec(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        // AWS refuses to enable signing until the zone has at least one
        // ACTIVE key-signing key.
        let has_active_ksk = account
            .key_signing_keys
            .values()
            .any(|k| k.hosted_zone_id == zone_id && k.status.eq_ignore_ascii_case("ACTIVE"));
        if !has_active_ksk {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "KeySigningKeyWithActiveStatusNotFound",
                format!("No ACTIVE key-signing key found for hosted zone {zone_id}"),
            ));
        }
        account
            .dnssec_status
            .insert(zone_id.clone(), "SIGNING".to_string());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("EnableHostedZoneDNSSEC {}", zone_id)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<EnableHostedZoneDNSSECResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</EnableHostedZoneDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn disable_hosted_zone_dnssec(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = strip_zone_prefix(&require_id(route)?);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        account
            .dnssec_status
            .insert(zone_id.clone(), "NOT_SIGNING".to_string());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DisableHostedZoneDNSSEC {}", zone_id)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DisableHostedZoneDNSSECResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DisableHostedZoneDNSSECResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn create_key_signing_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateKeySigningKeyRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateKeySigningKeyRequest XML: {e}"))
        })?;
        if cfg.caller_reference.is_empty()
            || cfg.hosted_zone_id.is_empty()
            || cfg.key_management_service_arn.is_empty()
            || cfg.name.is_empty()
            || cfg.status.is_empty()
        {
            return Err(invalid_argument(
                "CallerReference, HostedZoneId, KeyManagementServiceArn, Name, Status all required",
            ));
        }
        let zone_id = strip_zone_prefix(&cfg.hosted_zone_id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        // Real Route 53 enforces unique KSK Name per zone and unique KMS ARN per zone.
        if account
            .key_signing_keys
            .contains_key(&(zone_id.clone(), cfg.name.clone()))
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "KeySigningKeyAlreadyExists",
                format!(
                    "A key-signing key named '{}' already exists in zone {}",
                    cfg.name, zone_id
                ),
            ));
        }
        let now = Utc::now();
        // Derive a deterministic ECDSA P-256 keypair so persistence reloads
        // the same DNSKEY/DS material, then compute the standard DNSSEC
        // key tag and DS digest for the parent zone to publish.
        let key_material = crate::dnssec::derive_keypair(&zone_id, &cfg.name);
        let key_tag = crate::dnssec::key_tag_for(&key_material.dnskey_public_key);
        let zone_name = account
            .hosted_zones
            .get(&zone_id)
            .map(|z| z.name.clone())
            .unwrap_or_else(|| ".".to_string());
        let ds_digest_hex =
            crate::dnssec::ds_digest_sha256(&zone_name, key_tag, &key_material.dnskey_public_key);
        let ksk = StoredKeySigningKey {
            hosted_zone_id: zone_id.clone(),
            name: cfg.name.clone(),
            kms_arn: cfg.key_management_service_arn,
            status: cfg.status,
            caller_reference: cfg.caller_reference,
            created_date: now,
            last_modified_date: now,
            key_tag: key_tag as i32,
            private_key_pem: key_material.private_key_pem,
            public_key_der: key_material.public_key_der,
            ds_digest_hex,
        };
        account
            .key_signing_keys
            .insert((zone_id.clone(), cfg.name.clone()), ksk.clone());
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: Some(format!("CreateKeySigningKey {}/{}", zone_id, cfg.name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateKeySigningKeyResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("<KeySigningKey>");
        push_key_signing_key_inner(&mut body, &ksk);
        body.push_str("</KeySigningKey>");
        body.push_str("</CreateKeySigningKeyResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/keysigningkey/{}/{}",
            zone_id, ksk.name
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn delete_key_signing_key(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (zone_id, name) = require_zone_and_name(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        let ksk = account
            .key_signing_keys
            .get(&(zone_id.clone(), name.clone()))
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        // Real Route 53 requires Status == INACTIVE before delete.
        // ACTION_NEEDED / DELETING / other transient states are also
        // rejected — only INACTIVE is OK.
        if !ksk.status.eq_ignore_ascii_case("INACTIVE") {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeySigningKeyStatus",
                format!(
                    "KeySigningKey {}/{} must be INACTIVE before deletion (status={})",
                    zone_id, name, ksk.status
                ),
            ));
        }
        account
            .key_signing_keys
            .remove(&(zone_id.clone(), name.clone()));
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DeleteKeySigningKey {}/{}", zone_id, name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteKeySigningKeyResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DeleteKeySigningKeyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn activate_key_signing_key(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.set_ksk_status(route, "ACTIVE", "ActivateKeySigningKey")
    }

    pub(super) fn deactivate_key_signing_key(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.set_ksk_status(route, "INACTIVE", "DeactivateKeySigningKey")
    }

    pub(super) fn set_ksk_status(
        &self,
        route: &Route,
        status: &str,
        op: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (zone_id, name) = require_zone_and_name(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        let ksk = account
            .key_signing_keys
            .get_mut(&(zone_id.clone(), name.clone()))
            .ok_or_else(|| no_such_key_signing_key(&zone_id, &name))?;
        ksk.status = status.to_string();
        ksk.last_modified_date = Utc::now();
        let change = StoredChange {
            id: generate_change_id(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("{} {}/{}", op, zone_id, name)),
            read_count: 0,
        };
        account.changes.insert(change.id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<{op}Response xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str(&format!("</{op}Response>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
