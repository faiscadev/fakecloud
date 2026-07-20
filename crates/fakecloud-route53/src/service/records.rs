//! Route53 `records` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn change_resource_record_sets(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: ChangeResourceRecordSetsRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid ChangeResourceRecordSetsRequest XML: {e}"))
            })?;
        if cfg.change_batch.changes.change.is_empty() {
            return Err(invalid_argument("ChangeBatch.Changes is empty"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        // AWS applies a ChangeBatch atomically: either every change succeeds
        // or none do. Stage the mutations against a clone first; only swap
        // the live record set in once every action validates.
        let mut working = zone.resource_record_sets.clone();
        for ch in &cfg.change_batch.changes.change {
            let action = ch.action.to_uppercase();
            let rec = normalize_rrset(&ch.resource_record_set);
            match action.as_str() {
                "CREATE" => {
                    validate_rrset_in_zone(&rec, &zone.name)?;
                    if working.iter().any(|r| rrset_matches(r, &rec)) {
                        return Err(invalid_change_batch(format!(
                            "Tried to create resource record set [name='{}', type='{}'] but it already exists",
                            rec.name, rec.record_type
                        )));
                    }
                    working.push(rec);
                }
                "UPSERT" => {
                    validate_rrset_in_zone(&rec, &zone.name)?;
                    let pos = working.iter().position(|r| rrset_matches(r, &rec));
                    if let Some(p) = pos {
                        working[p] = rec;
                    } else {
                        working.push(rec);
                    }
                }
                "DELETE" => {
                    let pos = working.iter().position(|r| rrset_matches(r, &rec));
                    let p = pos.ok_or_else(|| {
                        invalid_change_batch(format!(
                            "Tried to delete resource record set [name='{}', type='{}'] but it was not found",
                            rec.name, rec.record_type
                        ))
                    })?;
                    if is_default_record(&working[p], &zone.name) {
                        return Err(invalid_change_batch(
                            "Cannot delete default SOA or NS record",
                        ));
                    }
                    // Route 53 requires a DELETE to submit the record set's
                    // current values (and TTL) exactly, not just a matching
                    // name/type/set-identifier.
                    if !rrset_values_match(&working[p], &rec) {
                        return Err(invalid_change_batch(format!(
                            "Tried to delete resource record set [name='{}', type='{}'] but the values provided do not match the current values",
                            rec.name, rec.record_type
                        )));
                    }
                    working.remove(p);
                }
                other => {
                    return Err(invalid_change_batch(format!(
                        "Unknown change action: {other}"
                    )));
                }
            }
        }
        zone.resource_record_sets = working;
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: cfg.change_batch.comment,
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ChangeResourceRecordSetsResponse xmlns=\"{NS}\">"
        ));
        push_change_info(&mut body, &change);
        body.push_str("</ChangeResourceRecordSetsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_resource_record_sets(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let state = self.state.read();
        let zone = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);

        // Route 53 orders record sets by reversed-label DNS name
        // (`www.example.com.` sorts under `com.example.www`) then by record
        // type, and paginates with maxitems + the StartRecordName/Type/
        // Identifier cursor. Names, record types, and set identifiers are
        // XML-safe DNS/enum values.
        let max_items = req
            .query_params
            .get("maxitems")
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(100);
        let start_name = req.query_params.get("name").map(|s| s.to_ascii_lowercase());
        let start_type = req.query_params.get("type").cloned();
        let start_ident = req.query_params.get("identifier").cloned();
        // Route 53 rejects StartRecordType supplied without StartRecordName.
        if start_type.is_some() && start_name.is_none() {
            return Err(invalid_argument(
                "The input is not valid: StartRecordName must be specified when StartRecordType is specified",
            ));
        }

        let mut sorted: Vec<&crate::model::ResourceRecordSet> =
            zone.resource_record_sets.iter().collect();
        sorted.sort_by(|a, b| {
            reverse_dns_key(&a.name)
                .cmp(&reverse_dns_key(&b.name))
                .then(a.record_type.cmp(&b.record_type))
                .then(a.set_identifier.cmp(&b.set_identifier))
        });

        let start_idx = match &start_name {
            None => 0,
            Some(sn) => {
                let start_key = reverse_dns_key(sn);
                sorted
                    .iter()
                    .position(|r| match reverse_dns_key(&r.name).cmp(&start_key) {
                        std::cmp::Ordering::Greater => true,
                        std::cmp::Ordering::Less => false,
                        std::cmp::Ordering::Equal => match &start_type {
                            None => true,
                            Some(st) => match r.record_type.cmp(st) {
                                std::cmp::Ordering::Greater => true,
                                std::cmp::Ordering::Less => false,
                                std::cmp::Ordering::Equal => {
                                    start_ident.as_deref().is_none_or(|si| {
                                        r.set_identifier.as_deref().unwrap_or("") >= si
                                    })
                                }
                            },
                        },
                    })
                    .unwrap_or(sorted.len())
            }
        };

        let page: Vec<&crate::model::ResourceRecordSet> = sorted
            .iter()
            .skip(start_idx)
            .take(max_items)
            .copied()
            .collect();
        let next = sorted.get(start_idx + page.len()).copied();

        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListResourceRecordSetsResponse xmlns=\"{NS}\">"));
        body.push_str("<ResourceRecordSets>");
        for r in &page {
            push_rrset(&mut body, r);
        }
        body.push_str("</ResourceRecordSets>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", next.is_some()));
        if let Some(n) = next {
            body.push_str(&format!(
                "<NextRecordName>{}</NextRecordName>",
                esc(&n.name)
            ));
            body.push_str(&format!(
                "<NextRecordType>{}</NextRecordType>",
                esc(&n.record_type)
            ));
            if let Some(si) = &n.set_identifier {
                body.push_str(&format!(
                    "<NextRecordIdentifier>{}</NextRecordIdentifier>",
                    esc(si)
                ));
            }
        }
        body.push_str(&format!("<MaxItems>{max_items}</MaxItems>"));
        body.push_str("</ListResourceRecordSetsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_change(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        // Mirror real Route 53's eventual-consistency window. Two
        // signals flip a change from PENDING to INSYNC, whichever
        // fires first:
        //   * Wall-clock age >= `PROPAGATION_AGE_SECS` since
        //     `submitted_at`. Real AWS converges in ~60s; we use a
        //     short window (default 1s, override via
        //     `FAKECLOUD_ROUTE53_PROPAGATION_SECS`) so polling tests
        //     observe the transition without dragging out wall-clock
        //     time.
        //   * `PROPAGATION_READS` GetChange polls. Lets tests that
        //     can't sleep (sync drivers, deterministic test runners)
        //     still drive the transition by polling.
        const PROPAGATION_READS: u32 = 5;
        let propagation_secs = std::env::var("FAKECLOUD_ROUTE53_PROPAGATION_SECS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(1);
        let change = {
            let mut state = self.state.write();
            let account = state.accounts.get_mut(DEFAULT_ACCOUNT).ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchChange",
                    format!("Change {} not found", id),
                )
            })?;
            let stored = account.changes.get_mut(&id).ok_or_else(|| {
                aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchChange",
                    format!("Change {} not found", id),
                )
            })?;
            stored.read_count = stored.read_count.saturating_add(1);
            let age_secs = (Utc::now() - stored.submitted_at).num_seconds();
            if stored.status == "PENDING"
                && (age_secs >= propagation_secs || stored.read_count >= PROPAGATION_READS)
            {
                stored.status = "INSYNC".to_string();
            }
            stored.clone()
        };
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetChangeResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</GetChangeResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn test_dns_answer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = req
            .query_params
            .get("hostedzoneid")
            .cloned()
            .ok_or_else(|| invalid_argument("hostedzoneid query parameter is required"))?;
        let record_name = req
            .query_params
            .get("recordname")
            .cloned()
            .ok_or_else(|| invalid_argument("recordname query parameter is required"))?;
        let record_type = req
            .query_params
            .get("recordtype")
            .cloned()
            .ok_or_else(|| invalid_argument("recordtype query parameter is required"))?;
        let resolver_ip = req
            .query_params
            .get("resolverip")
            .cloned()
            .unwrap_or_else(|| "8.8.8.8".to_string());
        let edns0_subnet = req.query_params.get("edns0clientsubnetip").cloned();
        let dnssec_requested = req
            .query_params
            .get("dnssec")
            .map(|v| matches!(v.as_str(), "true" | "1"))
            .unwrap_or(false);
        let zone_id = strip_zone_prefix(&zone_id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let zone = account
            .and_then(|a| a.hosted_zones.get(&zone_id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        let health_checks = account.map(|a| a.health_checks.clone()).unwrap_or_default();
        // Capture DNSSEC + query-logging context while we still hold
        // the read guard so we can drop it before doing any work that
        // re-enters state (or cross-calls into fakecloud-logs).
        let dnssec_signing = account
            .and_then(|a| a.dnssec_status.get(&zone_id).cloned())
            .map(|s| s == "SIGNING")
            .unwrap_or(false);
        let active_ksk: Option<StoredKeySigningKey> = account.and_then(|a| {
            a.key_signing_keys
                .values()
                .filter(|k| k.hosted_zone_id == zone_id && k.status.eq_ignore_ascii_case("ACTIVE"))
                .min_by(|a, b| a.name.cmp(&b.name))
                .cloned()
        });
        let query_log_arn = account.and_then(|a| {
            a.query_logging_configs
                .values()
                .find(|c| c.hosted_zone_id == zone_id)
                .map(|c| c.cloud_watch_logs_log_group_arn.clone())
        });
        drop(state);
        let normalized_name = if record_name.ends_with('.') {
            record_name.clone()
        } else {
            format!("{record_name}.")
        };

        let candidates: Vec<&crate::model::ResourceRecordSet> = zone
            .resource_record_sets
            .iter()
            .filter(|r| r.name == normalized_name && r.record_type == record_type)
            .collect();

        let original_ttl = candidates.iter().filter_map(|c| c.ttl).min().unwrap_or(300) as u32;

        let alias_lookup = AliasLookup {
            elbv2: self.elbv2_state.as_ref(),
            cloudfront: self.cloudfront_state.as_ref(),
            s3: self.s3_state.as_ref(),
        };
        let answers: Vec<String> = if candidates.is_empty() {
            Vec::new()
        } else {
            resolve_routing_policy(
                &candidates,
                &health_checks,
                edns0_subnet.as_deref(),
                &alias_lookup,
            )
        };

        // Compute RRSIG when DNSSEC is on and the caller asked for it
        // (via `?dnssec=true`). Real Route 53's TestDNSAnswer doesn't
        // include RRSIGs by default, so gating behind the flag keeps
        // existing test fixtures stable.
        let rrsig_b64 = if dnssec_requested && dnssec_signing && !answers.is_empty() {
            active_ksk.as_ref().and_then(|ksk| {
                self.compute_rrsig_for_answers(
                    &zone.name,
                    &normalized_name,
                    &record_type,
                    original_ttl,
                    &answers,
                    ksk,
                )
            })
        } else {
            None
        };

        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<TestDNSAnswerResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<Nameserver>{}</Nameserver>", esc(&resolver_ip)));
        body.push_str(&format!("<RecordName>{}</RecordName>", esc(&record_name)));
        body.push_str(&format!("<RecordType>{}</RecordType>", esc(&record_type)));
        body.push_str("<RecordData>");
        for v in &answers {
            body.push_str(&format!("<RecordDataEntry>{}</RecordDataEntry>", esc(v)));
        }
        body.push_str("</RecordData>");
        body.push_str("<ResponseCode>NOERROR</ResponseCode>");
        body.push_str(&format!(
            "<Protocol>{}</Protocol>",
            if edns0_subnet.is_some() {
                "EDNS0"
            } else {
                "UDP"
            }
        ));
        // Fakecloud extension: surface DNSSEC RRSIG bytes when the
        // caller asked for them. Real Route 53 doesn't expose this
        // field; it sits inside the namespace under
        // `<DnssecSignatures>` so AWS SDKs that don't model it just
        // ignore it.
        if let Some(sig) = &rrsig_b64 {
            body.push_str("<DnssecSignatures>");
            body.push_str(&format!(
                "<Algorithm>{}</Algorithm>",
                crate::dnssec::DNSSEC_ALGORITHM
            ));
            if let Some(ksk) = &active_ksk {
                body.push_str(&format!("<KeyTag>{}</KeyTag>", ksk.key_tag));
            }
            body.push_str(&format!("<Signature>{}</Signature>", esc(sig)));
            body.push_str("</DnssecSignatures>");
        }
        body.push_str("</TestDNSAnswerResponse>");

        // Query log delivery — best-effort. If logs state isn't wired
        // (unit-test harness, persistence-only build) we silently
        // skip; a real server always wires it via `with_logs`.
        if let (Some(logs_state), Some(arn)) = (&self.logs_state, query_log_arn) {
            if let Some((account_id, region, group_name)) = parse_log_group_arn(&arn) {
                let response_code = if answers.is_empty() {
                    "NXDOMAIN"
                } else {
                    "NOERROR"
                };
                let protocol = if edns0_subnet.is_some() {
                    "EDNS0"
                } else {
                    "UDP"
                };
                // Real Route 53 query log format (space-separated):
                //   <version> <ts> <zone_id> <name> <type> <rcode>
                //   <protocol> <edge_location> <client_ip> <ecs_subnet>
                let now = Utc::now();
                let line = format!(
                    "1.0 {ts} {zone} {name} {rtype} {rcode} {proto} FAKECLOUD {client} {ecs}",
                    ts = now.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    zone = zone_id,
                    name = normalized_name,
                    rtype = record_type,
                    rcode = response_code,
                    proto = protocol,
                    client = resolver_ip,
                    ecs = edns0_subnet.unwrap_or_else(|| "-".to_string()),
                );
                let stream_name = format!("FAKECLOUD/{}", now.format("%Y/%m/%d"));
                fakecloud_logs::ingest::append_events(
                    logs_state,
                    &account_id,
                    &region,
                    &group_name,
                    &stream_name,
                    &[fakecloud_logs::ingest::IngestEvent {
                        timestamp_ms: now.timestamp_millis(),
                        message: line,
                    }],
                );
            }
        }

        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
