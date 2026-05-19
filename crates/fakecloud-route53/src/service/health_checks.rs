//! Route53 `health_checks` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn create_health_check(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateHealthCheckRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateHealthCheckRequest XML: {e}")))?;
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        if cfg.health_check_config.health_check_type.is_empty() {
            return Err(invalid_argument("HealthCheckConfig.Type is required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(existing) = account
            .health_checks
            .values()
            .find(|h| h.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "HealthCheckAlreadyExists",
                format!(
                    "A health check with the same CallerReference already exists: {} (id={})",
                    cfg.caller_reference, existing.id
                ),
            ));
        }
        let id = generate_health_check_id();
        let stored = StoredHealthCheck {
            id: id.clone(),
            caller_reference: cfg.caller_reference,
            version: 1,
            config: cfg.health_check_config,
            created_time: Utc::now(),
            status: HealthCheckStatus::Success,
            last_failure_reason: None,
        };
        account.health_checks.insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &stored);
        body.push_str("</CreateHealthCheckResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/healthcheck/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn get_health_check(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &hc);
        body.push_str("</GetHealthCheckResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn update_health_check(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: UpdateHealthCheckRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid UpdateHealthCheckRequest XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_health_check(&id))?;
        let hc = account
            .health_checks
            .get_mut(&id)
            .ok_or_else(|| no_such_health_check(&id))?;
        if let Some(client_version) = cfg.health_check_version {
            if client_version != hc.version {
                return Err(aws_error(
                    StatusCode::CONFLICT,
                    "HealthCheckVersionMismatch",
                    format!(
                        "Provided HealthCheckVersion ({}) does not match the current version ({})",
                        client_version, hc.version
                    ),
                ));
            }
        }
        if let Some(v) = cfg.ip_address {
            hc.config.ip_address = Some(v);
        }
        if let Some(v) = cfg.port {
            hc.config.port = Some(v);
        }
        if let Some(v) = cfg.resource_path {
            hc.config.resource_path = Some(v);
        }
        if let Some(v) = cfg.fully_qualified_domain_name {
            hc.config.fully_qualified_domain_name = Some(v);
        }
        if let Some(v) = cfg.search_string {
            hc.config.search_string = Some(v);
        }
        if let Some(v) = cfg.failure_threshold {
            hc.config.failure_threshold = Some(v);
        }
        if let Some(v) = cfg.inverted {
            hc.config.inverted = Some(v);
        }
        if let Some(v) = cfg.disabled {
            hc.config.disabled = Some(v);
        }
        if let Some(v) = cfg.health_threshold {
            hc.config.health_threshold = Some(v);
        }
        if let Some(v) = cfg.child_health_checks {
            hc.config.child_health_checks = Some(v);
        }
        if let Some(v) = cfg.enable_sni {
            hc.config.enable_sni = Some(v);
        }
        if let Some(v) = cfg.regions {
            hc.config.regions = Some(v);
        }
        if let Some(v) = cfg.alarm_identifier {
            hc.config.alarm_identifier = Some(v);
        }
        if let Some(v) = cfg.insufficient_data_health_status {
            hc.config.insufficient_data_health_status = Some(v);
        }
        if let Some(reset) = cfg.reset_elements {
            for name in reset.resettable_element_name {
                match name.as_str() {
                    "ChildHealthChecks" => hc.config.child_health_checks = None,
                    "FullyQualifiedDomainName" => hc.config.fully_qualified_domain_name = None,
                    "Regions" => hc.config.regions = None,
                    "ResourcePath" => hc.config.resource_path = None,
                    _ => {}
                }
            }
        }
        hc.version += 1;
        let snap = hc.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<UpdateHealthCheckResponse xmlns=\"{NS}\">"));
        push_health_check(&mut body, &snap);
        body.push_str("</UpdateHealthCheckResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_health_check(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_health_check(&id))?;
        if !account.health_checks.contains_key(&id) {
            return Err(no_such_health_check(&id));
        }
        // Real Route 53 returns HealthCheckInUse if any record set still
        // references the health check. Mirror that across all hosted zones.
        for zone in account.hosted_zones.values() {
            for rrset in &zone.resource_record_sets {
                if rrset.health_check_id.as_deref() == Some(id.as_str()) {
                    return Err(aws_error(
                        StatusCode::BAD_REQUEST,
                        "HealthCheckInUse",
                        format!(
                            "Health check {} is in use by record set {} ({}) in zone {}",
                            id, rrset.name, rrset.record_type, zone.id
                        ),
                    ));
                }
            }
        }
        account.health_checks.remove(&id);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteHealthCheckResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_health_checks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "marker",
                    min: 0,
                    max: 64,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let marker = req.query_params.get("marker").cloned();
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut hcs: Vec<StoredHealthCheck> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.health_checks.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        hcs.sort_by(|a, b| a.id.cmp(&b.id));
        let start = match &marker {
            Some(m) => hcs
                .iter()
                .position(|h| h.id.as_str() >= m.as_str())
                .unwrap_or(hcs.len()),
            None => 0,
        };
        let slice: Vec<StoredHealthCheck> =
            hcs.iter().skip(start).take(max_items).cloned().collect();
        let next_marker = if start + slice.len() < hcs.len() {
            Some(hcs[start + slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHealthChecksResponse xmlns=\"{NS}\">"));
        body.push_str("<HealthChecks>");
        for hc in &slice {
            push_health_check(&mut body, hc);
        }
        body.push_str("</HealthChecks>");
        if let Some(m) = &marker {
            body.push_str(&format!("<Marker>{}</Marker>", esc(m)));
        } else {
            body.push_str("<Marker></Marker>");
        }
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        if let Some(nm) = &next_marker {
            body.push_str(&format!("<NextMarker>{}</NextMarker>", esc(nm)));
        }
        body.push_str("</ListHealthChecksResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_health_check_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.health_checks.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckCountResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<HealthCheckCount>{}</HealthCheckCount>", count));
        body.push_str("</GetHealthCheckCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_health_check_status(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let status_text = render_status_line(hc.status, hc.last_failure_reason.as_deref());
        let now = rfc3339(&Utc::now());
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHealthCheckStatusResponse xmlns=\"{NS}\">"));
        body.push_str("<HealthCheckObservations>");
        for region in checker_regions() {
            body.push_str("<HealthCheckObservation>");
            body.push_str(&format!("<Region>{}</Region>", esc(region)));
            body.push_str(&format!(
                "<IPAddress>{}</IPAddress>",
                esc(&checker_ip_for_region(region))
            ));
            body.push_str("<StatusReport>");
            body.push_str(&format!("<Status>{}</Status>", esc(&status_text)));
            body.push_str(&format!("<CheckedTime>{}</CheckedTime>", now));
            body.push_str("</StatusReport>");
            body.push_str("</HealthCheckObservation>");
        }
        body.push_str("</HealthCheckObservations>");
        body.push_str("</GetHealthCheckStatusResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    /// Admin: flip a health check's status and optional last-failure
    /// reason. Powers the
    /// `POST /_fakecloud/route53/health-checks/{id}/status` endpoint
    /// in fakecloud-server. Returns `false` if the id doesn't exist.
    /// When `status = Success`, `last_failure_reason` is ignored and
    /// any prior reason is preserved so a later read of
    /// `GetHealthCheckLastFailureReason` still surfaces the historical
    /// observation. When `status` is one of the failure-flavoured
    /// variants (`Failure`, `Timeout`, `DnsError`) and
    /// `last_failure_reason` is `Some`, the stored reason is
    /// overwritten; `None` leaves the prior reason untouched.
    pub fn set_health_check_status(
        &self,
        id: &str,
        status: HealthCheckStatus,
        last_failure_reason: Option<String>,
    ) -> bool {
        let mut state = self.state.write();
        let Some(account) = state.accounts.get_mut(DEFAULT_ACCOUNT) else {
            return false;
        };
        let Some(hc) = account.health_checks.get_mut(id) else {
            return false;
        };
        hc.status = status;
        let is_failure_flavoured = matches!(
            status,
            HealthCheckStatus::Failure | HealthCheckStatus::Timeout | HealthCheckStatus::DnsError
        );
        if is_failure_flavoured && last_failure_reason.is_some() {
            hc.last_failure_reason = last_failure_reason;
        }
        true
    }

    pub(super) fn get_health_check_last_failure_reason(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let hc = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.health_checks.get(&id).cloned())
            .ok_or_else(|| no_such_health_check(&id))?;
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetHealthCheckLastFailureReasonResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<HealthCheckObservations>");
        if let Some(reason) = hc.last_failure_reason.as_deref() {
            let now = rfc3339(&Utc::now());
            for region in checker_regions() {
                body.push_str("<HealthCheckObservation>");
                body.push_str(&format!("<Region>{}</Region>", esc(region)));
                body.push_str(&format!(
                    "<IPAddress>{}</IPAddress>",
                    esc(&checker_ip_for_region(region))
                ));
                body.push_str("<StatusReport>");
                body.push_str(&format!("<Status>{}</Status>", esc(reason)));
                body.push_str(&format!("<CheckedTime>{}</CheckedTime>", now));
                body.push_str("</StatusReport>");
                body.push_str("</HealthCheckObservation>");
            }
        }
        body.push_str("</HealthCheckObservations>");
        body.push_str("</GetHealthCheckLastFailureReasonResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_checker_ip_ranges(&self) -> Result<AwsResponse, AwsServiceError> {
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetCheckerIpRangesResponse xmlns=\"{NS}\">"));
        body.push_str("<CheckerIpRanges>");
        for cidr in CHECKER_IP_RANGES {
            body.push_str(&format!("<member>{}</member>", esc(cidr)));
        }
        body.push_str("</CheckerIpRanges>");
        body.push_str("</GetCheckerIpRangesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
