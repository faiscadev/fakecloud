//! Route53 `traffic_policies` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn create_traffic_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateTrafficPolicyRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateTrafficPolicyRequest XML: {e}"))
        })?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        if cfg.document.is_empty() {
            return Err(invalid_argument("Document is required"));
        }
        let policy_type = infer_policy_type(&cfg.document);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        // Match real Route 53: name uniqueness applies across all versions
        // of every existing policy. Checking only version == 1 would let a
        // duplicate name slip through whenever the v1 row had been deleted
        // but later versions remained.
        if account
            .traffic_policies
            .values()
            .any(|p| p.name == cfg.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "TrafficPolicyAlreadyExists",
                format!("A traffic policy named '{}' already exists", cfg.name),
            ));
        }
        let id = generate_traffic_policy_id();
        let stored = StoredTrafficPolicy {
            id: id.clone(),
            version: 1,
            name: cfg.name,
            policy_type,
            document: cfg.document,
            comment: cfg.comment,
            created_time: Utc::now(),
        };
        account
            .traffic_policies
            .insert((id.clone(), 1), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateTrafficPolicyResponse xmlns=\"{NS}\">"));
        push_traffic_policy(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/trafficpolicy/{}/{}",
            stored.id, stored.version
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn create_traffic_policy_version(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: CreateTrafficPolicyVersionRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid CreateTrafficPolicyVersionRequest XML: {e}"
                ))
            })?;
        if cfg.document.is_empty() {
            return Err(invalid_argument("Document is required"));
        }
        let policy_type = infer_policy_type(&cfg.document);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        let existing_versions: Vec<i64> = account
            .traffic_policies
            .keys()
            .filter(|(pid, _)| pid == &id)
            .map(|(_, v)| *v)
            .collect();
        if existing_versions.is_empty() {
            return Err(no_such_traffic_policy(&id));
        }
        let next_version = existing_versions.iter().max().copied().unwrap_or(0) + 1;
        // Borrow name from the latest existing version so the new one stays consistent.
        let (name, original_comment) = {
            let latest_v = existing_versions.iter().max().copied().unwrap();
            let p = account
                .traffic_policies
                .get(&(id.clone(), latest_v))
                .unwrap();
            (p.name.clone(), p.comment.clone())
        };
        let stored = StoredTrafficPolicy {
            id: id.clone(),
            version: next_version,
            name,
            policy_type,
            document: cfg.document,
            comment: cfg.comment.or(original_comment),
            created_time: Utc::now(),
        };
        account
            .traffic_policies
            .insert((id.clone(), next_version), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateTrafficPolicyVersionResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyVersionResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!(
            "/2013-04-01/trafficpolicy/{}/{}",
            stored.id, stored.version
        )) {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn get_traffic_policy(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let state = self.state.read();
        let p = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.traffic_policies.get(&(id.clone(), version)).cloned())
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetTrafficPolicyResponse xmlns=\"{NS}\">"));
        push_traffic_policy(&mut body, &p);
        body.push_str("</GetTrafficPolicyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn update_traffic_policy_comment(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let cfg: UpdateTrafficPolicyCommentRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid UpdateTrafficPolicyCommentRequest XML: {e}"
                ))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        let p = account
            .traffic_policies
            .get_mut(&(id.clone(), version))
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        p.comment = Some(cfg.comment);
        let snap = p.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateTrafficPolicyCommentResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy(&mut body, &snap);
        body.push_str("</UpdateTrafficPolicyCommentResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_traffic_policy(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let version = require_version(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy(&id))?;
        if !account
            .traffic_policies
            .contains_key(&(id.clone(), version))
        {
            return Err(no_such_traffic_policy(&id));
        }
        // TrafficPolicyInUse if any instance still references this (id, version).
        if account
            .traffic_policy_instances
            .values()
            .any(|i| i.traffic_policy_id == id && i.traffic_policy_version == version)
        {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "TrafficPolicyInUse",
                format!("Traffic policy {}/{} is in use by an instance", id, version),
            ));
        }
        account.traffic_policies.remove(&(id, version));
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteTrafficPolicyResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_traffic_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "trafficpolicyid",
                    min: 1,
                    max: 36,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let marker = req.query_params.get("trafficpolicyid").cloned();
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        // Group by policy id; emit only the latest version of each.
        let mut latest: BTreeMap<String, StoredTrafficPolicy> = BTreeMap::new();
        let mut counts: BTreeMap<String, i64> = BTreeMap::new();
        if let Some(account) = state.accounts.get(DEFAULT_ACCOUNT) {
            for p in account.traffic_policies.values() {
                let entry = latest.entry(p.id.clone()).or_insert_with(|| p.clone());
                if p.version > entry.version {
                    *entry = p.clone();
                }
                *counts.entry(p.id.clone()).or_insert(0) += 1;
            }
        }
        drop(state);
        let mut summaries: Vec<StoredTrafficPolicy> = latest.into_values().collect();
        summaries.sort_by(|a, b| a.id.cmp(&b.id));
        let start = match &marker {
            Some(m) => summaries
                .iter()
                .position(|p| p.id.as_str() >= m.as_str())
                .unwrap_or(summaries.len()),
            None => 0,
        };
        let slice: Vec<&StoredTrafficPolicy> =
            summaries.iter().skip(start).take(max_items).collect();
        let next_marker = if start + slice.len() < summaries.len() {
            Some(summaries[start + slice.len()].id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListTrafficPoliciesResponse xmlns=\"{NS}\">"));
        body.push_str("<TrafficPolicySummaries>");
        for p in &slice {
            push_traffic_policy_summary(&mut body, p, counts.get(&p.id).copied().unwrap_or(1));
        }
        body.push_str("</TrafficPolicySummaries>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = &next_marker {
            body.push_str(&format!(
                "<TrafficPolicyIdMarker>{}</TrafficPolicyIdMarker>",
                esc(nm)
            ));
        }
        body.push_str("</ListTrafficPoliciesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_traffic_policy_versions(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let marker: Option<i64> = req
            .query_params
            .get("trafficpolicyversion")
            .and_then(|s| s.parse().ok());
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut versions: Vec<StoredTrafficPolicy> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| {
                a.traffic_policies
                    .values()
                    .filter(|p| p.id == id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        if versions.is_empty() {
            return Err(no_such_traffic_policy(&id));
        }
        versions.sort_by_key(|p| p.version);
        let start = match marker {
            Some(m) => versions
                .iter()
                .position(|p| p.version >= m)
                .unwrap_or(versions.len()),
            None => 0,
        };
        let slice: Vec<&StoredTrafficPolicy> =
            versions.iter().skip(start).take(max_items).collect();
        let next_marker = if start + slice.len() < versions.len() {
            Some(versions[start + slice.len()].version)
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyVersionsResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicies>");
        for p in &slice {
            push_traffic_policy(&mut body, p);
        }
        body.push_str("</TrafficPolicies>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = next_marker {
            body.push_str(&format!(
                "<TrafficPolicyVersionMarker>{}</TrafficPolicyVersionMarker>",
                nm
            ));
        } else {
            body.push_str("<TrafficPolicyVersionMarker></TrafficPolicyVersionMarker>");
        }
        body.push_str("</ListTrafficPolicyVersionsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn create_traffic_policy_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateTrafficPolicyInstanceRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid CreateTrafficPolicyInstanceRequest XML: {e}"
                ))
            })?;
        if cfg.hosted_zone_id.is_empty() || cfg.name.is_empty() || cfg.traffic_policy_id.is_empty()
        {
            return Err(invalid_argument(
                "HostedZoneId, Name, and TrafficPolicyId are required",
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
        let policy = account
            .traffic_policies
            .get(&(cfg.traffic_policy_id.clone(), cfg.traffic_policy_version))
            .cloned()
            .ok_or_else(|| no_such_traffic_policy(&cfg.traffic_policy_id))?;
        let mut name = cfg.name.clone();
        if !name.ends_with('.') {
            name.push('.');
        }
        // Real Route 53 rejects a duplicate (HostedZoneId, Name, Type) instance
        // with TrafficPolicyInstanceAlreadyExists. Mirror that.
        if account.traffic_policy_instances.values().any(|i| {
            i.hosted_zone_id == zone_id
                && i.name == name
                && i.traffic_policy_type == policy.policy_type
        }) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "TrafficPolicyInstanceAlreadyExists",
                format!(
                    "A traffic policy instance for {} ({}) already exists in zone {}",
                    name, policy.policy_type, zone_id
                ),
            ));
        }
        let id = generate_traffic_policy_instance_id();
        let stored = StoredTrafficPolicyInstance {
            id: id.clone(),
            hosted_zone_id: zone_id,
            name,
            ttl: cfg.ttl,
            state: "Applied".to_string(),
            message: String::new(),
            traffic_policy_id: cfg.traffic_policy_id,
            traffic_policy_version: cfg.traffic_policy_version,
            traffic_policy_type: policy.policy_type,
            created_time: Utc::now(),
        };
        account
            .traffic_policy_instances
            .insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &stored);
        body.push_str("</CreateTrafficPolicyInstanceResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/trafficpolicyinstance/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn get_traffic_policy_instance(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let i = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.traffic_policy_instances.get(&id).cloned())
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &i);
        body.push_str("</GetTrafficPolicyInstanceResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn update_traffic_policy_instance(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: UpdateTrafficPolicyInstanceRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!(
                    "invalid UpdateTrafficPolicyInstanceRequest XML: {e}"
                ))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        let policy = account
            .traffic_policies
            .get(&(cfg.traffic_policy_id.clone(), cfg.traffic_policy_version))
            .cloned()
            .ok_or_else(|| no_such_traffic_policy(&cfg.traffic_policy_id))?;
        let i = account
            .traffic_policy_instances
            .get_mut(&id)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        i.ttl = cfg.ttl;
        i.traffic_policy_id = cfg.traffic_policy_id;
        i.traffic_policy_version = cfg.traffic_policy_version;
        i.traffic_policy_type = policy.policy_type;
        i.state = "Applied".to_string();
        let snap = i.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateTrafficPolicyInstanceResponse xmlns=\"{NS}\">"
        ));
        push_traffic_policy_instance(&mut body, &snap);
        body.push_str("</UpdateTrafficPolicyInstanceResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_traffic_policy_instance(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_traffic_policy_instance(&id))?;
        if account.traffic_policy_instances.remove(&id).is_none() {
            return Err(no_such_traffic_policy_instance(&id));
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteTrafficPolicyInstanceResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_traffic_policy_instances(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "trafficpolicyinstancename",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::Enum {
                    key: "trafficpolicyinstancetype",
                    allowed: &RR_TYPES,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut instances: Vec<StoredTrafficPolicyInstance> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.traffic_policy_instances.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        // Honor incoming pagination markers. Without consuming
        // `hostedzoneidmarker` / `trafficpolicyinstancenamemarker` /
        // `trafficpolicyinstancetypemarker`, follow-up pages
        // repeatedly returned the first page.
        let hz_marker = req
            .query_params
            .get("hostedzoneidmarker")
            .cloned()
            .unwrap_or_default();
        let name_marker = req
            .query_params
            .get("trafficpolicyinstancenamemarker")
            .cloned()
            .unwrap_or_default();
        let type_marker = req
            .query_params
            .get("trafficpolicyinstancetypemarker")
            .cloned()
            .unwrap_or_default();
        let start = if hz_marker.is_empty() && name_marker.is_empty() && type_marker.is_empty() {
            0
        } else {
            instances
                .iter()
                .position(|i| {
                    i.hosted_zone_id == hz_marker
                        && i.name == name_marker
                        && i.traffic_policy_type == type_marker
                })
                .map(|p| p + 1)
                .unwrap_or(0)
        };
        let slice: Vec<&StoredTrafficPolicyInstance> =
            instances.iter().skip(start).take(max_items).collect();
        let next_marker = if start + slice.len() < instances.len() {
            slice.last().map(|i| i.name.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!(
            "<IsTruncated>{}</IsTruncated>",
            next_marker.is_some()
        ));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if let Some(nm) = &next_marker {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(nm)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_traffic_policy_instances_by_hosted_zone(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let zone_id = req
            .query_params
            .get("id")
            .cloned()
            .ok_or_else(|| invalid_argument("id query parameter is required"))?;
        let zone_id = strip_zone_prefix(&zone_id);
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&zone_id))?;
        if !account.hosted_zones.contains_key(&zone_id) {
            return Err(no_such_hosted_zone(&zone_id));
        }
        let mut instances: Vec<StoredTrafficPolicyInstance> = account
            .traffic_policy_instances
            .values()
            .filter(|i| i.hosted_zone_id == zone_id)
            .cloned()
            .collect();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredTrafficPolicyInstance> = instances.iter().take(max_items).collect();
        let truncated = slice.len() < instances.len();
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesByHostedZoneResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", truncated));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if truncated {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(&instances[slice.len()].id)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesByHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_traffic_policy_instances_by_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "id",
                    min: 1,
                    max: 36,
                },
                QueryConstraint::IntRange {
                    key: "version",
                    min: 1,
                    max: 1000,
                },
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "trafficpolicyinstancename",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::Enum {
                    key: "trafficpolicyinstancetype",
                    allowed: &RR_TYPES,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let policy_id = req
            .query_params
            .get("id")
            .cloned()
            .ok_or_else(|| invalid_argument("id query parameter is required"))?;
        // `version` is `@required` in the Smithy model. Match AWS by rejecting
        // requests that omit it (a `negative_omit_TrafficPolicyVersion` probe
        // strips the query param entirely; without this guard the handler
        // would silently treat the filter as "any version" and return 200).
        let version_raw = req
            .query_params
            .get("version")
            .cloned()
            .ok_or_else(|| invalid_argument("version query parameter is required"))?;
        let version: i64 = version_raw.parse().map_err(|_| {
            invalid_argument(format!(
                "'version' value '{version_raw}' is not a valid integer"
            ))
        })?;
        let max_items: usize = req
            .query_params
            .get("maxitems")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut instances: Vec<StoredTrafficPolicyInstance> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| {
                a.traffic_policy_instances
                    .values()
                    .filter(|i| {
                        i.traffic_policy_id == policy_id && i.traffic_policy_version == version
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        drop(state);
        instances.sort_by(|a, b| a.id.cmp(&b.id));
        let slice: Vec<&StoredTrafficPolicyInstance> = instances.iter().take(max_items).collect();
        let truncated = slice.len() < instances.len();
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<ListTrafficPolicyInstancesByPolicyResponse xmlns=\"{NS}\">"
        ));
        body.push_str("<TrafficPolicyInstances>");
        for i in &slice {
            push_traffic_policy_instance(&mut body, i);
        }
        body.push_str("</TrafficPolicyInstances>");
        body.push_str(&format!("<IsTruncated>{}</IsTruncated>", truncated));
        body.push_str(&format!("<MaxItems>{}</MaxItems>", max_items));
        if truncated {
            body.push_str(&format!(
                "<TrafficPolicyInstanceNameMarker>{}</TrafficPolicyInstanceNameMarker>",
                esc(&instances[slice.len()].id)
            ));
        }
        body.push_str("</ListTrafficPolicyInstancesByPolicyResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_traffic_policy_instance_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.traffic_policy_instances.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<GetTrafficPolicyInstanceCountResponse xmlns=\"{NS}\">"
        ));
        body.push_str(&format!(
            "<TrafficPolicyInstanceCount>{}</TrafficPolicyInstanceCount>",
            count
        ));
        body.push_str("</GetTrafficPolicyInstanceCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
