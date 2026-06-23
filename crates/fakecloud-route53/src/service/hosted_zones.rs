//! Route53 `hosted_zones` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn create_hosted_zone(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateHostedZoneRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid CreateHostedZoneRequest XML: {e}")))?;
        if cfg.name.is_empty() {
            return Err(invalid_argument("Name is required"));
        }
        if cfg.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        let mut name = cfg.name.clone();
        if !name.ends_with('.') {
            name.push('.');
        }
        // A hosted zone is private iff it is created with a VPC association.
        // AWS treats `HostedZoneConfig.PrivateZone` as a read-only output, not
        // an input — supplying a VPC is what makes the zone private — so don't
        // require the caller to set it.
        let private_zone = cfg.vpc.is_some()
            || cfg
                .hosted_zone_config
                .as_ref()
                .and_then(|c| c.private_zone)
                .unwrap_or(false);
        let comment = cfg
            .hosted_zone_config
            .as_ref()
            .and_then(|c| c.comment.clone());

        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .hosted_zones
            .values()
            .any(|z| z.caller_reference == cfg.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "HostedZoneAlreadyExists",
                format!(
                    "A hosted zone with the same caller reference already exists: {}",
                    cfg.caller_reference
                ),
            ));
        }
        let id = generate_zone_id();
        let now = Utc::now();
        let name_servers = synth_name_servers(&id);
        let vpcs = cfg.vpc.into_iter().collect();
        // Both public and private hosted zones carry default NS + SOA records.
        // The Terraform provider reads a private zone's name servers from its
        // NS record set (findNameServersByZone), so omitting them made the
        // resource read crash on an empty name-server list.
        let default_records = default_zone_records(&name, &name_servers);
        let zone = StoredHostedZone {
            id: id.clone(),
            name: name.clone(),
            caller_reference: cfg.caller_reference,
            comment,
            private_zone,
            features: None,
            vpcs,
            delegation_set_id: cfg.delegation_set_id,
            name_servers: name_servers.clone(),
            created_time: now,
            resource_record_sets: default_records,
        };
        account.hosted_zones.insert(id.clone(), zone.clone());

        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: now,
            comment: Some(format!("CreateHostedZone {}", id)),
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);

        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateHostedZoneResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &zone);
        push_change_info(&mut body, &change);
        body.push_str("<DelegationSet>");
        if let Some(id) = &zone.delegation_set_id {
            body.push_str(&format!("<Id>{}</Id>", esc(id)));
        }
        body.push_str("<NameServers>");
        for ns in &zone.name_servers {
            body.push_str(&format!("<NameServer>{}</NameServer>", esc(ns)));
        }
        body.push_str("</NameServers>");
        body.push_str("</DelegationSet>");
        if !zone.vpcs.is_empty() {
            push_vpc_block(&mut body, "VPC", &zone.vpcs[0]);
        }
        body.push_str("</CreateHostedZoneResponse>");

        let mut headers = HeaderMap::new();
        if let Ok(loc) = http::HeaderValue::from_str(&format!("/2013-04-01/hostedzone/{}", zone.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn get_hosted_zone(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let state = self.state.read();
        let account = state.accounts.get(DEFAULT_ACCOUNT);
        let zone = account
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &zone);
        body.push_str("<DelegationSet>");
        if let Some(id) = &zone.delegation_set_id {
            body.push_str(&format!("<Id>{}</Id>", esc(id)));
        }
        body.push_str("<NameServers>");
        for ns in &zone.name_servers {
            body.push_str(&format!("<NameServer>{}</NameServer>", esc(ns)));
        }
        body.push_str("</NameServers>");
        body.push_str("</DelegationSet>");
        if !zone.vpcs.is_empty() {
            body.push_str("<VPCs>");
            for v in &zone.vpcs {
                push_vpc_block(&mut body, "VPC", v);
            }
            body.push_str("</VPCs>");
        }
        body.push_str("</GetHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_hosted_zone(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        if zone
            .resource_record_sets
            .iter()
            .any(|r| !is_default_record(r, &zone.name))
        {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "HostedZoneNotEmpty",
                format!("HostedZone {} has user-managed resource record sets", id),
            ));
        }
        account.hosted_zones.remove(&id);
        let change_id = generate_change_id();
        let change = StoredChange {
            id: change_id.clone(),
            status: "PENDING".to_string(),
            submitted_at: Utc::now(),
            comment: Some(format!("DeleteHostedZone {}", id)),
            read_count: 0,
        };
        account.changes.insert(change_id.clone(), change.clone());
        drop(state);

        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteHostedZoneResponse xmlns=\"{NS}\">"));
        push_change_info(&mut body, &change);
        body.push_str("</DeleteHostedZoneResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_hosted_zones(
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
                QueryConstraint::StrLen {
                    key: "delegationsetid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::Enum {
                    key: "hostedzonetype",
                    allowed: &["PrivateHostedZone"],
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let state = self.state.read();
        let mut zones: Vec<StoredHostedZone> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        zones.sort_by(|a, b| a.id.cmp(&b.id));
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHostedZonesResponse xmlns=\"{NS}\">"));
        body.push_str("<HostedZones>");
        for z in &zones {
            push_hosted_zone(&mut body, z);
        }
        body.push_str("</HostedZones>");
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("</ListHostedZonesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_hosted_zones_by_name(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "dnsname",
                    min: 0,
                    max: 1024,
                },
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                MAX_ITEMS_CONSTRAINT,
            ],
        )?;
        let dns_name = req.query_params.get("dnsname").cloned();
        let state = self.state.read();
        let mut zones: Vec<StoredHostedZone> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        zones.sort_by(|a, b| a.name.cmp(&b.name));
        if let Some(name) = &dns_name {
            let normalized = if name.ends_with('.') {
                name.clone()
            } else {
                format!("{name}.")
            };
            zones.retain(|z| z.name >= normalized);
        }
        let mut body = String::with_capacity(1024);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListHostedZonesByNameResponse xmlns=\"{NS}\">"));
        body.push_str("<HostedZones>");
        for z in &zones {
            push_hosted_zone(&mut body, z);
        }
        body.push_str("</HostedZones>");
        if let Some(name) = &dns_name {
            body.push_str(&format!("<DNSName>{}</DNSName>", esc(name)));
        }
        body.push_str("<MaxItems>100</MaxItems>");
        body.push_str("<IsTruncated>false</IsTruncated>");
        body.push_str("</ListHostedZonesByNameResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_hosted_zone_count(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let count = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.hosted_zones.len())
            .unwrap_or(0);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneCountResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<HostedZoneCount>{}</HostedZoneCount>", count));
        body.push_str("</GetHostedZoneCountResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn update_hosted_zone_comment(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: UpdateHostedZoneCommentRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateHostedZoneCommentRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        zone.comment = cfg.comment;
        let snap = zone.clone();
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<UpdateHostedZoneCommentResponse xmlns=\"{NS}\">"));
        push_hosted_zone(&mut body, &snap);
        body.push_str("</UpdateHostedZoneCommentResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn update_hosted_zone_features(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let cfg: UpdateHostedZoneFeaturesRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid UpdateHostedZoneFeaturesRequest XML: {e}"))
            })?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        let zone = account
            .hosted_zones
            .get_mut(&id)
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        zone.features = Some(crate::model::HostedZoneFeatures {
            enable_accelerated_recovery: cfg.enable_accelerated_recovery,
        });
        let snap = zone.clone();
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<UpdateHostedZoneFeaturesResponse xmlns=\"{NS}\">"
        ));
        push_hosted_zone(&mut body, &snap);
        body.push_str("</UpdateHostedZoneFeaturesResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn get_hosted_zone_limit(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let id = strip_zone_prefix(&id);
        let lim_type = route
            .second_id
            .clone()
            .ok_or_else(|| invalid_argument("limit Type is required"))?;
        let state = self.state.read();
        let zone = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.hosted_zones.get(&id).cloned())
            .ok_or_else(|| no_such_hosted_zone(&id))?;
        drop(state);
        let (value, count) = match lim_type.as_str() {
            "MAX_RRSETS_BY_ZONE" => (10000_u64, zone.resource_record_sets.len() as u64),
            "MAX_VPCS_ASSOCIATED_BY_ZONE" => (300_u64, zone.vpcs.len() as u64),
            other => {
                return Err(invalid_argument(format!(
                    "Unknown hosted zone limit type: {other}"
                )));
            }
        };
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetHostedZoneLimitResponse xmlns=\"{NS}\">"));
        body.push_str(&format!(
            "<Limit><Type>{}</Type><Value>{}</Value></Limit>",
            esc(&lim_type),
            value
        ));
        body.push_str(&format!("<Count>{}</Count>", count));
        body.push_str("</GetHostedZoneLimitResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
