//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `route`.

use super::*;

impl ResourceProvisioner {
    // --- Route53 ---

    pub(super) fn create_route53_hosted_zone(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let normalized_name = if name.ends_with('.') {
            name.clone()
        } else {
            format!("{name}.")
        };
        let comment = props
            .get("HostedZoneConfig")
            .and_then(|v| v.get("Comment"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let private_zone = props
            .get("VPCs")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let vpcs: Vec<fakecloud_route53::model::VPC> = props
            .get("VPCs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|vpc| fakecloud_route53::model::VPC {
                        vpc_id: vpc.get("VPCId").and_then(|v| v.as_str()).map(String::from),
                        vpc_region: vpc
                            .get("VPCRegion")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let id = format!(
            "Z{}",
            Uuid::new_v4().simple().to_string()[..14].to_uppercase()
        );
        let name_servers = (1..=4)
            .map(|i| format!("ns-{}.awsdns-{:02}.com", 100 + i, i))
            .collect::<Vec<_>>();

        let zone = StoredHostedZone {
            id: id.clone(),
            name: normalized_name,
            caller_reference: format!("cfn-{}", resource.logical_id),
            comment,
            private_zone,
            features: Some(HostedZoneFeatures::default()),
            vpcs,
            delegation_set_id: None,
            name_servers: name_servers.clone(),
            created_time: Utc::now(),
            resource_record_sets: Vec::new(),
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.hosted_zones.insert(id.clone(), zone);

        let mut result = ProvisionResult::new(id.clone()).with("Id", id);
        for (i, ns) in name_servers.iter().enumerate() {
            result = result.with(&format!("NameServers.{i}"), ns.clone());
        }
        result = result.with("NameServers", name_servers.join(","));
        Ok(result)
    }

    pub(super) fn delete_route53_hosted_zone(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.hosted_zones.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_route53_record_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let zone_id = props
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                "HostedZoneId is required (HostedZoneName lookups not supported)".to_string()
            })?
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let normalized_name = if name.ends_with('.') {
            name.clone()
        } else {
            format!("{name}.")
        };
        let record_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("Type is required")?
            .to_string();
        let ttl = props.get("TTL").and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| v.as_i64())
        });
        let resource_records = props
            .get("ResourceRecords")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let recs: Vec<fakecloud_route53::model::ResourceRecord> = arr
                    .iter()
                    .filter_map(|v| {
                        v.as_str()
                            .map(|s| fakecloud_route53::model::ResourceRecord {
                                value: s.to_string(),
                            })
                    })
                    .collect();
                fakecloud_route53::model::ResourceRecords {
                    resource_record: recs,
                }
            });
        let alias_target =
            props
                .get("AliasTarget")
                .map(|v| fakecloud_route53::model::AliasTarget {
                    hosted_zone_id: v
                        .get("HostedZoneId")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    dns_name: v
                        .get("DNSName")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    evaluate_target_health: v
                        .get("EvaluateTargetHealth")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false),
                });
        let set_identifier = props
            .get("SetIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let weight = props.get("Weight").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let region = props
            .get("Region")
            .and_then(|v| v.as_str())
            .map(String::from);
        let failover = props
            .get("Failover")
            .and_then(|v| v.as_str())
            .map(String::from);
        let multi_value_answer = props.get("MultiValueAnswer").and_then(|v| v.as_bool());
        let health_check_id = props
            .get("HealthCheckId")
            .and_then(|v| v.as_str())
            .map(String::from);

        let rrset = ResourceRecordSet {
            name: normalized_name.clone(),
            record_type: record_type.clone(),
            set_identifier: set_identifier.clone(),
            weight,
            region,
            geo_location: None,
            failover,
            multi_value_answer,
            ttl,
            resource_records,
            alias_target,
            health_check_id,
            traffic_policy_instance_id: None,
            cidr_routing_config: None,
            geo_proximity_location: None,
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        let zone = state.hosted_zones.get_mut(&zone_id).ok_or_else(|| {
            format!(
                "HostedZone {zone_id} not yet provisioned; will retry once it has been provisioned"
            )
        })?;
        // Replace existing record with matching (name, type, set_identifier).
        zone.resource_record_sets.retain(|r| {
            !(r.name == rrset.name
                && r.record_type == rrset.record_type
                && r.set_identifier == rrset.set_identifier)
        });
        zone.resource_record_sets.push(rrset);

        let physical_id = match &set_identifier {
            Some(sid) => format!("{zone_id}|{normalized_name}|{record_type}|{sid}"),
            None => format!("{zone_id}|{normalized_name}|{record_type}"),
        };
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_route53_record_set(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() < 3 {
            return Ok(());
        }
        let zone_id = parts[0];
        let name = parts[1];
        let record_type = parts[2];
        let set_identifier = parts.get(3).map(|s| s.to_string());

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        if let Some(zone) = state.hosted_zones.get_mut(zone_id) {
            zone.resource_record_sets.retain(|r| {
                !(r.name == name
                    && r.record_type == record_type
                    && r.set_identifier == set_identifier)
            });
        }
        Ok(())
    }

    pub(super) fn create_route53_health_check(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg_value = props
            .get("HealthCheckConfig")
            .ok_or("HealthCheckConfig is required")?;

        let health_check_type = cfg_value
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("HealthCheckConfig.Type is required")?
            .to_string();

        let cfg = HealthCheckConfig {
            ip_address: cfg_value
                .get("IPAddress")
                .and_then(|v| v.as_str())
                .map(String::from),
            port: cfg_value
                .get("Port")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            health_check_type,
            resource_path: cfg_value
                .get("ResourcePath")
                .and_then(|v| v.as_str())
                .map(String::from),
            fully_qualified_domain_name: cfg_value
                .get("FullyQualifiedDomainName")
                .and_then(|v| v.as_str())
                .map(String::from),
            search_string: cfg_value
                .get("SearchString")
                .and_then(|v| v.as_str())
                .map(String::from),
            request_interval: cfg_value
                .get("RequestInterval")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            failure_threshold: cfg_value
                .get("FailureThreshold")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            measure_latency: cfg_value.get("MeasureLatency").and_then(|v| v.as_bool()),
            inverted: cfg_value.get("Inverted").and_then(|v| v.as_bool()),
            disabled: cfg_value.get("Disabled").and_then(|v| v.as_bool()),
            health_threshold: cfg_value
                .get("HealthThreshold")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                })
                .map(|n| n as i32),
            child_health_checks: cfg_value
                .get("ChildHealthChecks")
                .and_then(|v| v.as_array())
                .map(|arr| fakecloud_route53::model::ChildHealthChecks {
                    child_health_check: arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                }),
            enable_sni: cfg_value.get("EnableSNI").and_then(|v| v.as_bool()),
            regions: cfg_value
                .get("Regions")
                .and_then(|v| v.as_array())
                .map(|arr| fakecloud_route53::model::HealthCheckRegions {
                    region: arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                }),
            alarm_identifier: cfg_value.get("AlarmIdentifier").map(|v| {
                fakecloud_route53::model::AlarmIdentifier {
                    region: v
                        .get("Region")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    name: v
                        .get("Name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                }
            }),
            insufficient_data_health_status: cfg_value
                .get("InsufficientDataHealthStatus")
                .and_then(|v| v.as_str())
                .map(String::from),
            routing_control_arn: cfg_value
                .get("RoutingControlArn")
                .and_then(|v| v.as_str())
                .map(String::from),
        };

        let id = Uuid::new_v4().to_string();
        let hc = StoredHealthCheck {
            id: id.clone(),
            caller_reference: format!("cfn-{}", resource.logical_id),
            version: 1,
            config: cfg,
            created_time: Utc::now(),
            status: fakecloud_route53::HealthCheckStatus::Success,
            last_failure_reason: None,
        };

        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.health_checks.insert(id.clone(), hc);

        Ok(ProvisionResult::new(id.clone()).with("HealthCheckId", id))
    }

    pub(super) fn delete_route53_health_check(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        // Route53 is a global service in fakecloud; all entries share the
        // default account bucket so SDK reads land on the same data.
        let state = accounts.entry("000000000000");
        state.health_checks.remove(physical_id);
        Ok(())
    }

    /// `AWS::Route53::DNSSEC` flips the hosted zone's `dnssec_status` to
    /// `SIGNING`. Physical id is the hosted zone id so the delete path can
    /// flip it back without consulting the template.
    pub(super) fn create_route53_dnssec(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let zone_id = resource
            .properties
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "HostedZoneId is required".to_string())?
            .trim_start_matches("/hostedzone/")
            .to_string();
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        if !state.hosted_zones.contains_key(&zone_id) {
            return Err(format!("HostedZone {zone_id} does not exist"));
        }
        state
            .dnssec_status
            .insert(zone_id.clone(), "SIGNING".to_string());
        Ok(ProvisionResult::new(zone_id))
    }

    pub(super) fn delete_route53_dnssec(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        state.dnssec_status.remove(physical_id);
        Ok(())
    }

    /// `AWS::Route53::KeySigningKey` registers a KSK against a hosted
    /// zone. Physical id encodes `<hosted_zone_id>/<name>` so the delete
    /// path can find the (zone, name) tuple without re-reading inputs.
    pub(super) fn create_route53_key_signing_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let zone_id = props
            .get("HostedZoneId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "HostedZoneId is required".to_string())?
            .trim_start_matches("/hostedzone/")
            .to_string();
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Name is required".to_string())?
            .to_string();
        let kms_arn = props
            .get("KeyManagementServiceArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "KeyManagementServiceArn is required".to_string())?
            .to_string();
        let status = props
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_string();
        let now = Utc::now();
        // Derive the same deterministic ECDSA P-256 KSK material the
        // direct CreateKeySigningKey path produces so DNSSEC features
        // (DS digest, RRSIG signing, the
        // `/_fakecloud/route53/zones/{id}/dnssec` admin endpoint)
        // round-trip identically through CloudFormation.
        let key_material = fakecloud_route53::dnssec::derive_keypair(&zone_id, &name);
        let key_tag = fakecloud_route53::dnssec::key_tag_for(&key_material.dnskey_public_key);
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        if !state.hosted_zones.contains_key(&zone_id) {
            return Err(format!("HostedZone {zone_id} does not exist"));
        }
        let zone_name = state
            .hosted_zones
            .get(&zone_id)
            .map(|z| z.name.clone())
            .unwrap_or_else(|| ".".to_string());
        let ds_digest_hex = fakecloud_route53::dnssec::ds_digest_sha256(
            &zone_name,
            key_tag,
            &key_material.dnskey_public_key,
        );
        let ksk = fakecloud_route53::StoredKeySigningKey {
            hosted_zone_id: zone_id.clone(),
            name: name.clone(),
            kms_arn,
            status,
            caller_reference: format!("cfn-{}", Uuid::new_v4()),
            created_date: now,
            last_modified_date: now,
            key_tag: key_tag as i32,
            private_key_pem: key_material.private_key_pem,
            public_key_der: key_material.public_key_der,
            ds_digest_hex,
        };
        state
            .key_signing_keys
            .insert((zone_id.clone(), name.clone()), ksk);
        Ok(ProvisionResult::new(format!("{zone_id}/{name}")))
    }

    pub(super) fn delete_route53_key_signing_key(&self, physical_id: &str) -> Result<(), String> {
        let (zone_id, name) = match physical_id.split_once('/') {
            Some(parts) => parts,
            None => return Ok(()),
        };
        let mut accounts = self.route53_state.write();
        let state = accounts.entry("000000000000");
        state
            .key_signing_keys
            .remove(&(zone_id.to_string(), name.to_string()));
        Ok(())
    }
}
