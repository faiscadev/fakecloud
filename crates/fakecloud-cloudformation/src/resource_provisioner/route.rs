//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `route`.

use super::*;

impl ResourceProvisioner {
    // --- Route53 ---

    /// Refuse a reference to a hosted zone another account owns: a stack's
    /// record sets, DNSSEC and key-signing keys can only use zones in the
    /// stack's own account. Unlike a zone that is not provisioned yet, this
    /// never resolves on a later pass, so it is reported as such.
    fn require_own_zone(
        &self,
        accounts: &fakecloud_route53::Route53Accounts,
        zone_id: &str,
    ) -> Result<(), String> {
        match accounts.zone_owner(zone_id) {
            Some(owner) if owner != self.account_id => Err(format!(
                "No hosted zone found with ID: {zone_id} in account {}; a stack can only use \
                 hosted zones in its own account",
                self.account_id
            )),
            _ => Ok(()),
        }
    }

    /// The account whose bucket holds `zone_id` for a delete: its owner when
    /// it exists anywhere, else the stack's account. New stacks can only put
    /// records, DNSSEC and KSKs in their own zones, but a snapshot from before
    /// Route 53 was account-scoped may have a stack's record in a zone that
    /// migration gave to another account; deleting the stack removes what it
    /// created there rather than skipping it or failing forever.
    fn zone_home(&self, accounts: &fakecloud_route53::Route53Accounts, zone_id: &str) -> String {
        accounts
            .zone_owner(zone_id)
            .unwrap_or(&self.account_id)
            .to_string()
    }

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

        // A private zone's VPCs must belong to the stack's account, as
        // `CreateHostedZone` requires (VPCs EC2 does not know are accepted).
        {
            let ec2 = self.ec2_state.read();
            for vpc in &vpcs {
                let id = vpc.vpc_id.as_deref().unwrap_or_default();
                if let Some((owner, _, _)) = ec2
                    .iter_regional()
                    .find(|(_, _, st)| st.vpcs.contains_key(id))
                    .filter(|(owner, _, _)| *owner != self.account_id)
                {
                    return Err(format!(
                        "InvalidVPCId: The VPC {id} belongs to account {owner}, not {}",
                        self.account_id
                    ));
                }
            }
        }
        // Same zone `CreateHostedZone` stores (name servers, default SOA + NS
        // record sets), under the stack's account, with a globally unique id.
        let mut accounts = self.route53_state.write();
        let id = accounts.unused_zone_id();
        let zone = fakecloud_route53::build_hosted_zone(
            &id,
            &name,
            self.physical_name(resource),
            comment,
            private_zone,
            vpcs,
            None,
        );
        let name_servers = zone.name_servers.clone();
        accounts
            .entry(&self.account_id)
            .hosted_zones
            .insert(id.clone(), zone);
        drop(accounts);

        let mut result = ProvisionResult::new(id.clone()).with("Id", id);
        for (i, ns) in name_servers.iter().enumerate() {
            result = result.with(&format!("NameServers.{i}"), ns.clone());
        }
        result = result.with("NameServers", name_servers.join(","));
        Ok(result)
    }

    pub(super) fn delete_route53_hosted_zone(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        let state = accounts.entry(&self.account_id);
        state.hosted_zones.remove(physical_id);
        state.cross_account_vpcs.remove(physical_id);
        state.vpc_authorizations.remove(physical_id);
        state.vpc_authorization_consumers.remove(physical_id);
        Ok(())
    }

    /// In-place update for `AWS::Route53::HostedZone`. AWS only supports an
    /// in-place mutation of the comment (`UpdateHostedZoneComment`); `Name` is
    /// immutable and a change forces replacement. Reprovisioning on a comment
    /// (or tag) edit would churn the zone id + 4 name servers AND wipe every
    /// record set (both `AWS::Route53::RecordSet` children and runtime
    /// `ChangeResourceRecordSets` records) stored inside the zone. So mutate the
    /// comment in place when the name is unchanged, preserving the record sets;
    /// fall back to replacement only when the (immutable) name actually changes.
    pub(super) fn update_route53_hosted_zone(
        &self,
        existing: &StackResource,
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

        let in_place = {
            let mut accounts = self.route53_state.write();
            let state = accounts.entry(&self.account_id);
            match state.hosted_zones.get_mut(&existing.physical_id) {
                Some(z) if z.name == normalized_name => {
                    z.comment = comment; // record sets / id / name servers preserved
                    let id = z.id.clone();
                    let name_servers = z.name_servers.clone();
                    let mut result = ProvisionResult::new(id.clone()).with("Id", id);
                    for (i, ns) in name_servers.iter().enumerate() {
                        result = result.with(&format!("NameServers.{i}"), ns.clone());
                    }
                    result = result.with("NameServers", name_servers.join(","));
                    Some(result)
                }
                _ => None,
            }
        };
        match in_place {
            Some(result) => Ok(result),
            // Name changed (immutable) or zone missing: AWS replaces the zone.
            None => self.reprovision_resource(existing, resource).map(|opt| {
                opt.unwrap_or_else(|| ProvisionResult::new(existing.physical_id.clone()))
            }),
        }
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
        self.require_own_zone(&accounts, &zone_id)?;
        let state = accounts.entry(&self.account_id);
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
        let home = self.zone_home(&accounts, zone_id);
        let state = accounts.entry(&home);
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
        let cfg = self.build_health_check_config(cfg_value)?;

        let id = Uuid::new_v4().to_string();
        let hc = StoredHealthCheck {
            id: id.clone(),
            caller_reference: self.physical_name(resource),
            version: 1,
            config: cfg,
            created_time: Utc::now(),
            status: fakecloud_route53::HealthCheckStatus::Success,
            last_failure_reason: None,
        };

        let mut accounts = self.route53_state.write();
        let state = accounts.entry(&self.account_id);
        state.health_checks.insert(id.clone(), hc);

        Ok(ProvisionResult::new(id.clone()).with("HealthCheckId", id))
    }

    /// In-place `UpdateHealthCheck`: reprovision would churn the random
    /// HealthCheckId (dangling every `AWS::Route53::RecordSet` that stored it for
    /// failover routing) and reset the version/status. AWS mutates the config in
    /// place, so rebuild the config and swap it into the stored health check,
    /// preserving the id/caller reference/created time and bumping the version.
    pub(super) fn update_route53_health_check(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg_value = resource
            .properties
            .get("HealthCheckConfig")
            .ok_or("HealthCheckConfig is required")?;
        let cfg = self.build_health_check_config(cfg_value)?;
        let id = existing.physical_id.clone();

        let mut accounts = self.route53_state.write();
        let state = accounts.entry(&self.account_id);
        let hc = state
            .health_checks
            .get_mut(&id)
            .ok_or_else(|| format!("Health check {id} not yet provisioned"))?;
        hc.config = cfg;
        hc.version += 1;
        // id / caller_reference / created_time / status preserved.

        Ok(ProvisionResult::new(id.clone()).with("HealthCheckId", id))
    }

    /// Parse an `AWS::Route53::HealthCheck` `HealthCheckConfig` block into the
    /// route53 model (shared by the create + in-place update arms).
    fn build_health_check_config(
        &self,
        cfg_value: &serde_json::Value,
    ) -> Result<HealthCheckConfig, String> {
        let health_check_type = cfg_value
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("HealthCheckConfig.Type is required")?
            .to_string();
        Ok(HealthCheckConfig {
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
        })
    }

    pub(super) fn delete_route53_health_check(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.route53_state.write();
        let state = accounts.entry(&self.account_id);
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
        self.require_own_zone(&accounts, &zone_id)?;
        let state = accounts.entry(&self.account_id);
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
        let home = self.zone_home(&accounts, physical_id);
        let state = accounts.entry(&home);
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
        self.require_own_zone(&accounts, &zone_id)?;
        let state = accounts.entry(&self.account_id);
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
        let home = self.zone_home(&accounts, zone_id);
        let state = accounts.entry(&home);
        state
            .key_signing_keys
            .remove(&(zone_id.to_string(), name.to_string()));
        Ok(())
    }
}
