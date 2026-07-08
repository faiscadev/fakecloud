// Operation handlers for AWS Shield, `include!`d into `service.rs` so they
// share its module scope (error constructors, `ok`, `now_epoch`, ARN helpers,
// and the `ShieldState` accessors).

/// Fetch a string body field.
fn sf<'a>(b: &'a Value, k: &str) -> Option<&'a str> {
    b.get(k).and_then(Value::as_str)
}

/// The account's Shield Advanced subscription limits. These mirror the
/// documented Shield Advanced per-account defaults.
fn default_subscription_limits() -> Value {
    json!({
        "ProtectionLimits": {
            "ProtectedResourceTypeLimits": [
                { "Type": "ELASTIC_IP_ADDRESS", "Max": 100 },
                { "Type": "APPLICATION_LOAD_BALANCER", "Max": 50 },
                { "Type": "CLASSIC_LOAD_BALANCER", "Max": 50 },
                { "Type": "CLOUDFRONT_DISTRIBUTION", "Max": 100 },
                { "Type": "GLOBAL_ACCELERATOR", "Max": 50 },
                { "Type": "ROUTE_53_HOSTED_ZONE", "Max": 100 }
            ]
        },
        "ProtectionGroupLimits": {
            "MaxProtectionGroups": 20000,
            "PatternTypeLimits": {
                "ArbitraryPatternLimits": { "MaxMembers": 10000 }
            }
        }
    })
}

/// Build a fresh Shield Advanced `Subscription` shape for `account`, starting
/// now with a one-year auto-renewing commitment.
fn build_subscription(account: &str) -> Value {
    let start = now_epoch();
    json!({
        "StartTime": start,
        "EndTime": start + ONE_YEAR_SECS,
        "TimeCommitmentInSeconds": ONE_YEAR_SECS as i64,
        "AutoRenew": "ENABLED",
        "ProactiveEngagementStatus": "DISABLED",
        "Limits": [ { "Type": "MitigationCapacityUnits", "Max": 10000 } ],
        "SubscriptionLimits": default_subscription_limits(),
        "SubscriptionArn": subscription_arn(account),
    })
}

/// Extract the health-check id from a Route 53 health-check ARN
/// (`arn:aws:route53:::healthcheck/<id>`), falling back to the whole value.
fn health_check_id(arn: &str) -> String {
    arn.rsplit('/').next().unwrap_or(arn).to_string()
}

impl ShieldService {
    // ---- protections ------------------------------------------------------

    pub(crate) fn create_protection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "Name").unwrap_or("").to_string();
        let resource_arn = sf(&body, "ResourceArn").unwrap_or("").to_string();
        // Shield only protects a fixed set of resource kinds; a value that is
        // not an ARN cannot be protected.
        if !resource_arn.starts_with("arn:aws") {
            return Err(invalid_resource(
                "The resource is not valid or does not have a supported resource type.",
            ));
        }
        let account = req.account_id.clone();
        let tags = body.get("Tags").cloned();

        // A resource may carry at most one protection.
        let dup = self.with_account(req, |acct| {
            acct.protections
                .values()
                .any(|p| p.get("ResourceArn").and_then(Value::as_str) == Some(&resource_arn))
        });
        if dup {
            return Err(resource_already_exists(
                "A Protection already exists for the specified resource.",
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = protection_arn(&account, &id);
        let protection = json!({
            "Id": id,
            "Name": name,
            "ResourceArn": resource_arn,
            "ProtectionArn": arn,
            "HealthCheckIds": [],
        });

        self.with_account_mut(req, |acct| {
            acct.protections.insert(id.clone(), protection);
            acct.protection_order.push(id.clone());
            if let Some(Value::Array(list)) = tags {
                if !list.is_empty() {
                    acct.tags.insert(arn.clone(), list);
                }
            }
        });

        ok(json!({ "ProtectionId": id }))
    }

    pub(crate) fn describe_protection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let by_id = sf(&body, "ProtectionId").map(String::from);
        let by_arn = sf(&body, "ResourceArn").map(String::from);
        if by_id.is_none() && by_arn.is_none() {
            return Err(invalid_parameter(
                "Exactly one of ProtectionId or ResourceArn must be provided.",
            ));
        }
        let found = self.with_account(req, |acct| {
            if let Some(id) = &by_id {
                return acct.protections.get(id).cloned();
            }
            let arn = by_arn.as_deref().unwrap_or("");
            acct.protections
                .values()
                .find(|p| p.get("ResourceArn").and_then(Value::as_str) == Some(arn))
                .cloned()
        });
        match found {
            Some(p) => ok(json!({ "Protection": p })),
            None => Err(resource_not_found(
                "The referenced protection does not exist.",
            )),
        }
    }

    pub(crate) fn delete_protection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionId").unwrap_or("").to_string();
        let removed = self.with_account_mut(req, |acct| {
            let removed = acct.protections.remove(&id);
            if removed.is_some() {
                acct.protection_order.retain(|x| x != &id);
            }
            removed
        });
        match removed {
            Some(_) => ok(json!({})),
            None => Err(resource_not_found(
                "The referenced protection does not exist.",
            )),
        }
    }

    pub(crate) fn list_protections(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filters = body.get("InclusionFilters");
        let want_arns = filter_strings(filters, "ResourceArns");
        let want_names = filter_strings(filters, "ProtectionNames");
        let want_types = filter_strings(filters, "ResourceTypes");

        let protections = self.with_account(req, |acct| {
            acct.protection_order
                .iter()
                .filter_map(|id| acct.protections.get(id).cloned())
                .filter(|p| {
                    let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
                    let name = p.get("Name").and_then(Value::as_str).unwrap_or("");
                    (want_arns.is_empty() || want_arns.iter().any(|a| a == arn))
                        && (want_names.is_empty() || want_names.iter().any(|n| n == name))
                        && (want_types.is_empty()
                            || want_types.iter().any(|t| arn_resource_type(arn) == *t))
                })
                .collect::<Vec<_>>()
        });
        ok(json!({ "Protections": protections }))
    }

    // ---- protection groups ------------------------------------------------

    pub(crate) fn create_protection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionGroupId").unwrap_or("").to_string();
        let account = req.account_id.clone();

        let exists = self.with_account(req, |acct| acct.protection_groups.contains_key(&id));
        if exists {
            return Err(resource_already_exists(
                "A ProtectionGroup already exists for the specified ProtectionGroupId.",
            ));
        }
        let group = build_protection_group(&body, &id, &account);
        let tags = body.get("Tags").cloned();
        let arn = protection_group_arn(&account, &id);
        self.with_account_mut(req, |acct| {
            acct.protection_groups.insert(id.clone(), group);
            acct.protection_group_order.push(id.clone());
            if let Some(Value::Array(list)) = tags {
                if !list.is_empty() {
                    acct.tags.insert(arn.clone(), list);
                }
            }
        });
        ok(json!({}))
    }

    pub(crate) fn update_protection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionGroupId").unwrap_or("").to_string();
        let account = req.account_id.clone();
        let updated = self.with_account_mut(req, |acct| {
            if !acct.protection_groups.contains_key(&id) {
                return false;
            }
            let group = build_protection_group(&body, &id, &account);
            acct.protection_groups.insert(id.clone(), group);
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection group does not exist.",
            ))
        }
    }

    pub(crate) fn delete_protection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionGroupId").unwrap_or("").to_string();
        let removed = self.with_account_mut(req, |acct| {
            let removed = acct.protection_groups.remove(&id).is_some();
            if removed {
                acct.protection_group_order.retain(|x| x != &id);
            }
            removed
        });
        if removed {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection group does not exist.",
            ))
        }
    }

    pub(crate) fn describe_protection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionGroupId").unwrap_or("").to_string();
        let found = self.with_account(req, |acct| acct.protection_groups.get(&id).cloned());
        match found {
            Some(g) => ok(json!({ "ProtectionGroup": g })),
            None => Err(resource_not_found(
                "The referenced protection group does not exist.",
            )),
        }
    }

    pub(crate) fn list_protection_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filters = body.get("InclusionFilters");
        let want_ids = filter_strings(filters, "ProtectionGroupIds");
        let want_patterns = filter_strings(filters, "Patterns");
        let want_aggs = filter_strings(filters, "Aggregations");
        let want_types = filter_strings(filters, "ResourceTypes");

        let groups = self.with_account(req, |acct| {
            acct.protection_group_order
                .iter()
                .filter_map(|id| acct.protection_groups.get(id).cloned())
                .filter(|g| {
                    let field = |k: &str| g.get(k).and_then(Value::as_str).unwrap_or("").to_string();
                    (want_ids.is_empty() || want_ids.contains(&field("ProtectionGroupId")))
                        && (want_patterns.is_empty() || want_patterns.contains(&field("Pattern")))
                        && (want_aggs.is_empty() || want_aggs.contains(&field("Aggregation")))
                        && (want_types.is_empty() || want_types.contains(&field("ResourceType")))
                })
                .collect::<Vec<_>>()
        });
        ok(json!({ "ProtectionGroups": groups }))
    }

    pub(crate) fn list_resources_in_protection_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionGroupId").unwrap_or("").to_string();
        let members = self.with_account(req, |acct| {
            acct.protection_groups
                .get(&id)
                .map(|g| g.get("Members").cloned().unwrap_or_else(|| json!([])))
        });
        match members {
            Some(m) => ok(json!({ "ResourceArns": m })),
            None => Err(resource_not_found(
                "The referenced protection group does not exist.",
            )),
        }
    }

    // ---- subscription -----------------------------------------------------

    pub(crate) fn create_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = req.account_id.clone();
        let already = self.with_account(req, |acct| acct.subscription.is_some());
        if already {
            return Err(resource_already_exists(
                "The account is already subscribed to Shield Advanced.",
            ));
        }
        let sub = build_subscription(&account);
        self.with_account_mut(req, |acct| {
            acct.subscription = Some(sub);
            acct.proactive_engagement_status
                .get_or_insert_with(|| "DISABLED".to_string());
        });
        ok(json!({}))
    }

    pub(crate) fn describe_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sub = self.with_account(req, |acct| acct.subscription.clone());
        match sub {
            Some(s) => ok(json!({ "Subscription": s })),
            None => Err(resource_not_found(
                "The account is not subscribed to Shield Advanced.",
            )),
        }
    }

    pub(crate) fn update_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let auto_renew = sf(&body, "AutoRenew").map(String::from);
        let updated = self.with_account_mut(req, |acct| {
            let Some(sub) = acct.subscription.as_mut().and_then(Value::as_object_mut) else {
                return false;
            };
            if let Some(ar) = auto_renew {
                sub.insert("AutoRenew".into(), json!(ar));
            }
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The account is not subscribed to Shield Advanced.",
            ))
        }
    }

    pub(crate) fn delete_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // A Shield Advanced subscription carries a one-year commitment and
        // auto-renews; it cannot be cancelled through the API. AWS returns
        // LockedSubscriptionException, or ResourceNotFoundException when the
        // account was never subscribed.
        let subscribed = self.with_account(req, |acct| acct.subscription.is_some());
        if subscribed {
            Err(locked_subscription(
                "The subscription is locked by a one-year commitment and cannot be deleted.",
            ))
        } else {
            Err(resource_not_found(
                "The account is not subscribed to Shield Advanced.",
            ))
        }
    }

    pub(crate) fn get_subscription_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let active = self.with_account(req, |acct| acct.subscription.is_some());
        let state = if active { "ACTIVE" } else { "INACTIVE" };
        ok(json!({ "SubscriptionState": state }))
    }

    // ---- attacks (honest: no synthetic DDoS records) ----------------------

    pub(crate) fn list_attacks(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // With no real DDoS traffic to observe, there are no attack records to
        // surface. Returning an empty list is the honest response.
        ok(json!({ "AttackSummaries": [] }))
    }

    pub(crate) fn describe_attack(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // No attack records are tracked, so no `AttackDetail` is returned. The
        // `Attack` member is optional in the Smithy model.
        ok(json!({}))
    }

    pub(crate) fn describe_attack_statistics(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // A zeroed time-series over the last 30 days: no attacks observed.
        let now = now_epoch();
        ok(json!({
            "TimeRange": {
                "FromInclusive": now - 30.0 * 24.0 * 60.0 * 60.0,
                "ToExclusive": now,
            },
            "DataItems": [],
        }))
    }

    // ---- DRT access -------------------------------------------------------

    pub(crate) fn associate_drt_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let role_arn = sf(&body, "RoleArn").unwrap_or("").to_string();
        self.with_account_mut(req, |acct| {
            acct.drt_role_arn = Some(role_arn);
        });
        ok(json!({}))
    }

    pub(crate) fn disassociate_drt_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |acct| {
            acct.drt_role_arn = None;
        });
        ok(json!({}))
    }

    pub(crate) fn associate_drt_log_bucket(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let bucket = sf(&body, "LogBucket").unwrap_or("").to_string();
        // A DRT role must be associated before granting log-bucket access.
        let has_role = self.with_account(req, |acct| acct.drt_role_arn.is_some());
        if !has_role {
            return Err(no_associated_role(
                "No DRT role is associated with the account.",
            ));
        }
        let too_many = self.with_account(req, |acct| {
            acct.drt_log_buckets.len() >= 10 && !acct.drt_log_buckets.contains(&bucket)
        });
        if too_many {
            return Err(limits_exceeded(
                "The maximum number of DRT log buckets has been reached.",
            ));
        }
        self.with_account_mut(req, |acct| {
            if !acct.drt_log_buckets.contains(&bucket) {
                acct.drt_log_buckets.push(bucket);
            }
        });
        ok(json!({}))
    }

    pub(crate) fn disassociate_drt_log_bucket(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let bucket = sf(&body, "LogBucket").unwrap_or("").to_string();
        let has_role = self.with_account(req, |acct| acct.drt_role_arn.is_some());
        if !has_role {
            return Err(no_associated_role(
                "No DRT role is associated with the account.",
            ));
        }
        self.with_account_mut(req, |acct| {
            acct.drt_log_buckets.retain(|b| b != &bucket);
        });
        ok(json!({}))
    }

    pub(crate) fn describe_drt_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (role, buckets) =
            self.with_account(req, |acct| (acct.drt_role_arn.clone(), acct.drt_log_buckets.clone()));
        let mut out = serde_json::Map::new();
        if let Some(role) = role {
            out.insert("RoleArn".into(), json!(role));
        }
        if !buckets.is_empty() {
            out.insert("LogBucketList".into(), json!(buckets));
        }
        ok(Value::Object(out))
    }

    // ---- health checks ----------------------------------------------------

    pub(crate) fn associate_health_check(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionId").unwrap_or("").to_string();
        let hc = health_check_id(sf(&body, "HealthCheckArn").unwrap_or(""));
        let updated = self.with_account_mut(req, |acct| {
            let Some(p) = acct.protections.get_mut(&id).and_then(Value::as_object_mut) else {
                return false;
            };
            let list = p
                .entry("HealthCheckIds")
                .or_insert_with(|| json!([]))
                .as_array_mut();
            if let Some(list) = list {
                if !list.iter().any(|v| v.as_str() == Some(hc.as_str())) {
                    list.push(json!(hc));
                }
            }
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection does not exist.",
            ))
        }
    }

    pub(crate) fn disassociate_health_check(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ProtectionId").unwrap_or("").to_string();
        let hc = health_check_id(sf(&body, "HealthCheckArn").unwrap_or(""));
        let updated = self.with_account_mut(req, |acct| {
            let Some(p) = acct.protections.get_mut(&id).and_then(Value::as_object_mut) else {
                return false;
            };
            if let Some(list) = p.get_mut("HealthCheckIds").and_then(Value::as_array_mut) {
                list.retain(|v| v.as_str() != Some(hc.as_str()));
            }
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection does not exist.",
            ))
        }
    }

    // ---- proactive engagement + emergency contacts ------------------------

    pub(crate) fn associate_proactive_engagement_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let contacts = body
            .get("EmergencyContactList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.with_account_mut(req, |acct| {
            acct.emergency_contacts = contacts;
            acct.proactive_engagement_status = Some("PENDING".to_string());
            set_proactive_status(acct, "PENDING");
        });
        ok(json!({}))
    }

    pub(crate) fn enable_proactive_engagement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |acct| {
            acct.proactive_engagement_status = Some("ENABLED".to_string());
            set_proactive_status(acct, "ENABLED");
        });
        ok(json!({}))
    }

    pub(crate) fn disable_proactive_engagement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |acct| {
            acct.proactive_engagement_status = Some("DISABLED".to_string());
            set_proactive_status(acct, "DISABLED");
        });
        ok(json!({}))
    }

    pub(crate) fn update_emergency_contact_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let contacts = body
            .get("EmergencyContactList")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.with_account_mut(req, |acct| {
            acct.emergency_contacts = contacts;
        });
        ok(json!({}))
    }

    pub(crate) fn describe_emergency_contact_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let contacts = self.with_account(req, |acct| acct.emergency_contacts.clone());
        ok(json!({ "EmergencyContactList": contacts }))
    }

    // ---- application-layer automatic response -----------------------------

    pub(crate) fn enable_application_layer_automatic_response(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceArn").unwrap_or("").to_string();
        let action = body.get("Action").cloned().unwrap_or_else(|| json!({}));
        self.set_alar(req, &arn, action)
    }

    pub(crate) fn update_application_layer_automatic_response(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceArn").unwrap_or("").to_string();
        let action = body.get("Action").cloned().unwrap_or_else(|| json!({}));
        self.set_alar(req, &arn, action)
    }

    pub(crate) fn disable_application_layer_automatic_response(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceArn").unwrap_or("").to_string();
        let updated = self.with_account_mut(req, |acct| {
            let Some(p) = protection_for_resource_mut(acct, &arn) else {
                return false;
            };
            p.remove("ApplicationLayerAutomaticResponseConfiguration");
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection does not exist.",
            ))
        }
    }

    /// Shared setter for enable/update application-layer automatic response.
    fn set_alar(
        &self,
        req: &AwsRequest,
        arn: &str,
        action: Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let updated = self.with_account_mut(req, |acct| {
            let Some(p) = protection_for_resource_mut(acct, arn) else {
                return false;
            };
            p.insert(
                "ApplicationLayerAutomaticResponseConfiguration".into(),
                json!({ "Status": "ENABLED", "Action": action }),
            );
            true
        });
        if updated {
            ok(json!({}))
        } else {
            Err(resource_not_found(
                "The referenced protection does not exist.",
            ))
        }
    }

    // ---- tags -------------------------------------------------------------

    pub(crate) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceARN").unwrap_or("").to_string();
        let new_tags = body
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.with_account_mut(req, |acct| {
            let existing = acct.tags.entry(arn).or_default();
            for tag in new_tags {
                let key = tag.get("Key").and_then(Value::as_str).map(String::from);
                if let Some(key) = key {
                    existing.retain(|t| t.get("Key").and_then(Value::as_str) != Some(&key));
                }
                existing.push(tag);
            }
        });
        ok(json!({}))
    }

    pub(crate) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceARN").unwrap_or("").to_string();
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        self.with_account_mut(req, |acct| {
            if let Some(list) = acct.tags.get_mut(&arn) {
                list.retain(|t| {
                    t.get("Key")
                        .and_then(Value::as_str)
                        .map(|k| !keys.iter().any(|x| x == k))
                        .unwrap_or(true)
                });
            }
        });
        ok(json!({}))
    }

    pub(crate) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceARN").unwrap_or("").to_string();
        let tags = self.with_account(req, |acct| acct.tags.get(&arn).cloned().unwrap_or_default());
        ok(json!({ "Tags": tags }))
    }
}

// ---- free helpers ---------------------------------------------------------

/// Collect the string values of a `[...]` list member inside an inclusion
/// filter object (each filter list holds a single value in the Shield model).
fn filter_strings(filters: Option<&Value>, key: &str) -> Vec<String> {
    filters
        .and_then(|f| f.get(key))
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

/// Map a protected resource ARN to its Shield `ProtectedResourceType` enum.
fn arn_resource_type(arn: &str) -> &'static str {
    if arn.contains(":cloudfront:") {
        "CLOUDFRONT_DISTRIBUTION"
    } else if arn.contains(":route53:") {
        "ROUTE_53_HOSTED_ZONE"
    } else if arn.contains(":globalaccelerator:") {
        "GLOBAL_ACCELERATOR"
    } else if arn.contains(":ec2:") {
        "ELASTIC_IP_ALLOCATION"
    } else if arn.contains("loadbalancer/app/") {
        "APPLICATION_LOAD_BALANCER"
    } else {
        "CLASSIC_LOAD_BALANCER"
    }
}

/// Build a `ProtectionGroup` shape from a Create/Update request body.
fn build_protection_group(body: &Value, id: &str, account: &str) -> Value {
    let members = body
        .get("Members")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut g = serde_json::Map::new();
    g.insert("ProtectionGroupId".into(), json!(id));
    g.insert(
        "Aggregation".into(),
        json!(body.get("Aggregation").and_then(Value::as_str).unwrap_or("SUM")),
    );
    g.insert(
        "Pattern".into(),
        json!(body.get("Pattern").and_then(Value::as_str).unwrap_or("ALL")),
    );
    if let Some(rt) = body.get("ResourceType").and_then(Value::as_str) {
        g.insert("ResourceType".into(), json!(rt));
    }
    g.insert("Members".into(), json!(members));
    g.insert(
        "ProtectionGroupArn".into(),
        json!(protection_group_arn(account, id)),
    );
    Value::Object(g)
}

/// Find the (mutable) protection object whose `ResourceArn` matches `arn`.
fn protection_for_resource_mut<'a>(
    acct: &'a mut ShieldState,
    arn: &str,
) -> Option<&'a mut serde_json::Map<String, Value>> {
    acct.protections
        .values_mut()
        .find(|p| p.get("ResourceArn").and_then(Value::as_str) == Some(arn))
        .and_then(Value::as_object_mut)
}

/// Reflect the account proactive-engagement status onto the stored
/// subscription (its `ProactiveEngagementStatus` member), if subscribed.
fn set_proactive_status(acct: &mut ShieldState, status: &str) {
    if let Some(sub) = acct.subscription.as_mut().and_then(Value::as_object_mut) {
        sub.insert("ProactiveEngagementStatus".into(), json!(status));
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> ShieldService {
        ShieldService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "shield".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn subscription_lifecycle() {
        let s = svc();
        assert_eq!(
            body_of(&s.get_subscription_state(&req("GetSubscriptionState", json!({}))).unwrap())
                ["SubscriptionState"],
            json!("INACTIVE")
        );
        s.create_subscription(&req("CreateSubscription", json!({})))
            .unwrap();
        assert_eq!(
            body_of(&s.get_subscription_state(&req("GetSubscriptionState", json!({}))).unwrap())
                ["SubscriptionState"],
            json!("ACTIVE")
        );
        // Second create -> already exists.
        assert!(s
            .create_subscription(&req("CreateSubscription", json!({})))
            .is_err());
        // Delete -> locked.
        assert!(s
            .delete_subscription(&req("DeleteSubscription", json!({})))
            .is_err());
    }

    #[test]
    fn protection_round_trip() {
        let s = svc();
        let arn = "arn:aws:cloudfront::000000000000:distribution/E123";
        let resp = s
            .create_protection(&req(
                "CreateProtection",
                json!({ "Name": "web", "ResourceArn": arn }),
            ))
            .unwrap();
        let id = body_of(&resp)["ProtectionId"].as_str().unwrap().to_string();
        let d = s
            .describe_protection(&req("DescribeProtection", json!({ "ProtectionId": id })))
            .unwrap();
        assert_eq!(body_of(&d)["Protection"]["ResourceArn"], json!(arn));
        // Duplicate resource -> already exists.
        assert!(s
            .create_protection(&req(
                "CreateProtection",
                json!({ "Name": "web2", "ResourceArn": arn })
            ))
            .is_err());
        // List shows it.
        let l = s
            .list_protections(&req("ListProtections", json!({})))
            .unwrap();
        assert_eq!(body_of(&l)["Protections"].as_array().unwrap().len(), 1);
        // Delete.
        s.delete_protection(&req("DeleteProtection", json!({ "ProtectionId": id })))
            .unwrap();
        assert!(s
            .describe_protection(&req("DescribeProtection", json!({ "ProtectionId": "missing" })))
            .is_err());
    }

    #[test]
    fn protection_group_round_trip() {
        let s = svc();
        s.create_protection_group(&req(
            "CreateProtectionGroup",
            json!({ "ProtectionGroupId": "grp", "Aggregation": "SUM", "Pattern": "ALL" }),
        ))
        .unwrap();
        let d = s
            .describe_protection_group(&req(
                "DescribeProtectionGroup",
                json!({ "ProtectionGroupId": "grp" }),
            ))
            .unwrap();
        assert_eq!(
            body_of(&d)["ProtectionGroup"]["Aggregation"],
            json!("SUM")
        );
        let l = s
            .list_resources_in_protection_group(&req(
                "ListResourcesInProtectionGroup",
                json!({ "ProtectionGroupId": "grp" }),
            ))
            .unwrap();
        assert!(body_of(&l)["ResourceArns"].as_array().unwrap().is_empty());
    }

    #[test]
    fn tags_round_trip() {
        let s = svc();
        let arn = "arn:aws:shield::000000000000:protection/abc";
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": [{ "Key": "env", "Value": "prod" }] }),
        ))
        .unwrap();
        let l = s
            .list_tags_for_resource(&req("ListTagsForResource", json!({ "ResourceARN": arn })))
            .unwrap();
        assert_eq!(body_of(&l)["Tags"].as_array().unwrap().len(), 1);
        s.untag_resource(&req(
            "UntagResource",
            json!({ "ResourceARN": arn, "TagKeys": ["env"] }),
        ))
        .unwrap();
        let l = s
            .list_tags_for_resource(&req("ListTagsForResource", json!({ "ResourceARN": arn })))
            .unwrap();
        assert!(body_of(&l)["Tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn attacks_are_empty() {
        let s = svc();
        let l = s.list_attacks(&req("ListAttacks", json!({}))).unwrap();
        assert!(body_of(&l)["AttackSummaries"]
            .as_array()
            .unwrap()
            .is_empty());
        let stats = s
            .describe_attack_statistics(&req("DescribeAttackStatistics", json!({})))
            .unwrap();
        assert!(body_of(&stats)["DataItems"].as_array().unwrap().is_empty());
    }
}
