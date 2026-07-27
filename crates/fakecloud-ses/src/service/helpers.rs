use super::*;

/// URL-decode a path segment (e.g. `test%40example.com` -> `test@example.com`).
pub(crate) fn decode_segment(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

pub(crate) fn resolve_account_action(method: &Method, segs: &[String]) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::GET, 3) => Some(("GetAccount", None, None)),
        (&Method::POST, 4) if segs[3] == "details" => Some(("PutAccountDetails", None, None)),
        (&Method::PUT, 4) if segs[3] == "sending" => {
            Some(("PutAccountSendingAttributes", None, None))
        }
        (&Method::PUT, 4) if segs[3] == "suppression" => {
            Some(("PutAccountSuppressionAttributes", None, None))
        }
        (&Method::PUT, 4) if segs[3] == "vdm" => Some(("PutAccountVdmAttributes", None, None)),
        (&Method::PUT, 4) if segs[3] == "pricing-attributes" => {
            Some(("PutAccountPricingAttributes", None, None))
        }
        (&Method::PUT, 5) if segs[3] == "dedicated-ips" && segs[4] == "warmup" => {
            Some(("PutAccountDedicatedIpWarmupAttributes", None, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_identities_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateEmailIdentity", None, None)),
        (&Method::GET, 3) => Some(("ListEmailIdentities", None, None)),
        (&Method::GET, 4) => Some(("GetEmailIdentity", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteEmailIdentity", resource, None)),
        (&Method::PUT, 5) if segs[4] == "dkim" => {
            Some(("PutEmailIdentityDkimAttributes", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "feedback" => {
            Some(("PutEmailIdentityFeedbackAttributes", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "mail-from" => {
            Some(("PutEmailIdentityMailFromAttributes", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "configuration-set" => {
            Some(("PutEmailIdentityConfigurationSetAttributes", resource, None))
        }
        (&Method::GET, 5) if segs[4] == "policies" => {
            Some(("GetEmailIdentityPolicies", resource, None))
        }
        (&Method::PUT, 6) if segs[4] == "dkim" && segs[5] == "signing" => {
            Some(("PutEmailIdentityDkimSigningAttributes", resource, None))
        }
        (&Method::POST, 6) if segs[4] == "policies" => Some((
            "CreateEmailIdentityPolicy",
            resource,
            Some(decode_segment(&segs[5])),
        )),
        (&Method::PUT, 6) if segs[4] == "policies" => Some((
            "UpdateEmailIdentityPolicy",
            resource,
            Some(decode_segment(&segs[5])),
        )),
        (&Method::DELETE, 6) if segs[4] == "policies" => Some((
            "DeleteEmailIdentityPolicy",
            resource,
            Some(decode_segment(&segs[5])),
        )),
        _ => None,
    }
}

pub(crate) fn resolve_configuration_sets_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateConfigurationSet", None, None)),
        (&Method::GET, 3) => Some(("ListConfigurationSets", None, None)),
        (&Method::GET, 4) => Some(("GetConfigurationSet", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteConfigurationSet", resource, None)),
        (&Method::POST, 5) if segs[4] == "event-destinations" => {
            Some(("CreateConfigurationSetEventDestination", resource, None))
        }
        (&Method::GET, 5) if segs[4] == "event-destinations" => {
            Some(("GetConfigurationSetEventDestinations", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "sending" => {
            Some(("PutConfigurationSetSendingOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "delivery-options" => {
            Some(("PutConfigurationSetDeliveryOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "tracking-options" => {
            Some(("PutConfigurationSetTrackingOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "suppression-options" => {
            Some(("PutConfigurationSetSuppressionOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "reputation-options" => {
            Some(("PutConfigurationSetReputationOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "vdm-options" => {
            Some(("PutConfigurationSetVdmOptions", resource, None))
        }
        (&Method::PUT, 5) if segs[4] == "archiving-options" => {
            Some(("PutConfigurationSetArchivingOptions", resource, None))
        }
        (&Method::PUT, 6) if segs[4] == "event-destinations" => Some((
            "UpdateConfigurationSetEventDestination",
            resource,
            Some(decode_segment(&segs[5])),
        )),
        (&Method::DELETE, 6) if segs[4] == "event-destinations" => Some((
            "DeleteConfigurationSetEventDestination",
            resource,
            Some(decode_segment(&segs[5])),
        )),
        _ => None,
    }
}

pub(crate) fn resolve_templates_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateEmailTemplate", None, None)),
        (&Method::GET, 3) => Some(("ListEmailTemplates", None, None)),
        (&Method::GET, 4) => Some(("GetEmailTemplate", resource, None)),
        (&Method::PUT, 4) => Some(("UpdateEmailTemplate", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteEmailTemplate", resource, None)),
        (&Method::POST, 5) if segs[4] == "render" => {
            Some(("TestRenderEmailTemplate", resource, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_contact_lists_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateContactList", None, None)),
        (&Method::GET, 3) => Some(("ListContactLists", None, None)),
        (&Method::GET, 4) => Some(("GetContactList", resource, None)),
        (&Method::PUT, 4) => Some(("UpdateContactList", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteContactList", resource, None)),
        (&Method::POST, 5) if segs[4] == "contacts" => Some(("CreateContact", resource, None)),
        (&Method::GET, 5) if segs[4] == "contacts" => Some(("ListContacts", resource, None)),
        // SDK sends POST .../contacts/list for ListContacts
        (&Method::POST, 6) if segs[4] == "contacts" && segs[5] == "list" => {
            Some(("ListContacts", resource, None))
        }
        (&Method::GET, 6) if segs[4] == "contacts" => {
            Some(("GetContact", resource, Some(decode_segment(&segs[5]))))
        }
        (&Method::PUT, 6) if segs[4] == "contacts" => {
            Some(("UpdateContact", resource, Some(decode_segment(&segs[5]))))
        }
        (&Method::DELETE, 6) if segs[4] == "contacts" => {
            Some(("DeleteContact", resource, Some(decode_segment(&segs[5]))))
        }
        _ => None,
    }
}

pub(crate) fn resolve_suppression_action(method: &Method, segs: &[String]) -> ResolvedAction {
    if segs.get(3).map(|s| s.as_str()) != Some("addresses") {
        return None;
    }
    match (method, segs.len()) {
        (&Method::PUT, 4) => Some(("PutSuppressedDestination", None, None)),
        (&Method::GET, 4) => Some(("ListSuppressedDestinations", None, None)),
        (&Method::GET, 5) => Some((
            "GetSuppressedDestination",
            Some(decode_segment(&segs[4])),
            None,
        )),
        (&Method::DELETE, 5) => Some((
            "DeleteSuppressedDestination",
            Some(decode_segment(&segs[4])),
            None,
        )),
        _ => None,
    }
}

pub(crate) fn resolve_custom_verification_template_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateCustomVerificationEmailTemplate", None, None)),
        (&Method::GET, 3) => Some(("ListCustomVerificationEmailTemplates", None, None)),
        (&Method::GET, 4) => Some(("GetCustomVerificationEmailTemplate", resource, None)),
        (&Method::PUT, 4) => Some(("UpdateCustomVerificationEmailTemplate", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteCustomVerificationEmailTemplate", resource, None)),
        _ => None,
    }
}

pub(crate) fn resolve_deliverability_dashboard_action(
    method: &Method,
    segs: &[String],
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::GET, 3) => Some(("GetDeliverabilityDashboardOptions", None, None)),
        (&Method::PUT, 3) => Some(("PutDeliverabilityDashboardOption", None, None)),
        (&Method::POST, 4) if segs[3] == "test" => {
            Some(("CreateDeliverabilityTestReport", None, None))
        }
        (&Method::GET, 4) if segs[3] == "blacklist-report" => {
            Some(("GetBlacklistReports", None, None))
        }
        (&Method::GET, 4) if segs[3] == "test-reports" => {
            Some(("ListDeliverabilityTestReports", None, None))
        }
        (&Method::GET, 5) if segs[3] == "test-reports" => Some((
            "GetDeliverabilityTestReport",
            Some(decode_segment(&segs[4])),
            None,
        )),
        (&Method::GET, 5) if segs[3] == "campaigns" => Some((
            "GetDomainDeliverabilityCampaign",
            Some(decode_segment(&segs[4])),
            None,
        )),
        (&Method::GET, 5) if segs[3] == "statistics-report" => Some((
            "GetDomainStatisticsReport",
            Some(decode_segment(&segs[4])),
            None,
        )),
        (&Method::GET, 6) if segs[3] == "domains" && segs[5] == "campaigns" => Some((
            "ListDomainDeliverabilityCampaigns",
            Some(decode_segment(&segs[4])),
            None,
        )),
        _ => None,
    }
}

pub(crate) fn resolve_dedicated_ip_pools_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateDedicatedIpPool", None, None)),
        (&Method::GET, 3) => Some(("ListDedicatedIpPools", None, None)),
        (&Method::GET, 4) => Some(("GetDedicatedIpPool", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteDedicatedIpPool", resource, None)),
        (&Method::PUT, 5) if segs[4] == "scaling" => {
            Some(("PutDedicatedIpPoolScalingAttributes", resource, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_dedicated_ips_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::GET, 3) => Some(("GetDedicatedIps", None, None)),
        (&Method::GET, 4) => Some(("GetDedicatedIp", resource, None)),
        (&Method::PUT, 5) if segs[4] == "pool" => Some(("PutDedicatedIpInPool", resource, None)),
        (&Method::PUT, 5) if segs[4] == "warmup" => {
            Some(("PutDedicatedIpWarmupAttributes", resource, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_multi_region_endpoints_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateMultiRegionEndpoint", None, None)),
        (&Method::GET, 3) => Some(("ListMultiRegionEndpoints", None, None)),
        (&Method::GET, 4) => Some(("GetMultiRegionEndpoint", resource, None)),
        (&Method::DELETE, 4) => Some(("DeleteMultiRegionEndpoint", resource, None)),
        _ => None,
    }
}

pub(crate) fn resolve_import_jobs_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateImportJob", None, None)),
        (&Method::POST, 4) if segs[3] == "list" => Some(("ListImportJobs", None, None)),
        (&Method::GET, 4) => Some(("GetImportJob", resource, None)),
        _ => None,
    }
}

pub(crate) fn resolve_export_jobs_action(
    method: &Method,
    segs: &[String],
    resource: Option<String>,
) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateExportJob", None, None)),
        (&Method::GET, 4) => Some(("GetExportJob", resource, None)),
        (&Method::PUT, 5) if segs[4] == "cancel" => Some(("CancelExportJob", resource, None)),
        _ => None,
    }
}

pub(crate) fn resolve_tenants_action(method: &Method, segs: &[String]) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 3) => Some(("CreateTenant", None, None)),
        (&Method::POST, 4) if segs[3] == "list" => Some(("ListTenants", None, None)),
        (&Method::POST, 4) if segs[3] == "get" => Some(("GetTenant", None, None)),
        (&Method::POST, 4) if segs[3] == "delete" => Some(("DeleteTenant", None, None)),
        (&Method::POST, 4) if segs[3] == "resources" => {
            Some(("CreateTenantResourceAssociation", None, None))
        }
        (&Method::POST, 5) if segs[3] == "resources" && segs[4] == "delete" => {
            Some(("DeleteTenantResourceAssociation", None, None))
        }
        (&Method::POST, 5) if segs[3] == "resources" && segs[4] == "list" => {
            Some(("ListTenantResources", None, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_resources_action(method: &Method, segs: &[String]) -> ResolvedAction {
    match (method, segs.len()) {
        (&Method::POST, 5) if segs[3] == "tenants" && segs[4] == "list" => {
            Some(("ListResourceTenants", None, None))
        }
        _ => None,
    }
}

pub(crate) fn resolve_reputation_action(method: &Method, segs: &[String]) -> ResolvedAction {
    if segs.get(3).map(|s| s.as_str()) != Some("entities") {
        return None;
    }
    match (method, segs.len()) {
        (&Method::POST, 4) => Some(("ListReputationEntities", None, None)),
        (&Method::GET, 6) => Some((
            "GetReputationEntity",
            Some(decode_segment(&segs[4])),
            Some(decode_segment(&segs[5])),
        )),
        (&Method::PUT, 7) if segs[6] == "customer-managed-status" => Some((
            "UpdateReputationEntityCustomerManagedStatus",
            Some(decode_segment(&segs[4])),
            Some(decode_segment(&segs[5])),
        )),
        (&Method::PUT, 7) if segs[6] == "policy" => Some((
            "UpdateReputationEntityPolicy",
            Some(decode_segment(&segs[4])),
            Some(decode_segment(&segs[5])),
        )),
        _ => None,
    }
}

pub(crate) fn parse_topics(value: &Value) -> Vec<Topic> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let display_name = v["DisplayName"].as_str().unwrap_or("").to_string();
                    let description = v["Description"].as_str().unwrap_or("").to_string();
                    let default_subscription_status = v["DefaultSubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(Topic {
                        topic_name,
                        display_name,
                        description,
                        default_subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn parse_topic_preferences(value: &Value) -> Vec<TopicPreference> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let subscription_status = v["SubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(TopicPreference {
                        topic_name,
                        subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn extract_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn parse_event_destination_definition(name: &str, def: &Value) -> EventDestination {
    let enabled = def["Enabled"].as_bool().unwrap_or(false);
    let matching_event_types = extract_string_array(&def["MatchingEventTypes"]);
    let kinesis_firehose_destination = def
        .get("KinesisFirehoseDestination")
        .filter(|v| v.is_object())
        .cloned();
    let cloud_watch_destination = def
        .get("CloudWatchDestination")
        .filter(|v| v.is_object())
        .cloned();
    let sns_destination = def.get("SnsDestination").filter(|v| v.is_object()).cloned();
    let event_bridge_destination = def
        .get("EventBridgeDestination")
        .filter(|v| v.is_object())
        .cloned();
    let pinpoint_destination = def
        .get("PinpointDestination")
        .filter(|v| v.is_object())
        .cloned();

    EventDestination {
        name: name.to_string(),
        enabled,
        matching_event_types,
        kinesis_firehose_destination,
        cloud_watch_destination,
        sns_destination,
        event_bridge_destination,
        pinpoint_destination,
    }
}

pub(crate) fn event_destination_to_json(dest: &EventDestination) -> Value {
    let mut obj = json!({
        "Name": dest.name,
        "Enabled": dest.enabled,
        "MatchingEventTypes": dest.matching_event_types,
    });
    if let Some(ref v) = dest.kinesis_firehose_destination {
        obj["KinesisFirehoseDestination"] = v.clone();
    }
    if let Some(ref v) = dest.cloud_watch_destination {
        obj["CloudWatchDestination"] = v.clone();
    }
    if let Some(ref v) = dest.sns_destination {
        obj["SnsDestination"] = v.clone();
    }
    if let Some(ref v) = dest.event_bridge_destination {
        obj["EventBridgeDestination"] = v.clone();
    }
    if let Some(ref v) = dest.pinpoint_destination {
        obj["PinpointDestination"] = v.clone();
    }
    obj
}

pub(crate) fn is_mutating_action(action: &str) -> bool {
    const MUTATING_PREFIXES: &[&str] = &[
        "Create",
        "Update",
        "Delete",
        "Put",
        "Tag",
        "Untag",
        "Send",
        "Cancel",
        "Verify",
        "Set",
        "Clone",
        "Reorder",
        "BatchUpdate",
    ];
    MUTATING_PREFIXES.iter().any(|p| action.starts_with(p))
}
