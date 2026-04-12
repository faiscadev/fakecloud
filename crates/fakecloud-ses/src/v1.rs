//! SES v1 Query protocol handlers for identity management, sending,
//! templates, configuration sets, receipt rules, receipt filters,
//! and inbound email processing.

use chrono::Utc;
use http::StatusCode;
use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    ConfigurationSet, EmailIdentity, EmailTemplate, EventDestination, IpFilter, ReceiptAction,
    ReceiptFilter, ReceiptRule, ReceiptRuleSet, SentEmail, SharedSesState,
};

/// XML namespace for SES v1 responses.
const SES_NS: &str = "http://ses.amazonaws.com/doc/2010-12-01/";

/// Wrap a v1 action result in the standard SES Query protocol XML envelope.
fn xml_wrap(action: &str, inner: &str, request_id: &str) -> String {
    fakecloud_core::query::query_response_xml(action, SES_NS, inner, request_id)
}

/// Response with only metadata (no result body).
fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    let xml = fakecloud_core::query::query_metadata_only_xml(action, SES_NS, request_id);
    AwsResponse::xml(StatusCode::OK, xml)
}

/// Dispatch a v1 Query protocol action.
pub fn handle_v1_action(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        // Identity management
        "VerifyEmailIdentity" => verify_email_identity(state, req),
        "VerifyDomainIdentity" => verify_domain_identity(state, req),
        "VerifyDomainDkim" => verify_domain_dkim(state, req),
        "ListIdentities" => list_identities(state, req),
        "GetIdentityVerificationAttributes" => get_identity_verification_attributes(state, req),
        "GetIdentityDkimAttributes" => get_identity_dkim_attributes(state, req),
        "DeleteIdentity" => delete_identity(state, req),
        "SetIdentityDkimEnabled" => set_identity_dkim_enabled(state, req),
        // Identity notification/mail-from attributes
        "SetIdentityNotificationTopic" => set_identity_notification_topic(state, req),
        "SetIdentityFeedbackForwardingEnabled" => {
            set_identity_feedback_forwarding_enabled(state, req)
        }
        "GetIdentityNotificationAttributes" => get_identity_notification_attributes(state, req),
        "GetIdentityMailFromDomainAttributes" => {
            get_identity_mail_from_domain_attributes(state, req)
        }
        "SetIdentityMailFromDomain" => set_identity_mail_from_domain(state, req),
        // Sending
        "SendEmail" => send_email(state, req),
        "SendRawEmail" => send_raw_email(state, req),
        "SendTemplatedEmail" => send_templated_email(state, req),
        "SendBulkTemplatedEmail" => send_bulk_templated_email(state, req),
        // Templates
        "CreateTemplate" => create_template(state, req),
        "GetTemplate" => get_template(state, req),
        "ListTemplates" => list_templates(state, req),
        "DeleteTemplate" => delete_template(state, req),
        "UpdateTemplate" => update_template(state, req),
        // Configuration Sets
        "CreateConfigurationSet" => create_configuration_set(state, req),
        "DeleteConfigurationSet" => delete_configuration_set(state, req),
        "DescribeConfigurationSet" => describe_configuration_set(state, req),
        "ListConfigurationSets" => list_configuration_sets(state, req),
        // Configuration Set Event Destinations
        "CreateConfigurationSetEventDestination" => {
            create_configuration_set_event_destination(state, req)
        }
        "UpdateConfigurationSetEventDestination" => {
            update_configuration_set_event_destination(state, req)
        }
        "DeleteConfigurationSetEventDestination" => {
            delete_configuration_set_event_destination(state, req)
        }
        // Account / Quota
        "GetSendQuota" => get_send_quota(state, req),
        "GetSendStatistics" => get_send_statistics(state, req),
        "GetAccountSendingEnabled" => get_account_sending_enabled(state, req),
        // Receipt Rule Sets
        "CreateReceiptRuleSet" => create_receipt_rule_set(state, req),
        "DeleteReceiptRuleSet" => delete_receipt_rule_set(state, req),
        "DescribeReceiptRuleSet" => describe_receipt_rule_set(state, req),
        "ListReceiptRuleSets" => list_receipt_rule_sets(state, req),
        "CloneReceiptRuleSet" => clone_receipt_rule_set(state, req),
        "SetActiveReceiptRuleSet" => set_active_receipt_rule_set(state, req),
        "ReorderReceiptRuleSet" => reorder_receipt_rule_set(state, req),
        // Receipt Rules
        "CreateReceiptRule" => create_receipt_rule(state, req),
        "DeleteReceiptRule" => delete_receipt_rule(state, req),
        "DescribeReceiptRule" => describe_receipt_rule(state, req),
        "UpdateReceiptRule" => update_receipt_rule(state, req),
        // Receipt Filters
        "CreateReceiptFilter" => create_receipt_filter(state, req),
        "DeleteReceiptFilter" => delete_receipt_filter(state, req),
        "ListReceiptFilters" => list_receipt_filters(state, req),
        _ => Err(AwsServiceError::action_not_implemented("ses", &req.action)),
    }
}

/// List of v1 actions supported.
pub const V1_ACTIONS: &[&str] = &[
    // Identity management
    "VerifyEmailIdentity",
    "VerifyDomainIdentity",
    "VerifyDomainDkim",
    "ListIdentities",
    "GetIdentityVerificationAttributes",
    "GetIdentityDkimAttributes",
    "DeleteIdentity",
    "SetIdentityDkimEnabled",
    // Identity notification/mail-from
    "SetIdentityNotificationTopic",
    "SetIdentityFeedbackForwardingEnabled",
    "GetIdentityNotificationAttributes",
    "GetIdentityMailFromDomainAttributes",
    "SetIdentityMailFromDomain",
    // Sending
    "SendEmail",
    "SendRawEmail",
    "SendTemplatedEmail",
    "SendBulkTemplatedEmail",
    // Templates
    "CreateTemplate",
    "GetTemplate",
    "ListTemplates",
    "DeleteTemplate",
    "UpdateTemplate",
    // Configuration Sets
    "CreateConfigurationSet",
    "DeleteConfigurationSet",
    "DescribeConfigurationSet",
    "ListConfigurationSets",
    "CreateConfigurationSetEventDestination",
    "UpdateConfigurationSetEventDestination",
    "DeleteConfigurationSetEventDestination",
    // Account / Quota
    "GetSendQuota",
    "GetSendStatistics",
    "GetAccountSendingEnabled",
    // Receipt Rule Sets
    "CreateReceiptRuleSet",
    "DeleteReceiptRuleSet",
    "DescribeReceiptRuleSet",
    "ListReceiptRuleSets",
    "CloneReceiptRuleSet",
    "SetActiveReceiptRuleSet",
    "ReorderReceiptRuleSet",
    // Receipt Rules
    "CreateReceiptRule",
    "DeleteReceiptRule",
    "DescribeReceiptRule",
    "UpdateReceiptRule",
    // Receipt Filters
    "CreateReceiptFilter",
    "DeleteReceiptFilter",
    "ListReceiptFilters",
];

// ── Helpers ──

fn required_param<'a>(
    params: &'a HashMap<String, String>,
    key: &str,
) -> Result<&'a str, AwsServiceError> {
    params.get(key).map(|s| s.as_str()).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            format!("Value for parameter {key} is required"),
        )
    })
}

/// Parse a receipt rule from form parameters (for Create/Update).
fn parse_receipt_rule(params: &HashMap<String, String>) -> Result<ReceiptRule, AwsServiceError> {
    let name = required_param(params, "Rule.Name")?.to_string();
    let enabled = params
        .get("Rule.Enabled")
        .map(|v| v == "true")
        .unwrap_or(false);
    let scan_enabled = params
        .get("Rule.ScanEnabled")
        .map(|v| v == "true")
        .unwrap_or(false);
    let tls_policy = params
        .get("Rule.TlsPolicy")
        .cloned()
        .unwrap_or_else(|| "Optional".to_string());

    // Parse recipients: Rule.Recipients.member.1, Rule.Recipients.member.2, ...
    let mut recipients = Vec::new();
    for i in 1.. {
        let key = format!("Rule.Recipients.member.{i}");
        match params.get(&key) {
            Some(v) => recipients.push(v.clone()),
            None => break,
        }
    }

    // Parse actions: Rule.Actions.member.1.*, Rule.Actions.member.2.*, ...
    let mut actions = Vec::new();
    for i in 1.. {
        let prefix = format!("Rule.Actions.member.{i}");
        // Detect which action type is present
        if let Some(action) = parse_action(params, &prefix) {
            actions.push(action);
        } else {
            break;
        }
    }

    Ok(ReceiptRule {
        name,
        enabled,
        scan_enabled,
        tls_policy,
        recipients,
        actions,
    })
}

fn parse_action(params: &HashMap<String, String>, prefix: &str) -> Option<ReceiptAction> {
    // S3Action
    if let Some(bucket) = params.get(&format!("{prefix}.S3Action.BucketName")) {
        return Some(ReceiptAction::S3 {
            bucket_name: bucket.clone(),
            object_key_prefix: params
                .get(&format!("{prefix}.S3Action.ObjectKeyPrefix"))
                .cloned(),
            topic_arn: params.get(&format!("{prefix}.S3Action.TopicArn")).cloned(),
            kms_key_arn: params.get(&format!("{prefix}.S3Action.KmsKeyArn")).cloned(),
        });
    }
    // SNSAction
    if let Some(topic_arn) = params.get(&format!("{prefix}.SNSAction.TopicArn")) {
        return Some(ReceiptAction::Sns {
            topic_arn: topic_arn.clone(),
            encoding: params.get(&format!("{prefix}.SNSAction.Encoding")).cloned(),
        });
    }
    // LambdaAction
    if let Some(function_arn) = params.get(&format!("{prefix}.LambdaAction.FunctionArn")) {
        return Some(ReceiptAction::Lambda {
            function_arn: function_arn.clone(),
            invocation_type: params
                .get(&format!("{prefix}.LambdaAction.InvocationType"))
                .cloned(),
            topic_arn: params
                .get(&format!("{prefix}.LambdaAction.TopicArn"))
                .cloned(),
        });
    }
    // BounceAction
    if let Some(smtp_code) = params.get(&format!("{prefix}.BounceAction.SmtpReplyCode")) {
        return Some(ReceiptAction::Bounce {
            smtp_reply_code: smtp_code.clone(),
            message: params
                .get(&format!("{prefix}.BounceAction.Message"))
                .cloned()
                .unwrap_or_default(),
            sender: params
                .get(&format!("{prefix}.BounceAction.Sender"))
                .cloned()
                .unwrap_or_default(),
            status_code: params
                .get(&format!("{prefix}.BounceAction.StatusCode"))
                .cloned(),
            topic_arn: params
                .get(&format!("{prefix}.BounceAction.TopicArn"))
                .cloned(),
        });
    }
    // AddHeaderAction
    if let Some(header_name) = params.get(&format!("{prefix}.AddHeaderAction.HeaderName")) {
        return Some(ReceiptAction::AddHeader {
            header_name: header_name.clone(),
            header_value: params
                .get(&format!("{prefix}.AddHeaderAction.HeaderValue"))
                .cloned()
                .unwrap_or_default(),
        });
    }
    // StopAction
    if let Some(scope) = params.get(&format!("{prefix}.StopAction.Scope")) {
        return Some(ReceiptAction::Stop {
            scope: scope.clone(),
            topic_arn: params
                .get(&format!("{prefix}.StopAction.TopicArn"))
                .cloned(),
        });
    }
    None
}

/// Serialize a ReceiptRule to XML.
fn rule_to_xml(rule: &ReceiptRule) -> String {
    let mut xml = String::new();
    xml.push_str("<member>");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(&rule.name)));
    xml.push_str(&format!("<Enabled>{}</Enabled>", rule.enabled));
    xml.push_str(&format!("<ScanEnabled>{}</ScanEnabled>", rule.scan_enabled));
    xml.push_str(&format!(
        "<TlsPolicy>{}</TlsPolicy>",
        xml_escape(&rule.tls_policy)
    ));
    if !rule.recipients.is_empty() {
        xml.push_str("<Recipients>");
        for r in &rule.recipients {
            xml.push_str(&format!("<member>{}</member>", xml_escape(r)));
        }
        xml.push_str("</Recipients>");
    }
    if !rule.actions.is_empty() {
        xml.push_str("<Actions>");
        for action in &rule.actions {
            xml.push_str("<member>");
            match action {
                ReceiptAction::S3 {
                    bucket_name,
                    object_key_prefix,
                    topic_arn,
                    kms_key_arn,
                } => {
                    xml.push_str("<S3Action>");
                    xml.push_str(&format!(
                        "<BucketName>{}</BucketName>",
                        xml_escape(bucket_name)
                    ));
                    if let Some(p) = object_key_prefix {
                        xml.push_str(&format!(
                            "<ObjectKeyPrefix>{}</ObjectKeyPrefix>",
                            xml_escape(p)
                        ));
                    }
                    if let Some(t) = topic_arn {
                        xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(t)));
                    }
                    if let Some(k) = kms_key_arn {
                        xml.push_str(&format!("<KmsKeyArn>{}</KmsKeyArn>", xml_escape(k)));
                    }
                    xml.push_str("</S3Action>");
                }
                ReceiptAction::Sns {
                    topic_arn,
                    encoding,
                } => {
                    xml.push_str("<SNSAction>");
                    xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(topic_arn)));
                    if let Some(e) = encoding {
                        xml.push_str(&format!("<Encoding>{}</Encoding>", xml_escape(e)));
                    }
                    xml.push_str("</SNSAction>");
                }
                ReceiptAction::Lambda {
                    function_arn,
                    invocation_type,
                    topic_arn,
                } => {
                    xml.push_str("<LambdaAction>");
                    xml.push_str(&format!(
                        "<FunctionArn>{}</FunctionArn>",
                        xml_escape(function_arn)
                    ));
                    if let Some(t) = invocation_type {
                        xml.push_str(&format!(
                            "<InvocationType>{}</InvocationType>",
                            xml_escape(t)
                        ));
                    }
                    if let Some(t) = topic_arn {
                        xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(t)));
                    }
                    xml.push_str("</LambdaAction>");
                }
                ReceiptAction::Bounce {
                    smtp_reply_code,
                    message,
                    sender,
                    status_code,
                    topic_arn,
                } => {
                    xml.push_str("<BounceAction>");
                    xml.push_str(&format!(
                        "<SmtpReplyCode>{}</SmtpReplyCode>",
                        xml_escape(smtp_reply_code)
                    ));
                    xml.push_str(&format!("<Message>{}</Message>", xml_escape(message)));
                    xml.push_str(&format!("<Sender>{}</Sender>", xml_escape(sender)));
                    if let Some(sc) = status_code {
                        xml.push_str(&format!("<StatusCode>{}</StatusCode>", xml_escape(sc)));
                    }
                    if let Some(t) = topic_arn {
                        xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(t)));
                    }
                    xml.push_str("</BounceAction>");
                }
                ReceiptAction::AddHeader {
                    header_name,
                    header_value,
                } => {
                    xml.push_str("<AddHeaderAction>");
                    xml.push_str(&format!(
                        "<HeaderName>{}</HeaderName>",
                        xml_escape(header_name)
                    ));
                    xml.push_str(&format!(
                        "<HeaderValue>{}</HeaderValue>",
                        xml_escape(header_value)
                    ));
                    xml.push_str("</AddHeaderAction>");
                }
                ReceiptAction::Stop { scope, topic_arn } => {
                    xml.push_str("<StopAction>");
                    xml.push_str(&format!("<Scope>{}</Scope>", xml_escape(scope)));
                    if let Some(t) = topic_arn {
                        xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(t)));
                    }
                    xml.push_str("</StopAction>");
                }
            }
            xml.push_str("</member>");
        }
        xml.push_str("</Actions>");
    }
    xml.push_str("</member>");
    xml
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

// ── Identity management operations ──

fn verify_email_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let email = required_param(&req.query_params, "EmailAddress")?;
    let mut st = state.write();
    st.identities
        .entry(email.to_string())
        .or_insert_with(|| EmailIdentity {
            identity_name: email.to_string(),
            identity_type: "EmailAddress".to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: false,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            configuration_set_name: None,
        });
    Ok(xml_metadata_only("VerifyEmailIdentity", &req.request_id))
}

fn verify_domain_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = required_param(&req.query_params, "Domain")?;
    let mut st = state.write();
    st.identities
        .entry(domain.to_string())
        .or_insert_with(|| EmailIdentity {
            identity_name: domain.to_string(),
            identity_type: "Domain".to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: false,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            configuration_set_name: None,
        });
    // Return a verification token
    let token = format!("{:x}{:x}{:x}", rand_u64(), rand_u64(), rand_u64());
    let inner = format!("<VerificationToken>{token}</VerificationToken>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("VerifyDomainIdentity", &inner, &req.request_id),
    ))
}

fn verify_domain_dkim(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = required_param(&req.query_params, "Domain")?;
    // Ensure identity exists
    let mut st = state.write();
    st.identities
        .entry(domain.to_string())
        .or_insert_with(|| EmailIdentity {
            identity_name: domain.to_string(),
            identity_type: "Domain".to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            configuration_set_name: None,
        });
    // Return 3 DKIM tokens
    let mut inner = String::from("<DkimTokens>");
    for _ in 0..3 {
        let token = format!("{:x}{:x}", rand_u64(), rand_u64());
        inner.push_str(&format!("<member>{token}</member>"));
    }
    inner.push_str("</DkimTokens>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("VerifyDomainDkim", &inner, &req.request_id),
    ))
}

fn list_identities(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity_type = req.query_params.get("IdentityType");
    let st = state.read();
    let mut inner = String::from("<Identities>");
    let mut identities: Vec<&EmailIdentity> = st.identities.values().collect();
    identities.sort_by_key(|i| &i.identity_name);
    for identity in identities {
        let include = match identity_type.map(|s| s.as_str()) {
            Some("EmailAddress") => identity.identity_type == "EmailAddress",
            Some("Domain") => identity.identity_type == "Domain",
            _ => true,
        };
        if include {
            inner.push_str(&format!(
                "<member>{}</member>",
                xml_escape(&identity.identity_name)
            ));
        }
    }
    inner.push_str("</Identities>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("ListIdentities", &inner, &req.request_id),
    ))
}

fn get_identity_verification_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<VerificationAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_name.as_str()) {
                    let status = if identity.verified {
                        "Success"
                    } else {
                        "Pending"
                    };
                    inner.push_str(&format!(
                        "<VerificationStatus>{status}</VerificationStatus>"
                    ));
                    if identity.identity_type == "Domain" {
                        let token = format!("{:x}", rand_u64());
                        inner.push_str(&format!("<VerificationToken>{token}</VerificationToken>"));
                    }
                } else {
                    inner.push_str("<VerificationStatus>NotStarted</VerificationStatus>");
                }
                inner.push_str("</value>");
                inner.push_str("</entry>");
            }
            None => break,
        }
    }
    inner.push_str("</VerificationAttributes>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetIdentityVerificationAttributes", &inner, &req.request_id),
    ))
}

fn get_identity_dkim_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<DkimAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_name.as_str()) {
                    let enabled = identity.dkim_signing_enabled;
                    let status = if identity.verified {
                        "Success"
                    } else {
                        "Pending"
                    };
                    inner.push_str(&format!(
                        "<DkimEnabled>{enabled}</DkimEnabled>\
                         <DkimVerificationStatus>{status}</DkimVerificationStatus>"
                    ));
                    // Return DKIM tokens for domains
                    if identity.identity_type == "Domain" {
                        inner.push_str("<DkimTokens>");
                        for _ in 0..3 {
                            let token = format!("{:x}{:x}", rand_u64(), rand_u64());
                            inner.push_str(&format!("<member>{token}</member>"));
                        }
                        inner.push_str("</DkimTokens>");
                    }
                } else {
                    inner.push_str(
                        "<DkimEnabled>false</DkimEnabled>\
                         <DkimVerificationStatus>NotStarted</DkimVerificationStatus>",
                    );
                }
                inner.push_str("</value>");
                inner.push_str("</entry>");
            }
            None => break,
        }
    }
    inner.push_str("</DkimAttributes>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetIdentityDkimAttributes", &inner, &req.request_id),
    ))
}

fn delete_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    state.write().identities.remove(identity);
    Ok(xml_metadata_only("DeleteIdentity", &req.request_id))
}

fn set_identity_dkim_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let enabled = required_param(&req.query_params, "DkimEnabled")? == "true";
    let mut st = state.write();
    if let Some(id) = st.identities.get_mut(identity) {
        id.dkim_signing_enabled = enabled;
    }
    Ok(xml_metadata_only("SetIdentityDkimEnabled", &req.request_id))
}

// ── Identity notification/mail-from attribute operations ──

fn set_identity_notification_topic(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let _identity = required_param(&req.query_params, "Identity")?;
    let _notification_type = required_param(&req.query_params, "NotificationType")?;
    // SnsTopic is optional — if absent, disables notification
    let _sns_topic = req.query_params.get("SnsTopic");
    // We store this on the identity but currently don't have notification topic fields.
    // For fakecloud, accepting the call is sufficient — notifications aren't sent.
    // Verify identity exists
    let st = state.read();
    if !st.identities.contains_key(_identity) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Identity '{_identity}' does not exist"),
        ));
    }
    drop(st);
    Ok(xml_metadata_only(
        "SetIdentityNotificationTopic",
        &req.request_id,
    ))
}

fn set_identity_feedback_forwarding_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let enabled = required_param(&req.query_params, "ForwardingEnabled")? == "true";
    let mut st = state.write();
    if let Some(id) = st.identities.get_mut(identity) {
        id.email_forwarding_enabled = enabled;
    } else {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Identity '{identity}' does not exist"),
        ));
    }
    Ok(xml_metadata_only(
        "SetIdentityFeedbackForwardingEnabled",
        &req.request_id,
    ))
}

fn get_identity_notification_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<NotificationAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_name.as_str()) {
                    inner.push_str(&format!(
                        "<ForwardingEnabled>{}</ForwardingEnabled>\
                         <HeadersInBounceNotificationsEnabled>false</HeadersInBounceNotificationsEnabled>\
                         <HeadersInComplaintNotificationsEnabled>false</HeadersInComplaintNotificationsEnabled>\
                         <HeadersInDeliveryNotificationsEnabled>false</HeadersInDeliveryNotificationsEnabled>",
                        identity.email_forwarding_enabled,
                    ));
                } else {
                    inner.push_str(
                        "<ForwardingEnabled>true</ForwardingEnabled>\
                         <HeadersInBounceNotificationsEnabled>false</HeadersInBounceNotificationsEnabled>\
                         <HeadersInComplaintNotificationsEnabled>false</HeadersInComplaintNotificationsEnabled>\
                         <HeadersInDeliveryNotificationsEnabled>false</HeadersInDeliveryNotificationsEnabled>",
                    );
                }
                inner.push_str("</value>");
                inner.push_str("</entry>");
            }
            None => break,
        }
    }
    inner.push_str("</NotificationAttributes>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetIdentityNotificationAttributes", &inner, &req.request_id),
    ))
}

fn get_identity_mail_from_domain_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<MailFromDomainAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_name.as_str()) {
                    let mail_from = identity.mail_from_domain.as_deref().unwrap_or("");
                    let behavior = &identity.mail_from_behavior_on_mx_failure;
                    let status = if mail_from.is_empty() {
                        "NotStarted"
                    } else {
                        "Success"
                    };
                    inner.push_str(&format!(
                        "<MailFromDomain>{}</MailFromDomain>\
                         <MailFromDomainStatus>{status}</MailFromDomainStatus>\
                         <BehaviorOnMXFailure>{}</BehaviorOnMXFailure>",
                        xml_escape(mail_from),
                        xml_escape(behavior),
                    ));
                } else {
                    inner.push_str(
                        "<MailFromDomain/>\
                         <MailFromDomainStatus>NotStarted</MailFromDomainStatus>\
                         <BehaviorOnMXFailure>UseDefaultValue</BehaviorOnMXFailure>",
                    );
                }
                inner.push_str("</value>");
                inner.push_str("</entry>");
            }
            None => break,
        }
    }
    inner.push_str("</MailFromDomainAttributes>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap(
            "GetIdentityMailFromDomainAttributes",
            &inner,
            &req.request_id,
        ),
    ))
}

fn set_identity_mail_from_domain(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let mail_from_domain = req.query_params.get("MailFromDomain").cloned();
    let behavior = req
        .query_params
        .get("BehaviorOnMXFailure")
        .cloned()
        .unwrap_or_else(|| "UseDefaultValue".to_string());
    let mut st = state.write();
    if let Some(id) = st.identities.get_mut(identity) {
        id.mail_from_domain = mail_from_domain.filter(|s| !s.is_empty());
        id.mail_from_behavior_on_mx_failure = behavior;
    } else {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Identity '{identity}' does not exist"),
        ));
    }
    Ok(xml_metadata_only(
        "SetIdentityMailFromDomain",
        &req.request_id,
    ))
}

// ── Sending operations ──

/// Parse a list of addresses from v1 query params (e.g. Message.Destination.ToAddresses.member.N)
fn parse_member_list(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
    let mut result = Vec::new();
    for i in 1.. {
        let key = format!("{prefix}.member.{i}");
        match params.get(&key) {
            Some(v) => result.push(v.clone()),
            None => break,
        }
    }
    result
}

fn send_email(state: &SharedSesState, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let to = parse_member_list(&req.query_params, "Destination.ToAddresses");
    let cc = parse_member_list(&req.query_params, "Destination.CcAddresses");
    let bcc = parse_member_list(&req.query_params, "Destination.BccAddresses");

    let subject = req.query_params.get("Message.Subject.Data").cloned();
    let html_body = req.query_params.get("Message.Body.Html.Data").cloned();
    let text_body = req.query_params.get("Message.Body.Text.Data").cloned();

    let message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    let sent = SentEmail {
        message_id: message_id.clone(),
        from: from.to_string(),
        to,
        cc,
        bcc,
        subject,
        html_body,
        text_body,
        raw_data: None,
        template_name: None,
        template_data: None,
        timestamp: Utc::now(),
    };

    state.write().sent_emails.push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("SendEmail", &inner, &req.request_id),
    ))
}

fn send_raw_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let raw_data = required_param(&req.query_params, "RawMessage.Data")?;
    let from = req.query_params.get("Source").cloned().unwrap_or_default();
    let to = parse_member_list(&req.query_params, "Destinations");

    let message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    let sent = SentEmail {
        message_id: message_id.clone(),
        from,
        to,
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: None,
        html_body: None,
        text_body: None,
        raw_data: Some(raw_data.to_string()),
        template_name: None,
        template_data: None,
        timestamp: Utc::now(),
    };

    state.write().sent_emails.push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("SendRawEmail", &inner, &req.request_id),
    ))
}

fn send_templated_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let template_name = required_param(&req.query_params, "Template")?;
    let template_data = required_param(&req.query_params, "TemplateData")?;
    let to = parse_member_list(&req.query_params, "Destination.ToAddresses");
    let cc = parse_member_list(&req.query_params, "Destination.CcAddresses");
    let bcc = parse_member_list(&req.query_params, "Destination.BccAddresses");

    // Verify template exists
    let st = state.read();
    if !st.templates.contains_key(template_name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "TemplateDoesNotExistException",
            format!("Template '{template_name}' does not exist"),
        ));
    }
    drop(st);

    let message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    let sent = SentEmail {
        message_id: message_id.clone(),
        from: from.to_string(),
        to,
        cc,
        bcc,
        subject: None,
        html_body: None,
        text_body: None,
        raw_data: None,
        template_name: Some(template_name.to_string()),
        template_data: Some(template_data.to_string()),
        timestamp: Utc::now(),
    };

    state.write().sent_emails.push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("SendTemplatedEmail", &inner, &req.request_id),
    ))
}

fn send_bulk_templated_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let template_name = required_param(&req.query_params, "Template")?;
    let default_template_data = req
        .query_params
        .get("DefaultTemplateData")
        .cloned()
        .unwrap_or_else(|| "{}".to_string());

    // Verify template exists
    let st = state.read();
    if !st.templates.contains_key(template_name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "TemplateDoesNotExistException",
            format!("Template '{template_name}' does not exist"),
        ));
    }
    drop(st);

    // Parse destinations: Destinations.member.N.Destination.ToAddresses.member.M
    let mut inner = String::from("<Status>");
    for i in 1.. {
        let dest_prefix = format!("Destinations.member.{i}");
        let to_key = format!("{dest_prefix}.Destination.ToAddresses.member.1");
        if !req.query_params.contains_key(&to_key) {
            break;
        }
        let to = parse_member_list(
            &req.query_params,
            &format!("{dest_prefix}.Destination.ToAddresses"),
        );
        let replacement_data = req
            .query_params
            .get(&format!("{dest_prefix}.ReplacementTemplateData"))
            .cloned()
            .unwrap_or_else(|| default_template_data.clone());

        let message_id = format!(
            "{:016x}{:016x}-{:08x}-{:04x}",
            rand_u64(),
            rand_u64(),
            rand_u32(),
            rand_u16(),
        );

        let sent = SentEmail {
            message_id: message_id.clone(),
            from: from.to_string(),
            to,
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: Some(template_name.to_string()),
            template_data: Some(replacement_data),
            timestamp: Utc::now(),
        };

        state.write().sent_emails.push(sent);

        inner.push_str(&format!(
            "<member><Status>Success</Status><MessageId>{message_id}</MessageId></member>"
        ));
    }
    inner.push_str("</Status>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("SendBulkTemplatedEmail", &inner, &req.request_id),
    ))
}

// ── Template operations ──

fn create_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Template.TemplateName")?;
    let subject = req.query_params.get("Template.SubjectPart").cloned();
    let html = req.query_params.get("Template.HtmlPart").cloned();
    let text = req.query_params.get("Template.TextPart").cloned();

    let mut st = state.write();
    if st.templates.contains_key(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AlreadyExistsException",
            format!("Template '{name}' already exists"),
        ));
    }
    st.templates.insert(
        name.to_string(),
        EmailTemplate {
            template_name: name.to_string(),
            subject,
            html_body: html,
            text_body: text,
            created_at: Utc::now(),
        },
    );
    Ok(xml_metadata_only("CreateTemplate", &req.request_id))
}

fn get_template(state: &SharedSesState, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "TemplateName")?;
    let st = state.read();
    let template = st.templates.get(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "TemplateDoesNotExistException",
            format!("Template '{name}' does not exist"),
        )
    })?;
    let mut inner = String::from("<Template>");
    inner.push_str(&format!(
        "<TemplateName>{}</TemplateName>",
        xml_escape(&template.template_name)
    ));
    if let Some(ref s) = template.subject {
        inner.push_str(&format!("<SubjectPart>{}</SubjectPart>", xml_escape(s)));
    }
    if let Some(ref h) = template.html_body {
        inner.push_str(&format!("<HtmlPart>{}</HtmlPart>", xml_escape(h)));
    }
    if let Some(ref t) = template.text_body {
        inner.push_str(&format!("<TextPart>{}</TextPart>", xml_escape(t)));
    }
    inner.push_str("</Template>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetTemplate", &inner, &req.request_id),
    ))
}

fn list_templates(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<TemplatesMetadata>");
    let mut templates: Vec<&EmailTemplate> = st.templates.values().collect();
    templates.sort_by_key(|t| &t.template_name);
    for t in templates {
        inner.push_str(&format!(
            "<member><Name>{}</Name><CreatedTimestamp>{}</CreatedTimestamp></member>",
            xml_escape(&t.template_name),
            t.created_at.to_rfc3339(),
        ));
    }
    inner.push_str("</TemplatesMetadata>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("ListTemplates", &inner, &req.request_id),
    ))
}

fn delete_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "TemplateName")?;
    state.write().templates.remove(name);
    // AWS returns success even if template doesn't exist
    Ok(xml_metadata_only("DeleteTemplate", &req.request_id))
}

fn update_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Template.TemplateName")?;
    let mut st = state.write();
    let template = st.templates.get_mut(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "TemplateDoesNotExistException",
            format!("Template '{name}' does not exist"),
        )
    })?;
    if let Some(s) = req.query_params.get("Template.SubjectPart") {
        template.subject = Some(s.clone());
    }
    if let Some(h) = req.query_params.get("Template.HtmlPart") {
        template.html_body = Some(h.clone());
    }
    if let Some(t) = req.query_params.get("Template.TextPart") {
        template.text_body = Some(t.clone());
    }
    Ok(xml_metadata_only("UpdateTemplate", &req.request_id))
}

// ── Configuration Set operations ──

fn create_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSet.Name")?;
    let mut st = state.write();
    if st.configuration_sets.contains_key(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConfigurationSetAlreadyExistsException",
            format!("Configuration set '{name}' already exists"),
        ));
    }
    st.configuration_sets.insert(
        name.to_string(),
        ConfigurationSet {
            name: name.to_string(),
            sending_enabled: true,
            tls_policy: "Optional".to_string(),
            sending_pool_name: None,
            custom_redirect_domain: None,
            https_policy: None,
            suppressed_reasons: Vec::new(),
            reputation_metrics_enabled: false,
            vdm_options: None,
            archive_arn: None,
        },
    );
    Ok(xml_metadata_only("CreateConfigurationSet", &req.request_id))
}

fn delete_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSetName")?;
    let mut st = state.write();
    if st.configuration_sets.remove(name).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConfigurationSetDoesNotExistException",
            format!("Configuration set '{name}' does not exist"),
        ));
    }
    // Also remove event destinations for this config set
    st.event_destinations.remove(name);
    Ok(xml_metadata_only("DeleteConfigurationSet", &req.request_id))
}

fn describe_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSetName")?;
    let st = state.read();
    let cs = st.configuration_sets.get(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConfigurationSetDoesNotExistException",
            format!("Configuration set '{name}' does not exist"),
        )
    })?;
    let mut inner = format!(
        "<ConfigurationSet><Name>{}</Name></ConfigurationSet>",
        xml_escape(&cs.name)
    );
    // Include event destinations if requested
    if let Some(dests) = st.event_destinations.get(name) {
        inner.push_str("<EventDestinations>");
        for dest in dests {
            inner.push_str(&format!(
                "<member><Name>{}</Name><Enabled>{}</Enabled>\
                 <MatchingEventTypes>",
                xml_escape(&dest.name),
                dest.enabled,
            ));
            for et in &dest.matching_event_types {
                inner.push_str(&format!("<member>{}</member>", xml_escape(et)));
            }
            inner.push_str("</MatchingEventTypes></member>");
        }
        inner.push_str("</EventDestinations>");
    }
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("DescribeConfigurationSet", &inner, &req.request_id),
    ))
}

fn list_configuration_sets(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<ConfigurationSets>");
    let mut sets: Vec<&ConfigurationSet> = st.configuration_sets.values().collect();
    sets.sort_by_key(|cs| &cs.name);
    for cs in sets {
        inner.push_str(&format!(
            "<member><Name>{}</Name></member>",
            xml_escape(&cs.name)
        ));
    }
    inner.push_str("</ConfigurationSets>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("ListConfigurationSets", &inner, &req.request_id),
    ))
}

// ── Configuration Set Event Destination operations ──

fn create_configuration_set_event_destination(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let config_set_name = required_param(&req.query_params, "ConfigurationSetName")?;
    let dest_name = required_param(&req.query_params, "EventDestination.Name")?;
    let enabled = req
        .query_params
        .get("EventDestination.Enabled")
        .map(|v| v == "true")
        .unwrap_or(true);
    let event_types = parse_member_list(&req.query_params, "EventDestination.MatchingEventTypes");

    let st = state.read();
    if !st.configuration_sets.contains_key(config_set_name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConfigurationSetDoesNotExistException",
            format!("Configuration set '{config_set_name}' does not exist"),
        ));
    }
    drop(st);

    let dest = EventDestination {
        name: dest_name.to_string(),
        enabled,
        matching_event_types: event_types,
        kinesis_firehose_destination: None,
        cloud_watch_destination: None,
        sns_destination: req
            .query_params
            .get("EventDestination.SNSDestination.TopicARN")
            .map(|arn| serde_json::json!({ "TopicArn": arn })),
        event_bridge_destination: None,
        pinpoint_destination: None,
    };

    let mut st = state.write();
    st.event_destinations
        .entry(config_set_name.to_string())
        .or_default()
        .push(dest);
    Ok(xml_metadata_only(
        "CreateConfigurationSetEventDestination",
        &req.request_id,
    ))
}

fn update_configuration_set_event_destination(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let config_set_name = required_param(&req.query_params, "ConfigurationSetName")?;
    let dest_name = required_param(&req.query_params, "EventDestination.Name")?;

    let mut st = state.write();
    let dests = st
        .event_destinations
        .get_mut(config_set_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EventDestinationDoesNotExistException",
                format!("Event destination '{dest_name}' does not exist"),
            )
        })?;
    let dest = dests
        .iter_mut()
        .find(|d| d.name == dest_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EventDestinationDoesNotExistException",
                format!("Event destination '{dest_name}' does not exist"),
            )
        })?;

    if let Some(v) = req.query_params.get("EventDestination.Enabled") {
        dest.enabled = v == "true";
    }
    let event_types = parse_member_list(&req.query_params, "EventDestination.MatchingEventTypes");
    if !event_types.is_empty() {
        dest.matching_event_types = event_types;
    }

    Ok(xml_metadata_only(
        "UpdateConfigurationSetEventDestination",
        &req.request_id,
    ))
}

fn delete_configuration_set_event_destination(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let config_set_name = required_param(&req.query_params, "ConfigurationSetName")?;
    let dest_name = required_param(&req.query_params, "EventDestinationName")?;

    let mut st = state.write();
    let dests = st
        .event_destinations
        .get_mut(config_set_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConfigurationSetDoesNotExistException",
                format!("Configuration set '{config_set_name}' does not exist"),
            )
        })?;
    let pos = dests
        .iter()
        .position(|d| d.name == dest_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EventDestinationDoesNotExistException",
                format!("Event destination '{dest_name}' does not exist"),
            )
        })?;
    dests.remove(pos);

    Ok(xml_metadata_only(
        "DeleteConfigurationSetEventDestination",
        &req.request_id,
    ))
}

// ── Account / Quota operations ──

fn get_send_quota(
    _state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inner = "\
        <Max24HourSend>50000.0</Max24HourSend>\
        <MaxSendRate>14.0</MaxSendRate>\
        <SentLast24Hours>0.0</SentLast24Hours>";
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetSendQuota", inner, &req.request_id),
    ))
}

fn get_send_statistics(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let count = st.sent_emails.len();
    let inner = format!(
        "<SendDataPoints>\
         <member>\
         <DeliveryAttempts>{count}</DeliveryAttempts>\
         <Bounces>0</Bounces>\
         <Complaints>0</Complaints>\
         <Rejects>0</Rejects>\
         <Timestamp>{}</Timestamp>\
         </member>\
         </SendDataPoints>",
        Utc::now().to_rfc3339()
    );
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetSendStatistics", &inner, &req.request_id),
    ))
}

fn get_account_sending_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let enabled = st.account_settings.sending_enabled;
    let inner = format!("<Enabled>{enabled}</Enabled>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("GetAccountSendingEnabled", &inner, &req.request_id),
    ))
}

// ── Simple random helpers (no external deps) ──

fn rand_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

fn rand_u32() -> u32 {
    rand_u64() as u32
}

fn rand_u16() -> u16 {
    rand_u64() as u16
}

// ── Receipt Rule Set operations ──

fn create_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let mut st = state.write();
    if st.receipt_rule_sets.contains_key(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AlreadyExistsException",
            format!("Rule set with name '{name}' already exists"),
        ));
    }
    st.receipt_rule_sets.insert(
        name.to_string(),
        ReceiptRuleSet {
            name: name.to_string(),
            rules: Vec::new(),
            created_at: Utc::now(),
        },
    );
    Ok(xml_metadata_only("CreateReceiptRuleSet", &req.request_id))
}

fn delete_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let mut st = state.write();
    if !st.receipt_rule_sets.contains_key(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{name}' does not exist"),
        ));
    }
    // Cannot delete the active rule set
    if st.active_receipt_rule_set.as_deref() == Some(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "CannotDeleteException",
            "Cannot delete the active receipt rule set. Deactivate it first.",
        ));
    }
    st.receipt_rule_sets.remove(name);
    Ok(xml_metadata_only("DeleteReceiptRuleSet", &req.request_id))
}

fn describe_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let st = state.read();
    let rule_set = st.receipt_rule_sets.get(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{name}' does not exist"),
        )
    })?;

    let mut rules_xml = String::from("<Rules>");
    for rule in &rule_set.rules {
        rules_xml.push_str(&rule_to_xml(rule));
    }
    rules_xml.push_str("</Rules>");

    let inner = format!(
        "<Metadata><Name>{}</Name><CreatedTimestamp>{}</CreatedTimestamp></Metadata>{}",
        xml_escape(&rule_set.name),
        rule_set.created_at.to_rfc3339(),
        rules_xml,
    );
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("DescribeReceiptRuleSet", &inner, &req.request_id),
    ))
}

fn list_receipt_rule_sets(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<RuleSets>");
    let mut sets: Vec<&ReceiptRuleSet> = st.receipt_rule_sets.values().collect();
    sets.sort_by_key(|s| &s.name);
    for rs in sets {
        inner.push_str(&format!(
            "<member><Name>{}</Name><CreatedTimestamp>{}</CreatedTimestamp></member>",
            xml_escape(&rs.name),
            rs.created_at.to_rfc3339(),
        ));
    }
    inner.push_str("</RuleSets>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("ListReceiptRuleSets", &inner, &req.request_id),
    ))
}

fn clone_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let new_name = required_param(&req.query_params, "RuleSetName")?;
    let source_name = required_param(&req.query_params, "OriginalRuleSetName")?;
    let mut st = state.write();

    if st.receipt_rule_sets.contains_key(new_name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AlreadyExistsException",
            format!("Rule set with name '{new_name}' already exists"),
        ));
    }
    let source = st.receipt_rule_sets.get(source_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{source_name}' does not exist"),
        )
    })?;
    let cloned = ReceiptRuleSet {
        name: new_name.to_string(),
        rules: source.rules.clone(),
        created_at: Utc::now(),
    };
    st.receipt_rule_sets.insert(new_name.to_string(), cloned);
    Ok(xml_metadata_only("CloneReceiptRuleSet", &req.request_id))
}

fn set_active_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut st = state.write();
    // If RuleSetName is empty or absent, deactivate.
    match req.query_params.get("RuleSetName") {
        Some(name) if !name.is_empty() => {
            if !st.receipt_rule_sets.contains_key(name.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "RuleSetDoesNotExistException",
                    format!("Rule set with name '{name}' does not exist"),
                ));
            }
            st.active_receipt_rule_set = Some(name.clone());
        }
        _ => {
            st.active_receipt_rule_set = None;
        }
    }
    Ok(xml_metadata_only(
        "SetActiveReceiptRuleSet",
        &req.request_id,
    ))
}

fn reorder_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let mut st = state.write();
    let rule_set = st.receipt_rule_sets.get_mut(rule_set_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{rule_set_name}' does not exist"),
        )
    })?;

    // Parse ordered rule names: RuleNames.member.1, RuleNames.member.2, ...
    let mut ordered_names = Vec::new();
    for i in 1.. {
        let key = format!("RuleNames.member.{i}");
        match req.query_params.get(&key) {
            Some(v) => ordered_names.push(v.clone()),
            None => break,
        }
    }

    // Validate all names exist
    for name in &ordered_names {
        if !rule_set.rules.iter().any(|r| &r.name == name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RuleDoesNotExistException",
                format!("Rule '{name}' does not exist in rule set '{rule_set_name}'"),
            ));
        }
    }

    // Reorder
    let mut reordered = Vec::with_capacity(rule_set.rules.len());
    for name in &ordered_names {
        if let Some(pos) = rule_set.rules.iter().position(|r| &r.name == name) {
            reordered.push(rule_set.rules.remove(pos));
        }
    }
    // Append any rules not mentioned in the new order
    reordered.append(&mut rule_set.rules);
    rule_set.rules = reordered;

    Ok(xml_metadata_only("ReorderReceiptRuleSet", &req.request_id))
}

// ── Receipt Rule operations ──

fn create_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule = parse_receipt_rule(&req.query_params)?;
    let after = req.query_params.get("After").cloned();

    let mut st = state.write();
    let rule_set = st.receipt_rule_sets.get_mut(rule_set_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{rule_set_name}' does not exist"),
        )
    })?;

    if rule_set.rules.iter().any(|r| r.name == rule.name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AlreadyExistsException",
            format!(
                "Rule '{}' already exists in rule set '{rule_set_name}'",
                rule.name
            ),
        ));
    }

    if let Some(after_name) = after {
        if let Some(pos) = rule_set.rules.iter().position(|r| r.name == after_name) {
            rule_set.rules.insert(pos + 1, rule);
        } else {
            rule_set.rules.push(rule);
        }
    } else {
        rule_set.rules.push(rule);
    }

    Ok(xml_metadata_only("CreateReceiptRule", &req.request_id))
}

fn delete_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule_name = required_param(&req.query_params, "RuleName")?;

    let mut st = state.write();
    let rule_set = st.receipt_rule_sets.get_mut(rule_set_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{rule_set_name}' does not exist"),
        )
    })?;

    let pos = rule_set
        .rules
        .iter()
        .position(|r| r.name == rule_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RuleDoesNotExistException",
                format!("Rule '{rule_name}' does not exist in rule set '{rule_set_name}'"),
            )
        })?;
    rule_set.rules.remove(pos);
    Ok(xml_metadata_only("DeleteReceiptRule", &req.request_id))
}

fn describe_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule_name = required_param(&req.query_params, "RuleName")?;

    let st = state.read();
    let rule_set = st.receipt_rule_sets.get(rule_set_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{rule_set_name}' does not exist"),
        )
    })?;
    let rule = rule_set
        .rules
        .iter()
        .find(|r| r.name == rule_name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RuleDoesNotExistException",
                format!("Rule '{rule_name}' does not exist in rule set '{rule_set_name}'"),
            )
        })?;

    // rule_to_xml wraps in <member>, strip it for describe
    let rule_xml = rule_to_xml(rule);
    let inner_xml = rule_xml
        .strip_prefix("<member>")
        .and_then(|s| s.strip_suffix("</member>"))
        .unwrap_or(&rule_xml);
    let inner = format!("<Rule>{inner_xml}</Rule>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("DescribeReceiptRule", &inner, &req.request_id),
    ))
}

fn update_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let new_rule = parse_receipt_rule(&req.query_params)?;

    let mut st = state.write();
    let rule_set = st.receipt_rule_sets.get_mut(rule_set_name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "RuleSetDoesNotExistException",
            format!("Rule set with name '{rule_set_name}' does not exist"),
        )
    })?;

    let rule = rule_set
        .rules
        .iter_mut()
        .find(|r| r.name == new_rule.name)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "RuleDoesNotExistException",
                format!(
                    "Rule '{}' does not exist in rule set '{rule_set_name}'",
                    new_rule.name
                ),
            )
        })?;

    *rule = new_rule;
    Ok(xml_metadata_only("UpdateReceiptRule", &req.request_id))
}

// ── Receipt Filter operations ──

fn create_receipt_filter(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Filter.Name")?;
    let cidr = required_param(&req.query_params, "Filter.IpFilter.Cidr")?;
    let policy = required_param(&req.query_params, "Filter.IpFilter.Policy")?;

    let mut st = state.write();
    if st.receipt_filters.contains_key(name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AlreadyExistsException",
            format!("Filter with name '{name}' already exists"),
        ));
    }

    st.receipt_filters.insert(
        name.to_string(),
        ReceiptFilter {
            name: name.to_string(),
            ip_filter: IpFilter {
                cidr: cidr.to_string(),
                policy: policy.to_string(),
            },
        },
    );
    Ok(xml_metadata_only("CreateReceiptFilter", &req.request_id))
}

fn delete_receipt_filter(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "FilterName")?;
    let mut st = state.write();
    if st.receipt_filters.remove(name).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "FilterDoesNotExistException",
            format!("Filter with name '{name}' does not exist"),
        ));
    }
    Ok(xml_metadata_only("DeleteReceiptFilter", &req.request_id))
}

fn list_receipt_filters(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let st = state.read();
    let mut inner = String::from("<Filters>");
    let mut filters: Vec<&ReceiptFilter> = st.receipt_filters.values().collect();
    filters.sort_by_key(|f| &f.name);
    for f in filters {
        inner.push_str(&format!(
            "<member><Name>{}</Name><IpFilter><Cidr>{}</Cidr><Policy>{}</Policy></IpFilter></member>",
            xml_escape(&f.name),
            xml_escape(&f.ip_filter.cidr),
            xml_escape(&f.ip_filter.policy),
        ));
    }
    inner.push_str("</Filters>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        xml_wrap("ListReceiptFilters", &inner, &req.request_id),
    ))
}

// ── Inbound email processing ──

/// Evaluate an inbound email against the active receipt rule set.
/// Returns the list of matched rules and actions that should be executed.
pub fn evaluate_inbound_email(
    state: &SharedSesState,
    from: &str,
    to: &[String],
    subject: &str,
    body: &str,
) -> (String, Vec<String>, Vec<(String, ReceiptAction)>) {
    let message_id = uuid::Uuid::new_v4().to_string();
    let st = state.read();

    let active_name = match &st.active_receipt_rule_set {
        Some(name) => name.clone(),
        None => return (message_id, Vec::new(), Vec::new()),
    };

    let rule_set = match st.receipt_rule_sets.get(&active_name) {
        Some(rs) => rs,
        None => return (message_id, Vec::new(), Vec::new()),
    };

    let mut matched_rules = Vec::new();
    let mut actions_to_execute = Vec::new();
    let mut stop = false;

    for rule in &rule_set.rules {
        if !rule.enabled {
            continue;
        }
        if stop {
            break;
        }

        // Check if any recipient matches the rule's recipients list.
        // If the rule has no recipients, it matches all emails.
        let matches = rule.recipients.is_empty()
            || to.iter().any(|recipient| {
                rule.recipients.iter().any(|r| {
                    // Match exact address or domain
                    recipient == r || recipient.ends_with(&format!("@{r}"))
                })
            });

        if matches {
            matched_rules.push(rule.name.clone());
            for action in &rule.actions {
                actions_to_execute.push((rule.name.clone(), action.clone()));
                if matches!(action, ReceiptAction::Stop { .. }) {
                    stop = true;
                    break;
                }
            }
        }
    }

    // Record the inbound email
    drop(st);
    let mut st = state.write();
    st.inbound_emails.push(crate::state::InboundEmail {
        message_id: message_id.clone(),
        from: from.to_string(),
        to: to.to_vec(),
        subject: subject.to_string(),
        body: body.to_string(),
        matched_rules: matched_rules.clone(),
        actions_executed: actions_to_execute
            .iter()
            .map(|(rule, action)| format!("{rule}:{}", action_type_name(action)))
            .collect(),
        timestamp: Utc::now(),
    });

    (message_id, matched_rules, actions_to_execute)
}

fn action_type_name(action: &ReceiptAction) -> &'static str {
    match action {
        ReceiptAction::S3 { .. } => "S3",
        ReceiptAction::Sns { .. } => "SNS",
        ReceiptAction::Lambda { .. } => "Lambda",
        ReceiptAction::Bounce { .. } => "Bounce",
        ReceiptAction::AddHeader { .. } => "AddHeader",
        ReceiptAction::Stop { .. } => "Stop",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SesState;
    use bytes::Bytes;

    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_state() -> SharedSesState {
        Arc::new(RwLock::new(SesState::new("123456789012", "us-east-1")))
    }

    fn make_v1_request(action: &str, params: Vec<(&str, &str)>) -> AwsRequest {
        let mut query_params: HashMap<String, String> = HashMap::new();
        query_params.insert("Action".to_string(), action.to_string());
        for (k, v) in params {
            query_params.insert(k.to_string(), v.to_string());
        }
        AwsRequest {
            service: "ses".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    #[test]
    fn test_create_receipt_rule_set() {
        let state = make_state();
        let req = make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-rules")]);
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("CreateReceiptRuleSetResponse"));

        // Duplicate should fail
        let req = make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-rules")]);
        match handle_v1_action(&state, &req) {
            Err(e) => assert_eq!(e.code(), "AlreadyExistsException"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn test_list_receipt_rule_sets() {
        let state = make_state();
        // Create two rule sets
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set-a")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set-b")]),
        )
        .unwrap();

        let req = make_v1_request("ListReceiptRuleSets", vec![]);
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>set-a</Name>"));
        assert!(body.contains("<Name>set-b</Name>"));
    }

    #[test]
    fn test_delete_receipt_rule_set() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "to-delete")]),
        )
        .unwrap();
        let req = make_v1_request("DeleteReceiptRuleSet", vec![("RuleSetName", "to-delete")]);
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Should not exist anymore
        match handle_v1_action(&state, &req) {
            Err(e) => assert_eq!(e.code(), "RuleSetDoesNotExistException"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn test_cannot_delete_active_rule_set() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "active-set")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "SetActiveReceiptRuleSet",
                vec![("RuleSetName", "active-set")],
            ),
        )
        .unwrap();

        match handle_v1_action(
            &state,
            &make_v1_request("DeleteReceiptRuleSet", vec![("RuleSetName", "active-set")]),
        ) {
            Err(e) => assert_eq!(e.code(), "CannotDeleteException"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn test_clone_receipt_rule_set() {
        let state = make_state();
        // Create source with a rule
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "source")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "source"),
                    ("Rule.Name", "rule1"),
                    ("Rule.Enabled", "true"),
                ],
            ),
        )
        .unwrap();

        // Clone
        let req = make_v1_request(
            "CloneReceiptRuleSet",
            vec![("RuleSetName", "cloned"), ("OriginalRuleSetName", "source")],
        );
        handle_v1_action(&state, &req).unwrap();

        // Verify clone has the rule
        let st = state.read();
        let cloned = st.receipt_rule_sets.get("cloned").unwrap();
        assert_eq!(cloned.rules.len(), 1);
        assert_eq!(cloned.rules[0].name, "rule1");
    }

    #[test]
    fn test_set_active_receipt_rule_set() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();

        // Activate
        handle_v1_action(
            &state,
            &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();
        assert_eq!(
            state.read().active_receipt_rule_set.as_deref(),
            Some("my-set")
        );

        // Deactivate (empty name)
        handle_v1_action(
            &state,
            &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "")]),
        )
        .unwrap();
        assert!(state.read().active_receipt_rule_set.is_none());
    }

    #[test]
    fn test_create_and_describe_receipt_rule() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();

        // Create rule with S3 action and recipients
        let req = make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "my-set"),
                ("Rule.Name", "store-email"),
                ("Rule.Enabled", "true"),
                ("Rule.ScanEnabled", "true"),
                ("Rule.TlsPolicy", "Require"),
                ("Rule.Recipients.member.1", "user@example.com"),
                ("Rule.Recipients.member.2", "example.com"),
                ("Rule.Actions.member.1.S3Action.BucketName", "my-bucket"),
                ("Rule.Actions.member.1.S3Action.ObjectKeyPrefix", "emails/"),
            ],
        );
        handle_v1_action(&state, &req).unwrap();

        // Describe the rule
        let req = make_v1_request(
            "DescribeReceiptRule",
            vec![("RuleSetName", "my-set"), ("RuleName", "store-email")],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>store-email</Name>"));
        assert!(body.contains("<Enabled>true</Enabled>"));
        assert!(body.contains("<ScanEnabled>true</ScanEnabled>"));
        assert!(body.contains("<TlsPolicy>Require</TlsPolicy>"));
        assert!(body.contains("<BucketName>my-bucket</BucketName>"));
        assert!(body.contains("<ObjectKeyPrefix>emails/</ObjectKeyPrefix>"));
        assert!(body.contains("user@example.com"));
    }

    #[test]
    fn test_update_receipt_rule() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "my-set"),
                    ("Rule.Name", "rule1"),
                    ("Rule.Enabled", "true"),
                ],
            ),
        )
        .unwrap();

        // Update: disable the rule and add action
        let req = make_v1_request(
            "UpdateReceiptRule",
            vec![
                ("RuleSetName", "my-set"),
                ("Rule.Name", "rule1"),
                ("Rule.Enabled", "false"),
                (
                    "Rule.Actions.member.1.SNSAction.TopicArn",
                    "arn:aws:sns:us-east-1:123456789012:my-topic",
                ),
            ],
        );
        handle_v1_action(&state, &req).unwrap();

        let st = state.read();
        let rule = &st.receipt_rule_sets.get("my-set").unwrap().rules[0];
        assert!(!rule.enabled);
        assert_eq!(rule.actions.len(), 1);
        assert!(matches!(&rule.actions[0], ReceiptAction::Sns { .. }));
    }

    #[test]
    fn test_delete_receipt_rule() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![("RuleSetName", "my-set"), ("Rule.Name", "rule1")],
            ),
        )
        .unwrap();

        let req = make_v1_request(
            "DeleteReceiptRule",
            vec![("RuleSetName", "my-set"), ("RuleName", "rule1")],
        );
        handle_v1_action(&state, &req).unwrap();

        let st = state.read();
        assert!(st.receipt_rule_sets.get("my-set").unwrap().rules.is_empty());
    }

    #[test]
    fn test_reorder_receipt_rule_set() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();
        for name in &["a", "b", "c"] {
            handle_v1_action(
                &state,
                &make_v1_request(
                    "CreateReceiptRule",
                    vec![("RuleSetName", "my-set"), ("Rule.Name", name)],
                ),
            )
            .unwrap();
        }

        // Reorder: c, a, b
        let req = make_v1_request(
            "ReorderReceiptRuleSet",
            vec![
                ("RuleSetName", "my-set"),
                ("RuleNames.member.1", "c"),
                ("RuleNames.member.2", "a"),
                ("RuleNames.member.3", "b"),
            ],
        );
        handle_v1_action(&state, &req).unwrap();

        let st = state.read();
        let names: Vec<&str> = st
            .receipt_rule_sets
            .get("my-set")
            .unwrap()
            .rules
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    #[test]
    fn test_receipt_filter_lifecycle() {
        let state = make_state();

        // Create filter
        let req = make_v1_request(
            "CreateReceiptFilter",
            vec![
                ("Filter.Name", "allow-internal"),
                ("Filter.IpFilter.Cidr", "10.0.0.0/8"),
                ("Filter.IpFilter.Policy", "Allow"),
            ],
        );
        handle_v1_action(&state, &req).unwrap();

        // List filters
        let req = make_v1_request("ListReceiptFilters", vec![]);
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>allow-internal</Name>"));
        assert!(body.contains("<Cidr>10.0.0.0/8</Cidr>"));
        assert!(body.contains("<Policy>Allow</Policy>"));

        // Delete filter
        let req = make_v1_request(
            "DeleteReceiptFilter",
            vec![("FilterName", "allow-internal")],
        );
        handle_v1_action(&state, &req).unwrap();

        // List should be empty
        let req = make_v1_request("ListReceiptFilters", vec![]);
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(!body.contains("allow-internal"));
    }

    #[test]
    fn test_evaluate_inbound_email_no_active_set() {
        let state = make_state();
        let (msg_id, matched, actions) = evaluate_inbound_email(
            &state,
            "sender@example.com",
            &["recipient@example.com".to_string()],
            "Test",
            "Hello",
        );
        assert!(!msg_id.is_empty());
        assert!(matched.is_empty());
        assert!(actions.is_empty());
    }

    #[test]
    fn test_evaluate_inbound_email_matching_rule() {
        let state = make_state();

        // Setup: create rule set, add rule, activate
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "active")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "active"),
                    ("Rule.Name", "catch-all"),
                    ("Rule.Enabled", "true"),
                    ("Rule.Actions.member.1.S3Action.BucketName", "emails-bucket"),
                ],
            ),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "active")]),
        )
        .unwrap();

        let (_msg_id, matched, actions) = evaluate_inbound_email(
            &state,
            "sender@example.com",
            &["anyone@example.com".to_string()],
            "Hello",
            "Body",
        );
        assert_eq!(matched, vec!["catch-all"]);
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0].1, ReceiptAction::S3 { bucket_name, .. } if bucket_name == "emails-bucket")
        );
    }

    #[test]
    fn test_evaluate_inbound_email_recipient_filter() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "set"),
                    ("Rule.Name", "domain-rule"),
                    ("Rule.Enabled", "true"),
                    ("Rule.Recipients.member.1", "example.com"),
                    (
                        "Rule.Actions.member.1.SNSAction.TopicArn",
                        "arn:aws:sns:us-east-1:123456789012:topic",
                    ),
                ],
            ),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "set")]),
        )
        .unwrap();

        // Should match: recipient@example.com matches domain "example.com"
        let (_msg_id, matched, _actions) = evaluate_inbound_email(
            &state,
            "sender@other.com",
            &["recipient@example.com".to_string()],
            "Test",
            "Body",
        );
        assert_eq!(matched, vec!["domain-rule"]);

        // Should NOT match: recipient@other.com
        let (_msg_id, matched, _actions) = evaluate_inbound_email(
            &state,
            "sender@other.com",
            &["recipient@other.com".to_string()],
            "Test",
            "Body",
        );
        assert!(matched.is_empty());
    }

    #[test]
    fn test_evaluate_inbound_email_stop_action() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
        )
        .unwrap();
        // Rule 1: stop action
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "set"),
                    ("Rule.Name", "stop-rule"),
                    ("Rule.Enabled", "true"),
                    ("Rule.Actions.member.1.StopAction.Scope", "RuleSet"),
                ],
            ),
        )
        .unwrap();
        // Rule 2: should not be reached
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "set"),
                    ("Rule.Name", "after-stop"),
                    ("Rule.Enabled", "true"),
                    ("Rule.Actions.member.1.S3Action.BucketName", "bucket"),
                ],
            ),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request("SetActiveReceiptRuleSet", vec![("RuleSetName", "set")]),
        )
        .unwrap();

        let (_msg_id, matched, actions) = evaluate_inbound_email(
            &state,
            "sender@example.com",
            &["anyone@example.com".to_string()],
            "Test",
            "Body",
        );
        // Only stop-rule should match, after-stop should not be evaluated
        assert_eq!(matched, vec!["stop-rule"]);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0].1, ReceiptAction::Stop { .. }));
    }

    #[test]
    fn test_describe_receipt_rule_set() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "my-set")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateReceiptRule",
                vec![
                    ("RuleSetName", "my-set"),
                    ("Rule.Name", "rule1"),
                    ("Rule.Enabled", "true"),
                ],
            ),
        )
        .unwrap();

        let req = make_v1_request("DescribeReceiptRuleSet", vec![("RuleSetName", "my-set")]);
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>my-set</Name>"));
        assert!(body.contains("<Name>rule1</Name>"));
        assert!(body.contains("<Rules>"));
    }

    #[test]
    fn test_all_action_types_parsing() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("CreateReceiptRuleSet", vec![("RuleSetName", "set")]),
        )
        .unwrap();

        let req = make_v1_request(
            "CreateReceiptRule",
            vec![
                ("RuleSetName", "set"),
                ("Rule.Name", "multi-action"),
                ("Rule.Enabled", "true"),
                ("Rule.Actions.member.1.S3Action.BucketName", "bucket"),
                (
                    "Rule.Actions.member.2.SNSAction.TopicArn",
                    "arn:aws:sns:us-east-1:123:topic",
                ),
                ("Rule.Actions.member.2.SNSAction.Encoding", "UTF-8"),
                (
                    "Rule.Actions.member.3.LambdaAction.FunctionArn",
                    "arn:aws:lambda:us-east-1:123:function:my-fn",
                ),
                ("Rule.Actions.member.3.LambdaAction.InvocationType", "Event"),
                ("Rule.Actions.member.4.BounceAction.SmtpReplyCode", "550"),
                ("Rule.Actions.member.4.BounceAction.Message", "rejected"),
                (
                    "Rule.Actions.member.4.BounceAction.Sender",
                    "noreply@example.com",
                ),
                ("Rule.Actions.member.5.AddHeaderAction.HeaderName", "X-Test"),
                ("Rule.Actions.member.5.AddHeaderAction.HeaderValue", "true"),
                ("Rule.Actions.member.6.StopAction.Scope", "RuleSet"),
            ],
        );
        handle_v1_action(&state, &req).unwrap();

        let st = state.read();
        let rule = &st.receipt_rule_sets.get("set").unwrap().rules[0];
        assert_eq!(rule.actions.len(), 6);
        assert!(matches!(&rule.actions[0], ReceiptAction::S3 { .. }));
        assert!(matches!(&rule.actions[1], ReceiptAction::Sns { .. }));
        assert!(matches!(&rule.actions[2], ReceiptAction::Lambda { .. }));
        assert!(matches!(&rule.actions[3], ReceiptAction::Bounce { .. }));
        assert!(matches!(&rule.actions[4], ReceiptAction::AddHeader { .. }));
        assert!(matches!(&rule.actions[5], ReceiptAction::Stop { .. }));
    }

    // ── Identity management tests ──

    #[test]
    fn test_verify_email_identity() {
        let state = make_state();
        let req = make_v1_request(
            "VerifyEmailIdentity",
            vec![("EmailAddress", "test@example.com")],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let st = state.read();
        let identity = st.identities.get("test@example.com").unwrap();
        assert!(identity.verified);
        assert_eq!(identity.identity_type, "EmailAddress");
    }

    #[test]
    fn test_verify_domain_identity() {
        let state = make_state();
        let req = make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]);
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<VerificationToken>"));

        let st = state.read();
        let identity = st.identities.get("example.com").unwrap();
        assert!(identity.verified);
        assert_eq!(identity.identity_type, "Domain");
    }

    #[test]
    fn test_verify_domain_dkim() {
        let state = make_state();
        let req = make_v1_request("VerifyDomainDkim", vec![("Domain", "example.com")]);
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<DkimTokens>"));
        // Should return 3 tokens
        assert_eq!(body.matches("<member>").count(), 3);
    }

    #[test]
    fn test_list_identities() {
        let state = make_state();
        // Create email and domain identities
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyDomainIdentity", vec![("Domain", "test.com")]),
        )
        .unwrap();

        // List all
        let resp = handle_v1_action(&state, &make_v1_request("ListIdentities", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("a@test.com"));
        assert!(body.contains("test.com"));

        // List emails only
        let resp = handle_v1_action(
            &state,
            &make_v1_request("ListIdentities", vec![("IdentityType", "EmailAddress")]),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("a@test.com"));
        assert!(!body.contains("<member>test.com</member>"));

        // List domains only
        let resp = handle_v1_action(
            &state,
            &make_v1_request("ListIdentities", vec![("IdentityType", "Domain")]),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(!body.contains("a@test.com"));
        assert!(body.contains("test.com"));
    }

    #[test]
    fn test_get_identity_verification_attributes() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();

        let req = make_v1_request(
            "GetIdentityVerificationAttributes",
            vec![
                ("Identities.member.1", "a@test.com"),
                ("Identities.member.2", "unknown@test.com"),
            ],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<VerificationStatus>Success</VerificationStatus>"));
        assert!(body.contains("<VerificationStatus>NotStarted</VerificationStatus>"));
    }

    #[test]
    fn test_delete_identity() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();

        let req = make_v1_request("DeleteIdentity", vec![("Identity", "a@test.com")]);
        handle_v1_action(&state, &req).unwrap();
        assert!(!state.read().identities.contains_key("a@test.com"));
    }

    #[test]
    fn test_set_identity_dkim_enabled() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();
        assert!(
            !state
                .read()
                .identities
                .get("a@test.com")
                .unwrap()
                .dkim_signing_enabled
        );

        handle_v1_action(
            &state,
            &make_v1_request(
                "SetIdentityDkimEnabled",
                vec![("Identity", "a@test.com"), ("DkimEnabled", "true")],
            ),
        )
        .unwrap();
        assert!(
            state
                .read()
                .identities
                .get("a@test.com")
                .unwrap()
                .dkim_signing_enabled
        );
    }

    #[test]
    fn test_get_identity_dkim_attributes() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
        )
        .unwrap();

        let req = make_v1_request(
            "GetIdentityDkimAttributes",
            vec![("Identities.member.1", "example.com")],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<DkimEnabled>"));
        assert!(body.contains("<DkimVerificationStatus>"));
        assert!(body.contains("<DkimTokens>"));
    }

    // ── Identity attributes tests ──

    #[test]
    fn test_set_identity_feedback_forwarding() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();

        handle_v1_action(
            &state,
            &make_v1_request(
                "SetIdentityFeedbackForwardingEnabled",
                vec![("Identity", "a@test.com"), ("ForwardingEnabled", "false")],
            ),
        )
        .unwrap();
        assert!(
            !state
                .read()
                .identities
                .get("a@test.com")
                .unwrap()
                .email_forwarding_enabled
        );
    }

    #[test]
    fn test_get_identity_notification_attributes() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyEmailIdentity", vec![("EmailAddress", "a@test.com")]),
        )
        .unwrap();

        let req = make_v1_request(
            "GetIdentityNotificationAttributes",
            vec![("Identities.member.1", "a@test.com")],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ForwardingEnabled>true</ForwardingEnabled>"));
    }

    #[test]
    fn test_set_identity_mail_from_domain() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
        )
        .unwrap();

        handle_v1_action(
            &state,
            &make_v1_request(
                "SetIdentityMailFromDomain",
                vec![
                    ("Identity", "example.com"),
                    ("MailFromDomain", "mail.example.com"),
                    ("BehaviorOnMXFailure", "RejectMessage"),
                ],
            ),
        )
        .unwrap();

        let st = state.read();
        let id = st.identities.get("example.com").unwrap();
        assert_eq!(id.mail_from_domain.as_deref(), Some("mail.example.com"));
        assert_eq!(id.mail_from_behavior_on_mx_failure, "RejectMessage");
    }

    #[test]
    fn test_get_identity_mail_from_domain_attributes() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request("VerifyDomainIdentity", vec![("Domain", "example.com")]),
        )
        .unwrap();

        let req = make_v1_request(
            "GetIdentityMailFromDomainAttributes",
            vec![("Identities.member.1", "example.com")],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<BehaviorOnMXFailure>"));
        assert!(body.contains("<MailFromDomainStatus>"));
    }

    // ── Sending tests ──

    #[test]
    fn test_send_email_v1() {
        let state = make_state();
        let req = make_v1_request(
            "SendEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Destination.ToAddresses.member.1", "to@example.com"),
                ("Destination.CcAddresses.member.1", "cc@example.com"),
                ("Message.Subject.Data", "Test Subject"),
                ("Message.Body.Html.Data", "<h1>Hello</h1>"),
                ("Message.Body.Text.Data", "Hello"),
            ],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<MessageId>"));

        let st = state.read();
        assert_eq!(st.sent_emails.len(), 1);
        let sent = &st.sent_emails[0];
        assert_eq!(sent.from, "sender@example.com");
        assert_eq!(sent.to, vec!["to@example.com"]);
        assert_eq!(sent.cc, vec!["cc@example.com"]);
        assert_eq!(sent.subject.as_deref(), Some("Test Subject"));
        assert_eq!(sent.html_body.as_deref(), Some("<h1>Hello</h1>"));
    }

    #[test]
    fn test_send_raw_email() {
        let state = make_state();
        let req = make_v1_request(
            "SendRawEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Destinations.member.1", "to@example.com"),
                (
                    "RawMessage.Data",
                    "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Test\r\n\r\nBody",
                ),
            ],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let st = state.read();
        assert_eq!(st.sent_emails.len(), 1);
        assert!(st.sent_emails[0].raw_data.is_some());
    }

    #[test]
    fn test_send_templated_email() {
        let state = make_state();
        // Create template first
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateTemplate",
                vec![
                    ("Template.TemplateName", "my-template"),
                    ("Template.SubjectPart", "Hello {{name}}"),
                    ("Template.HtmlPart", "<p>Hi {{name}}</p>"),
                ],
            ),
        )
        .unwrap();

        let req = make_v1_request(
            "SendTemplatedEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Destination.ToAddresses.member.1", "to@example.com"),
                ("Template", "my-template"),
                ("TemplateData", "{\"name\":\"World\"}"),
            ],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let st = state.read();
        assert_eq!(st.sent_emails.len(), 1);
        assert_eq!(
            st.sent_emails[0].template_name.as_deref(),
            Some("my-template")
        );
    }

    #[test]
    fn test_send_templated_email_missing_template() {
        let state = make_state();
        let req = make_v1_request(
            "SendTemplatedEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Destination.ToAddresses.member.1", "to@example.com"),
                ("Template", "nonexistent"),
                ("TemplateData", "{}"),
            ],
        );
        match handle_v1_action(&state, &req) {
            Err(e) => assert_eq!(e.code(), "TemplateDoesNotExistException"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn test_send_bulk_templated_email() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateTemplate",
                vec![
                    ("Template.TemplateName", "bulk-tmpl"),
                    ("Template.SubjectPart", "Hi"),
                ],
            ),
        )
        .unwrap();

        let req = make_v1_request(
            "SendBulkTemplatedEmail",
            vec![
                ("Source", "sender@example.com"),
                ("Template", "bulk-tmpl"),
                ("DefaultTemplateData", "{\"key\":\"default\"}"),
                (
                    "Destinations.member.1.Destination.ToAddresses.member.1",
                    "a@example.com",
                ),
                (
                    "Destinations.member.2.Destination.ToAddresses.member.1",
                    "b@example.com",
                ),
                (
                    "Destinations.member.2.ReplacementTemplateData",
                    "{\"key\":\"custom\"}",
                ),
            ],
        );
        let resp = handle_v1_action(&state, &req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>Success</Status>"));

        let st = state.read();
        assert_eq!(st.sent_emails.len(), 2);
    }

    // ── Template tests ──

    #[test]
    fn test_template_lifecycle() {
        let state = make_state();

        // Create
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateTemplate",
                vec![
                    ("Template.TemplateName", "t1"),
                    ("Template.SubjectPart", "Subject"),
                    ("Template.HtmlPart", "<p>html</p>"),
                    ("Template.TextPart", "text"),
                ],
            ),
        )
        .unwrap();

        // Duplicate should fail
        match handle_v1_action(
            &state,
            &make_v1_request("CreateTemplate", vec![("Template.TemplateName", "t1")]),
        ) {
            Err(e) => assert_eq!(e.code(), "AlreadyExistsException"),
            Ok(_) => panic!("expected error"),
        }

        // Get
        let resp = handle_v1_action(
            &state,
            &make_v1_request("GetTemplate", vec![("TemplateName", "t1")]),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<TemplateName>t1</TemplateName>"));
        assert!(body.contains("<SubjectPart>Subject</SubjectPart>"));

        // List
        let resp = handle_v1_action(&state, &make_v1_request("ListTemplates", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>t1</Name>"));

        // Update
        handle_v1_action(
            &state,
            &make_v1_request(
                "UpdateTemplate",
                vec![
                    ("Template.TemplateName", "t1"),
                    ("Template.SubjectPart", "Updated"),
                ],
            ),
        )
        .unwrap();
        let st = state.read();
        assert_eq!(
            st.templates.get("t1").unwrap().subject.as_deref(),
            Some("Updated")
        );
        drop(st);

        // Delete
        handle_v1_action(
            &state,
            &make_v1_request("DeleteTemplate", vec![("TemplateName", "t1")]),
        )
        .unwrap();
        assert!(!state.read().templates.contains_key("t1"));
    }

    // ── Configuration Set tests ──

    #[test]
    fn test_configuration_set_lifecycle() {
        let state = make_state();

        // Create
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateConfigurationSet",
                vec![("ConfigurationSet.Name", "my-config")],
            ),
        )
        .unwrap();

        // Duplicate
        match handle_v1_action(
            &state,
            &make_v1_request(
                "CreateConfigurationSet",
                vec![("ConfigurationSet.Name", "my-config")],
            ),
        ) {
            Err(e) => assert_eq!(e.code(), "ConfigurationSetAlreadyExistsException"),
            Ok(_) => panic!("expected error"),
        }

        // List
        let resp =
            handle_v1_action(&state, &make_v1_request("ListConfigurationSets", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>my-config</Name>"));

        // Describe
        let resp = handle_v1_action(
            &state,
            &make_v1_request(
                "DescribeConfigurationSet",
                vec![("ConfigurationSetName", "my-config")],
            ),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Name>my-config</Name>"));

        // Delete
        handle_v1_action(
            &state,
            &make_v1_request(
                "DeleteConfigurationSet",
                vec![("ConfigurationSetName", "my-config")],
            ),
        )
        .unwrap();
        assert!(!state.read().configuration_sets.contains_key("my-config"));
    }

    #[test]
    fn test_configuration_set_event_destination_lifecycle() {
        let state = make_state();
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateConfigurationSet",
                vec![("ConfigurationSet.Name", "cs")],
            ),
        )
        .unwrap();

        // Create event destination
        handle_v1_action(
            &state,
            &make_v1_request(
                "CreateConfigurationSetEventDestination",
                vec![
                    ("ConfigurationSetName", "cs"),
                    ("EventDestination.Name", "sns-dest"),
                    ("EventDestination.Enabled", "true"),
                    ("EventDestination.MatchingEventTypes.member.1", "send"),
                    ("EventDestination.MatchingEventTypes.member.2", "bounce"),
                    (
                        "EventDestination.SNSDestination.TopicARN",
                        "arn:aws:sns:us-east-1:123456789012:my-topic",
                    ),
                ],
            ),
        )
        .unwrap();

        let st = state.read();
        let dests = st.event_destinations.get("cs").unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].name, "sns-dest");
        assert_eq!(dests[0].matching_event_types, vec!["send", "bounce"]);
        drop(st);

        // Update
        handle_v1_action(
            &state,
            &make_v1_request(
                "UpdateConfigurationSetEventDestination",
                vec![
                    ("ConfigurationSetName", "cs"),
                    ("EventDestination.Name", "sns-dest"),
                    ("EventDestination.Enabled", "false"),
                ],
            ),
        )
        .unwrap();
        assert!(!state.read().event_destinations.get("cs").unwrap()[0].enabled);

        // Delete
        handle_v1_action(
            &state,
            &make_v1_request(
                "DeleteConfigurationSetEventDestination",
                vec![
                    ("ConfigurationSetName", "cs"),
                    ("EventDestinationName", "sns-dest"),
                ],
            ),
        )
        .unwrap();
        assert!(state
            .read()
            .event_destinations
            .get("cs")
            .unwrap()
            .is_empty());
    }

    // ── Account / Quota tests ──

    #[test]
    fn test_get_send_quota() {
        let state = make_state();
        let resp = handle_v1_action(&state, &make_v1_request("GetSendQuota", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Max24HourSend>50000.0</Max24HourSend>"));
        assert!(body.contains("<MaxSendRate>14.0</MaxSendRate>"));
    }

    #[test]
    fn test_get_send_statistics() {
        let state = make_state();
        // Send an email first
        handle_v1_action(
            &state,
            &make_v1_request(
                "SendEmail",
                vec![
                    ("Source", "a@b.com"),
                    ("Destination.ToAddresses.member.1", "c@d.com"),
                    ("Message.Subject.Data", "Hi"),
                    ("Message.Body.Text.Data", "Hello"),
                ],
            ),
        )
        .unwrap();

        let resp = handle_v1_action(&state, &make_v1_request("GetSendStatistics", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<DeliveryAttempts>1</DeliveryAttempts>"));
    }

    #[test]
    fn test_get_account_sending_enabled() {
        let state = make_state();
        let resp =
            handle_v1_action(&state, &make_v1_request("GetAccountSendingEnabled", vec![])).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Enabled>true</Enabled>"));
    }
}
