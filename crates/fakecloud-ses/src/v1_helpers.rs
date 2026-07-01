use super::*;

/// Response with an empty result body. The SES v1 Query protocol always wraps
/// the response in a `<{Action}Result>` element (empty for actions with no
/// output members); omitting it makes the AWS SDK fail to deserialize with
/// "{Action}Result node not found". Use this for write actions that return no
/// data of their own.
pub(crate) fn xml_metadata_only(action: &str, request_id: &str) -> AwsResponse {
    let xml = query_response_xml(action, SES_NS, "", request_id);
    AwsResponse::xml(StatusCode::OK, xml)
}

/// Normalize an `Identity` parameter to the bare identity name used as the
/// state map key. SES v1 accepts either the bare email/domain or the identity
/// ARN (`arn:aws:ses:<region>:<acct>:identity/<name>`); the Terraform provider
/// passes the ARN to actions like SetIdentityNotificationTopic and
/// DeleteIdentity, so a lookup keyed on the raw param would miss.
pub(crate) fn identity_key(identity: &str) -> &str {
    identity
        .rsplit_once(":identity/")
        .map(|(_, name)| name)
        .unwrap_or(identity)
}

/// Deterministic 64-hex-char domain-verification token for an identity. Real
/// SES returns a stable token per identity; deriving it from the name keeps
/// VerifyDomainIdentity and GetIdentityVerificationAttributes in agreement
/// across calls and process restarts.
pub(crate) fn verification_token_for(name: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"fakecloud-ses-verification:");
    hasher.update(name.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
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
        "VerifyEmailAddress" => verify_email_address(state, req),
        "ListVerifiedEmailAddresses" => list_verified_email_addresses(state, req),
        "DeleteVerifiedEmailAddress" => delete_verified_email_address(state, req),
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
        "SendBounce" => send_bounce(state, req),
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

/// SES-flavored required-parameter check. Intentionally diverges from
/// `fakecloud_core::query::required_param`: it returns a borrowed `&str` and
/// emits SES's `ValidationError` / "Value for parameter X is required" wording
/// (the AWS-correct response for the SES query protocol), where core emits
/// `MissingParameter` / "The request must contain the parameter X." Do not
/// "consolidate" this into the core helper - it would break SES error parity.
pub(crate) fn required_param<'a>(
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

/// Strip display-name wrappers like `Foo <foo@example.com>`.
fn extract_email_address(from: &str) -> &str {
    if let Some(start) = from.rfind('<') {
        if let Some(end) = from.rfind('>') {
            if end > start {
                return from[start + 1..end].trim();
            }
        }
    }
    from.trim()
}

/// True for mailbox-simulator addresses. Real SES treats every address
/// on `simulator.amazonses.com` as implicitly verified; we match that
/// behavior so callers can exercise bounce/complaint/suppression flows
/// without having to register the simulator domain as a verified identity.
fn is_simulator_address(email: &str) -> bool {
    matches!(email.split_once('@'), Some((_, "simulator.amazonses.com")))
}

/// Match an email against verified identities (exact email or verified
/// domain match). Mailbox-simulator addresses bypass the gate the same
/// way real SES does.
fn identity_is_verified(st: &SesState, email: &str) -> bool {
    if is_simulator_address(email) {
        return true;
    }
    if st
        .identities
        .get(email)
        .map(|id| id.verified)
        .unwrap_or(false)
    {
        return true;
    }
    if let Some((_, domain)) = email.split_once('@') {
        if !domain.is_empty()
            && st
                .identities
                .get(domain)
                .map(|id| id.verified)
                .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

/// Gate every SES v1 send path on a verified sender. Mirrors the v2
/// rule: the From address must match a verified email identity exactly,
/// or its domain must match a verified domain identity. Real SES v1
/// surfaces this as `MessageRejected`.
pub(crate) fn check_v1_verified_sender(
    state: &SharedSesState,
    account_id: &str,
    from: &str,
) -> Result<(), AwsServiceError> {
    let email = extract_email_address(from);
    if email.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MessageRejected",
            "Email address is not verified.".to_string(),
        ));
    }

    let accounts = state.read();
    let verified = accounts
        .get(account_id)
        .map(|st| identity_is_verified(st, email))
        .unwrap_or(false);

    if verified {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MessageRejected",
            format!("Email address is not verified. The following identities failed the check in region us-east-1: {email}"),
        ))
    }
}

/// In sandbox accounts, every recipient must also be a verified
/// identity. Real SES v1 surfaces failures as `MessageRejected` listing
/// the offending addresses.
pub(crate) fn check_v1_verified_recipients(
    state: &SharedSesState,
    account_id: &str,
    recipients: &[String],
) -> Result<(), AwsServiceError> {
    let accounts = state.read();
    let Some(st) = accounts.get(account_id) else {
        return Ok(());
    };
    if st.account_settings.production_access_enabled {
        return Ok(());
    }
    let mut failing: Vec<String> = Vec::new();
    for raw in recipients {
        let addr = extract_email_address(raw);
        if addr.is_empty() {
            continue;
        }
        if !identity_is_verified(st, addr) {
            failing.push(addr.to_string());
        }
    }
    if failing.is_empty() {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MessageRejected",
            format!(
                "Email address is not verified. The following identities failed the check: {}",
                failing.join(", ")
            ),
        ))
    }
}

/// Reject the send if either account-level sending or the resolved
/// configuration set's sending flag is paused. Real SES v1 surfaces both
/// as `MessageRejected` with a message identifying the paused scope.
pub(crate) fn check_v1_sending_enabled(
    state: &SharedSesState,
    account_id: &str,
    config_set_name: Option<&str>,
) -> Result<(), AwsServiceError> {
    let accounts = state.read();
    let Some(st) = accounts.get(account_id) else {
        return Ok(());
    };
    if !st.account_settings.sending_enabled {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MessageRejected",
            "Email sending for the account is paused.".to_string(),
        ));
    }
    if let Some(name) = config_set_name {
        if let Some(cs) = st.configuration_sets.get(name) {
            if !cs.sending_enabled {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MessageRejected",
                    format!("Email sending for the configuration set {name} is paused."),
                ));
            }
        }
    }
    Ok(())
}

/// Reject sends targeting a recipient on the account suppression list.
/// Real SES v1 surfaces this as `MessageRejected`. Suppression is gated
/// by the effective `SuppressedReasons` filter (configuration-set scope
/// first, then account-level fallback).
pub(crate) fn check_v1_recipients_not_suppressed(
    state: &SharedSesState,
    account_id: &str,
    recipients: &[String],
    config_set_name: Option<&str>,
) -> Result<(), AwsServiceError> {
    let mut hit = false;
    {
        let accounts = state.read();
        let Some(st) = accounts.get(account_id) else {
            return Ok(());
        };
        for r in recipients {
            let addr = extract_email_address(r);
            if addr.is_empty() {
                continue;
            }
            if st.suppressed_match(addr, config_set_name).is_some() {
                hit = true;
                break;
            }
        }
    }
    if hit {
        bump_suppression_drop(state, account_id);
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MessageRejected",
            "Address is on the suppression list".to_string(),
        ));
    }
    Ok(())
}

/// Increment the suppression-drop counter; mirrors the v2 helper so
/// both paths feed the same introspection counter.
pub(crate) fn bump_suppression_drop(state: &SharedSesState, account_id: &str) {
    let mut accounts = state.write();
    let st = accounts.get_or_create(account_id);
    st.suppressed_drops_total = st.suppressed_drops_total.saturating_add(1);
}

/// True when `addr` is on the account suppression list AND its stored
/// reason is enforced under the effective `SuppressedReasons` filter
/// (configuration-set scope first, then account-level fallback). Used by
/// bulk paths that filter per-destination instead of failing the whole
/// batch.
pub(crate) fn is_address_suppressed(
    state: &SharedSesState,
    account_id: &str,
    addr: &str,
    config_set_name: Option<&str>,
) -> bool {
    let accounts = state.read();
    let Some(st) = accounts.get(account_id) else {
        return false;
    };
    st.suppressed_match(extract_email_address(addr), config_set_name)
        .is_some()
}

/// Parse a receipt rule from form parameters (for Create/Update).
pub(crate) fn parse_receipt_rule(
    params: &HashMap<String, String>,
) -> Result<ReceiptRule, AwsServiceError> {
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

pub(crate) fn parse_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    parse_s3_action(params, prefix)
        .or_else(|| parse_sns_action(params, prefix))
        .or_else(|| parse_lambda_action(params, prefix))
        .or_else(|| parse_bounce_action(params, prefix))
        .or_else(|| parse_add_header_action(params, prefix))
        .or_else(|| parse_stop_action(params, prefix))
        .or_else(|| parse_workmail_action(params, prefix))
}

pub(crate) fn parse_s3_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let bucket = params.get(&format!("{prefix}.S3Action.BucketName"))?;
    Some(ReceiptAction::S3 {
        bucket_name: bucket.clone(),
        object_key_prefix: params
            .get(&format!("{prefix}.S3Action.ObjectKeyPrefix"))
            .cloned(),
        topic_arn: params.get(&format!("{prefix}.S3Action.TopicArn")).cloned(),
        kms_key_arn: params.get(&format!("{prefix}.S3Action.KmsKeyArn")).cloned(),
    })
}

pub(crate) fn parse_sns_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let topic_arn = params.get(&format!("{prefix}.SNSAction.TopicArn"))?;
    Some(ReceiptAction::Sns {
        topic_arn: topic_arn.clone(),
        encoding: params.get(&format!("{prefix}.SNSAction.Encoding")).cloned(),
    })
}

pub(crate) fn parse_lambda_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let function_arn = params.get(&format!("{prefix}.LambdaAction.FunctionArn"))?;
    Some(ReceiptAction::Lambda {
        function_arn: function_arn.clone(),
        invocation_type: params
            .get(&format!("{prefix}.LambdaAction.InvocationType"))
            .cloned(),
        topic_arn: params
            .get(&format!("{prefix}.LambdaAction.TopicArn"))
            .cloned(),
    })
}

pub(crate) fn parse_bounce_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let smtp_code = params.get(&format!("{prefix}.BounceAction.SmtpReplyCode"))?;
    Some(ReceiptAction::Bounce {
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
    })
}

pub(crate) fn parse_add_header_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let header_name = params.get(&format!("{prefix}.AddHeaderAction.HeaderName"))?;
    Some(ReceiptAction::AddHeader {
        header_name: header_name.clone(),
        header_value: params
            .get(&format!("{prefix}.AddHeaderAction.HeaderValue"))
            .cloned()
            .unwrap_or_default(),
    })
}

pub(crate) fn parse_stop_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let scope = params.get(&format!("{prefix}.StopAction.Scope"))?;
    Some(ReceiptAction::Stop {
        scope: scope.clone(),
        topic_arn: params
            .get(&format!("{prefix}.StopAction.TopicArn"))
            .cloned(),
    })
}

pub(crate) fn parse_workmail_action(
    params: &HashMap<String, String>,
    prefix: &str,
) -> Option<ReceiptAction> {
    let org_arn = params.get(&format!("{prefix}.WorkmailAction.OrganizationArn"))?;
    Some(ReceiptAction::Workmail {
        organization_arn: org_arn.clone(),
        topic_arn: params
            .get(&format!("{prefix}.WorkmailAction.TopicArn"))
            .cloned(),
    })
}

/// Serialize a `ReceiptRule` to its XML wire form.
pub(crate) fn rule_to_xml(rule: &ReceiptRule) -> String {
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
            xml.push_str(&receipt_action_xml(action));
            xml.push_str("</member>");
        }
        xml.push_str("</Actions>");
    }
    xml.push_str("</member>");
    xml
}

/// Serialize one `ReceiptAction` variant. Each variant has its own AWS
/// XML element name (`S3Action`, `SNSAction`, …) and a different set of
/// optional fields, so we just match-and-format per variant.
pub(crate) fn receipt_action_xml(action: &ReceiptAction) -> String {
    let mut xml = String::new();
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
        ReceiptAction::Workmail {
            organization_arn,
            topic_arn,
        } => {
            xml.push_str("<WorkmailAction>");
            xml.push_str(&format!(
                "<OrganizationArn>{}</OrganizationArn>",
                xml_escape(organization_arn)
            ));
            if let Some(t) = topic_arn {
                xml.push_str(&format!("<TopicArn>{}</TopicArn>", xml_escape(t)));
            }
            xml.push_str("</WorkmailAction>");
        }
    }
    xml
}

pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub(crate) fn verify_email_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let email = required_param(&req.query_params, "EmailAddress")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
            verification_token: None,
        });
    Ok(xml_metadata_only("VerifyEmailIdentity", &req.request_id))
}

/// Legacy alias: `VerifyEmailAddress` predates `VerifyEmailIdentity` and
/// is still accepted by real SES. Same effect: idempotently mark the
/// supplied email address as a verified identity.
pub(crate) fn verify_email_address(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let email = required_param(&req.query_params, "EmailAddress")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
            verification_token: None,
        });
    Ok(xml_metadata_only("VerifyEmailAddress", &req.request_id))
}

/// Legacy alias: `ListVerifiedEmailAddresses` returns email-type
/// identities only. New callers should use `ListIdentities` with
/// `IdentityType=EmailAddress`.
pub(crate) fn list_verified_email_addresses(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut emails: Vec<&str> = st
        .identities
        .values()
        .filter(|i| i.identity_type == "EmailAddress" && i.verified)
        .map(|i| i.identity_name.as_str())
        .collect();
    emails.sort();
    let mut inner = String::from("<VerifiedEmailAddresses>");
    for email in emails {
        inner.push_str(&format!("<member>{}</member>", xml_escape(email)));
    }
    inner.push_str("</VerifiedEmailAddresses>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(
            "ListVerifiedEmailAddresses",
            SES_NS,
            &inner,
            &req.request_id,
        ),
    ))
}

/// Legacy alias for `DeleteIdentity` scoped to email-type identities.
pub(crate) fn delete_verified_email_address(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let email = required_param(&req.query_params, "EmailAddress")?;
    state
        .write()
        .get_or_create(&req.account_id)
        .identities
        .remove(email);
    Ok(xml_metadata_only(
        "DeleteVerifiedEmailAddress",
        &req.request_id,
    ))
}

pub(crate) fn verify_domain_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = required_param(&req.query_params, "Domain")?;
    let token = verification_token_for(domain);
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
            verification_token: Some(token.clone()),
        });
    let inner = format!("<VerificationToken>{token}</VerificationToken>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("VerifyDomainIdentity", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn verify_domain_dkim(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let domain = required_param(&req.query_params, "Domain")?;
    // Ensure identity exists
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    let id = st
        .identities
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
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
            verification_token: None,
        });
    // VerifyDomainDkim is the moment SES tells you "ok, I generated DKIM
    // keys, here are the CNAMEs you must publish". Lazily create the
    // keypair the first time the action runs so SendRawEmail can stamp
    // a signature without a follow-up SetIdentityDkimEnabled call.
    id.dkim_signing_enabled = true;
    ensure_easy_dkim_keypair(id);
    // Return 3 DKIM tokens
    let mut inner = String::from("<DkimTokens>");
    for _ in 0..3 {
        let token = format!("{:x}{:x}", rand_u64(), rand_u64());
        inner.push_str(&format!("<member>{token}</member>"));
    }
    inner.push_str("</DkimTokens>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("VerifyDomainDkim", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn list_identities(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity_type = req.query_params.get("IdentityType");
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("ListIdentities", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn get_identity_verification_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut inner = String::from("<VerificationAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                // AWS omits unknown identities from the VerificationAttributes
                // map entirely; the Terraform provider treats absent-from-map as
                // NotFound (its CheckDestroy relies on this). Emitting a
                // NotStarted entry for a deleted identity would read as "still
                // exists", so only emit an entry when the identity is in state.
                let Some(identity) = st.identities.get(identity_key(identity_name)) else {
                    continue;
                };
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                let status = if identity.verified {
                    "Success"
                } else {
                    "Pending"
                };
                inner.push_str(&format!(
                    "<VerificationStatus>{status}</VerificationStatus>"
                ));
                if identity.identity_type == "Domain" {
                    // Report the stored deterministic token (falling back to
                    // deriving it for identities created before the field).
                    let token = identity
                        .verification_token
                        .clone()
                        .unwrap_or_else(|| verification_token_for(&identity.identity_name));
                    inner.push_str(&format!("<VerificationToken>{token}</VerificationToken>"));
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
        query_response_xml(
            "GetIdentityVerificationAttributes",
            SES_NS,
            &inner,
            &req.request_id,
        ),
    ))
}

pub(crate) fn get_identity_dkim_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut inner = String::from("<DkimAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_key(identity_name)) {
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
        query_response_xml("GetIdentityDkimAttributes", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn delete_identity(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let identity = identity_key(identity);
    state
        .write()
        .get_or_create(&req.account_id)
        .identities
        .remove(identity);
    Ok(xml_metadata_only("DeleteIdentity", &req.request_id))
}

pub(crate) fn set_identity_dkim_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let identity = identity_key(identity);
    let enabled = required_param(&req.query_params, "DkimEnabled")? == "true";
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    if let Some(id) = st.identities.get_mut(identity) {
        id.dkim_signing_enabled = enabled;
        ensure_easy_dkim_keypair(id);
    }
    Ok(xml_metadata_only("SetIdentityDkimEnabled", &req.request_id))
}

/// Lazily provision the Easy DKIM keypair for `id` when signing is
/// enabled but no caller-supplied key is on file. Mirrors how real SES
/// auto-generates the per-identity keypair the moment DKIM signing is
/// switched on. No-op when the identity already has a key (Easy or
/// BYODKIM) or when signing is disabled.
fn ensure_easy_dkim_keypair(id: &mut EmailIdentity) {
    if !id.dkim_signing_enabled {
        return;
    }
    if id.dkim_domain_signing_private_key.is_some() {
        return;
    }
    let (priv_pem, pub_b64) = crate::dkim::generate_easy_dkim_keypair();
    id.dkim_domain_signing_private_key = Some(priv_pem);
    id.dkim_public_key_b64 = Some(pub_b64);
    if id.dkim_domain_signing_selector.is_none() {
        id.dkim_domain_signing_selector = Some("fakecloudses".to_string());
    }
    if id.dkim_next_signing_key_length.is_none() {
        id.dkim_next_signing_key_length = Some("RSA_2048_BIT".to_string());
    }
}

pub(crate) fn set_identity_notification_topic(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let identity = identity_key(identity);
    let notification_type = required_param(&req.query_params, "NotificationType")?;
    // SnsTopic is optional, and an empty value clears the topic (disables SNS
    // notifications for that type), matching AWS.
    let topic = req
        .query_params
        .get("SnsTopic")
        .filter(|s| !s.is_empty())
        .cloned();

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    let id = st.identities.get_mut(identity).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Identity '{identity}' does not exist"),
        )
    })?;
    match notification_type {
        "Bounce" => id.bounce_topic = topic,
        "Complaint" => id.complaint_topic = topic,
        "Delivery" => id.delivery_topic = topic,
        other => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid notification type: {other}"),
            ));
        }
    }
    Ok(xml_metadata_only(
        "SetIdentityNotificationTopic",
        &req.request_id,
    ))
}

pub(crate) fn set_identity_feedback_forwarding_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let identity = identity_key(identity);
    let enabled = required_param(&req.query_params, "ForwardingEnabled")? == "true";
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn get_identity_notification_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut inner = String::from("<NotificationAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                if let Some(identity) = st.identities.get(identity_key(identity_name)) {
                    inner.push_str(&format!(
                        "<ForwardingEnabled>{}</ForwardingEnabled>\
                         <HeadersInBounceNotificationsEnabled>false</HeadersInBounceNotificationsEnabled>\
                         <HeadersInComplaintNotificationsEnabled>false</HeadersInComplaintNotificationsEnabled>\
                         <HeadersInDeliveryNotificationsEnabled>false</HeadersInDeliveryNotificationsEnabled>",
                        identity.email_forwarding_enabled,
                    ));
                    // Per-type SNS topics set via SetIdentityNotificationTopic
                    // (1.19). Only emitted when configured, like AWS.
                    if let Some(t) = &identity.bounce_topic {
                        inner.push_str(&format!("<BounceTopic>{}</BounceTopic>", xml_escape(t)));
                    }
                    if let Some(t) = &identity.complaint_topic {
                        inner.push_str(&format!(
                            "<ComplaintTopic>{}</ComplaintTopic>",
                            xml_escape(t)
                        ));
                    }
                    if let Some(t) = &identity.delivery_topic {
                        inner
                            .push_str(&format!("<DeliveryTopic>{}</DeliveryTopic>", xml_escape(t)));
                    }
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
        query_response_xml(
            "GetIdentityNotificationAttributes",
            SES_NS,
            &inner,
            &req.request_id,
        ),
    ))
}

pub(crate) fn get_identity_mail_from_domain_attributes(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    let mut inner = String::from("<MailFromDomainAttributes>");
    for i in 1.. {
        let key = format!("Identities.member.{i}");
        match req.query_params.get(&key) {
            Some(identity_name) => {
                // AWS omits unknown identities from the map; the provider treats
                // absent-from-map as NotFound (mail-from CheckDestroy relies on
                // it). Only emit an entry for an identity that exists.
                let Some(identity) = st.identities.get_mut(identity_key(identity_name)) else {
                    continue;
                };
                let mail_from = identity.mail_from_domain.clone().unwrap_or_default();
                if identity.mail_from_domain_status == "Pending" && !mail_from.is_empty() {
                    identity.mail_from_domain_status = "Success".to_string();
                }
                if mail_from.is_empty() {
                    identity.mail_from_domain_status = "NotStarted".to_string();
                }
                let behavior = identity.mail_from_behavior_on_mx_failure.clone();
                let status = identity.mail_from_domain_status.clone();
                inner.push_str("<entry>");
                inner.push_str(&format!("<key>{}</key>", xml_escape(identity_name)));
                inner.push_str("<value>");
                inner.push_str(&format!(
                    "<MailFromDomain>{}</MailFromDomain>\
                     <MailFromDomainStatus>{}</MailFromDomainStatus>\
                     <BehaviorOnMXFailure>{}</BehaviorOnMXFailure>",
                    xml_escape(&mail_from),
                    xml_escape(&status),
                    xml_escape(&behavior),
                ));
                inner.push_str("</value>");
                inner.push_str("</entry>");
            }
            None => break,
        }
    }
    inner.push_str("</MailFromDomainAttributes>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(
            "GetIdentityMailFromDomainAttributes",
            SES_NS,
            &inner,
            &req.request_id,
        ),
    ))
}

pub(crate) fn set_identity_mail_from_domain(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let identity = required_param(&req.query_params, "Identity")?;
    let identity = identity_key(identity);
    let mail_from_domain = req.query_params.get("MailFromDomain").cloned();
    let behavior = req
        .query_params
        .get("BehaviorOnMXFailure")
        .cloned()
        .unwrap_or_else(|| "UseDefaultValue".to_string());
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    if let Some(id) = st.identities.get_mut(identity) {
        id.mail_from_domain = mail_from_domain.filter(|s| !s.is_empty());
        id.mail_from_behavior_on_mx_failure = behavior;
        id.mail_from_domain_status = if id.mail_from_domain.is_some() {
            "Pending".to_string()
        } else {
            "NotStarted".to_string()
        };
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

/// Parse a list of addresses from v1 query params (e.g. Message.Destination.ToAddresses.member.N)
pub(crate) fn parse_member_list(params: &HashMap<String, String>, prefix: &str) -> Vec<String> {
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

/// Parse the `EventDestination.KinesisFirehoseDestination.*` query params (SES
/// v1 CreateConfigurationSetEventDestination). Returns `None` when neither
/// field is present so we don't attach an empty destination.
fn parse_kinesis_firehose_destination(
    params: &HashMap<String, String>,
) -> Option<serde_json::Value> {
    let role = params.get("EventDestination.KinesisFirehoseDestination.IAMRoleARN");
    let stream = params.get("EventDestination.KinesisFirehoseDestination.DeliveryStreamARN");
    if role.is_none() && stream.is_none() {
        return None;
    }
    Some(serde_json::json!({
        "IAMRoleARN": role.cloned().unwrap_or_default(),
        "DeliveryStreamARN": stream.cloned().unwrap_or_default(),
    }))
}

/// Parse the `EventDestination.CloudWatchDestination.DimensionConfigurations.*`
/// query params (SES v1). Returns `None` when no dimension configurations were
/// supplied.
fn parse_cloudwatch_destination(params: &HashMap<String, String>) -> Option<serde_json::Value> {
    let mut configs = Vec::new();
    for i in 1.. {
        let prefix =
            format!("EventDestination.CloudWatchDestination.DimensionConfigurations.member.{i}");
        let name = params.get(&format!("{prefix}.DimensionName"));
        let source = params.get(&format!("{prefix}.DimensionValueSource"));
        let default = params.get(&format!("{prefix}.DefaultDimensionValue"));
        if name.is_none() && source.is_none() && default.is_none() {
            break;
        }
        configs.push(serde_json::json!({
            "DimensionName": name.cloned().unwrap_or_default(),
            "DimensionValueSource": source.cloned().unwrap_or_default(),
            "DefaultDimensionValue": default.cloned().unwrap_or_default(),
        }));
    }
    if configs.is_empty() {
        None
    } else {
        Some(serde_json::json!({ "DimensionConfigurations": configs }))
    }
}

pub(crate) fn send_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let config_set_name = req.query_params.get("ConfigurationSetName").cloned();
    check_v1_sending_enabled(state, &req.account_id, config_set_name.as_deref())?;
    check_v1_verified_sender(state, &req.account_id, from)?;
    let to = parse_member_list(&req.query_params, "Destination.ToAddresses");
    let cc = parse_member_list(&req.query_params, "Destination.CcAddresses");
    let bcc = parse_member_list(&req.query_params, "Destination.BccAddresses");
    let recipients: Vec<String> = to
        .iter()
        .chain(cc.iter())
        .chain(bcc.iter())
        .cloned()
        .collect();
    check_v1_verified_recipients(state, &req.account_id, &recipients)?;
    check_v1_recipients_not_suppressed(
        state,
        &req.account_id,
        &recipients,
        config_set_name.as_deref(),
    )?;

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
        dkim_signature: None,
        headers: Vec::new(),
        timestamp: Utc::now(),
        email_tags: Vec::new(),
        delivery_insights: Vec::new(),
    };
    let sent = sign_sent_email(state, &req.account_id, &req.region, sent);

    state
        .write()
        .get_or_create(&req.account_id)
        .sent_emails
        .push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("SendEmail", SES_NS, &inner, &req.request_id),
    ))
}

/// DKIM-sign `sent` against the account's stored identities. No-op when
/// the sender has no matching verified identity or signing is disabled.
fn sign_sent_email(
    state: &SharedSesState,
    account_id: &str,
    _region: &str,
    sent: SentEmail,
) -> SentEmail {
    let signed = {
        let accounts = state.read();
        accounts
            .get(account_id)
            .and_then(|st| crate::dkim::signed_headers_for_sent_email(st, &sent))
    };
    match signed {
        Some((sig, hdrs)) => SentEmail {
            dkim_signature: Some(sig),
            headers: hdrs,
            ..sent
        },
        None => sent,
    }
}

pub(crate) fn send_raw_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let raw_data = required_param(&req.query_params, "RawMessage.Data")?;
    let from = req.query_params.get("Source").cloned().unwrap_or_default();
    let config_set_name = req.query_params.get("ConfigurationSetName").cloned();
    check_v1_sending_enabled(state, &req.account_id, config_set_name.as_deref())?;
    if !from.is_empty() {
        check_v1_verified_sender(state, &req.account_id, &from)?;
    }
    let to = parse_member_list(&req.query_params, "Destinations");
    check_v1_verified_recipients(state, &req.account_id, &to)?;
    check_v1_recipients_not_suppressed(state, &req.account_id, &to, config_set_name.as_deref())?;

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
        dkim_signature: None,
        headers: Vec::new(),
        timestamp: Utc::now(),
        email_tags: Vec::new(),
        delivery_insights: Vec::new(),
    };
    let sent = sign_sent_email(state, &req.account_id, &req.region, sent);

    state
        .write()
        .get_or_create(&req.account_id)
        .sent_emails
        .push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("SendRawEmail", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn send_templated_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let config_set_name = req.query_params.get("ConfigurationSetName").cloned();
    check_v1_sending_enabled(state, &req.account_id, config_set_name.as_deref())?;
    check_v1_verified_sender(state, &req.account_id, from)?;
    let template_name = required_param(&req.query_params, "Template")?;
    let template_data = required_param(&req.query_params, "TemplateData")?;
    let to = parse_member_list(&req.query_params, "Destination.ToAddresses");
    let cc = parse_member_list(&req.query_params, "Destination.CcAddresses");
    let bcc = parse_member_list(&req.query_params, "Destination.BccAddresses");

    // Verify template exists and capture a clone so we can render it
    // outside the read lock. Real SES surfaces missing templates as
    // `TemplateDoesNotExistException` (HTTP 400).
    let template_clone = {
        let accounts = state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        match st.templates.get(template_name) {
            Some(t) => t.clone(),
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "TemplateDoesNotExistException",
                    format!("Template '{template_name}' does not exist"),
                ));
            }
        }
    };

    let recipients: Vec<String> = to
        .iter()
        .chain(cc.iter())
        .chain(bcc.iter())
        .cloned()
        .collect();
    check_v1_verified_recipients(state, &req.account_id, &recipients)?;
    check_v1_recipients_not_suppressed(
        state,
        &req.account_id,
        &recipients,
        config_set_name.as_deref(),
    )?;

    let message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    // Render subject/html/text via the same engine TestRenderTemplate /
    // TestRenderEmailTemplate uses so introspection callers see the
    // materialized message, not the raw `{{placeholder}}` source.
    let rendered = crate::service::templates::render_template(&template_clone, template_data);

    let sent = SentEmail {
        message_id: message_id.clone(),
        from: from.to_string(),
        to,
        cc,
        bcc,
        subject: rendered.subject,
        html_body: rendered.html,
        text_body: rendered.text,
        raw_data: None,
        template_name: Some(template_name.to_string()),
        template_data: Some(template_data.to_string()),
        dkim_signature: None,
        headers: Vec::new(),
        timestamp: Utc::now(),
        email_tags: Vec::new(),
        delivery_insights: Vec::new(),
    };
    let sent = sign_sent_email(state, &req.account_id, &req.region, sent);

    state
        .write()
        .get_or_create(&req.account_id)
        .sent_emails
        .push(sent);

    let inner = format!("<MessageId>{message_id}</MessageId>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("SendTemplatedEmail", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn send_bulk_templated_email(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let from = required_param(&req.query_params, "Source")?;
    let config_set_name = req.query_params.get("ConfigurationSetName").cloned();
    check_v1_sending_enabled(state, &req.account_id, config_set_name.as_deref())?;
    check_v1_verified_sender(state, &req.account_id, from)?;
    let template_name = required_param(&req.query_params, "Template")?;
    let default_template_data = req
        .query_params
        .get("DefaultTemplateData")
        .cloned()
        .unwrap_or_else(|| "{}".to_string());

    {
        let accounts_r = state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let st_r = accounts_r.get(&req.account_id).unwrap_or(&empty);
        if !st_r.templates.contains_key(template_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TemplateDoesNotExistException",
                format!("Template '{template_name}' does not exist"),
            ));
        }
    }

    let mut inner = String::from("<Status>");
    for i in 1.. {
        let dest_prefix = format!("Destinations.member.{i}");
        if !req
            .query_params
            .contains_key(&format!("{dest_prefix}.Destination.ToAddresses.member.1"))
        {
            break;
        }
        let to = parse_member_list(
            &req.query_params,
            &format!("{dest_prefix}.Destination.ToAddresses"),
        );
        let cc = parse_member_list(
            &req.query_params,
            &format!("{dest_prefix}.Destination.CcAddresses"),
        );
        let bcc = parse_member_list(
            &req.query_params,
            &format!("{dest_prefix}.Destination.BccAddresses"),
        );
        let recipients: Vec<String> = to
            .iter()
            .chain(cc.iter())
            .chain(bcc.iter())
            .cloned()
            .collect();
        if let Err(err) = check_v1_verified_recipients(state, &req.account_id, &recipients) {
            // Real SES surfaces unverified recipients per-destination
            // rather than aborting the whole batch. (From-domain gate is
            // enforced up-front by `check_v1_verified_sender`.)
            inner.push_str(&format!(
                "<member><Status>MessageRejected</Status><Error>{}</Error></member>",
                xml_escape(&err.message()),
            ));
            continue;
        }
        let any_suppressed = recipients
            .iter()
            .any(|r| is_address_suppressed(state, &req.account_id, r, config_set_name.as_deref()));
        if any_suppressed {
            bump_suppression_drop(state, &req.account_id);
            inner.push_str(
                "<member><Status>MessageRejected</Status><Error>Address is on the suppression list</Error></member>",
            );
            continue;
        }
        let message_id = send_bulk_destination(
            state,
            &req.query_params,
            &dest_prefix,
            from,
            template_name,
            &default_template_data,
            &req.account_id,
        );
        inner.push_str(&format!(
            "<member><Status>Success</Status><MessageId>{message_id}</MessageId></member>"
        ));
    }
    inner.push_str("</Status>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("SendBulkTemplatedEmail", SES_NS, &inner, &req.request_id),
    ))
}

/// Record one destination entry from a SendBulkTemplatedEmail call and
/// return the generated message id.
pub(crate) fn send_bulk_destination(
    state: &SharedSesState,
    params: &HashMap<String, String>,
    dest_prefix: &str,
    from: &str,
    template_name: &str,
    default_template_data: &str,
    account_id: &str,
) -> String {
    let to = parse_member_list(params, &format!("{dest_prefix}.Destination.ToAddresses"));
    let replacement_data = params
        .get(&format!("{dest_prefix}.ReplacementTemplateData"))
        .cloned()
        .unwrap_or_else(|| default_template_data.to_string());

    let message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    // Look up the template once to render the destination's substitutions.
    // The caller has already checked that the template exists.
    let template_clone = {
        let accounts = state.read();
        accounts
            .get(account_id)
            .and_then(|st| st.templates.get(template_name).cloned())
    };
    let rendered = template_clone
        .as_ref()
        .map(|t| crate::service::templates::render_template(t, &replacement_data));

    let sent = SentEmail {
        message_id: message_id.clone(),
        from: from.to_string(),
        to,
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: rendered.as_ref().and_then(|r| r.subject.clone()),
        html_body: rendered.as_ref().and_then(|r| r.html.clone()),
        text_body: rendered.as_ref().and_then(|r| r.text.clone()),
        raw_data: None,
        template_name: Some(template_name.to_string()),
        template_data: Some(replacement_data),
        dkim_signature: None,
        headers: Vec::new(),
        timestamp: Utc::now(),
        email_tags: Vec::new(),
        delivery_insights: Vec::new(),
    };
    let sent = sign_sent_email(state, account_id, "", sent);

    state
        .write()
        .get_or_create(account_id)
        .sent_emails
        .push(sent);
    message_id
}

pub(crate) fn send_bounce(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let bounce_sender = required_param(&req.query_params, "BounceSender")?;
    let original_message_id = required_param(&req.query_params, "OriginalMessageId")?;

    let mut recipients: Vec<String> = Vec::new();
    let mut recipient_xml = String::new();
    let mut recipient_info: Vec<crate::state::BouncedRecipientInfo> = Vec::new();
    for i in 1.. {
        let prefix = format!("BouncedRecipientInfoList.member.{i}");
        let recipient = match req.query_params.get(&format!("{prefix}.Recipient")) {
            Some(r) => r.clone(),
            None => break,
        };
        recipients.push(recipient.clone());
        let bounce_type = req
            .query_params
            .get(&format!("{prefix}.BounceType"))
            .cloned()
            .unwrap_or_else(|| "ContentRejected".to_string());
        let action = req
            .query_params
            .get(&format!("{prefix}.RecipientDsnFields.Action"))
            .cloned()
            .unwrap_or_else(|| "failed".to_string());
        let status = req
            .query_params
            .get(&format!("{prefix}.RecipientDsnFields.Status"))
            .cloned()
            .unwrap_or_else(|| "5.1.1".to_string());
        let diagnostic = req
            .query_params
            .get(&format!("{prefix}.RecipientDsnFields.DiagnosticCode"))
            .cloned()
            .unwrap_or_else(|| "smtp; 550 5.1.1 user unknown".to_string());
        recipient_xml.push_str(&format!(
            "<member>\
             <Recipient>{recipient}</Recipient>\
             <StatusCode>{status}</StatusCode>\
             <Action>{action}</Action>\
             <DiagnosticCode>{diagnostic}</DiagnosticCode>\
             <BounceType>{bounce_type}</BounceType>\
             </member>"
        ));
        recipient_info.push(crate::state::BouncedRecipientInfo {
            recipient: recipient.clone(),
            bounce_type,
            action,
            status,
            diagnostic_code: diagnostic,
        });
    }
    if recipients.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            "BouncedRecipientInfoList is required",
        ));
    }

    let bounce_message_id = format!(
        "{:016x}{:016x}-{:08x}-{:04x}",
        rand_u64(),
        rand_u64(),
        rand_u32(),
        rand_u16(),
    );

    let explanation = req.query_params.get("Explanation").cloned();
    let bounce = crate::state::SentBounce {
        bounce_message_id: bounce_message_id.clone(),
        original_message_id: original_message_id.to_string(),
        bounce_sender: bounce_sender.to_string(),
        bounced_recipients: recipients,
        timestamp: Utc::now(),
        bounced_recipient_info: recipient_info,
        explanation,
    };
    state
        .write()
        .get_or_create(&req.account_id)
        .bounces
        .push(bounce);

    let inner = format!(
        "<MessageId>{bounce_message_id}</MessageId>\
         <BouncedRecipientInfoList>{recipient_xml}</BouncedRecipientInfoList>"
    );
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("SendBounce", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn create_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Template.TemplateName")?;
    let subject = req.query_params.get("Template.SubjectPart").cloned();
    let html = req.query_params.get("Template.HtmlPart").cloned();
    let text = req.query_params.get("Template.TextPart").cloned();

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn get_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "TemplateName")?;
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("GetTemplate", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn list_templates(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("ListTemplates", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn delete_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "TemplateName")?;
    state
        .write()
        .get_or_create(&req.account_id)
        .templates
        .remove(name);
    // AWS returns success even if template doesn't exist
    Ok(xml_metadata_only("DeleteTemplate", &req.request_id))
}

pub(crate) fn update_template(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Template.TemplateName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn create_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSet.Name")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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
            max_delivery_seconds: None,
            custom_redirect_domain: None,
            https_policy: None,
            suppressed_reasons: Vec::new(),
            reputation_metrics_enabled: false,
            vdm_options: None,
            archive_arn: None,
            archiving_options_present: false,
        },
    );
    Ok(xml_metadata_only("CreateConfigurationSet", &req.request_id))
}

pub(crate) fn delete_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSetName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn describe_configuration_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "ConfigurationSetName")?;
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
    // ReputationOptions: AWS always reports LastFreshStart (the time reputation
    // metrics were last reset for the set). The Terraform resource reads
    // `last_fresh_start` and asserts it is set. We report a stable timestamp so
    // it is present without re-planning.
    // The SES v1 DescribeConfigurationSet response nests SendingEnabled inside
    // ReputationOptions (the resource reads last_fresh_start /
    // reputation_metrics_enabled / sending_enabled all from this block).
    inner.push_str(&format!(
        "<ReputationOptions>\
         <SendingEnabled>{}</SendingEnabled>\
         <ReputationMetricsEnabled>{}</ReputationMetricsEnabled>\
         <LastFreshStart>2024-01-01T00:00:00Z</LastFreshStart>\
         </ReputationOptions>",
        cs.sending_enabled, cs.reputation_metrics_enabled
    ));
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
            inner.push_str("</MatchingEventTypes>");
            // Echo the configured destination target(s). Previously the
            // describe response dropped these entirely, so a Kinesis /
            // CloudWatch / SNS destination created against the set was
            // invisible on read.
            if let Some(k) = &dest.kinesis_firehose_destination {
                inner.push_str(&format!(
                    "<KinesisFirehoseDestination>\
                     <IAMRoleARN>{}</IAMRoleARN>\
                     <DeliveryStreamARN>{}</DeliveryStreamARN>\
                     </KinesisFirehoseDestination>",
                    xml_escape(k["IAMRoleARN"].as_str().unwrap_or("")),
                    xml_escape(k["DeliveryStreamARN"].as_str().unwrap_or("")),
                ));
            }
            if let Some(cw) = &dest.cloud_watch_destination {
                inner.push_str("<CloudWatchDestination><DimensionConfigurations>");
                if let Some(arr) = cw["DimensionConfigurations"].as_array() {
                    for dc in arr {
                        inner.push_str(&format!(
                            "<member>\
                             <DimensionName>{}</DimensionName>\
                             <DimensionValueSource>{}</DimensionValueSource>\
                             <DefaultDimensionValue>{}</DefaultDimensionValue>\
                             </member>",
                            xml_escape(dc["DimensionName"].as_str().unwrap_or("")),
                            xml_escape(dc["DimensionValueSource"].as_str().unwrap_or("")),
                            xml_escape(dc["DefaultDimensionValue"].as_str().unwrap_or("")),
                        ));
                    }
                }
                inner.push_str("</DimensionConfigurations></CloudWatchDestination>");
            }
            if let Some(sns) = &dest.sns_destination {
                inner.push_str(&format!(
                    "<SNSDestination><TopicARN>{}</TopicARN></SNSDestination>",
                    xml_escape(sns["TopicArn"].as_str().unwrap_or("")),
                ));
            }
            inner.push_str("</member>");
        }
        inner.push_str("</EventDestinations>");
    }
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("DescribeConfigurationSet", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn list_configuration_sets(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("ListConfigurationSets", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn create_configuration_set_event_destination(
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

    {
        let accounts = state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let st = accounts.get(&req.account_id).unwrap_or(&empty);
        if !st.configuration_sets.contains_key(config_set_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConfigurationSetDoesNotExistException",
                format!("Configuration set '{config_set_name}' does not exist"),
            ));
        }
    }

    let dest = EventDestination {
        name: dest_name.to_string(),
        enabled,
        matching_event_types: event_types,
        kinesis_firehose_destination: parse_kinesis_firehose_destination(&req.query_params),
        cloud_watch_destination: parse_cloudwatch_destination(&req.query_params),
        sns_destination: req
            .query_params
            .get("EventDestination.SNSDestination.TopicARN")
            .map(|arn| serde_json::json!({ "TopicArn": arn })),
        event_bridge_destination: None,
        pinpoint_destination: None,
    };

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    st.event_destinations
        .entry(config_set_name.to_string())
        .or_default()
        .push(dest);
    Ok(xml_metadata_only(
        "CreateConfigurationSetEventDestination",
        &req.request_id,
    ))
}

pub(crate) fn update_configuration_set_event_destination(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let config_set_name = required_param(&req.query_params, "ConfigurationSetName")?;
    let dest_name = required_param(&req.query_params, "EventDestination.Name")?;

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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
    // Apply destination-target changes when supplied so an update that
    // switches or (re)configures Kinesis/CloudWatch/SNS destinations is
    // reflected on the read side.
    if let Some(k) = parse_kinesis_firehose_destination(&req.query_params) {
        dest.kinesis_firehose_destination = Some(k);
    }
    if let Some(cw) = parse_cloudwatch_destination(&req.query_params) {
        dest.cloud_watch_destination = Some(cw);
    }
    if let Some(arn) = req
        .query_params
        .get("EventDestination.SNSDestination.TopicARN")
    {
        dest.sns_destination = Some(serde_json::json!({ "TopicArn": arn }));
    }

    Ok(xml_metadata_only(
        "UpdateConfigurationSetEventDestination",
        &req.request_id,
    ))
}

pub(crate) fn delete_configuration_set_event_destination(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let config_set_name = required_param(&req.query_params, "ConfigurationSetName")?;
    let dest_name = required_param(&req.query_params, "EventDestinationName")?;

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn get_send_quota(
    _state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inner = "\
        <Max24HourSend>50000.0</Max24HourSend>\
        <MaxSendRate>14.0</MaxSendRate>\
        <SentLast24Hours>0.0</SentLast24Hours>";
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("GetSendQuota", SES_NS, inner, &req.request_id),
    ))
}

pub(crate) fn get_send_statistics(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("GetSendStatistics", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn get_account_sending_enabled(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
    let enabled = st.account_settings.sending_enabled;
    let inner = format!("<Enabled>{enabled}</Enabled>");
    Ok(AwsResponse::xml(
        StatusCode::OK,
        query_response_xml("GetAccountSendingEnabled", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn rand_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

pub(crate) fn rand_u32() -> u32 {
    rand_u64() as u32
}

pub(crate) fn rand_u16() -> u16 {
    rand_u64() as u16
}

pub(crate) fn create_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn delete_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn describe_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "RuleSetName")?;
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("DescribeReceiptRuleSet", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn list_receipt_rule_sets(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("ListReceiptRuleSets", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn clone_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let new_name = required_param(&req.query_params, "RuleSetName")?;
    let source_name = required_param(&req.query_params, "OriginalRuleSetName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);

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

pub(crate) fn set_active_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn reorder_receipt_rule_set(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn create_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule = parse_receipt_rule(&req.query_params)?;
    let after = req.query_params.get("After").cloned();

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn delete_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule_name = required_param(&req.query_params, "RuleName")?;

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn describe_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let rule_name = required_param(&req.query_params, "RuleName")?;

    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("DescribeReceiptRule", SES_NS, &inner, &req.request_id),
    ))
}

pub(crate) fn update_receipt_rule(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rule_set_name = required_param(&req.query_params, "RuleSetName")?;
    let new_rule = parse_receipt_rule(&req.query_params)?;

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn create_receipt_filter(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "Filter.Name")?;
    let cidr = required_param(&req.query_params, "Filter.IpFilter.Cidr")?;
    let policy = required_param(&req.query_params, "Filter.IpFilter.Policy")?;

    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
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

pub(crate) fn delete_receipt_filter(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = required_param(&req.query_params, "FilterName")?;
    let mut accounts = state.write();
    let st = accounts.get_or_create(&req.account_id);
    if st.receipt_filters.remove(name).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "FilterDoesNotExistException",
            format!("Filter with name '{name}' does not exist"),
        ));
    }
    Ok(xml_metadata_only("DeleteReceiptFilter", &req.request_id))
}

pub(crate) fn list_receipt_filters(
    state: &SharedSesState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = state.read();
    let empty = SesState::new(&req.account_id, &req.region);
    let st = accounts.get(&req.account_id).unwrap_or(&empty);
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
        query_response_xml("ListReceiptFilters", SES_NS, &inner, &req.request_id),
    ))
}

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
    let accounts = state.read();
    let st = accounts.default_ref();

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
    drop(accounts);
    let mut mas_w = state.write();
    let st = mas_w.default_mut();
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

pub(crate) fn action_type_name(action: &ReceiptAction) -> &'static str {
    match action {
        ReceiptAction::S3 { .. } => "S3",
        ReceiptAction::Sns { .. } => "SNS",
        ReceiptAction::Lambda { .. } => "Lambda",
        ReceiptAction::Bounce { .. } => "Bounce",
        ReceiptAction::AddHeader { .. } => "AddHeader",
        ReceiptAction::Stop { .. } => "Stop",
        ReceiptAction::Workmail { .. } => "Workmail",
    }
}
