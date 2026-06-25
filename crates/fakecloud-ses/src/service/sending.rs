use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SentEmail;

use super::{extract_string_array, SesV2Service};

/// Extract bare email address from a "From" header. Strips display-name
/// wrappers like `Foo <foo@example.com>` and surrounding whitespace.
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

/// Real SES treats every address on the mailbox simulator domain as
/// implicitly verified, both for senders and (in sandbox accounts) for
/// recipients. The well-known mailboxes are
/// `bounce@simulator.amazonses.com`, `complaint@simulator.amazonses.com`,
/// `success@simulator.amazonses.com`, `suppressionlist@simulator.amazonses.com`,
/// and `ooto@simulator.amazonses.com`, plus the auto-responder
/// `*@simulator.amazonses.com` form (e.g. `auto-responder-N@`). We accept
/// the whole domain instead of hard-coding a list — AWS docs explicitly
/// say the domain itself bypasses the verification gate.
pub(super) fn is_simulator_address(email: &str) -> bool {
    matches!(email.split_once('@'), Some((_, "simulator.amazonses.com")))
}

/// Parse a SESv2 message-tag list (`[{Name, Value}]`) into name/value pairs.
fn parse_message_tags(value: &Value) -> Vec<(String, String)> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    Some((
                        t["Name"].as_str()?.to_string(),
                        t["Value"].as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Match an email against verified identities: exact email or matching
/// verified-domain. Shared between sender + sandbox-recipient gates.
/// Mailbox-simulator addresses bypass the gate (real SES treats them as
/// always-verified).
pub(super) fn identity_is_verified(state: &crate::state::SesState, email: &str) -> bool {
    if is_simulator_address(email) {
        return true;
    }
    if state
        .identities
        .get(email)
        .map(|id| id.verified)
        .unwrap_or(false)
    {
        return true;
    }
    if let Some((_, domain)) = email.split_once('@') {
        if !domain.is_empty()
            && state
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

impl SesV2Service {
    fn render_template_for_send(
        &self,
        account_id: &str,
        template_name: Option<&str>,
        template_data: Option<&str>,
    ) -> super::templates::RenderedTemplate {
        let empty = super::templates::RenderedTemplate {
            subject: None,
            html: None,
            text: None,
        };
        let Some(name) = template_name else {
            return empty;
        };
        let data_str = template_data.unwrap_or("{}");
        let accounts = self.state.read();
        let Some(state) = accounts.get(account_id) else {
            return empty;
        };
        let Some(template) = state.templates.get(name) else {
            return empty;
        };
        super::templates::render_template(template, data_str)
    }

    fn compute_dkim_signature(
        &self,
        account_id: &str,
        sent: &SentEmail,
    ) -> Option<(String, Vec<(String, String)>)> {
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        crate::dkim::signed_headers_for_sent_email(state, sent)
    }

    /// Reject the send if either account-level sending or the resolved
    /// configuration set's sending flag is paused. SES v2 collapses both
    /// to `SendingPausedException` (HTTP 400) per the Smithy model.
    fn check_sending_enabled(
        &self,
        account_id: &str,
        config_set_name: Option<&str>,
    ) -> Option<AwsResponse> {
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        if !state.account_settings.sending_enabled {
            return Some(Self::json_error(
                StatusCode::BAD_REQUEST,
                "SendingPausedException",
                "Email sending for the account is paused.",
            ));
        }
        if let Some(name) = config_set_name {
            if let Some(cs) = state.configuration_sets.get(name) {
                if !cs.sending_enabled {
                    return Some(Self::json_error(
                        StatusCode::BAD_REQUEST,
                        "SendingPausedException",
                        &format!("Email sending for the configuration set {name} is paused."),
                    ));
                }
            }
        }
        None
    }

    /// Returns `true` if `address` is on the account suppression list
    /// AND its stored reason matches the effective `SuppressedReasons`
    /// filter (configuration-set scope first, then account-level
    /// fallback). Address lookup is case-insensitive.
    pub(super) fn address_is_suppressed(
        &self,
        account_id: &str,
        address: &str,
        config_set_name: Option<&str>,
    ) -> bool {
        let accounts = self.state.read();
        let Some(state) = accounts.get(account_id) else {
            return false;
        };
        let bare = extract_email_address(address);
        state.suppressed_match(bare, config_set_name).is_some()
    }

    /// Increment the suppression-drop counter on the account state.
    /// Surfaced via the introspection endpoint so tests can assert the
    /// gate fired without scraping logs.
    pub(super) fn bump_suppression_drop(&self, account_id: &str) {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.suppressed_drops_total = state.suppressed_drops_total.saturating_add(1);
    }

    /// Reject sends where the sender is not a verified identity. Mirrors
    /// real SES: every From address must either match a verified email
    /// identity exactly, or its domain must match a verified domain
    /// identity. Real SES v2 surfaces this as
    /// `MailFromDomainNotVerifiedException` (HTTP 400).
    pub(super) fn reject_unverified_sender(
        &self,
        account_id: &str,
        from: &str,
    ) -> Option<AwsResponse> {
        let email = extract_email_address(from);
        if email.is_empty() {
            return Some(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "FromEmailAddress is required",
            ));
        }

        let accounts = self.state.read();
        let verified = accounts
            .get(account_id)
            .map(|st| identity_is_verified(st, email))
            .unwrap_or(false);

        if verified {
            None
        } else {
            Some(Self::json_error(
                StatusCode::BAD_REQUEST,
                "MailFromDomainNotVerifiedException",
                "Mail-From domain not verified.",
            ))
        }
    }

    /// In sandbox accounts (`production_access_enabled = false`), every
    /// recipient must also belong to a verified identity. Real SES
    /// surfaces this as `MessageRejected` listing the failing addresses.
    pub(super) fn reject_unverified_recipients(
        &self,
        account_id: &str,
        recipients: &[&str],
    ) -> Option<AwsResponse> {
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        if state.account_settings.production_access_enabled {
            return None;
        }
        let mut failing: Vec<String> = Vec::new();
        for raw in recipients {
            let addr = extract_email_address(raw);
            if addr.is_empty() {
                continue;
            }
            if !identity_is_verified(state, addr) {
                failing.push(addr.to_string());
            }
        }
        if failing.is_empty() {
            None
        } else {
            Some(Self::json_error(
                StatusCode::BAD_REQUEST,
                "MessageRejected",
                &format!(
                    "Email address is not verified. The following identities failed the check: {}",
                    failing.join(", ")
                ),
            ))
        }
    }

    pub(super) fn send_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        if !body["Content"].is_object()
            || (!body["Content"]["Simple"].is_object()
                && !body["Content"]["Raw"].is_object()
                && !body["Content"]["Template"].is_object())
        {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Content is required and must contain Simple, Raw, or Template",
            ));
        }

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();
        let config_set_name = body["ConfigurationSetName"].as_str().map(|s| s.to_string());
        if let Some(err) = self.check_sending_enabled(&req.account_id, config_set_name.as_deref()) {
            return Ok(err);
        }
        if let Some(err) = self.reject_unverified_sender(&req.account_id, &from) {
            return Ok(err);
        }

        let to = extract_string_array(&body["Destination"]["ToAddresses"]);
        let cc = extract_string_array(&body["Destination"]["CcAddresses"]);
        let bcc = extract_string_array(&body["Destination"]["BccAddresses"]);

        let recipients: Vec<&str> = to
            .iter()
            .chain(cc.iter())
            .chain(bcc.iter())
            .map(|s| s.as_str())
            .collect();
        if let Some(err) = self.reject_unverified_recipients(&req.account_id, &recipients) {
            return Ok(err);
        }

        // Single-recipient path: any suppressed recipient kills the send.
        // Real SES surfaces this as `MessageRejected`. Suppression is
        // gated by the effective `SuppressedReasons` filter — if the
        // address was suppressed for `COMPLAINT` but the config set only
        // enforces `BOUNCE`, the send proceeds.
        for r in &recipients {
            let addr = extract_email_address(r);
            if self.address_is_suppressed(&req.account_id, addr, config_set_name.as_deref()) {
                self.bump_suppression_drop(&req.account_id);
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "MessageRejected",
                    "Address is on the suppression list",
                ));
            }
        }

        let (subject, html_body, text_body, raw_data, template_name, template_data) =
            if body["Content"]["Simple"].is_object() {
                let simple = &body["Content"]["Simple"];
                let subject = simple["Subject"]["Data"].as_str().map(|s| s.to_string());
                let html = simple["Body"]["Html"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                let text = simple["Body"]["Text"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (subject, html, text, None, None, None)
            } else if body["Content"]["Raw"].is_object() {
                let raw = body["Content"]["Raw"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (None, None, None, raw, None, None)
            } else if body["Content"]["Template"].is_object() {
                let tmpl = &body["Content"]["Template"];
                let tmpl_name = tmpl["TemplateName"].as_str().map(|s| s.to_string());
                let tmpl_data = tmpl["TemplateData"].as_str().map(|s| s.to_string());
                // Real SES rejects sends that reference a missing template
                // with `TemplateDoesNotExistException` (HTTP 400). Without
                // this gate, we'd silently produce an empty rendered body.
                if let Some(name) = tmpl_name.as_deref() {
                    let accounts = self.state.read();
                    let exists = accounts
                        .get(&req.account_id)
                        .map(|st| st.templates.contains_key(name))
                        .unwrap_or(false);
                    drop(accounts);
                    if !exists {
                        return Ok(Self::json_error(
                            StatusCode::BAD_REQUEST,
                            "TemplateDoesNotExistException",
                            &format!("Template {name} does not exist"),
                        ));
                    }
                }
                // Render via the same engine RenderEmailTemplate uses so
                // SentEmail captures the materialized subject/html/text.
                let rendered = self.render_template_for_send(
                    &req.account_id,
                    tmpl_name.as_deref(),
                    tmpl_data.as_deref(),
                );
                (
                    rendered.subject,
                    rendered.html,
                    rendered.text,
                    None,
                    tmpl_name,
                    tmpl_data,
                )
            } else {
                (None, None, None, None, None, None)
            };

        let email_tags = body["EmailTags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let name = t["Name"].as_str()?;
                        let value = t["Value"].as_str()?;
                        Some((name.to_string(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let message_id = uuid::Uuid::new_v4().to_string();

        let sent = SentEmail {
            message_id: message_id.clone(),
            from,
            to,
            cc,
            bcc,
            subject,
            html_body,
            text_body,
            raw_data,
            template_name,
            template_data,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags,
            delivery_insights: Vec::new(),
        };

        let signed = self.compute_dkim_signature(&req.account_id, &sent);
        let mut sent = match signed {
            Some((sig, hdrs)) => SentEmail {
                dkim_signature: Some(sig),
                headers: hdrs,
                ..sent
            },
            None => sent,
        };

        // Event fanout: check suppression list, generate events, deliver to destinations
        if let Some(ref ctx) = self.delivery_ctx {
            crate::fanout::process_send_events(ctx, &mut sent, config_set_name.as_deref());
        }

        // Opt-in SMTP relay: when FAKECLOUD_SES_SMTP_RELAY is set, fire a
        // best-effort send to the configured MTA (e.g. Mailpit) so dev
        // setups see real mail in their inbox. Failures are logged and
        // don't fail the SES call — the in-memory store is the source
        // of truth for tests.
        if let Ok(relay_url) = std::env::var("FAKECLOUD_SES_SMTP_RELAY") {
            let outbound = crate::smtp_relay::OutboundMail {
                from: &sent.from,
                to: &sent.to,
                cc: &sent.cc,
                bcc: &sent.bcc,
                subject: sent.subject.as_deref(),
                text_body: sent.text_body.as_deref(),
                html_body: sent.html_body.as_deref(),
            };
            if let Err(err) = crate::smtp_relay::relay(&relay_url, &outbound) {
                tracing::warn!(relay = %relay_url, error = %err, "SES SMTP relay failed");
            }
        }

        self.state
            .write()
            .get_or_create(&req.account_id)
            .sent_emails
            .push(sent);

        let response = json!({
            "MessageId": message_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn send_bulk_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();
        if let Some(err) = self.reject_unverified_sender(&req.account_id, &from) {
            return Ok(err);
        }
        let config_set_name = body["ConfigurationSetName"].as_str().map(|s| s.to_string());
        if let Some(err) = self.check_sending_enabled(&req.account_id, config_set_name.as_deref()) {
            return Ok(err);
        }

        let entries = match body["BulkEmailEntries"].as_array() {
            Some(arr) if !arr.is_empty() => arr.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "BulkEmailEntries is required and must not be empty",
                ));
            }
        };

        // Reject the whole batch up-front if the referenced template
        // doesn't exist. Real SES surfaces this as
        // `TemplateDoesNotExistException` (HTTP 400).
        if let Some(name) = body["DefaultContent"]["Template"]["TemplateName"].as_str() {
            let accounts = self.state.read();
            let exists = accounts
                .get(&req.account_id)
                .map(|st| st.templates.contains_key(name))
                .unwrap_or(false);
            drop(accounts);
            if !exists {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "TemplateDoesNotExistException",
                    &format!("Template {name} does not exist"),
                ));
            }
        }

        // Tags applied to every entry unless overridden per-entry.
        let default_tags = parse_message_tags(&body["DefaultEmailTags"]);

        let mut results = Vec::new();

        for entry in &entries {
            let to = extract_string_array(&entry["Destination"]["ToAddresses"]);
            let cc = extract_string_array(&entry["Destination"]["CcAddresses"]);
            let bcc = extract_string_array(&entry["Destination"]["BccAddresses"]);

            let recipients: Vec<&str> = to
                .iter()
                .chain(cc.iter())
                .chain(bcc.iter())
                .map(|s| s.as_str())
                .collect();
            if self
                .reject_unverified_recipients(&req.account_id, &recipients)
                .is_some()
            {
                // Real SES surfaces unverified recipients per-entry,
                // not as a whole-batch failure.
                results.push(json!({
                    "Status": "MESSAGE_REJECTED",
                    "Error": "Email address is not verified.",
                }));
                continue;
            }

            // Drop entries with any suppressed recipient. Mirrors SES,
            // which fails the bulk entry rather than the whole batch.
            // Honors the effective `SuppressedReasons` filter so callers
            // can scope suppression to BOUNCE-only or COMPLAINT-only.
            let any_suppressed = recipients.iter().any(|r| {
                let addr = extract_email_address(r);
                self.address_is_suppressed(&req.account_id, addr, config_set_name.as_deref())
            });
            if any_suppressed {
                self.bump_suppression_drop(&req.account_id);
                results.push(json!({
                    "Status": "MESSAGE_REJECTED",
                    "Error": "Address is on the suppression list",
                }));
                continue;
            }

            let message_id = uuid::Uuid::new_v4().to_string();

            let template_name = body["DefaultContent"]["Template"]["TemplateName"]
                .as_str()
                .map(|s| s.to_string());
            let template_data = entry["ReplacementEmailContent"]["ReplacementTemplate"]
                ["ReplacementTemplateData"]
                .as_str()
                .or_else(|| body["DefaultContent"]["Template"]["TemplateData"].as_str())
                .map(|s| s.to_string());

            let rendered = self.render_template_for_send(
                &req.account_id,
                template_name.as_deref(),
                template_data.as_deref(),
            );

            // Per-entry ReplacementTags override the batch default tags by name
            // (previously dropped, so bulk sends carried no message tags).
            let mut email_tags = default_tags.clone();
            for (name, value) in parse_message_tags(&entry["ReplacementTags"]) {
                if let Some(existing) = email_tags.iter_mut().find(|(n, _)| *n == name) {
                    existing.1 = value;
                } else {
                    email_tags.push((name, value));
                }
            }

            let sent = SentEmail {
                message_id: message_id.clone(),
                from: from.clone(),
                to,
                cc,
                bcc,
                subject: rendered.subject,
                html_body: rendered.html,
                text_body: rendered.text,
                raw_data: None,
                template_name,
                template_data,
                dkim_signature: None,
                headers: Vec::new(),
                timestamp: Utc::now(),
                email_tags,
                delivery_insights: Vec::new(),
            };
            let signed = self.compute_dkim_signature(&req.account_id, &sent);
            let mut sent = match signed {
                Some((sig, hdrs)) => SentEmail {
                    dkim_signature: Some(sig),
                    headers: hdrs,
                    ..sent
                },
                None => sent,
            };

            // Event fanout for each bulk entry
            if let Some(ref ctx) = self.delivery_ctx {
                crate::fanout::process_send_events(ctx, &mut sent, config_set_name.as_deref());
            }

            self.state
                .write()
                .get_or_create(&req.account_id)
                .sent_emails
                .push(sent);

            results.push(json!({
                "Status": "SUCCESS",
                "MessageId": message_id,
            }));
        }

        let response = json!({
            "BulkEmailEntryResults": results,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}
