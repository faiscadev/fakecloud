// Auto-extracted from service.rs as part of carryover service.rs split.
// Methods on `SnsService` grouped by resource concern.

#![allow(clippy::too_many_arguments)]

use http::StatusCode;
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl SnsService {
    pub(super) fn create_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required(req, "Name")?;
        let platform = required(req, "Platform")?;
        let attributes = parse_entries(req, "Attributes");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = format!(
            "arn:aws:sns:{}:{}:app/{}/{}",
            req.region, state.account_id, platform, name
        );

        state.platform_applications.insert(
            arn.clone(),
            PlatformApplication {
                arn: arn.clone(),
                name,
                platform,
                attributes,
                endpoints: BTreeMap::new(),
            },
        );

        Ok(xml_resp(
            &format!(
                r#"<CreatePlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformApplicationResult>
    <PlatformApplicationArn>{arn}</PlatformApplicationArn>
  </CreatePlatformApplicationResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        self.state
            .write()
            .get_or_create(&req.account_id)
            .platform_applications
            .remove(&arn);

        Ok(xml_resp(
            &format!(
                r#"<DeletePlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeletePlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_platform_application_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let app = state
            .platform_applications
            .get(&arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        let attrs: String = app
            .attributes
            .iter()
            .map(|(k, v)| format_attr(k, v))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetPlatformApplicationAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetPlatformApplicationAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetPlatformApplicationAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetPlatformApplicationAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn set_platform_application_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required(req, "PlatformApplicationArn")?;
        let new_attrs = parse_entries(req, "Attributes");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let app = state
            .platform_applications
            .get_mut(&arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        for (k, v) in new_attrs {
            app.attributes.insert(k, v);
        }

        Ok(xml_resp(
            &format!(
                r#"<SetPlatformApplicationAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetPlatformApplicationAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn list_platform_applications(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        let items: Vec<(String, String)> = state
            .platform_applications
            .values()
            .map(|app| {
                let attrs: String = app
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");
                let member = format!(
                    r#"      <member>
        <PlatformApplicationArn>{}</PlatformApplicationArn>
        <Attributes>
{attrs}
        </Attributes>
      </member>"#,
                    app.arn
                );
                (app.arn.clone(), member)
            })
            .collect();
        let (members, next_token) = paginate_sns_members(items, param(req, "NextToken").as_deref());
        let next_token_xml = next_token
            .map(|t| format!("\n    <NextToken>{t}</NextToken>"))
            .unwrap_or_default();

        Ok(xml_resp(
            &format!(
                r#"<ListPlatformApplicationsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListPlatformApplicationsResult>
    <PlatformApplications>
{members}
    </PlatformApplications>{next_token_xml}
  </ListPlatformApplicationsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPlatformApplicationsResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn create_platform_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_arn = required(req, "PlatformApplicationArn")?;
        let token = required(req, "Token")?;
        let custom_user_data = param(req, "CustomUserData");
        let attrs = parse_entries(req, "Attributes");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let account_id = state.account_id.clone();
        let app = state
            .platform_applications
            .get_mut(&app_arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        // Check for existing endpoint with same token
        for (arn, ep) in &app.endpoints {
            if ep.token == token {
                // If attributes are different, check Enabled attribute
                let existing_enabled = ep
                    .attributes
                    .get("Enabled")
                    .cloned()
                    .unwrap_or_else(|| "true".to_string());
                let new_enabled = attrs
                    .get("Enabled")
                    .cloned()
                    .unwrap_or_else(|| "true".to_string());
                let custom_matches = match (&custom_user_data, ep.attributes.get("CustomUserData"))
                {
                    (Some(new), Some(old)) => new == old,
                    (None, None) => true,
                    (None, Some(_)) => true,
                    _ => false,
                };

                if existing_enabled == new_enabled && custom_matches {
                    // Return existing endpoint
                    return Ok(xml_resp(
                        &format!(
                            r#"<CreatePlatformEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformEndpointResult>
    <EndpointArn>{arn}</EndpointArn>
  </CreatePlatformEndpointResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformEndpointResponse>"#,
                            req.request_id
                        ),
                        &req.request_id,
                    ));
                } else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameter",
                        format!("Invalid parameter: Token Reason: Endpoint {} already exists with the same Token, but different attributes.", arn),
                    ));
                }
            }
        }

        let endpoint_id = uuid::Uuid::new_v4().to_string().replace('-', "");
        let endpoint_arn = format!(
            "arn:aws:sns:{}:{}:endpoint/{}/{}/{}",
            req.region, account_id, app.platform, app.name, endpoint_id
        );

        let mut endpoint_attrs = attrs;
        endpoint_attrs
            .entry("Enabled".to_string())
            .or_insert_with(|| "true".to_string());
        endpoint_attrs.insert("Token".to_string(), token.clone());
        if let Some(ref ud) = custom_user_data {
            endpoint_attrs
                .entry("CustomUserData".to_string())
                .or_insert_with(|| ud.clone());
        }

        let enabled = endpoint_attrs
            .get("Enabled")
            .map(|v| v == "true")
            .unwrap_or(true);

        app.endpoints.insert(
            endpoint_arn.clone(),
            PlatformEndpoint {
                arn: endpoint_arn.clone(),
                token,
                attributes: endpoint_attrs,
                enabled,
                messages: Vec::new(),
            },
        );

        Ok(xml_resp(
            &format!(
                r#"<CreatePlatformEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreatePlatformEndpointResult>
    <EndpointArn>{endpoint_arn}</EndpointArn>
  </CreatePlatformEndpointResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePlatformEndpointResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for app in state.platform_applications.values_mut() {
            app.endpoints.remove(&endpoint_arn);
        }

        Ok(xml_resp(
            &format!(
                r#"<DeleteEndpointResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteEndpointResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_endpoint_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        for app in state.platform_applications.values() {
            if let Some(ep) = app.endpoints.get(&endpoint_arn) {
                let attrs: String = ep
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");

                return Ok(xml_resp(
                    &format!(
                        r#"<GetEndpointAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetEndpointAttributesResult>
    <Attributes>
{attrs}
    </Attributes>
  </GetEndpointAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetEndpointAttributesResponse>"#,
                        req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        Err(not_found("Endpoint"))
    }

    pub(super) fn set_endpoint_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let endpoint_arn = required(req, "EndpointArn")?;
        let new_attrs = parse_entries(req, "Attributes");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for app in state.platform_applications.values_mut() {
            if let Some(ep) = app.endpoints.get_mut(&endpoint_arn) {
                for (k, v) in new_attrs {
                    if k == "Enabled" {
                        ep.enabled = v == "true";
                    }
                    ep.attributes.insert(k, v);
                }

                return Ok(xml_resp(
                    &format!(
                        r#"<SetEndpointAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetEndpointAttributesResponse>"#,
                        req.request_id
                    ),
                    &req.request_id,
                ));
            }
        }

        Err(not_found("Endpoint"))
    }

    pub(super) fn list_endpoints_by_platform_application(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_arn = required(req, "PlatformApplicationArn")?;

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let app = state
            .platform_applications
            .get(&app_arn)
            .ok_or_else(|| not_found("PlatformApplication"))?;

        let items: Vec<(String, String)> = app
            .endpoints
            .values()
            .map(|ep| {
                let attrs: String = ep
                    .attributes
                    .iter()
                    .map(|(k, v)| format_attr(k, v))
                    .collect::<Vec<_>>()
                    .join("\n");
                let member = format!(
                    r#"      <member>
        <EndpointArn>{}</EndpointArn>
        <Attributes>
{attrs}
        </Attributes>
      </member>"#,
                    ep.arn
                );
                (ep.arn.clone(), member)
            })
            .collect();
        let (members, next_token) = paginate_sns_members(items, param(req, "NextToken").as_deref());
        let next_token_xml = next_token
            .map(|t| format!("\n    <NextToken>{t}</NextToken>"))
            .unwrap_or_default();

        Ok(xml_resp(
            &format!(
                r#"<ListEndpointsByPlatformApplicationResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListEndpointsByPlatformApplicationResult>
    <Endpoints>
{members}
    </Endpoints>{next_token_xml}
  </ListEndpointsByPlatformApplicationResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListEndpointsByPlatformApplicationResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    // ===== SMS actions =====
}

/// SNS list page size for platform applications / endpoints. AWS returns at
/// most 100 per page and a `NextToken` to fetch the rest.
const SNS_LIST_PAGE_SIZE: usize = 100;

/// Paginate `(sort_key, member_xml)` pairs with SNS's `NextToken` convention.
/// The token is an opaque base64 of the first key on the next page; resuming
/// starts inclusively at that key. Returns the joined member XML for the page
/// plus the next token when more items remain.
fn paginate_sns_members(
    mut items: Vec<(String, String)>,
    next_token: Option<&str>,
) -> (String, Option<String>) {
    use base64::Engine;
    items.sort_by(|a, b| a.0.cmp(&b.0));
    // Resume at the first key >= the token key. `partition_point` (items are
    // sorted by key) makes this robust to the token'd item having been deleted
    // between pages: a plain `position(== key)` would return None and restart
    // at page 1 (a non-terminating delete-drain loop). The token points at the
    // first key of the next page, so the resume is inclusive.
    let start = next_token
        .and_then(|t| base64::engine::general_purpose::STANDARD.decode(t).ok())
        .and_then(|b| String::from_utf8(b).ok())
        .map(|key| items.partition_point(|(k, _)| k.as_str() < key.as_str()))
        .unwrap_or(0);
    let end = (start + SNS_LIST_PAGE_SIZE).min(items.len());
    let next = if end < items.len() {
        Some(base64::engine::general_purpose::STANDARD.encode(&items[end].0))
    } else {
        None
    };
    let members = items[start..end]
        .iter()
        .map(|(_, m)| m.clone())
        .collect::<Vec<_>>()
        .join("\n");
    (members, next)
}

#[cfg(test)]
mod pagination_tests {
    use super::*;

    fn items(n: usize) -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("arn-{i:04}"), format!("<member>{i}</member>")))
            .collect()
    }

    #[test]
    fn paginates_in_pages_of_100_with_next_token() {
        // 150 items -> page 1 of 100 + a NextToken, page 2 of 50 + no token
        // (bug-audit 2026-06-20, 1.14: NextToken was never emitted/honored).
        let (page1, tok1) = paginate_sns_members(items(150), None);
        assert_eq!(page1.matches("<member>").count(), 100);
        let tok1 = tok1.expect("first page truncated");

        let (page2, tok2) = paginate_sns_members(items(150), Some(&tok1));
        assert_eq!(page2.matches("<member>").count(), 50);
        assert!(tok2.is_none());
        // Page 2 resumes exactly where page 1 stopped (item 100).
        assert!(page2.contains("<member>100</member>"));
        assert!(!page2.contains("<member>99</member>"));
    }

    #[test]
    fn single_page_emits_no_token() {
        let (page, tok) = paginate_sns_members(items(10), None);
        assert_eq!(page.matches("<member>").count(), 10);
        assert!(tok.is_none());
    }

    #[test]
    fn resumes_past_a_deleted_marker_item() {
        // bug-audit 2026-07-27 (cycle 5): the token points at the first key of
        // the next page (item 100). If that item is deleted between pages, a
        // `position(== key)` resume returned None and restarted at page 1 (a
        // non-terminating delete-drain loop). The `partition_point` resume must
        // advance to the next surviving key instead.
        let (_page1, tok1) = paginate_sns_members(items(150), None);
        let tok1 = tok1.expect("first page truncated");

        // Drop the item the token points at (arn-0100) plus a few after it.
        let mut remaining = items(150);
        remaining.retain(|(k, _)| !matches!(k.as_str(), "arn-0100" | "arn-0101" | "arn-0102"));

        let (page2, tok2) = paginate_sns_members(remaining, Some(&tok1));
        // Must resume at the first surviving key >= arn-0100 (arn-0103), NOT
        // restart at arn-0000.
        assert!(page2.contains("<member>103</member>"));
        assert!(!page2.contains("<member>0</member>"));
        assert!(!page2.contains("<member>99</member>"));
        assert!(tok2.is_none());
    }
}
