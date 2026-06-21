use chrono::Utc;
use http::StatusCode;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{OidcProvider, SamlProvider, ServerCertificate};

use super::{
    empty_response, generate_id, paginated_tags_response, parse_tag_keys, parse_tags, tags_xml,
    IamService,
};
use fakecloud_core::query::required_param;

fn validate_string_length(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    super::validate_string_length_with_code(field, value, min, max, "InvalidInput")
}

fn validate_optional_string_length(
    field: &str,
    value: Option<&str>,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    super::validate_optional_string_length_with_code(field, value, min, max, "InvalidInput")
}

use fakecloud_aws::xml::xml_escape;

/// Validate the URL, thumbprint list, and client ID list for
/// `CreateOpenIDConnectProvider`. AWS uses two different error styles:
/// thumbprint count and client ID count violations are surfaced
/// immediately, while length violations are batched into a single
/// multi-error ValidationError to match the structure of the real
/// service.
fn validate_oidc_provider_input(
    url: &str,
    thumbprints: &[String],
    client_ids: &[String],
) -> Result<(), AwsServiceError> {
    let mut validation_errors: Vec<String> = Vec::new();

    if url.len() > 255 {
        validation_errors.push(
            "Value at \"url\" failed to satisfy constraint: Member must have length less than or equal to 255".to_string()
        );
    }

    if thumbprints.len() > 5 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInput",
            "Thumbprint list must contain fewer than 5 entries.".to_string(),
        ));
    }
    if thumbprints.iter().any(|tp| tp.len() != 40) {
        validation_errors.push(
            "Value at \"thumbprintList\" failed to satisfy constraint: Member must have length less than or equal to 40; Member must have length greater than or equal to 40".to_string()
        );
    }

    if client_ids.len() > 100 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "LimitExceeded",
            "Cannot exceed quota for ClientIdsPerOpenIdConnectProvider: 100".to_string(),
        ));
    }
    if client_ids
        .iter()
        .any(|cid| cid.len() > 255 || cid.is_empty())
    {
        validation_errors.push(
            "Value at \"clientIDList\" failed to satisfy constraint: Member must have length less than or equal to 255; Member must have length greater than or equal to 1".to_string()
        );
    }

    if !validation_errors.is_empty() {
        let count = validation_errors.len();
        let msg = format!(
            "{count} validation error{} detected: {}",
            if count == 1 { "" } else { "s" },
            validation_errors.join("; ")
        );
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInput",
            msg,
        ));
    }

    if !url.starts_with("https://") && !url.starts_with("http://") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInput",
            "Invalid Open ID Connect Provider URL".to_string(),
        ));
    }

    Ok(())
}

impl IamService {
    pub(super) fn create_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "Name")?;
        validate_string_length("Name", &name, 1, 128)?;
        let saml_metadata_document = required_param(&req.query_params, "SAMLMetadataDocument")?;
        validate_string_length(
            "SAMLMetadataDocument",
            &saml_metadata_document,
            1000,
            10_000_000,
        )?;
        validate_optional_string_length(
            "AddPrivateKey",
            req.query_params.get("AddPrivateKey").map(|s| s.as_str()),
            1,
            16384,
        )?;
        if let Some(mode) = req.query_params.get("AssertionEncryptionMode") {
            if !matches!(mode.as_str(), "Required" | "Allowed") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    format!("Value '{mode}' at 'assertionEncryptionMode' failed to satisfy constraint: Member must satisfy enum value set: [Required, Allowed]"),
                ));
            }
        }
        let tags = parse_tags(&req.query_params);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let arn =
            Arn::global("iam", &state.account_id, &format!("saml-provider/{name}")).to_string();

        let provider = SamlProvider {
            arn: arn.clone(),
            name,
            saml_metadata_document,
            created_at: Utc::now(),
            valid_until: Utc::now() + chrono::Duration::days(365),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateSAMLProviderResult>
    <SAMLProviderArn>{}</SAMLProviderArn>
  </CreateSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateSAMLProviderResponse>"#,
            arn, req.request_id
        );

        state.saml_providers.insert(arn, provider);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "SAMLProviderArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let provider = state.saml_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            )
        })?;

        let tags_members = tags_xml(&provider.tags);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetSAMLProviderResult>
    <SAMLMetadataDocument>{}</SAMLMetadataDocument>
    <CreateDate>{}</CreateDate>
    <ValidUntil>{}</ValidUntil>
    <Tags>
{tags_members}
    </Tags>
  </GetSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSAMLProviderResponse>"#,
            xml_escape(&provider.saml_metadata_document),
            provider.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            provider.valid_until.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "SAMLProviderArn")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.saml_providers.remove(&arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            ));
        }

        let xml = empty_response("DeleteSAMLProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_saml_providers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let members: String = state
            .saml_providers
            .values()
            .map(|p| {
                format!(
                    "      <member>\n        <Arn>{}</Arn>\n        <ValidUntil>{}</ValidUntil>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    p.arn,
                    p.valid_until.format("%Y-%m-%dT%H:%M:%SZ"),
                    p.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListSAMLProvidersResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListSAMLProvidersResult>
    <SAMLProviderList>
{members}
    </SAMLProviderList>
  </ListSAMLProvidersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSAMLProvidersResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Declared: ConcurrentModification, InvalidInputException, LimitExceeded,
        // NoSuchEntity, ServiceFailure. SAMLMetadataDocument is optional in
        // the Smithy model (AddPrivateKey / RemovePrivateKey are alternatives).
        let arn =
            super::required_param_with_code(&req.query_params, "SAMLProviderArn", "InvalidInput")?;
        let saml_metadata_document = req
            .query_params
            .get("SAMLMetadataDocument")
            .cloned()
            .filter(|v| !v.is_empty());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.saml_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            )
        })?;

        if let Some(doc) = saml_metadata_document {
            provider.saml_metadata_document = doc;
        }

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UpdateSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UpdateSAMLProviderResult>
    <SAMLProviderArn>{}</SAMLProviderArn>
  </UpdateSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UpdateSAMLProviderResponse>"#,
            arn, req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn create_oidc_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Declared: ConcurrentModification, EntityAlreadyExists,
        // InvalidInputException, LimitExceeded, OpenIdIdpCommunicationError,
        // ServiceFailure -> InvalidInput for missing/invalid Url.
        let url = super::required_param_with_code(&req.query_params, "Url", "InvalidInput")?;
        let tags = parse_tags(&req.query_params);

        let mut client_ids = Vec::new();
        let mut i = 1;
        while let Some(id) = req.query_params.get(&format!("ClientIDList.member.{i}")) {
            client_ids.push(id.clone());
            i += 1;
        }

        let mut thumbprints = Vec::new();
        i = 1;
        while let Some(tp) = req.query_params.get(&format!("ThumbprintList.member.{i}")) {
            thumbprints.push(tp.clone());
            i += 1;
        }

        validate_oidc_provider_input(&url, &thumbprints, &client_ids)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Store URL without scheme for responses (AWS behavior)
        let url_without_scheme = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .unwrap_or(&url)
            .to_string();

        // ARN uses URL path without query string
        let url_for_arn = url_without_scheme
            .split('?')
            .next()
            .unwrap_or(&url_without_scheme);
        let arn = format!(
            "arn:aws:iam::{}:oidc-provider/{}",
            state.account_id, url_for_arn
        );

        if state.oidc_providers.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                "Unknown".to_string(),
            ));
        }

        let provider = OidcProvider {
            arn: arn.clone(),
            url: url_without_scheme,
            client_id_list: client_ids,
            thumbprint_list: thumbprints,
            created_at: Utc::now(),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateOpenIDConnectProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateOpenIDConnectProviderResult>
    <OpenIDConnectProviderArn>{}</OpenIDConnectProviderArn>
  </CreateOpenIDConnectProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateOpenIDConnectProviderResponse>"#,
            arn, req.request_id
        );

        state.oidc_providers.insert(arn, provider);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_oidc_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let provider = state.oidc_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        let client_ids: String = provider
            .client_id_list
            .iter()
            .map(|id| format!("      <member>{id}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let thumbprints: String = provider
            .thumbprint_list
            .iter()
            .map(|tp| format!("      <member>{tp}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let tags_members = tags_xml(&provider.tags);

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetOpenIDConnectProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetOpenIDConnectProviderResult>
    <Url>{}</Url>
    <CreateDate>{}</CreateDate>
    <ClientIDList>
{client_ids}
    </ClientIDList>
    <ThumbprintList>
{thumbprints}
    </ThumbprintList>
    <Tags>
{tags_members}
    </Tags>
  </GetOpenIDConnectProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetOpenIDConnectProviderResponse>"#,
            xml_escape(&provider.url),
            provider.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_oidc_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        validate_string_length("OpenIDConnectProviderArn", &arn, 20, 2048)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // AWS silently succeeds when deleting a non-existing OIDC provider
        state.oidc_providers.remove(&arn);

        let xml = empty_response("DeleteOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_oidc_providers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let members: String = state
            .oidc_providers
            .values()
            .map(|p| {
                format!(
                    "      <member>\n        <Arn>{}</Arn>\n      </member>",
                    p.arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListOpenIDConnectProvidersResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListOpenIDConnectProvidersResult>
    <OpenIDConnectProviderList>
{members}
    </OpenIDConnectProviderList>
  </ListOpenIDConnectProvidersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListOpenIDConnectProvidersResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_oidc_thumbprint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;

        let mut thumbprints = Vec::new();
        let mut i = 1;
        while let Some(tp) = req.query_params.get(&format!("ThumbprintList.member.{i}")) {
            thumbprints.push(tp.clone());
            i += 1;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        provider.thumbprint_list = thumbprints;

        let xml = empty_response("UpdateOpenIDConnectProviderThumbprint", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn add_client_id_to_oidc(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let client_id = required_param(&req.query_params, "ClientID")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        if !provider.client_id_list.contains(&client_id) {
            provider.client_id_list.push(client_id);
        }

        let xml = empty_response("AddClientIDToOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn remove_client_id_from_oidc(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let client_id = required_param(&req.query_params, "ClientID")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        provider.client_id_list.retain(|id| id != &client_id);

        let xml = empty_response("RemoveClientIDFromOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn tag_oidc_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let new_tags = parse_tags(&req.query_params);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        for new_tag in new_tags {
            if let Some(existing) = provider.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                provider.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn untag_oidc_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        provider.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_oidc_provider_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let provider = state.oidc_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect Provider not found for arn {arn}"),
            )
        })?;

        let xml = paginated_tags_response("ListOpenIDConnectProviderTags", &provider.tags, req)?;
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn upload_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        validate_string_length("ServerCertificateName", &name, 1, 128)?;
        let certificate_body = required_param(&req.query_params, "CertificateBody")?;
        let _private_key = required_param(&req.query_params, "PrivateKey")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let certificate_chain = req.query_params.get("CertificateChain").cloned();
        let tags = parse_tags(&req.query_params);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.server_certificates.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Server certificate {name} already exists."),
            ));
        }

        let cert = ServerCertificate {
            server_certificate_id: format!("ASCA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:server-certificate{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                name
            ),
            server_certificate_name: name.clone(),
            path,
            // AWS strips trailing whitespace from the PEM material on upload,
            // so GetServerCertificate echoes the body without the trailing
            // newline the caller may have sent. Match that, otherwise the
            // Terraform provider sees a perpetual `certificate_body` diff.
            certificate_body: certificate_body.trim_end().to_string(),
            certificate_chain: certificate_chain.map(|c| c.trim_end().to_string()),
            upload_date: Utc::now(),
            expiration: Utc::now() + chrono::Duration::days(365),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UploadServerCertificateResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UploadServerCertificateResult>
    <ServerCertificateMetadata>
      <ServerCertificateName>{}</ServerCertificateName>
      <ServerCertificateId>{}</ServerCertificateId>
      <Arn>{}</Arn>
      <Path>{}</Path>
      <UploadDate>{}</UploadDate>
      <Expiration>{}</Expiration>
    </ServerCertificateMetadata>
  </UploadServerCertificateResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UploadServerCertificateResponse>"#,
            cert.server_certificate_name,
            cert.server_certificate_id,
            cert.arn,
            cert.path,
            cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );

        state.server_certificates.insert(name, cert);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let cert = state.server_certificates.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Server Certificate with name {name} cannot be found."),
            )
        })?;

        let chain_xml = cert
            .certificate_chain
            .as_ref()
            .map(|c| {
                format!(
                    "      <CertificateChain>{}</CertificateChain>",
                    xml_escape(c)
                )
            })
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetServerCertificateResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetServerCertificateResult>
    <ServerCertificate>
      <ServerCertificateMetadata>
        <ServerCertificateName>{}</ServerCertificateName>
        <ServerCertificateId>{}</ServerCertificateId>
        <Arn>{}</Arn>
        <Path>{}</Path>
        <UploadDate>{}</UploadDate>
        <Expiration>{}</Expiration>
      </ServerCertificateMetadata>
      <CertificateBody>{}</CertificateBody>
{chain_xml}
    </ServerCertificate>
  </GetServerCertificateResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetServerCertificateResponse>"#,
            cert.server_certificate_name,
            cert.server_certificate_id,
            cert.arn,
            cert.path,
            cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
            xml_escape(&cert.certificate_body),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.server_certificates.remove(&name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Server Certificate with name {name} cannot be found."),
            ));
        }

        let xml = empty_response("DeleteServerCertificate", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_server_certificates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = super::validate_list_pagination(req)?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let members: String = state
            .server_certificates
            .values()
            .map(|cert| {
                format!(
                    "      <member>\n        <ServerCertificateName>{}</ServerCertificateName>\n        <ServerCertificateId>{}</ServerCertificateId>\n        <Arn>{}</Arn>\n        <Path>{}</Path>\n        <UploadDate>{}</UploadDate>\n        <Expiration>{}</Expiration>\n      </member>",
                    cert.server_certificate_name,
                    cert.server_certificate_id,
                    cert.arn,
                    cert.path,
                    cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
                    cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListServerCertificatesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListServerCertificatesResult>
    <IsTruncated>false</IsTruncated>
    <ServerCertificateMetadataList>
{members}
    </ServerCertificateMetadataList>
  </ListServerCertificatesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListServerCertificatesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
