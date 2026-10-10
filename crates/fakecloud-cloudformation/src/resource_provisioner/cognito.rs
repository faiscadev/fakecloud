//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `cognito`.

use super::*;

impl ResourceProvisioner {
    // --- Cognito ---

    pub(super) fn create_cognito_user_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let pool_name = props
            .get("PoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();

        let user_pool_tags = parse_cognito_tags(props.get("UserPoolTags"));
        // A `_custom_id_` tag picks the pool id, as it does for CreateUserPool.
        let custom_pool_id = user_pool_tags
            .get(fakecloud_cognito::CUSTOM_ID_TAG)
            .map(|id| fakecloud_cognito::custom_user_pool_id(id, &self.region))
            .transpose()?;
        let pool_id = custom_pool_id.clone().unwrap_or_else(|| {
            format!(
                "{}_{}",
                self.region,
                Uuid::new_v4()
                    .simple()
                    .to_string()
                    .chars()
                    .take(9)
                    .collect::<String>()
            )
        });
        let arn = fakecloud_cognito::user_pool_arn(&self.region, &self.account_id, &pool_id);
        let now = Utc::now();

        let password_policy = parse_cognito_password_policy(props.get("Policies"));
        let auto_verified = parse_cognito_string_array(props.get("AutoVerifiedAttributes"));
        let username_attributes = props
            .get("UsernameAttributes")
            .and_then(|v| v.as_array())
            .map(|_| parse_cognito_string_array(props.get("UsernameAttributes")));
        let alias_attributes = props
            .get("AliasAttributes")
            .and_then(|v| v.as_array())
            .map(|_| parse_cognito_string_array(props.get("AliasAttributes")));
        let mut schema_attributes = default_schema_attributes();
        if let Some(arr) = props.get("Schema").and_then(|v| v.as_array()) {
            for attr in arr {
                if let Some(parsed) = parse_cognito_schema_attribute(attr) {
                    if !schema_attributes.iter().any(|a| a.name == parsed.name) {
                        schema_attributes.push(parsed);
                    }
                }
            }
        }
        let mfa_configuration = props
            .get("MfaConfiguration")
            .and_then(|v| v.as_str())
            .unwrap_or("OFF")
            .to_string();
        let user_pool_tier = props
            .get("UserPoolTier")
            .and_then(|v| v.as_str())
            .unwrap_or("ESSENTIALS")
            .to_string();
        let deletion_protection = props
            .get("DeletionProtection")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let email_configuration =
            parse_cognito_email_configuration(props.get("EmailConfiguration"));
        let sms_configuration = parse_cognito_sms_configuration(props.get("SmsConfiguration"));
        let admin_create_user_config =
            parse_cognito_admin_create_user_config(props.get("AdminCreateUserConfig"));
        let account_recovery_setting =
            parse_cognito_account_recovery(props.get("AccountRecoverySetting"));

        // Generate the RSA-2048 keypair eagerly. The kid is derived
        // from a SHA-256 of the public SPKI DER so it stays stable
        // across snapshots and matches the JWKS document.
        let signing = fakecloud_cognito::jwt::generate_pool_signing_key();
        let signing_key_pem = signing.private_key_pem;
        let signing_kid = signing.kid;
        let pool = UserPool {
            id: pool_id.clone(),
            name: pool_name,
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            creation_date: now,
            last_modified_date: now,
            policies: PoolPolicies {
                password_policy,
                sign_in_policy: SignInPolicy {
                    allowed_first_auth_factors: vec!["PASSWORD".to_string()],
                },
            },
            auto_verified_attributes: auto_verified,
            username_attributes,
            alias_attributes,
            schema_attributes,
            // Stored verbatim, as CreateUserPool stores it, so the pool's
            // triggers fire.
            lambda_config: props.get("LambdaConfig").filter(|v| v.is_object()).cloned(),
            mfa_configuration,
            email_configuration,
            sms_configuration,
            admin_create_user_config,
            user_pool_tags,
            account_recovery_setting,
            deletion_protection,
            estimated_number_of_users: 0,
            software_token_mfa_configuration: None,
            sms_mfa_configuration: None,
            user_pool_tier,
            verification_message_template: None,
            signing_key_pem: Some(signing_key_pem),
            signing_kid: Some(signing_kid),
            email_verification_message: None,
            email_verification_subject: None,
            sms_verification_message: None,
            sms_authentication_message: None,
            device_configuration: None,
            user_attribute_update_settings: None,
            user_pool_add_ons: None,
            username_configuration: None,
            acr_configuration: Default::default(),
        };

        let mut accounts = self.cognito_state.write();
        if let Some(id) = &custom_pool_id {
            fakecloud_cognito::ensure_user_pool_id_unused(&accounts, id)?;
        }
        let state = accounts.get_or_create(&self.account_id);
        state.user_pools.insert(pool_id.clone(), pool);

        let provider_name = format!("cognito-idp.{}.amazonaws.com/{}", self.region, pool_id);
        let provider_url = format!("https://{provider_name}");

        Ok(ProvisionResult::new(pool_id.clone())
            .with("Arn", arn)
            .with("ProviderName", provider_name)
            .with("ProviderURL", provider_url)
            .with("UserPoolId", pool_id))
    }

    /// Apply a CFN property update to an existing Cognito user pool in place.
    /// Mirrors the property extraction in `create_cognito_user_pool` for the
    /// fields that update without replacement (policies, MFA, tier, deletion
    /// protection, tags, email/SMS config, admin-create config, recovery
    /// settings, auto-verified attributes) so a stack update reaches the pool
    /// and `DescribeUserPool` reflects the new config instead of the stale one.
    /// The pool id/ARN, creation date and signing keys are preserved.
    pub(super) fn update_cognito_user_pool(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let pool_id = &existing.physical_id;

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let pool = state
            .user_pools
            .get_mut(pool_id)
            .ok_or_else(|| format!("User pool {pool_id} not yet provisioned"))?;

        // The id a `_custom_id_` tag picks is fixed at creation; a stack
        // update cannot move the pool to another id in place.
        if let Some(id) = props
            .get("UserPoolTags")
            .and_then(|tags| tags.get(fakecloud_cognito::CUSTOM_ID_TAG))
            .and_then(|v| v.as_str())
        {
            if id != pool_id {
                return Err(format!(
                    "Cannot change user pool {pool_id} to id {id} through the {} tag: a user pool id is fixed at creation",
                    fakecloud_cognito::CUSTOM_ID_TAG
                ));
            }
        }

        if let Some(pool_name) = props.get("PoolName").and_then(|v| v.as_str()) {
            pool.name = pool_name.to_string();
        }
        pool.policies.password_policy = parse_cognito_password_policy(props.get("Policies"));
        pool.auto_verified_attributes =
            parse_cognito_string_array(props.get("AutoVerifiedAttributes"));
        if let Some(mfa) = props.get("MfaConfiguration").and_then(|v| v.as_str()) {
            pool.mfa_configuration = mfa.to_string();
        }
        if let Some(tier) = props.get("UserPoolTier").and_then(|v| v.as_str()) {
            pool.user_pool_tier = tier.to_string();
        }
        if let Some(dp) = props.get("DeletionProtection").and_then(|v| v.as_str()) {
            pool.deletion_protection = Some(dp.to_string());
        }
        if props.get("UserPoolTags").is_some() {
            pool.user_pool_tags = parse_cognito_tags(props.get("UserPoolTags"));
        }
        if props.get("EmailConfiguration").is_some() {
            pool.email_configuration =
                parse_cognito_email_configuration(props.get("EmailConfiguration"));
        }
        if props.get("SmsConfiguration").is_some() {
            pool.sms_configuration = parse_cognito_sms_configuration(props.get("SmsConfiguration"));
        }
        if props.get("AdminCreateUserConfig").is_some() {
            pool.admin_create_user_config =
                parse_cognito_admin_create_user_config(props.get("AdminCreateUserConfig"));
        }
        if props.get("AccountRecoverySetting").is_some() {
            pool.account_recovery_setting =
                parse_cognito_account_recovery(props.get("AccountRecoverySetting"));
        }
        // A template that drops LambdaConfig removes the triggers, as an
        // UpdateUserPool without it does.
        pool.lambda_config = props.get("LambdaConfig").filter(|v| v.is_object()).cloned();
        pool.last_modified_date = Utc::now();

        let arn = pool.arn.clone();
        let provider_name = format!("cognito-idp.{}.amazonaws.com/{}", self.region, pool_id);
        let provider_url = format!("https://{provider_name}");
        Ok(ProvisionResult::new(pool_id.clone())
            .with("Arn", arn)
            .with("ProviderName", provider_name)
            .with("ProviderURL", provider_url)
            .with("UserPoolId", pool_id.clone()))
    }

    pub(super) fn delete_cognito_user_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        fakecloud_cognito::purge_user_pool(state, physical_id);
        Ok(())
    }

    pub(super) fn create_cognito_user_pool_client(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let pool_id = props
            .get("UserPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserPoolId is required".to_string())?
            .to_string();
        let generated_name = self.physical_name(resource);
        let client_name = props
            .get("ClientName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        // A `_custom_id_:<id>` ClientName picks the client id, as it does for
        // CreateUserPoolClient.
        let custom_client_id = fakecloud_cognito::custom_client_id(&client_name)?;

        let mut accounts = self.cognito_state.write();
        if !accounts
            .get_or_create(&self.account_id)
            .user_pools
            .contains_key(&pool_id)
        {
            // Force CFN to retry once UserPool resource provisions.
            return Err(format!(
                "User pool {pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }
        if let Some(id) = &custom_client_id {
            fakecloud_cognito::ensure_user_pool_client_id_unused(&accounts, id)?;
        }
        let state = accounts.get_or_create(&self.account_id);

        let client_id: String = custom_client_id.unwrap_or_else(|| {
            format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
                .chars()
                .filter(|c| c.is_ascii_alphanumeric())
                .take(26)
                .collect::<String>()
                .to_lowercase()
        });
        let generate_secret = props
            .get("GenerateSecret")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let client_secret = if generate_secret {
            use base64::Engine;
            let mut bytes = Vec::with_capacity(48);
            for _ in 0..3 {
                bytes.extend_from_slice(Uuid::new_v4().as_bytes());
            }
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&bytes)
                    .chars()
                    .take(51)
                    .collect(),
            )
        } else {
            None
        };

        let now = Utc::now();
        let validity = fakecloud_cognito::resolve_token_validity(
            props.get("AccessTokenValidity").and_then(|v| v.as_i64()),
            props.get("IdTokenValidity").and_then(|v| v.as_i64()),
            props.get("RefreshTokenValidity").and_then(|v| v.as_i64()),
            parse_cfn_token_validity_units(props.get("TokenValidityUnits")),
        )
        .map_err(str::to_string)?;
        let client = UserPoolClient {
            client_id: client_id.clone(),
            client_name,
            user_pool_id: pool_id.clone(),
            client_secret: client_secret.clone(),
            explicit_auth_flows: parse_cognito_string_array(props.get("ExplicitAuthFlows")),
            token_validity_units: validity.units,
            access_token_validity: validity.access,
            id_token_validity: validity.id,
            refresh_token_validity: Some(validity.refresh),
            callback_urls: parse_cognito_string_array(props.get("CallbackURLs")),
            logout_urls: parse_cognito_string_array(props.get("LogoutURLs")),
            supported_identity_providers: parse_cognito_string_array(
                props.get("SupportedIdentityProviders"),
            ),
            allowed_o_auth_flows: parse_cognito_string_array(props.get("AllowedOAuthFlows")),
            allowed_o_auth_scopes: parse_cognito_string_array(props.get("AllowedOAuthScopes")),
            allowed_o_auth_flows_user_pool_client: props
                .get("AllowedOAuthFlowsUserPoolClient")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            prevent_user_existence_errors: props
                .get("PreventUserExistenceErrors")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            read_attributes: parse_cognito_string_array(props.get("ReadAttributes")),
            write_attributes: parse_cognito_string_array(props.get("WriteAttributes")),
            creation_date: now,
            last_modified_date: now,
            enable_token_revocation: props
                .get("EnableTokenRevocation")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            auth_session_validity: Some(
                props
                    .get("AuthSessionValidity")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(3),
            ),
            enable_propagate_additional_user_context_data: props
                .get("EnablePropagateAdditionalUserContextData")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            client_secrets: Vec::new(),
            refresh_token_rotation: None,
            analytics_configuration: props
                .get("AnalyticsConfiguration")
                .filter(|v| !v.is_null())
                .cloned(),
        };

        state.user_pool_clients.insert(client_id.clone(), client);

        let mut result = ProvisionResult::new(client_id.clone())
            .with("ClientId", client_id.clone())
            .with("Name", client_id);
        if let Some(secret) = client_secret {
            result = result.with("ClientSecret", secret);
        }
        Ok(result)
    }

    /// Apply a CFN property update to an existing Cognito user pool client in
    /// place. Mirrors the property extraction in `create_cognito_user_pool_client`
    /// for the fields that update without replacement (auth flows, token
    /// validity, callback/logout URLs, OAuth config, read/write attributes,
    /// etc.) so a stack update reaches the client and `DescribeUserPoolClient`
    /// reflects the new config. The client id/secret, pool id and creation date
    /// are preserved (`GenerateSecret` is create-only).
    pub(super) fn update_cognito_user_pool_client(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let client_id = &existing.physical_id;

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let client = state
            .user_pool_clients
            .get_mut(client_id)
            .ok_or_else(|| format!("User pool client {client_id} not yet provisioned"))?;

        // Likewise a `_custom_id_:<id>` ClientName cannot move the client to
        // another id in place.
        if let Some(name) = props.get("ClientName").and_then(|v| v.as_str()) {
            if let Some(id) = fakecloud_cognito::custom_client_id(name)? {
                if id != *client_id {
                    return Err(format!(
                        "Cannot change user pool client {client_id} to custom id {id} in place: a client id is fixed at creation"
                    ));
                }
            }
        }

        // CloudFormation updates replace the whole resource model: a removed
        // TokenValidityUnits reverts to Cognito's default units (hours/days),
        // and removed validity values revert to their defaults (unset access/id,
        // 30-day refresh), so stale values are never reinterpreted in new units.
        let validity = fakecloud_cognito::resolve_token_validity(
            props.get("AccessTokenValidity").and_then(|v| v.as_i64()),
            props.get("IdTokenValidity").and_then(|v| v.as_i64()),
            props.get("RefreshTokenValidity").and_then(|v| v.as_i64()),
            parse_cfn_token_validity_units(props.get("TokenValidityUnits")),
        )
        .map_err(str::to_string)?;
        client.token_validity_units = validity.units;
        client.access_token_validity = validity.access;
        client.id_token_validity = validity.id;
        client.refresh_token_validity = Some(validity.refresh);
        if let Some(name) = props.get("ClientName").and_then(|v| v.as_str()) {
            client.client_name = name.to_string();
        }
        client.explicit_auth_flows = parse_cognito_string_array(props.get("ExplicitAuthFlows"));
        client.callback_urls = parse_cognito_string_array(props.get("CallbackURLs"));
        client.logout_urls = parse_cognito_string_array(props.get("LogoutURLs"));
        client.supported_identity_providers =
            parse_cognito_string_array(props.get("SupportedIdentityProviders"));
        client.allowed_o_auth_flows = parse_cognito_string_array(props.get("AllowedOAuthFlows"));
        client.allowed_o_auth_scopes = parse_cognito_string_array(props.get("AllowedOAuthScopes"));
        if let Some(b) = props
            .get("AllowedOAuthFlowsUserPoolClient")
            .and_then(|v| v.as_bool())
        {
            client.allowed_o_auth_flows_user_pool_client = b;
        }
        if let Some(s) = props
            .get("PreventUserExistenceErrors")
            .and_then(|v| v.as_str())
        {
            client.prevent_user_existence_errors = Some(s.to_string());
        }
        client.read_attributes = parse_cognito_string_array(props.get("ReadAttributes"));
        client.write_attributes = parse_cognito_string_array(props.get("WriteAttributes"));
        if let Some(b) = props.get("EnableTokenRevocation").and_then(|v| v.as_bool()) {
            client.enable_token_revocation = b;
        }
        if let Some(v) = props.get("AuthSessionValidity").and_then(|v| v.as_i64()) {
            client.auth_session_validity = Some(v);
        }
        if let Some(b) = props
            .get("EnablePropagateAdditionalUserContextData")
            .and_then(|v| v.as_bool())
        {
            client.enable_propagate_additional_user_context_data = b;
        }
        if let Some(v) = props.get("AnalyticsConfiguration") {
            client.analytics_configuration = if v.is_null() { None } else { Some(v.clone()) };
        }
        client.last_modified_date = Utc::now();

        Ok(ProvisionResult::new(client_id.clone())
            .with("ClientId", client_id.clone())
            .with("Name", client_id.clone()))
    }

    pub(super) fn delete_cognito_user_pool_client(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        fakecloud_cognito::purge_user_pool_client(state, physical_id);
        Ok(())
    }

    pub(super) fn create_cognito_user_pool_domain(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let domain = props
            .get("Domain")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Domain is required".to_string())?
            .to_string();
        let pool_id = props
            .get("UserPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserPoolId is required".to_string())?
            .to_string();
        let custom_domain_config = props
            .get("CustomDomainConfig")
            .and_then(|v| v.as_object())
            .and_then(|m| {
                m.get("CertificateArn")
                    .and_then(|v| v.as_str())
                    .map(|s| CustomDomainConfig {
                        certificate_arn: s.to_string(),
                    })
            });

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.user_pools.contains_key(&pool_id) {
            return Err(format!(
                "User pool {pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }
        if state.domains.contains_key(&domain) {
            return Err(format!("Domain {domain} already exists"));
        }
        state.domains.insert(
            domain.clone(),
            UserPoolDomain {
                user_pool_id: pool_id,
                domain: domain.clone(),
                status: "ACTIVE".to_string(),
                custom_domain_config: custom_domain_config.clone(),
                creation_date: Utc::now(),
            },
        );

        let cloudfront_distribution = if custom_domain_config.is_some() {
            format!("{domain}.cloudfront.net")
        } else {
            format!("{domain}.auth.{}.amazoncognito.com", self.region)
        };

        Ok(ProvisionResult::new(domain.clone())
            .with("Domain", domain)
            .with("CloudFrontDistribution", cloudfront_distribution))
    }

    pub(super) fn delete_cognito_user_pool_domain(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.domains.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_cognito_identity_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let identity_pool_name = props
            .get("IdentityPoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let allow_unauth = props
            .get("AllowUnauthenticatedIdentities")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let allow_classic = props
            .get("AllowClassicFlow")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let developer_provider_name = props
            .get("DeveloperProviderName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let cognito_identity_providers = props
            .get("CognitoIdentityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let obj = p.as_object()?;
                        let provider_name = obj
                            .get("ProviderName")
                            .and_then(|v| v.as_str())?
                            .to_string();
                        let client_id = obj.get("ClientId").and_then(|v| v.as_str())?.to_string();
                        let server_side_token_check = obj
                            .get("ServerSideTokenCheck")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        Some(CognitoIdentityProvider {
                            provider_name,
                            client_id,
                            server_side_token_check,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let open_id_connect_provider_arns =
            parse_cognito_string_array(props.get("OpenIdConnectProviderARNs"));
        let saml_provider_arns = parse_cognito_string_array(props.get("SamlProviderARNs"));
        let supported_login_providers = props
            .get("SupportedLoginProviders")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let identity_pool_tags = parse_cognito_tags(props.get("IdentityPoolTags"));
        let cognito_streams = props.get("CognitoStreams").cloned();
        let push_sync = props.get("PushSync").cloned();

        // Real Cognito identity pool ids look like `<region>:<uuid>`. Match
        // that shape so SDKs that parse it don't choke.
        let identity_pool_id = format!("{}:{}", self.region, Uuid::new_v4());

        let pool = IdentityPool {
            identity_pool_id: identity_pool_id.clone(),
            identity_pool_name,
            allow_unauthenticated_identities: allow_unauth,
            allow_classic_flow: allow_classic,
            developer_provider_name,
            cognito_identity_providers,
            open_id_connect_provider_arns,
            saml_provider_arns,
            supported_login_providers,
            cognito_streams,
            push_sync,
            identity_pool_tags,
            creation_date: Utc::now(),
        };

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pools.insert(identity_pool_id.clone(), pool);

        Ok(ProvisionResult::new(identity_pool_id.clone()).with("Name", identity_pool_id))
    }

    /// In-place `UpdateStack` for an `AWS::Cognito::IdentityPool`. Mutates the
    /// stored `IdentityPool` record instead of the reprovision fallback's
    /// delete+recreate. `delete_cognito_identity_pool` mints a brand-new pool id
    /// (`<region>:<uuid>`) on recreate AND cascade-drops every
    /// `identity_pool_role_attachment` tied to the pool, so a benign name/tag
    /// change would churn the physical id and silently wipe the separately-
    /// managed role attachment. This applies the mutable properties in place and
    /// preserves the pool id, `creation_date`, and the untouched role
    /// attachments.
    pub(super) fn update_cognito_identity_pool(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let pool_id = &existing.physical_id;

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let pool = state
            .identity_pools
            .get_mut(pool_id)
            .ok_or_else(|| format!("Identity pool {pool_id} not yet provisioned"))?;

        if let Some(name) = props.get("IdentityPoolName").and_then(|v| v.as_str()) {
            pool.identity_pool_name = name.to_string();
        }
        if let Some(b) = props
            .get("AllowUnauthenticatedIdentities")
            .and_then(|v| v.as_bool())
        {
            pool.allow_unauthenticated_identities = b;
        }
        if let Some(b) = props.get("AllowClassicFlow").and_then(|v| v.as_bool()) {
            pool.allow_classic_flow = b;
        }
        if let Some(dp) = props.get("DeveloperProviderName").and_then(|v| v.as_str()) {
            pool.developer_provider_name = Some(dp.to_string());
        }
        if let Some(providers) = props
            .get("CognitoIdentityProviders")
            .and_then(|v| v.as_array())
        {
            pool.cognito_identity_providers = providers
                .iter()
                .filter_map(|p| {
                    let obj = p.as_object()?;
                    Some(CognitoIdentityProvider {
                        provider_name: obj
                            .get("ProviderName")
                            .and_then(|v| v.as_str())?
                            .to_string(),
                        client_id: obj.get("ClientId").and_then(|v| v.as_str())?.to_string(),
                        server_side_token_check: obj
                            .get("ServerSideTokenCheck")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                    })
                })
                .collect();
        }
        if props.get("OpenIdConnectProviderARNs").is_some() {
            pool.open_id_connect_provider_arns =
                parse_cognito_string_array(props.get("OpenIdConnectProviderARNs"));
        }
        if props.get("SamlProviderARNs").is_some() {
            pool.saml_provider_arns = parse_cognito_string_array(props.get("SamlProviderARNs"));
        }
        if let Some(m) = props
            .get("SupportedLoginProviders")
            .and_then(|v| v.as_object())
        {
            pool.supported_login_providers = m
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        if let Some(v) = props.get("CognitoStreams") {
            pool.cognito_streams = Some(v.clone());
        }
        if let Some(v) = props.get("PushSync") {
            pool.push_sync = Some(v.clone());
        }
        if props.get("IdentityPoolTags").is_some() {
            pool.identity_pool_tags = parse_cognito_tags(props.get("IdentityPoolTags"));
        }

        Ok(ProvisionResult::new(pool_id.clone()).with("Name", pool_id.clone()))
    }

    pub(super) fn delete_cognito_identity_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pools.remove(physical_id);
        // Cascade: drop role attachments tied to this pool.
        state
            .identity_pool_role_attachments
            .retain(|_, a| a.identity_pool_id != physical_id);
        Ok(())
    }

    pub(super) fn create_cognito_identity_pool_role_attachment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identity_pool_id = props
            .get("IdentityPoolId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "IdentityPoolId is required".to_string())?
            .to_string();

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.identity_pools.contains_key(&identity_pool_id) {
            return Err(format!(
                "Identity pool {identity_pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }

        let roles = props
            .get("Roles")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let role_mappings = props
            .get("RoleMappings")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let attachment_id = Uuid::new_v4().simple().to_string();
        let physical_id = format!("{identity_pool_id}:{attachment_id}");
        let attachment = IdentityPoolRoleAttachment {
            identity_pool_id: identity_pool_id.clone(),
            attachment_id,
            roles,
            role_mappings,
        };
        state
            .identity_pool_role_attachments
            .insert(physical_id.clone(), attachment);

        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_cognito_identity_pool_role_attachment(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identity_pool_role_attachments.remove(physical_id);
        Ok(())
    }
}

/// `TokenValidityUnits` (`AccessToken` / `IdToken` / `RefreshToken`) from a
/// template, so the validity integers are interpreted in the units the
/// template names rather than Cognito's defaults.
fn parse_cfn_token_validity_units(
    v: Option<&serde_json::Value>,
) -> Option<fakecloud_cognito::TokenValidityUnits> {
    let v = v.filter(|v| v.is_object())?;
    let field = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    Some(fakecloud_cognito::TokenValidityUnits {
        access_token: field("AccessToken"),
        id_token: field("IdToken"),
        refresh_token: field("RefreshToken"),
    })
}
