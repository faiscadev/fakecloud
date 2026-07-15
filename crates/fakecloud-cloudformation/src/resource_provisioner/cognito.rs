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
        let pool_name = props
            .get("PoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let pool_id = format!(
            "{}_{}",
            self.region,
            Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(9)
                .collect::<String>()
        );
        let arn = format!(
            "arn:aws:cognito-idp:{}:{}:userpool/{}",
            self.region, self.account_id, pool_id
        );
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
        let user_pool_tags = parse_cognito_tags(props.get("UserPoolTags"));
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
            lambda_config: None,
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
        };

        let mut accounts = self.cognito_state.write();
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

    pub(super) fn delete_cognito_user_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_pools.remove(physical_id);
        // Cascade: drop clients tied to this pool, plus per-pool side maps.
        state
            .user_pool_clients
            .retain(|_, c| c.user_pool_id != physical_id);
        state.users.remove(physical_id);
        state.groups.remove(physical_id);
        state.user_groups.remove(physical_id);
        state.identity_providers.remove(physical_id);
        state.resource_servers.remove(physical_id);
        state.import_jobs.remove(physical_id);
        state.domains.retain(|_, d| d.user_pool_id != physical_id);
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
        let client_name = props
            .get("ClientName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();

        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.user_pools.contains_key(&pool_id) {
            // Force CFN to retry once UserPool resource provisions.
            return Err(format!(
                "User pool {pool_id} does not exist yet — retry once it has been provisioned"
            ));
        }

        let client_id: String = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(26)
            .collect::<String>()
            .to_lowercase();
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
        let client = UserPoolClient {
            client_id: client_id.clone(),
            client_name,
            user_pool_id: pool_id.clone(),
            client_secret: client_secret.clone(),
            explicit_auth_flows: parse_cognito_string_array(props.get("ExplicitAuthFlows")),
            token_validity_units: None,
            access_token_validity: props.get("AccessTokenValidity").and_then(|v| v.as_i64()),
            id_token_validity: props.get("IdTokenValidity").and_then(|v| v.as_i64()),
            refresh_token_validity: Some(
                props
                    .get("RefreshTokenValidity")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(30),
            ),
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

    pub(super) fn delete_cognito_user_pool_client(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cognito_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_pool_clients.remove(physical_id);
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
        let identity_pool_name = props
            .get("IdentityPoolName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
