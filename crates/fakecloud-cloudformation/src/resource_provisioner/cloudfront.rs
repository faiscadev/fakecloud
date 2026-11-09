//! `AWS::CLOUDFRONT::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

mod distribution_config;

use distribution_config::{cfn_distribution_config, cfn_invalid_request};

impl ResourceProvisioner {
    pub(crate) fn create_cf_origin_access_identity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("CloudFrontOriginAccessIdentityConfig")
            .ok_or("CloudFrontOriginAccessIdentityConfig is required")?;
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let caller_reference = self.physical_name(resource);

        let id = format!("E{}", fakecloud_core::ids::short_id(13).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());
        let s3_canonical_user_id = format!(
            "{:0<64}",
            Uuid::new_v4().simple().to_string().to_lowercase()
        );

        let oai = StoredOriginAccessIdentity {
            id: id.clone(),
            etag,
            s3_canonical_user_id: s3_canonical_user_id.clone(),
            config: CloudFrontOriginAccessIdentityConfig {
                caller_reference,
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_access_identities.insert(id.clone(), oai);

        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("S3CanonicalUserId", s3_canonical_user_id))
    }

    pub(crate) fn delete_cf_origin_access_identity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_access_identities.remove(physical_id);
        Ok(())
    }

    /// Provision an `AWS::CloudFront::Distribution`: translate the CFN
    /// `DistributionConfig` into the wire shape (see `distribution_config`) and
    /// persist a StoredDistribution in CloudFront state.
    pub(crate) fn create_cf_distribution(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("DistributionConfig")
            .ok_or_else(|| "DistributionConfig is required".to_string())?;

        let caller_reference = format!("cfn-{}-{}", resource.logical_id, Uuid::new_v4().simple());
        let config = cfn_distribution_config(cfg, caller_reference, None)?;

        // Mint distribution id + ARN + domain in the same shape the
        // CloudFront service uses.
        // Ids double as the global `<id>.cloudfront.net` domain, so they stay
        // unique across every account.
        let id = self.cloudfront_state.read().unused_id(
            || {
                let suffix: String = Uuid::new_v4()
                    .simple()
                    .to_string()
                    .chars()
                    .take(13)
                    .collect::<String>()
                    .to_uppercase();
                format!("E{suffix}")
            },
            |a, id| a.distributions.contains_key(id),
        );
        let etag = fakecloud_cloudfront::generate_etag();
        let domain_name = format!("{}.cloudfront.net", id.to_lowercase());
        let arn = fakecloud_cloudfront::distribution_arn(&self.region, &self.account_id, &id);

        let stored = StoredDistribution {
            id: id.clone(),
            arn: arn.clone(),
            // CloudFront flips this to Deployed on the first GetDistribution
            // poll, matching the rest of the service.
            status: "InProgress".to_string(),
            last_modified_time: Utc::now(),
            domain_name: domain_name.clone(),
            in_progress_invalidation_batches: 0,
            etag,
            config,
        };

        let mut accounts = self.cloudfront_state.write();
        // Under the guard that commits, so a reference can't be deleted in between.
        fakecloud_cloudfront::validate_distribution_references(
            &stored.config,
            None,
            accounts.get(&self.account_id),
        )
        .map_err(|e| cfn_invalid_request(e.message().as_str()))?;
        let state = accounts.entry(&self.account_id);
        state.distributions.insert(id.clone(), stored);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("DomainName", domain_name)
            .with("Arn", arn))
    }

    pub(crate) fn delete_cf_distribution(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.distributions.remove(physical_id);
        Ok(())
    }

    /// In-place `UpdateDistribution`. Reprovision would mint a NEW distribution
    /// id + `*.cloudfront.net` domain + ARN, breaking `Ref` (id), `GetAtt
    /// DomainName` and any Route53 alias record pointing at the old domain --
    /// the single most-referenced CloudFront attribute. AWS updates nearly the
    /// entire DistributionConfig in place, so rebuild the config and swap it into
    /// the stored distribution, preserving id/arn/domain and the (immutable)
    /// caller reference, and bump the ETag.
    pub(crate) fn update_cf_distribution(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("DistributionConfig")
            .ok_or_else(|| "DistributionConfig is required".to_string())?;

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let stored = state
            .distributions
            .get(&existing.physical_id)
            .ok_or_else(|| format!("Distribution {} not yet provisioned", existing.physical_id))?;
        // CallerReference is immutable across an update; keep the stored one.
        let config = cfn_distribution_config(
            cfg,
            stored.config.caller_reference.clone(),
            Some(&stored.config),
        )?;
        fakecloud_cloudfront::validate_distribution_references(
            &config,
            Some(&stored.config),
            Some(&*state),
        )
        .map_err(|e| cfn_invalid_request(e.message().as_str()))?;
        let dist = state
            .distributions
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Distribution {} not yet provisioned", existing.physical_id))?;
        dist.config = config;
        dist.status = "InProgress".to_string();
        dist.last_modified_time = Utc::now();
        dist.etag = fakecloud_cloudfront::generate_etag();

        Ok(ProvisionResult::new(dist.id.clone())
            .with("Id", dist.id.clone())
            .with("DomainName", dist.domain_name.clone())
            .with("Arn", dist.arn.clone()))
    }

    pub(crate) fn create_cf_origin_access_control(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("OriginAccessControlConfig")
            .ok_or("OriginAccessControlConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.Name is required")?
            .to_string();
        let signing_protocol = cfg
            .get("SigningProtocol")
            .and_then(|v| v.as_str())
            .unwrap_or("sigv4")
            .to_string();
        let signing_behavior = cfg
            .get("SigningBehavior")
            .and_then(|v| v.as_str())
            .unwrap_or("always")
            .to_string();
        let origin_type = cfg
            .get("OriginAccessControlOriginType")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.OriginAccessControlOriginType is required")?
            .to_string();
        let description = cfg
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("E{}", fakecloud_core::ids::short_id(13).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());
        let oac = StoredOriginAccessControl {
            id: id.clone(),
            etag,
            config: OriginAccessControlConfig {
                name,
                description,
                signing_protocol,
                signing_behavior,
                origin_access_control_origin_type: origin_type,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_access_controls.insert(id.clone(), oac);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_origin_access_control(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_access_controls.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_public_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("PublicKeyConfig")
            .ok_or("PublicKeyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.Name is required")?
            .to_string();
        let encoded_key = cfg
            .get("EncodedKey")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.EncodedKey is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let caller_reference = cfg
            .get("CallerReference")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let caller_reference = if caller_reference.is_empty() {
            self.physical_name(resource)
        } else {
            caller_reference
        };

        let id = format!("K{}", fakecloud_core::ids::short_id(13).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let pk = StoredPublicKey {
            id: id.clone(),
            etag,
            created_time: Utc::now(),
            config: PublicKeyConfig {
                caller_reference,
                name,
                encoded_key,
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.public_keys.insert(id.clone(), pk);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_public_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.public_keys.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_key_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("KeyGroupConfig")
            .ok_or("KeyGroupConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("KeyGroupConfig.Name is required")?
            .to_string();
        let items: Vec<String> = cfg
            .get("Items")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("KG{}", fakecloud_core::ids::short_id(12).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let kg = StoredKeyGroup {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: KeyGroupConfig {
                name,
                items: KeyGroupItems { public_key: items },
                comment,
            },
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.key_groups.insert(id.clone(), kg);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_key_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.key_groups.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_function(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let function_code = props
            .get("FunctionCode")
            .and_then(|v| v.as_str())
            .ok_or("FunctionCode is required")?
            .to_string();
        let cfg = props
            .get("FunctionConfig")
            .ok_or("FunctionConfig is required")?;
        let runtime = cfg
            .get("Runtime")
            .and_then(|v| v.as_str())
            .unwrap_or("cloudfront-js-2.0")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("FN{}", fakecloud_core::ids::short_id(12).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());
        let function_arn = fakecloud_cloudfront::cloudfront_arn(
            &self.region,
            &self.account_id,
            &format!("function/{name}"),
        );

        let now = Utc::now();
        let func = StoredFunction {
            name: name.clone(),
            etag,
            status: "UNPUBLISHED".to_string(),
            stage: "DEVELOPMENT".to_string(),
            function_arn: function_arn.clone(),
            created_time: now,
            last_modified_time: now,
            config: FunctionConfig {
                comment,
                runtime,
                key_value_store_associations: None,
            },
            function_code,
            live_function_code: None,
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        // Use the function's ARN/name as the registry key so subsequent
        // operations (Get/Update/Delete) keyed by name resolve.
        state.functions.insert(name.clone(), func);

        Ok(ProvisionResult::new(name.clone())
            .with("FunctionARN", function_arn)
            .with("FunctionMetadata.FunctionARN", id)
            .with("Stage", "DEVELOPMENT"))
    }

    pub(crate) fn delete_cf_function(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.functions.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_cache_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("CachePolicyConfig")
            .ok_or("CachePolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("CachePolicyConfig.Name is required")?
            .to_string();
        let min_ttl = cfg
            .get("MinTTL")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .unwrap_or(0);
        let default_ttl = cfg.get("DefaultTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let max_ttl = cfg.get("MaxTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("CP{}", fakecloud_core::ids::short_id(12).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let cache_policy = StoredCachePolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: CachePolicyConfig {
                comment,
                name,
                default_ttl,
                max_ttl,
                min_ttl,
                parameters_in_cache_key_and_forwarded_to_origin: None,
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.cache_policies.insert(id.clone(), cache_policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_cache_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.cache_policies.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_origin_request_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("OriginRequestPolicyConfig")
            .ok_or("OriginRequestPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginRequestPolicyConfig.Name is required")?
            .to_string();
        let header_behavior = cfg
            .get("HeadersConfig")
            .and_then(|v| v.get("HeaderBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let cookie_behavior = cfg
            .get("CookiesConfig")
            .and_then(|v| v.get("CookieBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let query_string_behavior = cfg
            .get("QueryStringsConfig")
            .and_then(|v| v.get("QueryStringBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("ORP{}", fakecloud_core::ids::short_id(11).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let policy = StoredOriginRequestPolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: OriginRequestPolicyConfig {
                comment,
                name,
                headers_config: OriginRequestPolicyHeadersConfig {
                    header_behavior,
                    headers: None,
                },
                cookies_config: OriginRequestPolicyCookiesConfig {
                    cookie_behavior,
                    cookies: None,
                },
                query_strings_config: OriginRequestPolicyQueryStringsConfig {
                    query_string_behavior,
                    query_strings: None,
                },
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_request_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_origin_request_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.origin_request_policies.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_cf_response_headers_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cfg = props
            .get("ResponseHeadersPolicyConfig")
            .ok_or("ResponseHeadersPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("ResponseHeadersPolicyConfig.Name is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = format!("RHP{}", fakecloud_core::ids::short_id(11).to_uppercase());
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let policy = StoredResponseHeadersPolicy {
            id: id.clone(),
            etag,
            last_modified_time: Utc::now(),
            config: ResponseHeadersPolicyConfig {
                comment,
                name,
                cors_config: None,
                security_headers_config: None,
                server_timing_headers_config: None,
                custom_headers_config: None,
                remove_headers_config: None,
            },
            policy_type: "custom".to_string(),
        };

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.response_headers_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_response_headers_policy(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        state.response_headers_policies.remove(physical_id);
        Ok(())
    }

    // --- In-place policy updates ---
    //
    // These policies mint a random id that a Distribution stores via
    // DefaultCacheBehavior.{CachePolicyId,OriginRequestPolicyId,
    // ResponseHeadersPolicyId}. Reprovision on a config edit churns the id and
    // dangles that reference (the Distribution is not re-run in the same update).
    // AWS updates the policy in place (UpdateCachePolicy etc.), so rebuild the
    // config, preserve the id, and bump the ETag.

    pub(crate) fn update_cf_cache_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("CachePolicyConfig")
            .ok_or("CachePolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("CachePolicyConfig.Name is required")?
            .to_string();
        let min_ttl = cfg
            .get("MinTTL")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0);
        let default_ttl = cfg.get("DefaultTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
        let max_ttl = cfg.get("MaxTTL").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let p = state
            .cache_policies
            .get_mut(&id)
            .ok_or_else(|| format!("Cache policy {id} not yet provisioned"))?;
        p.config = CachePolicyConfig {
            comment,
            name,
            default_ttl,
            max_ttl,
            min_ttl,
            parameters_in_cache_key_and_forwarded_to_origin: None,
        };
        p.etag = etag;
        p.last_modified_time = Utc::now();
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_origin_request_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("OriginRequestPolicyConfig")
            .ok_or("OriginRequestPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginRequestPolicyConfig.Name is required")?
            .to_string();
        let header_behavior = cfg
            .get("HeadersConfig")
            .and_then(|v| v.get("HeaderBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let cookie_behavior = cfg
            .get("CookiesConfig")
            .and_then(|v| v.get("CookieBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let query_string_behavior = cfg
            .get("QueryStringsConfig")
            .and_then(|v| v.get("QueryStringBehavior"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let p = state
            .origin_request_policies
            .get_mut(&id)
            .ok_or_else(|| format!("Origin request policy {id} not yet provisioned"))?;
        p.config = OriginRequestPolicyConfig {
            comment,
            name,
            headers_config: OriginRequestPolicyHeadersConfig {
                header_behavior,
                headers: None,
            },
            cookies_config: OriginRequestPolicyCookiesConfig {
                cookie_behavior,
                cookies: None,
            },
            query_strings_config: OriginRequestPolicyQueryStringsConfig {
                query_string_behavior,
                query_strings: None,
            },
        };
        p.etag = etag;
        p.last_modified_time = Utc::now();
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_response_headers_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("ResponseHeadersPolicyConfig")
            .ok_or("ResponseHeadersPolicyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("ResponseHeadersPolicyConfig.Name is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let p = state
            .response_headers_policies
            .get_mut(&id)
            .ok_or_else(|| format!("Response headers policy {id} not yet provisioned"))?;
        p.config = ResponseHeadersPolicyConfig {
            comment,
            name,
            cors_config: None,
            security_headers_config: None,
            server_timing_headers_config: None,
            custom_headers_config: None,
            remove_headers_config: None,
        };
        p.etag = etag;
        p.last_modified_time = Utc::now();
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_origin_access_control(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("OriginAccessControlConfig")
            .ok_or("OriginAccessControlConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.Name is required")?
            .to_string();
        let signing_protocol = cfg
            .get("SigningProtocol")
            .and_then(|v| v.as_str())
            .unwrap_or("sigv4")
            .to_string();
        let signing_behavior = cfg
            .get("SigningBehavior")
            .and_then(|v| v.as_str())
            .unwrap_or("always")
            .to_string();
        let origin_type = cfg
            .get("OriginAccessControlOriginType")
            .and_then(|v| v.as_str())
            .ok_or("OriginAccessControlConfig.OriginAccessControlOriginType is required")?
            .to_string();
        let description = cfg
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let oac = state
            .origin_access_controls
            .get_mut(&id)
            .ok_or_else(|| format!("Origin access control {id} not yet provisioned"))?;
        oac.config = OriginAccessControlConfig {
            name,
            description,
            signing_protocol,
            signing_behavior,
            origin_access_control_origin_type: origin_type,
        };
        oac.etag = etag;
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_public_key(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("PublicKeyConfig")
            .ok_or("PublicKeyConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.Name is required")?
            .to_string();
        let encoded_key = cfg
            .get("EncodedKey")
            .and_then(|v| v.as_str())
            .ok_or("PublicKeyConfig.EncodedKey is required")?
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let pk = state
            .public_keys
            .get_mut(&id)
            .ok_or_else(|| format!("Public key {id} not yet provisioned"))?;
        // CallerReference is immutable in CloudFront; preserve the stored one.
        let caller_reference = pk.config.caller_reference.clone();
        pk.config = PublicKeyConfig {
            caller_reference,
            name,
            encoded_key,
            comment,
        };
        pk.etag = etag;
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_key_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("KeyGroupConfig")
            .ok_or("KeyGroupConfig is required")?;
        let name = cfg
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("KeyGroupConfig.Name is required")?
            .to_string();
        let items: Vec<String> = cfg
            .get("Items")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let kg = state
            .key_groups
            .get_mut(&id)
            .ok_or_else(|| format!("Key group {id} not yet provisioned"))?;
        kg.config = KeyGroupConfig {
            name,
            items: KeyGroupItems { public_key: items },
            comment,
        };
        kg.etag = etag;
        kg.last_modified_time = Utc::now();
        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn update_cf_function(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_code = props
            .get("FunctionCode")
            .and_then(|v| v.as_str())
            .ok_or("FunctionCode is required")?
            .to_string();
        let cfg = props
            .get("FunctionConfig")
            .ok_or("FunctionConfig is required")?;
        let runtime = cfg
            .get("Runtime")
            .and_then(|v| v.as_str())
            .unwrap_or("cloudfront-js-2.0")
            .to_string();
        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .map(String::from);
        let etag = format!("E{}", fakecloud_core::ids::short_id(7).to_uppercase());

        // Functions are keyed by Name (== physical id). Name is the resource
        // identifier, so an in-place update mutates the dev-stage code + config
        // and preserves the ARN Ref/GetAtt targets. A Name change would fall
        // back to replacement through the generic path.
        let id = existing.physical_id.clone();
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry(&self.account_id);
        let func = state
            .functions
            .get_mut(&id)
            .ok_or_else(|| format!("Function {id} not yet provisioned"))?;
        func.config = FunctionConfig {
            comment,
            runtime,
            key_value_store_associations: func.config.key_value_store_associations.clone(),
        };
        func.function_code = function_code;
        func.status = "UNPUBLISHED".to_string();
        func.stage = "DEVELOPMENT".to_string();
        func.etag = etag;
        func.last_modified_time = Utc::now();
        let function_arn = func.function_arn.clone();
        Ok(ProvisionResult::new(id.clone())
            .with("FunctionARN", function_arn)
            .with("Stage", "DEVELOPMENT"))
    }
}
