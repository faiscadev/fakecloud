//! `AWS::CLOUDFRONT::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

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
        let caller_reference = format!("cfn-{}", resource.logical_id);

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
        let state = accounts.entry("000000000000");
        state.origin_access_identities.insert(id.clone(), oai);

        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("S3CanonicalUserId", s3_canonical_user_id))
    }

    pub(crate) fn delete_cf_origin_access_identity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        state.origin_access_identities.remove(physical_id);
        Ok(())
    }

    /// Translate the CFN-flat `DistributionConfig` members that the create /
    /// update paths would otherwise drop -- Aliases, CacheBehaviors,
    /// CustomErrorResponses, Logging, Restrictions -- into the CloudFront wire
    /// shape and apply them. CFN spells these as flat lists / a bare object;
    /// the service model nests them under Quantity+Items, mirroring the Origins
    /// translation. Only members present in the template are set, so an absent
    /// one stays `None` (create) / is cleared on update.
    fn apply_cfn_distribution_extras(config: &mut DistributionConfig, cfg: &serde_json::Value) {
        // Aliases: flat ["a.example.com", ...].
        config.aliases = cfg.get("Aliases").and_then(|v| v.as_array()).map(|arr| {
            let cname: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            Aliases {
                quantity: cname.len() as i32,
                items: Some(AliasItems { cname }),
            }
        });
        // CacheBehaviors: flat [{ PathPattern, ... }, ...].
        config.cache_behaviors = cfg
            .get("CacheBehaviors")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let cache_behavior: Vec<CacheBehavior> = arr
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                CacheBehaviors {
                    quantity: cache_behavior.len() as i32,
                    items: Some(CacheBehaviorItems { cache_behavior }),
                }
            });
        // CustomErrorResponses: flat [{ ErrorCode, ... }, ...].
        config.custom_error_responses = cfg
            .get("CustomErrorResponses")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let custom_error_response: Vec<CustomErrorResponse> = arr
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                CustomErrorResponses {
                    quantity: custom_error_response.len() as i32,
                    items: Some(CustomErrorResponseItems {
                        custom_error_response,
                    }),
                }
            });
        // Logging: { Bucket, IncludeCookies, Prefix } -- CFN has no Enabled, so
        // presence of the block means logging is on.
        config.logging = cfg
            .get("Logging")
            .filter(|v| v.is_object())
            .map(|log| LoggingConfig {
                enabled: true,
                include_cookies: log
                    .get("IncludeCookies")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                bucket: log
                    .get("Bucket")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                prefix: log
                    .get("Prefix")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            });
        // Restrictions: { GeoRestriction: { RestrictionType, Locations: [..] } }.
        config.restrictions = cfg
            .get("Restrictions")
            .and_then(|v| v.get("GeoRestriction"))
            .map(|geo| {
                let location: Vec<String> = geo
                    .get("Locations")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                Restrictions {
                    geo_restriction: GeoRestriction {
                        restriction_type: geo
                            .get("RestrictionType")
                            .and_then(|v| v.as_str())
                            .unwrap_or("none")
                            .to_string(),
                        quantity: location.len() as i32,
                        items: if location.is_empty() {
                            None
                        } else {
                            Some(LocationList { location })
                        },
                    },
                }
            });
    }

    /// Provision an `AWS::CloudFront::Distribution`. Reads
    /// DistributionConfig.Origins/DefaultCacheBehavior/etc. and persists a
    /// StoredDistribution in CloudFront state. CFN's Origins property is a flat
    /// array, so we wrap it back into the wire shape with a quantity +
    /// Items.Origin nesting; `apply_cfn_distribution_extras` does the same for
    /// Aliases / CacheBehaviors / CustomErrorResponses / Logging / Restrictions.
    pub(crate) fn create_cf_distribution(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = resource
            .properties
            .get("DistributionConfig")
            .ok_or_else(|| "DistributionConfig is required".to_string())?;

        // CFN Origins is a flat JSON array; the wire shape is
        // { Quantity, Items: { Origin: [...] } }. Translate. CustomOriginConfig
        // uses AWS's HTTPPort/HTTPSPort casing, which the model now accepts
        // natively (see CustomOriginConfig in fakecloud-cloudfront::model), so no
        // field patching is needed here.
        let origin_entries: Vec<Origin> = cfg
            .get("Origins")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "DistributionConfig.Origins is required".to_string())?
            .iter()
            .map(|o| {
                serde_json::from_value::<Origin>(o.clone())
                    .map_err(|e| format!("Invalid Origin entry: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if origin_entries.is_empty() {
            return Err("DistributionConfig.Origins must contain at least one origin".to_string());
        }
        let origins = Origins {
            quantity: origin_entries.len() as i32,
            items: Some(OriginItems {
                origin: origin_entries,
            }),
        };

        let dcb_value = cfg
            .get("DefaultCacheBehavior")
            .ok_or_else(|| "DistributionConfig.DefaultCacheBehavior is required".to_string())?;
        let default_cache_behavior: DefaultCacheBehavior =
            serde_json::from_value(dcb_value.clone())
                .map_err(|e| format!("Invalid DefaultCacheBehavior: {e}"))?;

        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let enabled = cfg.get("Enabled").and_then(|v| v.as_bool()).unwrap_or(true);
        let price_class = cfg
            .get("PriceClass")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let http_version = cfg
            .get("HttpVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let is_ipv6_enabled = cfg.get("IPV6Enabled").and_then(|v| v.as_bool());
        let default_root_object = cfg
            .get("DefaultRootObject")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let web_acl_id = cfg
            .get("WebACLId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let viewer_certificate: Option<ViewerCertificate> = cfg
            .get("ViewerCertificate")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| format!("Invalid ViewerCertificate: {e}"))?;

        let caller_reference = format!("cfn-{}-{}", resource.logical_id, Uuid::new_v4().simple());

        let mut config = DistributionConfig {
            caller_reference,
            comment,
            enabled,
            origins,
            default_cache_behavior,
            ..Default::default()
        };
        config.price_class = price_class;
        config.http_version = http_version;
        config.is_ipv6_enabled = is_ipv6_enabled;
        config.default_root_object = default_root_object;
        config.web_acl_id = web_acl_id;
        config.viewer_certificate = viewer_certificate;
        Self::apply_cfn_distribution_extras(&mut config, cfg);

        // Mint distribution id + ARN + domain in the same shape the
        // CloudFront service uses.
        let id_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(13)
            .collect::<String>()
            .to_uppercase();
        let id = format!("E{id_suffix}");
        let etag_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(7)
            .collect::<String>()
            .to_uppercase();
        let etag = format!("E{etag_suffix}");
        let domain_name = format!("{}.cloudfront.net", id.to_lowercase());
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution/{}",
            self.account_id, id
        );

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
        let state = accounts.entry("000000000000");
        state.distributions.insert(id.clone(), stored);
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("DomainName", domain_name)
            .with("Arn", arn))
    }

    pub(crate) fn delete_cf_distribution(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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

        let origin_entries: Vec<Origin> = cfg
            .get("Origins")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "DistributionConfig.Origins is required".to_string())?
            .iter()
            .map(|o| {
                serde_json::from_value::<Origin>(o.clone())
                    .map_err(|e| format!("Invalid Origin entry: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if origin_entries.is_empty() {
            return Err("DistributionConfig.Origins must contain at least one origin".to_string());
        }
        let origins = Origins {
            quantity: origin_entries.len() as i32,
            items: Some(OriginItems {
                origin: origin_entries,
            }),
        };

        let dcb_value = cfg
            .get("DefaultCacheBehavior")
            .ok_or_else(|| "DistributionConfig.DefaultCacheBehavior is required".to_string())?;
        let default_cache_behavior: DefaultCacheBehavior =
            serde_json::from_value(dcb_value.clone())
                .map_err(|e| format!("Invalid DefaultCacheBehavior: {e}"))?;

        let comment = cfg
            .get("Comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let enabled = cfg.get("Enabled").and_then(|v| v.as_bool()).unwrap_or(true);
        let viewer_certificate: Option<ViewerCertificate> = cfg
            .get("ViewerCertificate")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| format!("Invalid ViewerCertificate: {e}"))?;

        let mut config = DistributionConfig {
            caller_reference: String::new(), // preserved from the stored config below
            comment,
            enabled,
            origins,
            default_cache_behavior,
            ..Default::default()
        };
        config.price_class = cfg
            .get("PriceClass")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        config.http_version = cfg
            .get("HttpVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        config.is_ipv6_enabled = cfg.get("IPV6Enabled").and_then(|v| v.as_bool());
        config.default_root_object = cfg
            .get("DefaultRootObject")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        config.web_acl_id = cfg
            .get("WebACLId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        config.viewer_certificate = viewer_certificate;
        Self::apply_cfn_distribution_extras(&mut config, cfg);

        let etag_suffix: String = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(7)
            .collect::<String>()
            .to_uppercase();

        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
        let dist = state
            .distributions
            .get_mut(&existing.physical_id)
            .ok_or_else(|| format!("Distribution {} not yet provisioned", existing.physical_id))?;
        // CallerReference is immutable across an update; keep the stored one.
        config.caller_reference = dist.config.caller_reference.clone();
        dist.config = config;
        dist.status = "InProgress".to_string();
        dist.last_modified_time = Utc::now();
        dist.etag = format!("E{etag_suffix}");

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
        let state = accounts.entry("000000000000");
        state.origin_access_controls.insert(id.clone(), oac);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_origin_access_control(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
            format!("cfn-{}", resource.logical_id)
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
        let state = accounts.entry("000000000000");
        state.public_keys.insert(id.clone(), pk);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_public_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
        state.key_groups.insert(id.clone(), kg);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_key_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
        let function_arn =
            Arn::global("cloudfront", &self.account_id, &format!("function/{name}")).to_string();

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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
        state.cache_policies.insert(id.clone(), cache_policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_cache_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
        state.origin_request_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_origin_request_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
        state.response_headers_policies.insert(id.clone(), policy);

        Ok(ProvisionResult::new(id.clone()).with("Id", id))
    }

    pub(crate) fn delete_cf_response_headers_policy(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.cloudfront_state.write();
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
        let state = accounts.entry("000000000000");
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
