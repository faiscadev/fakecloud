//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `s3`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_s3_bucket(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.s3_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let bucket = state.buckets.get(physical_id)?;
        match attribute {
            "Arn" => Some(Arn::s3(&bucket.name).to_string()),
            "DomainName" => Some(format!("{}.s3.amazonaws.com", bucket.name)),
            "RegionalDomainName" => {
                Some(format!("{}.s3.{}.amazonaws.com", bucket.name, self.region))
            }
            "DualStackDomainName" => Some(format!(
                "{}.s3.dualstack.{}.amazonaws.com",
                bucket.name, self.region
            )),
            "WebsiteURL" => Some(format!(
                "http://{}.s3-website-{}.amazonaws.com",
                bucket.name, self.region
            )),
            _ => None,
        }
    }

    // --- S3 ---

    pub(super) fn create_s3_bucket(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let bucket_name = props
            .get("BucketName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        let region = state.region.clone();
        let mut bucket = S3Bucket::new(bucket_name, &state.region, &state.account_id);
        // Translate every modeled CFN property (VersioningConfiguration,
        // BucketEncryption, PublicAccessBlockConfiguration,
        // NotificationConfiguration, Tags, Website/Cors/Lifecycle/Logging) into
        // the S3 bucket state and persist each subresource through the SAME
        // store path the PutBucket* handlers use. Previously only BucketName was
        // read, so a CREATE_COMPLETE bucket surfaced with none of its
        // protections and S3->Lambda/SQS/SNS notifications never fired.
        fakecloud_s3::apply_cfn_bucket_properties(&mut bucket, props, &self.s3_store)?;
        // Write the bucket through to the S3 disk store, exactly as the real
        // CreateBucket handler does, so a CFN-provisioned bucket survives a
        // restart instead of living only in the in-memory map.
        let meta = bucket_meta_snapshot(&bucket);
        state.buckets.insert(bucket_name.to_string(), bucket);
        self.s3_store
            .put_bucket_meta(bucket_name, &meta)
            .map_err(|e| format!("failed to persist bucket {bucket_name}: {e}"))?;

        let arn = Arn::s3(bucket_name).to_string();
        let domain_name = format!("{bucket_name}.s3.amazonaws.com");
        let regional_domain_name = format!("{bucket_name}.s3.{region}.amazonaws.com");
        let dual_stack_domain_name = format!("{bucket_name}.s3.dualstack.{region}.amazonaws.com");
        let website_url = format!("http://{bucket_name}.s3-website-{region}.amazonaws.com");
        Ok(ProvisionResult::new(bucket_name)
            .with("Arn", arn)
            .with("DomainName", domain_name)
            .with("RegionalDomainName", regional_domain_name)
            .with("DualStackDomainName", dual_stack_domain_name)
            .with("WebsiteURL", website_url))
    }

    /// Apply a CFN stack update to an existing bucket in place. Re-runs the
    /// same property translation `create_s3_bucket` uses so a follow-up update
    /// that turns on versioning/encryption/etc. is applied instead of being a
    /// silent no-op. The bucket's objects are preserved; only its config
    /// (sub)resources are re-derived from the new template properties.
    pub(super) fn update_s3_bucket(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let bucket_name = &existing.physical_id;
        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        let region = state.region.clone();
        {
            let bucket = state
                .buckets
                .get_mut(bucket_name)
                .ok_or_else(|| format!("Bucket {bucket_name} not yet provisioned"))?;
            fakecloud_s3::apply_cfn_bucket_properties(
                bucket,
                &resource.properties,
                &self.s3_store,
            )?;
        }

        let arn = Arn::s3(bucket_name).to_string();
        let domain_name = format!("{bucket_name}.s3.amazonaws.com");
        let regional_domain_name = format!("{bucket_name}.s3.{region}.amazonaws.com");
        let dual_stack_domain_name = format!("{bucket_name}.s3.dualstack.{region}.amazonaws.com");
        let website_url = format!("http://{bucket_name}.s3-website-{region}.amazonaws.com");
        Ok(ProvisionResult::new(bucket_name.clone())
            .with("Arn", arn)
            .with("DomainName", domain_name)
            .with("RegionalDomainName", regional_domain_name)
            .with("DualStackDomainName", dual_stack_domain_name)
            .with("WebsiteURL", website_url))
    }

    pub(super) fn delete_s3_bucket(&self, physical_id: &str) -> Result<(), String> {
        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        state.buckets.remove(physical_id);
        // Remove the bucket from disk too, so a CFN-deleted bucket does not
        // reappear after a restart.
        self.s3_store
            .delete_bucket(physical_id)
            .map_err(|e| format!("failed to remove bucket {physical_id}: {e}"))?;
        Ok(())
    }

    // --- S3 BucketPolicy ---
    //
    // AWS::S3::BucketPolicy stores the PolicyDocument on `bucket.policy` — the
    // same field PutBucketPolicy writes — so GetBucketPolicy round-trips it.
    // The `Bucket` property is a single Ref already resolved to the bucket name
    // (which is the bucket's physical id). The policy resource's physical id is
    // `{bucket}-policy`; delete strips the suffix to recover the bucket name.

    pub(super) fn create_s3_bucket_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let bucket_name = s3_policy_bucket_name(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        let bucket = state
            .buckets
            .get_mut(&bucket_name)
            .ok_or_else(|| format!("Bucket {bucket_name} not yet provisioned"))?;
        bucket.policy = Some(policy.clone());
        // Persist the policy subresource to disk, as PutBucketPolicy does.
        self.s3_store
            .put_bucket_subresource(&bucket_name, BucketSubresource::Policy, &policy)
            .map_err(|e| format!("failed to persist bucket policy for {bucket_name}: {e}"))?;
        Ok(ProvisionResult::new(format!("{bucket_name}-policy")))
    }

    pub(super) fn update_s3_bucket_policy(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let bucket_name = s3_policy_bucket_name(&resource.properties)?;
        let policy = policy_document_string(&resource.properties)?;

        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        // If the policy was moved to a different bucket, clear the old one.
        let old_bucket = existing
            .physical_id
            .strip_suffix("-policy")
            .unwrap_or(&existing.physical_id);
        if old_bucket != bucket_name {
            if let Some(bucket) = state.buckets.get_mut(old_bucket) {
                bucket.policy = None;
            }
            self.s3_store
                .delete_bucket_subresource(old_bucket, BucketSubresource::Policy)
                .map_err(|e| format!("failed to clear bucket policy for {old_bucket}: {e}"))?;
        }
        let bucket = state
            .buckets
            .get_mut(&bucket_name)
            .ok_or_else(|| format!("Bucket {bucket_name} not yet provisioned"))?;
        bucket.policy = Some(policy.clone());
        self.s3_store
            .put_bucket_subresource(&bucket_name, BucketSubresource::Policy, &policy)
            .map_err(|e| format!("failed to persist bucket policy for {bucket_name}: {e}"))?;
        Ok(ProvisionResult::new(format!("{bucket_name}-policy")))
    }

    pub(super) fn delete_s3_bucket_policy(&self, physical_id: &str) -> Result<(), String> {
        let bucket_name = physical_id.strip_suffix("-policy").unwrap_or(physical_id);
        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        if let Some(bucket) = state.buckets.get_mut(bucket_name) {
            bucket.policy = None;
        }
        self.s3_store
            .delete_bucket_subresource(bucket_name, BucketSubresource::Policy)
            .map_err(|e| format!("failed to remove bucket policy for {bucket_name}: {e}"))?;
        Ok(())
    }
}

/// Resolve the `Bucket` property (a Ref already resolved to the bucket name).
fn s3_policy_bucket_name(props: &serde_json::Value) -> Result<String, String> {
    props
        .get("Bucket")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| "Bucket is required".to_string())
}
