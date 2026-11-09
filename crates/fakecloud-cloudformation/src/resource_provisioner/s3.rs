//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `s3`.

use super::*;
use fakecloud_aws::endpoint;

/// The `AWS::S3::Bucket` attributes CloudFormation reports for a bucket in
/// `region`: its ARN and the endpoint hostnames AWS publishes for it, under
/// the region's partition DNS suffix and website endpoint form.
fn bucket_attributes(bucket: &str, region: &str) -> ProvisionResult {
    ProvisionResult::new(bucket)
        .with("Arn", Arn::s3_in(region, bucket).to_string())
        .with(
            "DomainName",
            endpoint::s3_bucket_domain_name(bucket, region),
        )
        .with(
            "RegionalDomainName",
            endpoint::s3_regional_domain_name(bucket, region),
        )
        .with(
            "DualStackDomainName",
            endpoint::s3_dualstack_domain_name(bucket, region),
        )
        .with("WebsiteURL", endpoint::s3_website_url(bucket, region))
}

impl ResourceProvisioner {
    pub(super) fn get_att_s3_bucket(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.s3_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let bucket = state.buckets.get(physical_id)?;
        bucket_attributes(&bucket.name, &bucket.region)
            .attributes
            .remove(attribute)
    }

    // --- S3 ---

    pub(super) fn create_s3_bucket(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let bucket_name = props
            .get("BucketName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name);
        let bucket_limit = self.s3_bucket_limit();

        let mut __s3_mas = self.s3_state.write();
        // The bucket namespace is global: a name held by any account (this
        // one included, e.g. a bucket a deleted stack retained) fails the
        // create rather than being overwritten.
        if __s3_mas
            .iter()
            .any(|(_, acct)| acct.buckets.contains_key(bucket_name))
        {
            return Err(resource_already_exists("AWS::S3::Bucket", bucket_name));
        }
        let state = __s3_mas.get_or_create(&self.account_id);
        fakecloud_s3::quota::check_new_bucket(state, bucket_limit)
            .map_err(|e| super::quota::refusal("S3", e))?;
        let region = self.region.clone();
        let mut bucket = S3Bucket::new(bucket_name, &self.region, &state.account_id);
        // Translate every modeled CFN property (VersioningConfiguration,
        // BucketEncryption, PublicAccessBlockConfiguration, OwnershipControls,
        // AccessControl, NotificationConfiguration, Tags,
        // Website/Cors/Lifecycle/Logging) over the defaults S3 gives every new
        // bucket, and write the bucket through to the S3 disk store -- meta
        // first, then its subresources, through the same store paths the
        // CreateBucket / PutBucket* handlers use -- so a CFN-provisioned bucket
        // survives a restart. A failed write leaves nothing behind.
        fakecloud_s3::apply_cfn_new_bucket(&mut bucket, props, &self.s3_store)?;
        state.buckets.insert(bucket_name.to_string(), bucket);

        Ok(bucket_attributes(bucket_name, &region))
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
        let bucket = state
            .buckets
            .get_mut(bucket_name)
            .ok_or_else(|| format!("Bucket {bucket_name} not yet provisioned"))?;
        fakecloud_s3::apply_cfn_bucket_properties(bucket, &resource.properties, &self.s3_store)?;
        Ok(bucket_attributes(bucket_name, &bucket.region))
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
        if fakecloud_s3::bucket_policy_blocked(bucket, &policy) {
            return Err(public_policy_blocked(&bucket_name));
        }
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
        if fakecloud_s3::bucket_policy_blocked(bucket, &policy) {
            return Err(public_policy_blocked(&bucket_name));
        }
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

/// The failure S3 gives PutBucketPolicy for a public policy on a bucket whose
/// Block Public Access sets `BlockPublicPolicy` (on by default for new buckets).
fn public_policy_blocked(bucket: &str) -> String {
    format!(
        "User is not authorized to perform: s3:PutBucketPolicy on resource: \"arn:aws:s3:::{bucket}\" \
         because public policies are blocked by the BlockPublicPolicy block public access setting. \
         (Service: S3, Status Code: 403, Error Code: AccessDenied)"
    )
}

/// Resolve the `Bucket` property (a Ref already resolved to the bucket name).
fn s3_policy_bucket_name(props: &serde_json::Value) -> Result<String, String> {
    props
        .get("Bucket")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| "Bucket is required".to_string())
}
