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
        let bucket = S3Bucket::new(bucket_name, &state.region, &state.account_id);
        state.buckets.insert(bucket_name.to_string(), bucket);

        let arn = Arn::s3(&bucket_name).to_string();
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

    pub(super) fn delete_s3_bucket(&self, physical_id: &str) -> Result<(), String> {
        let mut __s3_mas = self.s3_state.write();
        let state = __s3_mas.get_or_create(&self.account_id);
        state.buckets.remove(physical_id);
        Ok(())
    }
}
