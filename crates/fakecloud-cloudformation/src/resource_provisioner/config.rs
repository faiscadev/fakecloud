//! CloudFormation provisioner arms for `AWS::Config::*`.
//!
//! Without a provisioner arm a service with a registered snapshot hook yields
//! phantom `CREATE_COMPLETE` resources that vanish on restart (#1766). These
//! arms mutate the snapshot-backed `config` state directly, and the server's
//! `config` snapshot hook writes them through to disk.

use super::*;

use fakecloud_config::provision as cfg;

impl ResourceProvisioner {
    pub(super) fn create_config_resource(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let account = self.account_id.clone();
        let region = self.region.clone();
        let mut accounts = self.config_state.write();
        let (physical_id, arn) = match resource.resource_type.as_str() {
            "AWS::Config::ConfigurationRecorder" => {
                let id = cfg::provision_recorder(&mut accounts, &account, props);
                (id, None)
            }
            "AWS::Config::DeliveryChannel" => {
                let id = cfg::provision_delivery_channel(&mut accounts, &account, props);
                (id, None)
            }
            "AWS::Config::ConfigRule" => {
                let id = cfg::provision_config_rule(&mut accounts, &account, &region, props);
                let arn = accounts
                    .account(&account)
                    .and_then(|a| a.rules.get(&id))
                    .map(|r| r.arn.clone());
                (id, arn)
            }
            "AWS::Config::ConfigurationAggregator" => {
                let id = cfg::provision_aggregator(&mut accounts, &account, &region, props);
                let arn = accounts
                    .account(&account)
                    .and_then(|a| a.aggregators.get(&id))
                    .map(|a| a.arn.clone());
                (id, arn)
            }
            "AWS::Config::AggregationAuthorization" => {
                let arn = cfg::provision_aggregation_authorization(
                    &mut accounts,
                    &account,
                    &region,
                    props,
                );
                (arn.clone(), Some(arn))
            }
            "AWS::Config::ConformancePack" => {
                let id = cfg::provision_conformance_pack(&mut accounts, &account, &region, props);
                let arn = accounts
                    .account(&account)
                    .and_then(|a| a.conformance_packs.get(&id))
                    .map(|p| p.arn.clone());
                (id, arn)
            }
            "AWS::Config::OrganizationConfigRule" => {
                let id = cfg::provision_org_config_rule(&mut accounts, &account, &region, props);
                let arn = accounts
                    .account(&account)
                    .and_then(|a| a.org_rules.get(&id))
                    .map(|r| r.arn.clone());
                (id, arn)
            }
            other => return Err(format!("unsupported AWS::Config resource type: {other}")),
        };
        let mut result = ProvisionResult::new(physical_id);
        if let Some(arn) = arn {
            if resource.resource_type == "AWS::Config::AggregationAuthorization" {
                result = result.with("AggregationAuthorizationArn", arn);
            } else {
                result = result.with("Arn", arn);
            }
        }
        Ok(result)
    }

    pub(super) fn delete_config_resource(
        &self,
        resource_type: &str,
        physical_id: &str,
    ) -> Result<(), String> {
        let account = self.account_id.clone();
        let mut accounts = self.config_state.write();
        cfg::deprovision(&mut accounts, &account, resource_type, physical_id);
        Ok(())
    }
}
