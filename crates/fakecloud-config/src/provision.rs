//! Helpers shared with the CloudFormation resource provisioner so that
//! `AWS::Config::*` resources land in real Config state (and therefore survive
//! a restart) rather than becoming phantom CREATE_COMPLETE resources.

use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use crate::state::*;

fn short_id() -> String {
    Uuid::new_v4().to_string().replace('-', "")[..6].to_string()
}

/// `AWS::Config::ConfigurationRecorder` -> physical id (the recorder name).
pub fn provision_recorder(accounts: &mut ConfigAccounts, account: &str, props: &Value) -> String {
    let name = props
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_string();
    let role_arn = props
        .get("RoleARN")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let acc = accounts.account_mut(account);
    acc.recorders.insert(
        name.clone(),
        ConfigurationRecorder {
            name: name.clone(),
            role_arn,
            recording_group: props.get("RecordingGroup").cloned(),
            recording_mode: props.get("RecordingMode").cloned(),
            arn: None,
            service_principal: None,
            recording: false,
            last_start_time: None,
            last_stop_time: None,
            last_status: "PENDING".into(),
            last_status_change_time: Some(Utc::now()),
        },
    );
    name
}

/// `AWS::Config::DeliveryChannel` -> physical id (the channel name).
pub fn provision_delivery_channel(
    accounts: &mut ConfigAccounts,
    account: &str,
    props: &Value,
) -> String {
    let name = props
        .get("Name")
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_string();
    let acc = accounts.account_mut(account);
    acc.delivery_channels.insert(
        name.clone(),
        DeliveryChannel {
            name: name.clone(),
            s3_bucket_name: props
                .get("S3BucketName")
                .and_then(Value::as_str)
                .map(String::from),
            s3_key_prefix: props
                .get("S3KeyPrefix")
                .and_then(Value::as_str)
                .map(String::from),
            s3_kms_key_arn: props
                .get("S3KmsKeyArn")
                .and_then(Value::as_str)
                .map(String::from),
            sns_topic_arn: props
                .get("SnsTopicARN")
                .and_then(Value::as_str)
                .map(String::from),
            config_snapshot_delivery_properties: props
                .get("ConfigSnapshotDeliveryProperties")
                .cloned(),
            last_status: "SUCCESS".into(),
        },
    );
    name
}

/// `AWS::Config::ConfigRule` -> physical id (the rule name).
pub fn provision_config_rule(
    accounts: &mut ConfigAccounts,
    account: &str,
    region: &str,
    props: &Value,
) -> String {
    let name = props
        .get("ConfigRuleName")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("config-rule-{}", short_id()));
    let source = props.get("Source").cloned().unwrap_or(Value::Null);
    let acc = accounts.account_mut(account);
    acc.rules.insert(
        name.clone(),
        ConfigRule {
            name: name.clone(),
            arn: format!(
                "arn:aws:config:{region}:{account}:config-rule/config-rule-{}",
                short_id()
            ),
            rule_id: format!("config-rule-{}", short_id()),
            description: props
                .get("Description")
                .and_then(Value::as_str)
                .map(String::from),
            scope: props.get("Scope").cloned(),
            source,
            input_parameters: props.get("InputParameters").map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }),
            maximum_execution_frequency: props
                .get("MaximumExecutionFrequency")
                .and_then(Value::as_str)
                .map(String::from),
            state: "ACTIVE".into(),
            created_by: None,
            evaluation_modes: props.get("EvaluationModes").cloned(),
            last_updated: Utc::now(),
            first_activated_time: None,
            last_successful_evaluation_time: None,
            last_successful_invocation_time: None,
        },
    );
    name
}

/// `AWS::Config::ConfigurationAggregator` -> physical id (the aggregator name).
pub fn provision_aggregator(
    accounts: &mut ConfigAccounts,
    account: &str,
    region: &str,
    props: &Value,
) -> String {
    let name = props
        .get("ConfigurationAggregatorName")
        .and_then(Value::as_str)
        .unwrap_or("aggregator")
        .to_string();
    let now = Utc::now();
    let acc = accounts.account_mut(account);
    acc.aggregators.insert(
        name.clone(),
        ConfigurationAggregator {
            name: name.clone(),
            arn: format!(
                "arn:aws:config:{region}:{account}:config-aggregator/config-aggregator-{}",
                short_id()
            ),
            account_aggregation_sources: props
                .get("AccountAggregationSources")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            organization_aggregation_source: props.get("OrganizationAggregationSource").cloned(),
            creation_time: now,
            last_updated_time: now,
            created_by: None,
        },
    );
    name
}

/// `AWS::Config::AggregationAuthorization` -> physical id (the ARN).
pub fn provision_aggregation_authorization(
    accounts: &mut ConfigAccounts,
    account: &str,
    region: &str,
    props: &Value,
) -> String {
    let authorized_account = props
        .get("AuthorizedAccountId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let authorized_region = props
        .get("AuthorizedAwsRegion")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arn = format!("arn:aws:config:{region}:{account}:aggregation-authorization/{authorized_account}/{authorized_region}");
    let key = format!("{authorized_account}\u{1}{authorized_region}");
    let acc = accounts.account_mut(account);
    acc.aggregation_authorizations.insert(
        key,
        AggregationAuthorization {
            arn: arn.clone(),
            authorized_account_id: authorized_account,
            authorized_aws_region: authorized_region,
            creation_time: Utc::now(),
        },
    );
    arn
}

/// `AWS::Config::ConformancePack` -> physical id (the pack name).
pub fn provision_conformance_pack(
    accounts: &mut ConfigAccounts,
    account: &str,
    region: &str,
    props: &Value,
) -> String {
    let name = props
        .get("ConformancePackName")
        .and_then(Value::as_str)
        .unwrap_or("pack")
        .to_string();
    let acc = accounts.account_mut(account);
    acc.conformance_packs.insert(
        name.clone(),
        ConformancePack {
            name: name.clone(),
            arn: format!(
                "arn:aws:config:{region}:{account}:conformance-pack/{name}-{}",
                short_id()
            ),
            id: format!("conformance-pack-{}", short_id()),
            delivery_s3_bucket: props
                .get("DeliveryS3Bucket")
                .and_then(Value::as_str)
                .map(String::from),
            delivery_s3_key_prefix: props
                .get("DeliveryS3KeyPrefix")
                .and_then(Value::as_str)
                .map(String::from),
            input_parameters: props
                .get("ConformancePackInputParameters")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            template_body: props
                .get("TemplateBody")
                .and_then(Value::as_str)
                .map(String::from),
            template_s3_uri: props
                .get("TemplateS3Uri")
                .and_then(Value::as_str)
                .map(String::from),
            template_ssm_document_details: props.get("TemplateSSMDocumentDetails").cloned(),
            last_update_requested_time: Utc::now(),
            created_by: None,
            rule_names: Vec::new(),
        },
    );
    name
}

/// `AWS::Config::OrganizationConfigRule` -> physical id (the rule name).
pub fn provision_org_config_rule(
    accounts: &mut ConfigAccounts,
    account: &str,
    region: &str,
    props: &Value,
) -> String {
    let name = props
        .get("OrganizationConfigRuleName")
        .and_then(Value::as_str)
        .unwrap_or("org-rule")
        .to_string();
    let acc = accounts.account_mut(account);
    acc.org_rules.insert(
        name.clone(),
        OrganizationConfigRule {
            name: name.clone(),
            arn: format!(
                "arn:aws:config:{region}:{account}:organization-config-rule/{name}-{}",
                short_id()
            ),
            managed_rule_metadata: props.get("OrganizationManagedRuleMetadata").cloned(),
            custom_rule_metadata: props.get("OrganizationCustomRuleMetadata").cloned(),
            custom_policy_rule_metadata: props.get("OrganizationCustomPolicyRuleMetadata").cloned(),
            excluded_accounts: props
                .get("ExcludedAccounts")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            last_update_time: Utc::now(),
        },
    );
    name
}

/// Remove a resource of the given CFN type by physical id.
pub fn deprovision(
    accounts: &mut ConfigAccounts,
    account: &str,
    cfn_type: &str,
    physical_id: &str,
) {
    let acc = accounts.account_mut(account);
    match cfn_type {
        "AWS::Config::ConfigurationRecorder" => {
            acc.recorders.remove(physical_id);
        }
        "AWS::Config::DeliveryChannel" => {
            acc.delivery_channels.remove(physical_id);
        }
        "AWS::Config::ConfigRule" => {
            acc.rules.remove(physical_id);
        }
        "AWS::Config::ConfigurationAggregator" => {
            acc.aggregators.remove(physical_id);
        }
        "AWS::Config::AggregationAuthorization" => {
            acc.aggregation_authorizations
                .retain(|_, v| v.arn != physical_id);
        }
        "AWS::Config::ConformancePack" => {
            acc.conformance_packs.remove(physical_id);
        }
        "AWS::Config::OrganizationConfigRule" => {
            acc.org_rules.remove(physical_id);
        }
        _ => {}
    }
}
