//! Service Quotas checks for the resources CloudFormation provisions by
//! writing service state directly. On AWS a stack creates them through the
//! service APIs, so a quota the user enforces fails the resource the same way
//! the API call would; these helpers resolve the same enforced limits the
//! services do (before any service lock is taken) and render the refusal as
//! the resource's failure reason.

use fakecloud_core::service::AwsServiceError;
use fakecloud_iam::quota::IamQuota;

use super::ResourceProvisioner;

/// The status reason of a resource a quota refused: the service's error
/// message, code and status, as a handler reports a failed API call.
pub(super) fn refusal(service: &str, err: AwsServiceError) -> String {
    fakecloud_core::quota::refusal_reason(service, &err)
}

/// An IAM refusal as a resource failure.
pub(super) fn iam_refusal(err: AwsServiceError) -> String {
    refusal("Iam", err)
}

impl ResourceProvisioner {
    /// The limit of an IAM quota when it is enforced for this stack's account.
    pub(super) fn iam_limit(&self, quota: IamQuota) -> Option<usize> {
        fakecloud_iam::quota::enforced_limit(
            self.quota_provider.as_ref(),
            &self.account_id,
            &self.region,
            quota,
        )
    }

    /// The enforced DynamoDB table limit of `region`.
    pub(super) fn dynamodb_table_limit(&self, region: &str) -> Option<usize> {
        fakecloud_dynamodb::quota::enforced_table_limit(
            self.quota_provider.as_ref(),
            &self.account_id,
            region,
        )
    }

    /// The enforced KMS customer managed key limit of `region`.
    pub(super) fn kms_key_limit(&self, region: &str) -> Option<usize> {
        fakecloud_kms::quota::enforced_key_limit(
            self.quota_provider.as_ref(),
            &self.account_id,
            region,
        )
    }

    /// The enforced S3 general purpose bucket limit of this account.
    pub(super) fn s3_bucket_limit(&self) -> Option<usize> {
        fakecloud_s3::quota::enforced_bucket_limit(
            self.quota_provider.as_ref(),
            &self.account_id,
            &self.region,
        )
    }

    /// The enforced Lambda code storage limit of this stack's region, in
    /// bytes.
    pub(super) fn lambda_storage_limit(&self) -> Option<i64> {
        fakecloud_lambda::quota::enforced_storage_limit(
            self.quota_provider.as_ref(),
            &self.account_id,
            &self.region,
        )
    }

    /// The applied Lambda concurrency limit, when Service Quotas is attached.
    pub(super) fn lambda_concurrency_quota(&self) -> Option<f64> {
        self.quota_provider.as_ref().and_then(|p| {
            p.applied_value(
                &self.account_id,
                &self.region,
                fakecloud_lambda::quota::SERVICE_CODE,
                fakecloud_lambda::quota::CONCURRENT_EXECUTIONS,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_provisioner, make_resource};
    use super::*;
    use std::sync::Arc;

    fn enforcing(quotas: fakecloud_core::quota::FixedQuotas) -> ResourceProvisioner {
        let mut prov = make_provisioner();
        prov.quota_provider = Some(Arc::new(quotas));
        prov
    }

    const TRUST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

    #[test]
    fn stack_resources_are_not_limited_without_a_provider() {
        let prov = make_provisioner();
        for i in 0..3 {
            prov.create_resource(&make_resource(
                "AWS::S3::Bucket",
                &format!("B{i}"),
                serde_json::json!({ "BucketName": format!("free-bucket-{i}") }),
            ))
            .expect("bucket");
        }
    }

    #[test]
    fn an_enforced_iam_quota_fails_the_stack_resource() {
        // Two seeded service-linked roles plus one created: a limit of 3.
        let prov = enforcing(
            fakecloud_core::quota::FixedQuotas::default()
                .with("iam", "L-FE177D64", 3.0)
                .with("iam", "L-0DA4ABF3", 1.0),
        );
        prov.create_resource(&make_resource(
            "AWS::IAM::Role",
            "R1",
            serde_json::json!({ "RoleName": "r1", "AssumeRolePolicyDocument": TRUST }),
        ))
        .expect("first role fits");
        let err = prov
            .create_resource(&make_resource(
                "AWS::IAM::Role",
                "R2",
                serde_json::json!({ "RoleName": "r2", "AssumeRolePolicyDocument": TRUST }),
            ))
            .expect_err("second role is over the quota");
        assert!(
            err.contains("Cannot exceed quota for RolesPerAccount: 3"),
            "{err}"
        );
        assert!(err.contains("Error Code: LimitExceeded"), "{err}");

        // A managed policy attaching to a role already at its per-role limit.
        let prov =
            enforcing(fakecloud_core::quota::FixedQuotas::default().with("iam", "L-0DA4ABF3", 1.0));
        prov.create_resource(&make_resource(
            "AWS::IAM::Role",
            "R",
            serde_json::json!({
                "RoleName": "full",
                "AssumeRolePolicyDocument": TRUST,
                "ManagedPolicyArns": ["arn:aws:iam::aws:policy/ReadOnlyAccess"],
            }),
        ))
        .expect("one managed policy fits");
        let err = prov
            .create_resource(&make_resource(
                "AWS::IAM::ManagedPolicy",
                "P",
                serde_json::json!({
                    "ManagedPolicyName": "extra",
                    "PolicyDocument": {"Version": "2012-10-17", "Statement": []},
                    "Roles": ["full"],
                }),
            ))
            .expect_err("the role has no room for another managed policy");
        assert!(err.contains("PoliciesPerRole: 1"), "{err}");
    }

    #[test]
    fn enforced_table_bucket_key_and_storage_quotas_fail_the_stack_resource() {
        let prov = enforcing(
            fakecloud_core::quota::FixedQuotas::default()
                .with("dynamodb", "L-F98FE922", 0.0)
                .with("s3", "L-DC2B2D3D", 0.0)
                .with("kms", "L-C2F1777E", 0.0)
                .with("lambda", "L-2ACBD22F", 0.0),
        );
        let table = prov
            .create_resource(&make_resource(
                "AWS::DynamoDB::Table",
                "T",
                serde_json::json!({
                    "TableName": "quota-table",
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                }),
            ))
            .expect_err("table refused");
        assert!(table.contains("LimitExceededException"), "{table}");

        let bucket = prov
            .create_resource(&make_resource(
                "AWS::S3::Bucket",
                "B",
                serde_json::json!({ "BucketName": "quota-bucket" }),
            ))
            .expect_err("bucket refused");
        assert!(bucket.contains("TooManyBuckets"), "{bucket}");

        let key = prov
            .create_resource(&make_resource("AWS::KMS::Key", "K", serde_json::json!({})))
            .expect_err("key refused");
        assert!(key.contains("LimitExceededException"), "{key}");

        let layer = prov
            .create_resource(&make_resource(
                "AWS::Lambda::LayerVersion",
                "L",
                serde_json::json!({
                    "LayerName": "quota-layer",
                    "Content": {"ZipFile": "YWJj"},
                }),
            ))
            .expect_err("layer refused");
        assert!(layer.contains("CodeStorageExceededException"), "{layer}");
    }

    #[test]
    fn refusal_carries_the_error_code_and_status() {
        let err = fakecloud_iam::quota::limit_exceeded(IamQuota::Roles, 3);
        assert_eq!(
            refusal("Iam", err),
            "Cannot exceed quota for RolesPerAccount: 3 (Service: Iam, Status Code: 409, \
             Error Code: LimitExceeded)"
        );
    }

    fn global_table(replicas: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "TableName": "glob",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"},
            "Replicas": replicas.iter().map(|r| serde_json::json!({"Region": r})).collect::<Vec<_>>(),
        })
    }

    /// A table limit of 1 in every region, with eu-west-1 already full.
    fn full_west() -> ResourceProvisioner {
        full_west_holding("occupant")
    }

    /// Like [`full_west`], with eu-west-1's one table named `name`.
    fn full_west_holding(name: &str) -> ResourceProvisioner {
        let prov = enforcing(fakecloud_core::quota::FixedQuotas::default().with(
            "dynamodb",
            "L-F98FE922",
            1.0,
        ));
        let mut accounts = prov.dynamodb_state.write();
        let west = accounts.regional_mut(&prov.account_id, "eu-west-1");
        west.tables.insert(
            name.to_string(),
            fakecloud_dynamodb::DynamoTable::new(
                name.to_string(),
                format!("arn:aws:dynamodb:eu-west-1:123456789012:table/{name}"),
                "id".to_string(),
                Vec::new(),
                Vec::new(),
                fakecloud_dynamodb::ProvisionedThroughput {
                    read_capacity_units: 0,
                    write_capacity_units: 0,
                },
                "PAY_PER_REQUEST".to_string(),
                chrono::Utc::now(),
            ),
        );
        drop(accounts);
        prov
    }

    fn local_tables(prov: &ResourceProvisioner) -> Vec<String> {
        prov.dynamodb_state
            .read()
            .regional(&prov.account_id, &prov.region)
            .map(|s| s.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[test]
    fn a_refused_global_table_replica_leaves_no_local_table() {
        let prov = full_west();
        let region = prov.region.clone();
        let err = prov
            .create_resource(&make_resource(
                "AWS::DynamoDB::GlobalTable",
                "G",
                global_table(&[&region, "eu-west-1"]),
            ))
            .expect_err("the eu-west-1 replica is over the quota");
        assert!(err.contains("LimitExceededException"), "{err}");
        assert!(local_tables(&prov).is_empty(), "no orphaned local table");
    }

    #[test]
    fn a_refused_replica_addition_leaves_the_global_table_unchanged() {
        let prov = full_west();
        let region = prov.region.clone();
        let created = prov
            .create_resource(&make_resource(
                "AWS::DynamoDB::GlobalTable",
                "G",
                global_table(&[&region]),
            ))
            .expect("a single-region global table fits");
        let mut props = global_table(&[&region, "eu-west-1"]);
        props["DeletionProtectionEnabled"] = serde_json::json!(true);
        let err = prov
            .update_resource(
                &created,
                &make_resource("AWS::DynamoDB::GlobalTable", "G", props),
            )
            .expect_err("the eu-west-1 replica is over the quota");
        assert!(err.contains("LimitExceededException"), "{err}");
        let accounts = prov.dynamodb_state.read();
        let table = &accounts.regional(&prov.account_id, &region).unwrap().tables["glob"];
        assert!(
            !table.deletion_protection_enabled,
            "local table not updated"
        );
        assert!(table.replica_regions.is_empty());
    }

    fn function(code: &str) -> serde_json::Value {
        serde_json::json!({
            "FunctionName": "sized",
            "Runtime": "python3.12",
            "Handler": "index.handler",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Code": {"ZipFile": code},
        })
    }

    #[test]
    fn a_refused_publish_to_latest_published_rolls_the_function_back() {
        // 10 bytes: the 8-byte $LATEST fits, its $LATEST.PUBLISHED copy does not.
        let prov = enforcing(fakecloud_core::quota::FixedQuotas::default().with(
            "lambda",
            "L-2ACBD22F",
            10.5 / (1024.0 * 1024.0 * 1024.0),
        ));
        prov.create_resource(&make_resource(
            "AWS::Lambda::CapacityProvider",
            "Cp",
            serde_json::json!({
                "CapacityProviderName": "cp",
                "VpcConfig": {"SubnetIds": ["subnet-1"], "SecurityGroupIds": ["sg-1"]},
                "PermissionsConfig": {
                    "CapacityProviderOperatorRoleArn": "arn:aws:iam::123456789012:role/op"
                }
            }),
        ))
        .unwrap();
        let mut props = function("eight888");
        props["Runtime"] = serde_json::json!("python3.13");
        props["CapacityProviderConfig"] = serde_json::json!({
            "LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp"
            }
        });
        props["PublishToLatestPublished"] = serde_json::json!(true);
        let err = prov
            .create_resource(&make_resource("AWS::Lambda::Function", "F", props))
            .expect_err("the $LATEST.PUBLISHED copy does not fit");
        assert!(err.contains("CodeStorageExceededException"), "{err}");
        let accounts = prov.lambda_state.read();
        let state = accounts.regional(&prov.account_id, &prov.region).unwrap();
        assert!(!state.functions.contains_key("sized"), "rolled back");
        assert!(!state.latest_published.contains_key("sized"));
    }

    #[test]
    fn a_refused_function_code_update_leaves_the_function_unchanged() {
        // 10 bytes of code storage, in the quota's gigabytes.
        let prov = enforcing(fakecloud_core::quota::FixedQuotas::default().with(
            "lambda",
            "L-2ACBD22F",
            10.5 / (1024.0 * 1024.0 * 1024.0),
        ));
        let created = prov
            .create_resource(&make_resource(
                "AWS::Lambda::Function",
                "F",
                function("eight888"),
            ))
            .expect("8 bytes fit");
        let err = prov
            .update_resource(
                &created,
                &make_resource("AWS::Lambda::Function", "F", function("eleven11111")),
            )
            .expect_err("11 bytes do not fit");
        assert!(err.contains("CodeStorageExceededException"), "{err}");
        let accounts = prov.lambda_state.read();
        let func = &accounts
            .regional(&prov.account_id, &prov.region)
            .unwrap()
            .functions["sized"];
        assert_eq!(func.code_size, 8, "$LATEST keeps its package");
    }

    #[test]
    fn a_replica_region_holding_the_declared_name_is_not_a_new_table_and_failure_rolls_back() {
        // eu-west-1 is at its limit of 1 with a table already named `glob`:
        // that is not a new table, so the quota does not refuse it, and the
        // replica step then fails (the region already holds the name). The
        // local table created before that step is rolled back.
        let prov = full_west_holding("glob");
        let region = prov.region.clone();
        let err = prov
            .create_resource(&make_resource(
                "AWS::DynamoDB::GlobalTable",
                "G",
                global_table(&[&region, "eu-west-1"]),
            ))
            .expect_err("the replica region already holds the table");
        assert!(!err.contains("LimitExceededException"), "{err}");
        assert!(local_tables(&prov).is_empty(), "local table rolled back");
    }
}
