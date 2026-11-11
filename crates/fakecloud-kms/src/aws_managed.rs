//! AWS-managed keys (`alias/aws/<service>`): their key policies and the
//! authorization those policies drive.
//!
//! AWS writes the key policy of every AWS-managed key and nobody can change
//! it (`PutKeyPolicy` is refused, see `KmsService::aws_managed_key_refusal`).
//! The recorded policies (`tests/fixtures/aws-managed-keys/*.json`) grant the
//! cryptographic operations only to principals of the owning account calling
//! *through* the owning service:
//!
//! ```json
//! "Condition": {"StringEquals": {
//!     "kms:CallerAccount": "<account>",
//!     "kms:ViaService": "<service>.<region>.amazonaws.com"}}
//! ```
//!
//! and give the account itself only `Describe*` / `Get*` / `List*` (plus
//! `RevokeGrant`). A direct `Encrypt` on `alias/aws/s3` is therefore refused
//! with `AccessDeniedException` ("... because no resource-based policy allows
//! the kms:Encrypt action"), while S3 encrypting an object under the same key
//! works. fakecloud evaluates the key's own policy for both paths with the IAM
//! policy evaluator: a direct API call carries no `kms:ViaService`, and an
//! internal service call through the KMS hook carries the `kms:ViaService` of
//! the service that makes it (see [`via_service`]).

use std::collections::BTreeMap;

use fakecloud_core::auth::{Principal, PrincipalType};
use fakecloud_iam::evaluator::{
    evaluate_resource_policy_only, EvalRequest, PolicyDocument, RequestContext,
};
use serde_json::{json, Value};

/// The `kms:ViaService` value of a request a service (identified by its
/// service principal, `s3.amazonaws.com`) makes to KMS in `region` on a
/// caller's behalf: the service's regional endpoint,
/// `s3.us-east-1.amazonaws.com` (`.amazonaws.com.cn` in China).
pub fn via_service(service_principal: &str, region: &str) -> String {
    let service = service_principal
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(service_principal);
    format!(
        "{service}.{region}.{}",
        fakecloud_aws::endpoint::dns_suffix_for_region(region)
    )
}

/// The endpoint prefix of the service an `aws/<service>` key belongs to: the
/// one in its policy's `kms:ViaService`. Recorded: `aws/ebs` is used through
/// EC2 (`ec2.<region>.amazonaws.com`).
fn endpoint_prefix(service: &str) -> &str {
    match service {
        "ebs" => "ec2",
        other => other,
    }
}

/// The description AWS gives the AWS-managed key behind `alias/aws/<service>`
/// (recorded for the services in the fixtures; other services keep the
/// generic form).
pub(crate) fn key_description(service: &str) -> String {
    let recorded = match service {
        "acm" => Some("my ACM private keys"),
        "ebs" => Some("my EBS volumes"),
        "lambda" => Some("my Lambda functions"),
        "s3" => Some("my S3 objects"),
        "secretsmanager" => Some("my Secrets Manager data"),
        _ => None,
    };
    match recorded {
        Some(what) => format!("Default key that protects {what} when no other key is defined"),
        None => {
            format!("Default master key that protects aws/{service} when no other key is defined")
        }
    }
}

/// The name a policy `Sid` uses for a service ("Allow access through <name>
/// for all principals in the account that are authorized to use <name>").
/// Recorded for S3, EBS, AWS Lambda and AWS Secrets Manager; the others are
/// the service's short product name.
fn display_name(service: &str) -> String {
    let name = match service {
        "s3" => "S3",
        "ebs" => "EBS",
        "lambda" => "AWS Lambda",
        "secretsmanager" => "AWS Secrets Manager",
        "dynamodb" => "Amazon DynamoDB",
        "sqs" => "Amazon SQS",
        "sns" => "Amazon SNS",
        "ssm" => "AWS Systems Manager",
        "rds" => "Amazon RDS",
        "kinesis" => "Amazon Kinesis",
        "logs" => "Amazon CloudWatch Logs",
        "glue" => "AWS Glue",
        "elasticache" => "Amazon ElastiCache",
        "backup" => "AWS Backup",
        "es" => "Amazon OpenSearch Service",
        "redshift" => "Amazon Redshift",
        "xray" => "AWS X-Ray",
        "elasticfilesystem" => "Amazon EFS",
        "cloudtrail" => "AWS CloudTrail",
        "sagemaker" => "Amazon SageMaker",
        other => return other.to_string(),
    };
    name.to_string()
}

/// The key policy of the AWS-managed key behind `alias/aws/<service>` in
/// `account` and `region`. ACM, EBS, Lambda, S3 and Secrets Manager are the
/// policies recorded from AWS; every other service gets the recorded EBS /
/// Lambda template (cryptographic use through the service, metadata access
/// for the account) with its own name and `kms:ViaService`.
pub(crate) fn key_policy(service: &str, account: &str, region: &str) -> String {
    let partition = fakecloud_aws::arn::partition_for(region);
    let root = format!("arn:{partition}:iam::{account}:root");
    let via = via_service(endpoint_prefix(service), region);
    let through_service = |actions: Value| {
        let name = display_name(service);
        json!({
            "Sid": format!("Allow access through {name} for all principals in the account that are authorized to use {name}"),
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": actions,
            "Resource": "*",
            "Condition": {"StringEquals": {
                "kms:CallerAccount": account,
                "kms:ViaService": via,
            }}
        })
    };
    let metadata = |actions: Value| {
        json!({
            "Sid": "Allow direct access to key metadata to the account",
            "Effect": "Allow",
            "Principal": {"AWS": root},
            "Action": actions,
            "Resource": "*"
        })
    };
    let full_metadata = json!(["kms:Describe*", "kms:Get*", "kms:List*", "kms:RevokeGrant"]);
    let policy = match service {
        "lambda" => lambda_policy(account, region, &root, &via, full_metadata),
        "acm" => acm_policy(account, &via),
        "s3" => json!({
            "Version": "2012-10-17",
            "Id": "auto-s3-2",
            "Statement": [
                through_service(json!(["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"])),
                metadata(json!(["kms:Describe*", "kms:Get*", "kms:List*"])),
            ]
        }),
        "secretsmanager" => {
            let name = display_name(service);
            let suffix = fakecloud_aws::endpoint::dns_suffix_for_region(region);
            json!({
                "Version": "2012-10-17",
                "Id": "auto-secretsmanager-2",
                "Statement": [
                    through_service(json!(["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:CreateGrant", "kms:DescribeKey"])),
                    {
                        "Sid": format!("Allow access through {name} for all principals in the account that are authorized to use {name}"),
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "kms:GenerateDataKey*",
                        "Resource": "*",
                        "Condition": {
                            "StringEquals": {"kms:CallerAccount": account},
                            "StringLike": {"kms:ViaService": format!("secretsmanager.*.{suffix}")}
                        }
                    },
                    metadata(full_metadata),
                ]
            })
        }
        // `ebs` is recorded with exactly this template (`auto-ebs-2`).
        _ => json!({
            "Version": "2012-10-17",
            "Id": format!("auto-{service}-2"),
            "Statement": [
                through_service(json!(["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:CreateGrant", "kms:DescribeKey"])),
                metadata(full_metadata),
            ]
        }),
    };
    policy.to_string()
}

/// The recorded `aws/lambda` key policy: account principals use it through
/// Lambda, may decrypt their functions' ciphertexts directly, and the Lambda
/// service principal may only derive data keys for the account's functions.
fn lambda_policy(account: &str, region: &str, root: &str, via: &str, metadata: Value) -> Value {
    let functions = format!("arn:*:lambda:{region}:{account}:function:*");
    let services = json!([
        format!("lambda.{region}.amazonaws.com"),
        "lambda.amazonaws.com"
    ]);
    json!({
        "Version": "2012-10-17",
        "Id": "auto-awslambda-3",
        "Statement": [
            {
                "Sid": "Allow access through AWS Lambda for all principals in the account that are authorized to use AWS Lambda",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:CreateGrant", "kms:DescribeKey"],
                "Resource": "*",
                "Condition": {"StringEquals": {
                    "kms:ViaService": via,
                    "kms:CallerAccount": account,
                }}
            },
            {
                "Sid": "Allow all principals in the account to decrypt ciphertexts for Lambda functions in the account",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "kms:Decrypt",
                "Resource": "*",
                "Condition": {
                    "StringEquals": {"kms:CallerAccount": account},
                    "ArnLike": {"kms:EncryptionContext:aws:lambda:FunctionArn": functions}
                }
            },
            {
                "Sid": "Allow Lambda service access",
                "Effect": "Allow",
                "Principal": {"Service": services},
                "Action": "kms:GenerateDataKey",
                "Resource": "*",
                "Condition": {"ArnLike": {"kms:EncryptionContext:aws:lambda:FunctionArn": functions}}
            },
            {
                "Sid": "Allow Lambda to directly describe the key and list its grants",
                "Effect": "Allow",
                "Principal": {"Service": services},
                "Action": ["kms:Describe*", "kms:Get*", "kms:List*"],
                "Resource": "*"
            },
            {
                "Sid": "Allow direct access to key metadata to the account",
                "Effect": "Allow",
                "Principal": {"AWS": root},
                "Action": metadata,
                "Resource": "*"
            }
        ]
    })
}

/// The recorded `aws/acm` key policy: ACM only creates grants and decrypts
/// through itself, and re-encryption to another key is denied.
fn acm_policy(account: &str, via: &str) -> Value {
    let grant = |sid: &str, operations: Value| {
        json!({
            "Sid": sid,
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "kms:CreateGrant",
            "Resource": "*",
            "Condition": {
                "StringEquals": {"kms:ViaService": via, "kms:CallerAccount": account},
                "ForAllValues:StringEquals": {"kms:GrantOperations": operations},
                "Bool": {"kms:GrantIsForAWSResource": "true"}
            }
        })
    };
    json!({
        "Version": "2012-10-17",
        "Id": "auto-acm-4",
        "Statement": [
            grant("Allow creation of decryption grants", json!("Decrypt")),
            grant("Allow creation of encryption grant", json!(["Encrypt", "ReEncryptFrom", "ReEncryptTo"])),
            {
                "Sid": "Allowed operations for the key owner",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["kms:DescribeKey", "kms:ListGrants", "kms:RevokeGrant", "kms:GetKeyPolicy"],
                "Resource": "*",
                "Condition": {"StringEquals": {"kms:CallerAccount": account}}
            },
            {
                "Sid": "Deny re-encryption to any other key",
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "kms:ReEncrypt*",
                "Resource": "*",
                "Condition": {"Bool": {"kms:ReEncryptOnSameKey": "false"}}
            },
            {
                "Sid": "Allow decrypt",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "kms:Decrypt",
                "Resource": "*",
                "Condition": {"StringEquals": {"kms:ViaService": via, "kms:CallerAccount": account}}
            }
        ]
    })
}

/// The `aws/<service>` an AWS-managed alias names (`alias/aws/s3` -> `s3`).
pub(crate) fn alias_service(alias_name: &str) -> Option<&str> {
    alias_name
        .strip_prefix("alias/aws/")
        .filter(|s| !s.is_empty())
}

/// A request's `EncryptionContext` object as a string map (non-string
/// values, which KMS rejects elsewhere, are skipped).
pub(crate) fn encryption_context_map(ec: &Value) -> BTreeMap<String, String> {
    ec.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Who asks KMS to use an AWS-managed key, which decides the `kms:ViaService`
/// and `kms:CallerAccount` its key policy sees.
pub(crate) enum KeyCaller<'a> {
    /// A direct KMS API call by `principal` (from `account`): no
    /// `kms:ViaService`.
    Direct {
        principal: &'a Principal,
        account: &'a str,
    },
    /// A fakecloud service (`service_principal`, `s3.amazonaws.com`) using
    /// the key in `region` on behalf of a caller in `account`, as AWS services
    /// do with the caller's forwarded credentials: `kms:ViaService` is the
    /// service's regional endpoint.
    ViaService {
        service_principal: &'a str,
        region: &'a str,
        account: &'a str,
    },
}

/// The principal of a request that carries no resolved identity: the
/// account's root user, which is who an unsigned / IAM-off request acts as.
pub(crate) fn account_root(account: &str, region: &str) -> Principal {
    let partition = fakecloud_aws::arn::partition_for(region);
    Principal {
        arn: format!("arn:{partition}:iam::{account}:root"),
        user_id: account.to_string(),
        account_id: account.to_string(),
        principal_type: PrincipalType::Root,
        source_identity: None,
        tags: None,
    }
}

/// Whether the key policy `policy` of the AWS-managed key `key_arn` lets
/// `caller` perform `action` (`kms:Encrypt`, ...) with `encryption_context`.
/// `extra` carries operation-specific keys (`kms:ReEncryptOnSameKey`).
pub(crate) fn policy_allows(
    policy: &str,
    key_arn: &str,
    caller: &KeyCaller<'_>,
    action: &str,
    encryption_context: &BTreeMap<String, String>,
    extra: &[(&str, &str)],
) -> bool {
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let (principal, account) = match caller {
        KeyCaller::Direct { principal, account } => ((*principal).clone(), *account),
        KeyCaller::ViaService {
            service_principal,
            region,
            account,
        } => {
            keys.insert(
                "kms:ViaService".to_string(),
                vec![via_service(service_principal, region)],
            );
            (account_root(account, region), *account)
        }
    };
    keys.insert("kms:CallerAccount".to_string(), vec![account.to_string()]);
    for (k, v) in encryption_context {
        keys.insert(format!("kms:EncryptionContext:{k}"), vec![v.clone()]);
    }
    keys.insert(
        "kms:EncryptionContextKeys".to_string(),
        encryption_context.keys().cloned().collect(),
    );
    for (k, v) in extra {
        keys.insert((*k).to_string(), vec![(*v).to_string()]);
    }
    let request = EvalRequest {
        principal: &principal,
        action: action.to_string(),
        resource: key_arn.to_string(),
        context: RequestContext {
            service_keys: keys,
            ..Default::default()
        },
    };
    evaluate_resource_policy_only(&PolicyDocument::parse(policy), &request).is_allow()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: &str = "123456789012";
    const RECORDED: &[&str] = &["acm", "ebs", "lambda", "s3", "secretsmanager"];

    fn fixture(service: &str) -> Value {
        let path = format!(
            "{}/tests/fixtures/aws-managed-keys/{service}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    /// Statement order matters (it is what GetKeyPolicy returns); key order
    /// inside JSON objects and the order of a Service principal list do not.
    fn normalize(mut v: Value) -> Value {
        for st in v["Statement"].as_array_mut().unwrap() {
            if let Some(list) = st["Principal"]["Service"].as_array_mut() {
                list.sort_by_key(|s| s.as_str().unwrap_or_default().to_string());
            }
        }
        v
    }

    #[test]
    fn recorded_policies_match_the_fixtures() {
        for service in RECORDED {
            let recorded = fixture(service);
            let ours: Value =
                serde_json::from_str(&key_policy(service, ACCOUNT, "us-east-1")).unwrap();
            assert_eq!(
                normalize(ours),
                normalize(recorded["policy"].clone()),
                "aws/{service}"
            );
            assert_eq!(
                key_description(service),
                recorded["describe_key"]["Description"],
                "aws/{service}"
            );
        }
    }

    #[test]
    fn via_service_is_the_regional_endpoint() {
        assert_eq!(
            via_service("s3.amazonaws.com", "us-east-1"),
            "s3.us-east-1.amazonaws.com"
        );
        assert_eq!(
            via_service("sqs.amazonaws.com", "cn-north-1"),
            "sqs.cn-north-1.amazonaws.com.cn"
        );
    }

    fn arn(service: &str) -> String {
        format!("arn:aws:kms:us-east-1:{ACCOUNT}:key/{service}")
    }

    fn direct_allows(service: &str, action: &str, ctx: &[(&str, &str)]) -> bool {
        let principal = account_root(ACCOUNT, "us-east-1");
        let ctx = ctx
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        policy_allows(
            &key_policy(service, ACCOUNT, "us-east-1"),
            &arn(service),
            &KeyCaller::Direct {
                principal: &principal,
                account: ACCOUNT,
            },
            action,
            &ctx,
            &[],
        )
    }

    fn via_allows(service: &str, via: &str, account: &str, action: &str) -> bool {
        policy_allows(
            &key_policy(service, ACCOUNT, "us-east-1"),
            &arn(service),
            &KeyCaller::ViaService {
                service_principal: via,
                region: "us-east-1",
                account,
            },
            action,
            &BTreeMap::new(),
            &[],
        )
    }

    /// Direct cryptographic use is refused on every AWS-managed key; key
    /// metadata stays readable.
    #[test]
    fn direct_crypto_is_refused_and_metadata_allowed() {
        for service in ["s3", "secretsmanager", "ebs", "lambda", "sqs", "dynamodb"] {
            for action in [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:GenerateDataKey",
                "kms:GenerateDataKeyWithoutPlaintext",
                "kms:ReEncryptFrom",
                "kms:ReEncryptTo",
            ] {
                assert!(!direct_allows(service, action, &[]), "{service} {action}");
            }
            assert!(direct_allows(service, "kms:DescribeKey", &[]), "{service}");
            assert!(direct_allows(service, "kms:GetKeyPolicy", &[]), "{service}");
        }
    }

    /// The recorded `aws/lambda` exception: an account principal decrypts a
    /// ciphertext bound to one of its functions directly.
    #[test]
    fn lambda_key_decrypts_function_ciphertexts_directly() {
        let fn_arn = format!("arn:aws:lambda:us-east-1:{ACCOUNT}:function:f");
        let ctx = [("aws:lambda:FunctionArn", fn_arn.as_str())];
        assert!(direct_allows("lambda", "kms:Decrypt", &ctx));
        assert!(!direct_allows("lambda", "kms:Encrypt", &ctx));
        let other = [(
            "aws:lambda:FunctionArn",
            "arn:aws:lambda:us-east-1:999999999999:function:f",
        )];
        assert!(!direct_allows("lambda", "kms:Decrypt", &other));
    }

    /// The owning service uses its key; another service and another account
    /// do not.
    #[test]
    fn crypto_through_the_owning_service_only() {
        assert!(via_allows(
            "s3",
            "s3.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
        ));
        assert!(via_allows("s3", "s3.amazonaws.com", ACCOUNT, "kms:Decrypt"));
        assert!(via_allows(
            "ebs",
            "ec2.amazonaws.com",
            ACCOUNT,
            "kms:Decrypt"
        ));
        assert!(via_allows(
            "sqs",
            "sqs.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
        ));
        assert!(via_allows(
            "secretsmanager",
            "secretsmanager.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
        ));
        assert!(!via_allows(
            "s3",
            "sqs.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
        ));
        assert!(!via_allows(
            "s3",
            "s3.amazonaws.com",
            "999999999999",
            "kms:Decrypt"
        ));
        // ACM only decrypts through itself; it never encrypts directly.
        assert!(via_allows(
            "acm",
            "acm.amazonaws.com",
            ACCOUNT,
            "kms:Decrypt"
        ));
        assert!(!via_allows(
            "acm",
            "acm.amazonaws.com",
            ACCOUNT,
            "kms:Encrypt"
        ));
    }
}
