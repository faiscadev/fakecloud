//! AWS-managed keys (`alias/aws/<service>`): which exist, their key
//! policies, and the authorization those policies drive.
//!
//! AWS writes the key policy of every AWS-managed key and nobody can change
//! it (`PutKeyPolicy` is refused, see `KmsService::aws_managed_key_refusal`).
//! The policies (one per AWS-managed key, recorded from a real account and vendored in
//! `data/aws_managed_keys.json` by `scripts/gen-kms-aws-managed-keys.py`)
//! grant the cryptographic operations only to principals of the owning
//! account calling *through* the owning service:
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
//! the service that makes it (see [`via_service`]), or is made as the service
//! principal itself (SES).
//!
//! Only the services in the catalog have an AWS-managed key: AWS answers
//! `NotFoundException` ("Alias ... is not found.") for `alias/aws/logs` or
//! any other name. A catalog key exists from the first time it is used,
//! directly or by its service (AWS creates it then; it is free and owned by
//! AWS), and the [`predefined`] aliases are listed by `ListAliases` before
//! that, without a target key.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::hook::CallerFacts;
use fakecloud_core::auth::{InternalCaller, Principal, PrincipalType};
use fakecloud_iam::evaluator::{
    evaluate_resource_policy_only, Decision, EvalRequest, PolicyDocument, RequestContext,
};
use serde_json::Value;

/// One catalog entry: the recorded key description and the key policy text
/// exactly as `GetKeyPolicy` returned it.
#[derive(serde::Deserialize)]
struct CatalogKey {
    description: String,
    policy: String,
}

#[derive(serde::Deserialize)]
struct Catalog {
    recorded_region: String,
    recorded_account: String,
    predefined: Vec<String>,
    keys: BTreeMap<String, CatalogKey>,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("../data/aws_managed_keys.json"))
            .expect("vendored AWS-managed key catalog is valid JSON")
    })
}

/// Whether AWS has an AWS-managed key for `service` (`alias/aws/<service>`).
pub fn is_aws_managed_service(service: &str) -> bool {
    catalog().keys.contains_key(service)
}

/// The services whose `alias/aws/<service>` alias every account lists before
/// the service first uses it (without a `TargetKeyId`).
pub fn predefined() -> impl Iterator<Item = &'static str> {
    catalog().predefined.iter().map(String::as_str)
}

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

/// The description AWS gives the AWS-managed key behind `alias/aws/<service>`.
pub(crate) fn key_description(service: &str) -> Option<String> {
    catalog().keys.get(service).map(|k| k.description.clone())
}

/// The key policy of the AWS-managed key behind `alias/aws/<service>` in
/// `account` and `region`: AWS's recorded text with the account, region and
/// partition substituted (and the partition's DNS suffix in the
/// `kms:ViaService` endpoints).
pub(crate) fn key_policy(service: &str, account: &str, region: &str) -> Option<String> {
    let cat = catalog();
    let text = &cat.keys.get(service)?.policy;
    let partition = fakecloud_aws::arn::partition_for(region);
    let suffix = fakecloud_aws::endpoint::dns_suffix_for_region(region);
    let lines: Vec<String> = text
        .split('\n')
        .map(|line| {
            let mut line = line
                .replace(&cat.recorded_account, account)
                .replace(&cat.recorded_region, region)
                .replace("\"arn:aws:", &format!("\"arn:{partition}:"));
            if line.contains("\"kms:ViaService\"") && suffix != "amazonaws.com" {
                line = line.replace(".amazonaws.com\"", &format!(".{suffix}\""));
            }
            line
        })
        .collect();
    Some(lines.join("\n"))
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
    /// service's regional endpoint. The key policy sees the calling identity
    /// (`caller_arn`, an IAM user or assumed-role session of `account`) when
    /// it is known, else the account root.
    ViaService {
        service_principal: &'a str,
        region: &'a str,
        account: &'a str,
        caller_arn: Option<&'a str>,
        /// What IAM and Organizations know about `caller_arn` (its role's
        /// IAM ARN with path, `aws:userid`, tags, organization).
        facts: Option<&'a crate::hook::CallerFacts>,
    },
    /// A service principal acting itself (SES writing received mail to S3)
    /// for its resource `source_arn` owned by `source_account`. Only a
    /// `"Service"` principal statement can match it. AWS-managed key
    /// policies name SES by its regional principal
    /// (`ses.us-east-1.amazonaws.com`) while SES documents customer key
    /// policies with the global one (`ses.amazonaws.com`); the service's own
    /// call satisfies either, so both forms are tried.
    Service {
        service_principal: &'a str,
        region: &'a str,
        source_arn: &'a str,
        source_account: &'a str,
        /// The service calling KMS for the principal (SQS for a message SNS
        /// delivers), which sets `kms:ViaService`.
        via_service: Option<&'a str>,
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

/// The account of the calling identity `arn`, `fallback` when the ARN names
/// none.
fn caller_account<'a>(arn: &'a str, fallback: &'a str) -> &'a str {
    fakecloud_aws::arn::account_of(arn).unwrap_or(fallback)
}

/// The principals a key policy can name for the calling identity `arn` of
/// `account`: the identity itself, and for an assumed-role session
/// (`arn:<p>:sts::<account>:assumed-role/<role>/<session>`) also its role,
/// which a policy names to grant every session of the role: `role_arn`, the
/// role's IAM ARN with its path, when known, else
/// `arn:<p>:iam::<account>:role/<role>`. An account-root or bare-account
/// principal in the policy matches either by account.
fn caller_principals(arn: &str, role_arn: Option<&str>, account: &str) -> Vec<Principal> {
    let principal = |arn: String, principal_type: PrincipalType| Principal {
        arn,
        user_id: account.to_string(),
        account_id: account.to_string(),
        principal_type,
        source_identity: None,
        tags: None,
    };
    let mut principals = vec![principal(arn.to_string(), PrincipalType::from_arn(arn))];
    if let Some((prefix, rest)) = arn.split_once(":assumed-role/") {
        let role = rest.split('/').next().unwrap_or(rest);
        let partition = prefix
            .strip_prefix("arn:")
            .and_then(|p| p.split(':').next())
            .unwrap_or("aws");
        let role_arn = role_arn
            .map(str::to_string)
            .unwrap_or_else(|| format!("arn:{partition}:iam::{account}:role/{role}"));
        principals.push(principal(role_arn, PrincipalType::Unknown));
    }
    principals
}

/// The key policy `policy` of the key `key_arn`'s decision on `caller`
/// performing `action` (`kms:Encrypt`, ...) with `encryption_context`:
/// an explicit Deny, an Allow, or neither (implicit deny, which a grant may
/// still turn into an allow). `extra` carries operation-specific keys
/// (`kms:ReEncryptOnSameKey`).
pub(crate) fn policy_decision(
    policy: &str,
    key_arn: &str,
    caller: &KeyCaller<'_>,
    action: &str,
    encryption_context: &BTreeMap<String, String>,
    extra: &[(&str, &str)],
) -> Decision {
    // Forms that can only deny: when an assumed-role caller's role path is
    // unknown (the role is not in fakecloud's IAM, or was deleted), a Deny
    // naming that role under any path applies, failing closed, while an
    // Allow still needs the exact role ARN.
    let mut deny_only: Vec<Principal> = Vec::new();
    if let KeyCaller::ViaService {
        account,
        caller_arn: Some(arn),
        facts,
        ..
    } = caller
    {
        let account = caller_account(arn, account);
        let role_known = facts.is_some_and(|f| f.role_arn.is_some());
        let role = arn
            .split_once(":assumed-role/")
            .and_then(|(_, rest)| rest.split('/').next());
        if let (false, Some(role)) = (role_known, role) {
            deny_only = pathed_role_principals(policy, account, role)
                .into_iter()
                .map(|role_arn| Principal {
                    arn: role_arn,
                    user_id: account.to_string(),
                    account_id: account.to_string(),
                    principal_type: PrincipalType::Unknown,
                    source_identity: None,
                    tags: None,
                })
                .collect();
        }
    }
    let context = request_context(caller, encryption_context, extra);
    let principals: Vec<Principal> = match caller {
        KeyCaller::Direct { principal, .. } => vec![(*principal).clone()],
        KeyCaller::ViaService {
            region,
            account,
            caller_arn,
            facts,
            ..
        } => {
            let account = caller_arn.map_or(*account, |arn| caller_account(arn, account));
            match caller_arn {
                Some(arn) => {
                    caller_principals(arn, facts.and_then(|f| f.role_arn.as_deref()), account)
                }
                None => vec![account_root(account, region)],
            }
        }
        KeyCaller::Service {
            service_principal,
            region,
            source_arn,
            source_account,
            ..
        } => {
            let name = service_principal.split('.').next().unwrap_or_default();
            let suffix = fakecloud_aws::endpoint::dns_suffix_for_region(region);
            let mut forms = vec![service_principal.to_string()];
            for alt in [
                format!("{name}.{region}.amazonaws.com"),
                format!("{name}.amazonaws.com"),
                format!("{name}.{region}.{suffix}"),
            ] {
                if !forms.contains(&alt) {
                    forms.push(alt);
                }
            }
            forms
                .into_iter()
                .map(|service| {
                    InternalCaller::Service {
                        service,
                        source_arn: source_arn.to_string(),
                        source_account: source_account.to_string(),
                    }
                    .principal()
                })
                .collect()
        }
    };
    let document = PolicyDocument::parse(policy);
    // One request whose principal is every form of the caller: a statement
    // applies when its Principal names any of them. A Deny that applies to
    // any form denies, whatever another form is allowed; otherwise any
    // Allow allows; otherwise nothing grants it (implicit deny).
    let evaluate = |principal: &Principal| {
        let request = EvalRequest {
            principal,
            action: action.to_string(),
            resource: key_arn.to_string(),
            context: context.clone(),
        };
        evaluate_resource_policy_only(&document, &request)
    };
    if deny_only
        .iter()
        .any(|principal| evaluate(principal) == Decision::ExplicitDeny)
    {
        return Decision::ExplicitDeny;
    }
    let mut decision = Decision::ImplicitDeny;
    for principal in &principals {
        match evaluate(principal) {
            Decision::ExplicitDeny => return Decision::ExplicitDeny,
            Decision::Allow => decision = Decision::Allow,
            Decision::ImplicitDeny => {}
        }
    }
    decision
}

/// The request context of `caller` using a key with `encryption_context`
/// (`extra`: operation-specific keys such as `kms:ReEncryptOnSameKey`): what
/// the key policy, and the caller's SCPs, are evaluated with.
///
/// It carries the global keys AWS sets from the calling identity
/// (`aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:PrincipalType`,
/// `aws:userid`, `aws:PrincipalTag/*`, `aws:PrincipalOrgID`, ...): guard
/// statements such as `Deny ... ArnNotLike aws:PrincipalArn` would otherwise
/// match every caller, a negated operator on a missing key being true.
pub(crate) fn request_context(
    caller: &KeyCaller<'_>,
    encryption_context: &BTreeMap<String, String>,
    extra: &[(&str, &str)],
) -> RequestContext {
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let base = match caller {
        KeyCaller::Direct { principal, account } => {
            let account = caller_account(&principal.arn, account);
            keys.insert("kms:CallerAccount".to_string(), vec![account.to_string()]);
            principal_context(
                &principal.arn,
                account,
                &CallerFacts {
                    user_id: Some(principal.user_id.clone()),
                    tags: principal.tags.clone(),
                    ..Default::default()
                },
            )
        }
        KeyCaller::ViaService {
            service_principal,
            region,
            account,
            caller_arn,
            facts,
        } => {
            keys.insert(
                "kms:ViaService".to_string(),
                vec![via_service(service_principal, region)],
            );
            // The caller's own account, not the account of the resource the
            // service acts on (an S3 bucket, an SQS queue), which may be
            // the key's.
            let account = caller_arn.map_or(*account, |arn| caller_account(arn, account));
            keys.insert("kms:CallerAccount".to_string(), vec![account.to_string()]);
            let none = CallerFacts::default();
            let facts = facts.unwrap_or(&none);
            match caller_arn {
                Some(arn) => principal_context(arn, account, facts),
                None => principal_context(&account_root(account, region).arn, account, facts),
            }
        }
        KeyCaller::Service {
            service_principal,
            region,
            source_arn,
            source_account,
            via_service: via,
        } => {
            if let Some(via) = via {
                keys.insert("kms:ViaService".to_string(), vec![via_service(via, region)]);
            }
            let internal = InternalCaller::Service {
                service: service_principal.to_string(),
                source_arn: source_arn.to_string(),
                source_account: source_account.to_string(),
            };
            keys.extend(internal.condition_keys());
            RequestContext::default()
        }
    };
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
    let mut service_keys = base.service_keys.clone();
    service_keys.extend(keys);
    RequestContext {
        service_keys,
        ..base
    }
}

/// The request context a key policy sees for the calling identity `arn` of
/// `account`, as AWS fills it: `aws:PrincipalArn` (for an assumed-role
/// session, its role's IAM ARN: with its path when known, else path-less),
/// `aws:PrincipalAccount`, `aws:PrincipalType` (fakecloud-core's labels:
/// `User`, `AssumedRole`, `Account`, ...), `aws:userid`, `aws:username` for
/// an IAM user, `aws:PrincipalTag/*`, `aws:PrincipalOrgID` /
/// `aws:PrincipalOrgPaths`, and `aws:PrincipalIsAWSService` `false`. What
/// `facts` does not know stays absent.
fn principal_context(arn: &str, account: &str, facts: &CallerFacts) -> RequestContext {
    let principal_type = PrincipalType::from_arn(arn);
    let principal_arn = match crate::hook::assumed_role_parts(arn) {
        Some((role_account, role)) => facts.role_arn.clone().unwrap_or_else(|| {
            let partition = arn
                .strip_prefix("arn:")
                .and_then(|rest| rest.split(':').next())
                .unwrap_or("aws");
            format!("arn:{partition}:iam::{role_account}:role/{role}")
        }),
        None => arn.to_string(),
    };
    let user_id = facts
        .user_id
        .clone()
        .or_else(|| (principal_type == PrincipalType::Root).then(|| account.to_string()));
    let username = (principal_type == PrincipalType::User)
        .then(|| arn.rsplit_once(":user/").map(|(_, rest)| rest))
        .flatten()
        .map(|rest| rest.rsplit('/').next().unwrap_or(rest).to_string());
    let mut service_keys = BTreeMap::new();
    service_keys.insert(
        "aws:PrincipalIsAWSService".to_string(),
        vec!["false".to_string()],
    );
    if let Some((org_id, paths)) = &facts.org {
        service_keys.insert("aws:principalorgid".to_string(), vec![org_id.clone()]);
        service_keys.insert("aws:principalorgpaths".to_string(), vec![paths.clone()]);
    }
    RequestContext {
        aws_username: username,
        aws_userid: user_id,
        aws_principal_arn: Some(principal_arn),
        aws_principal_account: Some(account.to_string()),
        aws_principal_type: Some(
            fakecloud_core::dispatch::principal_type_label(principal_type).to_string(),
        ),
        principal_tags: facts.tags.clone(),
        service_keys,
        ..Default::default()
    }
}

/// The role ARNs `policy` names as an `AWS` principal for the role `role`
/// of `account` under any path (`arn:<p>:iam::<account>:role/<path>/<role>`
/// and the path-less form).
fn pathed_role_principals(policy: &str, account: &str, role: &str) -> Vec<String> {
    let Ok(doc) = serde_json::from_str::<Value>(policy) else {
        return Vec::new();
    };
    let statements = match &doc["Statement"] {
        Value::Array(items) => items.clone(),
        single @ Value::Object(_) => vec![single.clone()],
        _ => Vec::new(),
    };
    let mut arns = Vec::new();
    for statement in &statements {
        let aws = &statement["Principal"]["AWS"];
        let named: Vec<&str> = match aws {
            Value::String(s) => vec![s.as_str()],
            Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        for arn in named {
            let Some((prefix, path_and_name)) = arn.split_once(":role/") else {
                continue;
            };
            let names_account = prefix.ends_with(&format!(":iam::{account}"));
            let names_role = path_and_name.rsplit('/').next() == Some(role);
            if names_account && names_role && !arns.iter().any(|a| a == arn) {
                arns.push(arn.to_string());
            }
        }
    }
    arns
}

/// Whether the key policy `policy` of the key `key_arn` lets `caller`
/// perform `action` (see [`policy_decision`]).
pub(crate) fn policy_allows(
    policy: &str,
    key_arn: &str,
    caller: &KeyCaller<'_>,
    action: &str,
    encryption_context: &BTreeMap<String, String>,
    extra: &[(&str, &str)],
) -> bool {
    policy_decision(policy, key_arn, caller, action, encryption_context, extra).is_allow()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: &str = "123456789012";

    fn fixtures() -> Vec<(String, Value)> {
        let dir = format!(
            "{}/tests/fixtures/aws-managed-keys",
            env!("CARGO_MANIFEST_DIR")
        );
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let rec: Value = match std::fs::read_to_string(&path)
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
            {
                Some(v) => v,
                None => continue,
            };
            if let Some(alias) = rec["alias"].as_str() {
                out.push((alias.trim_start_matches("alias/aws/").to_string(), rec));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The vendored catalog reproduces every recording byte for byte.
    #[test]
    fn catalog_matches_every_recording() {
        let recorded = fixtures();
        assert_eq!(recorded.len(), 26);
        for (service, rec) in &recorded {
            assert_eq!(
                key_policy(service, ACCOUNT, "us-east-1").as_deref(),
                rec["get_key_policy"]["Policy"].as_str(),
                "aws/{service}"
            );
            assert_eq!(
                key_description(service).as_deref(),
                rec["describe_key"]["Description"].as_str(),
                "aws/{service}"
            );
        }
        assert_eq!(catalog().keys.len(), recorded.len());
        // Recorded: no AWS-managed key exists for these.
        for absent in ["logs", "cloudtrail", "sagemaker", "nosuchservice"] {
            assert!(!is_aws_managed_service(absent), "{absent}");
        }
        assert!(predefined().all(is_aws_managed_service));
    }

    #[test]
    fn policies_take_the_account_region_and_partition() {
        let p = key_policy("s3", "111122223333", "cn-north-1").unwrap();
        assert!(
            p.contains("\"kms:ViaService\" : \"s3.cn-north-1.amazonaws.com.cn\""),
            "{p}"
        );
        assert!(p.contains("\"arn:aws-cn:iam::111122223333:root\""), "{p}");
        assert!(!p.contains(ACCOUNT) && !p.contains("us-east-1"), "{p}");
        let p = key_policy("dynamodb", "111122223333", "eu-west-1").unwrap();
        assert!(p.contains("\"dynamodb.*.amazonaws.com\""), "{p}");
        assert_eq!(key_policy("logs", ACCOUNT, "us-east-1"), None);
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

    fn policy(service: &str) -> String {
        key_policy(service, ACCOUNT, "us-east-1").unwrap()
    }

    fn ctx(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn direct_allows(service: &str, action: &str, c: &[(&str, &str)]) -> bool {
        let principal = account_root(ACCOUNT, "us-east-1");
        policy_allows(
            &policy(service),
            &arn(service),
            &KeyCaller::Direct {
                principal: &principal,
                account: ACCOUNT,
            },
            action,
            &ctx(c),
            &[],
        )
    }

    fn via_allows(service: &str, via: &str, account: &str, action: &str) -> bool {
        policy_allows(
            &policy(service),
            &arn(service),
            &KeyCaller::ViaService {
                service_principal: via,
                region: "us-east-1",
                account,
                caller_arn: None,
                facts: None,
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
        for (service, _) in fixtures() {
            for action in [
                "kms:Encrypt",
                "kms:GenerateDataKey",
                "kms:GenerateDataKeyWithoutPlaintext",
                "kms:ReEncryptTo",
            ] {
                assert!(!direct_allows(&service, action, &[]), "{service} {action}");
            }
            assert!(direct_allows(&service, "kms:DescribeKey", &[]), "{service}");
            // SES lets the account decrypt (and re-encrypt from) the mail it
            // sealed; every other key refuses a direct Decrypt.
            assert_eq!(
                direct_allows(&service, "kms:Decrypt", &[]),
                service == "ses",
                "{service}"
            );
        }
    }

    /// The recorded `aws/lambda` exception: an account principal decrypts a
    /// ciphertext bound to one of its functions directly.
    #[test]
    fn lambda_key_decrypts_function_ciphertexts_directly() {
        let fn_arn = format!("arn:aws:lambda:us-east-1:{ACCOUNT}:function:f");
        let c = [("aws:lambda:FunctionArn", fn_arn.as_str())];
        assert!(direct_allows("lambda", "kms:Decrypt", &c));
        assert!(!direct_allows("lambda", "kms:Encrypt", &c));
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
            "dynamodb",
            "dynamodb.amazonaws.com",
            ACCOUNT,
            "kms:CreateGrant"
        ));
        assert!(via_allows(
            "ssm",
            "ssm.amazonaws.com",
            ACCOUNT,
            "kms:Encrypt"
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
        // SNS's key never lets SNS encrypt directly, only derive data keys.
        assert!(!via_allows(
            "sns",
            "sns.amazonaws.com",
            ACCOUNT,
            "kms:Encrypt"
        ));
        assert!(via_allows(
            "sns",
            "sns.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
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
        // SES's key is used by SES as a service principal, never via SES.
        assert!(!via_allows(
            "ses",
            "ses.amazonaws.com",
            ACCOUNT,
            "kms:GenerateDataKey"
        ));
    }

    /// SES seals received mail under `aws/ses` as its regional service
    /// principal, with the source account, rule name and message id in the
    /// encryption context (all three required by the recorded policy).
    #[test]
    fn ses_seals_mail_as_its_service_principal() {
        let rule = format!("arn:aws:ses:us-east-1:{ACCOUNT}:receipt-rule-set/s:receipt-rule/r");
        let caller = KeyCaller::Service {
            service_principal: "ses.amazonaws.com",
            region: "us-east-1",
            source_arn: &rule,
            source_account: ACCOUNT,
            via_service: None,
        };
        let full = ctx(&[
            ("aws:ses:source-account", ACCOUNT),
            ("aws:ses:rule-name", "r"),
            ("aws:ses:message-id", "m"),
        ]);
        let allows = |c: &BTreeMap<String, String>| {
            policy_allows(
                &policy("ses"),
                &arn("ses"),
                &caller,
                "kms:GenerateDataKey",
                c,
                &[],
            )
        };
        assert!(allows(&full));
        let mut missing = full.clone();
        missing.remove("aws:ses:message-id");
        assert!(!allows(&missing));
        let mut other = full.clone();
        other.insert("aws:ses:source-account".into(), "999999999999".into());
        assert!(!allows(&other));
        // Another service principal is not SES.
        let sns = KeyCaller::Service {
            service_principal: "sns.amazonaws.com",
            region: "us-east-1",
            source_arn: &rule,
            source_account: ACCOUNT,
            via_service: None,
        };
        assert!(!policy_allows(
            &policy("ses"),
            &arn("ses"),
            &sns,
            "kms:GenerateDataKey",
            &full,
            &[]
        ));
    }
}
