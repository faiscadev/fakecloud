//! Physical names CloudFormation generates for resources whose name property
//! the template leaves out.
//!
//! AWS names such a resource `{StackName}-{LogicalId}-{SUFFIX}`: the stack
//! name and logical id are truncated to fit the resource type's name limit,
//! and the suffix is a random 13-character string. Using the bare logical id
//! instead made two stacks built from one template in the same account (a
//! stack set deployed to two regions, a dev and a prod stack) collide on every
//! unnamed resource.
//!
//! The suffix is random, as in AWS, so a resource replaced by an update gets a
//! new name (and never collides with an old one its UpdateReplacePolicy
//! retained). Updates that leave the name property out keep the resource's
//! current name rather than generating another.

use super::ResourceProvisioner;
use crate::state::StackResource;
use crate::template::ResourceDefinition;

const SUFFIX_LEN: usize = 13;
const ALPHABET: &[u8; 36] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// How a resource type's generated name is shaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NameRule {
    /// Longest name the type accepts.
    pub max_len: usize,
    /// The type only accepts lowercase names (S3 buckets, RDS and ElastiCache
    /// identifiers, ...).
    pub lowercase: bool,
    /// Whether the stack name leads the name. A few types are named
    /// `{LogicalId}-{SUFFIX}` only.
    pub include_stack: bool,
    /// Character joining the parts, for types that do not accept `-`.
    pub separator: char,
}

impl NameRule {
    const fn new(max_len: usize) -> Self {
        Self {
            max_len,
            lowercase: false,
            include_stack: true,
            separator: '-',
        }
    }

    const fn lower(self) -> Self {
        Self {
            lowercase: true,
            ..self
        }
    }

    const fn without_stack(self) -> Self {
        Self {
            include_stack: false,
            ..self
        }
    }

    const fn separated_by(self, separator: char) -> Self {
        Self { separator, ..self }
    }
}

/// The name rule for a resource type, from the type's documented name
/// constraints.
pub(crate) fn name_rule(resource_type: &str) -> NameRule {
    match resource_type {
        "AWS::S3::Bucket" => NameRule::new(63).lower(),
        "AWS::SQS::Queue" => NameRule::new(80),
        "AWS::SNS::Topic" => NameRule::new(256),
        "AWS::Lambda::Function" => NameRule::new(64),
        "AWS::Lambda::CapacityProvider" => NameRule::new(140),
        "AWS::Lambda::LayerVersion" => NameRule::new(140),
        "AWS::DynamoDB::Table" => NameRule::new(255),
        "AWS::Kinesis::Stream" => NameRule::new(128),
        "AWS::KinesisFirehose::DeliveryStream" => NameRule::new(64),
        "AWS::IAM::Role" | "AWS::IAM::User" => NameRule::new(64),
        "AWS::IAM::Policy"
        | "AWS::IAM::Group"
        | "AWS::IAM::ManagedPolicy"
        | "AWS::IAM::InstanceProfile"
        | "AWS::IAM::SAMLProvider" => NameRule::new(128),
        "AWS::Logs::LogGroup"
        | "AWS::Logs::LogStream"
        | "AWS::Logs::MetricFilter"
        | "AWS::Logs::SubscriptionFilter" => NameRule::new(512),
        "AWS::Events::Rule" | "AWS::Events::Connection" | "AWS::Events::ApiDestination" => {
            NameRule::new(64)
        }
        "AWS::Events::Archive" => NameRule::new(48),
        "AWS::ElasticLoadBalancingV2::LoadBalancer"
        | "AWS::ElasticLoadBalancingV2::TargetGroup"
        | "AWS::ElasticLoadBalancingV2::TrustStore" => NameRule::new(32),
        "AWS::EC2::SecurityGroup" => NameRule::new(255),
        "AWS::ECR::Repository" => NameRule::new(256).lower(),
        "AWS::ECS::Cluster"
        | "AWS::ECS::TaskDefinition"
        | "AWS::ECS::Service"
        | "AWS::ECS::CapacityProvider" => NameRule::new(255),
        "AWS::EKS::Cluster" | "AWS::EKS::FargateProfile" => NameRule::new(100),
        "AWS::EKS::Nodegroup" => NameRule::new(63),
        "AWS::Cognito::UserPool" | "AWS::Cognito::UserPoolClient" => NameRule::new(128),
        // Identity pool names allow only word characters and spaces.
        "AWS::Cognito::IdentityPool" => NameRule::new(128).separated_by('_'),
        // Topic rule names allow only letters, digits and underscores.
        "AWS::IoT::TopicRule" => NameRule::new(128).separated_by('_'),
        "AWS::RDS::DBSubnetGroup"
        | "AWS::RDS::DBParameterGroup"
        | "AWS::RDS::DBClusterParameterGroup"
        | "AWS::RDS::OptionGroup"
        | "AWS::RDS::DBSecurityGroup"
        | "AWS::RDS::EventSubscription" => NameRule::new(255).lower(),
        "AWS::RDS::DBProxy" | "AWS::RDS::DBInstance" | "AWS::RDS::DBCluster" => {
            NameRule::new(63).lower()
        }
        "AWS::Redshift::Cluster" | "AWS::DocDB::DBCluster" | "AWS::Neptune::DBCluster" => {
            NameRule::new(63).lower()
        }
        "AWS::CloudFormation::Stack" => NameRule::new(128),
        "AWS::ElastiCache::ParameterGroup"
        | "AWS::ElastiCache::SubnetGroup"
        | "AWS::ElastiCache::SecurityGroup" => NameRule::new(255).lower(),
        "AWS::ElastiCache::User"
        | "AWS::ElastiCache::UserGroup"
        | "AWS::ElastiCache::ReplicationGroup" => NameRule::new(40).lower(),
        "AWS::ElastiCache::CacheCluster" => NameRule::new(40).lower(),
        "AWS::ElasticBeanstalk::Environment" => NameRule::new(40),
        "AWS::ElasticBeanstalk::Application"
        | "AWS::ElasticBeanstalk::ApplicationVersion"
        | "AWS::ElasticBeanstalk::ConfigurationTemplate" => NameRule::new(100),
        "AWS::EFS::FileSystem" => NameRule::new(64),
        "AWS::CodeCommit::Repository" | "AWS::CodeDeploy::Application" => NameRule::new(100),
        "AWS::CodeDeploy::DeploymentGroup" | "AWS::CodePipeline::Pipeline" => NameRule::new(100),
        "AWS::CodeBuild::Project" => NameRule::new(150),
        "AWS::CodeArtifact::Domain" => NameRule::new(50).lower(),
        "AWS::CodeArtifact::Repository" => NameRule::new(100),
        "AWS::Batch::ComputeEnvironment"
        | "AWS::Batch::JobQueue"
        | "AWS::Batch::JobDefinition"
        | "AWS::Batch::SchedulingPolicy" => NameRule::new(128),
        "AWS::Backup::BackupVault" | "AWS::Backup::BackupPlan" => NameRule::new(50),
        // Data source names must be valid GraphQL identifiers.
        "AWS::AppSync::DataSource" => NameRule::new(255).separated_by('_'),
        "AWS::AppConfig::Application"
        | "AWS::AppConfig::Environment"
        | "AWS::AppConfig::ConfigurationProfile" => NameRule::new(64),
        "AWS::Athena::WorkGroup" => NameRule::new(128),
        "AWS::Athena::DataCatalog" => NameRule::new(129),
        "AWS::CloudWatch::Alarm" | "AWS::CloudWatch::Dashboard" => NameRule::new(255),
        "AWS::CloudFront::PublicKey" | "AWS::CloudFront::CloudFrontOriginAccessIdentity" => {
            NameRule::new(128)
        }
        // Caller references.
        "AWS::Route53::HostedZone" => NameRule::new(128),
        "AWS::Route53::HealthCheck" => NameRule::new(64),
        "AWS::MSK::Configuration" => NameRule::new(64),
        "AWS::MSK::Replicator" => NameRule::new(128),
        "AWS::Glue::Database" => NameRule::new(255).lower(),
        "AWS::MWAA::Environment" => NameRule::new(80),
        "AWS::AmazonMQ::Broker" => NameRule::new(50),
        "AWS::AmazonMQ::Configuration" => NameRule::new(150),
        "AWS::OpenSearchService::Domain" | "AWS::Elasticsearch::Domain" => {
            NameRule::new(28).lower()
        }
        "AWS::Pipes::Pipe" => NameRule::new(64),
        "AWS::ServiceDiscovery::Instance" => NameRule::new(64),
        "AWS::SES::ConfigurationSet"
        | "AWS::SES::ConfigurationSetEventDestination"
        | "AWS::SES::Template"
        | "AWS::SES::ContactList"
        | "AWS::SES::DedicatedIpPool"
        | "AWS::SES::ReceiptRuleSet"
        | "AWS::SES::ReceiptRule"
        | "AWS::SES::ReceiptFilter" => NameRule::new(64),
        "AWS::StepFunctions::StateMachine" => NameRule::new(80).without_stack(),
        "AWS::SecretsManager::Secret" => NameRule::new(512).without_stack(),
        "AWS::Organizations::OrganizationalUnit" | "AWS::Organizations::Policy" => {
            NameRule::new(128)
        }
        "AWS::Timestream::Database" | "AWS::Timestream::Table" => NameRule::new(256),
        "AWS::Amplify::App" => NameRule::new(255),
        "AWS::EMR::Cluster" => NameRule::new(256),
        t if t.starts_with("AWS::SageMaker::") => NameRule::new(63),
        _ => NameRule::new(255),
    }
}

/// The stack name inside a stack id ARN
/// (`arn:aws:cloudformation:{region}:{account}:stack/{name}/{uuid}`). Stack ids
/// that are not stack ARNs (Cloud Control's one-shot provisioners) have none.
fn stack_name(stack_id: &str) -> Option<&str> {
    let resource = stack_id.splitn(6, ':').nth(5)?;
    let mut parts = resource.split('/');
    (parts.next()? == "stack").then_some(())?;
    parts.next().filter(|name| !name.is_empty())
}

fn random_suffix() -> String {
    let bytes = [
        uuid::Uuid::new_v4().into_bytes(),
        uuid::Uuid::new_v4().into_bytes(),
    ]
    .concat();
    bytes
        .iter()
        .take(SUFFIX_LEN)
        .map(|b| ALPHABET[usize::from(*b) % ALPHABET.len()] as char)
        .collect()
}

/// Truncate `stack` and `logical` so both fit in `budget` characters, sharing
/// the space evenly and giving either one's unused share to the other.
fn fit(stack: &str, logical: &str, budget: usize) -> (String, String) {
    let take = |s: &str, n: usize| s.chars().take(n).collect::<String>();
    let (s_len, l_len) = (stack.chars().count(), logical.chars().count());
    if s_len + l_len <= budget {
        return (stack.to_string(), logical.to_string());
    }
    let half = budget / 2;
    if s_len <= half {
        (stack.to_string(), take(logical, budget - s_len))
    } else if l_len <= budget - half {
        (take(stack, budget - l_len), logical.to_string())
    } else {
        (take(stack, half), take(logical, budget - half))
    }
}

/// Generate the physical name for `logical_id` in the stack `stack_id`.
pub(crate) fn generate(stack_id: &str, logical_id: &str, rule: NameRule) -> String {
    generate_with_suffix(stack_id, logical_id, rule, &random_suffix())
}

fn generate_with_suffix(stack_id: &str, logical_id: &str, rule: NameRule, suffix: &str) -> String {
    let sep = rule.separator;
    let stack = stack_name(stack_id).filter(|_| rule.include_stack);
    // Room left for the stack name and logical id once the suffix and the
    // separators are accounted for.
    let separators = if stack.is_some() { 2 } else { 1 };
    let budget = rule.max_len.saturating_sub(SUFFIX_LEN + separators);
    let name = match stack {
        Some(stack) => {
            // A stack name may carry `-`, which a type with another separator
            // does not accept either.
            let stack = stack.replace('-', &sep.to_string());
            let (stack, logical) = fit(&stack, logical_id, budget);
            // A cut right after a `-` in the stack name would double it.
            let stack = stack.trim_end_matches(sep);
            format!("{stack}{sep}{logical}{sep}{suffix}")
        }
        None => {
            let logical: String = logical_id.chars().take(budget).collect();
            format!("{logical}{sep}{suffix}")
        }
    };
    let name: String = name.chars().take(rule.max_len).collect();
    if rule.lowercase {
        name.to_lowercase()
    } else {
        name
    }
}

/// Whether `name` is the name [`generate`] gives `logical_id` in this stack
/// (with whatever random suffix it drew).
pub(crate) fn is_generated_name(
    stack_id: &str,
    logical_id: &str,
    resource_type: &str,
    name: &str,
) -> bool {
    generated_with_rule(stack_id, logical_id, name_rule(resource_type), name)
}

fn generated_with_rule(stack_id: &str, logical_id: &str, rule: NameRule, name: &str) -> bool {
    // The suffix is whatever follows the last separator; regenerating with it
    // reproduces the name exactly when the name was generated.
    let Some((_, suffix)) = name.rsplit_once(rule.separator) else {
        return false;
    };
    generate_with_suffix(stack_id, logical_id, rule, suffix) == name
}

/// Whether `name` is one an older build gave an unnamed resource: the bare
/// logical id, `{LogicalId}-{8 hex}`, `cfn-{kind}-{LogicalId}`, or
/// `cfn-[{kind}-]{logicalid}-{8 alphanumerics}`.
fn is_legacy_name(name: &str, logical_id: &str) -> bool {
    if name.eq_ignore_ascii_case(logical_id) {
        return true;
    }
    let is_id8 = |s: &str, hex: bool| {
        s.len() == 8
            && s.bytes().all(|b| {
                if hex {
                    b.is_ascii_hexdigit()
                } else {
                    b.is_ascii_alphanumeric()
                }
            })
    };
    if let Some((head, tail)) = name.rsplit_once('-') {
        if head == logical_id && is_id8(tail, true) {
            return true;
        }
    }
    let tokens: Vec<&str> = name.split('-').collect();
    match tokens.as_slice() {
        ["cfn", logical, id] | ["cfn", _, logical, id]
            if *logical == logical_id.to_lowercase() && is_id8(id, false) =>
        {
            true
        }
        ["cfn", _kind, logical] => *logical == logical_id,
        _ => false,
    }
}

impl ResourceProvisioner {
    /// The name CloudFormation gives `resource` when its template leaves the
    /// name property out.
    pub(crate) fn physical_name(&self, resource: &ResourceDefinition) -> String {
        if let Some(name) = self.reused_names.lock().get(&resource.logical_id) {
            return name.clone();
        }
        generate(
            &self.stack_id,
            &resource.logical_id,
            name_rule(&resource.resource_type),
        )
    }

    /// Run `create` with `existing`'s name reserved for a resource it
    /// generates a name for, so a resource re-created in place of `existing`
    /// (an update fakecloud applies by re-provisioning) keeps its name.
    pub(crate) fn with_existing_name<T>(
        &self,
        existing: &StackResource,
        create: impl FnOnce() -> T,
    ) -> T {
        let Some(name) = self.existing_name(existing) else {
            return create();
        };
        self.reused_names
            .lock()
            .insert(existing.logical_id.clone(), name);
        let out = create();
        self.reused_names.lock().remove(&existing.logical_id);
        out
    }

    /// The name `existing` was given when its template left the name out,
    /// recovered from its physical id (a name, an ARN, a URL, `name:revision`).
    /// Resources created before names were generated were named after their
    /// logical id, which is recognized too.
    pub(crate) fn existing_name(&self, existing: &StackResource) -> Option<String> {
        let rule = name_rule(&existing.resource_type);
        let physical = &existing.physical_id;
        let is_suffix_char = |c: char| {
            if rule.lowercase {
                c.is_ascii_lowercase() || c.is_ascii_digit()
            } else {
                c.is_ascii_uppercase() || c.is_ascii_digit()
            }
        };
        // A FIFO queue or topic was generated with room left for `.fifo`.
        let mut rules = vec![rule];
        if matches!(
            existing.resource_type.as_str(),
            "AWS::SQS::Queue" | "AWS::SNS::Topic"
        ) {
            rules.push(NameRule {
                max_len: rule.max_len - ".fifo".len(),
                ..rule
            });
        }
        for rule in rules {
            let prefix = generate_with_suffix(&self.stack_id, &existing.logical_id, rule, "");
            for (start, _) in physical.match_indices(&prefix) {
                let rest = &physical[start + prefix.len()..];
                let suffix: String = rest.chars().take_while(|c| is_suffix_char(*c)).collect();
                if suffix.len() == SUFFIX_LEN {
                    return Some(format!("{prefix}{suffix}"));
                }
            }
        }
        // Only the part of the physical id that holds the name: the last path
        // segment, without a `:revision` or `|detail`.
        let last = physical.rsplit('/').next().unwrap_or(physical);
        let last = last.split('|').next().unwrap_or(last);
        let name = match last.rsplit_once(':') {
            Some((name, revision)) if revision.bytes().all(|b| b.is_ascii_digit()) => name,
            _ => last,
        };
        let name = name.rsplit(':').next().unwrap_or(name);
        is_legacy_name(name, &existing.logical_id).then(|| name.to_string())
    }

    /// `physical_name` for a resource type whose name must fit `max_len`
    /// with a trailing `ending` (an SQS FIFO queue's `.fifo`).
    pub(crate) fn physical_name_ending(
        &self,
        resource: &ResourceDefinition,
        ending: &str,
    ) -> String {
        if let Some(name) = self.reused_names.lock().get(&resource.logical_id) {
            // Recovered names stop short of the ending.
            return if name.ends_with(ending) {
                name.clone()
            } else {
                format!("{name}{ending}")
            };
        }
        let mut rule = name_rule(&resource.resource_type);
        rule.max_len = rule.max_len.saturating_sub(ending.len());
        format!(
            "{}{ending}",
            generate(&self.stack_id, &resource.logical_id, rule)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STACK: &str =
        "arn:aws:cloudformation:us-east-1:123456789012:stack/my-app/1b2c3d4e-0000-4000-8000-000000000000";

    #[test]
    fn names_follow_the_stack_logical_suffix_shape() {
        let name = generate(STACK, "OrdersQueue", name_rule("AWS::SQS::Queue"));
        let (prefix, suffix) = name.rsplit_once('-').unwrap();
        assert_eq!(prefix, "my-app-OrdersQueue");
        assert_eq!(suffix.len(), SUFFIX_LEN);
        assert!(suffix.bytes().all(|b| ALPHABET.contains(&b)), "{name}");
    }

    #[test]
    fn every_generated_name_is_distinct() {
        let rule = name_rule("AWS::SQS::Queue");
        // Two stacks with the same name (one per region), or a replacement in
        // the same stack, never reuse a name.
        assert_ne!(generate(STACK, "Q", rule), generate(STACK, "Q", rule));
        let fixed = "ABCDEFGHIJKLM";
        assert_eq!(
            generate_with_suffix(STACK, "Q", rule, fixed),
            "my-app-Q-ABCDEFGHIJKLM"
        );
    }

    #[test]
    fn long_parts_are_truncated_to_the_type_limit() {
        let stack = STACK.replace("my-app", &"s".repeat(100));
        let name = generate(
            &stack,
            &"L".repeat(100),
            name_rule("AWS::ElasticLoadBalancingV2::LoadBalancer"),
        );
        assert_eq!(name.len(), 32, "{name}");
        // Both parts survive the cut.
        assert!(name.starts_with("ss"), "{name}");
        assert!(name.contains("-LL"), "{name}");

        // A short stack name leaves the logical id the rest of the room.
        let name = generate(
            STACK,
            &"L".repeat(100),
            name_rule("AWS::ElasticLoadBalancingV2::LoadBalancer"),
        );
        assert_eq!(name.len(), 32, "{name}");
        assert!(name.starts_with("my-app-LLLLLLLLL"), "{name}");
    }

    #[test]
    fn a_cut_after_a_hyphen_does_not_double_it() {
        // 32-char limit: budget 17 split 8/9, and "my-apps-" is cut after "-".
        let stack = STACK.replace("my-app", "my-apps-production");
        let name = generate_with_suffix(
            &stack,
            "LoadBalancerMain",
            name_rule("AWS::ElasticLoadBalancingV2::LoadBalancer"),
            "ABCDEFGHIJKLM",
        );
        assert!(!name.contains("--"), "{name}");
        assert!(name.len() <= 32, "{name}");
    }

    #[test]
    fn lowercase_types_are_lowercased() {
        let name = generate(STACK, "AssetsBucket", name_rule("AWS::S3::Bucket"));
        assert_eq!(name, name.to_lowercase());
        assert!(name.starts_with("my-app-assetsbucket-"), "{name}");
    }

    #[test]
    fn some_types_leave_the_stack_name_out() {
        let name = generate(STACK, "Flow", name_rule("AWS::StepFunctions::StateMachine"));
        assert!(name.starts_with("Flow-"), "{name}");
        assert!(!name.contains("my-app"), "{name}");
    }

    #[test]
    fn a_stack_id_that_is_not_an_arn_has_no_stack_part() {
        let name = generate(
            "cloudcontrol-1234",
            "Resource",
            name_rule("AWS::SQS::Queue"),
        );
        assert!(name.starts_with("Resource-"), "{name}");
    }

    #[test]
    fn names_from_older_builds_are_recognized() {
        assert!(is_legacy_name("MySet", "MySet"));
        assert!(is_legacy_name("cfn-cs-MySet", "MySet"));
        assert!(is_legacy_name("cfn-db-4f3a9c1e", "Db"));
        assert!(is_legacy_name("cfn-cluster-db-x9k2m4pq", "Db"));
        assert!(is_legacy_name("Flow-1a2b3c4d", "Flow"));
        assert!(!is_legacy_name("orders", "MySet"));
        assert!(!is_legacy_name("Flow-notahexx", "Flow"));
        assert!(!is_legacy_name("cfn-x-other-Batch", "Batch"));
    }

    #[test]
    fn identity_pools_avoid_hyphens() {
        let name = generate(STACK, "Pool", name_rule("AWS::Cognito::IdentityPool"));
        assert!(name.starts_with("my_app_Pool_"), "{name}");
    }

    #[test]
    fn generated_names_are_recognised_for_every_rule_shape() {
        let long_stack = "arn:aws:cloudformation:us-east-1:123456789012:stack/a-really-long-stack-name-that-goes-on-and-on-and-on-forever/1b2c3d4e-0000-4000-8000-000000000000";
        let long_logical = "AVeryLongLogicalIdentifierThatWillNeedTruncatingForSure";
        let rules = [
            NameRule::new(255),
            NameRule::new(64).lower(),
            NameRule::new(64).without_stack(),
            NameRule::new(63).separated_by('_'),
            NameRule::new(32),
            NameRule::new(32).lower().separated_by('_'),
        ];
        for rule in rules {
            for (stack, logical) in [(STACK, "Tmpl"), (long_stack, long_logical)] {
                for _ in 0..5 {
                    let name = generate(stack, logical, rule);
                    assert!(
                        generated_with_rule(stack, logical, rule, &name),
                        "{rule:?} {name}"
                    );
                    // Another logical id's name, or a repeat with an extra
                    // part, is not this resource's generated name.
                    assert!(!generated_with_rule(stack, "Other", rule, &name), "{name}");
                    let repeated = format!("{name}{}{name}", rule.separator);
                    assert!(!generated_with_rule(stack, logical, rule, &repeated));
                }
            }
        }
        // Explicit and legacy names are not generated ones.
        let rule = name_rule("AWS::EC2::LaunchTemplate");
        assert!(!generated_with_rule(STACK, "Tmpl", rule, "named"));
        assert!(!generated_with_rule(
            STACK,
            "Tmpl",
            rule,
            "cfn-tmpl-4f3a9c1e"
        ));
    }
}
