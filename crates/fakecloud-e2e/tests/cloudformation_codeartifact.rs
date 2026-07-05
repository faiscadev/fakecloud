//! CloudFormation provisions AWS::CodeArtifact::Domain and
//! AWS::CodeArtifact::Repository as real records in the `codeartifact` service
//! control plane: they read back through DescribeDomain / DescribeRepository,
//! expose their ARN via Ref and every documented attribute via Fn::GetAtt, and
//! honor dependency order (the repository's DomainName resolves from the
//! domain's Name GetAtt). Deleting the stack removes both resources.

mod helpers;

use helpers::TestServer;

// A domain plus a repository inside it. The repository's DomainName resolves
// from the domain's `Name` attribute (GetAtt), so the repository can only be
// provisioned after the domain -- exercising dependency ordering. Outputs
// surface Ref (the ARN, per the AWS resource spec) and each GetAtt attribute so
// the test can assert intrinsic-function resolution.
const TEMPLATE: &str = r#"{
  "Resources": {
    "MyDomain": {
      "Type": "AWS::CodeArtifact::Domain",
      "Properties": {
        "DomainName": "cfn-ca-domain",
        "Tags": [ { "Key": "env", "Value": "test" } ]
      }
    },
    "MyRepo": {
      "Type": "AWS::CodeArtifact::Repository",
      "Properties": {
        "RepositoryName": "cfn-ca-repo",
        "DomainName": { "Fn::GetAtt": ["MyDomain", "Name"] },
        "Description": "provisioned by cfn",
        "ExternalConnections": ["public:npmjs"]
      }
    }
  },
  "Outputs": {
    "DomainRef":   { "Value": { "Ref": "MyDomain" } },
    "DomainArn":   { "Value": { "Fn::GetAtt": ["MyDomain", "Arn"] } },
    "DomainName":  { "Value": { "Fn::GetAtt": ["MyDomain", "Name"] } },
    "DomainOwner": { "Value": { "Fn::GetAtt": ["MyDomain", "Owner"] } },
    "RepoRef":     { "Value": { "Ref": "MyRepo" } },
    "RepoArn":     { "Value": { "Fn::GetAtt": ["MyRepo", "Arn"] } },
    "RepoName":    { "Value": { "Fn::GetAtt": ["MyRepo", "Name"] } },
    "RepoDomain":  { "Value": { "Fn::GetAtt": ["MyRepo", "DomainName"] } },
    "RepoOwner":   { "Value": { "Fn::GetAtt": ["MyRepo", "DomainOwner"] } }
  }
}"#;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> &'a str {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("missing output {key}"))
}

#[tokio::test]
async fn cfn_provisions_codeartifact_domain_and_repository() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let ca = aws_sdk_codeartifact::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("ca-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("ca-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Intrinsic-function resolution (Ref + GetAtt) ---
    let domain_arn = output(stack, "DomainArn");
    let domain_owner = output(stack, "DomainOwner");
    let repo_arn = output(stack, "RepoArn");

    // Ref resolves to the resource ARN for both types (AWS resource spec).
    assert_eq!(output(stack, "DomainRef"), domain_arn);
    assert_eq!(output(stack, "RepoRef"), repo_arn);
    // GetAtt shapes.
    assert!(
        domain_arn.ends_with(":domain/cfn-ca-domain"),
        "domain arn {domain_arn}"
    );
    assert_eq!(output(stack, "DomainName"), "cfn-ca-domain");
    assert_eq!(domain_owner.len(), 12, "owner is a 12-digit account id");
    assert!(
        repo_arn.ends_with(":repository/cfn-ca-domain/cfn-ca-repo"),
        "repo arn {repo_arn}"
    );
    assert_eq!(output(stack, "RepoName"), "cfn-ca-repo");
    // The repository's DomainName was wired from the domain's Name GetAtt.
    assert_eq!(output(stack, "RepoDomain"), "cfn-ca-domain");
    assert_eq!(output(stack, "RepoOwner"), domain_owner);

    // --- The resources exist in the CodeArtifact service ---
    let domain_out = ca
        .describe_domain()
        .domain("cfn-ca-domain")
        .send()
        .await
        .expect("DescribeDomain");
    let domain = domain_out.domain().expect("domain description");
    assert_eq!(domain.arn(), Some(domain_arn));
    assert_eq!(domain.name(), Some("cfn-ca-domain"));

    let repo_out = ca
        .describe_repository()
        .domain("cfn-ca-domain")
        .repository("cfn-ca-repo")
        .send()
        .await
        .expect("DescribeRepository");
    let repo = repo_out.repository().expect("repository description");
    assert_eq!(repo.arn(), Some(repo_arn));
    assert_eq!(repo.name(), Some("cfn-ca-repo"));
    assert_eq!(repo.domain_name(), Some("cfn-ca-domain"));
    assert_eq!(repo.description(), Some("provisioned by cfn"));
    // The CFN ExternalConnections string property was expanded into the API's
    // externalConnections object shape.
    assert!(
        repo.external_connections()
            .iter()
            .any(|c| c.external_connection_name() == Some("public:npmjs")),
        "expected public:npmjs external connection"
    );

    // Tags applied at create time round-trip.
    let tags = ca
        .list_tags_for_resource()
        .resource_arn(domain_arn)
        .send()
        .await
        .expect("ListTagsForResource");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == "env" && t.value() == "test"),
        "expected env=test tag on the domain"
    );

    // --- Deleting the stack removes both resources ---
    cfn.delete_stack()
        .stack_name("ca-stack")
        .send()
        .await
        .unwrap();

    let repo_gone = ca
        .describe_repository()
        .domain("cfn-ca-domain")
        .repository("cfn-ca-repo")
        .send()
        .await;
    assert!(
        repo_gone.is_err(),
        "stack delete should remove the repository"
    );
    let domain_gone = ca.describe_domain().domain("cfn-ca-domain").send().await;
    assert!(
        domain_gone.is_err(),
        "stack delete should remove the domain"
    );
}
