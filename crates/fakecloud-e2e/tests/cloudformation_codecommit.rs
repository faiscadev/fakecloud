//! CloudFormation provisions AWS::CodeCommit::Repository as a real record in the
//! `codecommit` service control plane: it reads back through GetRepository,
//! resolves Ref to the repository NAME (per the AWS resource spec) and every
//! documented attribute via Fn::GetAtt (Arn, CloneUrlHttp, CloneUrlSsh, Name),
//! and round-trips its tags. Deleting the stack removes the repository.

mod helpers;

use helpers::TestServer;

// A single repository with a description and a tag. Outputs surface Ref (the
// repository name, per the AWS resource spec -- distinct from CodeArtifact where
// Ref is the ARN) and each GetAtt attribute so the test can assert
// intrinsic-function resolution.
const TEMPLATE: &str = r#"{
  "Resources": {
    "MyRepo": {
      "Type": "AWS::CodeCommit::Repository",
      "Properties": {
        "RepositoryName": "cfn-cc-repo",
        "RepositoryDescription": "provisioned by cfn",
        "Tags": [ { "Key": "env", "Value": "test" } ]
      }
    }
  },
  "Outputs": {
    "RepoRef":      { "Value": { "Ref": "MyRepo" } },
    "RepoArn":      { "Value": { "Fn::GetAtt": ["MyRepo", "Arn"] } },
    "RepoName":     { "Value": { "Fn::GetAtt": ["MyRepo", "Name"] } },
    "CloneUrlHttp": { "Value": { "Fn::GetAtt": ["MyRepo", "CloneUrlHttp"] } },
    "CloneUrlSsh":  { "Value": { "Fn::GetAtt": ["MyRepo", "CloneUrlSsh"] } }
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
async fn cfn_provisions_codecommit_repository() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let cc = aws_sdk_codecommit::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("cc-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("cc-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Intrinsic-function resolution (Ref + GetAtt) ---
    let repo_arn = output(stack, "RepoArn");
    let clone_http = output(stack, "CloneUrlHttp");
    let clone_ssh = output(stack, "CloneUrlSsh");

    // Ref resolves to the repository NAME (AWS resource spec), not the ARN.
    assert_eq!(output(stack, "RepoRef"), "cfn-cc-repo");
    assert_eq!(output(stack, "RepoName"), "cfn-cc-repo");
    assert!(repo_arn.ends_with(":cfn-cc-repo"), "repo arn {repo_arn}");
    assert!(
        repo_arn.starts_with("arn:aws:codecommit:"),
        "repo arn {repo_arn}"
    );
    assert!(
        clone_http.ends_with("/v1/repos/cfn-cc-repo") && clone_http.starts_with("https://"),
        "clone http {clone_http}"
    );
    assert!(
        clone_ssh.ends_with("/v1/repos/cfn-cc-repo") && clone_ssh.starts_with("ssh://"),
        "clone ssh {clone_ssh}"
    );

    // --- The repository exists in the CodeCommit service ---
    let got = cc
        .get_repository()
        .repository_name("cfn-cc-repo")
        .send()
        .await
        .expect("GetRepository");
    let meta = got.repository_metadata().expect("repository metadata");
    assert_eq!(meta.repository_name(), Some("cfn-cc-repo"));
    assert_eq!(meta.arn(), Some(repo_arn));
    assert_eq!(meta.repository_description(), Some("provisioned by cfn"));
    // The clone URLs read back match the GetAtt values.
    assert_eq!(meta.clone_url_http(), Some(clone_http));
    assert_eq!(meta.clone_url_ssh(), Some(clone_ssh));

    // Tags applied at create time round-trip.
    let tags = cc
        .list_tags_for_resource()
        .resource_arn(repo_arn)
        .send()
        .await
        .expect("ListTagsForResource");
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("test"),
        "expected env=test tag on the repository"
    );

    // --- Deleting the stack removes the repository ---
    cfn.delete_stack()
        .stack_name("cc-stack")
        .send()
        .await
        .unwrap();

    let gone = cc
        .get_repository()
        .repository_name("cfn-cc-repo")
        .send()
        .await;
    assert!(gone.is_err(), "stack delete should remove the repository");
}
