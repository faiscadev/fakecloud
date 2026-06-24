//! SAM `AWS::Serverless::Function` `Policies` + `Events` expansion: a function's
//! Policies become an implicit execution role and its Events become the native
//! trigger resources (Events::Rule, Lambda::EventSourceMapping) + the
//! Lambda::Permission that lets the source invoke it. Without this a SAM deploy
//! produced a role-less, trigger-less function.

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

const TEMPLATE: &str = r#"
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  Worker:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: sam-worker
      Runtime: python3.12
      Handler: index.handler
      InlineCode: |
        def handler(event, context):
            return {}
      Policies:
        - AmazonS3ReadOnlyAccess
        - Statement:
            - Effect: Allow
              Action: dynamodb:GetItem
              Resource: '*'
      Events:
        Tick:
          Type: Schedule
          Properties:
            Schedule: rate(5 minutes)
        Jobs:
          Type: SQS
          Properties:
            Queue: arn:aws:sqs:us-east-1:000000000000:jobs
            BatchSize: 10
"#;

#[tokio::test]
async fn sam_function_expands_policies_and_events() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("sam-events")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("sam-events")
        .send()
        .await
        .expect("describe_stacks");
    assert_eq!(
        described
            .stacks()
            .first()
            .unwrap()
            .stack_status()
            .unwrap()
            .as_str(),
        "CREATE_COMPLETE"
    );

    // --- implicit execution role with the managed + inline policies ---
    let iam = server.iam_client().await;
    let roles = iam.list_roles().send().await.expect("list_roles");
    let role = roles
        .roles()
        .iter()
        .find(|r| r.role_name() == "WorkerRole")
        .expect("WorkerRole synthesized from Policies");
    let attached = iam
        .list_attached_role_policies()
        .role_name(role.role_name())
        .send()
        .await
        .expect("list_attached_role_policies");
    assert!(
        attached
            .attached_policies()
            .iter()
            .any(|p| p.policy_arn() == Some("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")),
        "managed policy attached: {:?}",
        attached.attached_policies()
    );
    let inline = iam
        .list_role_policies()
        .role_name(role.role_name())
        .send()
        .await
        .expect("list_role_policies");
    assert!(
        !inline.policy_names().is_empty(),
        "inline policy attached: {:?}",
        inline.policy_names()
    );

    // --- Schedule event -> Events::Rule targeting the function ---
    let events = server.eventbridge_client().await;
    let rules = events.list_rules().send().await.expect("list_rules");
    let rule = rules
        .rules()
        .iter()
        .find(|r| r.name() == Some("WorkerTickRule"))
        .expect("WorkerTickRule synthesized from Schedule event");
    let targets = events
        .list_targets_by_rule()
        .rule(rule.name().unwrap())
        .send()
        .await
        .expect("list_targets_by_rule");
    assert_eq!(
        targets.targets().len(),
        1,
        "schedule rule must target the function"
    );

    // --- SQS event -> Lambda::EventSourceMapping ---
    let lambda = server.lambda_client().await;
    let esms = lambda
        .list_event_source_mappings()
        .function_name("sam-worker")
        .send()
        .await
        .expect("list_event_source_mappings");
    assert!(
        esms.event_source_mappings()
            .iter()
            .any(|m| m.event_source_arn() == Some("arn:aws:sqs:us-east-1:000000000000:jobs")),
        "SQS event must create an EventSourceMapping: {:?}",
        esms.event_source_mappings()
    );
}
