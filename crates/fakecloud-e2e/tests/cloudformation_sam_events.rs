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
  JobsQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: jobs
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
            # An event source mapping's queue must exist.
            Queue: !GetAtt JobsQueue.Arn
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
        // The synthesized role has no RoleName, so it is named after the stack.
        .find(|r| r.role_name().starts_with("sam-events-WorkerRole-"))
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
        // Unnamed, so named after the stack like any generated resource.
        .find(|r| {
            r.name()
                .is_some_and(|n| n.starts_with("sam-events-WorkerTickRule-"))
        })
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
            .any(|m| m.event_source_arn() == Some("arn:aws:sqs:us-east-1:123456789012:jobs")),
        "SQS event must create an EventSourceMapping: {:?}",
        esms.event_source_mappings()
    );
}

const TRIGGERS_TEMPLATE: &str = r#"
Transform: AWS::Serverless-2016-10-31
Resources:
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      PoolName: sam-trigger-pool
  AppLogs:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: /sam/app
  Handler:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: sam-trigger-fn
      Runtime: python3.12
      Handler: index.handler
      InlineCode: |
        def handler(event, context):
            return event
      Events:
        SignUp:
          Type: Cognito
          Properties:
            UserPool: !Ref Pool
            Trigger: PreSignUp
        Errors:
          Type: CloudWatchLogs
          Properties:
            LogGroupName: !Ref AppLogs
            FilterPattern: ERROR
        Telemetry:
          Type: IoTRule
          Properties:
            Sql: "SELECT * FROM 'sensors/+'"
"#;

/// Cognito / CloudWatchLogs / IoTRule events (previously dropped) set the
/// pool's trigger, subscribe the log group and create the topic rule.
#[tokio::test]
async fn sam_cognito_logs_and_iot_events_wire_their_sources() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    cfn.create_stack()
        .stack_name("sam-triggers")
        .template_body(TRIGGERS_TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .capabilities(Capability::CapabilityAutoExpand)
        .send()
        .await
        .expect("create_stack");
    let status = helpers::wait_until(std::time::Duration::from_secs(60), || async {
        let out = cfn
            .describe_stacks()
            .stack_name("sam-triggers")
            .send()
            .await
            .ok()?;
        let s = out.stacks().first()?;
        let status = s.stack_status()?.as_str().to_string();
        (!status.ends_with("IN_PROGRESS")).then(|| {
            (
                status,
                s.stack_status_reason().unwrap_or_default().to_string(),
            )
        })
    })
    .await
    .unwrap();
    assert_eq!(status.0, "CREATE_COMPLETE", "{}", status.1);
    let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:sam-trigger-fn";
    let physical = |logical: &'static str| {
        let cfn = cfn.clone();
        async move {
            cfn.describe_stack_resource()
                .stack_name("sam-triggers")
                .logical_resource_id(logical)
                .send()
                .await
                .unwrap()
                .stack_resource_detail()
                .unwrap()
                .physical_resource_id()
                .unwrap()
                .to_string()
        }
    };

    let cognito = server.cognito_client().await;
    let pool = cognito
        .describe_user_pool()
        .user_pool_id(physical("Pool").await)
        .send()
        .await
        .unwrap();
    assert_eq!(
        pool.user_pool()
            .unwrap()
            .lambda_config()
            .and_then(|c| c.pre_sign_up()),
        Some(fn_arn)
    );

    let logs = server.logs_client().await;
    let filters = logs
        .describe_subscription_filters()
        .log_group_name("/sam/app")
        .send()
        .await
        .unwrap();
    let filter = &filters.subscription_filters()[0];
    assert_eq!(filter.destination_arn(), Some(fn_arn));
    assert_eq!(filter.filter_pattern(), Some("ERROR"));

    let iot = aws_sdk_iot::Client::from_conf(
        aws_sdk_iot::config::Builder::from(&server.aws_config().await).build(),
    );
    let rule = iot
        .get_topic_rule()
        .rule_name(physical("HandlerTelemetry").await)
        .send()
        .await
        .unwrap();
    let rule = rule.rule().unwrap();
    assert_eq!(rule.sql(), Some("SELECT * FROM 'sensors/+'"));
    assert_eq!(
        rule.actions()[0].lambda().map(|l| l.function_arn()),
        Some(fn_arn)
    );

    let lambda = server.lambda_client().await;
    let policy = lambda
        .get_policy()
        .function_name("sam-trigger-fn")
        .send()
        .await
        .unwrap();
    let policy = policy.policy().unwrap();
    for principal in [
        "cognito-idp.amazonaws.com",
        "logs.amazonaws.com",
        "iot.amazonaws.com",
    ] {
        assert!(
            policy.contains(principal),
            "{principal} may invoke: {policy}"
        );
    }
}
