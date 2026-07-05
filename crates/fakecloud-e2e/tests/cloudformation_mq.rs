//! CloudFormation provisions AWS::AmazonMQ::Broker and
//! AWS::AmazonMQ::Configuration as real records in the `mq` service control
//! plane: they read back through DescribeBroker / DescribeConfiguration, expose
//! their id via Ref and their documented attributes via Fn::GetAtt, and honor
//! dependency order. Deleting the stack removes both resources.

mod helpers;

use helpers::TestServer;

// A configuration plus a broker. Outputs surface Ref (the resource id) and the
// GetAtt attributes so the test can assert intrinsic-function resolution.
const TEMPLATE: &str = r#"{
  "Resources": {
    "MyConfig": {
      "Type": "AWS::AmazonMQ::Configuration",
      "Properties": {
        "Name": "cfn-mq-config",
        "EngineType": "ACTIVEMQ",
        "EngineVersion": "5.18",
        "Data": "PGJyb2tlcj48L2Jyb2tlcj4=",
        "Tags": [ { "Key": "env", "Value": "test" } ]
      }
    },
    "MyBroker": {
      "Type": "AWS::AmazonMQ::Broker",
      "Properties": {
        "BrokerName": "cfn-mq-broker",
        "EngineType": "RABBITMQ",
        "EngineVersion": "3.13",
        "HostInstanceType": "mq.m5.large",
        "DeploymentMode": "SINGLE_INSTANCE",
        "PubliclyAccessible": false,
        "AutoMinorVersionUpgrade": false,
        "Users": [ { "Username": "admin", "Password": "SuperSecret1234" } ]
      }
    }
  },
  "Outputs": {
    "BrokerRef":     { "Value": { "Ref": "MyBroker" } },
    "BrokerArn":     { "Value": { "Fn::GetAtt": ["MyBroker", "Arn"] } },
    "BrokerAmqp":    { "Value": { "Fn::GetAtt": ["MyBroker", "AmqpEndpoints"] } },
    "BrokerIp":      { "Value": { "Fn::GetAtt": ["MyBroker", "IpAddresses"] } },
    "ConfigRef":     { "Value": { "Ref": "MyConfig" } },
    "ConfigArn":     { "Value": { "Fn::GetAtt": ["MyConfig", "Arn"] } },
    "ConfigId":      { "Value": { "Fn::GetAtt": ["MyConfig", "Id"] } },
    "ConfigRev":     { "Value": { "Fn::GetAtt": ["MyConfig", "Revision"] } }
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
async fn cfn_provisions_mq_broker_and_configuration() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let mq = aws_sdk_mq::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("mq-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("mq-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Intrinsic-function resolution (Ref + GetAtt) ---
    let broker_ref = output(stack, "BrokerRef");
    let broker_arn = output(stack, "BrokerArn");
    let config_ref = output(stack, "ConfigRef");
    let config_arn = output(stack, "ConfigArn");

    // Broker Ref resolves to the broker id (`b-<uuid>`); Configuration Ref to
    // the configuration id (`c-<uuid>`).
    assert!(broker_ref.starts_with("b-"), "broker ref {broker_ref}");
    assert!(config_ref.starts_with("c-"), "config ref {config_ref}");
    assert!(
        broker_arn.contains(":broker:cfn-mq-broker:"),
        "broker arn {broker_arn}"
    );
    assert!(
        broker_arn.ends_with(broker_ref),
        "broker arn {broker_arn} ends with id"
    );
    assert!(
        config_arn.ends_with(&format!(":configuration:{config_ref}")),
        "config arn {config_arn}"
    );
    // RabbitMQ AMQP endpoint + a synthesized IP surface via GetAtt.
    assert!(
        output(stack, "BrokerAmqp").starts_with("amqps://"),
        "amqp endpoint {}",
        output(stack, "BrokerAmqp")
    );
    assert!(!output(stack, "BrokerIp").is_empty());
    assert_eq!(output(stack, "ConfigId"), config_ref);
    assert_eq!(output(stack, "ConfigRev"), "1");

    // --- The resources exist in the mq service ---
    let broker = mq
        .describe_broker()
        .broker_id(broker_ref)
        .send()
        .await
        .expect("DescribeBroker");
    assert_eq!(broker.broker_arn(), Some(broker_arn));
    assert_eq!(broker.broker_name(), Some("cfn-mq-broker"));
    assert_eq!(broker.engine_type().map(|e| e.as_str()), Some("RABBITMQ"));
    // CloudFormation provisions the broker already RUNNING.
    assert_eq!(broker.broker_state().map(|s| s.as_str()), Some("RUNNING"));

    let config = mq
        .describe_configuration()
        .configuration_id(config_ref)
        .send()
        .await
        .expect("DescribeConfiguration");
    assert_eq!(config.arn(), Some(config_arn));
    assert_eq!(config.name(), Some("cfn-mq-config"));
    assert_eq!(config.engine_type().map(|e| e.as_str()), Some("ACTIVEMQ"));

    // Tags applied at create time round-trip through ListTags.
    let tags = mq
        .list_tags()
        .resource_arn(config_arn)
        .send()
        .await
        .expect("ListTags");
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("test")
    );

    // --- Deleting the stack removes both resources ---
    cfn.delete_stack()
        .stack_name("mq-stack")
        .send()
        .await
        .unwrap();

    let broker_gone = mq.describe_broker().broker_id(broker_ref).send().await;
    assert!(
        broker_gone.is_err(),
        "stack delete should remove the broker"
    );
    let config_gone = mq
        .describe_configuration()
        .configuration_id(config_ref)
        .send()
        .await;
    assert!(
        config_gone.is_err(),
        "stack delete should remove the configuration"
    );
}
