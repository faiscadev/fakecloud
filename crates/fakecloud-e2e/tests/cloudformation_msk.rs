//! CloudFormation provisions every `AWS::MSK::*` type as real records in the
//! `kafka` service control plane: a cluster and a serverless cluster (settling
//! to ACTIVE), a configuration, a cluster policy, a batch SCRAM-secret
//! association, a VPC connection, and a replicator. Each reads back through the
//! MSK API, exposes its ARN via `Ref` and its documented attributes via
//! `Fn::GetAtt`, and honors dependency order (the policy / scram / VPC-connection
//! reference the cluster's ARN). Deleting the stack removes them all.
//!
//! The default test runs with the Kafka container backend DISABLED
//! (`FAKECLOUD_KAFKA_DISABLE_BACKEND=1`), so the cluster settles ACTIVE through
//! the in-memory control plane and no real broker is spawned -- it exercises the
//! Ref / GetAtt / Outputs / write-through-persistence path in the shared E2E
//! partition. The real-broker variant is gated behind `FAKECLOUD_E2E_MSK=1` and
//! runs only in the dedicated `msk-broker` CI job.

mod helpers;

use helpers::TestServer;

// One of every AWS::MSK::* resource. Outputs surface Ref (the ARN) and the
// GetAtt attributes so the test can assert intrinsic-function resolution. The
// policy / scram-secret / VPC-connection reference the cluster via Ref, so the
// multi-pass provisioner must order the cluster first.
const TEMPLATE: &str = r#"{
  "Resources": {
    "Config": {
      "Type": "AWS::MSK::Configuration",
      "Properties": {
        "Name": "cfn-msk-config",
        "ServerProperties": "auto.create.topics.enable=true\ndelete.topic.enable=true",
        "KafkaVersionsList": ["3.6.0"]
      }
    },
    "Cluster": {
      "Type": "AWS::MSK::Cluster",
      "Properties": {
        "ClusterName": "cfn-msk-cluster",
        "KafkaVersion": "3.6.0",
        "NumberOfBrokerNodes": 3,
        "BrokerNodeGroupInfo": {
          "InstanceType": "kafka.m5.large",
          "ClientSubnets": [
            "subnet-0123456789abcdef0",
            "subnet-0123456789abcdef1",
            "subnet-0123456789abcdef2"
          ],
          "StorageInfo": { "EBSStorageInfo": { "VolumeSize": 100 } }
        },
        "Tags": { "env": "test" }
      }
    },
    "Serverless": {
      "Type": "AWS::MSK::ServerlessCluster",
      "Properties": {
        "ClusterName": "cfn-msk-serverless",
        "VpcConfigs": [
          {
            "SubnetIds": ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"],
            "SecurityGroups": ["sg-0123456789abcdef0"]
          }
        ],
        "ClientAuthentication": { "Sasl": { "Iam": { "Enabled": true } } }
      }
    },
    "Policy": {
      "Type": "AWS::MSK::ClusterPolicy",
      "Properties": {
        "ClusterArn": { "Ref": "Cluster" },
        "Policy": {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": { "AWS": "arn:aws:iam::123456789012:root" },
              "Action": "kafka:CreateVpcConnection",
              "Resource": { "Ref": "Cluster" }
            }
          ]
        }
      }
    },
    "Scram": {
      "Type": "AWS::MSK::BatchScramSecret",
      "Properties": {
        "ClusterArn": { "Ref": "Cluster" },
        "SecretArnList": [
          "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_cfn-abc123"
        ]
      }
    },
    "VpcConn": {
      "Type": "AWS::MSK::VpcConnection",
      "Properties": {
        "Authentication": "SASL_IAM",
        "TargetClusterArn": { "Ref": "Cluster" },
        "VpcId": "vpc-0123456789abcdef0",
        "ClientSubnets": ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"],
        "SecurityGroups": ["sg-0123456789abcdef0"]
      }
    },
    "Replicator": {
      "Type": "AWS::MSK::Replicator",
      "Properties": {
        "ReplicatorName": "cfn-msk-replicator",
        "ServiceExecutionRoleArn": "arn:aws:iam::123456789012:role/msk-replicator",
        "KafkaClusters": [
          {
            "AmazonMskCluster": { "MskClusterArn": { "Ref": "Cluster" } },
            "VpcConfig": {
              "SubnetIds": ["subnet-0123456789abcdef0"],
              "SecurityGroupIds": ["sg-0123456789abcdef0"]
            }
          }
        ],
        "ReplicationInfoList": [
          {
            "SourceKafkaClusterArn": { "Ref": "Cluster" },
            "TargetKafkaClusterArn": { "Ref": "Cluster" },
            "TargetCompressionType": "NONE",
            "TopicReplication": { "TopicsToReplicate": ["*"] },
            "ConsumerGroupReplication": { "ConsumerGroupsToReplicate": ["*"] }
          }
        ]
      }
    }
  },
  "Outputs": {
    "ClusterRef":    { "Value": { "Ref": "Cluster" } },
    "ClusterArn":    { "Value": { "Fn::GetAtt": ["Cluster", "Arn"] } },
    "ServerlessRef": { "Value": { "Ref": "Serverless" } },
    "ConfigRef":     { "Value": { "Ref": "Config" } },
    "ConfigArn":     { "Value": { "Fn::GetAtt": ["Config", "Arn"] } },
    "PolicyRef":     { "Value": { "Ref": "Policy" } },
    "PolicyVersion": { "Value": { "Fn::GetAtt": ["Policy", "CurrentVersion"] } },
    "VpcConnRef":    { "Value": { "Ref": "VpcConn" } },
    "VpcConnArn":    { "Value": { "Fn::GetAtt": ["VpcConn", "Arn"] } },
    "ReplicatorRef": { "Value": { "Ref": "Replicator" } },
    "ReplicatorArn": { "Value": { "Fn::GetAtt": ["Replicator", "ReplicatorArn"] } }
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
async fn cfn_provisions_msk_resources_control_plane() {
    // Control-plane only: the Kafka container backend is disabled, so the
    // cluster settles ACTIVE in memory and no real broker is spawned. This runs
    // in the shared E2E partition and always exercises Ref/GetAtt/Outputs +
    // write-through to the kafka service.
    let s = TestServer::start_with_env(&[("FAKECLOUD_KAFKA_DISABLE_BACKEND", "1")]).await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let kafka = aws_sdk_kafka::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("msk-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("msk-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Intrinsic-function resolution (Ref + GetAtt) ---
    let cluster_ref = output(stack, "ClusterRef");
    let cluster_arn = output(stack, "ClusterArn");
    let config_ref = output(stack, "ConfigRef");
    let config_arn = output(stack, "ConfigArn");
    let vpcconn_ref = output(stack, "VpcConnRef");
    let replicator_ref = output(stack, "ReplicatorRef");

    // Every resource's Ref is its ARN; GetAtt Arn matches Ref.
    assert!(
        cluster_ref.starts_with("arn:aws:kafka:")
            && cluster_ref.contains(":cluster/cfn-msk-cluster/"),
        "cluster ref {cluster_ref}"
    );
    assert_eq!(cluster_ref, cluster_arn, "cluster Ref == GetAtt Arn");
    assert_eq!(config_ref, config_arn, "config Ref == GetAtt Arn");
    assert!(
        config_ref.contains(":configuration/cfn-msk-config/"),
        "config ref {config_ref}"
    );
    assert_eq!(output(stack, "VpcConnArn"), vpcconn_ref);
    assert!(
        vpcconn_ref.contains(":vpc-connection/"),
        "vpc-connection ref {vpcconn_ref}"
    );
    assert_eq!(output(stack, "ReplicatorArn"), replicator_ref);
    assert!(
        replicator_ref.contains(":replicator/cfn-msk-replicator/"),
        "replicator ref {replicator_ref}"
    );
    assert_eq!(
        output(stack, "PolicyRef"),
        cluster_ref,
        "policy Ref = cluster ARN"
    );
    assert_eq!(output(stack, "PolicyVersion"), "1");

    // --- The resources exist in the kafka service ---
    // The cluster settles ACTIVE via the in-memory control plane (backend off).
    let cluster = kafka
        .describe_cluster()
        .cluster_arn(cluster_ref)
        .send()
        .await
        .expect("DescribeCluster");
    let info = cluster.cluster_info().expect("cluster info");
    assert_eq!(info.cluster_name(), Some("cfn-msk-cluster"));
    assert_eq!(info.state().map(|st| st.as_str()), Some("ACTIVE"));
    assert_eq!(info.number_of_broker_nodes(), Some(3));

    // Serverless cluster reads back through the V2 describe as SERVERLESS/ACTIVE.
    let serverless_ref = output(stack, "ServerlessRef");
    let sv = kafka
        .describe_cluster_v2()
        .cluster_arn(serverless_ref)
        .send()
        .await
        .expect("DescribeClusterV2");
    let sv_info = sv.cluster_info().expect("serverless cluster info");
    assert_eq!(
        sv_info.cluster_type().map(|t| t.as_str()),
        Some("SERVERLESS")
    );
    assert_eq!(sv_info.state().map(|st| st.as_str()), Some("ACTIVE"));

    // Configuration round-trips.
    let config = kafka
        .describe_configuration()
        .arn(config_ref)
        .send()
        .await
        .expect("DescribeConfiguration");
    assert_eq!(config.name(), Some("cfn-msk-config"));
    assert_eq!(config.arn(), Some(config_arn));

    // Cluster policy round-trips (version 1).
    let policy = kafka
        .get_cluster_policy()
        .cluster_arn(cluster_ref)
        .send()
        .await
        .expect("GetClusterPolicy");
    assert_eq!(policy.current_version(), Some("1"));
    assert!(
        policy
            .policy()
            .unwrap_or_default()
            .contains("kafka:CreateVpcConnection"),
        "policy body should round-trip"
    );

    // The batch SCRAM-secret association is recorded on the cluster.
    let secrets = kafka
        .list_scram_secrets()
        .cluster_arn(cluster_ref)
        .send()
        .await
        .expect("ListScramSecrets");
    assert!(
        secrets
            .secret_arn_list()
            .iter()
            .any(|a| a.contains("AmazonMSK_cfn-abc123")),
        "CFN-declared SCRAM secret should be associated"
    );

    // The VPC connection round-trips.
    let vc = kafka
        .describe_vpc_connection()
        .arn(vpcconn_ref)
        .send()
        .await
        .expect("DescribeVpcConnection");
    assert_eq!(vc.vpc_connection_arn(), Some(vpcconn_ref));
    assert_eq!(vc.target_cluster_arn(), Some(cluster_ref));

    // The replicator round-trips.
    let rep = kafka
        .describe_replicator()
        .replicator_arn(replicator_ref)
        .send()
        .await
        .expect("DescribeReplicator");
    assert_eq!(rep.replicator_name(), Some("cfn-msk-replicator"));

    // Cluster tags applied at create time round-trip through ListTagsForResource.
    let tags = kafka
        .list_tags_for_resource()
        .resource_arn(cluster_ref)
        .send()
        .await
        .expect("ListTagsForResource");
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("test")
    );

    // --- Deleting the stack removes the resources ---
    cfn.delete_stack()
        .stack_name("msk-stack")
        .send()
        .await
        .unwrap();

    let cluster_gone = kafka
        .describe_cluster()
        .cluster_arn(cluster_ref)
        .send()
        .await;
    assert!(
        cluster_gone.is_err(),
        "stack delete should remove the cluster"
    );
    let config_gone = kafka.describe_configuration().arn(config_ref).send().await;
    assert!(
        config_gone.is_err(),
        "stack delete should remove the configuration"
    );
}

#[tokio::test]
async fn cfn_provisions_msk_broker_cluster() {
    // Spawns a REAL Apache Kafka broker container -> runs only in the dedicated,
    // resourced `msk-broker` CI job (which sets FAKECLOUD_E2E_MSK=1), not the
    // shared E2E partition.
    if std::env::var("FAKECLOUD_E2E_MSK").as_deref() != Ok("1") {
        eprintln!("Skipping cfn_provisions_msk_broker_cluster: FAKECLOUD_E2E_MSK!=1 (runs in the dedicated msk-broker CI job)");
        return;
    }
    // A single provisioned cluster backed by a real broker.
    const BROKER_TEMPLATE: &str = r#"{
  "Resources": {
    "Cluster": {
      "Type": "AWS::MSK::Cluster",
      "Properties": {
        "ClusterName": "cfn-msk-broker",
        "KafkaVersion": "3.6.0",
        "NumberOfBrokerNodes": 1,
        "BrokerNodeGroupInfo": {
          "InstanceType": "kafka.m5.large",
          "ClientSubnets": ["subnet-0123456789abcdef0"]
        }
      }
    }
  },
  "Outputs": {
    "ClusterRef": { "Value": { "Ref": "Cluster" } }
  }
}"#;

    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let kafka = aws_sdk_kafka::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("msk-broker-stack")
        .template_body(BROKER_TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("msk-broker-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");
    let cluster_ref = output(stack, "ClusterRef").to_string();

    // CloudFormation backgrounds the real broker container; poll DescribeCluster
    // until it settles ACTIVE (broker boot + first connection is slow).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        let resp = kafka
            .describe_cluster()
            .cluster_arn(&cluster_ref)
            .send()
            .await
            .expect("DescribeCluster");
        let state = resp
            .cluster_info()
            .and_then(|i| i.state())
            .map(|st| st.as_str())
            .unwrap_or("");
        if state == "ACTIVE" {
            break;
        }
        if state == "FAILED" {
            panic!("CFN MSK broker cluster reached FAILED");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "CFN MSK cluster did not reach ACTIVE (last state: {state})"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // A real, reachable bootstrap endpoint (not the cosmetic *.amazonaws.com).
    let bb = kafka
        .get_bootstrap_brokers()
        .cluster_arn(&cluster_ref)
        .send()
        .await
        .expect("GetBootstrapBrokers");
    let bootstrap = bb
        .bootstrap_broker_string()
        .expect("bootstrap broker string")
        .to_string();
    assert!(
        !bootstrap.contains("amazonaws.com") && bootstrap.contains(':'),
        "bootstrap must be a real mapped host:port, got {bootstrap}"
    );

    cfn.delete_stack()
        .stack_name("msk-broker-stack")
        .send()
        .await
        .unwrap();
}
