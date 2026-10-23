//! Provisioner fidelity tests, second batch: stack updates that reach the
//! owning service the way its own API does (SQS, Lambda, ECS, Kinesis, SNS),
//! RDS cluster membership and endpoints, AWS-shaped `Ref` values, and the
//! resource types that used to be recorded with no backing state.

use super::tests::{make_provisioner, make_resource};
use super::*;
use serde_json::json;

const ACCT: &str = "123456789012";

fn create(
    prov: &ResourceProvisioner,
    ty: &str,
    id: &str,
    props: serde_json::Value,
) -> StackResource {
    prov.create_resource(&make_resource(ty, id, props))
        .unwrap_or_else(|e| panic!("create {id}: {e}"))
}

fn update(
    prov: &ResourceProvisioner,
    existing: &StackResource,
    props: serde_json::Value,
) -> StackResource {
    prov.update_resource(
        existing,
        &make_resource(&existing.resource_type, &existing.logical_id, props),
    )
    .unwrap_or_else(|e| panic!("update {}: {e}", existing.logical_id))
    .expect("updatable")
}

// ---------------------------------------------------------------------------
// SQS
// ---------------------------------------------------------------------------

#[test]
fn sqs_update_refreshes_redrive_resets_dropped_properties_and_stamps_times() {
    let prov = make_provisioner();
    let dlq = create(&prov, "AWS::SQS::Queue", "Dlq", json!({"QueueName": "dlq"}));
    let q = create(
        &prov,
        "AWS::SQS::Queue",
        "Q",
        json!({"QueueName": "work", "VisibilityTimeout": 90}),
    );
    {
        let sqs = prov.sqs_state.read();
        let queue = &sqs.regional(ACCT, &prov.region).unwrap().queues[&q.physical_id];
        assert!(queue.attributes.contains_key("CreatedTimestamp"));
        assert!(queue.attributes.contains_key("LastModifiedTimestamp"));
        assert!(queue.redrive_policy.is_none());
    }
    let dlq_arn = dlq.attributes["Arn"].clone();
    update(
        &prov,
        &q,
        json!({
            "QueueName": "work",
            "RedrivePolicy": {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 3}
        }),
    );
    let sqs = prov.sqs_state.read();
    let queue = &sqs.regional(ACCT, &prov.region).unwrap().queues[&q.physical_id];
    // The typed policy DLQ routing reads is set, not just the attribute.
    let rp = queue.redrive_policy.as_ref().expect("typed redrive policy");
    assert_eq!(rp.dead_letter_target_arn, dlq_arn);
    assert_eq!(rp.max_receive_count, 3);
    // VisibilityTimeout was dropped from the template: back to the default.
    assert_eq!(queue.attributes["VisibilityTimeout"], "30");
}

#[test]
fn sqs_update_keeps_the_policy_a_queue_policy_resource_owns() {
    let prov = make_provisioner();
    let q = create(
        &prov,
        "AWS::SQS::Queue",
        "Q",
        json!({"QueueName": "pq", "DelaySeconds": 5}),
    );
    create(
        &prov,
        "AWS::SQS::QueuePolicy",
        "QP",
        json!({
            "Queues": [q.physical_id.clone()],
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage", "Resource": "*"
            }]}
        }),
    );
    update(&prov, &q, json!({"QueueName": "pq"}));
    let sqs = prov.sqs_state.read();
    let queue = &sqs.regional(ACCT, &prov.region).unwrap().queues[&q.physical_id];
    assert!(
        queue
            .attributes
            .get("Policy")
            .is_some_and(|p| p.contains("sqs:SendMessage")),
        "QueuePolicy's Policy must survive a Queue update: {:?}",
        queue.attributes
    );
    // A Queue-owned property dropped from the template still resets.
    assert_eq!(queue.attributes["DelaySeconds"], "0");
}

// ---------------------------------------------------------------------------
// Lambda
// ---------------------------------------------------------------------------

fn function_props(extra: serde_json::Value) -> serde_json::Value {
    let mut props = json!({
        "FunctionName": "fn-a",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": "def handler(e, c): return e"}
    });
    if let (Some(p), Some(e)) = (props.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            p.insert(k.clone(), v.clone());
        }
    }
    props
}

#[test]
fn lambda_image_config_reserved_concurrency_and_tags_follow_the_template() {
    let prov = make_provisioner();
    let f = create(
        &prov,
        "AWS::Lambda::Function",
        "F",
        json!({
            "FunctionName": "img",
            "PackageType": "Image",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Code": {"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/app:latest"},
            "ImageConfig": {"Command": ["app.handler"], "WorkingDirectory": "/srv"},
            "ReservedConcurrentExecutions": 5,
            "Tags": [{"Key": "team", "Value": "a"}]
        }),
    );
    {
        let lambda = prov.lambda_state.read();
        let st = lambda.regional(ACCT, "us-east-1").unwrap();
        let func = &st.functions["img"];
        assert_eq!(
            func.image_config.as_ref().unwrap()["Command"][0],
            "app.handler"
        );
        assert_eq!(st.function_concurrency.get("img"), Some(&5));
    }
    update(
        &prov,
        &f,
        json!({
            "FunctionName": "img",
            "PackageType": "Image",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Code": {"ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/app:latest"},
            "Tags": []
        }),
    );
    let lambda = prov.lambda_state.read();
    let st = lambda.regional(ACCT, "us-east-1").unwrap();
    let func = &st.functions["img"];
    assert!(func.image_config.is_none());
    assert!(func.tags.is_empty(), "an empty Tags list clears the tags");
    assert!(!st.function_concurrency.contains_key("img"));
}

#[test]
fn lambda_event_invoke_config_is_stored_for_the_qualifier() {
    let prov = make_provisioner();
    create(
        &prov,
        "AWS::Lambda::Function",
        "F",
        function_props(json!({})),
    );
    let cfg = create(
        &prov,
        "AWS::Lambda::EventInvokeConfig",
        "Cfg",
        json!({
            "FunctionName": "fn-a",
            "Qualifier": "$LATEST",
            "MaximumRetryAttempts": 0,
            "MaximumEventAgeInSeconds": 120,
            "DestinationConfig": {"OnFailure": {"Destination": "arn:aws:sqs:us-east-1:123456789012:dlq"}}
        }),
    );
    assert_eq!(cfg.physical_id, "fn-a:$LATEST");
    {
        let lambda = prov.lambda_state.read();
        let c = &lambda
            .regional(ACCT, "us-east-1")
            .unwrap()
            .event_invoke_configs["fn-a:$LATEST"];
        assert_eq!(c.maximum_retry_attempts, 0);
        assert_eq!(c.maximum_event_age, 120);
        assert!(c.destination_config.is_some());
    }
    let err = prov
        .create_resource(&make_resource(
            "AWS::Lambda::EventInvokeConfig",
            "Bad",
            json!({"FunctionName": "fn-a", "Qualifier": "$LATEST", "MaximumRetryAttempts": 5}),
        ))
        .unwrap_err();
    assert!(err.contains("MaximumRetryAttempts"), "{err}");
    prov.delete_resource(&cfg).unwrap();
    assert!(prov
        .lambda_state
        .read()
        .regional(ACCT, "us-east-1")
        .unwrap()
        .event_invoke_configs
        .is_empty());
}

// ---------------------------------------------------------------------------
// RDS
// ---------------------------------------------------------------------------

#[test]
fn rds_cluster_defaults_port_per_engine_stores_password_and_lists_members() {
    let prov = make_provisioner();
    let cluster = create(
        &prov,
        "AWS::RDS::DBCluster",
        "C",
        json!({
            "DBClusterIdentifier": "aurora1",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "s3cretpw"
        }),
    );
    assert_eq!(cluster.attributes["Endpoint.Port"], "3306");
    let hash = fakecloud_rds::endpoint_hash(ACCT, "us-east-1");
    assert_eq!(
        cluster.attributes["Endpoint.Address"],
        format!("aurora1.cluster-{hash}.us-east-1.rds.amazonaws.com")
    );
    let writer = create(
        &prov,
        "AWS::RDS::DBInstance",
        "W",
        json!({
            "DBInstanceIdentifier": "aurora1-a",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
            "DBClusterIdentifier": "aurora1"
        }),
    );
    create(
        &prov,
        "AWS::RDS::DBInstance",
        "R",
        json!({
            "DBInstanceIdentifier": "aurora1-b",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
            "DBClusterIdentifier": "aurora1"
        }),
    );
    assert_eq!(
        writer.attributes["Endpoint.Address"],
        format!("aurora1-a.{hash}.us-east-1.rds.amazonaws.com")
    );
    {
        let rds = prov.rds_state.read();
        let st = rds.get(ACCT).unwrap();
        let c = &st.extras["clusters"]["aurora1"];
        assert_eq!(c["MasterUserPassword"], "s3cretpw");
        let members = c["DBClusterMembers"].as_array().unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(c["WriterDBInstanceIdentifier"], "aurora1-a");
        // The member runs on the cluster's port and credentials.
        let inst = &st.instances["aurora1-a"];
        assert_eq!(inst.port, 3306);
        assert_eq!(inst.master_user_password, "s3cretpw");
        assert_eq!(inst.master_username, "admin");
    }
    // Deleting the writer promotes the remaining member.
    prov.delete_resource(&writer).unwrap();
    let rds = prov.rds_state.read();
    let c = &rds.get(ACCT).unwrap().extras["clusters"]["aurora1"];
    assert_eq!(c["DBClusterMembers"].as_array().unwrap().len(), 1);
    assert_eq!(c["WriterDBInstanceIdentifier"], "aurora1-b");
}

#[test]
fn rds_instance_endpoint_get_att_is_live() {
    let prov = make_provisioner();
    let inst = create(
        &prov,
        "AWS::RDS::DBInstance",
        "D",
        json!({"DBInstanceIdentifier": "pg1", "Engine": "postgres", "MasterUserPassword": "pw123456"}),
    );
    // A container coming up rebinds the endpoint after create.
    {
        let mut rds = prov.rds_state.write();
        let i = rds.get_or_create(ACCT).instances.get_mut("pg1").unwrap();
        i.endpoint_address = "127.0.0.1".to_string();
        i.port = 54321;
    }
    assert_eq!(
        prov.get_att(&inst, "Endpoint.Address").as_deref(),
        Some("127.0.0.1")
    );
    assert_eq!(
        prov.get_att(&inst, "Endpoint.Port").as_deref(),
        Some("54321")
    );
}

// ---------------------------------------------------------------------------
// ECS
// ---------------------------------------------------------------------------

fn ecs_basics(prov: &ResourceProvisioner) -> (StackResource, StackResource) {
    create(prov, "AWS::ECS::Cluster", "C", json!({"ClusterName": "c1"}));
    let td = create(
        prov,
        "AWS::ECS::TaskDefinition",
        "TD",
        json!({
            "Family": "web",
            "ContainerDefinitions": [{"Name": "app", "Image": "nginx", "Essential": true}]
        }),
    );
    let td2 = create(
        prov,
        "AWS::ECS::TaskDefinition",
        "TD2",
        json!({
            "Family": "web",
            "ContainerDefinitions": [{"Name": "app", "Image": "nginx:2", "Essential": true}]
        }),
    );
    (td, td2)
}

#[test]
fn ecs_service_starts_a_primary_deployment_with_its_circuit_breaker() {
    let prov = make_provisioner();
    let (td, td2) = ecs_basics(&prov);
    let svc = create(
        &prov,
        "AWS::ECS::Service",
        "S",
        json!({
            "ServiceName": "web",
            "Cluster": "c1",
            "TaskDefinition": td.physical_id,
            "DesiredCount": 2,
            "DeploymentConfiguration": {
                "DeploymentCircuitBreaker": {"Enable": true, "Rollback": true}
            }
        }),
    );
    {
        let ecs = prov.ecs_state.read();
        let s = &ecs.get(ACCT).unwrap().services["c1/web"];
        assert_eq!(s.deployments.len(), 1);
        assert_eq!(s.deployments[0].status, "PRIMARY");
        assert_eq!(s.deployments[0].desired_count, 2);
        assert_eq!(s.deployments[0].task_definition_arn, td.physical_id);
        let cb = s.circuit_breaker.as_ref().expect("circuit breaker");
        assert!(cb.enable && cb.rollback);
    }
    // A new task definition rolls a new PRIMARY and demotes the old one.
    update(
        &prov,
        &svc,
        json!({
            "ServiceName": "web",
            "Cluster": "c1",
            "TaskDefinition": td2.physical_id,
            "DesiredCount": 3
        }),
    );
    let ecs = prov.ecs_state.read();
    let s = &ecs.get(ACCT).unwrap().services["c1/web"];
    assert_eq!(s.deployments.len(), 2);
    assert_eq!(s.deployments[0].status, "PRIMARY");
    assert_eq!(s.deployments[0].task_definition_arn, td2.physical_id);
    assert_eq!(s.deployments[0].desired_count, 3);
    assert_eq!(s.deployments[1].status, "ACTIVE");
    // Dropping the circuit breaker turns it off.
    assert!(s.circuit_breaker.is_none());
}

// ---------------------------------------------------------------------------
// Kinesis
// ---------------------------------------------------------------------------

#[test]
fn kinesis_tags_encryption_and_reshard_keep_records() {
    let prov = make_provisioner();
    let s = create(
        &prov,
        "AWS::Kinesis::Stream",
        "KS",
        json!({
            "Name": "events",
            "ShardCount": 1,
            "StreamEncryption": {"EncryptionType": "KMS", "KeyId": "alias/aws/kinesis"},
            "Tags": [{"Key": "env", "Value": "dev"}]
        }),
    );
    {
        let mut k = prov.kinesis_state.write();
        let stream = k
            .regional_mut(ACCT, "us-east-1")
            .streams
            .get_mut("events")
            .unwrap();
        assert_eq!(stream.encryption_type, "KMS");
        assert_eq!(stream.key_id.as_deref(), Some("alias/aws/kinesis"));
        assert_eq!(stream.tags["env"], "dev");
        stream.shards[0]
            .records
            .push(fakecloud_kinesis::KinesisRecord {
                sequence_number: "1".to_string(),
                partition_key: "pk".to_string(),
                data: b"hello".to_vec(),
                approximate_arrival_timestamp: Utc::now(),
            });
    }
    update(&prov, &s, json!({"Name": "events", "ShardCount": 2}));
    let k = prov.kinesis_state.read();
    let stream = &k.regional(ACCT, "us-east-1").unwrap().streams["events"];
    assert_eq!(stream.open_shard_count, 2);
    // The record written before the reshard is still in the (closed) parent.
    let parent = &stream.shards[0];
    assert!(!parent.is_open);
    assert_eq!(parent.records.len(), 1);
    assert_eq!(stream.encryption_type, "NONE");
    assert!(stream.key_id.is_none());
    assert!(stream.tags.is_empty());
}

// ---------------------------------------------------------------------------
// SNS
// ---------------------------------------------------------------------------

#[test]
fn sns_topic_update_applies_tags_and_diffs_inline_subscriptions() {
    let prov = make_provisioner();
    let topic = create(
        &prov,
        "AWS::SNS::Topic",
        "T",
        json!({
            "TopicName": "orders",
            "Tags": [{"Key": "a", "Value": "1"}],
            "Subscription": [
                {"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q1"},
                {"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q2"}
            ]
        }),
    );
    // A subscription made outside the template must survive the update.
    let arn = topic.physical_id.clone();
    {
        let mut sns = prov.sns_state.write();
        let st = sns.regional_mut(ACCT, "us-east-1");
        st.subscriptions.insert(
            format!("{arn}:manual"),
            SnsSubscription {
                subscription_arn: format!("{arn}:manual"),
                topic_arn: arn.clone(),
                protocol: "email".to_string(),
                endpoint: "ops@example.com".to_string(),
                owner: ACCT.to_string(),
                attributes: BTreeMap::new(),
                confirmed: true,
                confirmation_token: None,
            },
        );
    }
    let updated = update(
        &prov,
        &topic,
        json!({
            "TopicName": "orders",
            "Tags": [{"Key": "b", "Value": "2"}],
            "Subscription": [
                {"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q2"},
                {"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:q3"}
            ]
        }),
    );
    assert_eq!(updated.attributes["TopicArn"], arn);
    let sns = prov.sns_state.read();
    let st = sns.regional(ACCT, "us-east-1").unwrap();
    assert_eq!(
        st.topics[&arn].tags,
        vec![("b".to_string(), "2".to_string())]
    );
    let mut endpoints: Vec<&str> = st
        .subscriptions
        .values()
        .filter(|s| s.topic_arn == arn)
        .map(|s| s.endpoint.as_str())
        .collect();
    endpoints.sort();
    assert_eq!(
        endpoints,
        vec![
            "arn:aws:sqs:us-east-1:123456789012:q2",
            "arn:aws:sqs:us-east-1:123456789012:q3",
            "ops@example.com"
        ]
    );
}

#[test]
fn sns_subscription_to_another_accounts_topic_lands_with_the_topic() {
    let prov = make_provisioner();
    let topic_arn = "arn:aws:sns:us-east-1:111111111111:shared";
    {
        let mut sns = prov.sns_state.write();
        let st = sns.regional_mut("111111111111", "us-east-1");
        st.topics.insert(
            topic_arn.to_string(),
            SnsTopic {
                topic_arn: topic_arn.to_string(),
                name: "shared".to_string(),
                attributes: BTreeMap::new(),
                tags: Vec::new(),
                is_fifo: false,
                created_at: Utc::now(),
                subscriptions_deleted: 0,
                fifo_sequence: 0,
                dedup_cache: BTreeMap::new(),
            },
        );
    }
    let sub = create(
        &prov,
        "AWS::SNS::Subscription",
        "S",
        json!({
            "TopicArn": topic_arn,
            "Protocol": "sqs",
            "Endpoint": "arn:aws:sqs:us-east-1:123456789012:mine"
        }),
    );
    {
        let sns = prov.sns_state.read();
        let s = &sns
            .regional("111111111111", "us-east-1")
            .unwrap()
            .subscriptions[&sub.physical_id];
        assert_eq!(s.owner, ACCT);
    }
    prov.delete_resource(&sub).unwrap();
    assert!(prov
        .sns_state
        .read()
        .regional("111111111111", "us-east-1")
        .unwrap()
        .subscriptions
        .is_empty());

    // Under strict IAM the topic policy has to allow the subscriber.
    let mut strict = make_provisioner();
    strict.sns_state = prov.sns_state.clone();
    strict.iam_mode = fakecloud_core::auth::IamMode::Strict;
    let err = strict
        .create_resource(&make_resource(
            "AWS::SNS::Subscription",
            "S",
            json!({"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:mine"}),
        ))
        .unwrap_err();
    assert!(err.contains("AuthorizationError"), "{err}");
    prov.sns_state
        .write()
        .regional_mut("111111111111", "us-east-1")
        .topics
        .get_mut(topic_arn)
        .unwrap()
        .attributes
        .insert(
            "Policy".to_string(),
            json!({"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "sns:Subscribe",
                "Resource": topic_arn
            }]})
            .to_string(),
        );
    strict
        .create_resource(&make_resource(
            "AWS::SNS::Subscription",
            "S",
            json!({"TopicArn": topic_arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:mine"}),
        ))
        .expect("allowed by the topic policy");
}

// ---------------------------------------------------------------------------
// Ref values
// ---------------------------------------------------------------------------

#[test]
fn dynamodb_table_ref_is_the_table_name() {
    let prov = make_provisioner();
    let t = create(
        &prov,
        "AWS::DynamoDB::Table",
        "T",
        json!({
            "TableName": "items",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
        }),
    );
    assert_eq!(t.physical_id, "items");
    assert!(t.attributes["Arn"].ends_with(":table/items"));
    assert_eq!(prov.get_att(&t, "Arn"), Some(t.attributes["Arn"].clone()));
    // A stack recorded with the old ARN physical id still deletes.
    let legacy = StackResource {
        physical_id: t.attributes["Arn"].clone(),
        ..t.clone()
    };
    prov.delete_resource(&legacy).unwrap();
    assert!(prov
        .dynamodb_state
        .read()
        .regional(ACCT, "us-east-1")
        .unwrap()
        .tables
        .is_empty());
}

#[test]
fn events_rule_ref_and_tags() {
    let prov = make_provisioner();
    let r = create(
        &prov,
        "AWS::Events::Rule",
        "R",
        json!({
            "Name": "nightly",
            "ScheduleExpression": "rate(1 day)",
            "Tags": [{"Key": "k", "Value": "v"}]
        }),
    );
    assert_eq!(r.physical_id, "nightly");
    {
        let eb = prov.eventbridge_state.read();
        let rule = &eb.get(ACCT).unwrap().rules[&("default".to_string(), "nightly".to_string())];
        assert_eq!(rule.tags["k"], "v");
    }
    let r = update(
        &prov,
        &r,
        json!({"Name": "nightly", "ScheduleExpression": "rate(2 days)"}),
    );
    assert_eq!(r.physical_id, "nightly");
    {
        let eb = prov.eventbridge_state.read();
        let rule = &eb.get(ACCT).unwrap().rules[&("default".to_string(), "nightly".to_string())];
        assert_eq!(rule.schedule_expression.as_deref(), Some("rate(2 days)"));
        assert!(rule.tags.is_empty());
    }
    prov.delete_resource(&r).unwrap();
    assert!(prov
        .eventbridge_state
        .read()
        .get(ACCT)
        .unwrap()
        .rules
        .is_empty());
}

// ---------------------------------------------------------------------------
// EC2 networking
// ---------------------------------------------------------------------------

#[test]
fn ec2_networking_resources_exist_in_ec2() {
    let prov = make_provisioner();
    let vpc = create(
        &prov,
        "AWS::EC2::VPC",
        "V",
        json!({"CidrBlock": "10.9.0.0/16"}),
    );
    let subnet = create(
        &prov,
        "AWS::EC2::Subnet",
        "S",
        json!({"VpcId": vpc.physical_id, "CidrBlock": "10.9.1.0/24"}),
    );
    let igw = create(&prov, "AWS::EC2::InternetGateway", "I", json!({}));
    let att = create(
        &prov,
        "AWS::EC2::VPCGatewayAttachment",
        "A",
        json!({"VpcId": vpc.physical_id, "InternetGatewayId": igw.physical_id}),
    );
    assert_eq!(att.physical_id, format!("IGW|{}", vpc.physical_id));
    let rtb = create(
        &prov,
        "AWS::EC2::RouteTable",
        "RT",
        json!({"VpcId": vpc.physical_id}),
    );
    let route = create(
        &prov,
        "AWS::EC2::Route",
        "Rt",
        json!({
            "RouteTableId": rtb.physical_id,
            "DestinationCidrBlock": "0.0.0.0/0",
            "GatewayId": igw.physical_id
        }),
    );
    let eip = create(&prov, "AWS::EC2::EIP", "E", json!({"Domain": "vpc"}));
    let nat = create(
        &prov,
        "AWS::EC2::NatGateway",
        "N",
        json!({"SubnetId": subnet.physical_id, "AllocationId": eip.attributes["AllocationId"]}),
    );
    let sg = create(
        &prov,
        "AWS::EC2::SecurityGroup",
        "SG",
        json!({"GroupDescription": "web", "VpcId": vpc.physical_id}),
    );
    let ingress = create(
        &prov,
        "AWS::EC2::SecurityGroupIngress",
        "In",
        json!({
            "GroupId": sg.physical_id,
            "IpProtocol": "tcp",
            "FromPort": 443,
            "ToPort": 443,
            "CidrIp": "0.0.0.0/0"
        }),
    );
    assert!(
        ingress.physical_id.starts_with("sgr-"),
        "{}",
        ingress.physical_id
    );
    {
        let ec2 = prov.ec2_state.read();
        let st = ec2.regional(ACCT, "us-east-1").unwrap();
        assert!(st.internet_gateways[&igw.physical_id]
            .attachments
            .iter()
            .any(|(v, _)| *v == vpc.physical_id));
        assert!(st
            .elastic_ips
            .values()
            .any(|e| e.public_ip == eip.physical_id));
        assert!(st.nat_gateways.contains_key(&nat.physical_id));
        assert!(st.security_groups[&sg.physical_id]
            .rules
            .iter()
            .any(|r| r.rule_id == ingress.physical_id && r.from_port == 443));
        assert!(format!("{:?}", st.route_tables[&rtb.physical_id]).contains("0.0.0.0/0"));
    }
    for r in [&ingress, &nat, &eip, &route, &att] {
        prov.delete_resource(r).unwrap();
    }
    let ec2 = prov.ec2_state.read();
    let st = ec2.regional(ACCT, "us-east-1").unwrap();
    assert!(st.internet_gateways[&igw.physical_id]
        .attachments
        .is_empty());
    assert!(!st
        .elastic_ips
        .values()
        .any(|e| e.public_ip == eip.physical_id));
    assert!(!st.security_groups[&sg.physical_id]
        .rules
        .iter()
        .any(|r| r.rule_id == ingress.physical_id));
    assert!(!format!("{:?}", st.route_tables[&rtb.physical_id]).contains("0.0.0.0/0"));
}

// ---------------------------------------------------------------------------
// IAM::RolePolicy, DynamoDB::GlobalTable, Scheduler
// ---------------------------------------------------------------------------

#[test]
fn iam_role_policy_is_an_inline_policy_on_the_role() {
    let prov = make_provisioner();
    create(
        &prov,
        "AWS::IAM::Role",
        "R",
        json!({"RoleName": "app", "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}}),
    );
    let rp = create(
        &prov,
        "AWS::IAM::RolePolicy",
        "P",
        json!({
            "RoleName": "app",
            "PolicyName": "read-items",
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}
            ]}
        }),
    );
    assert!(prov
        .iam_state
        .read()
        .get(ACCT)
        .unwrap()
        .role_inline_policies["app"]
        .contains_key("read-items"));
    prov.delete_resource(&rp).unwrap();
    assert!(!prov
        .iam_state
        .read()
        .get(ACCT)
        .unwrap()
        .role_inline_policies["app"]
        .contains_key("read-items"));
}

#[test]
fn dynamodb_global_table_creates_real_replicas_in_their_regions() {
    let prov = make_provisioner();
    let props = |replicas: serde_json::Value| {
        json!({
            "TableName": "global-items",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"},
            "Replicas": replicas
        })
    };
    let gt = create(
        &prov,
        "AWS::DynamoDB::GlobalTable",
        "G",
        props(json!([
            {"Region": "us-east-1", "DeletionProtectionEnabled": false},
            {"Region": "eu-west-1", "TableClass": "STANDARD_INFREQUENT_ACCESS"}
        ])),
    );
    assert_eq!(gt.physical_id, "global-items");
    assert!(gt.attributes.contains_key("StreamArn"));
    {
        let ddb = prov.dynamodb_state.read();
        let east = &ddb.regional(ACCT, "us-east-1").unwrap().tables["global-items"];
        assert_eq!(east.replica_regions, vec!["eu-west-1".to_string()]);
        let west = &ddb.regional(ACCT, "eu-west-1").unwrap().tables["global-items"];
        assert_eq!(
            west.arn,
            "arn:aws:dynamodb:eu-west-1:123456789012:table/global-items"
        );
        assert_eq!(west.replica_regions, vec!["us-east-1".to_string()]);
        assert_eq!(west.table_class, "STANDARD_INFREQUENT_ACCESS");
        assert!(west
            .stream_arn
            .as_deref()
            .unwrap()
            .starts_with("arn:aws:dynamodb:eu-west-1:"));
        // A 2019.11.21 global table is not a legacy one.
        assert!(ddb
            .regional(ACCT, "us-east-1")
            .unwrap()
            .global_tables
            .is_empty());
    }

    // A stack update swapping eu-west-1 for ap-south-1 moves the replica.
    let updated = prov
        .update_resource(
            &gt,
            &make_resource(
                "AWS::DynamoDB::GlobalTable",
                "G",
                props(json!([{"Region": "us-east-1"}, {"Region": "ap-south-1"}])),
            ),
        )
        .expect("update succeeds")
        .expect("GlobalTable is updatable");
    {
        let ddb = prov.dynamodb_state.read();
        assert!(!ddb
            .regional(ACCT, "eu-west-1")
            .unwrap()
            .tables
            .contains_key("global-items"));
        assert_eq!(
            ddb.regional(ACCT, "ap-south-1").unwrap().tables["global-items"].replica_regions,
            vec!["us-east-1".to_string()]
        );
    }

    // Deleting the stack resource removes every replica.
    prov.delete_resource(&updated).unwrap();
    let ddb = prov.dynamodb_state.read();
    for region in ["us-east-1", "eu-west-1", "ap-south-1"] {
        assert!(ddb
            .regional(ACCT, region)
            .is_none_or(|s| !s.tables.contains_key("global-items")));
    }
}

#[test]
fn scheduler_schedule_and_group_are_real() {
    let prov = make_provisioner();
    let group = create(
        &prov,
        "AWS::Scheduler::ScheduleGroup",
        "G",
        json!({"Name": "jobs"}),
    );
    assert!(group.attributes["Arn"].ends_with(":schedule-group/jobs"));
    let sched = create(
        &prov,
        "AWS::Scheduler::Schedule",
        "S",
        json!({
            "Name": "nightly",
            "GroupName": "jobs",
            "ScheduleExpression": "rate(1 day)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:123456789012:q",
                "RoleArn": "arn:aws:iam::123456789012:role/r"
            }
        }),
    );
    assert_eq!(sched.physical_id, "nightly");
    assert!(sched.attributes["Arn"].ends_with(":schedule/jobs/nightly"));
    let sched = update(
        &prov,
        &sched,
        json!({
            "Name": "nightly",
            "GroupName": "jobs",
            "ScheduleExpression": "rate(2 days)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:123456789012:q",
                "RoleArn": "arn:aws:iam::123456789012:role/r"
            }
        }),
    );
    {
        let sch = prov.scheduler_state.read();
        let st = sch.regional(ACCT, "us-east-1").unwrap();
        let s = &st.schedules[&("jobs".to_string(), "nightly".to_string())];
        assert_eq!(s.schedule_expression, "rate(2 days)");
        assert_eq!(s.target.arn, "arn:aws:sqs:us-east-1:123456789012:q");
    }
    // A missing required property fails the create the way the API does.
    let err = prov
        .create_resource(&make_resource(
            "AWS::Scheduler::Schedule",
            "Bad",
            json!({"Name": "x", "FlexibleTimeWindow": {"Mode": "OFF"}}),
        ))
        .unwrap_err();
    assert!(err.contains("ScheduleExpression"), "{err}");
    prov.delete_resource(&sched).unwrap();
    prov.delete_resource(&group).unwrap();
    let sch = prov.scheduler_state.read();
    let st = sch.regional(ACCT, "us-east-1").unwrap();
    assert!(st.schedules.is_empty());
    assert!(!st.groups.contains_key("jobs"));
}

// ---------------------------------------------------------------------------
// Batch consumable resources and service environments
// ---------------------------------------------------------------------------

#[test]
fn batch_consumable_resource_and_service_environment_go_through_batch() {
    let prov = make_provisioner();
    let cr = create(
        &prov,
        "AWS::Batch::ConsumableResource",
        "CR",
        json!({
            "ConsumableResourceName": "licenses",
            "TotalQuantity": 10,
            "ResourceType": "REPLENISHABLE",
            "Tags": {"Team": "ml"}
        }),
    );
    assert!(
        cr.physical_id.contains(":consumable-resource/licenses"),
        "{}",
        cr.physical_id
    );
    assert_eq!(cr.attributes["ConsumableResourceArn"], cr.physical_id);
    {
        let b = prov.batch_state.read();
        let st = b.get(ACCT).unwrap();
        assert_eq!(st.tags[&cr.physical_id]["Team"], "ml");
    }
    update(
        &prov,
        &cr,
        json!({"ConsumableResourceName": "licenses", "TotalQuantity": 25, "ResourceType": "REPLENISHABLE"}),
    );
    assert!(format!(
        "{:?}",
        prov.batch_state
            .read()
            .get(ACCT)
            .unwrap()
            .consumable_resources
    )
    .contains("25"));

    let env = create(
        &prov,
        "AWS::Batch::ServiceEnvironment",
        "SE",
        json!({
            "ServiceEnvironmentName": "training",
            "ServiceEnvironmentType": "SAGEMAKER_TRAINING",
            "CapacityLimits": [{"MaxCapacity": 4, "CapacityUnit": "NUM_INSTANCES"}]
        }),
    );
    assert!(
        env.physical_id.contains(":service-environment/training"),
        "{}",
        env.physical_id
    );
    // Delete disables first, then deletes (an ENABLED environment can't be).
    prov.delete_resource(&env).unwrap();
    prov.delete_resource(&cr).unwrap();
    let b = prov.batch_state.read();
    let st = b.get(ACCT).unwrap();
    assert!(st.service_environments.is_empty());
    assert!(st.consumable_resources.is_empty());
}

#[test]
fn ec2_eip_and_nat_gateway_tag_updates_are_in_place() {
    let prov = make_provisioner();
    let vpc = create(
        &prov,
        "AWS::EC2::VPC",
        "V",
        json!({"CidrBlock": "10.8.0.0/16"}),
    );
    let subnet = create(
        &prov,
        "AWS::EC2::Subnet",
        "S",
        json!({"VpcId": vpc.physical_id, "CidrBlock": "10.8.1.0/24"}),
    );
    let eip = create(
        &prov,
        "AWS::EC2::EIP",
        "E",
        json!({"Domain": "vpc", "Tags": [{"Key": "a", "Value": "1"}]}),
    );
    let alloc = eip.attributes["AllocationId"].clone();
    let nat = create(
        &prov,
        "AWS::EC2::NatGateway",
        "N",
        json!({"SubnetId": subnet.physical_id, "AllocationId": alloc}),
    );
    let tags_of = |id: &str| -> Vec<(String, String)> {
        prov.ec2_state
            .read()
            .regional(ACCT, "us-east-1")
            .unwrap()
            .tags_for(id)
            .iter()
            .map(|t| (t.key.clone(), t.value.clone()))
            .collect()
    };

    // A Tags-only change keeps the address and the NAT gateway.
    let eip2 = update(
        &prov,
        &eip,
        json!({"Domain": "vpc", "Tags": [{"Key": "b", "Value": "2"}]}),
    );
    assert_eq!(eip2.physical_id, eip.physical_id);
    assert_eq!(eip2.attributes["AllocationId"], alloc);
    assert_eq!(tags_of(&alloc), vec![("b".to_string(), "2".to_string())]);
    let nat2 = update(
        &prov,
        &nat,
        json!({
            "SubnetId": subnet.physical_id,
            "AllocationId": alloc,
            "Tags": [{"Key": "env", "Value": "dev"}]
        }),
    );
    assert_eq!(nat2.physical_id, nat.physical_id);
    assert_eq!(
        tags_of(&nat.physical_id),
        vec![("env".to_string(), "dev".to_string())]
    );

    // A replacement-requiring change (Domain) releases the old address.
    let standalone = create(&prov, "AWS::EC2::EIP", "E2", json!({"Domain": "vpc"}));
    let replaced = update(&prov, &standalone, json!({"Domain": "standard"}));
    assert_ne!(replaced.physical_id, standalone.physical_id);
    let ec2 = prov.ec2_state.read();
    let st = ec2.regional(ACCT, "us-east-1").unwrap();
    assert!(!st
        .elastic_ips
        .values()
        .any(|e| e.public_ip == standalone.physical_id));
    assert!(st
        .elastic_ips
        .values()
        .any(|e| e.public_ip == replaced.physical_id));
}
