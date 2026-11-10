//! `aws-sdk:<service>:<action>` Tasks over every wire protocol, driven by the
//! service's Smithy model: parameters in the model's casing, results in
//! PascalCase with ISO dates, as recorded from AWS.

mod helpers;

use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

async fn run_task(
    server: &TestServer,
    resource_action: &str,
    parameters: Value,
) -> Result<Value, (String, String)> {
    let client = server.sfn_client().await;
    let definition = json!({"StartAt":"T","States":{"T":{
        "Type":"Task",
        "Resource": format!("arn:aws:states:::aws-sdk:{resource_action}"),
        "Parameters": parameters,
        "End":true
    }}});
    let created = client
        .create_state_machine()
        .name(format!("sm-{}", uuid::Uuid::new_v4().simple()))
        .definition(definition.to_string())
        .role_arn("arn:aws:iam::123456789012:role/sfn-role")
        .send()
        .await
        .unwrap();
    let started = client
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .send()
        .await
        .unwrap();
    for _ in 0..400 {
        let desc = client
            .describe_execution()
            .execution_arn(started.execution_arn())
            .send()
            .await
            .unwrap();
        match desc.status().as_str() {
            "RUNNING" => sleep(Duration::from_millis(25)).await,
            "SUCCEEDED" => return Ok(serde_json::from_str(desc.output().unwrap()).unwrap()),
            _ => {
                return Err((
                    desc.error().unwrap_or_default().to_string(),
                    desc.cause().unwrap_or_default().to_string(),
                ))
            }
        }
    }
    panic!("task did not finish");
}

async fn ok(server: &TestServer, action: &str, parameters: Value) -> Value {
    run_task(server, action, parameters)
        .await
        .unwrap_or_else(|e| panic!("{action}: {e:?}"))
}

#[tokio::test]
async fn aws_json_services() {
    let server = TestServer::start().await;

    assert_eq!(
        ok(&server, "dynamodb:listTables", json!({})).await,
        json!({"TableNames": []})
    );

    // ECR: camelCase requests with PascalCase tags, ISO dates back.
    let created = ok(
        &server,
        "ecr:createRepository",
        json!({"RepositoryName": "repo", "Tags": [{"Key": "team", "Value": "a"}]}),
    )
    .await;
    assert_eq!(created["Repository"]["RepositoryName"], "repo");
    assert!(created["Repository"]["CreatedAt"]
        .as_str()
        .unwrap()
        .ends_with('Z'));
    let arn = created["Repository"]["RepositoryArn"].as_str().unwrap();
    let tags = server
        .ecr_client()
        .await
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags()[0].key(), "team");
    let described = ok(&server, "ecr:describeRepositories", json!({})).await;
    assert_eq!(described["Repositories"][0]["RepositoryName"], "repo");

    // CloudWatch Logs goes by its SDK id, with camelCase members.
    ok(
        &server,
        "cloudwatchlogs:createLogGroup",
        json!({"LogGroupName": "lg"}),
    )
    .await;
    let groups = ok(
        &server,
        "cloudwatchlogs:describeLogGroups",
        json!({"Limit": 1}),
    )
    .await;
    assert_eq!(groups["LogGroups"][0]["LogGroupName"], "lg");

    assert_eq!(ok(&server, "sqs:listQueues", json!({})).await, json!({}));
}

#[tokio::test]
async fn rest_json_services() {
    let server = TestServer::start().await;

    assert_eq!(
        ok(&server, "lambda:listFunctions", json!({"MaxItems": 1})).await,
        json!({"Functions": []})
    );

    let missing = run_task(
        &server,
        "lambda:getFunction",
        json!({"FunctionName": "nope"}),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.0, "Lambda.ResourceNotFoundException");
    assert!(
        missing
            .1
            .contains("(Service: Lambda, Status Code: 404, Request ID: "),
        "{missing:?}"
    );
}

#[tokio::test]
async fn query_services() {
    let server = TestServer::start().await;

    let created = ok(
        &server,
        "iam:createUser",
        json!({"UserName": "u", "Tags": [{"Key": "k", "Value": "v"}]}),
    )
    .await;
    assert_eq!(created["User"]["UserName"], "u");
    assert!(created["User"]["CreateDate"]
        .as_str()
        .unwrap()
        .ends_with('Z'));

    let users = ok(&server, "iam:listUsers", json!({})).await;
    assert_eq!(users["Users"][0]["UserName"], "u", "{users}");
    assert_eq!(users["IsTruncated"], false);

    let duplicate = run_task(&server, "iam:createUser", json!({"UserName": "u"}))
        .await
        .unwrap_err();
    assert_eq!(duplicate.0, "Iam.EntityAlreadyExistsException");

    let topic = ok(&server, "sns:createTopic", json!({"Name": "t"})).await;
    let topic_arn = topic["TopicArn"].as_str().unwrap();
    assert!(topic_arn.ends_with(":t"), "{topic}");
    let topics = ok(&server, "sns:listTopics", json!({})).await;
    assert_eq!(topics["Topics"][0]["TopicArn"], topic_arn);

    let identity = ok(&server, "sts:getCallerIdentity", json!({})).await;
    assert_eq!(identity["Account"], "123456789012", "{identity}");
    assert!(
        identity["Arn"]
            .as_str()
            .unwrap()
            .contains(":assumed-role/sfn-role/"),
        "a Task runs as the state machine's role: {identity}"
    );
}

#[tokio::test]
async fn ec2_query_service() {
    let server = TestServer::start().await;

    let created = ok(
        &server,
        "ec2:createVpc",
        json!({"CidrBlock": "10.9.0.0/16"}),
    )
    .await;
    let vpc_id = created["Vpc"]["VpcId"].as_str().unwrap();
    assert_eq!(created["Vpc"]["CidrBlock"], "10.9.0.0/16", "{created}");

    let described = ok(&server, "ec2:describeVpcs", json!({"VpcIds": [vpc_id]})).await;
    assert_eq!(
        described["Vpcs"].as_array().unwrap().len(),
        1,
        "{described}"
    );
    assert_eq!(described["Vpcs"][0]["VpcId"], vpc_id);

    let filtered = ok(
        &server,
        "ec2:describeVpcs",
        json!({"Filters": [{"Name": "cidr", "Values": ["10.9.0.0/16"]}]}),
    )
    .await;
    assert_eq!(filtered["Vpcs"][0]["VpcId"], vpc_id, "{filtered}");
}

#[tokio::test]
async fn rest_xml_services() {
    let server = TestServer::start().await;

    let zone = ok(
        &server,
        "route53:createHostedZone",
        json!({
            "Name": "example.com",
            "CallerReference": "ref-1",
            "HostedZoneConfig": {"Comment": "from a task", "PrivateZone": false}
        }),
    )
    .await;
    assert_eq!(zone["HostedZone"]["Name"], "example.com.", "{zone}");
    assert_eq!(zone["HostedZone"]["Config"]["Comment"], "from a task");

    let zones = ok(&server, "route53:listHostedZones", json!({})).await;
    assert_eq!(zones["HostedZones"][0]["Name"], "example.com.", "{zones}");
    assert_eq!(zones["IsTruncated"], false);

    // S3 bucket-level operations take the same generic path as object ones.
    ok(&server, "s3:createBucket", json!({"Bucket": "sdk-made"})).await;
    let tagging = json!({"TagSet": [{"Key": "k", "Value": "v"}]});
    ok(
        &server,
        "s3:putBucketTagging",
        json!({"Bucket": "sdk-made", "Tagging": tagging}),
    )
    .await;
    let read = ok(
        &server,
        "s3:getBucketTagging",
        json!({"Bucket": "sdk-made"}),
    )
    .await;
    assert_eq!(read["TagSet"][0]["Key"], "k", "{read}");
    let buckets = ok(&server, "s3:listBuckets", json!({})).await;
    assert_eq!(buckets["Buckets"][0]["Name"], "sdk-made", "{buckets}");
}

#[tokio::test]
async fn unknown_actions_and_services_fail_the_task() {
    let server = TestServer::start().await;

    let unknown = run_task(&server, "s3:notAnAction", json!({}))
        .await
        .unwrap_err();
    assert_eq!(unknown.0, "States.TaskFailed");
    let service = run_task(&server, "nosuchservice:doIt", json!({}))
        .await
        .unwrap_err();
    assert_eq!(service.0, "States.TaskFailed");
}
