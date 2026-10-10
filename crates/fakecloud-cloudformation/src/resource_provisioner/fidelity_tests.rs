//! Provisioner fidelity tests: same-name collisions, DynamoDB settings
//! applied through the service's own state, OpenAPI imports, IoT topic
//! rules, event source mapping shapes, and SAM templates provisioned end to
//! end.

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

fn create_err(prov: &ResourceProvisioner, ty: &str, id: &str, props: serde_json::Value) -> String {
    prov.create_resource(&make_resource(ty, id, props))
        .expect_err("create should fail")
}

fn function_props(name: &str) -> serde_json::Value {
    json!({
        "FunctionName": name,
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::123456789012:role/r",
        "Handler": "index.handler",
        "Code": {"ZipFile": "def handler(e, c): return e"}
    })
}

// ---------------------------------------------------------------------------
// 1. A same-name resource that already exists fails the create.
// ---------------------------------------------------------------------------

#[test]
fn named_resources_that_already_exist_fail_instead_of_overwriting() {
    let prov = make_provisioner();
    let table = json!({
        "TableName": "kept",
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    create(&prov, "AWS::DynamoDB::Table", "T", table.clone());
    // Data written to the retained table must survive the second create.
    prov.dynamodb_state
        .write()
        .regional_mut(ACCT, "us-east-1")
        .tables
        .get_mut("kept")
        .unwrap()
        .item_count = 7;
    let err = create_err(&prov, "AWS::DynamoDB::Table", "T", table);
    assert!(err.contains("already exists"), "{err}");
    assert_eq!(
        prov.dynamodb_state
            .read()
            .regional(ACCT, "us-east-1")
            .unwrap()
            .tables["kept"]
            .item_count,
        7
    );

    create(
        &prov,
        "AWS::S3::Bucket",
        "B",
        json!({"BucketName": "kept-bucket"}),
    );
    let err = create_err(
        &prov,
        "AWS::S3::Bucket",
        "B",
        json!({"BucketName": "kept-bucket"}),
    );
    assert!(err.contains("already exists"), "{err}");

    create(
        &prov,
        "AWS::SQS::Queue",
        "Q",
        json!({"QueueName": "kept-q"}),
    );
    let err = create_err(
        &prov,
        "AWS::SQS::Queue",
        "Q",
        json!({"QueueName": "kept-q"}),
    );
    assert!(err.contains("already exists"), "{err}");

    create(
        &prov,
        "AWS::SNS::Topic",
        "S",
        json!({"TopicName": "kept-t"}),
    );
    let err = create_err(
        &prov,
        "AWS::SNS::Topic",
        "S",
        json!({"TopicName": "kept-t"}),
    );
    assert!(err.contains("already exists"), "{err}");

    create(
        &prov,
        "AWS::Lambda::Function",
        "F",
        function_props("kept-fn"),
    );
    let err = create_err(
        &prov,
        "AWS::Lambda::Function",
        "F",
        function_props("kept-fn"),
    );
    assert!(err.contains("already exists"), "{err}");
}

#[test]
fn bucket_name_held_by_another_account_fails_the_create() {
    let prov = make_provisioner();
    {
        let mut s3 = prov.s3_state.write();
        let other = s3.get_or_create("999999999999");
        other.buckets.insert(
            "taken".to_string(),
            fakecloud_s3::S3Bucket::new("taken", "us-east-1", "999999999999"),
        );
    }
    let err = create_err(
        &prov,
        "AWS::S3::Bucket",
        "B",
        json!({"BucketName": "taken"}),
    );
    assert!(err.contains("already exists"), "{err}");
}

// ---------------------------------------------------------------------------
// 2. DynamoDB: billing default, TTL / PITR / Kinesis / stream settings.
// ---------------------------------------------------------------------------

fn table_props(extra: serde_json::Value) -> serde_json::Value {
    let mut props = json!({
        "TableName": "settings",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    for (k, v) in extra.as_object().unwrap() {
        props[k] = v.clone();
    }
    props
}

#[test]
fn dynamodb_billing_mode_defaults_to_provisioned() {
    let prov = make_provisioner();
    let err = create_err(&prov, "AWS::DynamoDB::Table", "T", table_props(json!({})));
    assert!(err.contains("ReadCapacityUnits"), "{err}");

    create(
        &prov,
        "AWS::DynamoDB::Table",
        "T",
        table_props(json!({
            "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": "4"}
        })),
    );
    let ddb = prov.dynamodb_state.read();
    let t = &ddb.regional(ACCT, "us-east-1").unwrap().tables["settings"];
    assert_eq!(t.billing_mode, "PROVISIONED");
    assert_eq!(t.provisioned_throughput.read_capacity_units, 3);
    assert_eq!(t.provisioned_throughput.write_capacity_units, 4);
}

#[test]
fn dynamodb_ttl_pitr_kinesis_and_stream_apply_on_create_and_update() {
    let prov = make_provisioner();
    let stream = "arn:aws:kinesis:us-east-1:123456789012:stream/s1";
    let def = make_resource(
        "AWS::DynamoDB::Table",
        "T",
        table_props(json!({
            "BillingMode": "PAY_PER_REQUEST",
            "TimeToLiveSpecification": {"AttributeName": "expires", "Enabled": true},
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": "true"},
            "KinesisStreamSpecification": {"StreamArn": stream}
        })),
    );
    let sr = prov.create_resource(&def).unwrap();
    {
        let ddb = prov.dynamodb_state.read();
        let t = &ddb.regional(ACCT, "us-east-1").unwrap().tables["settings"];
        assert!(t.ttl_enabled);
        assert_eq!(t.ttl_attribute.as_deref(), Some("expires"));
        assert!(t.pitr_enabled);
        assert_eq!(t.kinesis_destinations.len(), 1);
        assert_eq!(t.kinesis_destinations[0].stream_arn, stream);
        assert_eq!(t.kinesis_destinations[0].destination_status, "ACTIVE");
        assert!(!t.stream_enabled);
    }

    // Update: turn the table stream on, drop TTL / PITR / Kinesis.
    let updated = make_resource(
        "AWS::DynamoDB::Table",
        "T",
        table_props(json!({
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {"StreamViewType": "NEW_IMAGE"}
        })),
    );
    let result = prov.update_resource(&sr, &updated).unwrap().unwrap();
    let ddb = prov.dynamodb_state.read();
    let t = &ddb.regional(ACCT, "us-east-1").unwrap().tables["settings"];
    assert!(t.stream_enabled);
    assert_eq!(t.stream_view_type.as_deref(), Some("NEW_IMAGE"));
    let stream_arn = t.stream_arn.clone().unwrap();
    assert_eq!(result.attributes.get("StreamArn"), Some(&stream_arn));
    assert!(!t.ttl_enabled);
    assert!(!t.pitr_enabled);
    assert_eq!(t.kinesis_destinations[0].destination_status, "DISABLED");
}

// ---------------------------------------------------------------------------
// Cognito LambdaConfig.
// ---------------------------------------------------------------------------

#[test]
fn cognito_user_pool_applies_lambda_config_on_create_and_update() {
    let prov = make_provisioner();
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:pre";
    let sr = create(
        &prov,
        "AWS::Cognito::UserPool",
        "P",
        json!({"PoolName": "p", "LambdaConfig": {"PreSignUp": arn}}),
    );
    let lambda_config = |prov: &ResourceProvisioner| {
        prov.cognito_state.read().get(ACCT).unwrap().user_pools[&sr.physical_id]
            .lambda_config
            .clone()
    };
    assert_eq!(lambda_config(&prov), Some(json!({"PreSignUp": arn})));
    prov.update_resource(
        &sr,
        &make_resource("AWS::Cognito::UserPool", "P", json!({"PoolName": "p"})),
    )
    .unwrap();
    assert_eq!(lambda_config(&prov), None);
}

#[test]
fn cognito_user_pool_takes_id_from_custom_id_tag() {
    let prov = make_provisioner();
    let props = json!({"PoolName": "p", "UserPoolTags": {"_custom_id_": "us-east-1_Local"}});
    let sr = create(&prov, "AWS::Cognito::UserPool", "P", props.clone());
    assert_eq!(sr.physical_id, "us-east-1_Local");
    assert_eq!(
        prov.cognito_state.read().get(ACCT).unwrap().user_pools["us-east-1_Local"].arn,
        "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_Local"
    );

    let err = create_err(&prov, "AWS::Cognito::UserPool", "P2", props);
    assert!(err.contains("already exists"), "{err}");
    for bad in ["eu-west-1_Local", "us-east-1_", "us-east-1_lo-cal"] {
        let err = create_err(
            &prov,
            "AWS::Cognito::UserPool",
            "P3",
            json!({"PoolName": "bad", "UserPoolTags": {"_custom_id_": bad}}),
        );
        assert!(
            err.contains("Invalid _custom_id_ tag value"),
            "{bad}: {err}"
        );
    }
}

#[test]
fn cognito_user_pool_client_takes_id_from_custom_id_name() {
    let prov = make_provisioner();
    let pool = create(
        &prov,
        "AWS::Cognito::UserPool",
        "P",
        json!({"PoolName": "p"}),
    );
    let props = json!({"UserPoolId": pool.physical_id, "ClientName": "_custom_id_:localclient"});
    let sr = create(&prov, "AWS::Cognito::UserPoolClient", "C", props.clone());
    assert_eq!(sr.physical_id, "localclient");
    assert_eq!(
        prov.cognito_state
            .read()
            .get(ACCT)
            .unwrap()
            .user_pool_clients["localclient"]
            .user_pool_id,
        pool.physical_id
    );

    let err = create_err(&prov, "AWS::Cognito::UserPoolClient", "C2", props);
    assert!(err.contains("already exists"), "{err}");
    let err = create_err(
        &prov,
        "AWS::Cognito::UserPoolClient",
        "C3",
        json!({"UserPoolId": pool.physical_id, "ClientName": "_custom_id_:local-client"}),
    );
    assert!(err.contains("Invalid custom client id"), "{err}");
}

#[test]
fn cognito_custom_id_pool_delete_and_recreate_starts_clean() {
    let prov = make_provisioner();
    let props = json!({"PoolName": "p", "UserPoolTags": {"_custom_id_": "us-east-1_Local"}});
    let pool = create(&prov, "AWS::Cognito::UserPool", "P", props.clone());
    let client = create(
        &prov,
        "AWS::Cognito::UserPoolClient",
        "C",
        json!({"UserPoolId": "us-east-1_Local", "ClientName": "_custom_id_:localclient"}),
    );
    let arn = {
        let mut accounts = prov.cognito_state.write();
        let state = accounts.get_or_create(ACCT);
        let arn = state.user_pools["us-east-1_Local"].arn.clone();
        state
            .tags
            .insert(arn.clone(), [("env".to_string(), "old".to_string())].into());
        state
            .risk_configurations
            .insert("us-east-1_Local:localclient".to_string(), json!({}));
        arn
    };

    prov.delete_resource(&client).unwrap();
    prov.delete_resource(&pool).unwrap();
    {
        let accounts = prov.cognito_state.read();
        let state = accounts.get(ACCT).unwrap();
        assert!(!state.tags.contains_key(&arn));
        assert!(state.risk_configurations.is_empty());
        assert!(state.user_pool_clients.is_empty());
    }
    let again = create(&prov, "AWS::Cognito::UserPool", "P", props);
    assert_eq!(again.physical_id, "us-east-1_Local");
    assert!(!prov
        .cognito_state
        .read()
        .get(ACCT)
        .unwrap()
        .tags
        .contains_key(&arn));
}

#[test]
fn cognito_custom_client_id_waits_for_its_pool_first() {
    let prov = make_provisioner();
    let pool = create(
        &prov,
        "AWS::Cognito::UserPool",
        "P",
        json!({"PoolName": "p"}),
    );
    create(
        &prov,
        "AWS::Cognito::UserPoolClient",
        "C",
        json!({"UserPoolId": pool.physical_id, "ClientName": "_custom_id_:taken"}),
    );
    let err = create_err(
        &prov,
        "AWS::Cognito::UserPoolClient",
        "C2",
        json!({"UserPoolId": "us-east-1_missing", "ClientName": "_custom_id_:taken"}),
    );
    assert!(err.contains("does not exist yet"), "{err}");
}

#[test]
fn cognito_user_pool_client_update_cannot_change_custom_id() {
    let prov = make_provisioner();
    let pool = create(
        &prov,
        "AWS::Cognito::UserPool",
        "P",
        json!({"PoolName": "p"}),
    );
    let client = create(
        &prov,
        "AWS::Cognito::UserPoolClient",
        "C",
        json!({"UserPoolId": pool.physical_id, "ClientName": "_custom_id_:localclient"}),
    );
    let update = |name: &str| {
        prov.update_resource(
            &client,
            &make_resource(
                "AWS::Cognito::UserPoolClient",
                "C",
                json!({"UserPoolId": pool.physical_id, "ClientName": name}),
            ),
        )
    };

    let err = update("_custom_id_:otherclient").expect_err("changing the custom id must fail");
    assert!(err.contains("fixed at creation"), "{err}");
    update("_custom_id_:localclient").unwrap();
    update("plain-name").unwrap();
    let accounts = prov.cognito_state.read();
    let stored = &accounts.get(ACCT).unwrap().user_pool_clients["localclient"];
    assert_eq!(stored.client_name, "plain-name");
}

#[test]
fn cognito_user_pool_update_cannot_change_custom_id() {
    let prov = make_provisioner();
    let sr = create(
        &prov,
        "AWS::Cognito::UserPool",
        "P",
        json!({"PoolName": "p"}),
    );
    let err = prov
        .update_resource(
            &sr,
            &make_resource(
                "AWS::Cognito::UserPool",
                "P",
                json!({"PoolName": "p", "UserPoolTags": {"_custom_id_": "us-east-1_Other"}}),
            ),
        )
        .expect_err("adding a custom id must fail");
    assert!(err.contains("fixed at creation"), "{err}");

    let custom = create(
        &prov,
        "AWS::Cognito::UserPool",
        "Q",
        json!({"PoolName": "q", "UserPoolTags": {"_custom_id_": "us-east-1_Local"}}),
    );
    let err = prov
        .update_resource(
            &custom,
            &make_resource(
                "AWS::Cognito::UserPool",
                "Q",
                json!({"PoolName": "q", "UserPoolTags": {"_custom_id_": "us-east-1_Moved"}}),
            ),
        )
        .expect_err("changing the custom id must fail");
    assert!(err.contains("fixed at creation"), "{err}");

    // Keeping the same id, or dropping the tag, updates in place.
    for tags in [
        json!({"_custom_id_": "us-east-1_Local", "k": "v"}),
        json!({}),
    ] {
        prov.update_resource(
            &custom,
            &make_resource(
                "AWS::Cognito::UserPool",
                "Q",
                json!({"PoolName": "q", "UserPoolTags": tags}),
            ),
        )
        .unwrap();
    }
    assert!(
        prov.cognito_state.read().get(ACCT).unwrap().user_pools["us-east-1_Local"]
            .user_pool_tags
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// 3. RestApi / HttpApi OpenAPI import.
// ---------------------------------------------------------------------------

fn openapi_body(path: &str) -> serde_json::Value {
    json!({
        "openapi": "3.0.1",
        "info": {"title": "imported-title", "version": "1"},
        "paths": {
            path: {
                "get": {
                    "x-amazon-apigateway-integration": {
                        "type": "aws_proxy",
                        "httpMethod": "POST",
                        "uri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:f/invocations"
                    }
                }
            }
        }
    })
}

#[test]
fn rest_api_body_is_imported_and_named_from_its_title() {
    let prov = make_provisioner();
    let sr = create(
        &prov,
        "AWS::ApiGateway::RestApi",
        "Api",
        json!({"Body": openapi_body("/pets/{id}")}),
    );
    let id = sr.physical_id.clone();
    {
        let st = prov.apigateway_state.read();
        let st = st.get(ACCT).unwrap();
        assert_eq!(st.apis[&id].name, "imported-title");
        let pet = st.resources[&id]
            .values()
            .find(|r| r.path == "/pets/{id}")
            .expect("imported resource");
        let integ = &st.integrations[&format!("{id}/{}/GET", pet.id)];
        assert_eq!(integ.integration_type, "AWS_PROXY");
    }
    // An update with a new Body (and an explicit Name) replaces the paths.
    prov.update_resource(
        &sr,
        &make_resource(
            "AWS::ApiGateway::RestApi",
            "Api",
            json!({"Name": "named", "Body": openapi_body("/owners")}),
        ),
    )
    .unwrap();
    let st = prov.apigateway_state.read();
    let st = st.get(ACCT).unwrap();
    assert_eq!(st.apis[&id].name, "named");
    assert!(st.resources[&id].values().any(|r| r.path == "/owners"));
    assert!(!st.resources[&id].values().any(|r| r.path == "/pets"));
}

#[test]
fn rest_api_body_s3_location_is_read_and_imported() {
    let prov = make_provisioner();
    let spec =
        "openapi: 3.0.1\ninfo:\n  title: from-s3\n  version: '1'\npaths:\n  /yaml:\n    get: {}\n";
    {
        let mut s3 = prov.s3_state.write();
        let state = s3.get_or_create(ACCT);
        let mut bucket = fakecloud_s3::S3Bucket::new("specs", "us-east-1", ACCT);
        bucket.objects.insert(
            "api.yaml".to_string(),
            fakecloud_s3::S3Object {
                key: "api.yaml".to_string(),
                body: fakecloud_s3::memory_body(bytes::Bytes::from_static(spec.as_bytes())),
                size: spec.len() as u64,
                ..Default::default()
            },
        );
        state.buckets.insert("specs".to_string(), bucket);
    }
    let sr = create(
        &prov,
        "AWS::ApiGateway::RestApi",
        "Api",
        json!({"BodyS3Location": {"Bucket": "specs", "Key": "api.yaml"}}),
    );
    let st = prov.apigateway_state.read();
    let st = st.get(ACCT).unwrap();
    assert_eq!(st.apis[&sr.physical_id].name, "from-s3");
    assert!(st.resources[&sr.physical_id]
        .values()
        .any(|r| r.path == "/yaml"));
}

#[test]
fn rest_api_without_name_or_body_still_requires_a_name() {
    let prov = make_provisioner();
    let err = create_err(&prov, "AWS::ApiGateway::RestApi", "Api", json!({}));
    assert!(err.contains("Name is required"), "{err}");
}

#[test]
fn http_api_body_is_imported_as_routes() {
    let prov = make_provisioner();
    let sr = create(
        &prov,
        "AWS::ApiGatewayV2::Api",
        "H",
        json!({"Body": openapi_body("/items")}),
    );
    let st = prov.apigatewayv2_state.read();
    let st = st.get(ACCT).unwrap();
    assert_eq!(st.apis[&sr.physical_id].name, "imported-title");
    let routes = &st.routes[&sr.physical_id];
    let route = routes
        .values()
        .find(|r| r.route_key == "GET /items")
        .unwrap();
    assert!(route
        .target
        .as_deref()
        .unwrap()
        .starts_with("integrations/"));
}

// ---------------------------------------------------------------------------
// IoT topic rule.
// ---------------------------------------------------------------------------

#[test]
fn iot_topic_rule_round_trips_through_iot_state() {
    let prov = make_provisioner();
    let props = json!({
        "RuleName": "my_rule",
        "TopicRulePayload": {
            "Sql": "SELECT * FROM 'a/b'",
            "Actions": [{"Lambda": {"FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:f"}}]
        }
    });
    let sr = create(&prov, "AWS::IoT::TopicRule", "R", props.clone());
    assert_eq!(sr.physical_id, "my_rule");
    assert_eq!(
        sr.attributes.get("Arn").map(String::as_str),
        Some("arn:aws:iot:us-east-1:123456789012:rule/my_rule")
    );
    {
        let iot = prov.iot_state.read();
        let rule = iot
            .get(ACCT)
            .unwrap()
            .get_resource("rules", "my_rule")
            .unwrap();
        assert_eq!(rule["sql"], "SELECT * FROM 'a/b'");
        assert_eq!(
            rule["actions"][0]["lambda"]["functionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:f"
        );
        assert_eq!(rule["ruleDisabled"], false);
    }
    let err = create_err(&prov, "AWS::IoT::TopicRule", "R", props);
    assert!(err.contains("already exists"), "{err}");
    prov.delete_resource(&sr).unwrap();
    assert!(prov
        .iot_state
        .read()
        .get(ACCT)
        .unwrap()
        .get_resource("rules", "my_rule")
        .is_none());
}

#[test]
fn unnamed_iot_topic_rule_gets_an_underscore_name() {
    let prov = make_provisioner();
    let sr = create(
        &prov,
        "AWS::IoT::TopicRule",
        "R",
        json!({"TopicRulePayload": {"Sql": "SELECT 1 FROM 'x'", "Actions": []}}),
    );
    assert!(
        sr.physical_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "{}",
        sr.physical_id
    );
}

// ---------------------------------------------------------------------------
// Event source mappings.
// ---------------------------------------------------------------------------

#[test]
fn event_source_mapping_keeps_alias_qualifier_and_self_managed_config() {
    let prov = make_provisioner();
    create(
        &prov,
        "AWS::Lambda::Function",
        "F",
        function_props("esm-fn"),
    );
    let alias_arn = "arn:aws:lambda:us-east-1:123456789012:function:esm-fn:live";
    let sr = create(
        &prov,
        "AWS::Lambda::EventSourceMapping",
        "M",
        json!({
            "FunctionName": alias_arn,
            "SelfManagedEventSource": {"Endpoints": {"KafkaBootstrapServers": ["b1:9092"]}},
            "SelfManagedKafkaEventSourceConfig": {"ConsumerGroupId": "g"},
            "Topics": ["t"],
            "SourceAccessConfigurations": [{"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s"}],
            "StartingPosition": "LATEST"
        }),
    );
    let lam = prov.lambda_state.read();
    let esm = &lam
        .regional(ACCT, "us-east-1")
        .unwrap()
        .event_source_mappings[&sr.physical_id];
    assert_eq!(esm.function_arn, alias_arn);
    assert_eq!(
        esm.self_managed_event_source,
        Some(json!({"Endpoints": {"KAFKA_BOOTSTRAP_SERVERS": ["b1:9092"]}})),
        "stored in the Lambda API's shape"
    );
    assert_eq!(
        esm.self_managed_kafka_event_source_config,
        Some(json!({"ConsumerGroupId": "g"}))
    );
    assert_eq!(esm.source_access_configurations.len(), 1);
}

// ---------------------------------------------------------------------------
// SAM templates provisioned end to end.
// ---------------------------------------------------------------------------

fn provision_template(
    prov: &ResourceProvisioner,
    template: serde_json::Value,
) -> Vec<StackResource> {
    let body = template.to_string();
    let mut params = BTreeMap::new();
    params.insert("AWS::StackName".to_string(), "samstack".to_string());
    params.insert("AWS::Region".to_string(), "us-east-1".to_string());
    params.insert("AWS::AccountId".to_string(), ACCT.to_string());
    params.insert("AWS::Partition".to_string(), "aws".to_string());
    let parsed = crate::template::parse_template(&body, &params).expect("template parses");
    crate::service::provision_stack_resources(
        prov,
        &parsed.resources,
        &body,
        &params,
        &BTreeMap::new(),
    )
    .unwrap_or_else(|e| panic!("provision failed: {e:?}"))
}

fn by_logical<'a>(resources: &'a [StackResource], id: &str) -> &'a StackResource {
    resources
        .iter()
        .find(|r| r.logical_id == id)
        .unwrap_or_else(|| panic!("no resource {id}"))
}

#[test]
fn sam_explicit_api_s3_event_and_auto_publish_alias_provision() {
    let prov = make_provisioner();
    let resources = provision_template(
        &prov,
        json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Resources": {
                "Uploads": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "sam-uploads"}},
                "MyApi": {
                    "Type": "AWS::Serverless::Api",
                    "Properties": {"StageName": "dev", "Cors": "'*'"}
                },
                "Fn": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {
                        "FunctionName": "sam-fn",
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "InlineCode": "def handler(e, c): return {'statusCode': 200}",
                        "AutoPublishAlias": "live",
                        "FunctionUrlConfig": {"AuthType": "NONE"},
                        "Events": {
                            "Get": {"Type": "Api", "Properties": {"RestApiId": {"Ref": "MyApi"}, "Path": "/items/{id}", "Method": "get"}},
                            "Upload": {"Type": "S3", "Properties": {"Bucket": {"Ref": "Uploads"}, "Events": "s3:ObjectCreated:*"}}
                        }
                    }
                }
            }
        }),
    );

    // The explicit API is named after the stack and carries the route.
    let api = by_logical(&resources, "MyApi");
    let alias = by_logical(&resources, "FnAliaslive");
    let alias_arn = "arn:aws:lambda:us-east-1:123456789012:function:sam-fn:live";
    assert_eq!(alias.physical_id, alias_arn);
    {
        let st = prov.apigateway_state.read();
        let st = st.get(ACCT).unwrap();
        assert_eq!(st.apis[&api.physical_id].name, "samstack");
        let res = st.resources[&api.physical_id]
            .values()
            .find(|r| r.path == "/items/{id}")
            .expect("route resource");
        let integ = &st.integrations[&format!("{}/{}/GET", api.physical_id, res.id)];
        assert_eq!(integ.integration_type, "AWS_PROXY");
        assert!(
            integ.uri.as_deref().unwrap().contains(alias_arn),
            "integration targets the alias: {:?}",
            integ.uri
        );
        // Cors adds the preflight method.
        assert!(st
            .methods
            .contains_key(&format!("{}/{}/OPTIONS", api.physical_id, res.id)));
        assert!(st.stages[&api.physical_id].contains_key("dev"));
    }

    // AutoPublishAlias published version 1 and pointed the alias at it.
    {
        let lam = prov.lambda_state.read();
        let lam = lam.regional(ACCT, "us-east-1").unwrap();
        assert_eq!(lam.aliases["sam-fn:live"].function_version, "1");
        assert!(lam.function_url_configs.contains_key("sam-fn:live"));
        let policy = lam.functions["sam-fn"].policy.clone().unwrap();
        assert!(policy.contains("apigateway.amazonaws.com"), "{policy}");
        assert!(policy.contains("s3.amazonaws.com"), "{policy}");
        assert!(policy.contains("lambda:InvokeFunctionUrl"), "{policy}");
    }

    // The bucket notifies the alias.
    let s3 = prov.s3_state.read();
    let bucket = &s3.get(ACCT).unwrap().buckets["sam-uploads"];
    let notification = bucket.notification_config.clone().unwrap();
    assert!(notification.contains(alias_arn), "{notification}");
}

#[test]
fn sam_cognito_logs_iot_and_kafka_events_provision() {
    let prov = make_provisioner();
    let resources = provision_template(
        &prov,
        json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {"PoolName": "p"}},
                "Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": "/app/logs"}},
                "Fn": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {
                        "FunctionName": "events-fn",
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "InlineCode": "x",
                        "Events": {
                            "SignUp": {"Type": "Cognito", "Properties": {"UserPool": {"Ref": "Pool"}, "Trigger": ["PreSignUp", "PostConfirmation"]}},
                            "Tail": {"Type": "CloudWatchLogs", "Properties": {"LogGroupName": {"Ref": "Logs"}, "FilterPattern": "ERROR"}},
                            "Iot": {"Type": "IoTRule", "Properties": {"Sql": "SELECT * FROM 'sensors'"}},
                            "Kafka": {"Type": "SelfManagedKafka", "Properties": {
                                "KafkaBootstrapServers": ["broker:9092"],
                                "Topics": ["orders"],
                                "SourceAccessConfigurations": [{"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:k"}]
                            }}
                        }
                    }
                }
            }
        }),
    );
    let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:events-fn";
    let pool = by_logical(&resources, "Pool");
    let cognito = prov.cognito_state.read();
    let lc = cognito.get(ACCT).unwrap().user_pools[&pool.physical_id]
        .lambda_config
        .clone()
        .unwrap();
    assert_eq!(lc["PreSignUp"], fn_arn);
    assert_eq!(lc["PostConfirmation"], fn_arn);

    let filter = by_logical(&resources, "FnTail");
    assert_eq!(filter.resource_type, "AWS::Logs::SubscriptionFilter");

    let rule = by_logical(&resources, "FnIot");
    let iot = prov.iot_state.read();
    let record = iot
        .get(ACCT)
        .unwrap()
        .get_resource("rules", &rule.physical_id)
        .unwrap();
    assert_eq!(record["actions"][0]["lambda"]["functionArn"], fn_arn);

    let esm = by_logical(&resources, "FnKafkaEventSourceMapping");
    let lam = prov.lambda_state.read();
    let mapping = &lam
        .regional(ACCT, "us-east-1")
        .unwrap()
        .event_source_mappings[&esm.physical_id];
    assert_eq!(mapping.topics, vec!["orders".to_string()]);
    assert_eq!(
        mapping.self_managed_event_source,
        Some(json!({"Endpoints": {"KAFKA_BOOTSTRAP_SERVERS": ["broker:9092"]}}))
    );
}

#[test]
fn sam_deployment_preference_creates_codedeploy_group_and_alias() {
    let prov = make_provisioner();
    let resources = provision_template(
        &prov,
        json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Resources": {
                "Fn": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {
                        "FunctionName": "dp-fn",
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "InlineCode": "x",
                        "AutoPublishAlias": "live",
                        "ProvisionedConcurrencyConfig": {"ProvisionedConcurrentExecutions": 2},
                        "DeploymentPreference": {"Type": "AllAtOnce"}
                    }
                }
            }
        }),
    );
    by_logical(&resources, "ServerlessDeploymentApplication");
    by_logical(&resources, "FnDeploymentGroup");
    let lam = prov.lambda_state.read();
    let lam = lam.regional(ACCT, "us-east-1").unwrap();
    assert!(lam.aliases.contains_key("dp-fn:live"));
    assert_eq!(lam.provisioned_concurrency["dp-fn:live"].requested, 2);
}

#[test]
fn http_api_update_that_keeps_the_body_preserves_other_routes() {
    let prov = make_provisioner();
    let api_def = |desc: &str, path: &str| {
        make_resource(
            "AWS::ApiGatewayV2::Api",
            "H",
            json!({"Description": desc, "Body": openapi_body(path)}),
        )
    };
    let api = prov.create_resource(&api_def("one", "/items")).unwrap();
    let integ = create(
        &prov,
        "AWS::ApiGatewayV2::Integration",
        "I",
        json!({"ApiId": api.physical_id, "IntegrationType": "AWS_PROXY",
               "IntegrationUri": "arn:aws:lambda:us-east-1:123456789012:function:f",
               "PayloadFormatVersion": "2.0"}),
    );
    create(
        &prov,
        "AWS::ApiGatewayV2::Route",
        "R",
        json!({"ApiId": api.physical_id, "RouteKey": "GET /extra",
               "Target": format!("integrations/{}", integ.physical_id)}),
    );
    let route_keys = |prov: &ResourceProvisioner| -> Vec<String> {
        let st = prov.apigatewayv2_state.read();
        let mut keys: Vec<String> = st.get(ACCT).unwrap().routes[&api.physical_id]
            .values()
            .map(|r| r.route_key.clone())
            .collect();
        keys.sort();
        keys
    };
    // Only Description changes: the imported and the separate route stay.
    prov.update_resource(&api, &api_def("two", "/items"))
        .unwrap();
    assert_eq!(route_keys(&prov), vec!["GET /extra", "GET /items"]);
    assert_eq!(
        prov.apigatewayv2_state.read().get(ACCT).unwrap().apis[&api.physical_id]
            .description
            .as_deref(),
        Some("two")
    );
    // A changed definition is re-imported: the old path's route goes, the
    // new one arrives, and the separately owned route survives.
    prov.update_resource(&api, &api_def("two", "/other"))
        .unwrap();
    assert_eq!(route_keys(&prov), vec!["GET /extra", "GET /other"]);
    // The previous import's integration went with its route; the separately
    // owned integration is still there.
    let st = prov.apigatewayv2_state.read();
    let integrations = &st.get(ACCT).unwrap().integrations[&api.physical_id];
    assert!(integrations.contains_key(&integ.physical_id));
    assert_eq!(integrations.len(), 2);
}

#[test]
fn http_api_body_change_renames_and_drops_removed_paths() {
    let prov = make_provisioner();
    let body = |title: &str, paths: &[&str]| {
        let mut b =
            json!({"openapi": "3.0.1", "info": {"title": title, "version": "1"}, "paths": {}});
        for p in paths {
            b["paths"][*p] = json!({"get": {"x-amazon-apigateway-integration": {
                "type": "aws_proxy", "httpMethod": "POST", "payloadFormatVersion": "2.0",
                "uri": "arn:aws:lambda:us-east-1:123456789012:function:f"}}});
        }
        make_resource("AWS::ApiGatewayV2::Api", "H", json!({"Body": b}))
    };
    let api = prov.create_resource(&body("first", &["/a", "/b"])).unwrap();
    prov.update_resource(&api, &body("renamed", &["/a"]))
        .unwrap();
    let st = prov.apigatewayv2_state.read();
    let st = st.get(ACCT).unwrap();
    assert_eq!(st.apis[&api.physical_id].name, "renamed");
    let keys: Vec<&str> = st.routes[&api.physical_id]
        .values()
        .map(|r| r.route_key.as_str())
        .collect();
    assert_eq!(keys, vec!["GET /a"]);
    assert_eq!(st.integrations[&api.physical_id].len(), 1);
}

// ---------------------------------------------------------------------------
// A stack's DynamoDB table lives in the stack's region.
// ---------------------------------------------------------------------------

#[test]
fn dynamodb_tables_are_provisioned_in_the_stack_region() {
    let mut east = make_provisioner();
    east.region = "us-east-1".to_string();
    let mut west = make_provisioner();
    west.region = "eu-west-1".to_string();
    west.dynamodb_state = east.dynamodb_state.clone();
    let props = json!({
        "TableName": "regional",
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
    });
    // The same table name in two regions' stacks is two tables.
    let east_res = create(&east, "AWS::DynamoDB::Table", "T", props.clone());
    let west_res = create(&west, "AWS::DynamoDB::Table", "T", props);
    // Ref is the table name in both stacks; the Arn names each region.
    assert_eq!(west_res.physical_id, "regional");
    assert_eq!(east_res.physical_id, "regional");
    assert_eq!(
        west_res.attributes["Arn"],
        "arn:aws:dynamodb:eu-west-1:123456789012:table/regional"
    );
    {
        let ddb = east.dynamodb_state.read();
        for region in ["us-east-1", "eu-west-1"] {
            assert_eq!(
                ddb.regional(ACCT, region).unwrap().tables["regional"].arn,
                format!("arn:aws:dynamodb:{region}:123456789012:table/regional")
            );
        }
    }
    // Deleting the eu-west-1 stack's table leaves us-east-1's.
    west.delete_resource(&west_res).unwrap();
    let ddb = east.dynamodb_state.read();
    assert!(!ddb
        .regional(ACCT, "eu-west-1")
        .unwrap()
        .tables
        .contains_key("regional"));
    assert_eq!(
        ddb.regional(ACCT, "us-east-1").unwrap().tables["regional"].arn,
        east_res.attributes["Arn"]
    );
}
