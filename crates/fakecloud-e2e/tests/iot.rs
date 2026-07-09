//! End-to-end tests for the AWS IoT Core control plane, driven through the
//! real `aws-sdk-iot` client against a live fakecloud server. Exercises the
//! registry / policies / certificates / groups / jobs / rules / endpoint /
//! tagging control plane end to end: create a thing -> a thing type -> a policy
//! and attach it -> mint a certificate -> attach the certificate as a thing
//! principal -> create a thing group and add the thing -> create a job -> a
//! topic rule -> describe the endpoint -> tag the thing.

use aws_sdk_iot::types::{Action, TopicRulePayload};
use fakecloud_testkit::TestServer;

async fn iot_client(server: &TestServer) -> aws_sdk_iot::Client {
    let conf = aws_sdk_iot::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_iot::Client::from_conf(conf)
}

#[tokio::test]
async fn iot_control_plane_lifecycle() {
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    // --- Thing type + thing ---
    client
        .create_thing_type()
        .thing_type_name("sensor-type")
        .send()
        .await
        .expect("create_thing_type");

    let thing = client
        .create_thing()
        .thing_name("thermostat")
        .thing_type_name("sensor-type")
        .send()
        .await
        .expect("create_thing");
    let thing_arn = thing.thing_arn().expect("thingArn").to_string();
    assert!(thing_arn.contains(":thing/thermostat"));
    assert!(thing.thing_id().is_some());

    let described = client
        .describe_thing()
        .thing_name("thermostat")
        .send()
        .await
        .expect("describe_thing");
    assert_eq!(described.thing_name(), Some("thermostat"));
    assert_eq!(described.thing_type_name(), Some("sensor-type"));

    // --- Policy + attach ---
    client
        .create_policy()
        .policy_name("pub-sub")
        .policy_document(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .expect("create_policy");

    // --- Certificate (keys + certificate) ---
    let cert = client
        .create_keys_and_certificate()
        .set_as_active(true)
        .send()
        .await
        .expect("create_keys_and_certificate");
    let cert_arn = cert.certificate_arn().expect("certificateArn").to_string();
    let cert_id = cert.certificate_id().expect("certificateId");
    assert_eq!(cert_id.len(), 64);
    assert!(cert
        .certificate_pem()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
    assert!(cert.key_pair().is_some());

    // Attach the policy to the certificate principal, and the certificate to
    // the thing.
    client
        .attach_policy()
        .policy_name("pub-sub")
        .target(&cert_arn)
        .send()
        .await
        .expect("attach_policy");
    client
        .attach_thing_principal()
        .thing_name("thermostat")
        .principal(&cert_arn)
        .send()
        .await
        .expect("attach_thing_principal");
    let principals = client
        .list_thing_principals()
        .thing_name("thermostat")
        .send()
        .await
        .expect("list_thing_principals");
    assert_eq!(principals.principals().len(), 1);

    // --- Thing group + membership ---
    client
        .create_thing_group()
        .thing_group_name("floor-1")
        .send()
        .await
        .expect("create_thing_group");
    client
        .add_thing_to_thing_group()
        .thing_group_name("floor-1")
        .thing_name("thermostat")
        .send()
        .await
        .expect("add_thing_to_thing_group");
    let in_group = client
        .list_things_in_thing_group()
        .thing_group_name("floor-1")
        .send()
        .await
        .expect("list_things_in_thing_group");
    assert_eq!(in_group.things(), &["thermostat".to_string()]);

    // --- Job ---
    let job = client
        .create_job()
        .job_id("firmware-1")
        .targets(&thing_arn)
        .document(r#"{"operation":"update"}"#)
        .send()
        .await
        .expect("create_job");
    assert_eq!(job.job_id(), Some("firmware-1"));
    assert!(job.job_arn().unwrap().contains(":job/firmware-1"));

    // --- Topic rule ---
    let action = Action::builder().build();
    let payload = TopicRulePayload::builder()
        .sql("SELECT * FROM 'devices/+/telemetry'")
        .set_actions(Some(vec![action]))
        .build()
        .expect("topic rule payload");
    client
        .create_topic_rule()
        .rule_name("telemetry_rule")
        .topic_rule_payload(payload)
        .send()
        .await
        .expect("create_topic_rule");
    let rule = client
        .get_topic_rule()
        .rule_name("telemetry_rule")
        .send()
        .await
        .expect("get_topic_rule");
    assert_eq!(rule.rule().unwrap().rule_name(), Some("telemetry_rule"));

    // --- Endpoint ---
    let endpoint = client
        .describe_endpoint()
        .endpoint_type("iot:Data-ATS")
        .send()
        .await
        .expect("describe_endpoint");
    assert!(endpoint.endpoint_address().unwrap().contains("-ats.iot."));

    // --- Tagging ---
    client
        .tag_resource()
        .resource_arn(&thing_arn)
        .tags(
            aws_sdk_iot::types::Tag::builder()
                .key("env")
                .value("prod")
                .build()
                .expect("tag"),
        )
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&thing_arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == "env" && t.value() == Some("prod")));

    // --- List things ---
    let things = client.list_things().send().await.expect("list_things");
    assert!(things
        .things()
        .iter()
        .any(|t| t.thing_name() == Some("thermostat")));
}
