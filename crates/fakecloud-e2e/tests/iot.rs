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

    // --- Job document round-trip ---
    let job_doc = client
        .get_job_document()
        .job_id("firmware-1")
        .send()
        .await
        .expect("get_job_document");
    assert_eq!(job_doc.document(), Some(r#"{"operation":"update"}"#));

    // --- Principal listing is the inverse of the attachment ---
    let principal_things = client
        .list_principal_things()
        .principal(&cert_arn)
        .send()
        .await
        .expect("list_principal_things");
    assert!(principal_things.things().iter().any(|t| t == "thermostat"));
}

#[tokio::test]
async fn iot_policy_versioning() {
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    client
        .create_policy()
        .policy_name("versioned")
        .policy_document(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .expect("create_policy");

    // A second version, set as the new default.
    let v2 = client
        .create_policy_version()
        .policy_name("versioned")
        .policy_document(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}"#)
        .set_as_default(true)
        .send()
        .await
        .expect("create_policy_version");
    assert_eq!(v2.policy_version_id(), Some("2"));
    assert!(v2.is_default_version());

    let got = client
        .get_policy_version()
        .policy_name("versioned")
        .policy_version_id("2")
        .send()
        .await
        .expect("get_policy_version");
    assert_eq!(got.policy_version_id(), Some("2"));
    assert!(got.is_default_version());
    assert!(got.policy_document().unwrap().contains("Allow"));

    let versions = client
        .list_policy_versions()
        .policy_name("versioned")
        .send()
        .await
        .expect("list_policy_versions");
    assert_eq!(versions.policy_versions().len(), 2);
    assert_eq!(
        versions
            .policy_versions()
            .iter()
            .filter(|v| v.is_default_version())
            .count(),
        1
    );
}

#[tokio::test]
async fn iot_register_thing_returns_resource_arns() {
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    let registered = client
        .register_thing()
        .template_body(r#"{"Resources":{}}"#)
        .send()
        .await
        .expect("register_thing");
    assert!(registered
        .certificate_pem()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
    let arns = registered.resource_arns().expect("resourceArns");
    assert!(arns
        .get("thing")
        .map(|a| a.contains(":thing/"))
        .unwrap_or(false));
}

/// Regression: CreateTopicRuleDestination is server-minted (no path label), so
/// it used to hit the generic Action no-op and never persist — Get/List always
/// 404/empty. Now it stores a real destination the read path reflects.
#[tokio::test]
async fn iot_topic_rule_destination_persists() {
    use aws_sdk_iot::types::{HttpUrlDestinationConfiguration, TopicRuleDestinationConfiguration};
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    let created = client
        .create_topic_rule_destination()
        .destination_configuration(
            TopicRuleDestinationConfiguration::builder()
                .http_url_configuration(
                    HttpUrlDestinationConfiguration::builder()
                        .confirmation_url("https://example.com/confirm")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create destination");
    let arn = created
        .topic_rule_destination()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();
    assert!(arn.contains(":ruledestination/"), "arn: {arn}");

    // Get reflects it (previously 404).
    let got = client
        .get_topic_rule_destination()
        .arn(&arn)
        .send()
        .await
        .expect("get destination");
    assert_eq!(
        got.topic_rule_destination().unwrap().arn(),
        Some(arn.as_str())
    );

    // List reflects it (previously empty).
    let listed = client
        .list_topic_rule_destinations()
        .send()
        .await
        .expect("list destinations");
    assert!(
        listed
            .destination_summaries()
            .iter()
            .any(|s| s.arn() == Some(arn.as_str())),
        "created destination must appear in the list"
    );
}

/// Regression: UpdateThingGroupsForThing (bulk add/remove) used to be an Action
/// no-op, so the membership never took effect. Now it mirrors the
/// group-things relation that ListThingGroupsForThing reads.
#[tokio::test]
async fn iot_update_thing_groups_for_thing_reflected() {
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    client
        .create_thing()
        .thing_name("bulk-thing")
        .send()
        .await
        .unwrap();
    for g in ["grp-a", "grp-b"] {
        client
            .create_thing_group()
            .thing_group_name(g)
            .send()
            .await
            .unwrap();
    }

    client
        .update_thing_groups_for_thing()
        .thing_name("bulk-thing")
        .thing_groups_to_add("grp-a")
        .thing_groups_to_add("grp-b")
        .send()
        .await
        .expect("bulk add to groups");

    let groups = client
        .list_thing_groups_for_thing()
        .thing_name("bulk-thing")
        .send()
        .await
        .expect("list groups for thing");
    let names: Vec<&str> = groups
        .thing_groups()
        .iter()
        .filter_map(|g| g.group_name())
        .collect();
    assert!(
        names.contains(&"grp-a") && names.contains(&"grp-b"),
        "got: {names:?}"
    );

    // Remove one; it must drop out.
    client
        .update_thing_groups_for_thing()
        .thing_name("bulk-thing")
        .thing_groups_to_remove("grp-a")
        .send()
        .await
        .expect("bulk remove");
    let groups = client
        .list_thing_groups_for_thing()
        .thing_name("bulk-thing")
        .send()
        .await
        .unwrap();
    let names: Vec<&str> = groups
        .thing_groups()
        .iter()
        .filter_map(|g| g.group_name())
        .collect();
    assert!(
        !names.contains(&"grp-a") && names.contains(&"grp-b"),
        "got: {names:?}"
    );
}

#[tokio::test]
async fn iot_deprecate_thing_type_reflected() {
    // Bug-hunt 1.22: DeprecateThingType was an accept-and-discard no-op;
    // DescribeThingType must reflect the deprecation, and undoDeprecate revert.
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    client
        .create_thing_type()
        .thing_type_name("dep-type")
        .send()
        .await
        .expect("create_thing_type");

    client
        .deprecate_thing_type()
        .thing_type_name("dep-type")
        .send()
        .await
        .expect("deprecate_thing_type");

    let d = client
        .describe_thing_type()
        .thing_type_name("dep-type")
        .send()
        .await
        .expect("describe_thing_type");
    assert_eq!(
        d.thing_type_metadata().map(|m| m.deprecated()),
        Some(true),
        "thing type must read as deprecated"
    );

    client
        .deprecate_thing_type()
        .thing_type_name("dep-type")
        .undo_deprecate(true)
        .send()
        .await
        .expect("undo deprecate");
    let d = client
        .describe_thing_type()
        .thing_type_name("dep-type")
        .send()
        .await
        .expect("describe_thing_type");
    assert_eq!(
        d.thing_type_metadata().map(|m| m.deprecated()),
        Some(false),
        "undoDeprecate must clear the flag"
    );
}

#[tokio::test]
async fn iot_security_profile_attach_detach_reflected() {
    // Bug-hunt 1.22: Attach/DetachSecurityProfile + their list readers were
    // no-ops. Attachment must round-trip through both list directions.
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    client
        .create_security_profile()
        .security_profile_name("sp-1")
        .send()
        .await
        .expect("create_security_profile");
    // A thing group to use as the monitored target.
    let grp = client
        .create_thing_group()
        .thing_group_name("sp-target")
        .send()
        .await
        .expect("create_thing_group");
    let target_arn = grp.thing_group_arn().expect("group arn").to_string();

    client
        .attach_security_profile()
        .security_profile_name("sp-1")
        .security_profile_target_arn(&target_arn)
        .send()
        .await
        .expect("attach_security_profile");

    // Forward: targets for the profile.
    let targets = client
        .list_targets_for_security_profile()
        .security_profile_name("sp-1")
        .send()
        .await
        .expect("list_targets_for_security_profile");
    assert!(
        targets
            .security_profile_targets()
            .iter()
            .any(|t| t.arn() == target_arn.as_str()),
        "attached target must be listed"
    );

    // Inverse: profiles for the target.
    let profiles = client
        .list_security_profiles_for_target()
        .security_profile_target_arn(&target_arn)
        .send()
        .await
        .expect("list_security_profiles_for_target");
    assert!(
        profiles
            .security_profile_target_mappings()
            .iter()
            .any(|m| m
                .security_profile_identifier().map(|i| i.name()) == Some("sp-1")),
        "profile must map to the target"
    );

    // Detach clears both directions.
    client
        .detach_security_profile()
        .security_profile_name("sp-1")
        .security_profile_target_arn(&target_arn)
        .send()
        .await
        .expect("detach_security_profile");
    let targets = client
        .list_targets_for_security_profile()
        .security_profile_name("sp-1")
        .send()
        .await
        .expect("list after detach");
    assert!(
        targets.security_profile_targets().is_empty(),
        "detach must remove the target"
    );
}
