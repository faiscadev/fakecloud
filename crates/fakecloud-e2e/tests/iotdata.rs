//! End-to-end tests for the AWS IoT Data Plane, driven through the real
//! `aws-sdk-iotdataplane` client against a live fakecloud server. Exercises the
//! device-shadow state machine and the retained-message store end to end:
//! update a classic shadow -> get it back (verifying the merged state, the
//! computed `delta`, and the version) -> update a named shadow -> list the
//! thing's named shadows -> publish a retained message -> get the retained
//! message -> delete the shadow.

use aws_sdk_iotdataplane::primitives::Blob;
use fakecloud_testkit::TestServer;
use serde_json::Value;

async fn iot_client(server: &TestServer) -> aws_sdk_iotdataplane::Client {
    let conf = aws_sdk_iotdataplane::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_iotdataplane::Client::from_conf(conf)
}

fn parse(blob: &Blob) -> Value {
    serde_json::from_slice(blob.as_ref()).expect("shadow document is JSON")
}

#[tokio::test]
async fn iotdata_shadow_and_retained_lifecycle() {
    let server = TestServer::start().await;
    let client = iot_client(&server).await;

    // --- Update the classic shadow: desired != reported so a delta appears ---
    let update_doc =
        br#"{"state":{"reported":{"color":"red","power":"on"},"desired":{"color":"green","power":"on"}}}"#;
    let updated = client
        .update_thing_shadow()
        .thing_name("thermostat")
        .payload(Blob::new(update_doc.to_vec()))
        .send()
        .await
        .expect("update_thing_shadow");
    let doc = parse(updated.payload().expect("payload"));
    assert_eq!(doc["version"], 1);
    assert_eq!(doc["state"]["reported"]["color"], "red");
    assert_eq!(doc["state"]["delta"]["color"], "green");

    // --- Get the shadow back: merged state + delta + version ---
    let got = client
        .get_thing_shadow()
        .thing_name("thermostat")
        .send()
        .await
        .expect("get_thing_shadow");
    let doc = parse(got.payload().expect("payload"));
    assert_eq!(doc["version"], 1);
    assert_eq!(doc["state"]["desired"]["color"], "green");
    assert_eq!(doc["state"]["reported"]["color"], "red");
    assert_eq!(doc["state"]["delta"]["color"], "green");
    assert!(doc["metadata"]["reported"]["color"]["timestamp"].is_number());
    assert!(doc["timestamp"].is_number());

    // --- Update a named shadow (isolated from the classic one) ---
    client
        .update_thing_shadow()
        .thing_name("thermostat")
        .shadow_name("firmware")
        .payload(Blob::new(
            br#"{"state":{"reported":{"version":"1.2.3"}}}"#.to_vec(),
        ))
        .send()
        .await
        .expect("update named shadow");
    let named = client
        .get_thing_shadow()
        .thing_name("thermostat")
        .shadow_name("firmware")
        .send()
        .await
        .expect("get named shadow");
    let named_doc = parse(named.payload().expect("payload"));
    assert_eq!(named_doc["state"]["reported"]["version"], "1.2.3");

    // --- List the thing's named shadows ---
    let listed = client
        .list_named_shadows_for_thing()
        .thing_name("thermostat")
        .send()
        .await
        .expect("list_named_shadows_for_thing");
    assert_eq!(listed.results(), &["firmware".to_string()]);

    // --- Publish a retained message and read it back ---
    client
        .publish()
        .topic("devices/thermostat/telemetry")
        .qos(1)
        .retain(true)
        .payload(Blob::new(b"72F".to_vec()))
        .send()
        .await
        .expect("publish retained");

    let retained = client
        .get_retained_message()
        .topic("devices/thermostat/telemetry")
        .send()
        .await
        .expect("get_retained_message");
    assert_eq!(retained.topic(), Some("devices/thermostat/telemetry"));
    assert_eq!(retained.qos(), 1);
    assert_eq!(retained.payload().expect("payload").as_ref(), b"72F");

    let all = client
        .list_retained_messages()
        .send()
        .await
        .expect("list_retained_messages");
    assert!(all
        .retained_topics()
        .iter()
        .any(|s| s.topic() == Some("devices/thermostat/telemetry")));

    // --- A non-retained publish is accepted but not stored ---
    client
        .publish()
        .topic("devices/thermostat/ephemeral")
        .qos(0)
        .payload(Blob::new(b"transient".to_vec()))
        .send()
        .await
        .expect("publish non-retained");
    let missing = client
        .get_retained_message()
        .topic("devices/thermostat/ephemeral")
        .send()
        .await;
    assert!(missing.is_err(), "non-retained publish must not be stored");

    // --- Delete the classic shadow ---
    client
        .delete_thing_shadow()
        .thing_name("thermostat")
        .send()
        .await
        .expect("delete_thing_shadow");
    let after = client
        .get_thing_shadow()
        .thing_name("thermostat")
        .send()
        .await;
    assert!(after.is_err(), "deleted shadow must be gone");
}
