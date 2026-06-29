//! EC2 long-tail `Modify*`/`Create*` round-trip E2E. These ops previously
//! validated their input and returned a minimal success WITHOUT persisting
//! anything, so the paired `Describe*`/`Get*` reported the unchanged default.
//! Each test here drives the real AWS SDK to prove the change is now persisted
//! and reflected back: id-format, managed prefix lists, instance event windows,
//! default credit specs, VPC block-public-access, traffic mirroring, and
//! per-instance private DNS name options.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn id_format_modify_is_reflected_by_describe() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    c.modify_id_format()
        .resource("vpc")
        .use_long_ids(true)
        .send()
        .await
        .unwrap();

    let out = c.describe_id_format().resource("vpc").send().await.unwrap();
    let status = out
        .statuses()
        .iter()
        .find(|s| s.resource() == Some("vpc"))
        .expect("vpc id-format status present");
    assert_eq!(status.use_long_ids(), Some(true));
}

#[tokio::test]
async fn managed_prefix_list_create_modify_describe_round_trip() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let created = c
        .create_managed_prefix_list()
        .prefix_list_name("test-pl")
        .max_entries(20)
        .address_family("IPv4")
        .entries(
            aws_sdk_ec2::types::AddPrefixListEntry::builder()
                .cidr("10.0.0.0/24")
                .description("first")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let pl = created.prefix_list().unwrap();
    let id = pl.prefix_list_id().unwrap().to_string();
    assert_eq!(pl.prefix_list_name(), Some("test-pl"));

    // Add an entry; the version should bump.
    c.modify_managed_prefix_list()
        .prefix_list_id(&id)
        .add_entries(
            aws_sdk_ec2::types::AddPrefixListEntry::builder()
                .cidr("10.0.1.0/24")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let listed = c
        .describe_managed_prefix_lists()
        .prefix_list_ids(&id)
        .send()
        .await
        .unwrap();
    let found = listed
        .prefix_lists()
        .iter()
        .find(|p| p.prefix_list_id() == Some(id.as_str()))
        .expect("prefix list persisted");
    assert_eq!(found.version(), Some(2));

    let entries = c
        .get_managed_prefix_list_entries()
        .prefix_list_id(&id)
        .send()
        .await
        .unwrap();
    let cidrs: Vec<&str> = entries.entries().iter().filter_map(|e| e.cidr()).collect();
    assert!(cidrs.contains(&"10.0.0.0/24"));
    assert!(cidrs.contains(&"10.0.1.0/24"));
}

#[tokio::test]
async fn instance_event_window_create_modify_describe_round_trip() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let created = c
        .create_instance_event_window()
        .name("maint")
        .cron_expression("* 21-23 * * 2,3")
        .send()
        .await
        .unwrap();
    let id = created
        .instance_event_window()
        .unwrap()
        .instance_event_window_id()
        .unwrap()
        .to_string();

    c.modify_instance_event_window()
        .instance_event_window_id(&id)
        .name("maint-2")
        .send()
        .await
        .unwrap();

    let listed = c
        .describe_instance_event_windows()
        .instance_event_window_ids(&id)
        .send()
        .await
        .unwrap();
    let w = listed
        .instance_event_windows()
        .iter()
        .find(|w| w.instance_event_window_id() == Some(id.as_str()))
        .expect("event window persisted");
    assert_eq!(w.name(), Some("maint-2"));
}

#[tokio::test]
async fn default_credit_specification_modify_is_reflected_by_get() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    c.modify_default_credit_specification()
        .instance_family(aws_sdk_ec2::types::UnlimitedSupportedInstanceFamily::T3)
        .cpu_credits("unlimited")
        .send()
        .await
        .unwrap();

    let got = c
        .get_default_credit_specification()
        .instance_family(aws_sdk_ec2::types::UnlimitedSupportedInstanceFamily::T3)
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.instance_family_credit_specification()
            .and_then(|s| s.cpu_credits()),
        Some("unlimited")
    );
}

#[tokio::test]
async fn vpc_block_public_access_options_round_trip() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    c.modify_vpc_block_public_access_options()
        .internet_gateway_block_mode(
            aws_sdk_ec2::types::InternetGatewayBlockMode::BlockBidirectional,
        )
        .send()
        .await
        .unwrap();

    let got = c
        .describe_vpc_block_public_access_options()
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.vpc_block_public_access_options()
            .and_then(|o| o.internet_gateway_block_mode()),
        Some(&aws_sdk_ec2::types::InternetGatewayBlockMode::BlockBidirectional)
    );
}

#[tokio::test]
async fn traffic_mirror_target_filter_session_round_trip() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let target = c
        .create_traffic_mirror_target()
        .network_interface_id("eni-aaaa")
        .description("t")
        .send()
        .await
        .unwrap();
    let tid = target
        .traffic_mirror_target()
        .unwrap()
        .traffic_mirror_target_id()
        .unwrap()
        .to_string();

    let filter = c
        .create_traffic_mirror_filter()
        .description("f")
        .send()
        .await
        .unwrap();
    let fid = filter
        .traffic_mirror_filter()
        .unwrap()
        .traffic_mirror_filter_id()
        .unwrap()
        .to_string();

    c.create_traffic_mirror_filter_rule()
        .traffic_mirror_filter_id(&fid)
        .traffic_direction(aws_sdk_ec2::types::TrafficDirection::Ingress)
        .rule_number(100)
        .rule_action(aws_sdk_ec2::types::TrafficMirrorRuleAction::Accept)
        .destination_cidr_block("0.0.0.0/0")
        .source_cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();

    let session = c
        .create_traffic_mirror_session()
        .network_interface_id("eni-bbbb")
        .traffic_mirror_target_id(&tid)
        .traffic_mirror_filter_id(&fid)
        .session_number(1)
        .send()
        .await
        .unwrap();
    let sid = session
        .traffic_mirror_session()
        .unwrap()
        .traffic_mirror_session_id()
        .unwrap()
        .to_string();

    // Filter (with its rule) is reflected.
    let filters = c
        .describe_traffic_mirror_filters()
        .traffic_mirror_filter_ids(&fid)
        .send()
        .await
        .unwrap();
    let f = filters
        .traffic_mirror_filters()
        .iter()
        .find(|f| f.traffic_mirror_filter_id() == Some(fid.as_str()))
        .expect("filter persisted");
    assert!(f
        .ingress_filter_rules()
        .iter()
        .any(|r| r.rule_number() == Some(100)));

    // Session is reflected and points at the target/filter.
    let sessions = c
        .describe_traffic_mirror_sessions()
        .traffic_mirror_session_ids(&sid)
        .send()
        .await
        .unwrap();
    let sess = sessions
        .traffic_mirror_sessions()
        .iter()
        .find(|s| s.traffic_mirror_session_id() == Some(sid.as_str()))
        .expect("session persisted");
    assert_eq!(sess.traffic_mirror_target_id(), Some(tid.as_str()));
    assert_eq!(sess.traffic_mirror_filter_id(), Some(fid.as_str()));
}

#[tokio::test]
async fn modify_private_dns_name_options_reflected_by_describe_instances() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;

    let run = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .send()
        .await
        .unwrap();
    let iid = run.instances()[0].instance_id().unwrap().to_string();

    c.modify_private_dns_name_options()
        .instance_id(&iid)
        .private_dns_hostname_type(aws_sdk_ec2::types::HostnameType::ResourceName)
        .enable_resource_name_dns_a_record(true)
        .send()
        .await
        .unwrap();

    let desc = c
        .describe_instances()
        .instance_ids(&iid)
        .send()
        .await
        .unwrap();
    let opts = desc.reservations()[0].instances()[0]
        .private_dns_name_options()
        .expect("private dns name options present");
    assert_eq!(
        opts.hostname_type(),
        Some(&aws_sdk_ec2::types::HostnameType::ResourceName)
    );
    assert_eq!(opts.enable_resource_name_dns_a_record(), Some(true));
}
