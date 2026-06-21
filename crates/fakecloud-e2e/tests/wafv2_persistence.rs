mod helpers;

use aws_sdk_wafv2::types::{
    AllowAction, DefaultAction, IpAddressVersion, Regex, Scope, VisibilityConfig,
};
use helpers::TestServer;

fn vis(name: &str) -> VisibilityConfig {
    VisibilityConfig::builder()
        .sampled_requests_enabled(false)
        .cloud_watch_metrics_enabled(false)
        .metric_name(name)
        .build()
        .unwrap()
}

fn allow_default() -> DefaultAction {
    DefaultAction::builder()
        .allow(AllowAction::builder().build())
        .build()
}

/// Web ACLs, IP sets, and regex pattern sets (all keyed by a (scope, name)
/// tuple) survive a restart in persistent mode. Exercises the tuple-keyed-map
/// serde path that would otherwise fail to serialize.
#[tokio::test]
async fn persistence_round_trip_acl_ipset_regexset() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let waf = server.wafv2_client().await;

    let acl_id = waf
        .create_web_acl()
        .name("acl1")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("acl1"))
        .description("persist me")
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .id
        .unwrap();

    let ipset_id = waf
        .create_ip_set()
        .name("ips1")
        .scope(Scope::Regional)
        .ip_address_version(IpAddressVersion::Ipv4)
        .addresses("10.0.0.0/8")
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .id
        .unwrap();

    let regex_id = waf
        .create_regex_pattern_set()
        .name("rps1")
        .scope(Scope::Regional)
        .regular_expression_list(Regex::builder().regex_string("^/admin").build())
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .id
        .unwrap();

    server.restart().await;
    let waf = server.wafv2_client().await;

    let acl = waf
        .get_web_acl()
        .name("acl1")
        .scope(Scope::Regional)
        .id(&acl_id)
        .send()
        .await
        .unwrap();
    assert_eq!(acl.web_acl().unwrap().name(), "acl1");

    let ips = waf
        .get_ip_set()
        .name("ips1")
        .scope(Scope::Regional)
        .id(&ipset_id)
        .send()
        .await
        .unwrap();
    assert!(ips
        .ip_set()
        .unwrap()
        .addresses()
        .contains(&"10.0.0.0/8".to_string()));

    let rps = waf
        .get_regex_pattern_set()
        .name("rps1")
        .scope(Scope::Regional)
        .id(&regex_id)
        .send()
        .await
        .unwrap();
    assert!(rps
        .regex_pattern_set()
        .unwrap()
        .regular_expression_list()
        .iter()
        .any(|r| r.regex_string() == Some("^/admin")));
}

/// A deleted web ACL stays gone after restart.
#[tokio::test]
async fn persistence_delete_web_acl_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let waf = server.wafv2_client().await;

    let summary = waf
        .create_web_acl()
        .name("ephemeral")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("ephemeral"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap();
    let id = summary.id().unwrap().to_owned();
    let lock = summary.lock_token().unwrap().to_owned();

    waf.delete_web_acl()
        .name("ephemeral")
        .scope(Scope::Regional)
        .id(&id)
        .lock_token(&lock)
        .send()
        .await
        .unwrap();

    server.restart().await;
    let waf = server.wafv2_client().await;

    let listed = waf
        .list_web_acls()
        .scope(Scope::Regional)
        .send()
        .await
        .unwrap();
    assert!(!listed
        .web_acls()
        .iter()
        .any(|a| a.name() == Some("ephemeral")));
}
