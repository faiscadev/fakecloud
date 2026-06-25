//! ELBv2 control-plane validation E2E (Q4):
//! - Listener Protocol/Port matrix per LB type.
//! - `ipv6.enable_prefix_for_source_nat` round-trip as bool.
//! - WAFv2 `AssociateWebACL` accepts both LoadBalancer and Listener
//!   ARNs against an ALB.

mod helpers;

use aws_sdk_elasticloadbalancingv2::types::{
    Action, ActionTypeEnum, FixedResponseActionConfig, HostHeaderConditionConfig,
    LoadBalancerAttribute, LoadBalancerTypeEnum, PathPatternConditionConfig, ProtocolEnum,
    RuleCondition,
};
use aws_sdk_wafv2::types::{DefaultAction, Scope, VisibilityConfig};
use helpers::TestServer;

fn fixed_200() -> Action {
    Action::builder()
        .r#type(ActionTypeEnum::FixedResponse)
        .fixed_response_config(
            FixedResponseActionConfig::builder()
                .status_code("200")
                .build(),
        )
        .build()
}

#[tokio::test]
async fn create_listener_alb_rejects_tcp_protocol() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb = elbv2
        .create_load_balancer()
        .name("alb-bad-proto")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap();
    let lb_arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    let err = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Tcp)
        .port(80)
        .default_actions(fixed_200())
        .send()
        .await
        .expect_err("ALB should reject TCP listener");
    let msg = format!("{err:?}");
    assert!(msg.contains("InvalidConfigurationRequest"), "{msg}");
    assert!(msg.contains("application"), "{msg}");
}

#[tokio::test]
async fn create_listener_alb_accepts_http_and_https() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb_arn = elbv2
        .create_load_balancer()
        .name("alb-good-proto")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    for (proto, port) in [(ProtocolEnum::Http, 80), (ProtocolEnum::Https, 443)] {
        elbv2
            .create_listener()
            .load_balancer_arn(&lb_arn)
            .protocol(proto.clone())
            .port(port)
            .default_actions(fixed_200())
            .send()
            .await
            .unwrap_or_else(|e| panic!("ALB should accept {proto:?}: {e:?}"));
    }
}

#[tokio::test]
async fn create_listener_nlb_rejects_http_protocol() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb_arn = elbv2
        .create_load_balancer()
        .name("nlb-bad-proto")
        .r#type(LoadBalancerTypeEnum::Network)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    let err = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(fixed_200())
        .send()
        .await
        .expect_err("NLB should reject HTTP listener");
    let msg = format!("{err:?}");
    assert!(msg.contains("InvalidConfigurationRequest"), "{msg}");
    assert!(msg.contains("network"), "{msg}");
}

#[tokio::test]
async fn create_listener_nlb_accepts_tcp_udp_tls() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb_arn = elbv2
        .create_load_balancer()
        .name("nlb-good-proto")
        .r#type(LoadBalancerTypeEnum::Network)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    for (proto, port) in [
        (ProtocolEnum::Tcp, 1234),
        (ProtocolEnum::Udp, 1235),
        (ProtocolEnum::TcpUdp, 1236),
        (ProtocolEnum::Tls, 1237),
    ] {
        elbv2
            .create_listener()
            .load_balancer_arn(&lb_arn)
            .protocol(proto.clone())
            .port(port)
            .default_actions(fixed_200())
            .send()
            .await
            .unwrap_or_else(|e| panic!("NLB should accept {proto:?}: {e:?}"));
    }
}

#[tokio::test]
async fn create_listener_gwlb_requires_geneve_on_6081() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb_arn = elbv2
        .create_load_balancer()
        .name("gwlb-strict")
        .r#type(LoadBalancerTypeEnum::Gateway)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    // Wrong protocol on GWLB.
    let err = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Tcp)
        .port(6081)
        .default_actions(fixed_200())
        .send()
        .await
        .expect_err("GWLB should reject TCP");
    assert!(format!("{err:?}").contains("InvalidConfigurationRequest"));
    // GENEVE on the wrong port.
    let err = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Geneve)
        .port(443)
        .default_actions(fixed_200())
        .send()
        .await
        .expect_err("GWLB GENEVE on 443 should be rejected");
    assert!(format!("{err:?}").contains("6081"));
    // GENEVE on 6081 is accepted.
    elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Geneve)
        .port(6081)
        .default_actions(fixed_200())
        .send()
        .await
        .expect("GWLB GENEVE/6081 should be accepted");
}

#[tokio::test]
async fn modify_load_balancer_attributes_round_trips_ipv6_source_nat() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let lb_arn = elbv2
        .create_load_balancer()
        .name("nlb-snat")
        .r#type(LoadBalancerTypeEnum::Network)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();

    // Reject a non-bool value.
    let err = elbv2
        .modify_load_balancer_attributes()
        .load_balancer_arn(&lb_arn)
        .attributes(
            LoadBalancerAttribute::builder()
                .key("ipv6.enable_prefix_for_source_nat")
                .value("yes")
                .build(),
        )
        .send()
        .await
        .expect_err("non-bool ipv6 SNAT value should be rejected");
    assert!(format!("{err:?}").contains("InvalidConfigurationRequest"));

    // Accept all four valid values and verify round-trip via Describe.
    for v in ["true", "false", "on", "off"] {
        elbv2
            .modify_load_balancer_attributes()
            .load_balancer_arn(&lb_arn)
            .attributes(
                LoadBalancerAttribute::builder()
                    .key("ipv6.enable_prefix_for_source_nat")
                    .value(v)
                    .build(),
            )
            .send()
            .await
            .unwrap_or_else(|e| panic!("ipv6 SNAT value {v} should be accepted: {e:?}"));

        let described = elbv2
            .describe_load_balancer_attributes()
            .load_balancer_arn(&lb_arn)
            .send()
            .await
            .unwrap();
        let echoed = described
            .attributes()
            .iter()
            .find(|a| a.key() == Some("ipv6.enable_prefix_for_source_nat"))
            .and_then(|a| a.value())
            .map(str::to_owned)
            .unwrap_or_default();
        assert_eq!(echoed, v, "ipv6 SNAT value should round-trip verbatim");
    }
}

fn allow_default() -> DefaultAction {
    DefaultAction::builder()
        .allow(aws_sdk_wafv2::types::AllowAction::builder().build())
        .build()
}

fn vis(name: &str) -> VisibilityConfig {
    VisibilityConfig::builder()
        .sampled_requests_enabled(false)
        .cloud_watch_metrics_enabled(false)
        .metric_name(name)
        .build()
        .unwrap()
}

#[tokio::test]
async fn associate_web_acl_accepts_listener_arn_against_load_balancer() {
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;
    let waf = server.wafv2_client().await;

    let lb_arn = elbv2
        .create_load_balancer()
        .name("waf-alb")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();

    let listener_arn = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(fixed_200())
        .send()
        .await
        .unwrap()
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap()
        .to_string();

    let acl_arn = waf
        .create_web_acl()
        .name("alb-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("alb-acl"))
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .arn()
        .expect("arn")
        .to_owned();

    // Listener ARN should be normalized to the load-balancer ARN
    // server-side, so a follow-up GetWebACLForResource on the LB
    // ARN sees the association.
    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(&listener_arn)
        .send()
        .await
        .unwrap();

    let got_via_lb = waf
        .get_web_acl_for_resource()
        .resource_arn(&lb_arn)
        .send()
        .await
        .unwrap();
    assert!(
        got_via_lb.web_acl().is_some(),
        "associating via Listener ARN should be visible via the LB ARN"
    );
    let got_via_listener = waf
        .get_web_acl_for_resource()
        .resource_arn(&listener_arn)
        .send()
        .await
        .unwrap();
    assert!(
        got_via_listener.web_acl().is_some(),
        "Listener ARN lookups should also resolve once normalized"
    );

    // Disassociating via the LB ARN should clear the lookup whether
    // we go through the listener or the LB.
    waf.disassociate_web_acl()
        .resource_arn(&lb_arn)
        .send()
        .await
        .unwrap();
    let after = waf
        .get_web_acl_for_resource()
        .resource_arn(&listener_arn)
        .send()
        .await
        .unwrap();
    assert!(after.web_acl().is_none());
}

#[tokio::test]
async fn http_target_group_reports_protocol_version_and_attr_defaults() {
    // An HTTP target group reports its default ProtocolVersion (HTTP1), and
    // DescribeTargetGroupAttributes returns the cross-zone + anomaly-mitigation
    // defaults the aws_lb_target_group data source reads.
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;

    let tg = elbv2
        .create_target_group()
        .name("tg-pv")
        .protocol(ProtocolEnum::Http)
        .port(80)
        .vpc_id("vpc-12345678")
        .target_type(aws_sdk_elasticloadbalancingv2::types::TargetTypeEnum::Instance)
        .send()
        .await
        .expect("create_target_group");
    let arn = tg.target_groups()[0]
        .target_group_arn()
        .unwrap()
        .to_string();
    assert_eq!(
        tg.target_groups()[0].protocol_version(),
        Some("HTTP1"),
        "HTTP target group must report HTTP1 protocol version"
    );

    let attrs = elbv2
        .describe_target_group_attributes()
        .target_group_arn(&arn)
        .send()
        .await
        .expect("describe attributes");
    let get = |k: &str| {
        attrs
            .attributes()
            .iter()
            .find(|a| a.key() == Some(k))
            .and_then(|a| a.value())
            .map(str::to_string)
    };
    assert_eq!(
        get("load_balancing.cross_zone.enabled").as_deref(),
        Some("use_load_balancer_configuration")
    );
    assert_eq!(
        get("load_balancing.algorithm.anomaly_mitigation").as_deref(),
        Some("off")
    );
}

#[tokio::test]
async fn listener_rule_conditions_echo_typed_config() {
    // A listener rule created with typed condition configs (path-pattern,
    // host-header) must echo those typed `*Config` sub-objects back on
    // DescribeRules. AWS always returns them, and terraform-provider-aws
    // nil-derefs `condition.PathPatternConfig.Values` in resourceListenerRuleRead
    // if they are absent.
    let server = TestServer::start().await;
    let elbv2 = server.elbv2_client().await;

    let lb = elbv2
        .create_load_balancer()
        .name("alb-rules")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap();
    let lb_arn = lb.load_balancers()[0]
        .load_balancer_arn()
        .unwrap()
        .to_string();
    let tg = elbv2
        .create_target_group()
        .name("tg-rules")
        .protocol(ProtocolEnum::Http)
        .port(80)
        .vpc_id("vpc-12345678")
        .send()
        .await
        .unwrap();
    let tg_arn = tg.target_groups()[0]
        .target_group_arn()
        .unwrap()
        .to_string();
    let listener = elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener.listeners()[0].listener_arn().unwrap().to_string();

    elbv2
        .create_rule()
        .listener_arn(&listener_arn)
        .priority(100)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .path_pattern_config(
                    PathPatternConditionConfig::builder()
                        .values("/static/*")
                        .build(),
                )
                .build(),
        )
        .conditions(
            RuleCondition::builder()
                .field("host-header")
                .host_header_config(
                    HostHeaderConditionConfig::builder()
                        .values("example.com")
                        .build(),
                )
                .build(),
        )
        .actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .expect("create_rule");

    let rules = elbv2
        .describe_rules()
        .listener_arn(&listener_arn)
        .send()
        .await
        .expect("describe_rules");
    let rule = rules
        .rules()
        .iter()
        .find(|r| r.priority() == Some("100"))
        .expect("rule with priority 100");

    let path_cond = rule
        .conditions()
        .iter()
        .find(|c| c.field() == Some("path-pattern"))
        .expect("path-pattern condition");
    assert_eq!(
        path_cond
            .path_pattern_config()
            .map(|c| c.values())
            .unwrap_or_default(),
        &["/static/*"],
        "path-pattern condition must echo PathPatternConfig.Values"
    );

    let host_cond = rule
        .conditions()
        .iter()
        .find(|c| c.field() == Some("host-header"))
        .expect("host-header condition");
    assert_eq!(
        host_cond
            .host_header_config()
            .map(|c| c.values())
            .unwrap_or_default(),
        &["example.com"],
        "host-header condition must echo HostHeaderConfig.Values"
    );
}
