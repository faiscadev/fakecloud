//! ELBv2 (Elastic Load Balancing v2) Batch 1 conformance tests.
//!
//! Each `#[test_action]` pairs a real AWS SDK call with the Smithy shape
//! checksum. If AWS rev-bumps the ELBv2 model the checksum goes stale and
//! the build fails loudly so we know to refresh it.

mod helpers;

use aws_sdk_elasticloadbalancingv2::types::{
    Action, ActionTypeEnum, Certificate, FixedResponseActionConfig, IpAddressType,
    LoadBalancerSchemeEnum, LoadBalancerTypeEnum, ProtocolEnum, RuleCondition, Tag,
    TargetDescription, TargetGroupAttribute, TargetTypeEnum,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("elasticloadbalancingv2", "CreateLoadBalancer", checksum = "6b358bc7")]
#[tokio::test]
async fn elbv2_create_load_balancer() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let resp = client
        .create_load_balancer()
        .name("confo-create")
        .subnets("subnet-aaaa1111")
        .subnets("subnet-bbbb2222")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap();
    let lb = resp.load_balancers().first().unwrap();
    assert_eq!(lb.load_balancer_name(), Some("confo-create"));
    assert_eq!(lb.r#type(), Some(&LoadBalancerTypeEnum::Application));
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeLoadBalancers",
    checksum = "f6143c04"
)]
#[tokio::test]
async fn elbv2_describe_load_balancers() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    client
        .create_load_balancer()
        .name("confo-describe")
        .send()
        .await
        .unwrap();
    let resp = client.describe_load_balancers().send().await.unwrap();
    assert!(resp
        .load_balancers()
        .iter()
        .any(|lb| lb.load_balancer_name() == Some("confo-describe")));
}

#[test_action("elasticloadbalancingv2", "DeleteLoadBalancer", checksum = "bd05afdd")]
#[tokio::test]
async fn elbv2_delete_load_balancer() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-delete")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .delete_load_balancer()
        .load_balancer_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "SetSubnets", checksum = "6c077cf2")]
#[tokio::test]
async fn elbv2_set_subnets() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-subnets")
        .subnets("subnet-old")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    let resp = client
        .set_subnets()
        .load_balancer_arn(arn)
        .subnets("subnet-new1")
        .subnets("subnet-new2")
        .send()
        .await
        .unwrap();
    assert!(!resp.availability_zones().is_empty());
}

#[test_action("elasticloadbalancingv2", "SetSecurityGroups", checksum = "4df7135e")]
#[tokio::test]
async fn elbv2_set_security_groups() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-sg")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    let resp = client
        .set_security_groups()
        .load_balancer_arn(arn)
        .security_groups("sg-1234")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.security_group_ids().len(), 1);
}

#[test_action("elasticloadbalancingv2", "SetIpAddressType", checksum = "8445a5ab")]
#[tokio::test]
async fn elbv2_set_ip_address_type() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-ipt")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    let resp = client
        .set_ip_address_type()
        .load_balancer_arn(arn)
        .ip_address_type(IpAddressType::Dualstack)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.ip_address_type(), Some(&IpAddressType::Dualstack));
}

#[test_action("elasticloadbalancingv2", "AddTags", checksum = "04a8d014")]
#[tokio::test]
async fn elbv2_add_tags() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-addtags")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .add_tags()
        .resource_arns(arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "RemoveTags", checksum = "49e9e8cd")]
#[tokio::test]
async fn elbv2_remove_tags() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-removetags")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .add_tags()
        .resource_arns(arn)
        .tags(Tag::builder().key("k").value("v").build())
        .send()
        .await
        .unwrap();
    client
        .remove_tags()
        .resource_arns(arn)
        .tag_keys("k")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "DescribeTags", checksum = "965f2ac2")]
#[tokio::test]
async fn elbv2_describe_tags() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-desctags")
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    let resp = client
        .describe_tags()
        .resource_arns(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tag_descriptions().len(), 1);
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeAccountLimits",
    checksum = "629d38f7"
)]
#[tokio::test]
async fn elbv2_describe_account_limits() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let resp = client.describe_account_limits().send().await.unwrap();
    assert!(!resp.limits().is_empty());
}

#[test_action("elasticloadbalancingv2", "DescribeSSLPolicies", checksum = "6cba0418")]
#[tokio::test]
async fn elbv2_describe_ssl_policies() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let resp = client.describe_ssl_policies().send().await.unwrap();
    assert!(!resp.ssl_policies().is_empty());
}

#[test_action("elasticloadbalancingv2", "ModifyIpPools", checksum = "1bd4f0e7")]
#[tokio::test]
async fn elbv2_modify_ip_pools() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_load_balancer()
        .name("confo-pools")
        .scheme(LoadBalancerSchemeEnum::InternetFacing)
        .send()
        .await
        .unwrap();
    let arn = create
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .modify_ip_pools()
        .load_balancer_arn(arn)
        .send()
        .await
        .unwrap();
}

// ── Batch 2: TargetGroups + Targets ─────────────────────────────────

#[test_action("elasticloadbalancingv2", "CreateTargetGroup", checksum = "1f017667")]
#[tokio::test]
async fn elbv2_create_target_group() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let resp = client
        .create_target_group()
        .name("confo-tg-create")
        .protocol(ProtocolEnum::Http)
        .port(80)
        .target_type(TargetTypeEnum::Ip)
        .vpc_id("vpc-1234")
        .send()
        .await
        .unwrap();
    let tg = resp.target_groups().first().unwrap();
    assert_eq!(tg.target_group_name(), Some("confo-tg-create"));
    assert_eq!(tg.port(), Some(80));
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeTargetGroups",
    checksum = "46b00b84"
)]
#[tokio::test]
async fn elbv2_describe_target_groups() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    client
        .create_target_group()
        .name("confo-tg-desc")
        .send()
        .await
        .unwrap();
    let resp = client.describe_target_groups().send().await.unwrap();
    assert!(resp
        .target_groups()
        .iter()
        .any(|tg| tg.target_group_name() == Some("confo-tg-desc")));
}

#[test_action("elasticloadbalancingv2", "ModifyTargetGroup", checksum = "24ab6b92")]
#[tokio::test]
async fn elbv2_modify_target_group() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-mod")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    let resp = client
        .modify_target_group()
        .target_group_arn(arn)
        .health_check_path("/healthz")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.target_groups().first().unwrap().health_check_path(),
        Some("/healthz")
    );
}

#[test_action("elasticloadbalancingv2", "DeleteTargetGroup", checksum = "4d18f3de")]
#[tokio::test]
async fn elbv2_delete_target_group() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-del")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    client
        .delete_target_group()
        .target_group_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "RegisterTargets", checksum = "9c96083e")]
#[tokio::test]
async fn elbv2_register_targets() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-reg")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    client
        .register_targets()
        .target_group_arn(arn)
        .targets(TargetDescription::builder().id("i-aaaa").port(80).build())
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "DeregisterTargets", checksum = "a2e93f46")]
#[tokio::test]
async fn elbv2_deregister_targets() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-dereg")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    client
        .register_targets()
        .target_group_arn(arn)
        .targets(TargetDescription::builder().id("i-bbbb").build())
        .send()
        .await
        .unwrap();
    client
        .deregister_targets()
        .target_group_arn(arn)
        .targets(TargetDescription::builder().id("i-bbbb").build())
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeTargetHealth",
    checksum = "e09fc1ce"
)]
#[tokio::test]
async fn elbv2_describe_target_health() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-health")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    client
        .register_targets()
        .target_group_arn(arn)
        .targets(TargetDescription::builder().id("i-cccc").build())
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_target_health()
        .target_group_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.target_health_descriptions().len(), 1);
}

#[test_action(
    "elasticloadbalancingv2",
    "ModifyTargetGroupAttributes",
    checksum = "70f22772"
)]
#[tokio::test]
async fn elbv2_modify_target_group_attributes() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-attrs")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    client
        .modify_target_group_attributes()
        .target_group_arn(arn)
        .attributes(
            TargetGroupAttribute::builder()
                .key("deregistration_delay.timeout_seconds")
                .value("30")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeTargetGroupAttributes",
    checksum = "f426a1b9"
)]
#[tokio::test]
async fn elbv2_describe_target_group_attributes() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_target_group()
        .name("confo-tg-getattrs")
        .send()
        .await
        .unwrap();
    let arn = create
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap();
    let resp = client
        .describe_target_group_attributes()
        .target_group_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.attributes().is_empty());
}

// ── Batch 3: Listeners + Rules ──────────────────────────────────────

async fn make_lb_and_tg(
    server: &TestServer,
) -> (aws_sdk_elasticloadbalancingv2::Client, String, String) {
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-stack")
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
    let tg = client
        .create_target_group()
        .name("confo-stack-tg")
        .protocol(ProtocolEnum::Http)
        .port(80)
        .send()
        .await
        .unwrap();
    let tg_arn = tg
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap()
        .to_string();
    (client, lb_arn, tg_arn)
}

#[test_action("elasticloadbalancingv2", "CreateListener", checksum = "ad4f2eb9")]
#[tokio::test]
async fn elbv2_create_listener() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let resp = client
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
    assert_eq!(resp.listeners().first().unwrap().port(), Some(80));
}

#[test_action("elasticloadbalancingv2", "DescribeListeners", checksum = "05130066")]
#[tokio::test]
async fn elbv2_describe_listeners() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(8080)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_listeners()
        .load_balancer_arn(&lb_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.listeners().len(), 1);
}

#[test_action("elasticloadbalancingv2", "ModifyListener", checksum = "7e814e05")]
#[tokio::test]
async fn elbv2_modify_listener() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    let resp = client
        .modify_listener()
        .listener_arn(arn)
        .port(8081)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.listeners().first().unwrap().port(), Some(8081));
}

#[test_action("elasticloadbalancingv2", "DeleteListener", checksum = "de5fd7c9")]
#[tokio::test]
async fn elbv2_delete_listener() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    client
        .delete_listener()
        .listener_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "ModifyListenerAttributes",
    checksum = "f838206c"
)]
#[tokio::test]
async fn elbv2_modify_listener_attributes() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    client
        .modify_listener_attributes()
        .listener_arn(arn)
        .attributes(
            aws_sdk_elasticloadbalancingv2::types::ListenerAttribute::builder()
                .key("tcp.idle_timeout.seconds")
                .value("350")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeListenerAttributes",
    checksum = "3ab0c50b"
)]
#[tokio::test]
async fn elbv2_describe_listener_attributes() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    let _resp = client
        .describe_listener_attributes()
        .listener_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "AddListenerCertificates",
    checksum = "83647a8c"
)]
#[tokio::test]
async fn elbv2_add_listener_certificates() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(443)
        .protocol(ProtocolEnum::Https)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    client
        .add_listener_certificates()
        .listener_arn(arn)
        .certificates(
            Certificate::builder()
                .certificate_arn("arn:aws:acm:us-east-1:123:certificate/abc")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "RemoveListenerCertificates",
    checksum = "5f8d4fb1"
)]
#[tokio::test]
async fn elbv2_remove_listener_certificates() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(443)
        .protocol(ProtocolEnum::Https)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    client
        .add_listener_certificates()
        .listener_arn(arn)
        .certificates(
            Certificate::builder()
                .certificate_arn("arn:aws:acm:us-east-1:123:certificate/extra")
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .remove_listener_certificates()
        .listener_arn(arn)
        .certificates(
            Certificate::builder()
                .certificate_arn("arn:aws:acm:us-east-1:123:certificate/extra")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeListenerCertificates",
    checksum = "c640a1d4"
)]
#[tokio::test]
async fn elbv2_describe_listener_certificates() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let create = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(443)
        .protocol(ProtocolEnum::Https)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.listeners().first().unwrap().listener_arn().unwrap();
    client
        .describe_listener_certificates()
        .listener_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "CreateRule", checksum = "30f89c13")]
#[tokio::test]
async fn elbv2_create_rule() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let listener = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap();
    let resp = client
        .create_rule()
        .listener_arn(listener_arn)
        .priority(10)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .values("/api/*")
                .build(),
        )
        .actions(
            Action::builder()
                .r#type(ActionTypeEnum::FixedResponse)
                .fixed_response_config(
                    FixedResponseActionConfig::builder()
                        .status_code("200")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.rules().first().unwrap().priority(), Some("10"));
}

#[test_action("elasticloadbalancingv2", "DescribeRules", checksum = "918d09e8")]
#[tokio::test]
async fn elbv2_describe_rules() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let listener = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap();
    client
        .create_rule()
        .listener_arn(listener_arn)
        .priority(20)
        .conditions(
            RuleCondition::builder()
                .field("host-header")
                .values("api.example.com")
                .build(),
        )
        .actions(
            Action::builder()
                .r#type(ActionTypeEnum::FixedResponse)
                .fixed_response_config(
                    FixedResponseActionConfig::builder()
                        .status_code("404")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .describe_rules()
        .listener_arn(listener_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.rules().len(), 1);
}

#[test_action("elasticloadbalancingv2", "ModifyRule", checksum = "7544e745")]
#[tokio::test]
async fn elbv2_modify_rule() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let listener = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap();
    let rule = client
        .create_rule()
        .listener_arn(listener_arn)
        .priority(30)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .values("/v1/*")
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
        .unwrap();
    let rule_arn = rule.rules().first().unwrap().rule_arn().unwrap();
    client
        .modify_rule()
        .rule_arn(rule_arn)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .values("/v2/*")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "DeleteRule", checksum = "dd15d3f9")]
#[tokio::test]
async fn elbv2_delete_rule() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let listener = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap();
    let rule = client
        .create_rule()
        .listener_arn(listener_arn)
        .priority(40)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .values("/del/*")
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
        .unwrap();
    let rule_arn = rule.rules().first().unwrap().rule_arn().unwrap();
    client
        .delete_rule()
        .rule_arn(rule_arn)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "SetRulePriorities", checksum = "b20f678f")]
#[tokio::test]
async fn elbv2_set_rule_priorities() {
    let server = TestServer::start().await;
    let (client, lb_arn, tg_arn) = make_lb_and_tg(&server).await;
    let listener = client
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .port(80)
        .protocol(ProtocolEnum::Http)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let listener_arn = listener
        .listeners()
        .first()
        .unwrap()
        .listener_arn()
        .unwrap();
    let rule = client
        .create_rule()
        .listener_arn(listener_arn)
        .priority(50)
        .conditions(
            RuleCondition::builder()
                .field("path-pattern")
                .values("/p/*")
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
        .unwrap();
    let rule_arn = rule.rules().first().unwrap().rule_arn().unwrap();
    client
        .set_rule_priorities()
        .rule_priorities(
            aws_sdk_elasticloadbalancingv2::types::RulePriorityPair::builder()
                .rule_arn(rule_arn)
                .priority(60)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

// ── Batch 4: LB attributes + Trust Stores + Capacity ────────────────

#[test_action(
    "elasticloadbalancingv2",
    "ModifyLoadBalancerAttributes",
    checksum = "9b8482c9"
)]
#[tokio::test]
async fn elbv2_modify_load_balancer_attributes() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-lbattr")
        .send()
        .await
        .unwrap();
    let arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .modify_load_balancer_attributes()
        .load_balancer_arn(arn)
        .attributes(
            aws_sdk_elasticloadbalancingv2::types::LoadBalancerAttribute::builder()
                .key("idle_timeout.timeout_seconds")
                .value("120")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeLoadBalancerAttributes",
    checksum = "41dcd4c2"
)]
#[tokio::test]
async fn elbv2_describe_load_balancer_attributes() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-lbattr2")
        .send()
        .await
        .unwrap();
    let arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    let resp = client
        .describe_load_balancer_attributes()
        .load_balancer_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.attributes().is_empty());
}

#[test_action(
    "elasticloadbalancingv2",
    "ModifyCapacityReservation",
    checksum = "a337dfef"
)]
#[tokio::test]
async fn elbv2_modify_capacity_reservation() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-cap")
        .send()
        .await
        .unwrap();
    let arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .modify_capacity_reservation()
        .load_balancer_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeCapacityReservation",
    checksum = "07bdb2cb"
)]
#[tokio::test]
async fn elbv2_describe_capacity_reservation() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-cap2")
        .send()
        .await
        .unwrap();
    let arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .describe_capacity_reservation()
        .load_balancer_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "CreateTrustStore", checksum = "55af6076")]
#[tokio::test]
async fn elbv2_create_trust_store() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let resp = client
        .create_trust_store()
        .name("confo-ts")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.trust_stores().first().unwrap().name(),
        Some("confo-ts")
    );
}

#[test_action("elasticloadbalancingv2", "DescribeTrustStores", checksum = "7aba5e25")]
#[tokio::test]
async fn elbv2_describe_trust_stores() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    client
        .create_trust_store()
        .name("confo-ts-d")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let resp = client.describe_trust_stores().send().await.unwrap();
    assert!(!resp.trust_stores().is_empty());
}

#[test_action("elasticloadbalancingv2", "ModifyTrustStore", checksum = "9b488371")]
#[tokio::test]
async fn elbv2_modify_trust_store() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-m")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("v1.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .modify_trust_store()
        .trust_store_arn(arn)
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("v2.pem")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "DeleteTrustStore", checksum = "95a135c9")]
#[tokio::test]
async fn elbv2_delete_trust_store() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-del")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .delete_trust_store()
        .trust_store_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "AddTrustStoreRevocations",
    checksum = "252695df"
)]
#[tokio::test]
async fn elbv2_add_trust_store_revocations() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-rev")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .add_trust_store_revocations()
        .trust_store_arn(arn)
        .revocation_contents(
            aws_sdk_elasticloadbalancingv2::types::RevocationContent::builder()
                .s3_bucket("revocations")
                .s3_key("crl.pem")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "RemoveTrustStoreRevocations",
    checksum = "9c386fe2"
)]
#[tokio::test]
async fn elbv2_remove_trust_store_revocations() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-rmrev")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    let added = client
        .add_trust_store_revocations()
        .trust_store_arn(arn)
        .revocation_contents(
            aws_sdk_elasticloadbalancingv2::types::RevocationContent::builder()
                .s3_bucket("revocations")
                .s3_key("crl.pem")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = added
        .trust_store_revocations()
        .first()
        .unwrap()
        .revocation_id()
        .unwrap();
    client
        .remove_trust_store_revocations()
        .trust_store_arn(arn)
        .revocation_ids(id)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeTrustStoreRevocations",
    checksum = "cf2c6fa5"
)]
#[tokio::test]
async fn elbv2_describe_trust_store_revocations() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-descrev")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .describe_trust_store_revocations()
        .trust_store_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DescribeTrustStoreAssociations",
    checksum = "72bc3c3c"
)]
#[tokio::test]
async fn elbv2_describe_trust_store_associations() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-assoc")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .describe_trust_store_associations()
        .trust_store_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "DeleteSharedTrustStoreAssociation",
    checksum = "1a5d0ab7"
)]
#[tokio::test]
async fn elbv2_delete_shared_trust_store_association() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-shared")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    client
        .delete_shared_trust_store_association()
        .trust_store_arn(arn)
        .resource_arn("arn:aws:elasticloadbalancing:us-east-1:123:listener/app/x/y/z")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticloadbalancingv2",
    "GetTrustStoreCaCertificatesBundle",
    checksum = "a651bfd8"
)]
#[tokio::test]
async fn elbv2_get_trust_store_ca_certificates_bundle() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-bundle")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    let resp = client
        .get_trust_store_ca_certificates_bundle()
        .trust_store_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.location().unwrap_or("").is_empty());
}

#[test_action(
    "elasticloadbalancingv2",
    "GetTrustStoreRevocationContent",
    checksum = "8d2d6574"
)]
#[tokio::test]
async fn elbv2_get_trust_store_revocation_content() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let create = client
        .create_trust_store()
        .name("confo-ts-revcontent")
        .ca_certificates_bundle_s3_bucket("ca-bundles")
        .ca_certificates_bundle_s3_key("ca.pem")
        .send()
        .await
        .unwrap();
    let arn = create
        .trust_stores()
        .first()
        .unwrap()
        .trust_store_arn()
        .unwrap();
    let added = client
        .add_trust_store_revocations()
        .trust_store_arn(arn)
        .revocation_contents(
            aws_sdk_elasticloadbalancingv2::types::RevocationContent::builder()
                .s3_bucket("revs")
                .s3_key("crl.pem")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = added
        .trust_store_revocations()
        .first()
        .unwrap()
        .revocation_id()
        .unwrap();
    client
        .get_trust_store_revocation_content()
        .trust_store_arn(arn)
        .revocation_id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticloadbalancingv2", "GetResourcePolicy", checksum = "1424da94")]
#[tokio::test]
async fn elbv2_get_resource_policy() {
    let server = TestServer::start().await;
    let client = server.elbv2_client().await;
    let lb = client
        .create_load_balancer()
        .name("confo-rp")
        .send()
        .await
        .unwrap();
    let arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap();
    client
        .get_resource_policy()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}
