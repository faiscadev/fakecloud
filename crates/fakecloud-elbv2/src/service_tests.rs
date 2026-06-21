use super::*;
use bytes::Bytes;
use http::HeaderMap;
use parking_lot::RwLock;

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut q = std::collections::HashMap::new();
    for (k, v) in params {
        q.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "elasticloadbalancing".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "rid".to_string(),
        headers: HeaderMap::new(),
        query_params: q,
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn svc() -> Elbv2Service {
    Elbv2Service::new(Arc::new(RwLock::new(crate::state::Elbv2Accounts::new())))
}

fn body_string(resp: &AwsResponse) -> String {
    match &resp.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("not bytes"),
    }
}

#[tokio::test]
async fn create_then_describe_lb() {
    let svc = svc();
    let resp = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "myapp"),
                ("Subnets.member.1", "subnet-1"),
                ("Subnets.member.2", "subnet-2"),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    assert!(body.contains("<LoadBalancerName>myapp</LoadBalancerName>"));
    assert!(body.contains("<Type>application</Type>"));

    let resp = svc.handle(req("DescribeLoadBalancers", &[])).await.unwrap();
    let body = body_string(&resp);
    assert!(body.contains("<LoadBalancerName>myapp</LoadBalancerName>"));
}

#[tokio::test]
async fn create_validates_name() {
    let svc = svc();
    let err = svc
        .handle(req("CreateLoadBalancer", &[("Name", "internal-bad")]))
        .await
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
}

async fn create_lb_and_get_arn(svc: &Elbv2Service, name: &str) -> String {
    svc.handle(req(
        "CreateLoadBalancer",
        &[("Name", name), ("Subnets.member.1", "subnet-1")],
    ))
    .await
    .unwrap();
    let st = svc.state.read();
    st.get("123456789012")
        .unwrap()
        .load_balancers
        .values()
        .find(|lb| lb.name == name)
        .map(|lb| lb.arn.clone())
        .unwrap()
}

fn lb_exists(svc: &Elbv2Service, arn: &str) -> bool {
    let st = svc.state.read();
    st.get("123456789012")
        .map(|s| s.load_balancers.contains_key(arn))
        .unwrap_or(false)
}

#[tokio::test]
async fn delete_load_balancer_blocked_when_protection_enabled() {
    let svc = svc();
    let arn = create_lb_and_get_arn(&svc, "guarded").await;

    svc.handle(req(
        "ModifyLoadBalancerAttributes",
        &[
            ("LoadBalancerArn", &arn),
            ("Attributes.member.1.Key", "deletion_protection.enabled"),
            ("Attributes.member.1.Value", "true"),
        ],
    ))
    .await
    .unwrap();

    let err = svc
        .handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .err()
        .expect("delete must fail under deletion_protection");
    assert_eq!(err.code(), "OperationNotPermitted");
    assert!(
        err.message().contains("guarded") && err.message().contains("deletion protection"),
        "unexpected message: {}",
        err.message()
    );
    assert!(lb_exists(&svc, &arn), "LB must remain after blocked delete");
}

#[tokio::test]
async fn delete_load_balancer_succeeds_when_protection_disabled() {
    let svc = svc();
    let arn = create_lb_and_get_arn(&svc, "unguarded").await;

    svc.handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .unwrap();
    assert!(!lb_exists(&svc, &arn), "LB must be removed after delete");
}

#[tokio::test]
async fn delete_load_balancer_succeeds_after_protection_disabled() {
    let svc = svc();
    let arn = create_lb_and_get_arn(&svc, "toggled").await;

    svc.handle(req(
        "ModifyLoadBalancerAttributes",
        &[
            ("LoadBalancerArn", &arn),
            ("Attributes.member.1.Key", "deletion_protection.enabled"),
            ("Attributes.member.1.Value", "true"),
        ],
    ))
    .await
    .unwrap();
    svc.handle(req(
        "ModifyLoadBalancerAttributes",
        &[
            ("LoadBalancerArn", &arn),
            ("Attributes.member.1.Key", "deletion_protection.enabled"),
            ("Attributes.member.1.Value", "false"),
        ],
    ))
    .await
    .unwrap();

    svc.handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .unwrap();
    assert!(
        !lb_exists(&svc, &arn),
        "LB must be removed after protection disabled"
    );
}

#[tokio::test]
async fn delete_lb_is_idempotent() {
    let svc = svc();
    svc.handle(req("CreateLoadBalancer", &[("Name", "foo")]))
        .await
        .unwrap();
    let arn = {
        let st = svc.state.read();
        st.get("123456789012")
            .unwrap()
            .load_balancers
            .keys()
            .next()
            .cloned()
            .unwrap()
    };
    svc.handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .unwrap();
    svc.handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .unwrap();
}

#[tokio::test]
async fn add_remove_describe_tags_round_trip() {
    let svc = svc();
    svc.handle(req("CreateLoadBalancer", &[("Name", "tagged")]))
        .await
        .unwrap();
    let arn = svc
        .state
        .read()
        .get("123456789012")
        .unwrap()
        .load_balancers
        .keys()
        .next()
        .cloned()
        .unwrap();
    svc.handle(req(
        "AddTags",
        &[
            ("ResourceArns.member.1", &arn),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
        ],
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(req("DescribeTags", &[("ResourceArns.member.1", &arn)]))
        .await
        .unwrap();
    assert!(body_string(&resp).contains("<Key>env</Key>"));
    svc.handle(req(
        "RemoveTags",
        &[("ResourceArns.member.1", &arn), ("TagKeys.member.1", "env")],
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(req("DescribeTags", &[("ResourceArns.member.1", &arn)]))
        .await
        .unwrap();
    assert!(!body_string(&resp).contains("<Key>env</Key>"));
}

#[tokio::test]
async fn describe_account_limits_returns_known_keys() {
    let svc = svc();
    let resp = svc.handle(req("DescribeAccountLimits", &[])).await.unwrap();
    let body = body_string(&resp);
    assert!(body.contains("application-load-balancers"));
    assert!(body.contains("trust-stores"));
}

#[tokio::test]
async fn describe_ssl_policies_includes_tls13() {
    let svc = svc();
    let resp = svc.handle(req("DescribeSSLPolicies", &[])).await.unwrap();
    assert!(body_string(&resp).contains("ELBSecurityPolicy-TLS13-1-2-2021-06"));
}

async fn create_lb_and_tg_for_listener_test(svc: &Elbv2Service) -> (String, String) {
    let resp = svc
        .handle(req(
            "CreateLoadBalancer",
            &[("Name", "lvb"), ("Subnets.member.1", "subnet-1")],
        ))
        .await
        .unwrap();
    let lb_arn = {
        let st = svc.state.read();
        st.get("123456789012")
            .unwrap()
            .load_balancers
            .keys()
            .next()
            .unwrap()
            .clone()
    };
    let _ = resp;
    let resp = svc
        .handle(req(
            "CreateTargetGroup",
            &[("Name", "tg-1"), ("Protocol", "HTTP"), ("Port", "80")],
        ))
        .await
        .unwrap();
    let _ = resp;
    let tg_arn = {
        let st = svc.state.read();
        st.get("123456789012")
            .unwrap()
            .target_groups
            .keys()
            .next()
            .unwrap()
            .clone()
    };
    (lb_arn, tg_arn)
}

#[tokio::test]
async fn modify_listener_applies_mutual_authentication() {
    // ModifyListener dropped MutualAuthentication (bug-audit 2026-06-20, 1.24):
    // a listener could never toggle mTLS or change its trust store.
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    svc.handle(req(
        "CreateListener",
        &[
            ("LoadBalancerArn", &lb_arn),
            ("Protocol", "HTTP"),
            ("Port", "80"),
            ("DefaultActions.member.1.Type", "forward"),
            ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
        ],
    ))
    .await
    .unwrap();
    let listener_arn = {
        let st = svc.state.read();
        st.get("123456789012")
            .unwrap()
            .listeners
            .keys()
            .next()
            .unwrap()
            .clone()
    };

    let resp = svc
        .handle(req(
            "ModifyListener",
            &[
                ("ListenerArn", &listener_arn),
                ("MutualAuthentication.Mode", "verify"),
                (
                    "MutualAuthentication.TrustStoreArn",
                    "arn:aws:elasticloadbalancing:us-east-1:123456789012:truststore/ts/abc",
                ),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    assert!(body.contains("<Mode>verify</Mode>"), "{body}");
    assert!(body.contains("truststore/ts/abc"), "{body}");

    // Persisted, not just echoed.
    let st = svc.state.read();
    let mtls = st.get("123456789012").unwrap().listeners[&listener_arn]
        .mutual_authentication
        .as_ref()
        .unwrap();
    assert_eq!(mtls.mode.as_deref(), Some("verify"));
}

#[tokio::test]
async fn create_listener_rejects_invalid_protocol() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "BOGUS"),
                ("Port", "80"),
                ("DefaultActions.member.1.Type", "forward"),
                ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
            ],
        ))
        .await
        .err()
        .expect("expected validation error");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    assert!(format!("{err:?}").contains("BOGUS"));
}

#[tokio::test]
async fn create_listener_rejects_port_zero() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "HTTP"),
                ("Port", "0"),
                ("DefaultActions.member.1.Type", "forward"),
                ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
            ],
        ))
        .await
        .err()
        .expect("expected validation error");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
}

#[tokio::test]
async fn create_listener_rejects_port_above_65535() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "HTTP"),
                ("Port", "70000"),
                ("DefaultActions.member.1.Type", "forward"),
                ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
            ],
        ))
        .await
        .err()
        .expect("expected validation error");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
}

#[tokio::test]
async fn create_listener_accepts_alb_protocols() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    for proto in ["HTTP", "HTTPS"] {
        let res = svc
            .handle(req(
                "CreateListener",
                &[
                    ("LoadBalancerArn", &lb_arn),
                    ("Protocol", proto),
                    ("Port", "80"),
                    ("DefaultActions.member.1.Type", "forward"),
                    ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
                ],
            ))
            .await;
        if let Err(e) = res {
            panic!("protocol {proto} should be accepted on an ALB: {e:?}");
        }
    }
}

async fn create_typed_lb(svc: &Elbv2Service, name: &str, lb_type: &str) -> String {
    svc.handle(req(
        "CreateLoadBalancer",
        &[
            ("Name", name),
            ("Type", lb_type),
            ("Subnets.member.1", "subnet-1"),
        ],
    ))
    .await
    .unwrap();
    let st = svc.state.read();
    st.get("123456789012")
        .unwrap()
        .load_balancers
        .values()
        .find(|lb| lb.name == name)
        .map(|lb| lb.arn.clone())
        .unwrap()
}

#[tokio::test]
async fn create_listener_alb_rejects_tcp() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "TCP"),
                ("Port", "80"),
                ("DefaultActions.member.1.Type", "forward"),
                ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
            ],
        ))
        .await
        .err()
        .expect("TCP should be rejected on an ALB");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    assert!(format!("{err:?}").contains("application"));
}

#[tokio::test]
async fn create_listener_nlb_rejects_http() {
    let svc = svc();
    let lb_arn = create_typed_lb(&svc, "nlb", "network").await;
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "HTTP"),
                ("Port", "80"),
                ("DefaultActions.member.1.Type", "fixed-response"),
                (
                    "DefaultActions.member.1.FixedResponseConfig.StatusCode",
                    "200",
                ),
            ],
        ))
        .await
        .err()
        .expect("HTTP should be rejected on an NLB");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
}

#[tokio::test]
async fn create_listener_nlb_accepts_tcp_and_udp() {
    let svc = svc();
    let lb_arn = create_typed_lb(&svc, "nlb-ok", "network").await;
    for proto in ["TCP", "UDP", "TCP_UDP", "TLS"] {
        let res = svc
            .handle(req(
                "CreateListener",
                &[
                    ("LoadBalancerArn", &lb_arn),
                    ("Protocol", proto),
                    ("Port", "443"),
                    ("DefaultActions.member.1.Type", "fixed-response"),
                    (
                        "DefaultActions.member.1.FixedResponseConfig.StatusCode",
                        "200",
                    ),
                ],
            ))
            .await;
        if let Err(e) = res {
            panic!("protocol {proto} should be accepted on an NLB: {e:?}");
        }
    }
}

#[tokio::test]
async fn create_listener_gwlb_requires_geneve_on_6081() {
    let svc = svc();
    let lb_arn = create_typed_lb(&svc, "gwlb", "gateway").await;
    // Wrong protocol on GWLB.
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "TCP"),
                ("Port", "6081"),
                ("DefaultActions.member.1.Type", "fixed-response"),
                (
                    "DefaultActions.member.1.FixedResponseConfig.StatusCode",
                    "200",
                ),
            ],
        ))
        .await
        .err()
        .expect("TCP should be rejected on a GWLB");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    // GENEVE but wrong port.
    let err = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "GENEVE"),
                ("Port", "443"),
                ("DefaultActions.member.1.Type", "fixed-response"),
                (
                    "DefaultActions.member.1.FixedResponseConfig.StatusCode",
                    "200",
                ),
            ],
        ))
        .await
        .err()
        .expect("GENEVE on port 443 should be rejected on a GWLB");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    // GENEVE on 6081 succeeds.
    let res = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "GENEVE"),
                ("Port", "6081"),
                ("DefaultActions.member.1.Type", "fixed-response"),
                (
                    "DefaultActions.member.1.FixedResponseConfig.StatusCode",
                    "200",
                ),
            ],
        ))
        .await;
    if let Err(e) = res {
        panic!("GENEVE on 6081 should succeed: {e:?}");
    }
}

#[tokio::test]
async fn modify_load_balancer_attributes_validates_ipv6_source_nat_value() {
    let svc = svc();
    let lb_arn = create_typed_lb(&svc, "snat-lb", "network").await;
    let err = svc
        .handle(req(
            "ModifyLoadBalancerAttributes",
            &[
                ("LoadBalancerArn", &lb_arn),
                (
                    "Attributes.member.1.Key",
                    "ipv6.enable_prefix_for_source_nat",
                ),
                ("Attributes.member.1.Value", "yes"),
            ],
        ))
        .await
        .err()
        .expect("non-bool ipv6 SNAT value should be rejected");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    // All four supported values round-trip without error.
    for v in ["true", "false", "on", "off"] {
        let res = svc
            .handle(req(
                "ModifyLoadBalancerAttributes",
                &[
                    ("LoadBalancerArn", &lb_arn),
                    (
                        "Attributes.member.1.Key",
                        "ipv6.enable_prefix_for_source_nat",
                    ),
                    ("Attributes.member.1.Value", v),
                ],
            ))
            .await
            .unwrap_or_else(|e| panic!("ipv6 SNAT value {v} should be accepted: {e:?}"));
        let body = body_string(&res);
        assert!(
            body.contains(&format!(
                "<Key>ipv6.enable_prefix_for_source_nat</Key><Value>{v}</Value>"
            )),
            "round-trip should echo {v} verbatim: {body}"
        );
    }
}

#[tokio::test]
async fn modify_listener_validates_protocol_and_port() {
    let svc = svc();
    let (lb_arn, tg_arn) = create_lb_and_tg_for_listener_test(&svc).await;
    let resp = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb_arn),
                ("Protocol", "HTTP"),
                ("Port", "80"),
                ("DefaultActions.member.1.Type", "forward"),
                ("DefaultActions.member.1.TargetGroupArn", &tg_arn),
            ],
        ))
        .await
        .unwrap();
    let listener_arn = {
        let st = svc.state.read();
        st.get("123456789012")
            .unwrap()
            .listeners
            .keys()
            .next()
            .unwrap()
            .clone()
    };
    let _ = resp;
    let err = svc
        .handle(req(
            "ModifyListener",
            &[("ListenerArn", &listener_arn), ("Port", "0")],
        ))
        .await
        .err()
        .expect("port 0 should fail");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    let err = svc
        .handle(req(
            "ModifyListener",
            &[("ListenerArn", &listener_arn), ("Protocol", "BOGUS")],
        ))
        .await
        .err()
        .expect("bogus protocol should fail");
    assert_eq!(err.code(), "InvalidConfigurationRequest");
}

#[tokio::test]
async fn unimplemented_action_errors() {
    let svc = svc();
    // Use a name that is not in the AWS Smithy model so this test
    // remains stable as new ops are implemented.
    let err = svc
        .handle(req("ThisActionDoesNotExist", &[]))
        .await
        .err()
        .expect("expected error");
    assert!(matches!(err, AwsServiceError::ActionNotImplemented { .. }));
}
