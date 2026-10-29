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

    let resp = svc
        .handle(req("DeleteLoadBalancer", &[("LoadBalancerArn", &arn)]))
        .await
        .unwrap();
    assert!(!lb_exists(&svc, &arn), "LB must be removed after delete");
    // The Query-protocol response must carry the result node so the AWS SDK can
    // deserialize it ("DeleteLoadBalancerResult node not found" otherwise).
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        body.contains("<DeleteLoadBalancerResult"),
        "response must include the result node: {body}"
    );
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

// Regression: AddTrustStoreRevocations must record a real per-revocation
// entry count so DescribeTrustStore (aggregate TotalRevokedEntries) and
// DescribeTrustStoreRevocations (per-revocation NumberOfRevokedEntries)
// agree instead of reporting a total > 0 while every entry says 0.
#[tokio::test]
async fn add_trust_store_revocations_counts_agree() {
    let svc = svc();
    let resp = svc
        .handle(req(
            "CreateTrustStore",
            &[
                ("Name", "ts1"),
                ("CaCertificatesBundleS3Bucket", "certs"),
                ("CaCertificatesBundleS3Key", "bundle.pem"),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    let arn = body
        .split("<TrustStoreArn>")
        .nth(1)
        .and_then(|s| s.split("</TrustStoreArn>").next())
        .expect("arn in response")
        .to_string();

    let add = svc
        .handle(req(
            "AddTrustStoreRevocations",
            &[
                ("TrustStoreArn", &arn),
                ("RevocationContents.member.1.S3Bucket", "crls"),
                ("RevocationContents.member.1.S3Key", "a.crl"),
                ("RevocationContents.member.2.S3Bucket", "crls"),
                ("RevocationContents.member.2.S3Key", "b.crl"),
            ],
        ))
        .await
        .unwrap();
    let add_body = body_string(&add);
    // The Add response must not report 0 revoked entries per revocation.
    assert!(!add_body.contains("<NumberOfRevokedEntries>0</NumberOfRevokedEntries>"));
    assert_eq!(add_body.matches("<member>").count(), 2);

    // Aggregate on the trust store.
    let describe = svc
        .handle(req("DescribeTrustStores", &[("TrustStoreArn", &arn)]))
        .await
        .unwrap();
    let d_body = body_string(&describe);
    assert!(d_body.contains("<TotalRevokedEntries>2</TotalRevokedEntries>"));

    // Per-revocation view must sum to the aggregate.
    let revs = svc
        .handle(req(
            "DescribeTrustStoreRevocations",
            &[("TrustStoreArn", &arn)],
        ))
        .await
        .unwrap();
    let r_body = body_string(&revs);
    let per_sum: i64 = r_body
        .split("<NumberOfRevokedEntries>")
        .skip(1)
        .filter_map(|s| s.split("</NumberOfRevokedEntries>").next())
        .filter_map(|s| s.parse::<i64>().ok())
        .sum();
    assert_eq!(
        per_sum, 2,
        "per-revocation counts must sum to TotalRevokedEntries"
    );

    // Removing one revocation must keep the aggregate consistent with the
    // surviving per-revocation counts (RevocationIds are assigned from 1).
    svc.handle(req(
        "RemoveTrustStoreRevocations",
        &[("TrustStoreArn", &arn), ("RevocationIds.member.1", "1")],
    ))
    .await
    .unwrap();
    let describe = svc
        .handle(req("DescribeTrustStores", &[("TrustStoreArn", &arn)]))
        .await
        .unwrap();
    assert!(body_string(&describe).contains("<TotalRevokedEntries>1</TotalRevokedEntries>"));
}

#[tokio::test]
async fn china_region_arns_use_the_aws_cn_partition() {
    let svc = svc();
    let call = |action: &'static str, params: Vec<(&'static str, String)>| {
        let svc = &svc;
        async move {
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let mut r = req(action, &params);
            r.region = "cn-north-1".to_string();
            body_string(&svc.handle(r).await.unwrap())
        }
    };
    let between = |body: &str, tag: &str| {
        let open = format!("<{tag}>");
        let start = body.find(&open).unwrap() + open.len();
        let end = start + body[start..].find(&format!("</{tag}>")).unwrap();
        body[start..end].to_string()
    };

    let body = call(
        "CreateLoadBalancer",
        vec![
            ("Name", "cnlb".into()),
            ("Subnets.member.1", "subnet-1".into()),
        ],
    )
    .await;
    let lb_arn = between(&body, "LoadBalancerArn");
    assert!(
        lb_arn.starts_with(
            "arn:aws-cn:elasticloadbalancing:cn-north-1:123456789012:loadbalancer/app/cnlb/"
        ),
        "{lb_arn}"
    );

    let body = call(
        "CreateTargetGroup",
        vec![
            ("Name", "cntg".into()),
            ("Protocol", "HTTP".into()),
            ("Port", "80".into()),
            ("VpcId", "vpc-1".into()),
        ],
    )
    .await;
    let tg_arn = between(&body, "TargetGroupArn");
    assert!(
        tg_arn.starts_with(
            "arn:aws-cn:elasticloadbalancing:cn-north-1:123456789012:targetgroup/cntg/"
        ),
        "{tg_arn}"
    );

    let body = call(
        "CreateListener",
        vec![
            ("LoadBalancerArn", lb_arn.clone()),
            ("Protocol", "HTTP".into()),
            ("Port", "80".into()),
            ("DefaultActions.member.1.Type", "forward".into()),
            ("DefaultActions.member.1.TargetGroupArn", tg_arn.clone()),
        ],
    )
    .await;
    let listener_arn = between(&body, "ListenerArn");
    assert!(
        listener_arn.starts_with(
            "arn:aws-cn:elasticloadbalancing:cn-north-1:123456789012:listener/app/cnlb/"
        ),
        "{listener_arn}"
    );

    let body = call(
        "DescribeLoadBalancers",
        vec![("LoadBalancerArns.member.1", lb_arn.clone())],
    )
    .await;
    assert!(body.contains(&lb_arn), "{body}");
    let body = call(
        "DescribeTargetGroups",
        vec![("TargetGroupArns.member.1", tg_arn.clone())],
    )
    .await;
    assert!(body.contains(&tg_arn), "{body}");
}

// -----------------------------------------------------------------------
// VPC placement (EC2-backed) and full action config round trip
// -----------------------------------------------------------------------

fn ec2_state() -> fakecloud_ec2::SharedEc2State {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ))
}

fn ec2_svc() -> (Elbv2Service, fakecloud_ec2::SharedEc2State) {
    let ec2 = ec2_state();
    (
        Elbv2Service::new_without_dataplane(Arc::new(RwLock::new(
            crate::state::Elbv2Accounts::new(),
        )))
        .with_ec2_state(ec2.clone()),
        ec2,
    )
}

fn xml_field(body: &str, tag: &str) -> String {
    body.split(&format!("<{tag}>"))
        .nth(1)
        .and_then(|s| s.split(&format!("</{tag}>")).next())
        .unwrap_or_else(|| panic!("<{tag}> in {body}"))
        .to_string()
}

#[tokio::test]
async fn create_lb_derives_vpc_azs_and_default_sg_from_ec2() {
    let (svc, ec2) = ec2_svc();
    let subnets = fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
    let vpc = subnets[0].vpc_id.clone();
    let default_sg = fakecloud_ec2::vpc_lookup::default_security_group_id(
        &ec2,
        "123456789012",
        "us-east-1",
        &vpc,
    )
    .unwrap();
    let resp = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "placed"),
                ("Subnets.member.1", &subnets[0].subnet_id),
                ("Subnets.member.2", &subnets[1].subnet_id),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    assert_eq!(xml_field(&body, "VpcId"), vpc);
    assert_eq!(xml_field(&body, "CanonicalHostedZoneId"), "Z35SXDOTRQ7X7K");
    for s in &subnets[..2] {
        assert!(body.contains(&format!(
            "<ZoneName>{}</ZoneName><SubnetId>{}</SubnetId>",
            s.availability_zone, s.subnet_id
        )));
    }
    assert!(body.contains(&format!(
        "<SecurityGroups><member>{default_sg}</member></SecurityGroups>"
    )));

    // An NLB uses its own hosted zone and gets no default security group.
    let resp = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "net"),
                ("Type", "network"),
                ("SubnetMappings.member.1.SubnetId", &subnets[0].subnet_id),
                ("SubnetMappings.member.1.PrivateIPv4Address", "172.31.0.10"),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    assert_eq!(xml_field(&body, "CanonicalHostedZoneId"), "Z26RNL4JYFTOTI");
    assert!(body.contains("<SecurityGroups></SecurityGroups>"));
    assert!(body.contains("<PrivateIPv4Address>172.31.0.10</PrivateIPv4Address>"));
}

#[tokio::test]
async fn create_lb_rejects_unknown_subnets_and_security_groups() {
    let (svc, ec2) = ec2_svc();
    let subnets = fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
    let err = svc
        .handle(req(
            "CreateLoadBalancer",
            &[("Name", "a"), ("Subnets.member.1", "subnet-0000000000dead")],
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "SubnetNotFound");
    let err = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "b"),
                ("Subnets.member.1", &subnets[0].subnet_id),
                ("SecurityGroups.member.1", "sg-0000000000dead"),
            ],
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidSecurityGroup");
    let err = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "c"),
                ("Type", "network"),
                ("SubnetMappings.member.1.SubnetId", &subnets[0].subnet_id),
                ("SubnetMappings.member.1.PrivateIPv4Address", "10.200.0.1"),
            ],
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    let err = svc
        .handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "reserved"),
                ("Type", "network"),
                ("SubnetMappings.member.1.SubnetId", &subnets[0].subnet_id),
                ("SubnetMappings.member.1.PrivateIPv4Address", "172.31.0.2"),
            ],
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidConfigurationRequest");
    // Nothing was created by the rejected calls.
    let body = body_string(&svc.handle(req("DescribeLoadBalancers", &[])).await.unwrap());
    assert!(!body.contains("<LoadBalancerName>"));
}

#[tokio::test]
async fn listener_actions_round_trip_auth_and_stickiness() {
    let svc = svc();
    let (lb, tg) = create_lb_and_tg_for_listener_test(&svc).await;
    let resp = svc
        .handle(req(
            "CreateListener",
            &[
                ("LoadBalancerArn", &lb),
                ("Protocol", "HTTPS"),
                ("Port", "443"),
                ("Certificates.member.1.CertificateArn", "arn:aws:acm:us-east-1:123456789012:certificate/x"),
                ("DefaultActions.member.1.Type", "authenticate-oidc"),
                ("DefaultActions.member.1.Order", "1"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.Issuer", "https://idp.example.com"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.AuthorizationEndpoint", "https://idp.example.com/auth"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.TokenEndpoint", "https://idp.example.com/token"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.UserInfoEndpoint", "https://idp.example.com/userinfo"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.ClientId", "client-1"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.ClientSecret", "s3cret"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.SessionTimeout", "3600"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.AuthenticationRequestExtraParams.entry.1.key", "prompt"),
                ("DefaultActions.member.1.AuthenticateOidcConfig.AuthenticationRequestExtraParams.entry.1.value", "login"),
                ("DefaultActions.member.2.Type", "forward"),
                ("DefaultActions.member.2.Order", "2"),
                ("DefaultActions.member.2.ForwardConfig.TargetGroups.member.1.TargetGroupArn", &tg),
                ("DefaultActions.member.2.ForwardConfig.TargetGroupStickinessConfig.Enabled", "true"),
                ("DefaultActions.member.2.ForwardConfig.TargetGroupStickinessConfig.DurationSeconds", "600"),
            ],
        ))
        .await
        .unwrap();
    let listener_arn = xml_field(&body_string(&resp), "ListenerArn");
    let body = body_string(
        &svc.handle(req(
            "DescribeListeners",
            &[("ListenerArns.member.1", &listener_arn)],
        ))
        .await
        .unwrap(),
    );
    assert!(body.contains("<Issuer>https://idp.example.com</Issuer>"));
    assert!(body.contains("<ClientId>client-1</ClientId>"));
    assert!(body.contains("<SessionTimeout>3600</SessionTimeout>"));
    assert!(body.contains("<entry><key>prompt</key><value>login</value></entry>"));
    // AWS never returns the OIDC client secret.
    assert!(!body.contains("s3cret"));
    assert!(body.contains(
        "<TargetGroupStickinessConfig><Enabled>true</Enabled><DurationSeconds>600</DurationSeconds></TargetGroupStickinessConfig>"
    ));

    // A rule with a Cognito action round-trips too.
    let resp = svc
        .handle(req(
            "CreateRule",
            &[
                ("ListenerArn", &listener_arn),
                ("Priority", "10"),
                ("Conditions.member.1.Field", "path-pattern"),
                ("Conditions.member.1.Values.member.1", "/app/*"),
                ("Actions.member.1.Type", "authenticate-cognito"),
                ("Actions.member.1.Order", "1"),
                (
                    "Actions.member.1.AuthenticateCognitoConfig.UserPoolArn",
                    "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc",
                ),
                (
                    "Actions.member.1.AuthenticateCognitoConfig.UserPoolClientId",
                    "pool-client",
                ),
                (
                    "Actions.member.1.AuthenticateCognitoConfig.UserPoolDomain",
                    "auth-domain",
                ),
                (
                    "Actions.member.1.AuthenticateCognitoConfig.OnUnauthenticatedRequest",
                    "deny",
                ),
                ("Actions.member.2.Type", "forward"),
                ("Actions.member.2.Order", "2"),
                ("Actions.member.2.TargetGroupArn", &tg),
            ],
        ))
        .await
        .unwrap();
    let body = body_string(&resp);
    assert!(body.contains("<UserPoolClientId>pool-client</UserPoolClientId>"));
    assert!(body.contains("<UserPoolDomain>auth-domain</UserPoolDomain>"));
    assert!(body.contains("<OnUnauthenticatedRequest>deny</OnUnauthenticatedRequest>"));
}

#[tokio::test]
async fn legacy_invented_vpc_id_is_re_derived_not_enforced() {
    let (svc, ec2) = ec2_svc();
    let subnets = fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
    let vpc = subnets[0].vpc_id.clone();
    let default_sg = fakecloud_ec2::vpc_lookup::default_security_group_id(
        &ec2,
        "123456789012",
        "us-east-1",
        &vpc,
    )
    .unwrap();
    let body = body_string(
        &svc.handle(req(
            "CreateLoadBalancer",
            &[
                ("Name", "legacy"),
                ("Subnets.member.1", &subnets[0].subnet_id),
            ],
        ))
        .await
        .unwrap(),
    );
    let arn = xml_field(&body, "LoadBalancerArn");
    // A load balancer restored from an old snapshot carries an invented VPC.
    {
        let mut accounts = svc.state.write();
        let lb = accounts
            .get_or_create("123456789012")
            .load_balancers
            .get_mut(&arn)
            .unwrap();
        lb.vpc_id = "vpc-1a2b3c4d".to_string();
    }
    svc.handle(req(
        "SetSecurityGroups",
        &[
            ("LoadBalancerArn", &arn),
            ("SecurityGroups.member.1", &default_sg),
        ],
    ))
    .await
    .unwrap();
    svc.handle(req(
        "SetSubnets",
        &[
            ("LoadBalancerArn", &arn),
            ("Subnets.member.1", &subnets[0].subnet_id),
            ("Subnets.member.2", &subnets[1].subnet_id),
        ],
    ))
    .await
    .unwrap();
    let accounts = svc.state.read();
    assert_eq!(
        accounts.get("123456789012").unwrap().load_balancers[&arn].vpc_id,
        vpc
    );
}

#[tokio::test]
async fn restore_vpc_ids_re_derives_from_subnets() {
    let ec2 = ec2_state();
    let subnet =
        fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1").remove(0);
    let svc = Elbv2Service::new_without_dataplane(Arc::new(RwLock::new(
        crate::state::Elbv2Accounts::new(),
    )));
    let body = body_string(
        &svc.handle(req(
            "CreateLoadBalancer",
            &[("Name", "old"), ("Subnets.member.1", &subnet.subnet_id)],
        ))
        .await
        .unwrap(),
    );
    let arn = xml_field(&body, "LoadBalancerArn");
    let mut accounts = svc.state.write();
    accounts
        .get_or_create("123456789012")
        .load_balancers
        .get_mut(&arn)
        .unwrap()
        .vpc_id = "vpc-0000invented".to_string();
    assert_eq!(crate::network::restore_vpc_ids(&mut accounts, &ec2), 1);
    assert_eq!(
        accounts.get("123456789012").unwrap().load_balancers[&arn].vpc_id,
        subnet.vpc_id
    );
    assert_eq!(crate::network::restore_vpc_ids(&mut accounts, &ec2), 0);
}

/// A restored load balancer whose ARN names no region resolves its subnets
/// in the server's configured region (the EC2 container's), not us-east-1.
#[tokio::test]
async fn restore_vpc_ids_falls_back_to_server_region() {
    let ec2: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "eu-west-1", ""),
    ));
    let subnet =
        fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "eu-west-1").remove(0);
    let svc = Elbv2Service::new_without_dataplane(Arc::new(RwLock::new(
        crate::state::Elbv2Accounts::new(),
    )));
    let body = body_string(
        &svc.handle(req(
            "CreateLoadBalancer",
            &[("Name", "west"), ("Subnets.member.1", &subnet.subnet_id)],
        ))
        .await
        .unwrap(),
    );
    let arn = xml_field(&body, "LoadBalancerArn");
    let mut accounts = svc.state.write();
    {
        let lb = accounts
            .get_or_create("123456789012")
            .load_balancers
            .get_mut(&arn)
            .unwrap();
        lb.vpc_id = "vpc-0000invented".to_string();
        let mut parts: Vec<&str> = arn.split(':').collect();
        parts[3] = "";
        lb.arn = parts.join(":");
    }
    assert_eq!(crate::network::restore_vpc_ids(&mut accounts, &ec2), 1);
    assert_eq!(
        accounts.get("123456789012").unwrap().load_balancers[&arn].vpc_id,
        subnet.vpc_id
    );
}

/// `PageSize` bounds a `Describe*` page and `NextMarker` walks the rest,
/// appearing only while items remain; a marker this server never handed out
/// is rejected.
#[tokio::test]
async fn describe_target_groups_pages_with_markers() {
    let svc = svc();
    for i in 0..5 {
        let name = format!("tg-{i}");
        svc.handle(req(
            "CreateTargetGroup",
            &[
                ("Name", &name),
                ("Protocol", "HTTP"),
                ("Port", "80"),
                ("VpcId", "vpc-1"),
            ],
        ))
        .await
        .unwrap();
    }
    let names = |body: &str| -> Vec<String> {
        body.split("<TargetGroupName>")
            .skip(1)
            .map(|s| s.split("</TargetGroupName>").next().unwrap().to_string())
            .collect()
    };
    let marker = |body: &str| {
        body.split("<NextMarker>")
            .nth(1)
            .map(|s| s.split("</NextMarker>").next().unwrap().to_string())
    };
    let all = names(&body_string(
        &svc.handle(req("DescribeTargetGroups", &[])).await.unwrap(),
    ));
    assert_eq!(all.len(), 5);

    let mut walked = Vec::new();
    let mut next: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut params = vec![("PageSize", "2")];
        if let Some(m) = &next {
            params.push(("Marker", m.as_str()));
        }
        let body = body_string(
            &svc.handle(req("DescribeTargetGroups", &params))
                .await
                .unwrap(),
        );
        let page = names(&body);
        assert!(page.len() <= 2);
        walked.extend(page);
        pages += 1;
        next = marker(&body);
        if next.is_none() {
            break;
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(walked, all);

    let err = match svc
        .handle(req("DescribeTargetGroups", &[("Marker", "bogus")]))
        .await
    {
        Ok(_) => panic!("a foreign marker must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), "ValidationError");
}

/// The static catalogs page too: account limits at two per page.
#[tokio::test]
async fn describe_account_limits_pages() {
    let svc = svc();
    let body = body_string(
        &svc.handle(req("DescribeAccountLimits", &[("PageSize", "2")]))
            .await
            .unwrap(),
    );
    assert_eq!(body.matches("<member>").count(), 2, "{body}");
    assert!(body.contains("<NextMarker>2</NextMarker>"), "{body}");
}
