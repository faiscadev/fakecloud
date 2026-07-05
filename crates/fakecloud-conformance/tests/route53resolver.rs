//! Conformance coverage for Amazon Route 53 Resolver (`route53resolver`,
//! awsJson1_1, target prefix `Route53Resolver`).
//!
//! There is no typed `aws-sdk-route53resolver` in this workspace, so every
//! operation is driven over raw HTTP with the `X-Amz-Target` header, mirroring
//! the acm-pca suite. Resolver endpoints reference real EC2 VPC subnets +
//! security groups, so the setup helper creates those with the EC2 SDK client
//! first. One `#[test_action]` per `SUPPORTED_ACTIONS` entry pins the operation
//! to its Smithy checksum so model drift fails the build; the audit
//! cross-checks this list against the service crate.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use serde_json::{json, Value};

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/route53resolver/aws4_request, SignedHeaders=host, Signature=0";

/// POST an awsJson1_1 Route 53 Resolver action, returning `(status, body)`.
async fn r53r(server: &TestServer, op: &str, body: Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-amz-json-1.1")
        .header("x-amz-target", format!("Route53Resolver.{op}"))
        .header("Authorization", AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, parsed)
}

/// Real VPC + two subnets + a security group, created with the EC2 SDK so the
/// Resolver endpoint's cross-service validation passes.
struct Vpc {
    subnet1: String,
    subnet2: String,
    sg: String,
    vpc: String,
}

async fn setup_vpc(server: &TestServer) -> Vpc {
    let ec2 = server.ec2_client().await;
    let vpc = ec2
        .create_vpc()
        .cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap()
        .vpc
        .unwrap()
        .vpc_id
        .unwrap();
    let subnet1 = ec2
        .create_subnet()
        .vpc_id(&vpc)
        .cidr_block("10.0.1.0/24")
        .availability_zone("us-east-1a")
        .send()
        .await
        .unwrap()
        .subnet
        .unwrap()
        .subnet_id
        .unwrap();
    let subnet2 = ec2
        .create_subnet()
        .vpc_id(&vpc)
        .cidr_block("10.0.2.0/24")
        .availability_zone("us-east-1b")
        .send()
        .await
        .unwrap()
        .subnet
        .unwrap()
        .subnet_id
        .unwrap();
    let sg = ec2
        .create_security_group()
        .group_name("resolver-sg")
        .description("resolver sg")
        .vpc_id(&vpc)
        .send()
        .await
        .unwrap()
        .group_id
        .unwrap();
    Vpc {
        subnet1,
        subnet2,
        sg,
        vpc,
    }
}

/// Create an OUTBOUND resolver endpoint and return its id.
async fn make_outbound_endpoint(server: &TestServer, v: &Vpc) -> String {
    let (status, body) = r53r(
        server,
        "CreateResolverEndpoint",
        json!({
            "CreatorRequestId": "conf-ep-1",
            "Direction": "OUTBOUND",
            "SecurityGroupIds": [v.sg],
            "IpAddresses": [
                { "SubnetId": v.subnet1 },
                { "SubnetId": v.subnet2 },
            ],
        }),
    )
    .await;
    assert_eq!(status, 200, "create endpoint: {body}");
    body["ResolverEndpoint"]["Id"].as_str().unwrap().to_string()
}

/// Create a FORWARD resolver rule bound to the given outbound endpoint.
async fn make_forward_rule(server: &TestServer, endpoint_id: &str) -> String {
    let (status, body) = r53r(
        server,
        "CreateResolverRule",
        json!({
            "CreatorRequestId": "conf-rule-1",
            "RuleType": "FORWARD",
            "DomainName": "example.com",
            "Name": "conf-rule",
            "ResolverEndpointId": endpoint_id,
            "TargetIps": [ { "Ip": "10.0.1.5", "Port": 53 } ],
        }),
    )
    .await;
    assert_eq!(status, 200, "create rule: {body}");
    body["ResolverRule"]["Id"].as_str().unwrap().to_string()
}

async fn make_firewall_rule_group(server: &TestServer) -> String {
    let (status, body) = r53r(
        server,
        "CreateFirewallRuleGroup",
        json!({ "CreatorRequestId": "conf-frg-1", "Name": "conf-frg" }),
    )
    .await;
    assert_eq!(status, 200, "create rule group: {body}");
    body["FirewallRuleGroup"]["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn make_firewall_domain_list(server: &TestServer) -> String {
    let (status, body) = r53r(
        server,
        "CreateFirewallDomainList",
        json!({ "CreatorRequestId": "conf-fdl-1", "Name": "conf-fdl" }),
    )
    .await;
    assert_eq!(status, 200, "create domain list: {body}");
    body["FirewallDomainList"]["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn make_query_log_config(server: &TestServer) -> String {
    let (status, body) = r53r(
        server,
        "CreateResolverQueryLogConfig",
        json!({
            "Name": "conf-qlc",
            "DestinationArn": "arn:aws:logs:us-east-1:000000000000:log-group:/conf",
            "CreatorRequestId": "conf-qlc-1",
        }),
    )
    .await;
    assert_eq!(status, 200, "create query log config: {body}");
    body["ResolverQueryLogConfig"]["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

// ─── Resolver endpoints ──────────────────────────────────────────────────

#[test_action("route53resolver", "CreateResolverEndpoint", checksum = "862fcc40")]
#[tokio::test]
async fn create_resolver_endpoint() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    assert!(id.starts_with("rslvr-out-"));
}

#[test_action("route53resolver", "GetResolverEndpoint", checksum = "b76962f2")]
#[tokio::test]
async fn get_resolver_endpoint() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(
        &server,
        "GetResolverEndpoint",
        json!({ "ResolverEndpointId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverEndpoint"]["Direction"], "OUTBOUND");
}

#[test_action("route53resolver", "UpdateResolverEndpoint", checksum = "c80793ed")]
#[tokio::test]
async fn update_resolver_endpoint() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(
        &server,
        "UpdateResolverEndpoint",
        json!({ "ResolverEndpointId": id, "Name": "renamed" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverEndpoint"]["Name"], "renamed");
}

#[test_action("route53resolver", "DeleteResolverEndpoint", checksum = "080f5a71")]
#[tokio::test]
async fn delete_resolver_endpoint() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(
        &server,
        "DeleteResolverEndpoint",
        json!({ "ResolverEndpointId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverEndpoint"]["Status"], "DELETING");
}

#[test_action("route53resolver", "ListResolverEndpoints", checksum = "edd43ab7")]
#[tokio::test]
async fn list_resolver_endpoints() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(&server, "ListResolverEndpoints", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverEndpoints"].as_array().unwrap().is_empty());
}

#[test_action(
    "route53resolver",
    "ListResolverEndpointIpAddresses",
    checksum = "3e2b4662"
)]
#[tokio::test]
async fn list_resolver_endpoint_ip_addresses() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(
        &server,
        "ListResolverEndpointIpAddresses",
        json!({ "ResolverEndpointId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["IpAddresses"].as_array().unwrap().len(), 2);
}

#[test_action(
    "route53resolver",
    "AssociateResolverEndpointIpAddress",
    checksum = "036b51dd"
)]
#[tokio::test]
async fn associate_resolver_endpoint_ip_address() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    let (status, body) = r53r(
        &server,
        "AssociateResolverEndpointIpAddress",
        json!({ "ResolverEndpointId": id, "IpAddress": { "SubnetId": v.subnet1 } }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverEndpoint"]["IpAddressCount"], 3);
}

#[test_action(
    "route53resolver",
    "DisassociateResolverEndpointIpAddress",
    checksum = "b5009729"
)]
#[tokio::test]
async fn disassociate_resolver_endpoint_ip_address() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_outbound_endpoint(&server, &v).await;
    r53r(
        &server,
        "AssociateResolverEndpointIpAddress",
        json!({ "ResolverEndpointId": id, "IpAddress": { "SubnetId": v.subnet1, "Ip": "10.0.1.99" } }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "DisassociateResolverEndpointIpAddress",
        json!({ "ResolverEndpointId": id, "IpAddress": { "SubnetId": v.subnet1, "Ip": "10.0.1.99" } }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverEndpoint"]["IpAddressCount"], 2);
}

// ─── Resolver rules + associations ───────────────────────────────────────

#[test_action("route53resolver", "CreateResolverRule", checksum = "95847947")]
#[tokio::test]
async fn create_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let id = make_forward_rule(&server, &ep).await;
    assert!(id.starts_with("rslvr-rr-"));
}

#[test_action("route53resolver", "GetResolverRule", checksum = "3b739d05")]
#[tokio::test]
async fn get_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let id = make_forward_rule(&server, &ep).await;
    let (status, body) = r53r(&server, "GetResolverRule", json!({ "ResolverRuleId": id })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRule"]["DomainName"], "example.com");
}

#[test_action("route53resolver", "UpdateResolverRule", checksum = "a8d80862")]
#[tokio::test]
async fn update_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let id = make_forward_rule(&server, &ep).await;
    let (status, body) = r53r(
        &server,
        "UpdateResolverRule",
        json!({ "ResolverRuleId": id, "Config": { "Name": "updated", "ResolverEndpointId": ep } }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRule"]["Name"], "updated");
}

#[test_action("route53resolver", "DeleteResolverRule", checksum = "12c01e07")]
#[tokio::test]
async fn delete_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let id = make_forward_rule(&server, &ep).await;
    let (status, body) = r53r(
        &server,
        "DeleteResolverRule",
        json!({ "ResolverRuleId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRule"]["Status"], "DELETING");
}

#[test_action("route53resolver", "ListResolverRules", checksum = "cad95e96")]
#[tokio::test]
async fn list_resolver_rules() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    make_forward_rule(&server, &ep).await;
    let (status, body) = r53r(&server, "ListResolverRules", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverRules"].as_array().unwrap().is_empty());
}

#[test_action("route53resolver", "AssociateResolverRule", checksum = "7f11ed15")]
#[tokio::test]
async fn associate_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    let (status, body) = r53r(
        &server,
        "AssociateResolverRule",
        json!({ "ResolverRuleId": rule, "VPCId": v.vpc, "Name": "assoc" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["ResolverRuleAssociation"]["Id"]
        .as_str()
        .unwrap()
        .starts_with("rslvr-rrassoc-"));
}

#[test_action("route53resolver", "DisassociateResolverRule", checksum = "c535bbf3")]
#[tokio::test]
async fn disassociate_resolver_rule() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    r53r(
        &server,
        "AssociateResolverRule",
        json!({ "ResolverRuleId": rule, "VPCId": v.vpc }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "DisassociateResolverRule",
        json!({ "ResolverRuleId": rule, "VPCId": v.vpc }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRuleAssociation"]["Status"], "DELETING");
}

#[test_action("route53resolver", "GetResolverRuleAssociation", checksum = "728a2d4b")]
#[tokio::test]
async fn get_resolver_rule_association() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    let (_, a) = r53r(
        &server,
        "AssociateResolverRule",
        json!({ "ResolverRuleId": rule, "VPCId": v.vpc }),
    )
    .await;
    let assoc_id = a["ResolverRuleAssociation"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = r53r(
        &server,
        "GetResolverRuleAssociation",
        json!({ "ResolverRuleAssociationId": assoc_id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRuleAssociation"]["VPCId"], v.vpc);
}

#[test_action(
    "route53resolver",
    "ListResolverRuleAssociations",
    checksum = "204dd520"
)]
#[tokio::test]
async fn list_resolver_rule_associations() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    r53r(
        &server,
        "AssociateResolverRule",
        json!({ "ResolverRuleId": rule, "VPCId": v.vpc }),
    )
    .await;
    let (status, body) = r53r(&server, "ListResolverRuleAssociations", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverRuleAssociations"]
        .as_array()
        .unwrap()
        .is_empty());
}

// ─── Query-log configs + associations ────────────────────────────────────

#[test_action(
    "route53resolver",
    "CreateResolverQueryLogConfig",
    checksum = "4991477a"
)]
#[tokio::test]
async fn create_resolver_query_log_config() {
    let server = TestServer::start().await;
    let id = make_query_log_config(&server).await;
    assert!(id.starts_with("rslvr-qlc-"));
}

#[test_action("route53resolver", "GetResolverQueryLogConfig", checksum = "45f77ba4")]
#[tokio::test]
async fn get_resolver_query_log_config() {
    let server = TestServer::start().await;
    let id = make_query_log_config(&server).await;
    let (status, body) = r53r(
        &server,
        "GetResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverQueryLogConfig"]["Name"], "conf-qlc");
}

#[test_action(
    "route53resolver",
    "DeleteResolverQueryLogConfig",
    checksum = "f080d104"
)]
#[tokio::test]
async fn delete_resolver_query_log_config() {
    let server = TestServer::start().await;
    let id = make_query_log_config(&server).await;
    let (status, body) = r53r(
        &server,
        "DeleteResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverQueryLogConfig"]["Status"], "DELETING");
}

#[test_action(
    "route53resolver",
    "ListResolverQueryLogConfigs",
    checksum = "e0b60c85"
)]
#[tokio::test]
async fn list_resolver_query_log_configs() {
    let server = TestServer::start().await;
    make_query_log_config(&server).await;
    let (status, body) = r53r(&server, "ListResolverQueryLogConfigs", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverQueryLogConfigs"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test_action(
    "route53resolver",
    "AssociateResolverQueryLogConfig",
    checksum = "3d2f0d17"
)]
#[tokio::test]
async fn associate_resolver_query_log_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_query_log_config(&server).await;
    let (status, body) = r53r(
        &server,
        "AssociateResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id, "ResourceId": v.vpc }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["ResolverQueryLogConfigAssociation"]["Id"]
        .as_str()
        .unwrap()
        .starts_with("rslvr-qlcassoc-"));
}

#[test_action(
    "route53resolver",
    "DisassociateResolverQueryLogConfig",
    checksum = "ad10bdd5"
)]
#[tokio::test]
async fn disassociate_resolver_query_log_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_query_log_config(&server).await;
    r53r(
        &server,
        "AssociateResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id, "ResourceId": v.vpc }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "DisassociateResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id, "ResourceId": v.vpc }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["ResolverQueryLogConfigAssociation"]["Status"],
        "DELETING"
    );
}

#[test_action(
    "route53resolver",
    "GetResolverQueryLogConfigAssociation",
    checksum = "cc486aab"
)]
#[tokio::test]
async fn get_resolver_query_log_config_association() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_query_log_config(&server).await;
    let (_, a) = r53r(
        &server,
        "AssociateResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id, "ResourceId": v.vpc }),
    )
    .await;
    let assoc = a["ResolverQueryLogConfigAssociation"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = r53r(
        &server,
        "GetResolverQueryLogConfigAssociation",
        json!({ "ResolverQueryLogConfigAssociationId": assoc }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["ResolverQueryLogConfigAssociation"]["ResourceId"],
        v.vpc
    );
}

#[test_action(
    "route53resolver",
    "ListResolverQueryLogConfigAssociations",
    checksum = "727d3e75"
)]
#[tokio::test]
async fn list_resolver_query_log_config_associations() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let id = make_query_log_config(&server).await;
    r53r(
        &server,
        "AssociateResolverQueryLogConfig",
        json!({ "ResolverQueryLogConfigId": id, "ResourceId": v.vpc }),
    )
    .await;
    let (status, body) = r53r(&server, "ListResolverQueryLogConfigAssociations", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverQueryLogConfigAssociations"]
        .as_array()
        .unwrap()
        .is_empty());
}

// ─── Resolver config + DNSSEC config ─────────────────────────────────────

#[test_action("route53resolver", "GetResolverConfig", checksum = "96d5566a")]
#[tokio::test]
async fn get_resolver_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(&server, "GetResolverConfig", json!({ "ResourceId": v.vpc })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverConfig"]["ResourceId"], v.vpc);
}

#[test_action("route53resolver", "UpdateResolverConfig", checksum = "8ee8b2cf")]
#[tokio::test]
async fn update_resolver_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(
        &server,
        "UpdateResolverConfig",
        json!({ "ResourceId": v.vpc, "AutodefinedReverseFlag": "DISABLE" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverConfig"]["AutodefinedReverse"], "DISABLING");
}

#[test_action("route53resolver", "ListResolverConfigs", checksum = "5105ec1b")]
#[tokio::test]
async fn list_resolver_configs() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    r53r(
        &server,
        "UpdateResolverConfig",
        json!({ "ResourceId": v.vpc, "AutodefinedReverseFlag": "DISABLE" }),
    )
    .await;
    let (status, body) = r53r(&server, "ListResolverConfigs", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverConfigs"].as_array().unwrap().is_empty());
}

#[test_action("route53resolver", "GetResolverDnssecConfig", checksum = "124951e2")]
#[tokio::test]
async fn get_resolver_dnssec_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(
        &server,
        "GetResolverDnssecConfig",
        json!({ "ResourceId": v.vpc }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverDNSSECConfig"]["ResourceId"], v.vpc);
}

#[test_action("route53resolver", "UpdateResolverDnssecConfig", checksum = "266e3e3a")]
#[tokio::test]
async fn update_resolver_dnssec_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(
        &server,
        "UpdateResolverDnssecConfig",
        json!({ "ResourceId": v.vpc, "Validation": "ENABLE" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverDNSSECConfig"]["ValidationStatus"], "ENABLING");
}

#[test_action("route53resolver", "ListResolverDnssecConfigs", checksum = "d405c445")]
#[tokio::test]
async fn list_resolver_dnssec_configs() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    r53r(
        &server,
        "UpdateResolverDnssecConfig",
        json!({ "ResourceId": v.vpc, "Validation": "ENABLE" }),
    )
    .await;
    let (status, body) = r53r(&server, "ListResolverDnssecConfigs", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["ResolverDnssecConfigs"].as_array().unwrap().is_empty());
}

// ─── DNS Firewall: rule groups ───────────────────────────────────────────

#[test_action("route53resolver", "CreateFirewallRuleGroup", checksum = "60507228")]
#[tokio::test]
async fn create_firewall_rule_group() {
    let server = TestServer::start().await;
    let id = make_firewall_rule_group(&server).await;
    assert!(id.starts_with("rslvr-frg-"));
}

#[test_action("route53resolver", "GetFirewallRuleGroup", checksum = "3a1637e4")]
#[tokio::test]
async fn get_firewall_rule_group() {
    let server = TestServer::start().await;
    let id = make_firewall_rule_group(&server).await;
    let (status, body) = r53r(
        &server,
        "GetFirewallRuleGroup",
        json!({ "FirewallRuleGroupId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroup"]["Name"], "conf-frg");
}

#[test_action("route53resolver", "DeleteFirewallRuleGroup", checksum = "040f5ef1")]
#[tokio::test]
async fn delete_firewall_rule_group() {
    let server = TestServer::start().await;
    let id = make_firewall_rule_group(&server).await;
    let (status, body) = r53r(
        &server,
        "DeleteFirewallRuleGroup",
        json!({ "FirewallRuleGroupId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroup"]["Status"], "DELETING");
}

#[test_action("route53resolver", "ListFirewallRuleGroups", checksum = "6bb20d32")]
#[tokio::test]
async fn list_firewall_rule_groups() {
    let server = TestServer::start().await;
    make_firewall_rule_group(&server).await;
    let (status, body) = r53r(&server, "ListFirewallRuleGroups", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["FirewallRuleGroups"].as_array().unwrap().is_empty());
}

// ─── DNS Firewall: domain lists ──────────────────────────────────────────

#[test_action("route53resolver", "CreateFirewallDomainList", checksum = "ab487838")]
#[tokio::test]
async fn create_firewall_domain_list() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    assert!(id.starts_with("rslvr-fdl-"));
}

#[test_action("route53resolver", "GetFirewallDomainList", checksum = "5c9d4af4")]
#[tokio::test]
async fn get_firewall_domain_list() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    let (status, body) = r53r(
        &server,
        "GetFirewallDomainList",
        json!({ "FirewallDomainListId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallDomainList"]["Name"], "conf-fdl");
}

#[test_action("route53resolver", "DeleteFirewallDomainList", checksum = "98609fff")]
#[tokio::test]
async fn delete_firewall_domain_list() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    let (status, body) = r53r(
        &server,
        "DeleteFirewallDomainList",
        json!({ "FirewallDomainListId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallDomainList"]["Status"], "DELETING");
}

#[test_action("route53resolver", "ListFirewallDomainLists", checksum = "f9046475")]
#[tokio::test]
async fn list_firewall_domain_lists() {
    let server = TestServer::start().await;
    make_firewall_domain_list(&server).await;
    let (status, body) = r53r(&server, "ListFirewallDomainLists", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["FirewallDomainLists"].as_array().unwrap().is_empty());
}

#[test_action("route53resolver", "ImportFirewallDomains", checksum = "38f5d2a6")]
#[tokio::test]
async fn import_firewall_domains() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    let (status, body) = r53r(
        &server,
        "ImportFirewallDomains",
        json!({ "FirewallDomainListId": id, "Operation": "REPLACE", "DomainFileUrl": "s3://bucket/domains.txt" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Status"], "COMPLETE");
}

#[test_action("route53resolver", "UpdateFirewallDomains", checksum = "673b9aee")]
#[tokio::test]
async fn update_firewall_domains() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    let (status, body) = r53r(
        &server,
        "UpdateFirewallDomains",
        json!({ "FirewallDomainListId": id, "Operation": "ADD", "Domains": ["evil.example.com", "bad.example.net"] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Status"], "COMPLETE");
}

#[test_action("route53resolver", "ListFirewallDomains", checksum = "bcc58e1b")]
#[tokio::test]
async fn list_firewall_domains() {
    let server = TestServer::start().await;
    let id = make_firewall_domain_list(&server).await;
    r53r(
        &server,
        "UpdateFirewallDomains",
        json!({ "FirewallDomainListId": id, "Operation": "ADD", "Domains": ["evil.example.com"] }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "ListFirewallDomains",
        json!({ "FirewallDomainListId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Domains"].as_array().unwrap().len(), 1);
}

// ─── DNS Firewall: rules ─────────────────────────────────────────────────

async fn make_firewall_rule(server: &TestServer, group: &str, list: &str) -> Value {
    let (status, body) = r53r(
        server,
        "CreateFirewallRule",
        json!({
            "CreatorRequestId": "conf-fr-1",
            "FirewallRuleGroupId": group,
            "FirewallDomainListId": list,
            "Priority": 100,
            "Action": "BLOCK",
            "BlockResponse": "NXDOMAIN",
            "Name": "conf-fr",
        }),
    )
    .await;
    assert_eq!(status, 200, "create firewall rule: {body}");
    body
}

#[test_action("route53resolver", "CreateFirewallRule", checksum = "21347386")]
#[tokio::test]
async fn create_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    let body = make_firewall_rule(&server, &group, &list).await;
    assert_eq!(body["FirewallRule"]["Action"], "BLOCK");
}

#[test_action("route53resolver", "UpdateFirewallRule", checksum = "1223034a")]
#[tokio::test]
async fn update_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    make_firewall_rule(&server, &group, &list).await;
    let (status, body) = r53r(
        &server,
        "UpdateFirewallRule",
        json!({ "FirewallRuleGroupId": group, "FirewallDomainListId": list, "Action": "ALERT" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRule"]["Action"], "ALERT");
}

#[test_action("route53resolver", "DeleteFirewallRule", checksum = "2b17fec0")]
#[tokio::test]
async fn delete_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    make_firewall_rule(&server, &group, &list).await;
    let (status, body) = r53r(
        &server,
        "DeleteFirewallRule",
        json!({ "FirewallRuleGroupId": group, "FirewallDomainListId": list }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRule"]["FirewallDomainListId"], list);
}

#[test_action("route53resolver", "ListFirewallRules", checksum = "2a4f7dcb")]
#[tokio::test]
async fn list_firewall_rules() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    make_firewall_rule(&server, &group, &list).await;
    let (status, body) = r53r(
        &server,
        "ListFirewallRules",
        json!({ "FirewallRuleGroupId": group }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRules"].as_array().unwrap().len(), 1);
}

#[test_action("route53resolver", "ListFirewallRuleTypes", checksum = "2db895f7")]
#[tokio::test]
async fn list_firewall_rule_types() {
    let server = TestServer::start().await;
    let (status, body) = r53r(&server, "ListFirewallRuleTypes", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["FirewallRuleTypes"].as_array().unwrap().is_empty());
}

#[test_action("route53resolver", "BatchCreateFirewallRule", checksum = "14195b83")]
#[tokio::test]
async fn batch_create_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    let (status, body) = r53r(
        &server,
        "BatchCreateFirewallRule",
        json!({
            "CreateFirewallRuleEntries": [
                { "CreatorRequestId": "b1", "FirewallRuleGroupId": group, "FirewallDomainListId": list, "Priority": 200, "Action": "ALLOW", "Name": "b1" }
            ]
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["CreatedFirewallRules"].as_array().unwrap().len(), 1);
}

#[test_action("route53resolver", "BatchUpdateFirewallRule", checksum = "b2e31d7c")]
#[tokio::test]
async fn batch_update_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    make_firewall_rule(&server, &group, &list).await;
    let (status, body) = r53r(
        &server,
        "BatchUpdateFirewallRule",
        json!({
            "UpdateFirewallRuleEntries": [
                { "FirewallRuleGroupId": group, "FirewallDomainListId": list, "Action": "ALERT" }
            ]
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["UpdatedFirewallRules"][0]["Action"], "ALERT");
}

#[test_action("route53resolver", "BatchDeleteFirewallRule", checksum = "264197e5")]
#[tokio::test]
async fn batch_delete_firewall_rule() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let list = make_firewall_domain_list(&server).await;
    make_firewall_rule(&server, &group, &list).await;
    let (status, body) = r53r(
        &server,
        "BatchDeleteFirewallRule",
        json!({
            "DeleteFirewallRuleEntries": [
                { "FirewallRuleGroupId": group, "FirewallDomainListId": list }
            ]
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["DeletedFirewallRules"].as_array().unwrap().len(), 1);
}

// ─── DNS Firewall: rule-group associations + config ──────────────────────

async fn make_firewall_assoc(server: &TestServer, group: &str, vpc: &str) -> String {
    let (status, body) = r53r(
        server,
        "AssociateFirewallRuleGroup",
        json!({
            "CreatorRequestId": "conf-fga-1",
            "FirewallRuleGroupId": group,
            "VpcId": vpc,
            "Priority": 101,
            "Name": "conf-fga",
        }),
    )
    .await;
    assert_eq!(status, 200, "associate firewall rule group: {body}");
    body["FirewallRuleGroupAssociation"]["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test_action("route53resolver", "AssociateFirewallRuleGroup", checksum = "23f11e8d")]
#[tokio::test]
async fn associate_firewall_rule_group() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let group = make_firewall_rule_group(&server).await;
    let id = make_firewall_assoc(&server, &group, &v.vpc).await;
    assert!(id.starts_with("rslvr-frgassoc-"));
}

#[test_action(
    "route53resolver",
    "DisassociateFirewallRuleGroup",
    checksum = "3621c0de"
)]
#[tokio::test]
async fn disassociate_firewall_rule_group() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let group = make_firewall_rule_group(&server).await;
    let id = make_firewall_assoc(&server, &group, &v.vpc).await;
    let (status, body) = r53r(
        &server,
        "DisassociateFirewallRuleGroup",
        json!({ "FirewallRuleGroupAssociationId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroupAssociation"]["Status"], "DELETING");
}

#[test_action(
    "route53resolver",
    "GetFirewallRuleGroupAssociation",
    checksum = "e2a06b4a"
)]
#[tokio::test]
async fn get_firewall_rule_group_association() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let group = make_firewall_rule_group(&server).await;
    let id = make_firewall_assoc(&server, &group, &v.vpc).await;
    let (status, body) = r53r(
        &server,
        "GetFirewallRuleGroupAssociation",
        json!({ "FirewallRuleGroupAssociationId": id }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroupAssociation"]["VpcId"], v.vpc);
}

#[test_action(
    "route53resolver",
    "UpdateFirewallRuleGroupAssociation",
    checksum = "01cfd0d0"
)]
#[tokio::test]
async fn update_firewall_rule_group_association() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let group = make_firewall_rule_group(&server).await;
    let id = make_firewall_assoc(&server, &group, &v.vpc).await;
    let (status, body) = r53r(
        &server,
        "UpdateFirewallRuleGroupAssociation",
        json!({ "FirewallRuleGroupAssociationId": id, "Name": "renamed", "Priority": 150 }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroupAssociation"]["Name"], "renamed");
}

#[test_action(
    "route53resolver",
    "ListFirewallRuleGroupAssociations",
    checksum = "cadc37ce"
)]
#[tokio::test]
async fn list_firewall_rule_group_associations() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let group = make_firewall_rule_group(&server).await;
    make_firewall_assoc(&server, &group, &v.vpc).await;
    let (status, body) = r53r(&server, "ListFirewallRuleGroupAssociations", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["FirewallRuleGroupAssociations"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test_action("route53resolver", "GetFirewallConfig", checksum = "9d744418")]
#[tokio::test]
async fn get_firewall_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(&server, "GetFirewallConfig", json!({ "ResourceId": v.vpc })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallConfig"]["ResourceId"], v.vpc);
}

#[test_action("route53resolver", "UpdateFirewallConfig", checksum = "751e9c33")]
#[tokio::test]
async fn update_firewall_config() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let (status, body) = r53r(
        &server,
        "UpdateFirewallConfig",
        json!({ "ResourceId": v.vpc, "FirewallFailOpen": "ENABLED" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallConfig"]["FirewallFailOpen"], "ENABLED");
}

#[test_action("route53resolver", "ListFirewallConfigs", checksum = "336d08a6")]
#[tokio::test]
async fn list_firewall_configs() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    r53r(
        &server,
        "UpdateFirewallConfig",
        json!({ "ResourceId": v.vpc, "FirewallFailOpen": "ENABLED" }),
    )
    .await;
    let (status, body) = r53r(&server, "ListFirewallConfigs", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["FirewallConfigs"].as_array().unwrap().is_empty());
}

// ─── Outpost resolvers ───────────────────────────────────────────────────

async fn make_outpost_resolver(server: &TestServer) -> String {
    let (status, body) = r53r(
        server,
        "CreateOutpostResolver",
        json!({
            "CreatorRequestId": "conf-op-1",
            "Name": "conf-op",
            "PreferredInstanceType": "m5.large",
            "OutpostArn": "arn:aws:outposts:us-east-1:000000000000:outpost/op-1234567890abcdef0",
        }),
    )
    .await;
    assert_eq!(status, 200, "create outpost resolver: {body}");
    body["OutpostResolver"]["Id"].as_str().unwrap().to_string()
}

#[test_action("route53resolver", "CreateOutpostResolver", checksum = "3b313eda")]
#[tokio::test]
async fn create_outpost_resolver() {
    let server = TestServer::start().await;
    let id = make_outpost_resolver(&server).await;
    assert!(id.starts_with("rslvr-op-"));
}

#[test_action("route53resolver", "GetOutpostResolver", checksum = "5a0d54a8")]
#[tokio::test]
async fn get_outpost_resolver() {
    let server = TestServer::start().await;
    let id = make_outpost_resolver(&server).await;
    let (status, body) = r53r(&server, "GetOutpostResolver", json!({ "Id": id })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["OutpostResolver"]["Name"], "conf-op");
}

#[test_action("route53resolver", "UpdateOutpostResolver", checksum = "721009c8")]
#[tokio::test]
async fn update_outpost_resolver() {
    let server = TestServer::start().await;
    let id = make_outpost_resolver(&server).await;
    let (status, body) = r53r(
        &server,
        "UpdateOutpostResolver",
        json!({ "Id": id, "Name": "renamed-op" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["OutpostResolver"]["Name"], "renamed-op");
}

#[test_action("route53resolver", "DeleteOutpostResolver", checksum = "6780f654")]
#[tokio::test]
async fn delete_outpost_resolver() {
    let server = TestServer::start().await;
    let id = make_outpost_resolver(&server).await;
    let (status, body) = r53r(&server, "DeleteOutpostResolver", json!({ "Id": id })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["OutpostResolver"]["Status"], "DELETING");
}

#[test_action("route53resolver", "ListOutpostResolvers", checksum = "fa94e7f9")]
#[tokio::test]
async fn list_outpost_resolvers() {
    let server = TestServer::start().await;
    make_outpost_resolver(&server).await;
    let (status, body) = r53r(&server, "ListOutpostResolvers", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body["OutpostResolvers"].as_array().unwrap().is_empty());
}

// ─── Resource policies ───────────────────────────────────────────────────

#[test_action("route53resolver", "PutFirewallRuleGroupPolicy", checksum = "3a1d16ca")]
#[tokio::test]
async fn put_firewall_rule_group_policy() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/{group}");
    let (status, body) = r53r(
        &server,
        "PutFirewallRuleGroupPolicy",
        json!({ "Arn": arn, "FirewallRuleGroupPolicy": "{\"Version\":\"2012-10-17\"}" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ReturnValue"], true);
}

#[test_action("route53resolver", "GetFirewallRuleGroupPolicy", checksum = "2a14df36")]
#[tokio::test]
async fn get_firewall_rule_group_policy() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/{group}");
    r53r(
        &server,
        "PutFirewallRuleGroupPolicy",
        json!({ "Arn": arn, "FirewallRuleGroupPolicy": "{\"a\":1}" }),
    )
    .await;
    let (status, body) = r53r(&server, "GetFirewallRuleGroupPolicy", json!({ "Arn": arn })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["FirewallRuleGroupPolicy"], "{\"a\":1}");
}

#[test_action(
    "route53resolver",
    "PutResolverQueryLogConfigPolicy",
    checksum = "ea663065"
)]
#[tokio::test]
async fn put_resolver_query_log_config_policy() {
    let server = TestServer::start().await;
    let id = make_query_log_config(&server).await;
    let arn =
        format!("arn:aws:route53resolver:us-east-1:000000000000:resolver-query-log-config/{id}");
    let (status, body) = r53r(
        &server,
        "PutResolverQueryLogConfigPolicy",
        json!({ "Arn": arn, "ResolverQueryLogConfigPolicy": "{\"a\":1}" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ReturnValue"], true);
}

#[test_action(
    "route53resolver",
    "GetResolverQueryLogConfigPolicy",
    checksum = "84921900"
)]
#[tokio::test]
async fn get_resolver_query_log_config_policy() {
    let server = TestServer::start().await;
    let id = make_query_log_config(&server).await;
    let arn =
        format!("arn:aws:route53resolver:us-east-1:000000000000:resolver-query-log-config/{id}");
    r53r(
        &server,
        "PutResolverQueryLogConfigPolicy",
        json!({ "Arn": arn, "ResolverQueryLogConfigPolicy": "{\"a\":1}" }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "GetResolverQueryLogConfigPolicy",
        json!({ "Arn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverQueryLogConfigPolicy"], "{\"a\":1}");
}

#[test_action("route53resolver", "PutResolverRulePolicy", checksum = "c36c9b32")]
#[tokio::test]
async fn put_resolver_rule_policy() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:resolver-rule/{rule}");
    let (status, body) = r53r(
        &server,
        "PutResolverRulePolicy",
        json!({ "Arn": arn, "ResolverRulePolicy": "{\"a\":1}" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ReturnValue"], true);
}

#[test_action("route53resolver", "GetResolverRulePolicy", checksum = "0e7260cc")]
#[tokio::test]
async fn get_resolver_rule_policy() {
    let server = TestServer::start().await;
    let v = setup_vpc(&server).await;
    let ep = make_outbound_endpoint(&server, &v).await;
    let rule = make_forward_rule(&server, &ep).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:resolver-rule/{rule}");
    r53r(
        &server,
        "PutResolverRulePolicy",
        json!({ "Arn": arn, "ResolverRulePolicy": "{\"a\":1}" }),
    )
    .await;
    let (status, body) = r53r(&server, "GetResolverRulePolicy", json!({ "Arn": arn })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ResolverRulePolicy"], "{\"a\":1}");
}

// ─── Tags ────────────────────────────────────────────────────────────────

#[test_action("route53resolver", "TagResource", checksum = "1d379055")]
#[tokio::test]
async fn tag_resource() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/{group}");
    let (status, body) = r53r(
        &server,
        "TagResource",
        json!({ "ResourceArn": arn, "Tags": [ { "Key": "env", "Value": "conf" } ] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[test_action("route53resolver", "ListTagsForResource", checksum = "fa6bb755")]
#[tokio::test]
async fn list_tags_for_resource() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/{group}");
    r53r(
        &server,
        "TagResource",
        json!({ "ResourceArn": arn, "Tags": [ { "Key": "env", "Value": "conf" } ] }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "ListTagsForResource",
        json!({ "ResourceArn": arn }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Tags"][0]["Key"], "env");
}

#[test_action("route53resolver", "UntagResource", checksum = "c675b2bd")]
#[tokio::test]
async fn untag_resource() {
    let server = TestServer::start().await;
    let group = make_firewall_rule_group(&server).await;
    let arn = format!("arn:aws:route53resolver:us-east-1:000000000000:firewall-rule-group/{group}");
    r53r(
        &server,
        "TagResource",
        json!({ "ResourceArn": arn, "Tags": [ { "Key": "env", "Value": "conf" } ] }),
    )
    .await;
    let (status, body) = r53r(
        &server,
        "UntagResource",
        json!({ "ResourceArn": arn, "TagKeys": ["env"] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (_, listed) = r53r(
        &server,
        "ListTagsForResource",
        json!({ "ResourceArn": arn }),
    )
    .await;
    assert!(listed["Tags"].as_array().unwrap().is_empty());
}
