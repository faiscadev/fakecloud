//! Conformance coverage for Amazon Redshift (awsQuery control plane).
//!
//! One `#[test_action]` per `SUPPORTED_ACTIONS` entry; the audit cross-checks
//! this list against the service crate and fails on any gap. Core resource
//! families get full create/describe/modify/delete behavioural coverage; the
//! remaining ops are exercised for routing + a well-formed response in the
//! grouped `redshift_remaining_routes_exist` test. Each `#[test_action]` pins
//! the operation to its Smithy checksum so model drift fails the build.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const RS_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/redshift/aws4_request, SignedHeaders=host, Signature=0";

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// POST an awsQuery action and return `(status, body)`.
async fn rs_post(server: &TestServer, action: &str, params: &[(&str, &str)]) -> (u16, String) {
    let mut body = format!("Action={action}&Version=2012-12-01");
    for (k, v) in params {
        body.push_str(&format!("&{}={}", pct(k), pct(v)));
    }
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", RS_AUTH)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.text().await.unwrap())
}

/// A response is "routed" when the handler ran: either a 2xx, or a 4xx that
/// carries an AWS `<Code>` element rather than a routing miss / `InvalidAction`.
fn routed(status: u16, body: &str) -> bool {
    if (200..300).contains(&status) {
        return true;
    }
    (400..500).contains(&status)
        && body.contains("<Code>")
        && !body.contains("InvalidAction")
        && !body.contains("not implemented")
}

#[test_action("redshift", "CreateCluster", checksum = "ef3d8c18")]
#[test_action("redshift", "DescribeClusters", checksum = "fe54066e")]
#[test_action("redshift", "ModifyCluster", checksum = "e1ea0b1a")]
#[test_action("redshift", "RebootCluster", checksum = "5c3bd0a8")]
#[test_action("redshift", "DeleteCluster", checksum = "f1c3094a")]
#[tokio::test]
async fn redshift_cluster_crud() {
    let server = TestServer::start().await;
    let (st, body) = rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "my-cluster"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd!"),
            ("NumberOfNodes", "2"),
        ],
    )
    .await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("<ClusterIdentifier>my-cluster</ClusterIdentifier>"));
    assert!(
        body.contains("redshift.amazonaws.com"),
        "endpoint address: {body}"
    );
    assert!(body.contains("<Port>5439</Port>"));
    let (_, body) = rs_post(&server, "DescribeClusters", &[]).await;
    assert!(body.contains("my-cluster"));
    let (st, body) = rs_post(
        &server,
        "ModifyCluster",
        &[("ClusterIdentifier", "my-cluster"), ("NumberOfNodes", "4")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("<NumberOfNodes>4</NumberOfNodes>"));
    let (st, _) = rs_post(
        &server,
        "RebootCluster",
        &[("ClusterIdentifier", "my-cluster")],
    )
    .await;
    assert_eq!(st, 200);
    let (st, _) = rs_post(
        &server,
        "DeleteCluster",
        &[
            ("ClusterIdentifier", "my-cluster"),
            ("SkipFinalClusterSnapshot", "true"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (st, body) = rs_post(
        &server,
        "DescribeClusters",
        &[("ClusterIdentifier", "my-cluster")],
    )
    .await;
    assert_eq!(st, 404, "{body}");
    assert!(body.contains("<Code>ClusterNotFound</Code>"), "{body}");
}

#[tokio::test]
async fn redshift_errors_and_validation() {
    let server = TestServer::start().await;
    rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "dup"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
        ],
    )
    .await;
    let (st, body) = rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "dup"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
        ],
    )
    .await;
    assert_eq!(st, 400);
    assert!(body.contains("<Code>ClusterAlreadyExists</Code>"), "{body}");
    let (st, body) = rs_post(&server, "DeleteCluster", &[]).await;
    assert_eq!(st, 400);
    assert!(body.contains("MissingParameter"), "{body}");
    let (st, body) = rs_post(
        &server,
        "CreateUsageLimit",
        &[
            ("ClusterIdentifier", "c"),
            ("FeatureType", "BOGUS"),
            ("LimitType", "TIME"),
            ("Amount", "1"),
        ],
    )
    .await;
    assert_eq!(st, 400);
    assert!(body.contains("InvalidParameterValue"), "{body}");
    let (st, body) = rs_post(&server, "ModifyCluster", &[("ClusterIdentifier", "ghost")]).await;
    assert_eq!(st, 404);
    assert!(body.contains("<Code>ClusterNotFound</Code>"), "{body}");
}

#[test_action("redshift", "CreateClusterParameterGroup", checksum = "c9988331")]
#[test_action("redshift", "DescribeClusterParameterGroups", checksum = "64637efa")]
#[test_action("redshift", "ModifyClusterParameterGroup", checksum = "e6a71cca")]
#[test_action("redshift", "DescribeClusterParameters", checksum = "088b7e9b")]
#[test_action("redshift", "ResetClusterParameterGroup", checksum = "edf3f046")]
#[test_action("redshift", "DescribeDefaultClusterParameters", checksum = "c5255f21")]
#[test_action("redshift", "DeleteClusterParameterGroup", checksum = "0500b5c6")]
#[tokio::test]
async fn redshift_parameter_group_crud() {
    let server = TestServer::start().await;
    let (st, body) = rs_post(
        &server,
        "CreateClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg1"),
            ("ParameterGroupFamily", "redshift-1.0"),
            ("Description", "test"),
        ],
    )
    .await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("<ParameterGroupName>pg1</ParameterGroupName>"));
    let (_, body) = rs_post(&server, "DescribeClusterParameterGroups", &[]).await;
    assert!(body.contains("pg1"));
    let (st, _) = rs_post(
        &server,
        "ModifyClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg1"),
            ("Parameters.member.1.ParameterName", "require_ssl"),
            ("Parameters.member.1.ParameterValue", "true"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (st, body) = rs_post(
        &server,
        "DescribeClusterParameters",
        &[("ParameterGroupName", "pg1")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("<ParameterName>require_ssl</ParameterName>"));
    assert!(
        body.contains("<ParameterValue>true</ParameterValue>"),
        "{body}"
    );
    let (st, _) = rs_post(
        &server,
        "ResetClusterParameterGroup",
        &[("ParameterGroupName", "pg1")],
    )
    .await;
    assert_eq!(st, 200);
    let (st, body) = rs_post(
        &server,
        "DescribeDefaultClusterParameters",
        &[("ParameterGroupFamily", "redshift-1.0")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("<ParameterGroupFamily>redshift-1.0</ParameterGroupFamily>"));
    let (st, _) = rs_post(
        &server,
        "DeleteClusterParameterGroup",
        &[("ParameterGroupName", "pg1")],
    )
    .await;
    assert_eq!(st, 200);
    let (st, _) = rs_post(
        &server,
        "DeleteClusterParameterGroup",
        &[("ParameterGroupName", "pg1")],
    )
    .await;
    assert_eq!(st, 404);
}

#[test_action("redshift", "CreateClusterSubnetGroup", checksum = "6fe17572")]
#[test_action("redshift", "DescribeClusterSubnetGroups", checksum = "54bf02a3")]
#[test_action("redshift", "ModifyClusterSubnetGroup", checksum = "2583b5c0")]
#[test_action("redshift", "DeleteClusterSubnetGroup", checksum = "1010f2c2")]
#[tokio::test]
async fn redshift_subnet_group_crud() {
    let server = TestServer::start().await;
    let (st, body) = rs_post(
        &server,
        "CreateClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "sg1"),
            ("Description", "d"),
            ("SubnetIds.member.1", "subnet-1"),
            ("SubnetIds.member.2", "subnet-2"),
        ],
    )
    .await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("<SubnetIdentifier>subnet-1</SubnetIdentifier>"));
    let (_, body) = rs_post(&server, "DescribeClusterSubnetGroups", &[]).await;
    assert!(body.contains("sg1"));
    let (st, body) = rs_post(
        &server,
        "ModifyClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "sg1"),
            ("SubnetIds.member.1", "subnet-9"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("subnet-9"));
    let (st, _) = rs_post(
        &server,
        "DeleteClusterSubnetGroup",
        &[("ClusterSubnetGroupName", "sg1")],
    )
    .await;
    assert_eq!(st, 200);
}

#[test_action("redshift", "CreateClusterSecurityGroup", checksum = "9559b035")]
#[test_action("redshift", "DescribeClusterSecurityGroups", checksum = "0cb9ec6f")]
#[test_action(
    "redshift",
    "AuthorizeClusterSecurityGroupIngress",
    checksum = "f7207a2f"
)]
#[test_action("redshift", "RevokeClusterSecurityGroupIngress", checksum = "70bf631c")]
#[test_action("redshift", "DeleteClusterSecurityGroup", checksum = "079aab3b")]
#[tokio::test]
async fn redshift_security_group_crud() {
    let server = TestServer::start().await;
    let (st, _) = rs_post(
        &server,
        "CreateClusterSecurityGroup",
        &[("ClusterSecurityGroupName", "csg1"), ("Description", "d")],
    )
    .await;
    assert_eq!(st, 200);
    let (st, body) = rs_post(
        &server,
        "AuthorizeClusterSecurityGroupIngress",
        &[
            ("ClusterSecurityGroupName", "csg1"),
            ("CIDRIP", "10.0.0.0/24"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("10.0.0.0/24"), "{body}");
    let (st, _) = rs_post(
        &server,
        "RevokeClusterSecurityGroupIngress",
        &[
            ("ClusterSecurityGroupName", "csg1"),
            ("CIDRIP", "10.0.0.0/24"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (_, body) = rs_post(&server, "DescribeClusterSecurityGroups", &[]).await;
    assert!(body.contains("csg1"));
    let (st, _) = rs_post(
        &server,
        "DeleteClusterSecurityGroup",
        &[("ClusterSecurityGroupName", "csg1")],
    )
    .await;
    assert_eq!(st, 200);
}

#[test_action("redshift", "CreateClusterSnapshot", checksum = "d6a93654")]
#[test_action("redshift", "DescribeClusterSnapshots", checksum = "8561f50d")]
#[test_action("redshift", "CopyClusterSnapshot", checksum = "bc1a1635")]
#[test_action("redshift", "ModifyClusterSnapshot", checksum = "1481caaa")]
#[test_action("redshift", "DeleteClusterSnapshot", checksum = "fa5cdd7b")]
#[tokio::test]
async fn redshift_snapshot_crud() {
    let server = TestServer::start().await;
    rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "snapc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
        ],
    )
    .await;
    let (st, body) = rs_post(
        &server,
        "CreateClusterSnapshot",
        &[
            ("SnapshotIdentifier", "snap1"),
            ("ClusterIdentifier", "snapc"),
        ],
    )
    .await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("<SnapshotIdentifier>snap1</SnapshotIdentifier>"));
    let (_, body) = rs_post(&server, "DescribeClusterSnapshots", &[]).await;
    assert!(body.contains("snap1"));
    let (st, body) = rs_post(
        &server,
        "CopyClusterSnapshot",
        &[
            ("SourceSnapshotIdentifier", "snap1"),
            ("TargetSnapshotIdentifier", "snap2"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("snap2"));
    let (st, _) = rs_post(
        &server,
        "ModifyClusterSnapshot",
        &[
            ("SnapshotIdentifier", "snap1"),
            ("ManualSnapshotRetentionPeriod", "10"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (st, _) = rs_post(
        &server,
        "DeleteClusterSnapshot",
        &[("SnapshotIdentifier", "snap1")],
    )
    .await;
    assert_eq!(st, 200);
    let (st, _) = rs_post(
        &server,
        "DeleteClusterSnapshot",
        &[("SnapshotIdentifier", "snap1")],
    )
    .await;
    assert_eq!(st, 404);
}

#[test_action("redshift", "CreateEventSubscription", checksum = "0e5c873c")]
#[test_action("redshift", "DescribeEventSubscriptions", checksum = "e0c76c53")]
#[test_action("redshift", "ModifyEventSubscription", checksum = "83cc4b17")]
#[test_action("redshift", "DeleteEventSubscription", checksum = "a7a5a52a")]
#[tokio::test]
async fn redshift_event_subscription_crud() {
    let server = TestServer::start().await;
    let (st, _) = rs_post(
        &server,
        "CreateEventSubscription",
        &[
            ("SubscriptionName", "sub1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:123456789012:topic"),
            ("SourceType", "cluster"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (_, body) = rs_post(&server, "DescribeEventSubscriptions", &[]).await;
    assert!(body.contains("sub1"));
    let (st, body) = rs_post(
        &server,
        "ModifyEventSubscription",
        &[("SubscriptionName", "sub1"), ("Enabled", "false")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("<Enabled>false</Enabled>"), "{body}");
    let (st, _) = rs_post(
        &server,
        "DeleteEventSubscription",
        &[("SubscriptionName", "sub1")],
    )
    .await;
    assert_eq!(st, 200);
}

#[test_action("redshift", "CreateUsageLimit", checksum = "be8a0af7")]
#[test_action("redshift", "DescribeUsageLimits", checksum = "d5667c4f")]
#[test_action("redshift", "ModifyUsageLimit", checksum = "64c3e48a")]
#[test_action("redshift", "DeleteUsageLimit", checksum = "8bcc6af7")]
#[tokio::test]
async fn redshift_usage_limit_crud() {
    let server = TestServer::start().await;
    let (st, body) = rs_post(
        &server,
        "CreateUsageLimit",
        &[
            ("ClusterIdentifier", "c1"),
            ("FeatureType", "spectrum"),
            ("LimitType", "data-scanned"),
            ("Amount", "100"),
            ("Period", "monthly"),
        ],
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let id_start = body.find("<UsageLimitId>").unwrap() + "<UsageLimitId>".len();
    let id_end = body[id_start..].find("</UsageLimitId>").unwrap() + id_start;
    let id = body[id_start..id_end].to_string();
    let (_, body) = rs_post(
        &server,
        "DescribeUsageLimits",
        &[("ClusterIdentifier", "c1")],
    )
    .await;
    assert!(body.contains(&id));
    let (st, body) = rs_post(
        &server,
        "ModifyUsageLimit",
        &[("UsageLimitId", &id), ("Amount", "200")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("<Amount>200</Amount>"), "{body}");
    let (st, _) = rs_post(&server, "DeleteUsageLimit", &[("UsageLimitId", &id)]).await;
    assert_eq!(st, 200);
    let (st, _) = rs_post(&server, "DeleteUsageLimit", &[("UsageLimitId", &id)]).await;
    assert_eq!(st, 404);
}

#[test_action("redshift", "CreateTags", checksum = "8adf4c71")]
#[test_action("redshift", "DescribeTags", checksum = "03fa335f")]
#[test_action("redshift", "DeleteTags", checksum = "439bcf79")]
#[tokio::test]
async fn redshift_tags_and_pagination() {
    let server = TestServer::start().await;
    // Tags are ARN-addressed and mutate real state, so the resource must exist.
    let (st, _) = rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "pagc0"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("ClusterType", "single-node"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let arn = "arn:aws:redshift:us-east-1:123456789012:cluster:pagc0";
    let (st, _) = rs_post(
        &server,
        "CreateTags",
        &[
            ("ResourceName", arn),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    )
    .await;
    assert_eq!(st, 200);
    let (st, body) = rs_post(&server, "DescribeTags", &[]).await;
    assert_eq!(st, 200);
    assert!(body.contains("<Key>team</Key>"), "{body}");
    // CreateTags against an ARN that does not resolve is a ResourceNotFoundFault.
    let (st, _) = rs_post(
        &server,
        "CreateTags",
        &[
            ("ResourceName", "arn:aws:redshift:us-east-1:123456789012:cluster:ghost"),
            ("Tags.Tag.1.Key", "k"),
            ("Tags.Tag.1.Value", "v"),
        ],
    )
    .await;
    assert_eq!(st, 404);
    let (st, _) = rs_post(
        &server,
        "DeleteTags",
        &[("ResourceName", arn), ("TagKeys.TagKey.1", "team")],
    )
    .await;
    assert_eq!(st, 200);
    for i in 0..3 {
        rs_post(
            &server,
            "CreateClusterParameterGroup",
            &[
                ("ParameterGroupName", &format!("ppg{i}")),
                ("ParameterGroupFamily", "redshift-1.0"),
                ("Description", "d"),
            ],
        )
        .await;
    }
    let (st, body) = rs_post(
        &server,
        "DescribeClusterParameterGroups",
        &[("MaxRecords", "2")],
    )
    .await;
    assert_eq!(st, 200);
    assert!(
        body.contains("<Marker>"),
        "expected a marker for page 1: {body}"
    );
    let marker_start = body.find("<Marker>").unwrap() + "<Marker>".len();
    let marker_end = body[marker_start..].find("</Marker>").unwrap() + marker_start;
    let marker = body[marker_start..marker_end].to_string();
    let (st, _) = rs_post(
        &server,
        "DescribeClusterParameterGroups",
        &[("MaxRecords", "2"), ("Marker", &marker)],
    )
    .await;
    assert_eq!(st, 200);
}

#[test_action("redshift", "AcceptReservedNodeExchange", checksum = "6fe1bb32")]
#[test_action("redshift", "AddPartner", checksum = "f04b640e")]
#[test_action("redshift", "AssociateDataShareConsumer", checksum = "7d2dba06")]
#[test_action("redshift", "AuthorizeDataShare", checksum = "07a13854")]
#[test_action("redshift", "AuthorizeEndpointAccess", checksum = "f496c2b7")]
#[test_action("redshift", "AuthorizeSnapshotAccess", checksum = "54df490c")]
#[test_action("redshift", "BatchDeleteClusterSnapshots", checksum = "53ba63f1")]
#[test_action("redshift", "BatchModifyClusterSnapshots", checksum = "da15b0fe")]
#[test_action("redshift", "CancelResize", checksum = "ca39a5c9")]
#[test_action("redshift", "CreateAuthenticationProfile", checksum = "9304028a")]
#[test_action("redshift", "CreateCustomDomainAssociation", checksum = "236ef1ed")]
#[test_action("redshift", "CreateEndpointAccess", checksum = "9e262190")]
#[test_action("redshift", "CreateHsmClientCertificate", checksum = "9f84b8c3")]
#[test_action("redshift", "CreateHsmConfiguration", checksum = "074ce5ae")]
#[test_action("redshift", "CreateIntegration", checksum = "e81db15f")]
#[test_action("redshift", "CreateRedshiftIdcApplication", checksum = "4d98aaa5")]
#[test_action("redshift", "CreateScheduledAction", checksum = "fe6b101a")]
#[test_action("redshift", "CreateSnapshotCopyGrant", checksum = "d0856ecc")]
#[test_action("redshift", "CreateSnapshotSchedule", checksum = "4b8c04ae")]
#[test_action("redshift", "DeauthorizeDataShare", checksum = "9cd1aad3")]
#[test_action("redshift", "DeleteAuthenticationProfile", checksum = "6e33f912")]
#[test_action("redshift", "DeleteCustomDomainAssociation", checksum = "aab5b65c")]
#[test_action("redshift", "DeleteEndpointAccess", checksum = "b50ddc6a")]
#[test_action("redshift", "DeleteHsmClientCertificate", checksum = "d104a4f0")]
#[test_action("redshift", "DeleteHsmConfiguration", checksum = "633b1720")]
#[test_action("redshift", "DeleteIntegration", checksum = "9ce7f430")]
#[test_action("redshift", "DeletePartner", checksum = "63c0f315")]
#[test_action("redshift", "DeleteRedshiftIdcApplication", checksum = "32f4dad0")]
#[test_action("redshift", "DeleteResourcePolicy", checksum = "da82d1a5")]
#[test_action("redshift", "DeleteScheduledAction", checksum = "9042a00d")]
#[test_action("redshift", "DeleteSnapshotCopyGrant", checksum = "c0661b17")]
#[test_action("redshift", "DeleteSnapshotSchedule", checksum = "1f64c3b6")]
#[test_action("redshift", "DeregisterNamespace", checksum = "52bc7434")]
#[test_action("redshift", "DescribeAccountAttributes", checksum = "1d54313c")]
#[test_action("redshift", "DescribeAuthenticationProfiles", checksum = "a5ade568")]
#[test_action("redshift", "DescribeClusterDbRevisions", checksum = "db9057e7")]
#[test_action("redshift", "DescribeClusterTracks", checksum = "1b76ff67")]
#[test_action("redshift", "DescribeClusterVersions", checksum = "f9270dc7")]
#[test_action("redshift", "DescribeCustomDomainAssociations", checksum = "90a11b4a")]
#[test_action("redshift", "DescribeDataShares", checksum = "17ae2cc0")]
#[test_action("redshift", "DescribeDataSharesForConsumer", checksum = "cdd045da")]
#[test_action("redshift", "DescribeDataSharesForProducer", checksum = "15c595a9")]
#[test_action("redshift", "DescribeEndpointAccess", checksum = "d354f22a")]
#[test_action("redshift", "DescribeEndpointAuthorization", checksum = "ea5c4ae0")]
#[test_action("redshift", "DescribeEventCategories", checksum = "3a28a6e3")]
#[test_action("redshift", "DescribeEvents", checksum = "b67a50ce")]
#[test_action("redshift", "DescribeHsmClientCertificates", checksum = "3385a066")]
#[test_action("redshift", "DescribeHsmConfigurations", checksum = "6808d175")]
#[test_action("redshift", "DescribeInboundIntegrations", checksum = "7cec832c")]
#[test_action("redshift", "DescribeIntegrations", checksum = "cefcf36c")]
#[test_action("redshift", "DescribeLoggingStatus", checksum = "dfa7bfb9")]
#[test_action("redshift", "DescribeNodeConfigurationOptions", checksum = "855d10ce")]
#[test_action("redshift", "DescribeOrderableClusterOptions", checksum = "4d178753")]
#[test_action("redshift", "DescribePartners", checksum = "b2ee3e5b")]
#[test_action("redshift", "DescribeRedshiftIdcApplications", checksum = "41970e23")]
#[test_action(
    "redshift",
    "DescribeReservedNodeExchangeStatus",
    checksum = "a00b0f18"
)]
#[test_action("redshift", "DescribeReservedNodeOfferings", checksum = "2e0b5e59")]
#[test_action("redshift", "DescribeReservedNodes", checksum = "55a145e7")]
#[test_action("redshift", "DescribeResize", checksum = "30ad885a")]
#[test_action("redshift", "DescribeScheduledActions", checksum = "46db0056")]
#[test_action("redshift", "DescribeSnapshotCopyGrants", checksum = "c1c1ab25")]
#[test_action("redshift", "DescribeSnapshotSchedules", checksum = "e774feb5")]
#[test_action("redshift", "DescribeStorage", checksum = "13d3b7d9")]
#[test_action("redshift", "DescribeTableRestoreStatus", checksum = "2ed1f063")]
#[test_action("redshift", "DisableLogging", checksum = "6ae9ff2b")]
#[test_action("redshift", "DisableSnapshotCopy", checksum = "559e5b80")]
#[test_action("redshift", "DisassociateDataShareConsumer", checksum = "7db77f76")]
#[test_action("redshift", "EnableLogging", checksum = "3032a75b")]
#[test_action("redshift", "EnableSnapshotCopy", checksum = "a6f6cef6")]
#[test_action("redshift", "FailoverPrimaryCompute", checksum = "f6cbcda1")]
#[test_action("redshift", "GetClusterCredentials", checksum = "2d5d547f")]
#[test_action("redshift", "GetClusterCredentialsWithIAM", checksum = "e6675db5")]
#[test_action("redshift", "GetIdentityCenterAuthToken", checksum = "3ed6c495")]
#[test_action(
    "redshift",
    "GetReservedNodeExchangeConfigurationOptions",
    checksum = "596e25fa"
)]
#[test_action("redshift", "GetReservedNodeExchangeOfferings", checksum = "2fe21213")]
#[test_action("redshift", "GetResourcePolicy", checksum = "1b0f9ea8")]
#[test_action("redshift", "ListRecommendations", checksum = "780dbf61")]
#[test_action("redshift", "ModifyAquaConfiguration", checksum = "d8bafec9")]
#[test_action("redshift", "ModifyAuthenticationProfile", checksum = "7961eb26")]
#[test_action("redshift", "ModifyClusterDbRevision", checksum = "97fccc7e")]
#[test_action("redshift", "ModifyClusterIamRoles", checksum = "95501292")]
#[test_action("redshift", "ModifyClusterMaintenance", checksum = "dceb3578")]
#[test_action("redshift", "ModifyClusterSnapshotSchedule", checksum = "4bd21a7c")]
#[test_action("redshift", "ModifyCustomDomainAssociation", checksum = "b8232aa1")]
#[test_action("redshift", "ModifyEndpointAccess", checksum = "9b1d8647")]
#[test_action("redshift", "ModifyIntegration", checksum = "c6cdffbb")]
#[test_action("redshift", "ModifyLakehouseConfiguration", checksum = "62abdb91")]
#[test_action("redshift", "ModifyRedshiftIdcApplication", checksum = "f8f40c9f")]
#[test_action("redshift", "ModifyScheduledAction", checksum = "66a08236")]
#[test_action("redshift", "ModifySnapshotCopyRetentionPeriod", checksum = "1e7d6902")]
#[test_action("redshift", "ModifySnapshotSchedule", checksum = "0e55379f")]
#[test_action("redshift", "PauseCluster", checksum = "e1402bbb")]
#[test_action("redshift", "PurchaseReservedNodeOffering", checksum = "e894e7b3")]
#[test_action("redshift", "PutResourcePolicy", checksum = "8582dd43")]
#[test_action("redshift", "RegisterNamespace", checksum = "a09baabc")]
#[test_action("redshift", "RejectDataShare", checksum = "ac709d29")]
#[test_action("redshift", "ResizeCluster", checksum = "acba8073")]
#[test_action("redshift", "RestoreFromClusterSnapshot", checksum = "474c4bf4")]
#[test_action("redshift", "RestoreTableFromClusterSnapshot", checksum = "6fc8e4aa")]
#[test_action("redshift", "ResumeCluster", checksum = "c7258858")]
#[test_action("redshift", "RevokeEndpointAccess", checksum = "cfa6e220")]
#[test_action("redshift", "RevokeSnapshotAccess", checksum = "38ebeeb4")]
#[test_action("redshift", "RotateEncryptionKey", checksum = "7ff30b46")]
#[test_action("redshift", "UpdatePartnerStatus", checksum = "9acbe430")]
#[tokio::test]
async fn redshift_remaining_routes_exist() {
    let server = TestServer::start().await;
    rs_post(
        &server,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "c1"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
        ],
    )
    .await;
    rs_post(
        &server,
        "CreateClusterSnapshot",
        &[("SnapshotIdentifier", "s1"), ("ClusterIdentifier", "c1")],
    )
    .await;
    let cases: &[(&str, &[(&str, &str)])] = &[
        ("AcceptReservedNodeExchange", &[("ReservedNodeId", "fc-test"), ("TargetReservedNodeOfferingId", "fc-test")]),
        ("AddPartner", &[("AccountId", "123456789012"), ("ClusterIdentifier", "fc-test"), ("DatabaseName", "fc-test"), ("PartnerName", "fc-test")]),
        ("AssociateDataShareConsumer", &[("DataShareArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("AuthorizeDataShare", &[("DataShareArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01"), ("ConsumerIdentifier", "fc-test")]),
        ("AuthorizeEndpointAccess", &[("Account", "fc-test")]),
        ("AuthorizeSnapshotAccess", &[("AccountWithRestoreAccess", "fc-test")]),
        ("BatchDeleteClusterSnapshots", &[("Identifiers.member.1.SnapshotIdentifier", "s1")]),
        ("BatchModifyClusterSnapshots", &[("SnapshotIdentifierList.member.1", "fc-test")]),
        ("CancelResize", &[("ClusterIdentifier", "fc-test")]),
        ("CreateAuthenticationProfile", &[("AuthenticationProfileName", "fc-test"), ("AuthenticationProfileContent", "fc-test")]),
        ("CreateCustomDomainAssociation", &[("CustomDomainName", "example.com"), ("CustomDomainCertificateArn", "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-1111-2222-3333-444455556666"), ("ClusterIdentifier", "fc-test")]),
        ("CreateEndpointAccess", &[("EndpointName", "fc-test"), ("SubnetGroupName", "fc-test")]),
        ("CreateHsmClientCertificate", &[("HsmClientCertificateIdentifier", "fc-test")]),
        ("CreateHsmConfiguration", &[("HsmConfigurationIdentifier", "fc-test"), ("Description", "fc-test"), ("HsmIpAddress", "fc-test"), ("HsmPartitionName", "fc-test"), ("HsmPartitionPassword", "fc-test"), ("HsmServerPublicCertificate", "fc-test")]),
        ("CreateIntegration", &[("SourceArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01"), ("TargetArn", "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-1111-2222-3333-444455556666"), ("IntegrationName", "fc-test")]),
        ("CreateRedshiftIdcApplication", &[("IdcInstanceArn", "arn:aws:redshift:us-east-1:123456789012:namespace:test"), ("RedshiftIdcApplicationName", "fc-test"), ("IdcDisplayName", "fc-test"), ("IamRoleArn", "arn:aws:redshift:us-east-1:123456789012:namespace:test")]),
        ("CreateScheduledAction", &[("ScheduledActionName", "fc-test"), ("TargetAction.ResizeCluster.ClusterIdentifier", "c1"), ("TargetAction.ResizeCluster.ClusterType", "multi-node"), ("Schedule", "rate(12 hours)"), ("IamRole", "fc-test")]),
        ("CreateSnapshotCopyGrant", &[("SnapshotCopyGrantName", "fc-test")]),
        ("CreateSnapshotSchedule", &[]),
        ("DeauthorizeDataShare", &[("DataShareArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01"), ("ConsumerIdentifier", "fc-test")]),
        ("DeleteAuthenticationProfile", &[("AuthenticationProfileName", "fc-test")]),
        ("DeleteCustomDomainAssociation", &[("ClusterIdentifier", "fc-test"), ("CustomDomainName", "example.com")]),
        ("DeleteEndpointAccess", &[("EndpointName", "fc-test")]),
        ("DeleteHsmClientCertificate", &[("HsmClientCertificateIdentifier", "fc-test")]),
        ("DeleteHsmConfiguration", &[("HsmConfigurationIdentifier", "fc-test")]),
        ("DeleteIntegration", &[("IntegrationArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("DeletePartner", &[("AccountId", "123456789012"), ("ClusterIdentifier", "fc-test"), ("DatabaseName", "fc-test"), ("PartnerName", "fc-test")]),
        ("DeleteRedshiftIdcApplication", &[("RedshiftIdcApplicationArn", "arn:aws:redshift:us-east-1:123456789012:redshiftidcapplication:test")]),
        ("DeleteResourcePolicy", &[("ResourceArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("DeleteScheduledAction", &[("ScheduledActionName", "fc-test")]),
        ("DeleteSnapshotCopyGrant", &[("SnapshotCopyGrantName", "fc-test")]),
        ("DeleteSnapshotSchedule", &[("ScheduleIdentifier", "fc-test")]),
        ("DeregisterNamespace", &[("NamespaceIdentifier", "fc-test"), ("ConsumerIdentifiers.member.1", "fc-test")]),
        ("DescribeAccountAttributes", &[]),
        ("DescribeAuthenticationProfiles", &[]),
        ("DescribeClusterDbRevisions", &[]),
        ("DescribeClusterTracks", &[]),
        ("DescribeClusterVersions", &[]),
        ("DescribeCustomDomainAssociations", &[]),
        ("DescribeDataShares", &[]),
        ("DescribeDataSharesForConsumer", &[]),
        ("DescribeDataSharesForProducer", &[]),
        ("DescribeEndpointAccess", &[]),
        ("DescribeEndpointAuthorization", &[]),
        ("DescribeEventCategories", &[]),
        ("DescribeEvents", &[]),
        ("DescribeHsmClientCertificates", &[]),
        ("DescribeHsmConfigurations", &[]),
        ("DescribeInboundIntegrations", &[]),
        ("DescribeIntegrations", &[]),
        ("DescribeLoggingStatus", &[("ClusterIdentifier", "fc-test")]),
        ("DescribeNodeConfigurationOptions", &[("ActionType", "restore-cluster")]),
        ("DescribeOrderableClusterOptions", &[]),
        ("DescribePartners", &[("AccountId", "123456789012"), ("ClusterIdentifier", "fc-test")]),
        ("DescribeRedshiftIdcApplications", &[]),
        ("DescribeReservedNodeExchangeStatus", &[]),
        ("DescribeReservedNodeOfferings", &[]),
        ("DescribeReservedNodes", &[]),
        ("DescribeResize", &[("ClusterIdentifier", "fc-test")]),
        ("DescribeScheduledActions", &[]),
        ("DescribeSnapshotCopyGrants", &[]),
        ("DescribeSnapshotSchedules", &[]),
        ("DescribeStorage", &[]),
        ("DescribeTableRestoreStatus", &[]),
        ("DisableLogging", &[("ClusterIdentifier", "fc-test")]),
        ("DisableSnapshotCopy", &[("ClusterIdentifier", "fc-test")]),
        ("DisassociateDataShareConsumer", &[("DataShareArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("EnableLogging", &[("ClusterIdentifier", "fc-test")]),
        ("EnableSnapshotCopy", &[("ClusterIdentifier", "fc-test"), ("DestinationRegion", "fc-test")]),
        ("FailoverPrimaryCompute", &[("ClusterIdentifier", "fc-test")]),
        ("GetClusterCredentials", &[("DbUser", "fc-test")]),
        ("GetClusterCredentialsWithIAM", &[]),
        ("GetIdentityCenterAuthToken", &[("ClusterIds.member.1", "fc-test")]),
        ("GetReservedNodeExchangeConfigurationOptions", &[("ActionType", "restore-cluster")]),
        ("GetReservedNodeExchangeOfferings", &[("ReservedNodeId", "fc-test")]),
        ("GetResourcePolicy", &[("ResourceArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("ListRecommendations", &[]),
        ("ModifyAquaConfiguration", &[("ClusterIdentifier", "fc-test")]),
        ("ModifyAuthenticationProfile", &[("AuthenticationProfileName", "fc-test"), ("AuthenticationProfileContent", "fc-test")]),
        ("ModifyClusterDbRevision", &[("ClusterIdentifier", "fc-test"), ("RevisionTarget", "fc-test")]),
        ("ModifyClusterIamRoles", &[("ClusterIdentifier", "fc-test")]),
        ("ModifyClusterMaintenance", &[("ClusterIdentifier", "fc-test")]),
        ("ModifyClusterSnapshotSchedule", &[("ClusterIdentifier", "fc-test")]),
        ("ModifyCustomDomainAssociation", &[("CustomDomainName", "example.com"), ("CustomDomainCertificateArn", "arn:aws:acm:us-east-1:123456789012:certificate/abcd1234-1111-2222-3333-444455556666"), ("ClusterIdentifier", "fc-test")]),
        ("ModifyEndpointAccess", &[("EndpointName", "fc-test")]),
        ("ModifyIntegration", &[("IntegrationArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("ModifyLakehouseConfiguration", &[("ClusterIdentifier", "fc-test")]),
        ("ModifyRedshiftIdcApplication", &[("RedshiftIdcApplicationArn", "arn:aws:redshift:us-east-1:123456789012:redshiftidcapplication:test")]),
        ("ModifyScheduledAction", &[("ScheduledActionName", "fc-test")]),
        ("ModifySnapshotCopyRetentionPeriod", &[("ClusterIdentifier", "fc-test"), ("RetentionPeriod", "100")]),
        ("ModifySnapshotSchedule", &[("ScheduleIdentifier", "fc-test"), ("ScheduleDefinitions.member.1", "fc-test")]),
        ("PauseCluster", &[("ClusterIdentifier", "fc-test")]),
        ("PurchaseReservedNodeOffering", &[("ReservedNodeOfferingId", "fc-test")]),
        ("PutResourcePolicy", &[("ResourceArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01"), ("Policy", "policy-allow-all")]),
        ("RegisterNamespace", &[("NamespaceIdentifier", "fc-test"), ("ConsumerIdentifiers.member.1", "fc-test")]),
        ("RejectDataShare", &[("DataShareArn", "arn:aws:redshift:us-east-1:123456789012:integration:test-integ-01")]),
        ("ResizeCluster", &[("ClusterIdentifier", "fc-test")]),
        ("RestoreFromClusterSnapshot", &[("ClusterIdentifier", "fc-test")]),
        ("RestoreTableFromClusterSnapshot", &[("ClusterIdentifier", "fc-test"), ("SnapshotIdentifier", "fc-test"), ("SourceDatabaseName", "fc-test"), ("SourceTableName", "fc-test"), ("NewTableName", "fc-test")]),
        ("ResumeCluster", &[("ClusterIdentifier", "fc-test")]),
        ("RevokeEndpointAccess", &[]),
        ("RevokeSnapshotAccess", &[("AccountWithRestoreAccess", "fc-test")]),
        ("RotateEncryptionKey", &[("ClusterIdentifier", "fc-test")]),
        ("UpdatePartnerStatus", &[("AccountId", "123456789012"), ("ClusterIdentifier", "fc-test"), ("DatabaseName", "fc-test"), ("PartnerName", "fc-test"), ("Status", "Active")]),
    ];
    for (action, params) in cases {
        let (status, body) = rs_post(&server, action, params).await;
        assert!(
            routed(status, &body),
            "{action} not routed: HTTP {status} body={body}"
        );
    }
}
