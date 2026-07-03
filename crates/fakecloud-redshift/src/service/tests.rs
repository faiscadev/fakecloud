//! Handler-level unit tests: drive `dispatch` directly with synthetic
//! awsQuery requests and assert on the rendered XML, covering the CRUD
//! lifecycle, filtering, error codes, and the AWS-fidelity details the
//! Terraform provider depends on (Source=user parameter filtering, MultiAZ
//! status strings, SnapshotArn, LogExports round-trip, schedule
//! associations, and the endpoint VpcEndpoint block).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};

use fakecloud_core::service::{AwsRequest, AwsResponse};

use super::RedshiftService;
use crate::state::RedshiftAccounts;

fn service() -> RedshiftService {
    RedshiftService::new(Arc::new(RwLock::new(RedshiftAccounts::default())))
}

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::new();
    query_params.insert("Action".to_string(), action.to_string());
    query_params.insert("Version".to_string(), "2012-12-01".to_string());
    for (k, v) in params {
        query_params.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "redshift".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::new(),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn body(resp: &AwsResponse) -> String {
    String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
}

/// Run an op, expecting a 2xx, and return the rendered body.
fn ok(svc: &RedshiftService, action: &str, params: &[(&str, &str)]) -> String {
    match svc.dispatch(&req(action, params)) {
        Ok(resp) => {
            assert!(
                resp.status.is_success(),
                "{action} returned {}",
                resp.status
            );
            body(&resp)
        }
        Err(e) => panic!("{action} should succeed, got {}", e.code()),
    }
}

/// Run an op expecting a failure and return the AWS error code.
fn err_code(svc: &RedshiftService, action: &str, params: &[(&str, &str)]) -> String {
    match svc.dispatch(&req(action, params)) {
        Ok(_) => panic!("{action} should have failed"),
        Err(e) => e.code().to_string(),
    }
}

#[test]
fn cluster_create_describe_delete_lifecycle() {
    let svc = service();
    let out = ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "c1"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("ClusterType", "single-node"),
        ],
    );
    assert!(out.contains("<ClusterIdentifier>c1</ClusterIdentifier>"));
    // New clusters progress straight to `available` so the create waiter ends.
    assert!(out.contains("<ClusterStatus>available</ClusterStatus>"));
    // Synthetic endpoint + leader node are well formed.
    assert!(out.contains(".us-east-1.redshift.amazonaws.com"));
    assert!(out.contains("<Port>5439</Port>"));

    let listed = ok(&svc, "DescribeClusters", &[]);
    assert!(listed.contains("<ClusterIdentifier>c1</ClusterIdentifier>"));

    ok(&svc, "DeleteCluster", &[("ClusterIdentifier", "c1")]);
    // A second describe of the deleted cluster is a ClusterNotFound error.
    assert_eq!(
        err_code(&svc, "DescribeClusters", &[("ClusterIdentifier", "c1")]),
        "ClusterNotFound"
    );
}

#[test]
fn duplicate_cluster_conflicts() {
    let svc = service();
    let p = &[
        ("ClusterIdentifier", "dup"),
        ("NodeType", "ra3.xlplus"),
        ("MasterUsername", "admin"),
        ("MasterUserPassword", "Passw0rd123"),
    ];
    ok(&svc, "CreateCluster", p);
    assert_eq!(err_code(&svc, "CreateCluster", p), "ClusterAlreadyExists");
}

#[test]
fn multi_az_renders_enabled_disabled() {
    let svc = service();
    let off = ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "az-off"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    assert!(off.contains("<MultiAZ>Disabled</MultiAZ>"));
    let on = ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "az-on"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("MultiAZ", "true"),
        ],
    );
    assert!(on.contains("<MultiAZ>Enabled</MultiAZ>"));
}

#[test]
fn parameter_group_source_user_filter() {
    let svc = service();
    ok(
        &svc,
        "CreateClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg"),
            ("ParameterGroupFamily", "redshift-1.0"),
            ("Description", "d"),
        ],
    );
    ok(
        &svc,
        "ModifyClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg"),
            ("Parameters.Parameter.1.ParameterName", "require_ssl"),
            ("Parameters.Parameter.1.ParameterValue", "true"),
        ],
    );
    // Source=user returns ONLY the modified parameter, not engine defaults —
    // this is what keeps the Terraform provider from seeing perpetual drift.
    let user = ok(
        &svc,
        "DescribeClusterParameters",
        &[("ParameterGroupName", "pg"), ("Source", "user")],
    );
    assert!(user.contains("<ParameterName>require_ssl</ParameterName>"));
    assert!(!user.contains("max_cursor_result_set_size"));
    // Without a filter, engine defaults are visible too.
    let all = ok(
        &svc,
        "DescribeClusterParameters",
        &[("ParameterGroupName", "pg")],
    );
    assert!(all.contains("max_cursor_result_set_size"));
}

#[test]
fn parameters_accept_member_wrapper_too() {
    let svc = service();
    ok(
        &svc,
        "CreateClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg2"),
            ("ParameterGroupFamily", "redshift-1.0"),
            ("Description", "d"),
        ],
    );
    ok(
        &svc,
        "ModifyClusterParameterGroup",
        &[
            ("ParameterGroupName", "pg2"),
            ("Parameters.member.1.ParameterName", "require_ssl"),
            ("Parameters.member.1.ParameterValue", "true"),
        ],
    );
    let user = ok(
        &svc,
        "DescribeClusterParameters",
        &[("ParameterGroupName", "pg2"), ("Source", "user")],
    );
    assert!(user.contains("<ParameterName>require_ssl</ParameterName>"));
}

#[test]
fn snapshot_has_arn_and_filters_by_cluster() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "snapc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    let snap = ok(
        &svc,
        "CreateClusterSnapshot",
        &[("SnapshotIdentifier", "s1"), ("ClusterIdentifier", "snapc")],
    );
    assert!(snap.contains(
        "<SnapshotArn>arn:aws:redshift:us-east-1:123456789012:snapshot:snapc/s1</SnapshotArn>"
    ));
    let listed = ok(
        &svc,
        "DescribeClusterSnapshots",
        &[("ClusterIdentifier", "snapc")],
    );
    assert!(listed.contains("<SnapshotIdentifier>s1</SnapshotIdentifier>"));
    // A snapshot filtered to a different cluster is not returned.
    let other = ok(
        &svc,
        "DescribeClusterSnapshots",
        &[("ClusterIdentifier", "nope")],
    );
    assert!(!other.contains("<SnapshotIdentifier>s1</SnapshotIdentifier>"));
}

#[test]
fn snapshot_copy_status_round_trips() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "scc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    let enabled = ok(
        &svc,
        "EnableSnapshotCopy",
        &[
            ("ClusterIdentifier", "scc"),
            ("DestinationRegion", "us-west-2"),
            ("RetentionPeriod", "14"),
        ],
    );
    assert!(enabled.contains("<ClusterSnapshotCopyStatus>"));
    assert!(enabled.contains("<DestinationRegion>us-west-2</DestinationRegion>"));
    assert!(enabled.contains("<RetentionPeriod>14</RetentionPeriod>"));
    // The status is visible on a subsequent DescribeClusters read.
    let described = ok(&svc, "DescribeClusters", &[("ClusterIdentifier", "scc")]);
    assert!(described.contains("<DestinationRegion>us-west-2</DestinationRegion>"));
    // Disabling clears it.
    let disabled = ok(&svc, "DisableSnapshotCopy", &[("ClusterIdentifier", "scc")]);
    assert!(!disabled.contains("<ClusterSnapshotCopyStatus>"));
}

#[test]
fn logging_log_exports_round_trip() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "logc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    ok(
        &svc,
        "EnableLogging",
        &[
            ("ClusterIdentifier", "logc"),
            ("LogDestinationType", "cloudwatch"),
            ("LogExports.member.1", "connectionlog"),
            ("LogExports.member.2", "userlog"),
        ],
    );
    let status = ok(
        &svc,
        "DescribeLoggingStatus",
        &[("ClusterIdentifier", "logc")],
    );
    assert!(status.contains("<LoggingEnabled>true</LoggingEnabled>"));
    assert!(status.contains(
        "<LogExports><member>connectionlog</member><member>userlog</member></LogExports>"
    ));
}

#[test]
fn snapshot_schedule_association_visible_on_describe() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "schedc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    ok(
        &svc,
        "CreateSnapshotSchedule",
        &[
            ("ScheduleIdentifier", "sch1"),
            ("ScheduleDefinitions.ScheduleDefinition.1", "rate(12 hours)"),
        ],
    );
    ok(
        &svc,
        "ModifyClusterSnapshotSchedule",
        &[
            ("ClusterIdentifier", "schedc"),
            ("ScheduleIdentifier", "sch1"),
        ],
    );
    // The schedule now lists the cluster under AssociatedClusters and the
    // response is filtered to the single requested schedule.
    let described = ok(
        &svc,
        "DescribeSnapshotSchedules",
        &[
            ("ScheduleIdentifier", "sch1"),
            ("ClusterIdentifier", "schedc"),
        ],
    );
    assert!(described.contains("<ScheduleIdentifier>sch1</ScheduleIdentifier>"));
    assert!(described.contains("<ClusterIdentifier>schedc</ClusterIdentifier>"));
    assert!(described.contains("<AssociatedClusterCount>1</AssociatedClusterCount>"));
}

#[test]
fn endpoint_access_inherits_default_sg_and_vpc_endpoint() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "epc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    let created = ok(
        &svc,
        "CreateEndpointAccess",
        &[
            ("EndpointName", "ep"),
            ("ClusterIdentifier", "epc"),
            ("SubnetGroupName", "sg"),
        ],
    );
    assert!(created.contains("<EndpointStatus>active</EndpointStatus>"));
    // No SG supplied -> a default one is attached so the resource is non-empty.
    assert!(created.contains("<VpcSecurityGroupId>"));
    // A well-formed interface VPC endpoint is surfaced.
    assert!(created.contains("<VpcEndpoint><VpcEndpointId>vpce-"));
    // DescribeEndpointAccess filters by EndpointName (provider asserts single).
    let one = ok(&svc, "DescribeEndpointAccess", &[("EndpointName", "ep")]);
    assert!(one.contains("<EndpointName>ep</EndpointName>"));
    let none = ok(&svc, "DescribeEndpointAccess", &[("EndpointName", "other")]);
    assert!(!none.contains("<EndpointName>ep</EndpointName>"));
}

#[test]
fn authentication_profile_list_uses_member_wrapper() {
    let svc = service();
    ok(
        &svc,
        "CreateAuthenticationProfile",
        &[
            ("AuthenticationProfileName", "ap"),
            ("AuthenticationProfileContent", "{\"a\":\"b\"}"),
        ],
    );
    let listed = ok(
        &svc,
        "DescribeAuthenticationProfiles",
        &[("AuthenticationProfileName", "ap")],
    );
    // The AuthenticationProfileList member uses the default `member` wrapper,
    // which the AWS SDK requires to parse the list as non-empty.
    assert!(listed.contains(
        "<AuthenticationProfiles><member><AuthenticationProfileName>ap</AuthenticationProfileName>"
    ));
}

#[test]
fn subnet_group_crud_and_tags() {
    let svc = service();
    let created = ok(
        &svc,
        "CreateClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "sng"),
            ("Description", "desc"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-1"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-2"),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "test"),
        ],
    );
    assert!(created.contains("<SubnetIdentifier>subnet-1</SubnetIdentifier>"));
    assert!(created.contains("<SubnetIdentifier>subnet-2</SubnetIdentifier>"));
    assert!(created.contains("<Key>env</Key><Value>test</Value>"));

    ok(
        &svc,
        "DeleteClusterSubnetGroup",
        &[("ClusterSubnetGroupName", "sng")],
    );
    assert_eq!(
        err_code(
            &svc,
            "DescribeClusterSubnetGroups",
            &[("ClusterSubnetGroupName", "sng")],
        ),
        "ClusterSubnetGroupNotFoundFault"
    );
}

#[test]
fn unknown_cluster_operations_error_cleanly() {
    let svc = service();
    for action in [
        "ModifyCluster",
        "RebootCluster",
        "DeleteCluster",
        "EnableLogging",
    ] {
        assert_eq!(
            err_code(&svc, action, &[("ClusterIdentifier", "ghost")]),
            "ClusterNotFound",
            "{action}"
        );
    }
}

#[test]
fn endpoint_authorization_round_trip() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "eac"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    // Authorize returns a persisted authorization.
    let authed = ok(
        &svc,
        "AuthorizeEndpointAccess",
        &[("ClusterIdentifier", "eac"), ("Account", "210987654321")],
    );
    assert!(authed.contains("<Grantee>210987654321</Grantee>"));
    assert!(authed.contains("<Status>Authorized</Status>"));
    assert!(authed.contains("<AllowedAllVPCs>true</AllowedAllVPCs>"));

    // Describe sees it, wrapped in the `member` list wrapper.
    let listed = ok(&svc, "DescribeEndpointAuthorization", &[]);
    assert!(
        listed.contains("<EndpointAuthorizationList><member><Grantor>"),
        "expected member wrapper, got: {listed}"
    );
    assert!(listed.contains("<Grantee>210987654321</Grantee>"));
    assert!(listed.contains("<ClusterIdentifier>eac</ClusterIdentifier>"));

    // A duplicate authorization for the same (cluster, account) conflicts.
    assert_eq!(
        err_code(
            &svc,
            "AuthorizeEndpointAccess",
            &[("ClusterIdentifier", "eac"), ("Account", "210987654321")],
        ),
        "EndpointAuthorizationAlreadyExists"
    );

    // Revoke removes it (status flips to Revoking on the echoed copy).
    let revoked = ok(
        &svc,
        "RevokeEndpointAccess",
        &[("ClusterIdentifier", "eac"), ("Account", "210987654321")],
    );
    assert!(revoked.contains("<Status>Revoking</Status>"));

    // Describe is now empty, and a second revoke is a not-found fault.
    let empty = ok(&svc, "DescribeEndpointAuthorization", &[]);
    assert!(!empty.contains("<Grantee>210987654321</Grantee>"));
    assert_eq!(
        err_code(
            &svc,
            "RevokeEndpointAccess",
            &[("ClusterIdentifier", "eac"), ("Account", "210987654321")],
        ),
        "EndpointAuthorizationNotFound"
    );
}

#[test]
fn endpoint_authorization_unknown_cluster_and_filters() {
    let svc = service();
    for id in ["ea1", "ea2"] {
        ok(
            &svc,
            "CreateCluster",
            &[
                ("ClusterIdentifier", id),
                ("NodeType", "ra3.xlplus"),
                ("MasterUsername", "admin"),
                ("MasterUserPassword", "Passw0rd123"),
            ],
        );
    }
    // Authorizing against a cluster that does not exist is ClusterNotFound.
    assert_eq!(
        err_code(
            &svc,
            "AuthorizeEndpointAccess",
            &[("ClusterIdentifier", "ghost"), ("Account", "210987654321")],
        ),
        "ClusterNotFound"
    );
    ok(
        &svc,
        "AuthorizeEndpointAccess",
        &[("ClusterIdentifier", "ea1"), ("Account", "210987654321")],
    );
    ok(
        &svc,
        "AuthorizeEndpointAccess",
        &[("ClusterIdentifier", "ea2"), ("Account", "310987654321")],
    );
    // Filter by ClusterIdentifier.
    let by_cluster = ok(
        &svc,
        "DescribeEndpointAuthorization",
        &[("ClusterIdentifier", "ea1")],
    );
    assert!(by_cluster.contains("<Grantee>210987654321</Grantee>"));
    assert!(!by_cluster.contains("<Grantee>310987654321</Grantee>"));
    // Filter by grantee Account.
    let by_account = ok(
        &svc,
        "DescribeEndpointAuthorization",
        &[("Account", "310987654321")],
    );
    assert!(by_account.contains("<ClusterIdentifier>ea2</ClusterIdentifier>"));
    assert!(!by_account.contains("<Grantee>210987654321</Grantee>"));
    // Grantee=true asks for authorizations received by the caller; the caller is
    // the grantor of both, so the list is empty.
    let as_grantee = ok(
        &svc,
        "DescribeEndpointAuthorization",
        &[("Grantee", "true")],
    );
    assert!(!as_grantee.contains("<Grantee>"));
}

#[test]
fn tag_operations_round_trip() {
    let svc = service();
    ok(
        &svc,
        "CreateClusterParameterGroup",
        &[
            ("ParameterGroupName", "tagpg"),
            ("ParameterGroupFamily", "redshift-1.0"),
            ("Description", "d"),
        ],
    );
    let arn = "arn:aws:redshift:us-east-1:123456789012:parametergroup:tagpg";
    ok(
        &svc,
        "CreateTags",
        &[
            ("ResourceName", arn),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    );
    let listed = ok(&svc, "DescribeTags", &[("ResourceName", arn)]);
    assert!(listed.contains("<Key>team</Key>"));
    assert!(listed.contains("<Value>data</Value>"));
}
