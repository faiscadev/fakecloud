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
fn endpoint_access_creates_vpc_endpoint_in_subnet_group_subnets() {
    let ec2: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let subnets = fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
    let vpc = subnets[0].vpc_id.clone();
    let default_sg = fakecloud_ec2::vpc_lookup::default_security_group_id(
        &ec2,
        "123456789012",
        "us-east-1",
        &vpc,
    )
    .unwrap();
    let svc = service().with_ec2_state(ec2.clone());
    let group = ok(
        &svc,
        "CreateClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "sg"),
            ("Description", "d"),
            ("SubnetIds.member.1", &subnets[0].subnet_id),
            ("SubnetIds.member.2", &subnets[1].subnet_id),
        ],
    );
    // The group reports the subnets' real VPC and zones.
    assert!(group.contains(&format!("<VpcId>{vpc}</VpcId>")));
    assert!(group.contains(&format!("<Name>{}</Name>", subnets[1].availability_zone)));
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "epc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("ClusterSubnetGroupName", "sg"),
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
    // No SG supplied -> the VPC's default group is attached.
    assert!(created.contains(&format!(
        "<VpcSecurityGroupId>{default_sg}</VpcSecurityGroupId>"
    )));
    let vpce = created
        .split("<VpcEndpointId>")
        .nth(1)
        .and_then(|s| s.split("</VpcEndpointId>").next())
        .unwrap()
        .to_string();
    assert!(created.contains(&format!("<VpcId>{vpc}</VpcId>")));
    {
        let accounts = ec2.read();
        let state = accounts.regional("123456789012", "us-east-1").unwrap();
        assert!(state.vpc_endpoints.contains_key(&vpce));
        let enis: Vec<_> = state
            .network_interfaces
            .values()
            .filter(|e| e.description == format!("VPC Endpoint Interface {vpce}"))
            .collect();
        assert_eq!(enis.len(), 2);
        assert!(enis.iter().all(|e| e.group_ids == vec![default_sg.clone()]));
    }
    // DescribeEndpointAccess filters by EndpointName (provider asserts single).
    let one = ok(&svc, "DescribeEndpointAccess", &[("EndpointName", "ep")]);
    assert!(one.contains("<EndpointName>ep</EndpointName>"));
    let none = ok(&svc, "DescribeEndpointAccess", &[("EndpointName", "other")]);
    assert!(!none.contains("<EndpointName>ep</EndpointName>"));

    // Deleting the endpoint removes its EC2 resources.
    ok(&svc, "DeleteEndpointAccess", &[("EndpointName", "ep")]);
    let accounts = ec2.read();
    let state = accounts.regional("123456789012", "us-east-1").unwrap();
    assert!(!state.vpc_endpoints.contains_key(&vpce));
    assert!(state
        .network_interfaces
        .values()
        .all(|e| !e.description.contains(&vpce)));
}

#[test]
fn cluster_subnet_group_rejects_unknown_subnets() {
    let ec2: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = service().with_ec2_state(ec2);
    let err = svc
        .dispatch(&req(
            "CreateClusterSubnetGroup",
            &[
                ("ClusterSubnetGroupName", "bad"),
                ("Description", "d"),
                ("SubnetIds.member.1", "subnet-0000000000dead"),
            ],
        ))
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidSubnet");
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
fn custom_domain_associations_use_association_wrapper() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "cdc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    ok(
        &svc,
        "CreateCustomDomainAssociation",
        &[
            ("ClusterIdentifier", "cdc"),
            ("CustomDomainName", "redshift.example.com"),
            (
                "CustomDomainCertificateArn",
                "arn:aws:acm:us-east-1:123456789012:certificate/abc123def456",
            ),
        ],
    );
    let listed = ok(&svc, "DescribeCustomDomainAssociations", &[]);
    // The `AssociationList` member's xmlName is `Association`; the AWS SDK /
    // Terraform provider parses an empty list if the wrapper is anything else.
    assert!(
        listed.contains("<Associations><Association>"),
        "expected <Association> list wrapper, got: {listed}"
    );
    assert!(!listed.contains("<CustomDomainAssociation>"));
    assert!(listed.contains("<CustomDomainName>redshift.example.com</CustomDomainName>"));
    assert!(listed.contains("<ClusterIdentifier>cdc</ClusterIdentifier>"));
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

#[test]
fn scheduled_action_start_end_time_round_trip() {
    let svc = service();
    // Create with an explicit schedule window.
    let out = ok(
        &svc,
        "CreateScheduledAction",
        &[
            ("ScheduledActionName", "sa1"),
            ("Schedule", "cron(0 10 ? * MON *)"),
            ("IamRole", "arn:aws:iam::123456789012:role/r"),
            ("StartTime", "2026-08-01T00:00:00Z"),
            ("EndTime", "2026-09-01T00:00:00Z"),
        ],
    );
    assert!(out.contains("<StartTime>2026-08-01T00:00:00.000Z</StartTime>"));
    assert!(out.contains("<EndTime>2026-09-01T00:00:00.000Z</EndTime>"));

    // Describe returns them too (they were previously dropped).
    let listed = ok(&svc, "DescribeScheduledActions", &[]);
    assert!(listed.contains("<StartTime>2026-08-01T00:00:00.000Z</StartTime>"));
    assert!(listed.contains("<EndTime>2026-09-01T00:00:00.000Z</EndTime>"));

    // Modify shifts the window; the new values persist.
    let modified = ok(
        &svc,
        "ModifyScheduledAction",
        &[
            ("ScheduledActionName", "sa1"),
            ("StartTime", "2026-08-15T12:30:00Z"),
        ],
    );
    assert!(modified.contains("<StartTime>2026-08-15T12:30:00.000Z</StartTime>"));
    // EndTime untouched by the modify is preserved.
    assert!(modified.contains("<EndTime>2026-09-01T00:00:00.000Z</EndTime>"));

    let after = ok(&svc, "DescribeScheduledActions", &[]);
    assert!(after.contains("<StartTime>2026-08-15T12:30:00.000Z</StartTime>"));
    assert!(after.contains("<EndTime>2026-09-01T00:00:00.000Z</EndTime>"));
}

#[test]
fn modify_cluster_rename_rekeys_the_cluster() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "old-name"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    // The rename echoes the new identifier in the ModifyCluster response.
    let renamed = ok(
        &svc,
        "ModifyCluster",
        &[
            ("ClusterIdentifier", "old-name"),
            ("NewClusterIdentifier", "new-name"),
        ],
    );
    assert!(renamed.contains("<ClusterIdentifier>new-name</ClusterIdentifier>"));

    // DescribeClusters on the new id returns it; the old id 404s.
    let listed = ok(
        &svc,
        "DescribeClusters",
        &[("ClusterIdentifier", "new-name")],
    );
    assert!(listed.contains("<ClusterIdentifier>new-name</ClusterIdentifier>"));
    assert_eq!(
        err_code(
            &svc,
            "DescribeClusters",
            &[("ClusterIdentifier", "old-name")]
        ),
        "ClusterNotFound"
    );
    // A later op on the new name resolves (delete succeeds, no 404).
    ok(&svc, "DeleteCluster", &[("ClusterIdentifier", "new-name")]);
}

#[test]
fn modify_cluster_persists_encryption_kms_and_port() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "enc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    let out = ok(
        &svc,
        "ModifyCluster",
        &[
            ("ClusterIdentifier", "enc"),
            ("Encrypted", "true"),
            ("KmsKeyId", "arn:aws:kms:us-east-1:123456789012:key/abcd"),
            ("Port", "5555"),
            ("AvailabilityZone", "us-east-1b"),
            ("AvailabilityZoneRelocation", "true"),
        ],
    );
    assert!(out.contains("<Encrypted>true</Encrypted>"));

    let listed = ok(&svc, "DescribeClusters", &[("ClusterIdentifier", "enc")]);
    assert!(listed.contains("<Encrypted>true</Encrypted>"));
    assert!(listed.contains("<KmsKeyId>arn:aws:kms:us-east-1:123456789012:key/abcd</KmsKeyId>"));
    assert!(listed.contains("<Port>5555</Port>"));
    assert!(listed.contains("<AvailabilityZone>us-east-1b</AvailabilityZone>"));
    assert!(listed
        .contains("<AvailabilityZoneRelocationStatus>enabled</AvailabilityZoneRelocationStatus>"));
}

#[test]
fn modify_aqua_configuration_echoes_requested_status() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "aq"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("ClusterType", "single-node"),
        ],
    );

    // AquaStatus must reflect the requested/stored status, not a hardcoded
    // `disabled` literal.
    let out = ok(
        &svc,
        "ModifyAquaConfiguration",
        &[
            ("ClusterIdentifier", "aq"),
            ("AquaConfigurationStatus", "enabled"),
        ],
    );
    assert!(
        out.contains("<AquaStatus>enabled</AquaStatus>"),
        "AquaStatus should echo the requested status: {out}"
    );
    assert!(out.contains("<AquaConfigurationStatus>enabled</AquaConfigurationStatus>"));
}

#[test]
fn update_partner_status_unknown_partner_faults() {
    let svc = service();
    // No partner registered: UpdatePartnerStatus must fault instead of a silent
    // 200 that pretends the update succeeded.
    assert_eq!(
        err_code(
            &svc,
            "UpdatePartnerStatus",
            &[
                ("AccountId", "123456789012"),
                ("ClusterIdentifier", "c1"),
                ("DatabaseName", "db1"),
                ("PartnerName", "ghost"),
                ("Status", "Active"),
            ],
        ),
        "PartnerNotFound"
    );
}

#[test]
fn update_partner_status_existing_partner_persists() {
    let svc = service();
    ok(
        &svc,
        "AddPartner",
        &[
            ("AccountId", "123456789012"),
            ("ClusterIdentifier", "c1"),
            ("DatabaseName", "db1"),
            ("PartnerName", "datashare"),
        ],
    );
    ok(
        &svc,
        "UpdatePartnerStatus",
        &[
            ("AccountId", "123456789012"),
            ("ClusterIdentifier", "c1"),
            ("DatabaseName", "db1"),
            ("PartnerName", "datashare"),
            ("Status", "Inactive"),
            ("StatusMessage", "paused"),
        ],
    );
    let listed = ok(
        &svc,
        "DescribePartners",
        &[
            ("AccountId", "123456789012"),
            ("ClusterIdentifier", "c1"),
            ("DatabaseName", "db1"),
        ],
    );
    assert!(
        listed.contains("<Status>Inactive</Status>"),
        "DescribePartners should reflect the updated status: {listed}"
    );
}

#[test]
fn china_region_arns_use_the_china_partition_and_resolve_for_tagging() {
    let svc = service();
    let in_cn = |action: &str, params: &[(&str, &str)]| {
        let mut r = req(action, params);
        r.region = "cn-north-1".to_string();
        let resp = svc
            .dispatch(&r)
            .unwrap_or_else(|e| panic!("{action}: {}", e.code()));
        body(&resp)
    };
    in_cn(
        "CreateCluster",
        &[
            ("ClusterIdentifier", "cnc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    let snap = in_cn(
        "CreateClusterSnapshot",
        &[("SnapshotIdentifier", "s1"), ("ClusterIdentifier", "cnc")],
    );
    assert!(snap.contains(
        "<SnapshotArn>arn:aws-cn:redshift:cn-north-1:123456789012:snapshot:cnc/s1</SnapshotArn>"
    ));
    let arn = "arn:aws-cn:redshift:cn-north-1:123456789012:cluster:cnc";
    in_cn(
        "CreateTags",
        &[
            ("ResourceName", arn),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    );
    let listed = in_cn("DescribeTags", &[("ResourceName", arn)]);
    assert!(
        listed.contains(&format!("<ResourceName>{arn}</ResourceName>")),
        "{listed}"
    );
    assert!(listed.contains("<Key>team</Key>"));
}

use fakecloud_kms::test_support::{assert_aws_managed_key, kms_hook};

#[test]
fn snapshot_copy_grant_without_kms_key_uses_aws_managed_key() {
    let (kms_state, hook) = kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    let out = ok(
        &svc,
        "CreateSnapshotCopyGrant",
        &[("SnapshotCopyGrantName", "grant-default")],
    );
    let key = out
        .split("<KmsKeyId>")
        .nth(1)
        .and_then(|rest| rest.split("</KmsKeyId>").next())
        .unwrap_or_else(|| panic!("no KmsKeyId in {out}"));
    assert!(
        key.starts_with("arn:aws:kms:us-east-1:123456789012:key/"),
        "{key}"
    );
    assert_aws_managed_key(
        &kms_state,
        "123456789012",
        "us-east-1",
        key,
        "alias/aws/redshift",
    );
}

fn kms_key_of(xml: &str) -> Option<String> {
    xml.split("<KmsKeyId>")
        .nth(1)
        .and_then(|rest| rest.split("</KmsKeyId>").next())
        .map(str::to_string)
}

fn ok_in(svc: &RedshiftService, region: &str, action: &str, params: &[(&str, &str)]) -> String {
    let mut r = req(action, params);
    r.region = region.to_string();
    let resp = svc
        .dispatch(&r)
        .unwrap_or_else(|e| panic!("{action}: {e:?}"));
    body(&resp)
}

#[test]
fn encrypted_cluster_without_kms_key_uses_the_regions_aws_managed_key() {
    let (kms_state, hook) = kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    let cluster = |region: &str, id: &str, extra: &[(&str, &str)]| {
        let mut params = vec![
            ("ClusterIdentifier", id),
            ("NodeType", "dc2.large"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd!"),
        ];
        params.extend_from_slice(extra);
        ok_in(&svc, region, "CreateCluster", &params)
    };
    let cn = kms_key_of(&cluster("cn-north-1", "enc-cn", &[("Encrypted", "true")]))
        .expect("encrypted cluster reports a key");
    assert_aws_managed_key(
        &kms_state,
        "123456789012",
        "cn-north-1",
        &cn,
        "alias/aws/redshift",
    );
    let east = kms_key_of(&cluster("us-east-1", "enc-east", &[("Encrypted", "true")])).unwrap();
    assert_ne!(east, cn);
    assert_aws_managed_key(
        &kms_state,
        "123456789012",
        "us-east-1",
        &east,
        "alias/aws/redshift",
    );
    // An unencrypted cluster reports no key.
    assert_eq!(kms_key_of(&cluster("us-east-1", "plain", &[])), None);

    // Turning encryption on later without a key also uses the managed key;
    // turning it off clears the key.
    let modified = ok_in(
        &svc,
        "us-east-1",
        "ModifyCluster",
        &[("ClusterIdentifier", "plain"), ("Encrypted", "true")],
    );
    assert_eq!(kms_key_of(&modified).as_deref(), Some(east.as_str()));
    let decrypted = ok_in(
        &svc,
        "us-east-1",
        "ModifyCluster",
        &[("ClusterIdentifier", "plain"), ("Encrypted", "false")],
    );
    assert_eq!(kms_key_of(&decrypted), None);
}

#[test]
fn no_kms_key_is_reported_without_kms() {
    let svc = service();
    let grant = ok(
        &svc,
        "CreateSnapshotCopyGrant",
        &[("SnapshotCopyGrantName", "grant-nokms")],
    );
    assert_eq!(kms_key_of(&grant), None, "{grant}");
    let cluster = ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "enc-nokms"),
            ("NodeType", "dc2.large"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd!"),
            ("Encrypted", "true"),
        ],
    );
    assert_eq!(kms_key_of(&cluster), None, "{cluster}");
}

fn cluster_params<'a>(id: &'a str, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut params = vec![
        ("ClusterIdentifier", id),
        ("NodeType", "dc2.large"),
        ("MasterUsername", "admin"),
        ("MasterUserPassword", "Passw0rd!"),
    ];
    params.extend_from_slice(extra);
    params
}

#[test]
fn modify_cluster_encryption_flags_and_empty_key() {
    let (kms_state, hook) = kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    let mine = "arn:aws:kms:us-east-1:123456789012:key/mine";
    ok(
        &svc,
        "CreateCluster",
        &cluster_params("mc", &[("Encrypted", "true"), ("KmsKeyId", mine)]),
    );

    // Encrypted=false clears the key even when a KmsKeyId is also sent.
    let off = ok(
        &svc,
        "ModifyCluster",
        &[
            ("ClusterIdentifier", "mc"),
            ("Encrypted", "false"),
            ("KmsKeyId", mine),
        ],
    );
    assert_eq!(kms_key_of(&off), None, "{off}");
    assert!(off.contains("<Encrypted>false</Encrypted>"));

    // Encrypted=true with an empty KmsKeyId uses the aws/redshift default.
    let on = ok(
        &svc,
        "ModifyCluster",
        &[
            ("ClusterIdentifier", "mc"),
            ("Encrypted", "true"),
            ("KmsKeyId", ""),
        ],
    );
    let key = kms_key_of(&on).expect("default key");
    assert_aws_managed_key(
        &kms_state,
        "123456789012",
        "us-east-1",
        &key,
        "alias/aws/redshift",
    );

    // CreateCluster with an empty KmsKeyId behaves the same.
    let created = ok(
        &svc,
        "CreateCluster",
        &cluster_params("empty-key", &[("Encrypted", "true"), ("KmsKeyId", "")]),
    );
    assert_eq!(kms_key_of(&created).as_deref(), Some(key.as_str()));
}

/// Requests that fail (existing cluster/grant, unknown cluster) never mint.
#[test]
fn rejected_requests_do_not_mint_a_managed_key() {
    let (kms_state, hook) = kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    let mine = "arn:aws:kms:us-east-1:123456789012:key/mine";
    ok(
        &svc,
        "CreateCluster",
        &cluster_params("dup", &[("Encrypted", "true"), ("KmsKeyId", mine)]),
    );
    ok(
        &svc,
        "CreateSnapshotCopyGrant",
        &[("SnapshotCopyGrantName", "g"), ("KmsKeyId", mine)],
    );
    assert!(svc
        .dispatch(&req(
            "CreateCluster",
            &cluster_params("dup", &[("Encrypted", "true")])
        ))
        .is_err());
    assert!(svc
        .dispatch(&req(
            "CreateSnapshotCopyGrant",
            &[("SnapshotCopyGrantName", "g")]
        ))
        .is_err());
    assert!(svc
        .dispatch(&req(
            "ModifyCluster",
            &[("ClusterIdentifier", "missing"), ("Encrypted", "true")]
        ))
        .is_err());
    assert!(kms_state
        .read()
        .get("123456789012")
        .is_none_or(|st| st.keys.is_empty()));
}

/// Re-sending Encrypted=true on a cluster that already has a customer key
/// keeps that key and mints no AWS-managed key.
#[test]
fn reencrypting_a_customer_key_cluster_mints_nothing() {
    let (kms_state, hook) = kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    let mine = "arn:aws:kms:us-east-1:123456789012:key/mine";
    ok(
        &svc,
        "CreateCluster",
        &cluster_params("cust", &[("Encrypted", "true"), ("KmsKeyId", mine)]),
    );
    let out = ok(
        &svc,
        "ModifyCluster",
        &[("ClusterIdentifier", "cust"), ("Encrypted", "true")],
    );
    assert_eq!(kms_key_of(&out).as_deref(), Some(mine));
    assert!(kms_state
        .read()
        .get("123456789012")
        .is_none_or(|st| st.keys.is_empty()));
}

/// DescribeTags reported snapshots as `snapshot:<id>`, an ARN no other
/// operation returns; it now reports the `snapshot:<cluster>/<id>` ARN
/// that CreateClusterSnapshot returned, and CreateTags/DeleteTags act on
/// exactly that ARN.
#[test]
fn snapshot_tags_use_the_snapshot_arn() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "tagc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
        ],
    );
    ok(
        &svc,
        "CreateClusterSnapshot",
        &[("SnapshotIdentifier", "ts1"), ("ClusterIdentifier", "tagc")],
    );
    let arn = "arn:aws:redshift:us-east-1:123456789012:snapshot:tagc/ts1";
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
    assert!(
        listed.contains(&format!("<ResourceName>{arn}</ResourceName>")),
        "{listed}"
    );
    assert!(listed.contains("<Key>team</Key>"), "{listed}");
    assert!(listed.contains("<ResourceType>snapshot</ResourceType>"));

    // The cluster-less form names no resource.
    let bare = "arn:aws:redshift:us-east-1:123456789012:snapshot:ts1";
    assert_eq!(
        err_code(
            &svc,
            "CreateTags",
            &[
                ("ResourceName", bare),
                ("Tags.Tag.1.Key", "k"),
                ("Tags.Tag.1.Value", "v"),
            ],
        ),
        "ResourceNotFoundFault"
    );

    // A copy gets its own ARN, so tagging the source leaves the copy alone.
    let copy = ok(
        &svc,
        "CopyClusterSnapshot",
        &[
            ("SourceSnapshotIdentifier", "ts1"),
            ("TargetSnapshotIdentifier", "ts2"),
        ],
    );
    let copy_arn = "arn:aws:redshift:us-east-1:123456789012:snapshot:tagc/ts2";
    assert!(copy.contains(copy_arn), "{copy}");

    ok(
        &svc,
        "DeleteTags",
        &[("ResourceName", arn), ("TagKeys.TagKey.1", "team")],
    );
    let listed = ok(&svc, "DescribeTags", &[("ResourceName", arn)]);
    assert!(!listed.contains("<Key>team</Key>"), "{listed}");
}

/// DescribeTags' ResourceType filter takes AWS's documented display
/// values ("Cluster", "Subnet group", ...) case-insensitively, as well as
/// the ARN form it reports back.
#[test]
fn describe_tags_resource_type_matches_documented_values() {
    let svc = service();
    ok(
        &svc,
        "CreateCluster",
        &[
            ("ClusterIdentifier", "rtc"),
            ("NodeType", "ra3.xlplus"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "Passw0rd123"),
            ("Tags.Tag.1.Key", "on"),
            ("Tags.Tag.1.Value", "cluster"),
        ],
    );
    ok(
        &svc,
        "CreateClusterParameterGroup",
        &[
            ("ParameterGroupName", "rtpg"),
            ("ParameterGroupFamily", "redshift-1.0"),
            ("Description", "d"),
            ("Tags.Tag.1.Key", "on"),
            ("Tags.Tag.1.Value", "pg"),
        ],
    );
    for value in ["Cluster", "cluster", "CLUSTER"] {
        let listed = ok(&svc, "DescribeTags", &[("ResourceType", value)]);
        assert!(
            listed.contains("<Value>cluster</Value>"),
            "{value}: {listed}"
        );
        assert!(!listed.contains("<Value>pg</Value>"), "{value}: {listed}");
        assert!(listed.contains("<ResourceType>cluster</ResourceType>"));
    }
    for value in ["Parameter group", "parametergroup"] {
        let listed = ok(&svc, "DescribeTags", &[("ResourceType", value)]);
        assert!(listed.contains("<Value>pg</Value>"), "{value}: {listed}");
        assert!(
            !listed.contains("<Value>cluster</Value>"),
            "{value}: {listed}"
        );
    }
}

/// Integrations were invisible to CreateTags/DescribeTags, and
/// CreateIntegration dropped its `TagList` and
/// `AdditionalEncryptionContext`.
#[test]
fn integration_tags_round_trip() {
    let svc = service();
    let created = ok(
        &svc,
        "CreateIntegration",
        &[
            ("IntegrationName", "zetl"),
            (
                "SourceArn",
                "arn:aws:dynamodb:us-east-1:123456789012:table/t",
            ),
            (
                "TargetArn",
                "arn:aws:redshift:us-east-1:123456789012:namespace:ns",
            ),
            ("TagList.Tag.1.Key", "team"),
            ("TagList.Tag.1.Value", "etl"),
            ("AdditionalEncryptionContext.entry.1.key", "purpose"),
            ("AdditionalEncryptionContext.entry.1.value", "zetl"),
        ],
    );
    assert!(created.contains("<Key>team</Key>"), "{created}");
    assert!(
        created.contains("<entry><key>purpose</key><value>zetl</value></entry>"),
        "{created}"
    );
    let start = created.find("<IntegrationArn>").unwrap() + "<IntegrationArn>".len();
    let end = created.find("</IntegrationArn>").unwrap();
    let arn = created[start..end].to_string();

    ok(
        &svc,
        "CreateTags",
        &[
            ("ResourceName", &arn),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "dev"),
        ],
    );
    let listed = ok(&svc, "DescribeTags", &[("ResourceName", &arn)]);
    assert!(listed.contains("<Key>team</Key>"), "{listed}");
    assert!(listed.contains("<Key>env</Key>"), "{listed}");
    assert!(listed.contains("<ResourceType>integration</ResourceType>"));

    let described = ok(&svc, "DescribeIntegrations", &[("IntegrationArn", &arn)]);
    assert!(described.contains("<Key>env</Key>"), "{described}");
    assert!(described.contains("<key>purpose</key>"), "{described}");

    ok(
        &svc,
        "DeleteTags",
        &[("ResourceName", &arn), ("TagKeys.TagKey.1", "team")],
    );
    let listed = ok(&svc, "DescribeTags", &[("ResourceName", &arn)]);
    assert!(!listed.contains("<Key>team</Key>"), "{listed}");
}

/// CreateRedshiftIdcApplication dropped Tags, ApplicationType,
/// AuthorizedTokenIssuerList, ServiceIntegrations and SsoTagKeys.
#[test]
fn idc_application_create_fields_round_trip() {
    let svc = service();
    ok(
        &svc,
        "CreateRedshiftIdcApplication",
        &[
            (
                "IdcInstanceArn",
                "arn:aws:sso:::instance/ssoins-1111111111111111",
            ),
            ("RedshiftIdcApplicationName", "idcapp"),
            ("IdcDisplayName", "Idc App"),
            ("IamRoleArn", "arn:aws:iam::123456789012:role/idc"),
            ("ApplicationType", "Lakehouse"),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "bi"),
            ("SsoTagKeys.TagKey.1", "dept"),
            (
                "AuthorizedTokenIssuerList.member.1.TrustedTokenIssuerArn",
                "arn:aws:sso::123456789012:trustedTokenIssuer/ssoins-1/tti-1",
            ),
            (
                "AuthorizedTokenIssuerList.member.1.AuthorizedAudiencesList.member.1",
                "aud-a",
            ),
            (
                "ServiceIntegrations.member.1.LakeFormation.member.1.LakeFormationQuery.Authorization",
                "ENABLED",
            ),
        ],
    );
    let listed = ok(&svc, "DescribeRedshiftIdcApplications", &[]);
    assert!(
        listed.contains("<ApplicationType>Lakehouse</ApplicationType>"),
        "{listed}"
    );
    assert!(
        listed.contains("<Tags><Tag><Key>team</Key><Value>bi</Value></Tag></Tags>"),
        "{listed}"
    );
    assert!(
        listed.contains("<SsoTagKeys><TagKey>dept</TagKey></SsoTagKeys>"),
        "{listed}"
    );
    assert!(
        listed.contains(
            "<AuthorizedTokenIssuerList><member><TrustedTokenIssuerArn>\
             arn:aws:sso::123456789012:trustedTokenIssuer/ssoins-1/tti-1\
             </TrustedTokenIssuerArn><AuthorizedAudiencesList><member>aud-a</member>\
             </AuthorizedAudiencesList></member></AuthorizedTokenIssuerList>"
        ),
        "{listed}"
    );
    assert!(
        listed.contains(
            "<ServiceIntegrations><member><LakeFormation><member><LakeFormationQuery>\
             <Authorization>ENABLED</Authorization></LakeFormationQuery></member>\
             </LakeFormation></member></ServiceIntegrations>"
        ),
        "{listed}"
    );

    // ModifyRedshiftIdcApplication replaces the issuer list.
    let start =
        listed.find("<RedshiftIdcApplicationArn>").unwrap() + "<RedshiftIdcApplicationArn>".len();
    let end = listed.find("</RedshiftIdcApplicationArn>").unwrap();
    let arn = listed[start..end].to_string();
    ok(
        &svc,
        "ModifyRedshiftIdcApplication",
        &[
            ("RedshiftIdcApplicationArn", &arn),
            (
                "AuthorizedTokenIssuerList.member.1.TrustedTokenIssuerArn",
                "arn:aws:sso::123456789012:trustedTokenIssuer/ssoins-1/tti-2",
            ),
        ],
    );
    let listed = ok(&svc, "DescribeRedshiftIdcApplications", &[]);
    assert!(
        listed.contains("tti-2") && !listed.contains("tti-1"),
        "{listed}"
    );
    // The other create-time fields survive a modify that does not name them.
    assert!(listed.contains("<Authorization>ENABLED</Authorization>"));

    // The application's ARN is taggable and listed by DescribeTags.
    ok(
        &svc,
        "CreateTags",
        &[
            ("ResourceName", &arn),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
        ],
    );
    let tags = ok(&svc, "DescribeTags", &[("ResourceName", &arn)]);
    assert!(tags.contains("<Key>team</Key>") && tags.contains("<Key>env</Key>"));
}

#[test]
fn modify_cluster_subnet_group_cannot_move_vpc() {
    let ec2: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let subnets = fakecloud_ec2::vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
    // A second VPC with one subnet.
    {
        let mut accounts = ec2.write();
        let state = accounts.regional_mut("123456789012", "us-east-1");
        let mut vpc = state.vpcs.values().next().unwrap().clone();
        vpc.vpc_id = "vpc-0other".into();
        vpc.is_default = false;
        state.vpcs.insert(vpc.vpc_id.clone(), vpc);
        let mut other = state.subnets[&subnets[0].subnet_id].clone();
        other.subnet_id = "subnet-0other".into();
        other.vpc_id = "vpc-0other".into();
        state.subnets.insert(other.subnet_id.clone(), other);
    }
    let svc = service().with_ec2_state(ec2);
    ok(
        &svc,
        "CreateClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "g"),
            ("Description", "first"),
            ("SubnetIds.member.1", &subnets[0].subnet_id),
        ],
    );
    let err = svc
        .dispatch(&req(
            "ModifyClusterSubnetGroup",
            &[
                ("ClusterSubnetGroupName", "g"),
                ("Description", "second"),
                ("SubnetIds.member.1", "subnet-0other"),
            ],
        ))
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidSubnet");
    let described = ok(
        &svc,
        "DescribeClusterSubnetGroups",
        &[("ClusterSubnetGroupName", "g")],
    );
    assert!(described.contains("<Description>first</Description>"));
    // Same-VPC changes still apply.
    ok(
        &svc,
        "ModifyClusterSubnetGroup",
        &[
            ("ClusterSubnetGroupName", "g"),
            ("SubnetIds.member.1", &subnets[1].subnet_id),
        ],
    );
}
