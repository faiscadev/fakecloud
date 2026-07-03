//! Model-derived input validation for Redshift's awsQuery surface.
//!
//! Every handler runs [`validate_request`] first. It enforces exactly the
//! constraints the Smithy model declares on each operation's *top-level* input
//! members — required-presence, enum membership, and string length — because
//! those are the constraints AWS itself rejects up front and the ones the
//! conformance probe's negative/boundary generators exercise. Patterns are
//! intentionally not enforced: AWS accepts many pattern-adjacent values and the
//! probe's positive generators don't respect `@pattern`, so enforcing it would
//! reject legitimate calls.
//!
//! The tables are keyed by `(operation, member)` (not member name alone)
//! because the same member name carries different constraints across ops
//! (e.g. `ClusterIdentifier` is unbounded on `CreateCluster` but `0..=63` on
//! `AddPartner`). Keying per-op keeps validation aligned with the exact shape
//! the probe fills for that operation.

use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// `(operation, [required member names])`.
const REQUIRED: &[(&str, &[&str])] = &[
    (
        "AcceptReservedNodeExchange",
        &["ReservedNodeId", "TargetReservedNodeOfferingId"],
    ),
    (
        "AddPartner",
        &[
            "AccountId",
            "ClusterIdentifier",
            "DatabaseName",
            "PartnerName",
        ],
    ),
    ("AssociateDataShareConsumer", &["DataShareArn"]),
    (
        "AuthorizeClusterSecurityGroupIngress",
        &["ClusterSecurityGroupName"],
    ),
    (
        "AuthorizeDataShare",
        &["DataShareArn", "ConsumerIdentifier"],
    ),
    ("AuthorizeEndpointAccess", &["Account"]),
    ("AuthorizeSnapshotAccess", &["AccountWithRestoreAccess"]),
    ("CancelResize", &["ClusterIdentifier"]),
    (
        "CopyClusterSnapshot",
        &["SourceSnapshotIdentifier", "TargetSnapshotIdentifier"],
    ),
    (
        "CreateAuthenticationProfile",
        &["AuthenticationProfileName", "AuthenticationProfileContent"],
    ),
    (
        "CreateCluster",
        &["ClusterIdentifier", "NodeType", "MasterUsername"],
    ),
    (
        "CreateClusterParameterGroup",
        &["ParameterGroupName", "ParameterGroupFamily", "Description"],
    ),
    (
        "CreateClusterSecurityGroup",
        &["ClusterSecurityGroupName", "Description"],
    ),
    (
        "CreateClusterSnapshot",
        &["SnapshotIdentifier", "ClusterIdentifier"],
    ),
    (
        "CreateClusterSubnetGroup",
        &["ClusterSubnetGroupName", "Description"],
    ),
    (
        "CreateCustomDomainAssociation",
        &[
            "CustomDomainName",
            "CustomDomainCertificateArn",
            "ClusterIdentifier",
        ],
    ),
    ("CreateEndpointAccess", &["EndpointName", "SubnetGroupName"]),
    (
        "CreateEventSubscription",
        &["SubscriptionName", "SnsTopicArn"],
    ),
    (
        "CreateHsmClientCertificate",
        &["HsmClientCertificateIdentifier"],
    ),
    (
        "CreateHsmConfiguration",
        &[
            "HsmConfigurationIdentifier",
            "Description",
            "HsmIpAddress",
            "HsmPartitionName",
            "HsmPartitionPassword",
            "HsmServerPublicCertificate",
        ],
    ),
    (
        "CreateIntegration",
        &["SourceArn", "TargetArn", "IntegrationName"],
    ),
    (
        "CreateRedshiftIdcApplication",
        &[
            "IdcInstanceArn",
            "RedshiftIdcApplicationName",
            "IdcDisplayName",
            "IamRoleArn",
        ],
    ),
    (
        "CreateScheduledAction",
        &["ScheduledActionName", "Schedule", "IamRole"],
    ),
    ("CreateSnapshotCopyGrant", &["SnapshotCopyGrantName"]),
    ("CreateTags", &["ResourceName"]),
    (
        "CreateUsageLimit",
        &["ClusterIdentifier", "FeatureType", "LimitType", "Amount"],
    ),
    (
        "DeauthorizeDataShare",
        &["DataShareArn", "ConsumerIdentifier"],
    ),
    (
        "DeleteAuthenticationProfile",
        &["AuthenticationProfileName"],
    ),
    ("DeleteCluster", &["ClusterIdentifier"]),
    ("DeleteClusterParameterGroup", &["ParameterGroupName"]),
    ("DeleteClusterSecurityGroup", &["ClusterSecurityGroupName"]),
    ("DeleteClusterSnapshot", &["SnapshotIdentifier"]),
    ("DeleteClusterSubnetGroup", &["ClusterSubnetGroupName"]),
    (
        "DeleteCustomDomainAssociation",
        &["ClusterIdentifier", "CustomDomainName"],
    ),
    ("DeleteEndpointAccess", &["EndpointName"]),
    ("DeleteEventSubscription", &["SubscriptionName"]),
    (
        "DeleteHsmClientCertificate",
        &["HsmClientCertificateIdentifier"],
    ),
    ("DeleteHsmConfiguration", &["HsmConfigurationIdentifier"]),
    ("DeleteIntegration", &["IntegrationArn"]),
    (
        "DeletePartner",
        &[
            "AccountId",
            "ClusterIdentifier",
            "DatabaseName",
            "PartnerName",
        ],
    ),
    (
        "DeleteRedshiftIdcApplication",
        &["RedshiftIdcApplicationArn"],
    ),
    ("DeleteResourcePolicy", &["ResourceArn"]),
    ("DeleteScheduledAction", &["ScheduledActionName"]),
    ("DeleteSnapshotCopyGrant", &["SnapshotCopyGrantName"]),
    ("DeleteSnapshotSchedule", &["ScheduleIdentifier"]),
    ("DeleteTags", &["ResourceName"]),
    ("DeleteUsageLimit", &["UsageLimitId"]),
    ("DescribeClusterParameters", &["ParameterGroupName"]),
    (
        "DescribeDefaultClusterParameters",
        &["ParameterGroupFamily"],
    ),
    ("DescribeLoggingStatus", &["ClusterIdentifier"]),
    ("DescribeNodeConfigurationOptions", &["ActionType"]),
    ("DescribePartners", &["AccountId", "ClusterIdentifier"]),
    ("DescribeResize", &["ClusterIdentifier"]),
    ("DisableLogging", &["ClusterIdentifier"]),
    ("DisableSnapshotCopy", &["ClusterIdentifier"]),
    ("DisassociateDataShareConsumer", &["DataShareArn"]),
    ("EnableLogging", &["ClusterIdentifier"]),
    (
        "EnableSnapshotCopy",
        &["ClusterIdentifier", "DestinationRegion"],
    ),
    ("FailoverPrimaryCompute", &["ClusterIdentifier"]),
    ("GetClusterCredentials", &["DbUser"]),
    (
        "GetReservedNodeExchangeConfigurationOptions",
        &["ActionType"],
    ),
    ("GetReservedNodeExchangeOfferings", &["ReservedNodeId"]),
    ("GetResourcePolicy", &["ResourceArn"]),
    ("ModifyAquaConfiguration", &["ClusterIdentifier"]),
    (
        "ModifyAuthenticationProfile",
        &["AuthenticationProfileName", "AuthenticationProfileContent"],
    ),
    ("ModifyCluster", &["ClusterIdentifier"]),
    (
        "ModifyClusterDbRevision",
        &["ClusterIdentifier", "RevisionTarget"],
    ),
    ("ModifyClusterIamRoles", &["ClusterIdentifier"]),
    ("ModifyClusterMaintenance", &["ClusterIdentifier"]),
    ("ModifyClusterParameterGroup", &["ParameterGroupName"]),
    ("ModifyClusterSnapshot", &["SnapshotIdentifier"]),
    ("ModifyClusterSnapshotSchedule", &["ClusterIdentifier"]),
    ("ModifyClusterSubnetGroup", &["ClusterSubnetGroupName"]),
    (
        "ModifyCustomDomainAssociation",
        &[
            "CustomDomainName",
            "CustomDomainCertificateArn",
            "ClusterIdentifier",
        ],
    ),
    ("ModifyEndpointAccess", &["EndpointName"]),
    ("ModifyEventSubscription", &["SubscriptionName"]),
    ("ModifyIntegration", &["IntegrationArn"]),
    ("ModifyLakehouseConfiguration", &["ClusterIdentifier"]),
    (
        "ModifyRedshiftIdcApplication",
        &["RedshiftIdcApplicationArn"],
    ),
    ("ModifyScheduledAction", &["ScheduledActionName"]),
    (
        "ModifySnapshotCopyRetentionPeriod",
        &["ClusterIdentifier", "RetentionPeriod"],
    ),
    ("ModifySnapshotSchedule", &["ScheduleIdentifier"]),
    ("ModifyUsageLimit", &["UsageLimitId"]),
    ("PauseCluster", &["ClusterIdentifier"]),
    ("PurchaseReservedNodeOffering", &["ReservedNodeOfferingId"]),
    ("PutResourcePolicy", &["ResourceArn", "Policy"]),
    ("RebootCluster", &["ClusterIdentifier"]),
    ("RejectDataShare", &["DataShareArn"]),
    ("ResetClusterParameterGroup", &["ParameterGroupName"]),
    ("ResizeCluster", &["ClusterIdentifier"]),
    ("RestoreFromClusterSnapshot", &["ClusterIdentifier"]),
    (
        "RestoreTableFromClusterSnapshot",
        &[
            "ClusterIdentifier",
            "SnapshotIdentifier",
            "SourceDatabaseName",
            "SourceTableName",
            "NewTableName",
        ],
    ),
    ("ResumeCluster", &["ClusterIdentifier"]),
    (
        "RevokeClusterSecurityGroupIngress",
        &["ClusterSecurityGroupName"],
    ),
    ("RevokeSnapshotAccess", &["AccountWithRestoreAccess"]),
    ("RotateEncryptionKey", &["ClusterIdentifier"]),
    (
        "UpdatePartnerStatus",
        &[
            "AccountId",
            "ClusterIdentifier",
            "DatabaseName",
            "PartnerName",
            "Status",
        ],
    ),
];

/// `(operation, member, [allowed enum values])`.
const ENUMS: &[(&str, &str, &[&str])] = &[
    (
        "CreateCluster",
        "AquaConfigurationStatus",
        &["enabled", "disabled", "auto"],
    ),
    (
        "CreateRedshiftIdcApplication",
        "ApplicationType",
        &["None", "Lakehouse"],
    ),
    (
        "CreateUsageLimit",
        "FeatureType",
        &[
            "spectrum",
            "concurrency-scaling",
            "cross-region-datasharing",
            "extra-compute-for-automatic-optimization",
        ],
    ),
    ("CreateUsageLimit", "LimitType", &["time", "data-scanned"]),
    (
        "CreateUsageLimit",
        "Period",
        &["daily", "weekly", "monthly"],
    ),
    (
        "CreateUsageLimit",
        "BreachAction",
        &["log", "emit-metric", "disable"],
    ),
    (
        "DescribeDataSharesForConsumer",
        "Status",
        &["ACTIVE", "AVAILABLE"],
    ),
    (
        "DescribeDataSharesForProducer",
        "Status",
        &[
            "ACTIVE",
            "AUTHORIZED",
            "PENDING_AUTHORIZATION",
            "DEAUTHORIZED",
            "REJECTED",
        ],
    ),
    (
        "DescribeEvents",
        "SourceType",
        &[
            "cluster",
            "cluster-parameter-group",
            "cluster-security-group",
            "cluster-snapshot",
            "scheduled-action",
        ],
    ),
    (
        "DescribeNodeConfigurationOptions",
        "ActionType",
        &["restore-cluster", "recommend-node-config", "resize-cluster"],
    ),
    (
        "DescribeScheduledActions",
        "TargetActionType",
        &["ResizeCluster", "PauseCluster", "ResumeCluster"],
    ),
    (
        "DescribeUsageLimits",
        "FeatureType",
        &[
            "spectrum",
            "concurrency-scaling",
            "cross-region-datasharing",
            "extra-compute-for-automatic-optimization",
        ],
    ),
    ("EnableLogging", "LogDestinationType", &["s3", "cloudwatch"]),
    (
        "GetReservedNodeExchangeConfigurationOptions",
        "ActionType",
        &["restore-cluster", "resize-cluster"],
    ),
    (
        "ModifyAquaConfiguration",
        "AquaConfigurationStatus",
        &["enabled", "disabled", "auto"],
    ),
    (
        "ModifyLakehouseConfiguration",
        "LakehouseRegistration",
        &["Register", "Deregister"],
    ),
    (
        "ModifyLakehouseConfiguration",
        "LakehouseIdcRegistration",
        &["Associate", "Disassociate"],
    ),
    (
        "ModifyUsageLimit",
        "BreachAction",
        &["log", "emit-metric", "disable"],
    ),
    (
        "RestoreFromClusterSnapshot",
        "AquaConfigurationStatus",
        &["enabled", "disabled", "auto"],
    ),
    (
        "UpdatePartnerStatus",
        "Status",
        &["Active", "Inactive", "RuntimeFailure", "ConnectionFailure"],
    ),
];

/// `(operation, member, min_len, max_len)` for string members with real bounds.
const LENGTHS: &[(&str, &str, usize, usize)] = &[
    ("AddPartner", "AccountId", 12, 12),
    ("AddPartner", "ClusterIdentifier", 0, 63),
    ("AddPartner", "DatabaseName", 0, 127),
    ("AddPartner", "PartnerName", 0, 255),
    (
        "CreateAuthenticationProfile",
        "AuthenticationProfileName",
        0,
        63,
    ),
    ("CreateCluster", "CatalogName", 1, 64),
    ("CreateCustomDomainAssociation", "CustomDomainName", 1, 253),
    (
        "CreateCustomDomainAssociation",
        "CustomDomainCertificateArn",
        20,
        2048,
    ),
    ("CreateIntegration", "SourceArn", 1, 255),
    ("CreateIntegration", "TargetArn", 20, 2048),
    ("CreateIntegration", "IntegrationName", 1, 63),
    ("CreateIntegration", "Description", 0, 1000),
    (
        "CreateRedshiftIdcApplication",
        "RedshiftIdcApplicationName",
        1,
        63,
    ),
    ("CreateRedshiftIdcApplication", "IdentityNamespace", 1, 127),
    ("CreateRedshiftIdcApplication", "IdcDisplayName", 1, 127),
    (
        "DeleteAuthenticationProfile",
        "AuthenticationProfileName",
        0,
        63,
    ),
    ("DeleteCustomDomainAssociation", "CustomDomainName", 1, 253),
    ("DeleteIntegration", "IntegrationArn", 1, 255),
    ("DeletePartner", "AccountId", 12, 12),
    ("DeletePartner", "ClusterIdentifier", 0, 63),
    ("DeletePartner", "DatabaseName", 0, 127),
    ("DeletePartner", "PartnerName", 0, 255),
    (
        "DescribeAuthenticationProfiles",
        "AuthenticationProfileName",
        0,
        63,
    ),
    (
        "DescribeCustomDomainAssociations",
        "CustomDomainName",
        1,
        253,
    ),
    (
        "DescribeCustomDomainAssociations",
        "CustomDomainCertificateArn",
        20,
        2048,
    ),
    ("DescribeInboundIntegrations", "IntegrationArn", 1, 255),
    ("DescribeInboundIntegrations", "TargetArn", 20, 2048),
    ("DescribeIntegrations", "IntegrationArn", 1, 255),
    ("DescribePartners", "AccountId", 12, 12),
    ("DescribePartners", "ClusterIdentifier", 0, 63),
    ("DescribePartners", "DatabaseName", 0, 127),
    ("DescribePartners", "PartnerName", 0, 255),
    ("EnableLogging", "S3KeyPrefix", 0, 256),
    (
        "ModifyAuthenticationProfile",
        "AuthenticationProfileName",
        0,
        63,
    ),
    ("ModifyCustomDomainAssociation", "CustomDomainName", 1, 253),
    (
        "ModifyCustomDomainAssociation",
        "CustomDomainCertificateArn",
        20,
        2048,
    ),
    ("ModifyIntegration", "IntegrationArn", 1, 255),
    ("ModifyIntegration", "Description", 0, 1000),
    ("ModifyIntegration", "IntegrationName", 1, 63),
    ("ModifyLakehouseConfiguration", "CatalogName", 1, 64),
    ("ModifyRedshiftIdcApplication", "IdentityNamespace", 1, 127),
    ("ModifyRedshiftIdcApplication", "IdcDisplayName", 1, 127),
    ("RestoreFromClusterSnapshot", "CatalogName", 1, 64),
    ("UpdatePartnerStatus", "AccountId", 12, 12),
    ("UpdatePartnerStatus", "ClusterIdentifier", 0, 63),
    ("UpdatePartnerStatus", "DatabaseName", 0, 127),
    ("UpdatePartnerStatus", "PartnerName", 0, 255),
];

fn missing_parameter(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}."),
    )
}

fn invalid_parameter_value(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterValue", msg.into())
}

/// Does the request carry the member `name` in any wire form — scalar
/// (`name=…`), list (`name.member.1=…`), or nested struct (`name.Field=…`)?
///
/// A present-but-empty scalar (`name=`) counts as provided: AWS accepts an
/// empty string for members whose `@length` lower bound is 0, and the
/// conformance probe's `boundary_len_min_*_0` variants send exactly that. Only
/// a key entirely absent from the wire is "missing".
fn present(req: &AwsRequest, name: &str) -> bool {
    let dotted = format!("{name}.");
    req.query_params
        .keys()
        .any(|k| k == name || k.starts_with(&dotted))
}

/// Enforce required-presence, enum membership, and string-length bounds for the
/// given operation's top-level input members. Called at the top of every
/// handler.
pub(crate) fn validate_request(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    if let Some((_, members)) = REQUIRED.iter().find(|(op, _)| *op == action) {
        for m in *members {
            if !present(req, m) {
                return Err(missing_parameter(m));
            }
        }
    }
    for (op, member, allowed) in ENUMS {
        if *op != action {
            continue;
        }
        if let Some(v) = req.query_params.get(*member) {
            if !v.is_empty() && !allowed.contains(&v.as_str()) {
                return Err(invalid_parameter_value(format!(
                    "Invalid value '{v}' for {member}."
                )));
            }
        }
    }
    for (op, member, min, max) in LENGTHS {
        if *op != action {
            continue;
        }
        if let Some(v) = req.query_params.get(*member) {
            let len = v.chars().count();
            if len < *min || len > *max {
                return Err(invalid_parameter_value(format!(
                    "Length of value for {member} must be between {min} and {max}."
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;

    fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut query_params = HashMap::new();
        for (k, v) in params {
            query_params.insert((*k).to_string(), (*v).to_string());
        }
        AwsRequest {
            service: "redshift".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "r".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn missing_required_field_rejected() {
        let e = validate_request("DeleteCluster", &req("DeleteCluster", &[])).unwrap_err();
        assert_eq!(e.code(), "MissingParameter");
    }

    #[test]
    fn present_required_field_accepted() {
        assert!(validate_request(
            "DeleteCluster",
            &req("DeleteCluster", &[("ClusterIdentifier", "c1")])
        )
        .is_ok());
    }

    #[test]
    fn list_member_counts_as_present() {
        let r = req(
            "CreateClusterSubnetGroup",
            &[
                ("ClusterSubnetGroupName", "g"),
                ("Description", "d"),
                ("SubnetIds.member.1", "subnet-1"),
            ],
        );
        assert!(validate_request("CreateClusterSubnetGroup", &r).is_ok());
    }

    #[test]
    fn invalid_enum_rejected() {
        let r = req(
            "CreateUsageLimit",
            &[
                ("ClusterIdentifier", "c1"),
                ("FeatureType", "NOT_A_FEATURE"),
                ("LimitType", "TIME"),
                ("Amount", "10"),
            ],
        );
        let e = validate_request("CreateUsageLimit", &r).unwrap_err();
        assert_eq!(e.code(), "InvalidParameterValue");
    }

    #[test]
    fn valid_enum_accepted() {
        let r = req(
            "CreateUsageLimit",
            &[
                ("ClusterIdentifier", "c1"),
                ("FeatureType", "spectrum"),
                ("LimitType", "time"),
                ("Amount", "10"),
            ],
        );
        assert!(validate_request("CreateUsageLimit", &r).is_ok());
    }

    #[test]
    fn too_long_string_rejected() {
        let long = "a".repeat(64);
        let r = req(
            "AddPartner",
            &[
                ("AccountId", "123456789012"),
                ("ClusterIdentifier", long.as_str()),
                ("DatabaseName", "db"),
                ("PartnerName", "p"),
            ],
        );
        let e = validate_request("AddPartner", &r).unwrap_err();
        assert_eq!(e.code(), "InvalidParameterValue");
    }

    #[test]
    fn account_id_wrong_length_rejected() {
        let r = req(
            "AddPartner",
            &[
                ("AccountId", "123"),
                ("ClusterIdentifier", "c1"),
                ("DatabaseName", "db"),
                ("PartnerName", "p"),
            ],
        );
        assert_eq!(
            validate_request("AddPartner", &r).unwrap_err().code(),
            "InvalidParameterValue"
        );
    }
}
