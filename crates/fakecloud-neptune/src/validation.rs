//! Smithy-derived required-parameter validation, run before each handler.
//!
//! Returns the Query-framework `MissingParameter` fault (HTTP 400) when a
//! modelled `@required` member is absent from the form-encoded request —
//! matching what the real Neptune endpoint returns and satisfying the
//! conformance `negative_omit_*` variants (which accept any error).

use http::StatusCode;

use fakecloud_core::query::optional_query_param;
use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// The `@required` scalar members for each operation, keyed by the wire
/// parameter name the AWS SDK sends. `@required` list members are omitted
/// here (they are checked structurally by the handlers) so that the
/// positive variants — which send the indexed `.member.N` form rather than
/// a scalar key — are not spuriously rejected.
fn required_params(action: &str) -> &'static [&'static str] {
    match action {
        "AddRoleToDBCluster" => &["DBClusterIdentifier", "RoleArn"],
        "AddSourceIdentifierToSubscription" => &["SubscriptionName", "SourceIdentifier"],
        "AddTagsToResource" => &["ResourceName"],
        "ApplyPendingMaintenanceAction" => &["ResourceIdentifier", "ApplyAction", "OptInType"],
        "CopyDBClusterParameterGroup" => &[
            "SourceDBClusterParameterGroupIdentifier",
            "TargetDBClusterParameterGroupIdentifier",
            "TargetDBClusterParameterGroupDescription",
        ],
        "CopyDBClusterSnapshot" => &[
            "SourceDBClusterSnapshotIdentifier",
            "TargetDBClusterSnapshotIdentifier",
        ],
        "CopyDBParameterGroup" => &[
            "SourceDBParameterGroupIdentifier",
            "TargetDBParameterGroupIdentifier",
            "TargetDBParameterGroupDescription",
        ],
        "CreateDBCluster" => &["DBClusterIdentifier", "Engine"],
        "CreateDBClusterEndpoint" => &[
            "DBClusterIdentifier",
            "DBClusterEndpointIdentifier",
            "EndpointType",
        ],
        "CreateDBClusterParameterGroup" => &[
            "DBClusterParameterGroupName",
            "DBParameterGroupFamily",
            "Description",
        ],
        "CreateDBClusterSnapshot" => &["DBClusterSnapshotIdentifier", "DBClusterIdentifier"],
        "CreateDBInstance" => &[
            "DBInstanceIdentifier",
            "DBInstanceClass",
            "Engine",
            "DBClusterIdentifier",
        ],
        "CreateDBParameterGroup" => &[
            "DBParameterGroupName",
            "DBParameterGroupFamily",
            "Description",
        ],
        "CreateDBSubnetGroup" => &["DBSubnetGroupName", "DBSubnetGroupDescription"],
        "CreateEventSubscription" => &["SubscriptionName", "SnsTopicArn"],
        "CreateGlobalCluster" => &["GlobalClusterIdentifier"],
        "DeleteDBCluster" => &["DBClusterIdentifier"],
        "DeleteDBClusterEndpoint" => &["DBClusterEndpointIdentifier"],
        "DeleteDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
        "DeleteDBClusterSnapshot" => &["DBClusterSnapshotIdentifier"],
        "DeleteDBInstance" => &["DBInstanceIdentifier"],
        "DeleteDBParameterGroup" => &["DBParameterGroupName"],
        "DeleteDBSubnetGroup" => &["DBSubnetGroupName"],
        "DeleteEventSubscription" => &["SubscriptionName"],
        "DeleteGlobalCluster" => &["GlobalClusterIdentifier"],
        "DescribeDBClusterParameters" => &["DBClusterParameterGroupName"],
        "DescribeDBClusterSnapshotAttributes" => &["DBClusterSnapshotIdentifier"],
        "DescribeDBParameters" => &["DBParameterGroupName"],
        "DescribeEngineDefaultClusterParameters" => &["DBParameterGroupFamily"],
        "DescribeEngineDefaultParameters" => &["DBParameterGroupFamily"],
        "DescribeOrderableDBInstanceOptions" => &["Engine"],
        "DescribeValidDBInstanceModifications" => &["DBInstanceIdentifier"],
        "FailoverGlobalCluster" => &["GlobalClusterIdentifier", "TargetDbClusterIdentifier"],
        "ListTagsForResource" => &["ResourceName"],
        "ModifyDBCluster" => &["DBClusterIdentifier"],
        "ModifyDBClusterEndpoint" => &["DBClusterEndpointIdentifier"],
        "ModifyDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
        "ModifyDBClusterSnapshotAttribute" => &["DBClusterSnapshotIdentifier", "AttributeName"],
        "ModifyDBInstance" => &["DBInstanceIdentifier"],
        "ModifyDBParameterGroup" => &["DBParameterGroupName"],
        "ModifyDBSubnetGroup" => &["DBSubnetGroupName"],
        "ModifyEventSubscription" => &["SubscriptionName"],
        "ModifyGlobalCluster" => &["GlobalClusterIdentifier"],
        "PromoteReadReplicaDBCluster" => &["DBClusterIdentifier"],
        "RebootDBInstance" => &["DBInstanceIdentifier"],
        "RemoveFromGlobalCluster" => &["GlobalClusterIdentifier", "DbClusterIdentifier"],
        "RemoveRoleFromDBCluster" => &["DBClusterIdentifier", "RoleArn"],
        "RemoveSourceIdentifierFromSubscription" => &["SubscriptionName", "SourceIdentifier"],
        "RemoveTagsFromResource" => &["ResourceName"],
        "ResetDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
        "ResetDBParameterGroup" => &["DBParameterGroupName"],
        "RestoreDBClusterFromSnapshot" => &["DBClusterIdentifier", "SnapshotIdentifier", "Engine"],
        "RestoreDBClusterToPointInTime" => &["DBClusterIdentifier", "SourceDBClusterIdentifier"],
        "StartDBCluster" => &["DBClusterIdentifier"],
        "StopDBCluster" => &["DBClusterIdentifier"],
        "SwitchoverGlobalCluster" => &["GlobalClusterIdentifier", "TargetDbClusterIdentifier"],
        _ => &[],
    }
}

/// Return a `MissingParameter` fault for the first absent required member.
pub(crate) fn prevalidate(action: &str, request: &AwsRequest) -> Result<(), AwsServiceError> {
    for name in required_params(action) {
        if optional_query_param(request, name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                format!("The request must contain the parameter {name}."),
            ));
        }
    }
    Ok(())
}
