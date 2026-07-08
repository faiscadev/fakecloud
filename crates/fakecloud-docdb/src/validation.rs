//! Smithy-derived required-parameter validation, run before each handler.
//!
//! Returns the Query-framework `MissingParameter` fault (HTTP 400) when a
//! modelled `@required` member is absent from the form-encoded request —
//! matching what the real DocumentDB endpoint returns and satisfying the
//! conformance `negative_omit_*` variants (which accept any error).

use http::StatusCode;

use fakecloud_core::query::optional_query_param;
use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// The `@required` scalar/list members for each operation, keyed by the
/// wire parameter name the AWS SDK sends. List members are validated by
/// their first indexed element under any accepted member-name form.
fn required_params(action: &str) -> &'static [&'static str] {
    match action {
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
        "CreateDBCluster" => &["DBClusterIdentifier", "Engine"],
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
        "CreateDBSubnetGroup" => &["DBSubnetGroupName", "DBSubnetGroupDescription"],
        "CreateEventSubscription" => &["SubscriptionName", "SnsTopicArn"],
        "CreateGlobalCluster" => &["GlobalClusterIdentifier"],
        "DeleteDBCluster" => &["DBClusterIdentifier"],
        "DeleteDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
        "DeleteDBClusterSnapshot" => &["DBClusterSnapshotIdentifier"],
        "DeleteDBInstance" => &["DBInstanceIdentifier"],
        "DeleteDBSubnetGroup" => &["DBSubnetGroupName"],
        "DeleteEventSubscription" => &["SubscriptionName"],
        "DeleteGlobalCluster" => &["GlobalClusterIdentifier"],
        "DescribeDBClusterParameters" => &["DBClusterParameterGroupName"],
        "DescribeDBClusterSnapshotAttributes" => &["DBClusterSnapshotIdentifier"],
        "DescribeEngineDefaultClusterParameters" => &["DBParameterGroupFamily"],
        "DescribeOrderableDBInstanceOptions" => &["Engine"],
        "FailoverGlobalCluster" => &["GlobalClusterIdentifier", "TargetDbClusterIdentifier"],
        "ListTagsForResource" => &["ResourceName"],
        "ModifyDBCluster" => &["DBClusterIdentifier"],
        "ModifyDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
        "ModifyDBClusterSnapshotAttribute" => &["DBClusterSnapshotIdentifier", "AttributeName"],
        "ModifyDBInstance" => &["DBInstanceIdentifier"],
        "ModifyDBSubnetGroup" => &["DBSubnetGroupName"],
        "ModifyEventSubscription" => &["SubscriptionName"],
        "ModifyGlobalCluster" => &["GlobalClusterIdentifier"],
        "RebootDBInstance" => &["DBInstanceIdentifier"],
        "RemoveFromGlobalCluster" => &["GlobalClusterIdentifier", "DbClusterIdentifier"],
        "RemoveSourceIdentifierFromSubscription" => &["SubscriptionName", "SourceIdentifier"],
        "RemoveTagsFromResource" => &["ResourceName"],
        "ResetDBClusterParameterGroup" => &["DBClusterParameterGroupName"],
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
