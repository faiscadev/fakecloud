use super::*;

/// Actions that do NOT mutate SSM state. Everything else triggers a
/// snapshot save on HTTP 2xx. Listing non-mutating actions (rather than
/// mutating ones) is safer here because SSM has ~150 actions and the
/// read-only set is small and stable.
pub(crate) fn is_read_only_action(action: &str) -> bool {
    matches!(
        action,
        "GetParameter"
            | "GetParameters"
            | "GetParametersByPath"
            | "DescribeParameters"
            | "GetParameterHistory"
            | "ListTagsForResource"
            | "GetDocument"
            | "DescribeDocument"
            | "ListDocuments"
            | "DescribeDocumentPermission"
            | "ListCommands"
            | "GetCommandInvocation"
            | "ListCommandInvocations"
            | "DescribeMaintenanceWindows"
            | "GetMaintenanceWindow"
            | "DescribeMaintenanceWindowTargets"
            | "DescribeMaintenanceWindowTasks"
            | "DescribePatchBaselines"
            | "GetPatchBaseline"
            | "GetPatchBaselineForPatchGroup"
            | "DescribePatchGroups"
            | "DescribeAssociation"
            | "ListAssociations"
            | "ListAssociationVersions"
            | "DescribeAssociationExecutions"
            | "DescribeAssociationExecutionTargets"
            | "GetOpsItem"
            | "DescribeOpsItems"
            | "ListDocumentVersions"
            | "ListDocumentMetadataHistory"
            | "GetResourcePolicies"
            | "GetInventory"
            | "GetInventorySchema"
            | "ListInventoryEntries"
            | "DescribeInventoryDeletions"
            | "ListComplianceItems"
            | "ListComplianceSummaries"
            | "ListResourceComplianceSummaries"
            | "GetMaintenanceWindowTask"
            | "GetMaintenanceWindowExecution"
            | "GetMaintenanceWindowExecutionTask"
            | "GetMaintenanceWindowExecutionTaskInvocation"
            | "DescribeMaintenanceWindowExecutions"
            | "DescribeMaintenanceWindowExecutionTasks"
            | "DescribeMaintenanceWindowExecutionTaskInvocations"
            | "DescribeMaintenanceWindowSchedule"
            | "DescribeMaintenanceWindowsForTarget"
            | "DescribeInstancePatchStates"
            | "DescribeInstancePatchStatesForPatchGroup"
            | "DescribeInstancePatches"
            | "DescribeEffectivePatchesForPatchBaseline"
            | "GetDeployablePatchSnapshotForInstance"
            | "ListResourceDataSync"
            | "ListOpsItemRelatedItems"
            | "ListOpsItemEvents"
            | "GetOpsMetadata"
            | "ListOpsMetadata"
            | "GetOpsSummary"
            | "GetAutomationExecution"
            | "DescribeAutomationExecutions"
            | "DescribeAutomationStepExecutions"
            | "GetExecutionPreview"
            | "DescribeSessions"
            | "GetAccessToken"
            | "DescribeActivations"
            | "DescribeInstanceInformation"
            | "DescribeInstanceProperties"
            | "ListNodes"
            | "ListNodesSummary"
            | "DescribeEffectiveInstanceAssociations"
            | "DescribeInstanceAssociationsStatus"
            | "GetConnectionStatus"
            | "GetCalendarState"
            | "DescribePatchGroupState"
            | "DescribePatchProperties"
            | "DescribeAvailablePatches"
            | "GetDefaultPatchBaseline"
            | "GetServiceSetting"
            | "GetCloudConnector"
            | "ListCloudConnectors"
            | "ValidateCloudConnector"
    )
}

pub(crate) fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

/// Variant of `missing` that emits a specific Smithy-declared wire code.
/// Use when the op's `errors` list does not include `ValidationException`
/// so the strict conformance probe would otherwise reject the response.
pub(crate) fn missing_with_code(name: &str, code: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        code,
        format!("The request must contain the parameter {name}"),
    )
}

/// Build a 400 with an arbitrary code + message. Used by operation
/// handlers that need to emit a wire code declared on that specific op
/// (e.g. `InvalidFilterValue`, `InvalidCommandId`, `InvalidDocument`)
/// instead of the generic `ValidationException`.
pub(crate) fn aws_400(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

/// Reject a `NextToken` that is present but does not parse as a valid offset.
/// SSM's paginated list/describe ops declare `InvalidNextToken` in their
/// Smithy model, so a malformed token must surface that wire code rather than
/// being silently treated as the first page (which can drive an infinite
/// client pagination loop). bug-audit 2026-05-28, 1.7.
pub(crate) fn invalid_next_token() -> AwsServiceError {
    aws_400("InvalidNextToken", "The specified token is not valid.")
}

/// Many SSM operations declare specific `Invalid*` errors in their Smithy
/// model but the shared `validate_*` helpers in `fakecloud-core` emit the
/// generic `ValidationException` (which AWS itself returns at runtime but
/// most ops do *not* list in their `errors`). Strict-mode probes reject
/// the generic code, so handlers can wrap their input-parsing result in
/// `remap_validation_to(err, "InvalidCommandId")` to flip the wire code
/// while preserving the human-readable message. Errors with any other
/// code pass through unchanged.
pub(crate) fn remap_validation_to(err: AwsServiceError, target: &str) -> AwsServiceError {
    if err.code() == "ValidationException" {
        AwsServiceError::aws_error(StatusCode::BAD_REQUEST, target, err.message())
    } else {
        err
    }
}
