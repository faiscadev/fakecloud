//! SSO Admin (`sso-admin`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 79-operation control plane. State is account-partitioned and
//! persisted. Nested configuration objects (PortalOptions, Grant,
//! TrustedTokenIssuerConfiguration, ...) are stored as the raw request `Value`
//! so they round-trip verbatim.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    SharedSsoAdminState, StoredApplication, StoredAssignment, StoredInstance, StoredManagedPolicy,
    StoredOpStatus, StoredPermissionSet, StoredProvisioningStatus, StoredRegion,
    StoredTrustedTokenIssuer,
};

/// Every operation name in the SSO Admin Smithy model.
pub const SSOADMIN_ACTIONS: &[&str] = &[
    "AddRegion",
    "AttachCustomerManagedPolicyReferenceToPermissionSet",
    "AttachManagedPolicyToPermissionSet",
    "CreateAccountAssignment",
    "CreateApplication",
    "CreateApplicationAssignment",
    "CreateInstance",
    "CreateInstanceAccessControlAttributeConfiguration",
    "CreatePermissionSet",
    "CreateTrustedTokenIssuer",
    "DeleteAccountAssignment",
    "DeleteApplication",
    "DeleteApplicationAccessScope",
    "DeleteApplicationAssignment",
    "DeleteApplicationAuthenticationMethod",
    "DeleteApplicationGrant",
    "DeleteInlinePolicyFromPermissionSet",
    "DeleteInstance",
    "DeleteInstanceAccessControlAttributeConfiguration",
    "DeletePermissionSet",
    "DeletePermissionsBoundaryFromPermissionSet",
    "DeleteTrustedTokenIssuer",
    "DescribeAccountAssignmentCreationStatus",
    "DescribeAccountAssignmentDeletionStatus",
    "DescribeApplication",
    "DescribeApplicationAssignment",
    "DescribeApplicationProvider",
    "DescribeInstance",
    "DescribeInstanceAccessControlAttributeConfiguration",
    "DescribePermissionSet",
    "DescribePermissionSetProvisioningStatus",
    "DescribeRegion",
    "DescribeTrustedTokenIssuer",
    "DetachCustomerManagedPolicyReferenceFromPermissionSet",
    "DetachManagedPolicyFromPermissionSet",
    "GetApplicationAccessScope",
    "GetApplicationAssignmentConfiguration",
    "GetApplicationAuthenticationMethod",
    "GetApplicationGrant",
    "GetApplicationSessionConfiguration",
    "GetInlinePolicyForPermissionSet",
    "GetPermissionsBoundaryForPermissionSet",
    "ListAccountAssignmentCreationStatus",
    "ListAccountAssignmentDeletionStatus",
    "ListAccountAssignments",
    "ListAccountAssignmentsForPrincipal",
    "ListAccountsForProvisionedPermissionSet",
    "ListApplicationAccessScopes",
    "ListApplicationAssignments",
    "ListApplicationAssignmentsForPrincipal",
    "ListApplicationAuthenticationMethods",
    "ListApplicationGrants",
    "ListApplicationProviders",
    "ListApplications",
    "ListCustomerManagedPolicyReferencesInPermissionSet",
    "ListInstances",
    "ListManagedPoliciesInPermissionSet",
    "ListPermissionSetProvisioningStatus",
    "ListPermissionSets",
    "ListPermissionSetsProvisionedToAccount",
    "ListRegions",
    "ListTagsForResource",
    "ListTrustedTokenIssuers",
    "ProvisionPermissionSet",
    "PutApplicationAccessScope",
    "PutApplicationAssignmentConfiguration",
    "PutApplicationAuthenticationMethod",
    "PutApplicationGrant",
    "PutApplicationSessionConfiguration",
    "PutInlinePolicyToPermissionSet",
    "PutPermissionsBoundaryToPermissionSet",
    "RemoveRegion",
    "TagResource",
    "UntagResource",
    "UpdateApplication",
    "UpdateInstance",
    "UpdateInstanceAccessControlAttributeConfiguration",
    "UpdatePermissionSet",
    "UpdateTrustedTokenIssuer",
];

/// The fixed AWS application-provider catalogue. IAM Identity Center exposes a
/// closed set of managed application providers; we surface a representative
/// subset with their real ARNs and federation protocol.
const APPLICATION_PROVIDERS: &[(&str, &str, &str)] = &[
    (
        "arn:aws:sso::aws:applicationProvider/custom",
        "OAUTH",
        "Custom application",
    ),
    (
        "arn:aws:sso::aws:applicationProvider/sso",
        "SAML",
        "IAM Identity Center",
    ),
    (
        "arn:aws:sso::aws:applicationProvider/salesforce",
        "SAML",
        "Salesforce",
    ),
    ("arn:aws:sso::aws:applicationProvider/box", "SAML", "Box"),
    (
        "arn:aws:sso::aws:applicationProvider/slack",
        "SAML",
        "Slack",
    ),
    (
        "arn:aws:sso::aws:applicationProvider/google",
        "SAML",
        "Google Workspace",
    ),
    (
        "arn:aws:sso::aws:applicationProvider/microsoft365",
        "SAML",
        "Microsoft 365",
    ),
];

pub struct SsoAdminService {
    state: SharedSsoAdminState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl SsoAdminService {
    pub fn new(state: SharedSsoAdminState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

#[async_trait]
impl AwsService for SsoAdminService {
    fn service_name(&self) -> &str {
        "sso"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SSOADMIN_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "Create",
        "Update",
        "Delete",
        "Put",
        "Attach",
        "Detach",
        "Add",
        "Remove",
        "Tag",
        "Untag",
        "Provision",
    ];
    PREFIXES.iter().any(|p| action.starts_with(p))
}

fn dispatch(s: &SsoAdminService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        // instances
        "CreateInstance" => s.create_instance(req),
        "DescribeInstance" => s.describe_instance(req),
        "UpdateInstance" => s.update_instance(req),
        "DeleteInstance" => s.delete_instance(req),
        "ListInstances" => s.list_instances(req),
        // permission sets
        "CreatePermissionSet" => s.create_permission_set(req),
        "DescribePermissionSet" => s.describe_permission_set(req),
        "UpdatePermissionSet" => s.update_permission_set(req),
        "DeletePermissionSet" => s.delete_permission_set(req),
        "ListPermissionSets" => s.list_permission_sets(req),
        // permission set policies
        "AttachManagedPolicyToPermissionSet" => s.attach_managed_policy(req),
        "DetachManagedPolicyFromPermissionSet" => s.detach_managed_policy(req),
        "ListManagedPoliciesInPermissionSet" => s.list_managed_policies(req),
        "AttachCustomerManagedPolicyReferenceToPermissionSet" => s.attach_customer_policy(req),
        "DetachCustomerManagedPolicyReferenceFromPermissionSet" => s.detach_customer_policy(req),
        "ListCustomerManagedPolicyReferencesInPermissionSet" => s.list_customer_policies(req),
        "PutInlinePolicyToPermissionSet" => s.put_inline_policy(req),
        "GetInlinePolicyForPermissionSet" => s.get_inline_policy(req),
        "DeleteInlinePolicyFromPermissionSet" => s.delete_inline_policy(req),
        "PutPermissionsBoundaryToPermissionSet" => s.put_permissions_boundary(req),
        "GetPermissionsBoundaryForPermissionSet" => s.get_permissions_boundary(req),
        "DeletePermissionsBoundaryFromPermissionSet" => s.delete_permissions_boundary(req),
        // account assignments
        "CreateAccountAssignment" => s.create_account_assignment(req),
        "DeleteAccountAssignment" => s.delete_account_assignment(req),
        "DescribeAccountAssignmentCreationStatus" => s.describe_aa_creation_status(req),
        "DescribeAccountAssignmentDeletionStatus" => s.describe_aa_deletion_status(req),
        "ListAccountAssignmentCreationStatus" => s.list_aa_creation_status(req),
        "ListAccountAssignmentDeletionStatus" => s.list_aa_deletion_status(req),
        "ListAccountAssignments" => s.list_account_assignments(req),
        "ListAccountAssignmentsForPrincipal" => s.list_account_assignments_for_principal(req),
        "ListAccountsForProvisionedPermissionSet" => s.list_accounts_for_provisioned(req),
        "ListPermissionSetsProvisionedToAccount" => s.list_ps_provisioned_to_account(req),
        // provisioning
        "ProvisionPermissionSet" => s.provision_permission_set(req),
        "DescribePermissionSetProvisioningStatus" => s.describe_ps_provisioning_status(req),
        "ListPermissionSetProvisioningStatus" => s.list_ps_provisioning_status(req),
        // applications
        "CreateApplication" => s.create_application(req),
        "DescribeApplication" => s.describe_application(req),
        "UpdateApplication" => s.update_application(req),
        "DeleteApplication" => s.delete_application(req),
        "ListApplications" => s.list_applications(req),
        // application assignments
        "CreateApplicationAssignment" => s.create_application_assignment(req),
        "DeleteApplicationAssignment" => s.delete_application_assignment(req),
        "DescribeApplicationAssignment" => s.describe_application_assignment(req),
        "ListApplicationAssignments" => s.list_application_assignments(req),
        "ListApplicationAssignmentsForPrincipal" => {
            s.list_application_assignments_for_principal(req)
        }
        "GetApplicationAssignmentConfiguration" => s.get_application_assignment_configuration(req),
        "PutApplicationAssignmentConfiguration" => s.put_application_assignment_configuration(req),
        // application access scopes
        "PutApplicationAccessScope" => s.put_application_access_scope(req),
        "GetApplicationAccessScope" => s.get_application_access_scope(req),
        "DeleteApplicationAccessScope" => s.delete_application_access_scope(req),
        "ListApplicationAccessScopes" => s.list_application_access_scopes(req),
        // application authentication methods
        "PutApplicationAuthenticationMethod" => s.put_application_authentication_method(req),
        "GetApplicationAuthenticationMethod" => s.get_application_authentication_method(req),
        "DeleteApplicationAuthenticationMethod" => s.delete_application_authentication_method(req),
        "ListApplicationAuthenticationMethods" => s.list_application_authentication_methods(req),
        // application grants
        "PutApplicationGrant" => s.put_application_grant(req),
        "GetApplicationGrant" => s.get_application_grant(req),
        "DeleteApplicationGrant" => s.delete_application_grant(req),
        "ListApplicationGrants" => s.list_application_grants(req),
        // application session config
        "GetApplicationSessionConfiguration" => s.get_application_session_configuration(req),
        "PutApplicationSessionConfiguration" => s.put_application_session_configuration(req),
        // application providers
        "ListApplicationProviders" => s.list_application_providers(req),
        "DescribeApplicationProvider" => s.describe_application_provider(req),
        // trusted token issuers
        "CreateTrustedTokenIssuer" => s.create_trusted_token_issuer(req),
        "DescribeTrustedTokenIssuer" => s.describe_trusted_token_issuer(req),
        "UpdateTrustedTokenIssuer" => s.update_trusted_token_issuer(req),
        "DeleteTrustedTokenIssuer" => s.delete_trusted_token_issuer(req),
        "ListTrustedTokenIssuers" => s.list_trusted_token_issuers(req),
        // regions
        "AddRegion" => s.add_region(req),
        "RemoveRegion" => s.remove_region(req),
        "DescribeRegion" => s.describe_region(req),
        "ListRegions" => s.list_regions(req),
        // access control attribute configuration
        "CreateInstanceAccessControlAttributeConfiguration" => s.create_attr_config(req),
        "DescribeInstanceAccessControlAttributeConfiguration" => s.describe_attr_config(req),
        "UpdateInstanceAccessControlAttributeConfiguration" => s.update_attr_config(req),
        "DeleteInstanceAccessControlAttributeConfiguration" => s.delete_attr_config(req),
        // tagging
        "TagResource" => s.tag_resource(req),
        "UntagResource" => s.untag_resource(req),
        "ListTagsForResource" => s.list_tags_for_resource(req),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===== helpers =====

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| validation(&format!("Request body is malformed: {e}")))
}

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation(&format!("{f} must be specified.")))
}

/// Character-length bound check for a present string field, matching the
/// Smithy `@length` constraint. Absent fields are the caller's concern.
fn check_len(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let n = s.chars().count();
        if n < min || n > max {
            return Err(validation(&format!(
                "{field} must have length between {min} and {max}, inclusive."
            )));
        }
    }
    Ok(())
}

/// Enum-membership check for a present string field.
fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&s) {
            return Err(validation(&format!(
                "{field} must be one of [{}].",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// `@range` check for a present integer field.
fn check_range(b: &Value, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field) {
        let n = v.as_i64().unwrap_or(min - 1);
        if n < min || n > max {
            return Err(validation(&format!(
                "{field} must be between {min} and {max}, inclusive."
            )));
        }
    }
    Ok(())
}

/// Validate every input field whose constraints are consistent across all
/// operations that carry it. Per-operation fields (Name, Description) are
/// validated by the individual handlers.
fn validate_common(b: &Value) -> Result<(), AwsServiceError> {
    check_len(b, "InstanceArn", 10, 1224)?;
    check_len(b, "PermissionSetArn", 10, 1224)?;
    check_len(b, "ApplicationArn", 10, 1224)?;
    check_len(b, "ApplicationProviderArn", 10, 1224)?;
    check_len(b, "TrustedTokenIssuerArn", 10, 1224)?;
    check_len(b, "ResourceArn", 10, 2048)?;
    check_len(b, "ManagedPolicyArn", 20, 2048)?;
    check_len(b, "PrincipalId", 1, 47)?;
    check_len(b, "TargetId", 12, 12)?;
    check_len(b, "AccountId", 12, 12)?;
    check_len(b, "RegionName", 1, 32)?;
    check_len(b, "NextToken", 0, 2048)?;
    check_len(b, "ClientToken", 1, 64)?;
    check_len(b, "RelayState", 1, 240)?;
    check_len(b, "SessionDuration", 1, 100)?;
    check_len(b, "InlinePolicy", 1, 32768)?;
    check_len(b, "AccountAssignmentCreationRequestId", 36, 36)?;
    check_len(b, "AccountAssignmentDeletionRequestId", 36, 36)?;
    check_len(b, "ProvisionPermissionSetRequestId", 36, 36)?;
    check_range(b, "MaxResults", 1, 100)?;
    check_enum(b, "PrincipalType", &["USER", "GROUP"])?;
    check_enum(
        b,
        "TargetType",
        &["AWS_ACCOUNT", "ALL_PROVISIONED_ACCOUNTS"],
    )?;
    check_enum(
        b,
        "GrantType",
        &[
            "AUTHORIZATION_CODE",
            "REFRESH_TOKEN",
            "JWT_BEARER",
            "TOKEN_EXCHANGE",
        ],
    )?;
    check_enum(b, "AuthenticationMethodType", &["IAM"])?;
    check_enum(
        b,
        "ProvisioningStatus",
        &[
            "LATEST_PERMISSION_SET_PROVISIONED",
            "LATEST_PERMISSION_SET_NOT_PROVISIONED",
        ],
    )?;
    check_enum(b, "Status", &["ENABLED", "DISABLED"])?;
    check_enum(b, "TrustedTokenIssuerType", &["OIDC_JWT"])?;
    check_enum(
        b,
        "UserBackgroundSessionApplicationStatus",
        &["ENABLED", "DISABLED"],
    )?;
    Ok(())
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn hex16() -> String {
    fakecloud_core::ids::short_id(16).to_string()
}

fn hex10() -> String {
    fakecloud_core::ids::short_id(10).to_string()
}

/// Validate pagination inputs, then window an ordered slice of result rows.
fn paginate(rows: Vec<Value>, b: &Value) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 100) as usize)
        .unwrap_or(100);
    let end = (start + max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Build a paginated list response body from ordered rows.
fn list_response(key: &str, rows: Vec<Value>, b: &Value) -> Result<AwsResponse, AwsServiceError> {
    let (page, next) = paginate(rows, b);
    let mut out = json!({ key: page });
    if let Some(t) = next {
        out["NextToken"] = json!(t);
    }
    ok(out)
}

fn opt_str(m: &mut serde_json::Map<String, Value>, key: &str, v: &Option<String>) {
    if let Some(val) = v {
        m.insert(key.to_string(), json!(val));
    }
}

fn build_permission_set(ps: &StoredPermissionSet) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("PermissionSetArn".into(), json!(ps.arn));
    m.insert("Name".into(), json!(ps.name));
    m.insert("CreatedDate".into(), json!(ps.created_date));
    opt_str(&mut m, "Description", &ps.description);
    opt_str(&mut m, "SessionDuration", &ps.session_duration);
    opt_str(&mut m, "RelayState", &ps.relay_state);
    Value::Object(m)
}

fn build_region(r: &StoredRegion) -> Value {
    json!({
        "RegionName": r.region_name,
        "Status": r.status,
        "AddedDate": r.added_date,
        "IsPrimaryRegion": r.is_primary,
    })
}

fn build_instance_describe(inst: &StoredInstance) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("InstanceArn".into(), json!(inst.arn));
    m.insert("IdentityStoreId".into(), json!(inst.identity_store_id));
    m.insert("OwnerAccountId".into(), json!(inst.owner_account_id));
    m.insert("CreatedDate".into(), json!(inst.created_date));
    m.insert("Status".into(), json!(inst.status));
    opt_str(&mut m, "Name", &inst.name);
    m.insert(
        "PermissionSetsEnabled".into(),
        json!(inst.permission_sets_enabled),
    );
    // Every instance is encrypted; absent an explicit `UpdateInstance` the key
    // is the AWS-owned one, which carries no ARN.
    let key_type = inst
        .encryption_key_type
        .clone()
        .unwrap_or_else(|| "AWS_OWNED_KMS_KEY".to_string());
    let mut enc = serde_json::Map::new();
    enc.insert("KeyType".into(), json!(key_type));
    opt_str(&mut enc, "KmsKeyArn", &inst.encryption_kms_key_arn);
    enc.insert("EncryptionStatus".into(), json!("ENABLED"));
    m.insert("EncryptionConfigurationDetails".into(), Value::Object(enc));
    Value::Object(m)
}

fn build_instance_metadata(inst: &StoredInstance) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("InstanceArn".into(), json!(inst.arn));
    m.insert("IdentityStoreId".into(), json!(inst.identity_store_id));
    m.insert("OwnerAccountId".into(), json!(inst.owner_account_id));
    m.insert("CreatedDate".into(), json!(inst.created_date));
    m.insert("Status".into(), json!(inst.status));
    opt_str(&mut m, "Name", &inst.name);
    opt_str(&mut m, "PrimaryRegion", &inst.primary_region);
    let regions: Vec<Value> = inst.regions.iter().map(build_region).collect();
    m.insert("Regions".into(), json!(regions));
    Value::Object(m)
}

fn build_application(app: &StoredApplication) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("ApplicationArn".into(), json!(app.arn));
    m.insert("ApplicationProviderArn".into(), json!(app.provider_arn));
    m.insert("ApplicationAccount".into(), json!(app.account));
    m.insert("InstanceArn".into(), json!(app.instance_arn));
    m.insert("IdentityStoreArn".into(), json!(app.identity_store_arn));
    m.insert("CreatedDate".into(), json!(app.created_date));
    opt_str(&mut m, "Name", &app.name);
    opt_str(&mut m, "Status", &app.status);
    opt_str(&mut m, "Description", &app.description);
    opt_str(&mut m, "CreatedFrom", &app.created_from);
    if let Some(po) = &app.portal_options {
        m.insert("PortalOptions".into(), po.clone());
    }
    Value::Object(m)
}

fn build_tti_describe(tti: &StoredTrustedTokenIssuer) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("TrustedTokenIssuerArn".into(), json!(tti.arn));
    m.insert("TrustedTokenIssuerType".into(), json!(tti.issuer_type));
    opt_str(&mut m, "Name", &tti.name);
    if let Some(cfg) = &tti.config {
        m.insert("TrustedTokenIssuerConfiguration".into(), cfg.clone());
    }
    Value::Object(m)
}

fn build_tti_metadata(tti: &StoredTrustedTokenIssuer) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("TrustedTokenIssuerArn".into(), json!(tti.arn));
    m.insert("TrustedTokenIssuerType".into(), json!(tti.issuer_type));
    opt_str(&mut m, "Name", &tti.name);
    Value::Object(m)
}

fn build_op_status(st: &StoredOpStatus) -> Value {
    json!({
        "Status": st.status,
        "RequestId": st.request_id,
        "TargetId": st.target_id,
        "TargetType": st.target_type,
        "PermissionSetArn": st.permission_set_arn,
        "PrincipalType": st.principal_type,
        "PrincipalId": st.principal_id,
        "CreatedDate": st.created_date,
    })
}

fn build_prov_status(st: &StoredProvisioningStatus) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("Status".into(), json!(st.status));
    m.insert("RequestId".into(), json!(st.request_id));
    m.insert("PermissionSetArn".into(), json!(st.permission_set_arn));
    m.insert("CreatedDate".into(), json!(st.created_date));
    if let Some(acct) = &st.account_id {
        m.insert("AccountId".into(), json!(acct));
    }
    Value::Object(m)
}

/// Persist the create-time `Tags` list of a resource into the tag store,
/// keyed by its ARN, so `ListTagsForResource` round-trips them.
fn store_tags(acct: &mut crate::state::SsoAdminData, arn: &str, b: &Value) {
    let Some(tags) = b.get("Tags").and_then(Value::as_array) else {
        return;
    };
    if tags.is_empty() {
        return;
    }
    let entry = acct.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        if let (Some(k), Some(v)) = (
            tag.get("Key").and_then(Value::as_str),
            tag.get("Value").and_then(Value::as_str),
        ) {
            entry.insert(k.to_string(), v.to_string());
        }
    }
}

/// Ensure a stored `PortalOptions` echoes AWS's server-side defaults —
/// `Visibility` defaults to `ENABLED` when the caller omits it, and the
/// describe response must surface that default.
fn portal_options_with_defaults(po: Option<Value>) -> Option<Value> {
    po.map(|mut v| {
        if let Value::Object(m) = &mut v {
            m.entry("Visibility").or_insert(json!("ENABLED"));
        }
        v
    })
}

/// Recursively merge `patch` into `base`: object members are merged key by
/// key; every other node is overwritten. Used so `UpdateTrustedTokenIssuer`
/// (whose update configuration omits the immutable `IssuerUrl`) keeps the
/// fields it doesn't restate instead of dropping them.
fn deep_merge(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(b), Value::Object(p)) => {
            for (k, v) in p {
                deep_merge(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, p) => *b = p.clone(),
    }
}

impl SsoAdminService {
    // ---------- instances ----------

    fn create_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        check_len(&b, "Name", 0, 255)?;
        let ssoins = format!("ssoins-{}", hex16());
        let arn = format!("arn:aws:sso:::instance/{ssoins}");
        let identity_store_id = format!("d-{}", hex10());
        let inst = StoredInstance {
            arn: arn.clone(),
            ssoins,
            identity_store_id,
            owner_account_id: req.account_id.clone(),
            name: b.get("Name").and_then(Value::as_str).map(str::to_string),
            status: "ACTIVE".into(),
            created_date: now(),
            regions: Vec::new(),
            primary_region: Some(req.region.clone()),
            attr_config: None,
            attr_config_status: None,
            permission_sets_enabled: false,
            encryption_key_type: None,
            encryption_kms_key_arn: None,
        };
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            acct.instances.insert(arn.clone(), inst);
            store_tags(acct, &arn, &b);
        }
        ok(json!({ "InstanceArn": arn }))
    }

    fn describe_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        if let Some(inst) = guard
            .get(&req.account_id)
            .and_then(|a| a.instances.get(&arn))
        {
            return ok(build_instance_describe(inst));
        }
        // `DescribeInstance` declares no ResourceNotFoundException, so a valid
        // instance ARN that we don't have on record is surfaced as an
        // auto-provisioned ACTIVE instance rather than an error.
        let ssoins = arn.rsplit('/').next().unwrap_or("ssoins-unknown");
        let synth = StoredInstance {
            arn: arn.clone(),
            ssoins: ssoins.to_string(),
            identity_store_id: format!("d-{}", hex10()),
            owner_account_id: req.account_id.clone(),
            name: None,
            status: "ACTIVE".into(),
            created_date: now(),
            regions: Vec::new(),
            primary_region: Some(req.region.clone()),
            attr_config: None,
            attr_config_status: None,
            permission_sets_enabled: false,
            encryption_key_type: None,
            encryption_kms_key_arn: None,
        };
        ok(build_instance_describe(&synth))
    }

    fn update_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        check_len(&b, "Name", 0, 255)?;
        let arn = req_str(&b, "InstanceArn")?.to_string();
        let mut guard = self.state.write();
        let inst = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.instances.get_mut(&arn))
            .ok_or_else(|| not_found("Instance not found."))?;
        if let Some(name) = b.get("Name").and_then(Value::as_str) {
            inst.name = Some(name.to_string());
        }
        if let Some(enabled) = b.get("PermissionSetsEnabled") {
            // Documented on the member: the only accepted value is `true`, and
            // it can't be combined with an encryption change in one call.
            if b.get("EncryptionConfiguration").is_some() {
                return Err(validation(
                    "EncryptionConfiguration and PermissionSetsEnabled cannot be set in the same request.",
                ));
            }
            match enabled.as_bool() {
                Some(true) => inst.permission_sets_enabled = true,
                _ => {
                    return Err(validation(
                        "PermissionSetsEnabled only accepts the value true.",
                    ))
                }
            }
        }
        if let Some(cfg) = b.get("EncryptionConfiguration") {
            let key_type = cfg
                .get("KeyType")
                .and_then(Value::as_str)
                .ok_or_else(|| validation("EncryptionConfiguration.KeyType must be specified."))?;
            let kms_key_arn = cfg.get("KmsKeyArn").and_then(Value::as_str);
            match key_type {
                "AWS_OWNED_KMS_KEY" => {
                    inst.encryption_key_type = Some(key_type.to_string());
                    inst.encryption_kms_key_arn = None;
                }
                "CUSTOMER_MANAGED_KEY" => {
                    let arn = kms_key_arn.filter(|a| !a.is_empty()).ok_or_else(|| {
                        validation(
                            "EncryptionConfiguration.KmsKeyArn must be specified for a CUSTOMER_MANAGED_KEY.",
                        )
                    })?;
                    inst.encryption_key_type = Some(key_type.to_string());
                    inst.encryption_kms_key_arn = Some(arn.to_string());
                }
                other => {
                    return Err(validation(&format!(
                        "EncryptionConfiguration.KeyType must be one of AWS_OWNED_KMS_KEY, CUSTOMER_MANAGED_KEY; got {other}."
                    )))
                }
            }
        }
        ok(json!({}))
    }

    fn delete_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "InstanceArn")?.to_string();
        if let Some(a) = self.state.write().get_mut(&req.account_id) {
            a.instances.remove(&arn);
        }
        ok(json!({}))
    }

    fn list_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| a.instances.values().map(build_instance_metadata).collect())
            .unwrap_or_default();
        list_response("Instances", rows, &b)
    }

    // ---------- permission sets ----------

    fn permission_set<'a>(
        &self,
        guard: &'a fakecloud_core::multi_account::MultiAccountState<crate::state::SsoAdminData>,
        account: &str,
        arn: &str,
    ) -> Option<&'a StoredPermissionSet> {
        guard.get(account).and_then(|a| a.permission_sets.get(arn))
    }

    fn create_permission_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let name = req_str(&b, "Name")?.to_string();
        check_len(&b, "Name", 1, 32)?;
        check_len(&b, "Description", 1, 700)?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let inst = acct
            .instances
            .get(&instance_arn)
            .ok_or_else(|| not_found("Instance not found."))?;
        let ssoins = inst.ssoins.clone();
        let arn = format!("arn:aws:sso:::permissionSet/{ssoins}/ps-{}", hex16());
        let ps = StoredPermissionSet {
            arn: arn.clone(),
            instance_arn,
            name,
            description: b
                .get("Description")
                .and_then(Value::as_str)
                .map(str::to_string),
            session_duration: b
                .get("SessionDuration")
                .and_then(Value::as_str)
                .map(str::to_string),
            relay_state: b
                .get("RelayState")
                .and_then(Value::as_str)
                .map(str::to_string),
            created_date: now(),
            inline_policy: None,
            permissions_boundary: None,
            managed_policies: Vec::new(),
            customer_managed: Vec::new(),
        };
        acct.permission_sets.insert(arn.clone(), ps.clone());
        store_tags(acct, &arn, &b);
        ok(json!({ "PermissionSet": build_permission_set(&ps) }))
    }

    fn describe_permission_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let ps = self
            .permission_set(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Permission set not found."))?;
        ok(json!({ "PermissionSet": build_permission_set(ps) }))
    }

    fn update_permission_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        check_len(&b, "Description", 1, 700)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let mut guard = self.state.write();
        let ps = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.permission_sets.get_mut(&arn))
            .ok_or_else(|| not_found("Permission set not found."))?;
        if let Some(d) = b.get("Description").and_then(Value::as_str) {
            ps.description = Some(d.to_string());
        }
        if let Some(d) = b.get("SessionDuration").and_then(Value::as_str) {
            ps.session_duration = Some(d.to_string());
        }
        if let Some(d) = b.get("RelayState").and_then(Value::as_str) {
            ps.relay_state = Some(d.to_string());
        }
        ok(json!({}))
    }

    fn delete_permission_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        if let Some(a) = self.state.write().get_mut(&req.account_id) {
            a.permission_sets.remove(&arn);
        }
        ok(json!({}))
    }

    fn list_permission_sets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.permission_sets
                    .values()
                    .filter(|ps| ps.instance_arn == instance_arn)
                    .map(|ps| json!(ps.arn))
                    .collect()
            })
            .unwrap_or_default();
        list_response("PermissionSets", rows, &b)
    }

    // ---------- permission set policies ----------

    fn with_permission_set<F>(
        &self,
        req: &AwsRequest,
        b: &Value,
        f: F,
    ) -> Result<AwsResponse, AwsServiceError>
    where
        F: FnOnce(&mut StoredPermissionSet) -> Result<AwsResponse, AwsServiceError>,
    {
        let arn = req_str(b, "PermissionSetArn")?.to_string();
        let mut guard = self.state.write();
        let ps = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.permission_sets.get_mut(&arn))
            .ok_or_else(|| not_found("Permission set not found."))?;
        f(ps)
    }

    fn attach_managed_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let policy_arn = req_str(&b, "ManagedPolicyArn")?.to_string();
        let name = policy_arn
            .rsplit('/')
            .next()
            .unwrap_or(&policy_arn)
            .to_string();
        self.with_permission_set(req, &b, |ps| {
            if !ps.managed_policies.iter().any(|p| p.arn == policy_arn) {
                ps.managed_policies.push(StoredManagedPolicy {
                    name,
                    arn: policy_arn,
                });
            }
            ok(json!({}))
        })
    }

    fn detach_managed_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let policy_arn = req_str(&b, "ManagedPolicyArn")?.to_string();
        self.with_permission_set(req, &b, |ps| {
            ps.managed_policies.retain(|p| p.arn != policy_arn);
            ok(json!({}))
        })
    }

    fn list_managed_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .permission_set(&guard, &req.account_id, &arn)
            .map(|ps| {
                ps.managed_policies
                    .iter()
                    .map(|p| json!({ "Name": p.name, "Arn": p.arn }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("AttachedManagedPolicies", rows, &b)
    }

    fn attach_customer_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let reference = b
            .get("CustomerManagedPolicyReference")
            .cloned()
            .ok_or_else(|| validation("CustomerManagedPolicyReference must be specified."))?;
        let ref_name = reference
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string);
        self.with_permission_set(req, &b, |ps| {
            let exists = ps
                .customer_managed
                .iter()
                .any(|r| r.get("Name").and_then(Value::as_str).map(str::to_string) == ref_name);
            if !exists {
                ps.customer_managed.push(reference);
            }
            ok(json!({}))
        })
    }

    fn detach_customer_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let reference = b
            .get("CustomerManagedPolicyReference")
            .cloned()
            .ok_or_else(|| validation("CustomerManagedPolicyReference must be specified."))?;
        let ref_name = reference
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string);
        self.with_permission_set(req, &b, |ps| {
            ps.customer_managed
                .retain(|r| r.get("Name").and_then(Value::as_str).map(str::to_string) != ref_name);
            ok(json!({}))
        })
    }

    fn list_customer_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .permission_set(&guard, &req.account_id, &arn)
            .map(|ps| ps.customer_managed.clone())
            .unwrap_or_default();
        list_response("CustomerManagedPolicyReferences", rows, &b)
    }

    fn put_inline_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let policy = req_str(&b, "InlinePolicy")?.to_string();
        self.with_permission_set(req, &b, |ps| {
            ps.inline_policy = Some(policy);
            ok(json!({}))
        })
    }

    fn get_inline_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let ps = self
            .permission_set(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Permission set not found."))?;
        // An unset inline policy resolves to the empty string, matching AWS.
        ok(json!({ "InlinePolicy": ps.inline_policy.clone().unwrap_or_default() }))
    }

    fn delete_inline_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        self.with_permission_set(req, &b, |ps| {
            ps.inline_policy = None;
            ok(json!({}))
        })
    }

    fn put_permissions_boundary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let boundary = b
            .get("PermissionsBoundary")
            .cloned()
            .ok_or_else(|| validation("PermissionsBoundary must be specified."))?;
        // A permissions boundary is exactly one of a managed policy ARN or a
        // customer-managed policy reference, never both.
        if boundary.get("ManagedPolicyArn").is_some()
            && boundary.get("CustomerManagedPolicyReference").is_some()
        {
            return Err(validation(
                "Only ManagedPolicyArn or CustomerManagedPolicyReference should be given.",
            ));
        }
        self.with_permission_set(req, &b, |ps| {
            ps.permissions_boundary = Some(boundary);
            ok(json!({}))
        })
    }

    fn get_permissions_boundary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let ps = self
            .permission_set(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Permission set not found."))?;
        let boundary = ps
            .permissions_boundary
            .clone()
            .ok_or_else(|| not_found("No permissions boundary is set."))?;
        ok(json!({ "PermissionsBoundary": boundary }))
    }

    fn delete_permissions_boundary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        self.with_permission_set(req, &b, |ps| {
            ps.permissions_boundary = None;
            ok(json!({}))
        })
    }

    // ---------- account assignments ----------

    fn create_account_assignment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let target_id = req_str(&b, "TargetId")?.to_string();
        req_str(&b, "TargetType")?;
        let ps_arn = req_str(&b, "PermissionSetArn")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let request_id = Uuid::new_v4().to_string();
        let st = StoredOpStatus {
            request_id: request_id.clone(),
            status: "IN_PROGRESS".into(),
            target_id: target_id.clone(),
            target_type: b
                .get("TargetType")
                .and_then(Value::as_str)
                .unwrap_or("AWS_ACCOUNT")
                .to_string(),
            permission_set_arn: ps_arn.clone(),
            principal_type: principal_type.clone(),
            principal_id: principal_id.clone(),
            created_date: now(),
        };
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        // Record the actual assignment (no duplicate) plus the async status.
        if !acct.assignments.iter().any(|a| {
            a.account_id == target_id
                && a.permission_set_arn == ps_arn
                && a.principal_id == principal_id
        }) {
            acct.assignments.push(StoredAssignment {
                account_id: target_id,
                permission_set_arn: ps_arn,
                principal_type,
                principal_id,
            });
        }
        acct.aa_create_status.insert(request_id, st.clone());
        ok(json!({ "AccountAssignmentCreationStatus": build_op_status(&st) }))
    }

    fn delete_account_assignment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let target_id = req_str(&b, "TargetId")?.to_string();
        req_str(&b, "TargetType")?;
        let ps_arn = req_str(&b, "PermissionSetArn")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let request_id = Uuid::new_v4().to_string();
        let st = StoredOpStatus {
            request_id: request_id.clone(),
            status: "IN_PROGRESS".into(),
            target_id: target_id.clone(),
            target_type: b
                .get("TargetType")
                .and_then(Value::as_str)
                .unwrap_or("AWS_ACCOUNT")
                .to_string(),
            permission_set_arn: ps_arn.clone(),
            principal_type,
            principal_id: principal_id.clone(),
            created_date: now(),
        };
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        acct.assignments.retain(|a| {
            !(a.account_id == target_id
                && a.permission_set_arn == ps_arn
                && a.principal_id == principal_id)
        });
        acct.aa_delete_status.insert(request_id, st.clone());
        ok(json!({ "AccountAssignmentDeletionStatus": build_op_status(&st) }))
    }

    fn describe_aa_creation_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let id = req_str(&b, "AccountAssignmentCreationRequestId")?.to_string();
        let mut guard = self.state.write();
        let st = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.aa_create_status.get_mut(&id))
            .ok_or_else(|| not_found("Assignment creation status not found."))?;
        st.status = "SUCCEEDED".into();
        ok(json!({ "AccountAssignmentCreationStatus": build_op_status(st) }))
    }

    fn describe_aa_deletion_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let id = req_str(&b, "AccountAssignmentDeletionRequestId")?.to_string();
        let mut guard = self.state.write();
        let st = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.aa_delete_status.get_mut(&id))
            .ok_or_else(|| not_found("Assignment deletion status not found."))?;
        st.status = "SUCCEEDED".into();
        ok(json!({ "AccountAssignmentDeletionStatus": build_op_status(st) }))
    }

    fn list_aa_creation_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.aa_create_status
                    .values()
                    .map(|st| json!({ "Status": st.status, "RequestId": st.request_id, "CreatedDate": st.created_date }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("AccountAssignmentsCreationStatus", rows, &b)
    }

    fn list_aa_deletion_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.aa_delete_status
                    .values()
                    .map(|st| json!({ "Status": st.status, "RequestId": st.request_id, "CreatedDate": st.created_date }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("AccountAssignmentsDeletionStatus", rows, &b)
    }

    fn list_account_assignments(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let account_id = req_str(&b, "AccountId")?.to_string();
        let ps_arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.assignments
                    .iter()
                    .filter(|x| x.account_id == account_id && x.permission_set_arn == ps_arn)
                    .map(|x| {
                        json!({
                            "AccountId": x.account_id,
                            "PermissionSetArn": x.permission_set_arn,
                            "PrincipalType": x.principal_type,
                            "PrincipalId": x.principal_id,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("AccountAssignments", rows, &b)
    }

    fn list_account_assignments_for_principal(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.assignments
                    .iter()
                    // `PrincipalType` is a required input and AWS filters on it
                    // together with `PrincipalId`; ignoring it would surface a
                    // USER assignment when the caller queried a GROUP (and vice
                    // versa) whose id happened to collide.
                    .filter(|x| {
                        x.principal_id == principal_id && x.principal_type == principal_type
                    })
                    .map(|x| {
                        json!({
                            "AccountId": x.account_id,
                            "PermissionSetArn": x.permission_set_arn,
                            "PrincipalId": x.principal_id,
                            "PrincipalType": x.principal_type,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("AccountAssignments", rows, &b)
    }

    fn list_accounts_for_provisioned(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let ps_arn = req_str(&b, "PermissionSetArn")?.to_string();
        let guard = self.state.read();
        let mut accounts: Vec<String> = guard
            .get(&req.account_id)
            .map(|a| {
                a.assignments
                    .iter()
                    .filter(|x| x.permission_set_arn == ps_arn)
                    .map(|x| x.account_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        accounts.sort();
        accounts.dedup();
        let rows: Vec<Value> = accounts.into_iter().map(|a| json!(a)).collect();
        list_response("AccountIds", rows, &b)
    }

    fn list_ps_provisioned_to_account(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let account_id = req_str(&b, "AccountId")?.to_string();
        let guard = self.state.read();
        let mut ps: Vec<String> = guard
            .get(&req.account_id)
            .map(|a| {
                a.assignments
                    .iter()
                    .filter(|x| x.account_id == account_id)
                    .map(|x| x.permission_set_arn.clone())
                    .collect()
            })
            .unwrap_or_default();
        ps.sort();
        ps.dedup();
        let rows: Vec<Value> = ps.into_iter().map(|a| json!(a)).collect();
        list_response("PermissionSets", rows, &b)
    }

    // ---------- provisioning ----------

    fn provision_permission_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let ps_arn = req_str(&b, "PermissionSetArn")?.to_string();
        req_str(&b, "TargetType")?;
        let request_id = Uuid::new_v4().to_string();
        let st = StoredProvisioningStatus {
            request_id: request_id.clone(),
            status: "IN_PROGRESS".into(),
            account_id: b
                .get("TargetId")
                .and_then(Value::as_str)
                .map(str::to_string),
            permission_set_arn: ps_arn,
            created_date: now(),
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .ps_provisioning
            .insert(request_id, st.clone());
        ok(json!({ "PermissionSetProvisioningStatus": build_prov_status(&st) }))
    }

    fn describe_ps_provisioning_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let id = req_str(&b, "ProvisionPermissionSetRequestId")?.to_string();
        let mut guard = self.state.write();
        let st = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.ps_provisioning.get_mut(&id))
            .ok_or_else(|| not_found("Provisioning status not found."))?;
        st.status = "SUCCEEDED".into();
        ok(json!({ "PermissionSetProvisioningStatus": build_prov_status(st) }))
    }

    fn list_ps_provisioning_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.ps_provisioning
                    .values()
                    .map(|st| json!({ "Status": st.status, "RequestId": st.request_id, "CreatedDate": st.created_date }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("PermissionSetsProvisioningStatus", rows, &b)
    }

    // ---------- applications ----------

    fn application<'a>(
        &self,
        guard: &'a fakecloud_core::multi_account::MultiAccountState<crate::state::SsoAdminData>,
        account: &str,
        arn: &str,
    ) -> Option<&'a StoredApplication> {
        guard.get(account).and_then(|a| a.applications.get(arn))
    }

    fn create_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let provider_arn = req_str(&b, "ApplicationProviderArn")?.to_string();
        let name = req_str(&b, "Name")?.to_string();
        check_len(&b, "Name", 1, 100)?;
        check_len(&b, "Description", 1, 128)?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let inst = acct
            .instances
            .get(&instance_arn)
            .ok_or_else(|| not_found("Instance not found."))?;
        let ssoins = inst.ssoins.clone();
        let identity_store_id = inst.identity_store_id.clone();
        let arn = format!(
            "arn:aws:sso::{}:application/{ssoins}/apl-{}",
            req.account_id,
            hex16()
        );
        let identity_store_arn = format!(
            "arn:aws:identitystore::{}:identitystore/{identity_store_id}",
            req.account_id
        );
        let app = StoredApplication {
            arn: arn.clone(),
            provider_arn,
            name: Some(name),
            account: req.account_id.clone(),
            instance_arn: instance_arn.clone(),
            identity_store_arn: identity_store_arn.clone(),
            status: Some(
                b.get("Status")
                    .and_then(Value::as_str)
                    .unwrap_or("ENABLED")
                    .to_string(),
            ),
            portal_options: portal_options_with_defaults(b.get("PortalOptions").cloned()),
            description: b
                .get("Description")
                .and_then(Value::as_str)
                .map(str::to_string),
            created_date: now(),
            created_from: Some(req.region.clone()),
            assignments: Vec::new(),
            assignment_required: None,
            access_scopes: Default::default(),
            auth_methods: Default::default(),
            grants: Default::default(),
            session_status: None,
        };
        acct.applications.insert(arn.clone(), app);
        store_tags(acct, &arn, &b);
        ok(json!({
            "ApplicationArn": arn,
            "InstanceArn": instance_arn,
            "IdentityStoreArn": identity_store_arn,
        }))
    }

    fn describe_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        ok(build_application(app))
    }

    fn update_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        check_len(&b, "Name", 1, 100)?;
        check_len(&b, "Description", 1, 128)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.applications.get_mut(&arn))
            .ok_or_else(|| not_found("Application not found."))?;
        if let Some(n) = b.get("Name").and_then(Value::as_str) {
            app.name = Some(n.to_string());
        }
        if let Some(d) = b.get("Description").and_then(Value::as_str) {
            app.description = Some(d.to_string());
        }
        if let Some(s) = b.get("Status").and_then(Value::as_str) {
            app.status = Some(s.to_string());
        }
        if let Some(po) = b.get("PortalOptions") {
            app.portal_options = portal_options_with_defaults(Some(po.clone()));
        }
        ok(json!({}))
    }

    fn delete_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        if let Some(a) = self.state.write().get_mut(&req.account_id) {
            a.applications.remove(&arn);
        }
        ok(json!({}))
    }

    fn list_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.applications
                    .values()
                    .filter(|app| app.instance_arn == instance_arn)
                    .map(build_application)
                    .collect()
            })
            .unwrap_or_default();
        list_response("Applications", rows, &b)
    }

    // ---------- application assignments ----------

    fn with_application<F>(
        &self,
        req: &AwsRequest,
        b: &Value,
        f: F,
    ) -> Result<AwsResponse, AwsServiceError>
    where
        F: FnOnce(&mut StoredApplication) -> Result<AwsResponse, AwsServiceError>,
    {
        let arn = req_str(b, "ApplicationArn")?.to_string();
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.applications.get_mut(&arn))
            .ok_or_else(|| not_found("Application not found."))?;
        f(app)
    }

    fn create_application_assignment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        self.with_application(req, &b, |app| {
            if !app
                .assignments
                .iter()
                .any(|(pid, pt)| pid == &principal_id && pt == &principal_type)
            {
                app.assignments.push((principal_id, principal_type));
            }
            ok(json!({}))
        })
    }

    fn delete_application_assignment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        self.with_application(req, &b, |app| {
            app.assignments
                .retain(|(pid, pt)| !(pid == &principal_id && pt == &principal_type));
            ok(json!({}))
        })
    }

    fn describe_application_assignment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        let found = app
            .assignments
            .iter()
            .any(|(pid, pt)| pid == &principal_id && pt == &principal_type);
        if !found {
            return Err(not_found("Application assignment not found."));
        }
        ok(json!({
            "ApplicationArn": arn,
            "PrincipalId": principal_id,
            "PrincipalType": principal_type,
        }))
    }

    fn list_application_assignments(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .application(&guard, &req.account_id, &arn)
            .map(|app| {
                app.assignments
                    .iter()
                    .map(|(pid, pt)| {
                        json!({ "ApplicationArn": arn, "PrincipalId": pid, "PrincipalType": pt })
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("ApplicationAssignments", rows, &b)
    }

    fn list_application_assignments_for_principal(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        req_str(&b, "InstanceArn")?;
        let principal_id = req_str(&b, "PrincipalId")?.to_string();
        let principal_type = req_str(&b, "PrincipalType")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.applications
                    .values()
                    .flat_map(|app| {
                        app.assignments
                            .iter()
                            .filter(|(pid, pt)| pid == &principal_id && pt == &principal_type)
                            .map(|(pid, pt)| {
                                json!({
                                    "ApplicationArn": app.arn,
                                    "PrincipalId": pid,
                                    "PrincipalType": pt,
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("ApplicationAssignments", rows, &b)
    }

    fn get_application_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        // AWS defaults AssignmentRequired to true when unset.
        ok(json!({ "AssignmentRequired": app.assignment_required.unwrap_or(true) }))
    }

    fn put_application_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let required = b
            .get("AssignmentRequired")
            .and_then(Value::as_bool)
            .ok_or_else(|| validation("AssignmentRequired must be specified."))?;
        self.with_application(req, &b, |app| {
            app.assignment_required = Some(required);
            ok(json!({}))
        })
    }

    // ---------- application access scopes ----------

    fn put_application_access_scope(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let scope = req_str(&b, "Scope")?.to_string();
        let targets = b.get("AuthorizedTargets").cloned();
        self.with_application(req, &b, |app| {
            app.access_scopes
                .insert(scope, targets.unwrap_or(Value::Null));
            ok(json!({}))
        })
    }

    fn get_application_access_scope(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let scope = req_str(&b, "Scope")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        let targets = app
            .access_scopes
            .get(&scope)
            .ok_or_else(|| not_found("Access scope not found."))?;
        let mut m = serde_json::Map::new();
        m.insert("Scope".into(), json!(scope));
        if !targets.is_null() {
            m.insert("AuthorizedTargets".into(), targets.clone());
        }
        ok(Value::Object(m))
    }

    fn delete_application_access_scope(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let scope = req_str(&b, "Scope")?.to_string();
        self.with_application(req, &b, |app| {
            app.access_scopes.remove(&scope);
            ok(json!({}))
        })
    }

    fn list_application_access_scopes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .application(&guard, &req.account_id, &arn)
            .map(|app| {
                app.access_scopes
                    .iter()
                    .map(|(scope, targets)| {
                        let mut m = serde_json::Map::new();
                        m.insert("Scope".into(), json!(scope));
                        if !targets.is_null() {
                            m.insert("AuthorizedTargets".into(), targets.clone());
                        }
                        Value::Object(m)
                    })
                    .collect()
            })
            .unwrap_or_default();
        list_response("Scopes", rows, &b)
    }

    // ---------- application authentication methods ----------

    fn put_application_authentication_method(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let mtype = req_str(&b, "AuthenticationMethodType")?.to_string();
        let method = b
            .get("AuthenticationMethod")
            .cloned()
            .ok_or_else(|| validation("AuthenticationMethod must be specified."))?;
        self.with_application(req, &b, |app| {
            app.auth_methods.insert(mtype, method);
            ok(json!({}))
        })
    }

    fn get_application_authentication_method(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let mtype = req_str(&b, "AuthenticationMethodType")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        let method = app
            .auth_methods
            .get(&mtype)
            .ok_or_else(|| not_found("Authentication method not found."))?;
        ok(json!({ "AuthenticationMethod": method }))
    }

    fn delete_application_authentication_method(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let mtype = req_str(&b, "AuthenticationMethodType")?.to_string();
        self.with_application(req, &b, |app| {
            app.auth_methods.remove(&mtype);
            ok(json!({}))
        })
    }

    fn list_application_authentication_methods(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .application(&guard, &req.account_id, &arn)
            .map(|app| {
                app.auth_methods
                    .iter()
                    .map(|(t, m)| json!({ "AuthenticationMethodType": t, "AuthenticationMethod": m }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("AuthenticationMethods", rows, &b)
    }

    // ---------- application grants ----------

    fn put_application_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let gtype = req_str(&b, "GrantType")?.to_string();
        let grant = b
            .get("Grant")
            .cloned()
            .ok_or_else(|| validation("Grant must be specified."))?;
        self.with_application(req, &b, |app| {
            app.grants.insert(gtype, grant);
            ok(json!({}))
        })
    }

    fn get_application_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let gtype = req_str(&b, "GrantType")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        let grant = app
            .grants
            .get(&gtype)
            .ok_or_else(|| not_found("Grant not found."))?;
        ok(json!({ "Grant": grant }))
    }

    fn delete_application_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let gtype = req_str(&b, "GrantType")?.to_string();
        self.with_application(req, &b, |app| {
            app.grants.remove(&gtype);
            ok(json!({}))
        })
    }

    fn list_application_grants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = self
            .application(&guard, &req.account_id, &arn)
            .map(|app| {
                app.grants
                    .iter()
                    .map(|(t, g)| json!({ "GrantType": t, "Grant": g }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("Grants", rows, &b)
    }

    // ---------- application session configuration ----------

    fn get_application_session_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationArn")?.to_string();
        let guard = self.state.read();
        let app = self
            .application(&guard, &req.account_id, &arn)
            .ok_or_else(|| not_found("Application not found."))?;
        let mut m = serde_json::Map::new();
        if let Some(status) = &app.session_status {
            m.insert(
                "UserBackgroundSessionApplicationStatus".into(),
                json!(status),
            );
        }
        ok(Value::Object(m))
    }

    fn put_application_session_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let status = b
            .get("UserBackgroundSessionApplicationStatus")
            .and_then(Value::as_str)
            .map(str::to_string);
        self.with_application(req, &b, |app| {
            app.session_status = status;
            ok(json!({}))
        })
    }

    // ---------- application providers ----------

    fn list_application_providers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let rows: Vec<Value> = APPLICATION_PROVIDERS
            .iter()
            .map(|(arn, proto, display)| {
                json!({
                    "ApplicationProviderArn": arn,
                    "FederationProtocol": proto,
                    "DisplayData": { "DisplayName": display },
                })
            })
            .collect();
        list_response("ApplicationProviders", rows, &b)
    }

    fn describe_application_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "ApplicationProviderArn")?.to_string();
        let found = APPLICATION_PROVIDERS
            .iter()
            .find(|(a, _, _)| *a == arn)
            .ok_or_else(|| not_found("Application provider not found."))?;
        ok(json!({
            "ApplicationProviderArn": found.0,
            "FederationProtocol": found.1,
            "DisplayData": { "DisplayName": found.2 },
        }))
    }

    // ---------- trusted token issuers ----------

    fn create_trusted_token_issuer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let name = req_str(&b, "Name")?.to_string();
        check_len(&b, "Name", 1, 255)?;
        req_str(&b, "TrustedTokenIssuerType")?;
        // `CreateTrustedTokenIssuer` declares no ResourceNotFoundException, so
        // synthesize the parent instance's fragment when it's not on record.
        let guard = self.state.read();
        let ssoins = guard
            .get(&req.account_id)
            .and_then(|a| a.instances.get(&instance_arn))
            .map(|i| i.ssoins.clone())
            .unwrap_or_else(|| format!("ssoins-{}", hex16()));
        drop(guard);
        let arn = format!(
            "arn:aws:sso::{}:trustedTokenIssuer/{ssoins}/tti-{}",
            req.account_id,
            Uuid::new_v4()
        );
        let tti = StoredTrustedTokenIssuer {
            arn: arn.clone(),
            instance_arn,
            name: Some(name),
            issuer_type: b
                .get("TrustedTokenIssuerType")
                .and_then(Value::as_str)
                .unwrap_or("OIDC_JWT")
                .to_string(),
            config: b.get("TrustedTokenIssuerConfiguration").cloned(),
        };
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            acct.trusted_token_issuers.insert(arn.clone(), tti);
            store_tags(acct, &arn, &b);
        }
        ok(json!({ "TrustedTokenIssuerArn": arn }))
    }

    fn describe_trusted_token_issuer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "TrustedTokenIssuerArn")?.to_string();
        let guard = self.state.read();
        let tti = guard
            .get(&req.account_id)
            .and_then(|a| a.trusted_token_issuers.get(&arn))
            .ok_or_else(|| not_found("Trusted token issuer not found."))?;
        ok(build_tti_describe(tti))
    }

    fn update_trusted_token_issuer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        check_len(&b, "Name", 1, 255)?;
        let arn = req_str(&b, "TrustedTokenIssuerArn")?.to_string();
        let mut guard = self.state.write();
        let tti = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.trusted_token_issuers.get_mut(&arn))
            .ok_or_else(|| not_found("Trusted token issuer not found."))?;
        if let Some(n) = b.get("Name").and_then(Value::as_str) {
            tti.name = Some(n.to_string());
        }
        if let Some(c) = b.get("TrustedTokenIssuerConfiguration") {
            // The update configuration omits the immutable `IssuerUrl`; merge so
            // the stored describe config keeps it instead of dropping it (which
            // would make Terraform see a perpetual replacement diff).
            match &mut tti.config {
                Some(existing) => deep_merge(existing, c),
                None => tti.config = Some(c.clone()),
            }
        }
        ok(json!({}))
    }

    fn delete_trusted_token_issuer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let arn = req_str(&b, "TrustedTokenIssuerArn")?.to_string();
        if let Some(a) = self.state.write().get_mut(&req.account_id) {
            a.trusted_token_issuers.remove(&arn);
        }
        ok(json!({}))
    }

    fn list_trusted_token_issuers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.trusted_token_issuers
                    .values()
                    .filter(|t| t.instance_arn == instance_arn)
                    .map(build_tti_metadata)
                    .collect()
            })
            .unwrap_or_default();
        list_response("TrustedTokenIssuers", rows, &b)
    }

    // ---------- regions ----------

    fn add_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let region_name = req_str(&b, "RegionName")?.to_string();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        // `AddRegion` declares no ResourceNotFoundException, so materialize the
        // instance record if it isn't already present.
        let ssoins = instance_arn
            .rsplit('/')
            .next()
            .unwrap_or("ssoins-unknown")
            .to_string();
        let inst = acct
            .instances
            .entry(instance_arn.clone())
            .or_insert_with(|| StoredInstance {
                arn: instance_arn.clone(),
                ssoins,
                identity_store_id: format!("d-{}", hex10()),
                owner_account_id: req.account_id.clone(),
                name: None,
                status: "ACTIVE".into(),
                created_date: now(),
                regions: Vec::new(),
                primary_region: Some(req.region.clone()),
                attr_config: None,
                attr_config_status: None,
                permission_sets_enabled: false,
                encryption_key_type: None,
                encryption_kms_key_arn: None,
            });
        if !inst.regions.iter().any(|r| r.region_name == region_name) {
            let is_primary = inst.regions.is_empty();
            if is_primary {
                inst.primary_region = Some(region_name.clone());
            }
            inst.regions.push(StoredRegion {
                region_name,
                status: "ACTIVE".into(),
                added_date: now(),
                is_primary,
            });
        }
        ok(json!({ "Status": "ACTIVE" }))
    }

    fn remove_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let region_name = req_str(&b, "RegionName")?.to_string();
        if let Some(inst) = self
            .state
            .write()
            .get_mut(&req.account_id)
            .and_then(|a| a.instances.get_mut(&instance_arn))
        {
            inst.regions.retain(|r| r.region_name != region_name);
        }
        ok(json!({ "Status": "REMOVING" }))
    }

    fn describe_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let region_name = req_str(&b, "RegionName")?.to_string();
        let guard = self.state.read();
        let region = guard
            .get(&req.account_id)
            .and_then(|a| a.instances.get(&instance_arn))
            .and_then(|i| i.regions.iter().find(|r| r.region_name == region_name))
            .ok_or_else(|| not_found("Region not found."))?;
        ok(build_region(region))
    }

    fn list_regions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.instances.get(&instance_arn))
            .map(|i| i.regions.iter().map(build_region).collect())
            .unwrap_or_default();
        list_response("Regions", rows, &b)
    }

    // ---------- access control attribute configuration ----------

    fn create_attr_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let config = b
            .get("InstanceAccessControlAttributeConfiguration")
            .cloned()
            .ok_or_else(|| {
                validation("InstanceAccessControlAttributeConfiguration must be specified.")
            })?;
        let mut guard = self.state.write();
        let inst = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.instances.get_mut(&instance_arn))
            .ok_or_else(|| not_found("Instance not found."))?;
        inst.attr_config = Some(config);
        inst.attr_config_status = Some("ENABLED".into());
        ok(json!({}))
    }

    fn describe_attr_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let guard = self.state.read();
        let inst = guard
            .get(&req.account_id)
            .and_then(|a| a.instances.get(&instance_arn))
            .ok_or_else(|| not_found("Instance not found."))?;
        let config = inst
            .attr_config
            .clone()
            .ok_or_else(|| not_found("Access control attribute configuration not found."))?;
        ok(json!({
            "Status": inst.attr_config_status.clone().unwrap_or_else(|| "ENABLED".into()),
            "InstanceAccessControlAttributeConfiguration": config,
        }))
    }

    fn update_attr_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        let config = b
            .get("InstanceAccessControlAttributeConfiguration")
            .cloned()
            .ok_or_else(|| {
                validation("InstanceAccessControlAttributeConfiguration must be specified.")
            })?;
        let mut guard = self.state.write();
        let inst = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.instances.get_mut(&instance_arn))
            .ok_or_else(|| not_found("Instance not found."))?;
        inst.attr_config = Some(config);
        inst.attr_config_status = Some("ENABLED".into());
        ok(json!({}))
    }

    fn delete_attr_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let instance_arn = req_str(&b, "InstanceArn")?.to_string();
        if let Some(inst) = self
            .state
            .write()
            .get_mut(&req.account_id)
            .and_then(|a| a.instances.get_mut(&instance_arn))
        {
            inst.attr_config = None;
            inst.attr_config_status = None;
        }
        ok(json!({}))
    }

    // ---------- tagging ----------

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let tags = b
            .get("Tags")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("Tags must be specified."))?;
        let mut guard = self.state.write();
        let entry = guard
            .get_or_create(&req.account_id)
            .tags
            .entry(resource_arn)
            .or_default();
        for tag in tags {
            if let (Some(k), Some(v)) = (
                tag.get("Key").and_then(Value::as_str),
                tag.get("Value").and_then(Value::as_str),
            ) {
                entry.insert(k.to_string(), v.to_string());
            }
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let keys = b
            .get("TagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("TagKeys must be specified."))?;
        if let Some(entry) = self
            .state
            .write()
            .get_mut(&req.account_id)
            .and_then(|a| a.tags.get_mut(&resource_arn))
        {
            for k in keys {
                if let Some(k) = k.as_str() {
                    entry.remove(k);
                }
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_common(&b)?;
        let resource_arn = req_str(&b, "ResourceArn")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.tags.get(&resource_arn))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        list_response("Tags", rows, &b)
    }
}

#[cfg(test)]
mod tests;
