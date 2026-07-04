//! AWS Resource Access Manager (RAM) restJson1 service handler.
//!
//! Implements the RAM control plane: resource shares plus their resource and
//! principal associations and attached permissions, resource-share invitations
//! (created when a share adds an external account principal), customer-managed
//! permissions with their versions, tags, and the read paths over a seeded
//! catalogue of AWS-managed default permissions and supported resource types.
//!
//! Every RAM operation is a single-segment restJson1 POST/DELETE (`@http`
//! `POST /createresourceshare`, ...) with a JSON body, so routing keys purely
//! on `(method, path)`. Associations progress synthetically to `ASSOCIATED` on
//! create, mirroring the eventual-consistency of the real service.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    invitation_arn, managed_permission_arn, permission_arn, resource_share_arn, AssociationRecord,
    InvitationRecord, PermissionRecord, PermissionVersionRecord, ResourceShareRecord,
    SharedRamState, TagMap,
};
use crate::validate;

pub const RAM_ACTIONS: &[&str] = &[
    "AcceptResourceShareInvitation",
    "AssociateResourceShare",
    "AssociateResourceSharePermission",
    "CreatePermission",
    "CreatePermissionVersion",
    "CreateResourceShare",
    "DeletePermission",
    "DeletePermissionVersion",
    "DeleteResourceShare",
    "DisassociateResourceShare",
    "DisassociateResourceSharePermission",
    "EnableSharingWithAwsOrganization",
    "GetPermission",
    "GetResourcePolicies",
    "GetResourceShareAssociations",
    "GetResourceShareInvitations",
    "GetResourceShares",
    "ListPendingInvitationResources",
    "ListPermissionAssociations",
    "ListPermissionVersions",
    "ListPermissions",
    "ListPrincipals",
    "ListReplacePermissionAssociationsWork",
    "ListResourceSharePermissions",
    "ListResourceTypes",
    "ListResources",
    "ListSourceAssociations",
    "PromotePermissionCreatedFromPolicy",
    "PromoteResourceShareCreatedFromPolicy",
    "RejectResourceShareInvitation",
    "ReplacePermissionAssociations",
    "SetDefaultPermissionVersion",
    "TagResource",
    "UntagResource",
    "UpdateResourceShare",
];

const MUTATING: &[&str] = &[
    "AcceptResourceShareInvitation",
    "AssociateResourceShare",
    "AssociateResourceSharePermission",
    "CreatePermission",
    "CreatePermissionVersion",
    "CreateResourceShare",
    "DeletePermission",
    "DeletePermissionVersion",
    "DeleteResourceShare",
    "DisassociateResourceShare",
    "DisassociateResourceSharePermission",
    // EnableSharingWithAwsOrganization is intentionally NOT listed: it records
    // no state (just returns `{returnValue:true}`), so snapshotting it would
    // clone + persist the whole store on a no-op.
    "PromotePermissionCreatedFromPolicy",
    "PromoteResourceShareCreatedFromPolicy",
    "RejectResourceShareInvitation",
    "ReplacePermissionAssociations",
    "SetDefaultPermissionVersion",
    "TagResource",
    "UntagResource",
    "UpdateResourceShare",
];

/// AWS-managed default permissions seeded for the read paths: `(name, resourceType)`.
const MANAGED_PERMISSIONS: &[(&str, &str)] = &[
    ("AWSRAMDefaultPermissionSubnet", "ec2:Subnet"),
    (
        "AWSRAMDefaultPermissionTransitGateway",
        "ec2:TransitGateway",
    ),
    (
        "AWSRAMDefaultPermissionResolverRule",
        "route53resolver:ResolverRule",
    ),
    ("AWSRAMDefaultPermissionPrefixList", "ec2:PrefixList"),
    (
        "AWSRAMDefaultPermissionLicenseConfiguration",
        "license-manager:LicenseConfiguration",
    ),
];

/// Resource types RAM can share: `(resourceType, serviceName, resourceRegionScope)`.
const RESOURCE_TYPES: &[(&str, &str, &str)] = &[
    ("ec2:Subnet", "ec2", "REGIONAL"),
    ("ec2:TransitGateway", "ec2", "REGIONAL"),
    ("ec2:PrefixList", "ec2", "REGIONAL"),
    ("ec2:LocalGatewayRouteTable", "ec2", "REGIONAL"),
    (
        "route53resolver:ResolverRule",
        "route53resolver",
        "REGIONAL",
    ),
    (
        "license-manager:LicenseConfiguration",
        "license-manager",
        "GLOBAL",
    ),
];

pub struct RamService {
    state: SharedRamState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl RamService {
    pub fn new(state: SharedRamState) -> Self {
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

    async fn save_snapshot(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an action name by method + single path segment.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let seg = raw.strip_prefix('/').unwrap_or(raw);
        let seg = seg.strip_suffix('/').unwrap_or(seg).to_ascii_lowercase();
        let action = match (&req.method, seg.as_str()) {
            (&Method::POST, "acceptresourceshareinvitation") => "AcceptResourceShareInvitation",
            (&Method::POST, "associateresourceshare") => "AssociateResourceShare",
            (&Method::POST, "associateresourcesharepermission") => {
                "AssociateResourceSharePermission"
            }
            (&Method::POST, "createpermission") => "CreatePermission",
            (&Method::POST, "createpermissionversion") => "CreatePermissionVersion",
            (&Method::POST, "createresourceshare") => "CreateResourceShare",
            (&Method::DELETE, "deletepermission") => "DeletePermission",
            (&Method::DELETE, "deletepermissionversion") => "DeletePermissionVersion",
            (&Method::DELETE, "deleteresourceshare") => "DeleteResourceShare",
            (&Method::POST, "disassociateresourceshare") => "DisassociateResourceShare",
            (&Method::POST, "disassociateresourcesharepermission") => {
                "DisassociateResourceSharePermission"
            }
            (&Method::POST, "enablesharingwithawsorganization") => {
                "EnableSharingWithAwsOrganization"
            }
            (&Method::POST, "getpermission") => "GetPermission",
            (&Method::POST, "getresourcepolicies") => "GetResourcePolicies",
            (&Method::POST, "getresourceshareassociations") => "GetResourceShareAssociations",
            (&Method::POST, "getresourceshareinvitations") => "GetResourceShareInvitations",
            (&Method::POST, "getresourceshares") => "GetResourceShares",
            (&Method::POST, "listpendinginvitationresources") => "ListPendingInvitationResources",
            (&Method::POST, "listpermissionassociations") => "ListPermissionAssociations",
            (&Method::POST, "listpermissionversions") => "ListPermissionVersions",
            (&Method::POST, "listpermissions") => "ListPermissions",
            (&Method::POST, "listprincipals") => "ListPrincipals",
            (&Method::POST, "listreplacepermissionassociationswork") => {
                "ListReplacePermissionAssociationsWork"
            }
            (&Method::POST, "listresourcesharepermissions") => "ListResourceSharePermissions",
            (&Method::POST, "listresourcetypes") => "ListResourceTypes",
            (&Method::POST, "listresources") => "ListResources",
            (&Method::POST, "listsourceassociations") => "ListSourceAssociations",
            (&Method::POST, "promotepermissioncreatedfrompolicy") => {
                "PromotePermissionCreatedFromPolicy"
            }
            (&Method::POST, "promoteresourcesharecreatedfrompolicy") => {
                "PromoteResourceShareCreatedFromPolicy"
            }
            (&Method::POST, "rejectresourceshareinvitation") => "RejectResourceShareInvitation",
            (&Method::POST, "replacepermissionassociations") => "ReplacePermissionAssociations",
            (&Method::POST, "setdefaultpermissionversion") => "SetDefaultPermissionVersion",
            (&Method::POST, "tagresource") => "TagResource",
            (&Method::POST, "untagresource") => "UntagResource",
            (&Method::POST, "updateresourceshare") => "UpdateResourceShare",
            _ => return None,
        };
        Some(action)
    }
}

#[async_trait]
impl AwsService for RamService {
    fn service_name(&self) -> &str {
        "ram"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let result = self.dispatch(action, &req);

        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        RAM_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn gen_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn aws_err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}
fn unknown_resource(msg: impl Into<String>) -> AwsServiceError {
    aws_err("UnknownResourceException", msg)
}
fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    aws_err("InvalidParameterException", msg)
}
fn invitation_not_found(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ResourceShareInvitationArnNotFoundException", msg)
}

fn req_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

/// Read an `@httpQuery`-bound parameter (used by the `Delete*` operations,
/// which carry their identifiers in the query string rather than a body).
fn query_str(req: &AwsRequest, key: &str) -> Option<String> {
    req.query_params.get(key).cloned()
}

fn req_str(body: &Value, key: &str) -> Result<String, AwsServiceError> {
    str_field(body, key).ok_or_else(|| invalid_param(format!("{key} is required.")))
}

fn bool_field(body: &Value, key: &str) -> Option<bool> {
    body.get(key).and_then(|v| v.as_bool())
}

fn u32_field(body: &Value, key: &str) -> Option<u32> {
    body.get(key).and_then(|v| v.as_u64()).map(|n| n as u32)
}

fn string_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a `TagList` (`[{key,value}]`) into a map.
fn parse_tag_list(body: &Value, key: &str) -> TagMap {
    let mut out = TagMap::new();
    if let Some(arr) = body.get(key).and_then(|v| v.as_array()) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("key").and_then(|x| x.as_str()),
                t.get("value").and_then(|x| x.as_str()),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

fn tag_list_value(t: &TagMap) -> Value {
    Value::Array(
        t.iter()
            .map(|(k, v)| json!({ "key": k, "value": v }))
            .collect(),
    )
}

/// Whether a share matches a `resourceOwner` filter for the calling account.
/// `SELF` selects shares the account owns; `OTHER_ACCOUNTS` selects inbound
/// shares owned by another account (materialised into the account's partition
/// when its invitation was accepted).
fn owner_matches(share: &ResourceShareRecord, owner: &str, account_id: &str) -> bool {
    if owner == "SELF" {
        share.owning_account_id == account_id
    } else {
        share.owning_account_id != account_id
    }
}

/// Derive the RAM resource type (`<service>:<TypePascalCase>`, e.g.
/// `ec2:Subnet`, `ec2:TransitGateway`) from a shared resource's ARN, matching
/// the naming used by the seeded `ListResourceTypes` catalogue.
fn ram_resource_type(arn: &str) -> String {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 6 {
        let service = parts[2];
        let resource = parts[5..].join(":");
        let kind = resource.split(['/', ':']).next().unwrap_or(&resource);
        if !service.is_empty() && !kind.is_empty() {
            let pascal: String = kind
                .split('-')
                .map(|w| {
                    let mut c = w.chars();
                    match c.next() {
                        Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
                        None => String::new(),
                    }
                })
                .collect();
            return format!("{service}:{pascal}");
        }
    }
    "unknown".to_string()
}

/// True when a principal id is an external AWS account (12-digit account id or
/// an account/org ARN) rather than the caller's own account.
fn is_external_principal(principal: &str, account_id: &str) -> bool {
    if principal == account_id {
        return false;
    }
    let is_account = principal.len() == 12 && principal.chars().all(|c| c.is_ascii_digit());
    is_account || principal.starts_with("arn:aws:organizations::")
}

/// Numeric-offset pagination reading `maxResults`/`nextToken` from the JSON body.
fn page_response(
    items: Vec<Value>,
    list_key: &str,
    body: &Value,
    extra: &[(&str, Value)],
) -> Result<AwsResponse, AwsServiceError> {
    let max = body
        .get("maxResults")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .filter(|n| *n > 0)
        .unwrap_or(500);
    let token = body
        .get("nextToken")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let (page, next) = paginate_checked(&items, token.as_deref(), max)
        .map_err(|_| aws_err("InvalidNextTokenException", "Invalid nextToken"))?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(t) = next {
        out.insert("nextToken".to_string(), Value::String(t));
    }
    for (k, v) in extra {
        out.insert((*k).to_string(), v.clone());
    }
    Ok(ok(Value::Object(out)))
}

// ---------------------------------------------------------------------------
// Value builders
// ---------------------------------------------------------------------------

fn share_to_value(r: &ResourceShareRecord) -> Value {
    let mut m = Map::new();
    m.insert("resourceShareArn".into(), json!(r.arn));
    m.insert("name".into(), json!(r.name));
    m.insert("owningAccountId".into(), json!(r.owning_account_id));
    m.insert(
        "allowExternalPrincipals".into(),
        json!(r.allow_external_principals),
    );
    m.insert("status".into(), json!(r.status));
    if let Some(msg) = &r.status_message {
        m.insert("statusMessage".into(), json!(msg));
    }
    m.insert("tags".into(), tag_list_value(&r.tags));
    m.insert("creationTime".into(), ts(r.creation_time));
    m.insert("lastUpdatedTime".into(), ts(r.last_updated_time));
    m.insert("featureSet".into(), json!(r.feature_set));
    if let Some(retain) = r.retain_sharing {
        m.insert(
            "resourceShareConfiguration".into(),
            json!({ "retainSharingOnAccountLeaveOrganization": retain }),
        );
    }
    Value::Object(m)
}

fn association_to_value(arn: &str, name: &str, a: &AssociationRecord) -> Value {
    let mut m = Map::new();
    m.insert("resourceShareArn".into(), json!(arn));
    m.insert("resourceShareName".into(), json!(name));
    m.insert("associatedEntity".into(), json!(a.associated_entity));
    m.insert("associationType".into(), json!(a.association_type));
    m.insert("status".into(), json!(a.status));
    if let Some(msg) = &a.status_message {
        m.insert("statusMessage".into(), json!(msg));
    }
    m.insert("creationTime".into(), ts(a.creation_time));
    m.insert("lastUpdatedTime".into(), ts(a.last_updated_time));
    m.insert("external".into(), json!(a.external));
    Value::Object(m)
}

fn invitation_to_value(inv: &InvitationRecord) -> Value {
    json!({
        "resourceShareInvitationArn": inv.arn,
        "resourceShareName": inv.resource_share_name,
        "resourceShareArn": inv.resource_share_arn,
        "senderAccountId": inv.sender_account_id,
        "receiverAccountId": inv.receiver_account_id,
        "invitationTimestamp": ts(inv.invitation_timestamp),
        "status": inv.status,
    })
}

fn managed_summary(name: &str, resource_type: &str, now: DateTime<Utc>) -> Value {
    json!({
        "arn": managed_permission_arn(name),
        "version": "1",
        "defaultVersion": true,
        "name": name,
        "resourceType": resource_type,
        "status": "ATTACHABLE",
        "creationTime": ts(now),
        "lastUpdatedTime": ts(now),
        "isResourceTypeDefault": true,
        "permissionType": "AWS_MANAGED",
        "featureSet": "STANDARD",
        "tags": [],
    })
}

fn customer_summary(p: &PermissionRecord) -> Value {
    json!({
        "arn": p.arn,
        "version": p.default_version.to_string(),
        "defaultVersion": true,
        "name": p.name,
        "resourceType": p.resource_type,
        "status": p.status,
        "creationTime": ts(p.creation_time),
        "lastUpdatedTime": ts(p.last_updated_time),
        "isResourceTypeDefault": false,
        "permissionType": "CUSTOMER_MANAGED",
        "featureSet": p.feature_set,
        "tags": tag_list_value(&p.tags),
    })
}

fn customer_detail(p: &PermissionRecord, v: &PermissionVersionRecord) -> Value {
    json!({
        "arn": p.arn,
        "version": v.version.to_string(),
        "defaultVersion": v.is_default,
        "name": p.name,
        "resourceType": p.resource_type,
        "permission": v.policy_template,
        "creationTime": ts(v.creation_time),
        "lastUpdatedTime": ts(v.last_updated_time),
        "isResourceTypeDefault": false,
        "permissionType": "CUSTOMER_MANAGED",
        "featureSet": p.feature_set,
        "status": p.status,
        "tags": tag_list_value(&p.tags),
    })
}

fn managed_detail(name: &str, resource_type: &str, now: DateTime<Utc>) -> Value {
    let policy = json!({
        "Effect": "Allow",
        "Action": [format!("{}:*", resource_type.split(':').next().unwrap_or("*"))],
    })
    .to_string();
    json!({
        "arn": managed_permission_arn(name),
        "version": "1",
        "defaultVersion": true,
        "name": name,
        "resourceType": resource_type,
        "permission": policy,
        "creationTime": ts(now),
        "lastUpdatedTime": ts(now),
        "isResourceTypeDefault": true,
        "permissionType": "AWS_MANAGED",
        "featureSet": "STANDARD",
        "status": "ATTACHABLE",
        "tags": [],
    })
}

fn find_managed(arn: &str) -> Option<(&'static str, &'static str)> {
    MANAGED_PERMISSIONS
        .iter()
        .find(|(name, _)| managed_permission_arn(name) == arn)
        .copied()
}

// ---------------------------------------------------------------------------
// Dispatch + handlers
// ---------------------------------------------------------------------------

impl RamService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate::validate(action, req)?;
        let body = req_body(req);
        match action {
            "CreateResourceShare" => self.create_resource_share(req, &body),
            "UpdateResourceShare" => self.update_resource_share(req, &body),
            "DeleteResourceShare" => self.delete_resource_share(req, &body),
            "GetResourceShares" => self.get_resource_shares(req, &body),
            "AssociateResourceShare" => self.associate_resource_share(req, &body, true),
            "DisassociateResourceShare" => self.associate_resource_share(req, &body, false),
            "AssociateResourceSharePermission" => self.associate_permission(req, &body, true),
            "DisassociateResourceSharePermission" => self.associate_permission(req, &body, false),
            "GetResourceShareAssociations" => self.get_share_associations(req, &body),
            "GetResourceShareInvitations" => self.get_invitations(req, &body),
            "AcceptResourceShareInvitation" => self.respond_invitation(req, &body, true),
            "RejectResourceShareInvitation" => self.respond_invitation(req, &body, false),
            "ListPendingInvitationResources" => self.list_pending_invitation_resources(req, &body),
            "EnableSharingWithAwsOrganization" => Ok(ok(json!({ "returnValue": true }))),
            "CreatePermission" => self.create_permission(req, &body),
            "CreatePermissionVersion" => self.create_permission_version(req, &body),
            "DeletePermission" => self.delete_permission(req, &body),
            "DeletePermissionVersion" => self.delete_permission_version(req, &body),
            "GetPermission" => self.get_permission(req, &body),
            "ListPermissions" => self.list_permissions(req, &body),
            "ListPermissionVersions" => self.list_permission_versions(req, &body),
            "ListResourceSharePermissions" => self.list_resource_share_permissions(req, &body),
            "SetDefaultPermissionVersion" => self.set_default_permission_version(req, &body),
            "PromotePermissionCreatedFromPolicy" => self.promote_permission(req, &body),
            "PromoteResourceShareCreatedFromPolicy" => self.promote_resource_share(req, &body),
            "ReplacePermissionAssociations" => self.replace_permission_associations(req, &body),
            "ListReplacePermissionAssociationsWork" => self.list_replace_work(req, &body),
            "ListPermissionAssociations" => self.list_permission_associations(req, &body),
            "ListPrincipals" => self.list_principals(req, &body),
            "ListResources" => self.list_resources(req, &body),
            "ListSourceAssociations" => page_response(Vec::new(), "sourceAssociations", &body, &[]),
            "ListResourceTypes" => self.list_resource_types(req, &body),
            "GetResourcePolicies" => self.get_resource_policies(req, &body),
            "TagResource" => self.tag_resource(req, &body),
            "UntagResource" => self.untag_resource(req, &body),
            _ => Err(AwsServiceError::action_not_implemented("ram", action)),
        }
    }

    // ---------------- resource shares ----------------

    fn create_resource_share(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(body, "name")?;
        let now = Utc::now();
        let id = gen_id();
        let arn = resource_share_arn(&req.region, &req.account_id, &id);
        let tags = parse_tag_list(body, "tags");
        let retain = body
            .get("resourceShareConfiguration")
            .and_then(|c| c.get("retainSharingOnAccountLeaveOrganization"))
            .and_then(|v| v.as_bool());

        let mut resource_associations = Vec::new();
        for r in string_list(body, "resourceArns") {
            resource_associations.push(AssociationRecord {
                associated_entity: r,
                association_type: "RESOURCE".into(),
                status: "ASSOCIATED".into(),
                status_message: None,
                creation_time: now,
                last_updated_time: now,
                external: false,
            });
        }
        let mut principal_associations = Vec::new();
        let mut invitations = Vec::new();
        for p in string_list(body, "principals") {
            let external = is_external_principal(&p, &req.account_id);
            principal_associations.push(AssociationRecord {
                associated_entity: p.clone(),
                association_type: "PRINCIPAL".into(),
                status: "ASSOCIATED".into(),
                status_message: None,
                creation_time: now,
                last_updated_time: now,
                external,
            });
            // Sharing to an external account raises a pending invitation.
            if external && p.len() == 12 && p.chars().all(|c| c.is_ascii_digit()) {
                invitations.push(InvitationRecord {
                    arn: invitation_arn(&req.region, &req.account_id, &gen_id()),
                    resource_share_arn: arn.clone(),
                    resource_share_name: name.clone(),
                    sender_account_id: req.account_id.clone(),
                    receiver_account_id: p.clone(),
                    invitation_timestamp: now,
                    status: "PENDING".into(),
                });
            }
        }

        let record = ResourceShareRecord {
            arn: arn.clone(),
            name,
            owning_account_id: req.account_id.clone(),
            allow_external_principals: bool_field(body, "allowExternalPrincipals").unwrap_or(true),
            status: "ACTIVE".into(),
            status_message: None,
            creation_time: now,
            last_updated_time: now,
            feature_set: "STANDARD".into(),
            tags,
            retain_sharing: retain,
            resource_associations,
            principal_associations,
            permission_arns: string_list(body, "permissionArns"),
        };
        let value = share_to_value(&record);

        let mut accounts = self.state.write();
        accounts
            .get_or_create(&req.account_id)
            .resource_shares
            .insert(arn, record);
        // Invitations are receiver-side objects: store each one in the receiver
        // account's partition so that account's GetResourceShareInvitations /
        // Accept / Reject can find it.
        for inv in invitations {
            let receiver = inv.receiver_account_id.clone();
            accounts
                .get_or_create(&receiver)
                .invitations
                .insert(inv.arn.clone(), inv);
        }

        Ok(ok(json!({
            "resourceShare": value,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn update_resource_share(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let share = st
            .resource_shares
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))?;
        if let Some(name) = str_field(body, "name") {
            share.name = name;
        }
        if let Some(allow) = bool_field(body, "allowExternalPrincipals") {
            share.allow_external_principals = allow;
        }
        share.last_updated_time = Utc::now();
        let value = share_to_value(share);
        Ok(ok(json!({
            "resourceShare": value,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn delete_resource_share(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = query_str(req, "resourceShareArn")
            .ok_or_else(|| invalid_param("resourceShareArn is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.resource_shares.remove(&arn).is_none() {
            return Err(unknown_resource(format!(
                "ResourceShare {arn} could not be found."
            )));
        }
        st.tags.remove(&arn);
        Ok(ok(json!({
            "returnValue": true,
            "clientToken": query_str(req, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn get_resource_shares(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let owner = req_str(body, "resourceOwner")?;
        let arns_filter = string_list(body, "resourceShareArns");
        let status_filter = str_field(body, "resourceShareStatus");
        let name_filter = str_field(body, "name");
        let accounts = self.state.read();
        // SELF lists the account's own shares; OTHER_ACCOUNTS lists inbound
        // shares (accepted invitations) whose owner is a different account.
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.resource_shares
                    .values()
                    .filter(|s| owner_matches(s, &owner, &req.account_id))
                    .filter(|s| arns_filter.is_empty() || arns_filter.contains(&s.arn))
                    .filter(|s| status_filter.as_deref().is_none_or(|st| st == s.status))
                    .filter(|s| name_filter.as_deref().is_none_or(|n| n == s.name))
                    .map(share_to_value)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "resourceShares", body, &[])
    }

    fn associate_resource_share(
        &self,
        req: &AwsRequest,
        body: &Value,
        associate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareArn")?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let share = st
            .resource_shares
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))?;
        let name = share.name.clone();
        let mut affected: Vec<Value> = Vec::new();
        let status = if associate {
            "ASSOCIATED"
        } else {
            "DISASSOCIATED"
        };

        for r in string_list(body, "resourceArns") {
            if associate {
                share.resource_associations.push(AssociationRecord {
                    associated_entity: r.clone(),
                    association_type: "RESOURCE".into(),
                    status: status.into(),
                    status_message: None,
                    creation_time: now,
                    last_updated_time: now,
                    external: false,
                });
            } else {
                share
                    .resource_associations
                    .retain(|a| a.associated_entity != r);
            }
            affected.push(association_to_value(
                &arn,
                &name,
                &AssociationRecord {
                    associated_entity: r,
                    association_type: "RESOURCE".into(),
                    status: status.into(),
                    status_message: None,
                    creation_time: now,
                    last_updated_time: now,
                    external: false,
                },
            ));
        }
        for p in string_list(body, "principals") {
            let external = is_external_principal(&p, &req.account_id);
            if associate {
                share.principal_associations.push(AssociationRecord {
                    associated_entity: p.clone(),
                    association_type: "PRINCIPAL".into(),
                    status: status.into(),
                    status_message: None,
                    creation_time: now,
                    last_updated_time: now,
                    external,
                });
            } else {
                share
                    .principal_associations
                    .retain(|a| a.associated_entity != p);
            }
            affected.push(association_to_value(
                &arn,
                &name,
                &AssociationRecord {
                    associated_entity: p,
                    association_type: "PRINCIPAL".into(),
                    status: status.into(),
                    status_message: None,
                    creation_time: now,
                    last_updated_time: now,
                    external,
                },
            ));
        }
        share.last_updated_time = now;
        Ok(ok(json!({
            "resourceShareAssociations": affected,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn associate_permission(
        &self,
        req: &AwsRequest,
        body: &Value,
        associate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareArn")?;
        let permission = req_str(body, "permissionArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let share = st
            .resource_shares
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))?;
        if associate {
            if !share.permission_arns.contains(&permission) {
                share.permission_arns.push(permission);
            }
        } else {
            share.permission_arns.retain(|p| p != &permission);
        }
        share.last_updated_time = Utc::now();
        Ok(ok(json!({
            "returnValue": true,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn get_share_associations(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let assoc_type = req_str(body, "associationType")?;
        let arns_filter = string_list(body, "resourceShareArns");
        let status_filter = str_field(body, "associationStatus");
        let accounts = self.state.read();
        let mut items: Vec<Value> = Vec::new();
        if let Some(st) = accounts.get(&_req.account_id) {
            for share in st.resource_shares.values() {
                if !arns_filter.is_empty() && !arns_filter.contains(&share.arn) {
                    continue;
                }
                let src = if assoc_type == "PRINCIPAL" {
                    &share.principal_associations
                } else {
                    &share.resource_associations
                };
                for a in src {
                    if status_filter.as_deref().is_some_and(|s| s != a.status) {
                        continue;
                    }
                    items.push(association_to_value(&share.arn, &share.name, a));
                }
            }
        }
        page_response(items, "resourceShareAssociations", body, &[])
    }

    fn get_invitations(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inv_filter = string_list(body, "resourceShareInvitationArns");
        let share_filter = string_list(body, "resourceShareArns");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.invitations
                    .values()
                    .filter(|i| inv_filter.is_empty() || inv_filter.contains(&i.arn))
                    .filter(|i| {
                        share_filter.is_empty() || share_filter.contains(&i.resource_share_arn)
                    })
                    .map(invitation_to_value)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "resourceShareInvitations", body, &[])
    }

    fn respond_invitation(
        &self,
        req: &AwsRequest,
        body: &Value,
        accept: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareInvitationArn")?;
        let mut accounts = self.state.write();
        // The invitation lives in the receiver (caller) account's partition.
        let st = accounts.get_or_create(&req.account_id);
        let inv = st.invitations.get_mut(&arn).ok_or_else(|| {
            invitation_not_found(format!("ResourceShareInvitation {arn} could not be found."))
        })?;
        if inv.status != "PENDING" {
            let code = if inv.status == "ACCEPTED" {
                "ResourceShareInvitationAlreadyAcceptedException"
            } else {
                "ResourceShareInvitationAlreadyRejectedException"
            };
            return Err(aws_err(code, format!("Invitation {arn} is {}", inv.status)));
        }
        inv.status = if accept { "ACCEPTED" } else { "REJECTED" }.into();
        let value = invitation_to_value(inv);
        let sender = inv.sender_account_id.clone();
        let share_arn = inv.resource_share_arn.clone();

        if accept {
            // Materialise an inbound view of the shared share in the receiver's
            // partition so GetResourceShares(OTHER_ACCOUNTS) / ListResources /
            // ListPrincipals surface it. The clone keeps owningAccountId set to
            // the sender, which the `resourceOwner` filter uses to tell inbound
            // shares apart from the caller's own.
            if let Some(shared) = accounts
                .get(&sender)
                .and_then(|s| s.resource_shares.get(&share_arn))
                .cloned()
            {
                accounts
                    .get_or_create(&req.account_id)
                    .resource_shares
                    .insert(share_arn, shared);
            }
        }
        Ok(ok(json!({
            "resourceShareInvitation": value,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn list_pending_invitation_resources(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareInvitationArn")?;
        let accounts = self.state.read();
        let found = accounts
            .get(&req.account_id)
            .is_some_and(|st| st.invitations.contains_key(&arn));
        if !found {
            return Err(invitation_not_found(format!(
                "ResourceShareInvitation {arn} could not be found."
            )));
        }
        page_response(Vec::new(), "resources", body, &[])
    }

    // ---------------- permissions ----------------

    fn create_permission(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = req_str(body, "name")?;
        let resource_type = req_str(body, "resourceType")?;
        let policy = req_str(body, "policyTemplate")?;
        let now = Utc::now();
        let arn = permission_arn(&req.region, &req.account_id, &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.permissions.contains_key(&arn) {
            return Err(aws_err(
                "PermissionAlreadyExistsException",
                format!("A permission named {name} already exists."),
            ));
        }
        let record = PermissionRecord {
            arn: arn.clone(),
            name,
            resource_type,
            status: "ATTACHABLE".into(),
            feature_set: "STANDARD".into(),
            creation_time: now,
            last_updated_time: now,
            default_version: 1,
            versions: vec![PermissionVersionRecord {
                version: 1,
                policy_template: policy,
                is_default: true,
                creation_time: now,
                last_updated_time: now,
            }],
            tags: parse_tag_list(body, "tags"),
        };
        let value = customer_summary(&record);
        st.permissions.insert(arn, record);
        Ok(ok(json!({
            "permission": value,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn create_permission_version(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "permissionArn")?;
        let policy = req_str(body, "policyTemplate")?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .permissions
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        let next = p.versions.iter().map(|v| v.version).max().unwrap_or(0) + 1;
        let rec = PermissionVersionRecord {
            version: next,
            policy_template: policy,
            is_default: false,
            creation_time: now,
            last_updated_time: now,
        };
        p.versions.push(rec.clone());
        p.last_updated_time = now;
        Ok(ok(json!({
            "permission": customer_detail(p, &rec),
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn delete_permission(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = query_str(req, "permissionArn")
            .ok_or_else(|| invalid_param("permissionArn is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.permissions.remove(&arn).is_none() {
            return Err(unknown_resource(format!(
                "Permission {arn} could not be found."
            )));
        }
        Ok(ok(json!({
            "returnValue": true,
            "clientToken": query_str(req, "clientToken").unwrap_or_else(gen_id),
            "permissionStatus": "DELETED",
        })))
    }

    fn delete_permission_version(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = query_str(req, "permissionArn")
            .ok_or_else(|| invalid_param("permissionArn is required."))?;
        let version = query_str(req, "permissionVersion")
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| invalid_param("permissionVersion is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .permissions
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        if !p.versions.iter().any(|v| v.version == version) {
            return Err(invalid_param(format!(
                "Permission version {version} could not be found."
            )));
        }
        // AWS refuses to delete the default version (it would leave the
        // permission without a resolvable default); the caller must first move
        // the default with SetDefaultPermissionVersion.
        if version == p.default_version {
            return Err(aws_err(
                "OperationNotPermittedException",
                format!("The default version {version} of a permission cannot be deleted."),
            ));
        }
        p.versions.retain(|v| v.version != version);
        // A non-default version was removed, so at least the default remains.
        let status = "ATTACHABLE";
        Ok(ok(json!({
            "returnValue": true,
            "clientToken": query_str(req, "clientToken").unwrap_or_else(gen_id),
            "permissionStatus": status,
        })))
    }

    fn get_permission(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "permissionArn")?;
        let now = Utc::now();
        if let Some((name, rt)) = find_managed(&arn) {
            return Ok(ok(json!({ "permission": managed_detail(name, rt, now) })));
        }
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|st| st.permissions.get(&arn))
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        let want = u32_field(body, "permissionVersion");
        let v = match want {
            Some(n) => p.versions.iter().find(|v| v.version == n),
            None => p.versions.iter().find(|v| v.version == p.default_version),
        }
        .ok_or_else(|| invalid_param("Permission version could not be found."))?;
        Ok(ok(json!({ "permission": customer_detail(p, v) })))
    }

    fn list_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let now = Utc::now();
        let type_filter = str_field(body, "resourceType");
        let perm_type = str_field(body, "permissionType").unwrap_or_else(|| "ALL".into());
        let mut items: Vec<Value> = Vec::new();
        if perm_type == "ALL" || perm_type == "AWS_MANAGED" {
            for (name, rt) in MANAGED_PERMISSIONS {
                if type_filter.as_deref().is_none_or(|t| t == *rt) {
                    items.push(managed_summary(name, rt, now));
                }
            }
        }
        if perm_type == "ALL" || perm_type == "CUSTOMER_MANAGED" {
            if let Some(st) = self.state.read().get(&req.account_id) {
                for p in st.permissions.values() {
                    if type_filter.as_deref().is_none_or(|t| t == p.resource_type) {
                        items.push(customer_summary(p));
                    }
                }
            }
        }
        page_response(items, "permissions", body, &[])
    }

    fn list_permission_versions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "permissionArn")?;
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|st| st.permissions.get(&arn))
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        let items: Vec<Value> = p
            .versions
            .iter()
            .map(|v| {
                json!({
                    "arn": p.arn,
                    "version": v.version.to_string(),
                    "defaultVersion": v.is_default,
                    "name": p.name,
                    "resourceType": p.resource_type,
                    "status": p.status,
                    "creationTime": ts(v.creation_time),
                    "lastUpdatedTime": ts(v.last_updated_time),
                    "isResourceTypeDefault": false,
                    "permissionType": "CUSTOMER_MANAGED",
                    "featureSet": p.feature_set,
                    "tags": tag_list_value(&p.tags),
                })
            })
            .collect();
        page_response(items, "permissions", body, &[])
    }

    fn list_resource_share_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareArn")?;
        let now = Utc::now();
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .filter(|st| st.resource_shares.contains_key(&arn))
            .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))?;
        let share = &st.resource_shares[&arn];
        let mut items: Vec<Value> = Vec::new();
        for perm_arn in &share.permission_arns {
            if let Some((name, rt)) = find_managed(perm_arn) {
                items.push(managed_summary(name, rt, now));
            } else if let Some(p) = st.permissions.get(perm_arn) {
                items.push(customer_summary(p));
            }
        }
        page_response(items, "permissions", body, &[])
    }

    fn set_default_permission_version(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "permissionArn")?;
        let version = u32_field(body, "permissionVersion")
            .ok_or_else(|| invalid_param("permissionVersion is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .permissions
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        if !p.versions.iter().any(|v| v.version == version) {
            return Err(invalid_param(format!(
                "Permission version {version} could not be found."
            )));
        }
        p.default_version = version;
        for v in &mut p.versions {
            v.is_default = v.version == version;
        }
        p.last_updated_time = Utc::now();
        Ok(ok(json!({
            "returnValue": true,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn promote_permission(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "permissionArn")?;
        let _name = req_str(body, "name")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .permissions
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("Permission {arn} could not be found.")))?;
        p.feature_set = "STANDARD".into();
        p.last_updated_time = Utc::now();
        Ok(ok(json!({
            "permission": customer_summary(p),
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn promote_resource_share(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_str(body, "resourceShareArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let share = st
            .resource_shares
            .get_mut(&arn)
            .ok_or_else(|| unknown_resource(format!("ResourceShare {arn} could not be found.")))?;
        share.feature_set = "STANDARD".into();
        share.last_updated_time = Utc::now();
        Ok(ok(json!({ "returnValue": true })))
    }

    fn replace_permission_associations(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let from = req_str(body, "fromPermissionArn")?;
        let to = req_str(body, "toPermissionArn")?;
        let now = Utc::now();
        // The source permission must exist (managed or customer-managed).
        let known = find_managed(&from).is_some()
            || self
                .state
                .read()
                .get(&req.account_id)
                .is_some_and(|st| st.permissions.contains_key(&from));
        if !known {
            return Err(unknown_resource(format!(
                "Permission {from} could not be found."
            )));
        }
        let from_version = u32_field(body, "fromPermissionVersion").unwrap_or(1);
        let id = gen_id();
        let work = json!({
            "id": id,
            "fromPermissionArn": from,
            "fromPermissionVersion": from_version.to_string(),
            "toPermissionArn": to,
            "toPermissionVersion": "1",
            "status": "IN_PROGRESS",
            "creationTime": ts(now),
            "lastUpdatedTime": ts(now),
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.replace_works.insert(id, work.clone());
        Ok(ok(json!({
            "replacePermissionAssociationsWork": work,
            "clientToken": str_field(body, "clientToken").unwrap_or_else(gen_id),
        })))
    }

    fn list_permission_associations(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let perm_filter = str_field(body, "permissionArn");
        let version_filter = u32_field(body, "permissionVersion");
        let default_filter = bool_field(body, "defaultVersion");
        let status_filter = str_field(body, "associationStatus");
        let type_filter = str_field(body, "resourceType");
        let mut items: Vec<Value> = Vec::new();
        if let Some(st) = self.state.read().get(&req.account_id) {
            for share in st.resource_shares.values() {
                for perm_arn in &share.permission_arns {
                    if perm_filter.as_deref().is_some_and(|p| p != perm_arn) {
                        continue;
                    }
                    // Resolve the permission's type / version / feature set from
                    // the managed catalogue or the account's customer permissions.
                    let (rtype, version, feature) = if let Some((_, rt)) = find_managed(perm_arn) {
                        (rt.to_string(), 1u32, "STANDARD".to_string())
                    } else if let Some(p) = st.permissions.get(perm_arn) {
                        (
                            p.resource_type.clone(),
                            p.default_version,
                            p.feature_set.clone(),
                        )
                    } else {
                        ("unknown".to_string(), 1, "STANDARD".to_string())
                    };
                    if type_filter.as_deref().is_some_and(|t| t != rtype) {
                        continue;
                    }
                    if version_filter.is_some_and(|v| v != version) {
                        continue;
                    }
                    // Every association here uses the default version.
                    if default_filter == Some(false) {
                        continue;
                    }
                    if status_filter.as_deref().is_some_and(|s| s != "ASSOCIATED") {
                        continue;
                    }
                    items.push(json!({
                        "arn": perm_arn,
                        "permissionVersion": version.to_string(),
                        "defaultVersion": true,
                        "resourceType": rtype,
                        "status": "ASSOCIATED",
                        "featureSet": feature,
                        "lastUpdatedTime": ts(share.last_updated_time),
                        "resourceShareArn": share.arn,
                    }));
                }
            }
        }
        page_response(items, "permissions", body, &[])
    }

    fn list_replace_work(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id_filter = string_list(body, "workIds");
        let status_filter = str_field(body, "status");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.replace_works
                    .iter()
                    .filter(|(id, _)| id_filter.is_empty() || id_filter.contains(id))
                    .filter(|(_, w)| {
                        status_filter
                            .as_deref()
                            .is_none_or(|s| w.get("status").and_then(|v| v.as_str()) == Some(s))
                    })
                    .map(|(_, w)| w.clone())
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "replacePermissionAssociationsWorks", body, &[])
    }

    // ---------------- resources / principals / types ----------------

    fn list_principals(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let owner = req_str(body, "resourceOwner")?;
        let principal_filter = string_list(body, "principals");
        let arns_filter = string_list(body, "resourceShareArns");
        let mut items: Vec<Value> = Vec::new();
        if let Some(st) = self.state.read().get(&req.account_id) {
            for share in st.resource_shares.values() {
                if !owner_matches(share, &owner, &req.account_id) {
                    continue;
                }
                if !arns_filter.is_empty() && !arns_filter.contains(&share.arn) {
                    continue;
                }
                for a in &share.principal_associations {
                    if !principal_filter.is_empty()
                        && !principal_filter.contains(&a.associated_entity)
                    {
                        continue;
                    }
                    items.push(json!({
                        "id": a.associated_entity,
                        "resourceShareArn": share.arn,
                        "creationTime": ts(a.creation_time),
                        "lastUpdatedTime": ts(a.last_updated_time),
                        "external": a.external,
                    }));
                }
            }
        }
        page_response(items, "principals", body, &[])
    }

    fn list_resources(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let owner = req_str(body, "resourceOwner")?;
        let arns_filter = string_list(body, "resourceArns");
        let type_filter = str_field(body, "resourceType");
        let mut items: Vec<Value> = Vec::new();
        if let Some(st) = self.state.read().get(&req.account_id) {
            for share in st.resource_shares.values() {
                if !owner_matches(share, &owner, &req.account_id) {
                    continue;
                }
                for a in &share.resource_associations {
                    if !arns_filter.is_empty() && !arns_filter.contains(&a.associated_entity) {
                        continue;
                    }
                    let rtype = ram_resource_type(&a.associated_entity);
                    if type_filter.as_deref().is_some_and(|t| t != rtype) {
                        continue;
                    }
                    items.push(json!({
                        "arn": a.associated_entity,
                        "type": rtype,
                        "resourceShareArn": share.arn,
                        "status": "AVAILABLE",
                        "creationTime": ts(a.creation_time),
                        "lastUpdatedTime": ts(a.last_updated_time),
                        "resourceRegionScope": "REGIONAL",
                    }));
                }
            }
        }
        page_response(items, "resources", body, &[])
    }

    fn list_resource_types(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let scope_filter = str_field(body, "resourceRegionScope");
        let items: Vec<Value> = RESOURCE_TYPES
            .iter()
            .filter(|(_, _, scope)| match scope_filter.as_deref() {
                None | Some("ALL") => true,
                Some(s) => s == *scope,
            })
            .map(|(rt, svc, scope)| {
                json!({
                    "resourceType": rt,
                    "serviceName": svc,
                    "resourceRegionScope": scope,
                })
            })
            .collect();
        page_response(items, "resourceTypes", body, &[])
    }

    fn get_resource_policies(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        // No shared-resource policies are materialised for synthetic ARNs.
        page_response(Vec::new(), "policies", body, &[])
    }

    // ---------------- tags ----------------

    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let tags = parse_tag_list(body, "tags");
        let target = str_field(body, "resourceShareArn").or_else(|| str_field(body, "resourceArn"));
        let target = target.ok_or_else(|| {
            invalid_param("Either resourceShareArn or resourceArn must be specified.")
        })?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(share) = st.resource_shares.get_mut(&target) {
            share.tags.extend(tags);
        } else if let Some(perm) = st.permissions.get_mut(&target) {
            perm.tags.extend(tags);
        } else {
            return Err(unknown_resource(format!(
                "Resource {target} could not be found."
            )));
        }
        Ok(ok(json!({})))
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let keys = string_list(body, "tagKeys");
        let target = str_field(body, "resourceShareArn").or_else(|| str_field(body, "resourceArn"));
        let target = target.ok_or_else(|| {
            invalid_param("Either resourceShareArn or resourceArn must be specified.")
        })?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(share) = st.resource_shares.get_mut(&target) {
            for k in &keys {
                share.tags.remove(k);
            }
        } else if let Some(perm) = st.permissions.get_mut(&target) {
            for k in &keys {
                perm.tags.remove(k);
            }
        } else {
            return Err(unknown_resource(format!(
                "Resource {target} could not be found."
            )));
        }
        Ok(ok(json!({})))
    }
}
