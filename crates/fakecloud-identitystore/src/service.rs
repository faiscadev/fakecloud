//! Identity Store (`identitystore`) awsJson1.1 dispatch + operation handlers.
//!
//! The full 19-operation directory control plane: users, groups, and the
//! memberships linking them, plus the attribute-lookup helpers
//! (`GetUserId`/`GetGroupId`/`GetGroupMembershipId`) and `IsMemberInGroups`.
//! State is account-partitioned and persisted. Nested SCIM attribute bags are
//! stored as the raw request `Value` so they round-trip verbatim.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{SharedIdentityStoreState, StoredGroup, StoredMembership, StoredUser};

/// Every operation name in the Identity Store Smithy model.
pub const IDENTITYSTORE_ACTIONS: &[&str] = &[
    "CreateGroup",
    "CreateGroupMembership",
    "CreateUser",
    "DeleteGroup",
    "DeleteGroupMembership",
    "DeleteUser",
    "DescribeGroup",
    "DescribeGroupMembership",
    "DescribeUser",
    "GetGroupId",
    "GetGroupMembershipId",
    "GetUserId",
    "IsMemberInGroups",
    "ListGroupMemberships",
    "ListGroupMembershipsForMember",
    "ListGroups",
    "ListUsers",
    "UpdateGroup",
    "UpdateUser",
];

/// Free-form user profile attributes, all `SensitiveStringType` (@length
/// 1..=1024) in the Smithy model.
const SENSITIVE_USER_FIELDS: &[&str] = &[
    "DisplayName",
    "NickName",
    "ProfileUrl",
    "UserType",
    "Title",
    "PreferredLanguage",
    "Locale",
    "Timezone",
    "Website",
    "Birthdate",
];

pub struct IdentityStoreService {
    state: SharedIdentityStoreState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl IdentityStoreService {
    pub fn new(state: SharedIdentityStoreState) -> Self {
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
impl AwsService for IdentityStoreService {
    fn service_name(&self) -> &str {
        "identitystore"
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
        IDENTITYSTORE_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    action.starts_with("Create") || action.starts_with("Delete") || action.starts_with("Update")
}

fn dispatch(s: &IdentityStoreService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        "CreateUser" => s.create_user(req),
        "DescribeUser" => s.describe_user(req),
        "UpdateUser" => s.update_user(req),
        "DeleteUser" => s.delete_user(req),
        "GetUserId" => s.get_user_id(req),
        "ListUsers" => s.list_users(req),
        "CreateGroup" => s.create_group(req),
        "DescribeGroup" => s.describe_group(req),
        "UpdateGroup" => s.update_group(req),
        "DeleteGroup" => s.delete_group(req),
        "GetGroupId" => s.get_group_id(req),
        "ListGroups" => s.list_groups(req),
        "CreateGroupMembership" => s.create_group_membership(req),
        "DescribeGroupMembership" => s.describe_group_membership(req),
        "DeleteGroupMembership" => s.delete_group_membership(req),
        "GetGroupMembershipId" => s.get_group_membership_id(req),
        "ListGroupMemberships" => s.list_group_memberships(req),
        "ListGroupMembershipsForMember" => s.list_group_memberships_for_member(req),
        "IsMemberInGroups" => s.is_member_in_groups(req),
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

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ConflictException", msg)
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

fn store_id(b: &Value) -> Result<String, AwsServiceError> {
    let s = req_str(b, "IdentityStoreId")?;
    // IdentityStoreId @length 1..=36.
    if s.chars().count() > 36 {
        return Err(validation(
            "IdentityStoreId must have length between 1 and 36, inclusive.",
        ));
    }
    Ok(s.to_string())
}

/// Read a required `ResourceId` field (`@length 1..=47`).
fn resource_id<'a>(b: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    let s = req_str(b, field)?;
    if s.chars().count() > 47 {
        return Err(validation(&format!(
            "{field} must have length between 1 and 47, inclusive."
        )));
    }
    Ok(s)
}

/// Validate the `MaxResults` (@range 1..=100) and `NextToken` (@length
/// 1..=65535) pagination inputs shared by the list operations.
fn check_pagination(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get("MaxResults") {
        let n = v.as_i64().unwrap_or(-1);
        if !(1..=100).contains(&n) {
            return Err(validation(
                "MaxResults must be between 1 and 100, inclusive.",
            ));
        }
    }
    check_len(b, "NextToken", 1, 65535)
}

fn epoch(dt: &DateTime<Utc>) -> Value {
    json!(dt.timestamp())
}

/// `MemberId` is a union; only the `UserId` member is modeled today. Its value
/// is a `ResourceId` (@length 1..=47).
fn member_user_id(b: &Value) -> Result<String, AwsServiceError> {
    let id = b
        .get("MemberId")
        .and_then(|m| m.get("UserId"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation("MemberId.UserId must be specified."))?;
    if id.chars().count() > 47 {
        return Err(validation(
            "MemberId.UserId must have length between 1 and 47, inclusive.",
        ));
    }
    Ok(id.to_string())
}

/// Validate pagination inputs, then window an ordered slice of result rows.
fn paginate(rows: Vec<Value>, b: &Value) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    check_pagination(b)?;
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
    Ok((page, next))
}

/// Set a (possibly dotted) attribute path on the bag; `None` removes it.
fn set_attribute(bag: &mut Map<String, Value>, path: &str, value: Option<Value>) {
    let mut parts = path.split('.').peekable();
    let head = match parts.next() {
        Some(h) => h,
        None => return,
    };
    if parts.peek().is_none() {
        match value {
            Some(v) => {
                bag.insert(head.to_string(), v);
            }
            None => {
                bag.remove(head);
            }
        }
        return;
    }
    let rest: String = parts.collect::<Vec<_>>().join(".");
    let child = bag
        .entry(head.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(m) = child {
        set_attribute(m, &rest, value);
    }
}

fn apply_operations(attributes: &mut Value, ops: &Value) {
    let Value::Object(bag) = attributes else {
        return;
    };
    let Some(list) = ops.as_array() else {
        return;
    };
    for op in list {
        let Some(path) = op.get("AttributePath").and_then(Value::as_str) else {
            continue;
        };
        set_attribute(bag, path, op.get("AttributeValue").cloned());
    }
}

fn build_user(u: &StoredUser, store: &str) -> Value {
    let mut m = u.attributes.as_object().cloned().unwrap_or_default();
    m.insert("IdentityStoreId".into(), json!(store));
    m.insert("UserId".into(), json!(u.user_id));
    m.entry("UserStatus").or_insert(json!("ENABLED"));
    m.insert("CreatedAt".into(), epoch(&u.created_at));
    m.insert("UpdatedAt".into(), epoch(&u.updated_at));
    Value::Object(m)
}

fn build_group(g: &StoredGroup, store: &str) -> Value {
    let mut m = g.attributes.as_object().cloned().unwrap_or_default();
    m.insert("IdentityStoreId".into(), json!(store));
    m.insert("GroupId".into(), json!(g.group_id));
    m.insert("CreatedAt".into(), epoch(&g.created_at));
    m.insert("UpdatedAt".into(), epoch(&g.updated_at));
    Value::Object(m)
}

fn build_membership(m: &StoredMembership, store: &str) -> Value {
    json!({
        "IdentityStoreId": store,
        "MembershipId": m.membership_id,
        "GroupId": m.group_id,
        "MemberId": { "UserId": m.member_user_id },
        "CreatedAt": epoch(&m.created_at),
        "UpdatedAt": epoch(&m.updated_at),
    })
}

/// Equality filters (`Filters: [{AttributePath, AttributeValue}]`) are a
/// deprecated-but-still-emitted request shape; apply them as top-level string
/// equality so older SDKs / Terraform data sources behave.
fn matches_filters(bag: &Value, filters: Option<&Value>) -> bool {
    let Some(arr) = filters.and_then(Value::as_array) else {
        return true;
    };
    arr.iter().all(|f| {
        let path = f.get("AttributePath").and_then(Value::as_str);
        let want = f.get("AttributeValue").and_then(Value::as_str);
        match (path, want) {
            (Some(p), Some(w)) => bag.get(p).and_then(Value::as_str) == Some(w),
            _ => true,
        }
    })
}

impl IdentityStoreService {
    // ---- users ----

    fn create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        // UserName @length 1..=128; the free-form profile attributes are all
        // `SensitiveStringType` @length 1..=1024.
        check_len(&b, "UserName", 1, 128)?;
        for field in SENSITIVE_USER_FIELDS {
            check_len(&b, field, 1, 1024)?;
        }
        let user_name = b
            .get("UserName")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let dir = acct.stores.entry(sid.clone()).or_default();
        if let Some(name) = &user_name {
            if dir
                .users
                .values()
                .any(|u| u.user_name.as_deref() == Some(name))
            {
                return Err(conflict(&format!(
                    "User with the specified UserName `{name}` already exists."
                )));
            }
        }
        let user_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        // Strip control/routing members from the persisted attribute bag.
        let mut attributes = b.clone();
        if let Value::Object(m) = &mut attributes {
            m.remove("IdentityStoreId");
        }
        dir.users.insert(
            user_id.clone(),
            StoredUser {
                user_id: user_id.clone(),
                user_name,
                attributes,
                created_at: now,
                updated_at: now,
            },
        );
        ok(json!({ "IdentityStoreId": sid, "UserId": user_id }))
    }

    fn describe_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let user_id = resource_id(&b, "UserId")?;
        let guard = self.state.read();
        let dir = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .ok_or_else(|| not_found("USER not found."))?;
        let u = dir
            .users
            .get(user_id)
            .ok_or_else(|| not_found("USER not found."))?;
        ok(build_user(u, &sid))
    }

    fn update_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let user_id = resource_id(&b, "UserId")?.to_string();
        let ops = b
            .get("Operations")
            .cloned()
            .ok_or_else(|| validation("Operations must be specified."))?;
        let mut guard = self.state.write();
        let dir = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&sid))
            .ok_or_else(|| not_found("USER not found."))?;
        let u = dir
            .users
            .get_mut(&user_id)
            .ok_or_else(|| not_found("USER not found."))?;
        apply_operations(&mut u.attributes, &ops);
        u.user_name = u
            .attributes
            .get("UserName")
            .and_then(Value::as_str)
            .map(str::to_string);
        u.updated_at = Utc::now();
        ok(json!({}))
    }

    fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let user_id = resource_id(&b, "UserId")?.to_string();
        let mut guard = self.state.write();
        if let Some(dir) = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&sid))
        {
            dir.users.remove(&user_id);
            dir.memberships.retain(|_, m| m.member_user_id != user_id);
        }
        ok(json!({}))
    }

    fn get_user_id(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let (path, want) = alternate_identifier(&b)?;
        let guard = self.state.read();
        let dir = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .ok_or_else(|| not_found("USER not found."))?;
        let found = dir.users.values().find(|u| {
            if path.eq_ignore_ascii_case("UserName") {
                u.user_name.as_deref() == Some(want.as_str())
            } else {
                u.attributes.get(&path).and_then(Value::as_str) == Some(want.as_str())
            }
        });
        match found {
            Some(u) => ok(json!({ "IdentityStoreId": sid, "UserId": u.user_id })),
            None => Err(not_found("USER not found.")),
        }
    }

    fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .map(|dir| {
                dir.users
                    .values()
                    .filter(|u| matches_filters(&u.attributes, b.get("Filters")))
                    .map(|u| build_user(u, &sid))
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b)?;
        let mut out = json!({ "Users": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        ok(out)
    }

    // ---- groups ----

    fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        // DisplayName is `GroupDisplayName` @length 1..=1024; Description is
        // `SensitiveStringType` @length 1..=1024.
        check_len(&b, "DisplayName", 1, 1024)?;
        check_len(&b, "Description", 1, 1024)?;
        let display_name = b
            .get("DisplayName")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let dir = acct.stores.entry(sid.clone()).or_default();
        if let Some(name) = &display_name {
            if dir
                .groups
                .values()
                .any(|g| g.display_name.as_deref() == Some(name))
            {
                return Err(conflict(&format!(
                    "Group with the specified DisplayName `{name}` already exists."
                )));
            }
        }
        let group_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let mut attributes = b.clone();
        if let Value::Object(m) = &mut attributes {
            m.remove("IdentityStoreId");
        }
        dir.groups.insert(
            group_id.clone(),
            StoredGroup {
                group_id: group_id.clone(),
                display_name,
                attributes,
                created_at: now,
                updated_at: now,
            },
        );
        ok(json!({ "GroupId": group_id, "IdentityStoreId": sid }))
    }

    fn describe_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?;
        let guard = self.state.read();
        let g = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .and_then(|d| d.groups.get(group_id))
            .ok_or_else(|| not_found("GROUP not found."))?;
        ok(build_group(g, &sid))
    }

    fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?.to_string();
        let ops = b
            .get("Operations")
            .cloned()
            .ok_or_else(|| validation("Operations must be specified."))?;
        let mut guard = self.state.write();
        let g = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&sid))
            .and_then(|d| d.groups.get_mut(&group_id))
            .ok_or_else(|| not_found("GROUP not found."))?;
        apply_operations(&mut g.attributes, &ops);
        g.display_name = g
            .attributes
            .get("DisplayName")
            .and_then(Value::as_str)
            .map(str::to_string);
        g.updated_at = Utc::now();
        ok(json!({}))
    }

    fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?.to_string();
        let mut guard = self.state.write();
        if let Some(dir) = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&sid))
        {
            dir.groups.remove(&group_id);
            dir.memberships.retain(|_, m| m.group_id != group_id);
        }
        ok(json!({}))
    }

    fn get_group_id(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let (path, want) = alternate_identifier(&b)?;
        let guard = self.state.read();
        let dir = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .ok_or_else(|| not_found("GROUP not found."))?;
        let found = dir.groups.values().find(|g| {
            if path.eq_ignore_ascii_case("DisplayName") {
                g.display_name.as_deref() == Some(want.as_str())
            } else {
                g.attributes.get(&path).and_then(Value::as_str) == Some(want.as_str())
            }
        });
        match found {
            Some(g) => ok(json!({ "GroupId": g.group_id, "IdentityStoreId": sid })),
            None => Err(not_found("GROUP not found.")),
        }
    }

    fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .map(|dir| {
                dir.groups
                    .values()
                    .filter(|g| matches_filters(&g.attributes, b.get("Filters")))
                    .map(|g| build_group(g, &sid))
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b)?;
        let mut out = json!({ "Groups": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        ok(out)
    }

    // ---- memberships ----

    fn create_group_membership(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?.to_string();
        let member = member_user_id(&b)?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let dir = acct.stores.entry(sid.clone()).or_default();
        if !dir.groups.contains_key(&group_id) {
            return Err(not_found("GROUP not found."));
        }
        if !dir.users.contains_key(&member) {
            return Err(not_found("USER not found."));
        }
        if let Some(existing) = dir
            .memberships
            .values()
            .find(|m| m.group_id == group_id && m.member_user_id == member)
        {
            return Err(conflict(&format!(
                "Membership `{}` already exists.",
                existing.membership_id
            )));
        }
        let membership_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        dir.memberships.insert(
            membership_id.clone(),
            StoredMembership {
                membership_id: membership_id.clone(),
                group_id,
                member_user_id: member,
                created_at: now,
                updated_at: now,
            },
        );
        ok(json!({ "MembershipId": membership_id, "IdentityStoreId": sid }))
    }

    fn describe_group_membership(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let membership_id = resource_id(&b, "MembershipId")?;
        let guard = self.state.read();
        let m = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .and_then(|d| d.memberships.get(membership_id))
            .ok_or_else(|| not_found("MEMBERSHIP not found."))?;
        ok(build_membership(m, &sid))
    }

    fn delete_group_membership(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let membership_id = resource_id(&b, "MembershipId")?.to_string();
        let mut guard = self.state.write();
        if let Some(dir) = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&sid))
        {
            dir.memberships.remove(&membership_id);
        }
        ok(json!({}))
    }

    fn get_group_membership_id(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?.to_string();
        let member = member_user_id(&b)?;
        let guard = self.state.read();
        let m = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .and_then(|d| {
                d.memberships
                    .values()
                    .find(|m| m.group_id == group_id && m.member_user_id == member)
            })
            .ok_or_else(|| not_found("MEMBERSHIP not found."))?;
        ok(json!({ "MembershipId": m.membership_id, "IdentityStoreId": sid }))
    }

    fn list_group_memberships(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let group_id = resource_id(&b, "GroupId")?.to_string();
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .map(|dir| {
                dir.memberships
                    .values()
                    .filter(|m| m.group_id == group_id)
                    .map(|m| build_membership(m, &sid))
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b)?;
        let mut out = json!({ "GroupMemberships": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        ok(out)
    }

    fn list_group_memberships_for_member(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let member = member_user_id(&b)?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .and_then(|a| a.stores.get(&sid))
            .map(|dir| {
                dir.memberships
                    .values()
                    .filter(|m| m.member_user_id == member)
                    .map(|m| build_membership(m, &sid))
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b)?;
        let mut out = json!({ "GroupMemberships": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        ok(out)
    }

    fn is_member_in_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let sid = store_id(&b)?;
        let member = member_user_id(&b)?;
        let group_ids: Vec<String> = b
            .get("GroupIds")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .ok_or_else(|| validation("GroupIds must be specified."))?;
        // Each element is a `GroupId` (`ResourceId` @length 1..=47).
        if let Some(bad) = group_ids
            .iter()
            .find(|g| g.is_empty() || g.chars().count() > 47)
        {
            return Err(validation(&format!(
                "GroupId `{bad}` must have length between 1 and 47, inclusive."
            )));
        }
        let guard = self.state.read();
        let dir = guard.get(&req.account_id).and_then(|a| a.stores.get(&sid));
        let results: Vec<Value> = group_ids
            .iter()
            .map(|gid| {
                let exists = dir
                    .map(|d| {
                        d.memberships
                            .values()
                            .any(|m| &m.group_id == gid && m.member_user_id == member)
                    })
                    .unwrap_or(false);
                json!({ "GroupId": gid, "MembershipExists": exists })
            })
            .collect();
        ok(json!({ "Results": results }))
    }
}

/// Extract `(AttributePath, AttributeValue)` from an `AlternateIdentifier`'s
/// `UniqueAttribute` member. `ExternalId` identifiers are not modeled in the
/// directory yet, so they resolve to not-found by the caller.
fn alternate_identifier(b: &Value) -> Result<(String, String), AwsServiceError> {
    let ai = b
        .get("AlternateIdentifier")
        .ok_or_else(|| validation("AlternateIdentifier must be specified."))?;
    if let Some(ua) = ai.get("UniqueAttribute") {
        let path = ua
            .get("AttributePath")
            .and_then(Value::as_str)
            .ok_or_else(|| validation("AttributePath must be specified."))?;
        let val = ua
            .get("AttributeValue")
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .ok_or_else(|| validation("AttributeValue must be specified."))?;
        return Ok((path.to_string(), val));
    }
    // ExternalId identifier: nothing indexed -> resolve to a sentinel that
    // never matches, producing a clean ResourceNotFoundException upstream.
    Ok((String::new(), "\u{0}__no_match__".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;

    fn svc() -> IdentityStoreService {
        IdentityStoreService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "identitystore".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "req".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: Mutex::new(None),
            path_segments: vec![],
            raw_path: String::new(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(s: &IdentityStoreService, action: &str, body: Value) -> Value {
        let resp = dispatch(s, &req(action, body)).expect("op ok");
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn user_lifecycle_and_get_by_username() {
        let s = svc();
        let created = call(
            &s,
            "CreateUser",
            json!({ "IdentityStoreId": "d-1234567890", "UserName": "alice", "DisplayName": "Alice" }),
        );
        let uid = created["UserId"].as_str().unwrap().to_string();

        let got = call(
            &s,
            "GetUserId",
            json!({
                "IdentityStoreId": "d-1234567890",
                "AlternateIdentifier": { "UniqueAttribute": { "AttributePath": "UserName", "AttributeValue": "alice" } }
            }),
        );
        assert_eq!(got["UserId"], json!(uid));

        let desc = call(
            &s,
            "DescribeUser",
            json!({ "IdentityStoreId": "d-1234567890", "UserId": uid }),
        );
        assert_eq!(desc["UserName"], json!("alice"));
        assert_eq!(desc["UserStatus"], json!("ENABLED"));
    }

    #[test]
    fn duplicate_username_conflicts() {
        let s = svc();
        let body = json!({ "IdentityStoreId": "d-1", "UserName": "bob" });
        dispatch(&s, &req("CreateUser", body.clone())).unwrap();
        let err = dispatch(&s, &req("CreateUser", body)).err().unwrap();
        assert_eq!(err.code(), "ConflictException");
    }

    #[test]
    fn membership_and_is_member_in_groups() {
        let s = svc();
        let sid = "d-9";
        let u = call(
            &s,
            "CreateUser",
            json!({ "IdentityStoreId": sid, "UserName": "u" }),
        );
        let uid = u["UserId"].as_str().unwrap().to_string();
        let g = call(
            &s,
            "CreateGroup",
            json!({ "IdentityStoreId": sid, "DisplayName": "g" }),
        );
        let gid = g["GroupId"].as_str().unwrap().to_string();
        call(
            &s,
            "CreateGroupMembership",
            json!({ "IdentityStoreId": sid, "GroupId": gid, "MemberId": { "UserId": uid } }),
        );
        let res = call(
            &s,
            "IsMemberInGroups",
            json!({ "IdentityStoreId": sid, "MemberId": { "UserId": uid }, "GroupIds": [gid, "d-nope"] }),
        );
        let arr = res["Results"].as_array().unwrap();
        assert_eq!(arr[0]["MembershipExists"], json!(true));
        assert_eq!(arr[1]["MembershipExists"], json!(false));
    }

    #[test]
    fn update_user_applies_operations() {
        let s = svc();
        let sid = "d-u";
        let u = call(
            &s,
            "CreateUser",
            json!({ "IdentityStoreId": sid, "UserName": "c" }),
        );
        let uid = u["UserId"].as_str().unwrap().to_string();
        call(
            &s,
            "UpdateUser",
            json!({
                "IdentityStoreId": sid, "UserId": uid,
                "Operations": [{ "AttributePath": "DisplayName", "AttributeValue": "Charlie" }]
            }),
        );
        let desc = call(
            &s,
            "DescribeUser",
            json!({ "IdentityStoreId": sid, "UserId": uid }),
        );
        assert_eq!(desc["DisplayName"], json!("Charlie"));
    }

    #[test]
    fn list_users_paginates() {
        let s = svc();
        let sid = "d-p";
        for i in 0..3 {
            call(
                &s,
                "CreateUser",
                json!({ "IdentityStoreId": sid, "UserName": format!("u{i}") }),
            );
        }
        let page1 = call(
            &s,
            "ListUsers",
            json!({ "IdentityStoreId": sid, "MaxResults": 2 }),
        );
        assert_eq!(page1["Users"].as_array().unwrap().len(), 2);
        let token = page1["NextToken"].as_str().unwrap().to_string();
        let page2 = call(
            &s,
            "ListUsers",
            json!({ "IdentityStoreId": sid, "MaxResults": 2, "NextToken": token }),
        );
        assert_eq!(page2["Users"].as_array().unwrap().len(), 1);
        assert!(page2.get("NextToken").is_none());
    }

    #[test]
    fn describe_missing_user_is_not_found() {
        let s = svc();
        let err = dispatch(
            &s,
            &req(
                "DescribeUser",
                json!({ "IdentityStoreId": "d-x", "UserId": "missing" }),
            ),
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }
}
