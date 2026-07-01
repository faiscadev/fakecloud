//! Resource Groups restJson1 dispatch + operation handlers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    AccountSettings, ResourceGroup, ResourceQuery, SharedResourceGroupsState, TagSyncTask,
};

/// Every operation name in the Resource Groups Smithy model.
pub const RESOURCE_GROUPS_ACTIONS: &[&str] = &[
    "CancelTagSyncTask",
    "CreateGroup",
    "DeleteGroup",
    "GetAccountSettings",
    "GetGroup",
    "GetGroupConfiguration",
    "GetGroupQuery",
    "GetTagSyncTask",
    "GetTags",
    "GroupResources",
    "ListGroupResources",
    "ListGroupingStatuses",
    "ListGroups",
    "ListTagSyncTasks",
    "PutGroupConfiguration",
    "SearchResources",
    "StartTagSyncTask",
    "Tag",
    "UngroupResources",
    "Untag",
    "UpdateAccountSettings",
    "UpdateGroup",
    "UpdateGroupQuery",
];

pub struct ResourceGroupsService {
    state: SharedResourceGroupsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// A resolved route: the operation plus any path-derived argument.
enum Route {
    CreateGroup,
    GetGroup,
    GetGroupQuery,
    UpdateGroup,
    UpdateGroupQuery,
    DeleteGroup,
    ListGroups,
    GroupResources,
    UngroupResources,
    ListGroupResources,
    SearchResources,
    GetGroupConfiguration,
    PutGroupConfiguration,
    GetTags(String),
    Tag(String),
    Untag(String),
    GetAccountSettings,
    UpdateAccountSettings,
    ListGroupingStatuses,
    StartTagSyncTask,
    GetTagSyncTask,
    ListTagSyncTasks,
    CancelTagSyncTask,
}

impl Route {
    fn mutates(&self) -> bool {
        matches!(
            self,
            Route::CreateGroup
                | Route::UpdateGroup
                | Route::UpdateGroupQuery
                | Route::DeleteGroup
                | Route::GroupResources
                | Route::UngroupResources
                | Route::PutGroupConfiguration
                | Route::Tag(_)
                | Route::Untag(_)
                | Route::UpdateAccountSettings
                | Route::StartTagSyncTask
                | Route::CancelTagSyncTask
        )
    }
}

impl ResourceGroupsService {
    pub fn new(state: SharedResourceGroupsState) -> Self {
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

    async fn persist(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    fn resolve_route(req: &AwsRequest) -> Option<Route> {
        let segs: Vec<&str> = req
            .raw_path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        match (&req.method, segs.as_slice()) {
            (&Method::POST, ["groups"]) => Some(Route::CreateGroup),
            (&Method::POST, ["groups-list"]) => Some(Route::ListGroups),
            (&Method::POST, ["get-group"]) => Some(Route::GetGroup),
            (&Method::POST, ["get-group-query"]) => Some(Route::GetGroupQuery),
            (&Method::POST, ["update-group"]) => Some(Route::UpdateGroup),
            (&Method::POST, ["update-group-query"]) => Some(Route::UpdateGroupQuery),
            (&Method::POST, ["delete-group"]) => Some(Route::DeleteGroup),
            (&Method::POST, ["group-resources"]) => Some(Route::GroupResources),
            (&Method::POST, ["ungroup-resources"]) => Some(Route::UngroupResources),
            (&Method::POST, ["list-group-resources"]) => Some(Route::ListGroupResources),
            (&Method::POST, ["resources", "search"]) => Some(Route::SearchResources),
            (&Method::POST, ["get-group-configuration"]) => Some(Route::GetGroupConfiguration),
            (&Method::POST, ["put-group-configuration"]) => Some(Route::PutGroupConfiguration),
            (&Method::POST, ["get-account-settings"]) => Some(Route::GetAccountSettings),
            (&Method::POST, ["update-account-settings"]) => Some(Route::UpdateAccountSettings),
            (&Method::POST, ["list-grouping-statuses"]) => Some(Route::ListGroupingStatuses),
            (&Method::POST, ["start-tag-sync-task"]) => Some(Route::StartTagSyncTask),
            (&Method::POST, ["get-tag-sync-task"]) => Some(Route::GetTagSyncTask),
            (&Method::POST, ["list-tag-sync-tasks"]) => Some(Route::ListTagSyncTasks),
            (&Method::POST, ["cancel-tag-sync-task"]) => Some(Route::CancelTagSyncTask),
            (&Method::GET, ["resources", arn, "tags"]) => Some(Route::GetTags(decode(arn))),
            (&Method::PUT, ["resources", arn, "tags"]) => Some(Route::Tag(decode(arn))),
            (&Method::PATCH, ["resources", arn, "tags"]) => Some(Route::Untag(decode(arn))),
            _ => None,
        }
    }

    // --- group lifecycle --------------------------------------------------

    fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let name = require_str(&body, "Name")?.to_string();
        validate_group_name(&name)?;
        let query = parse_resource_query(body.get("ResourceQuery"))?;
        let configuration = body
            .get("Configuration")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if query.is_none() && configuration.is_empty() {
            return Err(bad_request(
                "A group must be created with either a ResourceQuery or a Configuration.",
            ));
        }
        let tags = parse_tags(body.get("Tags"));

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.groups.contains_key(&name) {
            return Err(bad_request(&format!(
                "A group with the name '{name}' already exists."
            )));
        }
        let arn = group_arn(&req.region, &req.account_id, &name);
        let group = ResourceGroup {
            name: name.clone(),
            arn,
            description: opt_string(&body, "Description"),
            query,
            configuration,
            tags: tags.clone(),
            criticality: body
                .get("Criticality")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            owner: opt_string(&body, "Owner"),
            display_name: opt_string(&body, "DisplayName"),
            application_tag: BTreeMap::new(),
            resources: BTreeSet::new(),
            created_at: Utc::now(),
        };
        let out = json!({
            "Group": group_json(&group),
            "ResourceQuery": group.query.as_ref().map(resource_query_json),
            "Tags": tags,
            "GroupConfiguration": group_configuration_json(&group),
        });
        st.groups.insert(name, group);
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key(&body)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.resolve_name(&key).and_then(|n| s.groups.get(n)))
            .ok_or_else(|| not_found(&key))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Group": group_json(group) }),
        ))
    }

    fn get_group_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key(&body)?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.resolve_name(&key).and_then(|n| s.groups.get(n)))
            .ok_or_else(|| not_found(&key))?;
        let query = group.query.as_ref().ok_or_else(|| {
            bad_request("The group is not a resource-query group and has no query.")
        })?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({
                "GroupQuery": {
                    "GroupName": group.name,
                    "ResourceQuery": resource_query_json(query),
                }
            }),
        ))
    }

    fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key(&body)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group = st.groups.get_mut(&name).unwrap();
        if let Some(d) = body.get("Description").and_then(|v| v.as_str()) {
            group.description = Some(d.to_string());
        }
        if let Some(c) = body.get("Criticality").and_then(|v| v.as_i64()) {
            group.criticality = Some(c as i32);
        }
        if let Some(o) = body.get("Owner").and_then(|v| v.as_str()) {
            group.owner = Some(o.to_string());
        }
        if let Some(dn) = body.get("DisplayName").and_then(|v| v.as_str()) {
            group.display_name = Some(dn.to_string());
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Group": group_json(group) }),
        ))
    }

    fn update_group_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key(&body)?;
        let query = parse_resource_query(body.get("ResourceQuery"))?
            .ok_or_else(|| bad_request("ResourceQuery is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group = st.groups.get_mut(&name).unwrap();
        group.query = Some(query.clone());
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({
                "GroupQuery": {
                    "GroupName": group.name,
                    "ResourceQuery": resource_query_json(&query),
                }
            }),
        ))
    }

    fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key(&body)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group = st.groups.remove(&name).unwrap();
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Group": group_json(&group) }),
        ))
    }

    fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ListGroups binds MaxResults/NextToken to the query string (@httpQuery),
        // not the body, so fold them in before validating + paginating.
        let mut body = parse_json(&req.body)?;
        inject_query_pagination(req, &mut body);
        validate_max_results(&body)?;
        validate_next_token(&body)?;
        let accounts = self.state.read();
        let groups: Vec<&ResourceGroup> = accounts
            .get(&req.account_id)
            .map(|s| s.groups.values().collect())
            .unwrap_or_default();
        let identifiers: Vec<Value> = groups.iter().map(|g| group_identifier_json(g)).collect();
        let full: Vec<Value> = groups.iter().map(|g| group_json(g)).collect();
        let (ident_page, next) = paginate(identifiers, &body);
        let (full_page, _) = paginate(full, &body);
        let mut out = json!({
            "GroupIdentifiers": ident_page,
            "Groups": full_page,
        });
        if let Some(nt) = next {
            out["NextToken"] = json!(nt);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    // --- membership -------------------------------------------------------

    fn group_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = require_str(&body, "Group")?.to_string();
        let arns = string_list(body.get("ResourceArns"));
        if arns.is_empty() {
            return Err(bad_request("ResourceArns must not be empty."));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group = st.groups.get_mut(&name).unwrap();
        if group.query.is_some() {
            return Err(bad_request(
                "Resources cannot be added to a resource-query group; membership is computed from its query.",
            ));
        }
        for a in &arns {
            group.resources.insert(a.clone());
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Succeeded": arns, "Failed": [], "Pending": [] }),
        ))
    }

    fn ungroup_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = require_str(&body, "Group")?.to_string();
        let arns = string_list(body.get("ResourceArns"));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group = st.groups.get_mut(&name).unwrap();
        for a in &arns {
            group.resources.remove(a);
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Succeeded": arns, "Failed": [], "Pending": [] }),
        ))
    }

    fn list_group_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body)?;
        validate_next_token(&body)?;
        let key = group_key_field(&body, "GroupName", "Group")?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.resolve_name(&key).and_then(|n| s.groups.get(n)))
            .ok_or_else(|| not_found(&key))?;
        let items: Vec<Value> = group
            .resources
            .iter()
            .map(|arn| {
                json!({
                    "Identifier": resource_identifier_json(arn),
                    "Status": { "Name": "ACTIVE" },
                })
            })
            .collect();
        let idents: Vec<Value> = group
            .resources
            .iter()
            .map(|arn| resource_identifier_json(arn))
            .collect();
        let (items_page, next) = paginate(items, &body);
        let (idents_page, _) = paginate(idents, &body);
        let mut out = json!({
            "Resources": items_page,
            "ResourceIdentifiers": idents_page,
            "QueryErrors": [],
        });
        if let Some(nt) = next {
            out["NextToken"] = json!(nt);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn search_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body)?;
        validate_next_token(&body)?;
        // Evaluating a free-standing ResourceQuery against live resources needs
        // the cross-service tag index (Resource Groups Tagging API batch). Until
        // then SearchResources returns no matches rather than a fabricated set.
        parse_resource_query(body.get("ResourceQuery"))?
            .ok_or_else(|| bad_request("ResourceQuery is required."))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "ResourceIdentifiers": [], "QueryErrors": [] }),
        ))
    }

    // --- configuration ----------------------------------------------------

    fn get_group_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = group_key_field(&body, "Group", "Group")?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.resolve_name(&key).and_then(|n| s.groups.get(n)))
            .ok_or_else(|| not_found(&key))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "GroupConfiguration": group_configuration_json(group) }),
        ))
    }

    fn put_group_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = require_str(&body, "Group")?.to_string();
        let configuration = body
            .get("Configuration")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        st.groups.get_mut(&name).unwrap().configuration = configuration;
        Ok(AwsResponse::json_value(StatusCode::OK, json!({})))
    }

    // --- tagging ----------------------------------------------------------

    fn get_tags(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.groups.values().find(|g| g.arn == arn))
            .ok_or_else(|| not_found(arn))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Arn": arn, "Tags": group.tags }),
        ))
    }

    fn tag(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let tags = parse_tags(body.get("Tags"));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .groups
            .values_mut()
            .find(|g| g.arn == arn)
            .ok_or_else(|| not_found(arn))?;
        for (k, v) in &tags {
            group.tags.insert(k.clone(), v.clone());
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Arn": arn, "Tags": tags }),
        ))
    }

    fn untag(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let keys = string_list(body.get("Keys"));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let group = st
            .groups
            .values_mut()
            .find(|g| g.arn == arn)
            .ok_or_else(|| not_found(arn))?;
        for k in &keys {
            group.tags.remove(k);
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Arn": arn, "Keys": keys }),
        ))
    }

    // --- account settings -------------------------------------------------

    fn get_account_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let settings = accounts
            .get(&req.account_id)
            .map(|s| s.account_settings.clone())
            .unwrap_or_default();
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "AccountSettings": account_settings_json(&settings) }),
        ))
    }

    fn update_account_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let desired = body
            .get("GroupLifecycleEventsDesiredStatus")
            .and_then(|v| v.as_str());
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(d) = desired {
            if d != "ACTIVE" && d != "INACTIVE" {
                return Err(bad_request(
                    "GroupLifecycleEventsDesiredStatus must be ACTIVE or INACTIVE.",
                ));
            }
            st.account_settings.desired_status = Some(d.to_string());
        }
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "AccountSettings": account_settings_json(&st.account_settings) }),
        ))
    }

    fn list_grouping_statuses(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body)?;
        validate_next_token(&body)?;
        let key = require_str(&body, "Group")?.to_string();
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let group = st
            .and_then(|s| s.resolve_name(&key).and_then(|n| s.groups.get(n)))
            // ListGroupingStatuses does not declare NotFoundException in its
            // Smithy error set, so a missing group is a BadRequest here.
            .ok_or_else(|| bad_request(&format!("The specified group '{key}' was not found.")))?;
        // Membership changes here are synchronous, so nothing is ever pending.
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Group": group.arn, "GroupingStatuses": [] }),
        ))
    }

    // --- tag-sync tasks ---------------------------------------------------

    fn start_tag_sync_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let key = require_str(&body, "Group")?.to_string();
        let role_arn = require_str(&body, "RoleArn")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(name) = st.resolve_name(&key).map(String::from) else {
            return Err(not_found(&key));
        };
        let group_arn = st.groups.get(&name).unwrap().arn.clone();
        let task_arn = tag_sync_task_arn(&req.region, &req.account_id, &name);
        let task = TagSyncTask {
            task_arn: task_arn.clone(),
            group_arn: group_arn.clone(),
            group_name: name.clone(),
            tag_key: opt_string(&body, "TagKey"),
            tag_value: opt_string(&body, "TagValue"),
            resource_query: parse_resource_query(body.get("ResourceQuery"))?,
            role_arn: role_arn.clone(),
            status: "ACTIVE".to_string(),
            created_at: Utc::now(),
        };
        st.tag_sync_tasks.insert(task_arn.clone(), task.clone());
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({
                "GroupArn": group_arn,
                "GroupName": name,
                "TaskArn": task_arn,
                "TagKey": task.tag_key,
                "TagValue": task.tag_value,
                "ResourceQuery": task.resource_query.as_ref().map(resource_query_json),
                "RoleArn": role_arn,
            }),
        ))
    }

    fn get_tag_sync_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let task_arn = require_str(&body, "TaskArn")?.to_string();
        let accounts = self.state.read();
        let task = accounts
            .get(&req.account_id)
            .and_then(|s| s.tag_sync_tasks.get(&task_arn))
            .ok_or_else(|| not_found(&task_arn))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            tag_sync_task_json(task),
        ))
    }

    fn list_tag_sync_tasks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body)?;
        validate_next_token(&body)?;
        let accounts = self.state.read();
        let all: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.tag_sync_tasks.values().map(tag_sync_task_json).collect())
            .unwrap_or_default();
        let (page, next) = paginate(all, &body);
        let mut out = json!({ "TagSyncTasks": page });
        if let Some(nt) = next {
            out["NextToken"] = json!(nt);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn cancel_tag_sync_task(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let task_arn = require_str(&body, "TaskArn")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.tag_sync_tasks.remove(&task_arn).is_none() {
            // CancelTagSyncTask does not declare NotFoundException; a missing
            // task is reported as a BadRequest.
            return Err(bad_request(&format!(
                "The specified tag-sync task '{task_arn}' was not found."
            )));
        }
        Ok(AwsResponse::json_value(StatusCode::OK, json!({})))
    }
}

#[async_trait]
impl AwsService for ResourceGroupsService {
    fn service_name(&self) -> &str {
        "resource-groups"
    }

    fn supported_actions(&self) -> &[&str] {
        RESOURCE_GROUPS_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(route) = Self::resolve_route(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let mutates = route.mutates();
        let result = match route {
            Route::CreateGroup => self.create_group(&req),
            Route::GetGroup => self.get_group(&req),
            Route::GetGroupQuery => self.get_group_query(&req),
            Route::UpdateGroup => self.update_group(&req),
            Route::UpdateGroupQuery => self.update_group_query(&req),
            Route::DeleteGroup => self.delete_group(&req),
            Route::ListGroups => self.list_groups(&req),
            Route::GroupResources => self.group_resources(&req),
            Route::UngroupResources => self.ungroup_resources(&req),
            Route::ListGroupResources => self.list_group_resources(&req),
            Route::SearchResources => self.search_resources(&req),
            Route::GetGroupConfiguration => self.get_group_configuration(&req),
            Route::PutGroupConfiguration => self.put_group_configuration(&req),
            Route::GetTags(arn) => self.get_tags(&req, &arn),
            Route::Tag(arn) => self.tag(&req, &arn),
            Route::Untag(arn) => self.untag(&req, &arn),
            Route::GetAccountSettings => self.get_account_settings(&req),
            Route::UpdateAccountSettings => self.update_account_settings(&req),
            Route::ListGroupingStatuses => self.list_grouping_statuses(&req),
            Route::StartTagSyncTask => self.start_tag_sync_task(&req),
            Route::GetTagSyncTask => self.get_tag_sync_task(&req),
            Route::ListTagSyncTasks => self.list_tag_sync_tasks(&req),
            Route::CancelTagSyncTask => self.cancel_tag_sync_task(&req),
        };
        if mutates && result.is_ok() {
            self.persist().await;
        }
        result
    }
}

// ---------------------------------------------------------------------------
// JSON builders
// ---------------------------------------------------------------------------

fn group_json(g: &ResourceGroup) -> Value {
    let mut v = json!({ "GroupArn": g.arn, "Name": g.name });
    if let Some(d) = &g.description {
        v["Description"] = json!(d);
    }
    if let Some(c) = g.criticality {
        v["Criticality"] = json!(c);
    }
    if let Some(o) = &g.owner {
        v["Owner"] = json!(o);
    }
    if let Some(dn) = &g.display_name {
        v["DisplayName"] = json!(dn);
    }
    if !g.application_tag.is_empty() {
        v["ApplicationTag"] = json!(g.application_tag);
    }
    v
}

fn group_identifier_json(g: &ResourceGroup) -> Value {
    let mut v = json!({ "GroupName": g.name, "GroupArn": g.arn });
    if let Some(d) = &g.description {
        v["Description"] = json!(d);
    }
    if let Some(c) = g.criticality {
        v["Criticality"] = json!(c);
    }
    if let Some(o) = &g.owner {
        v["Owner"] = json!(o);
    }
    if let Some(dn) = &g.display_name {
        v["DisplayName"] = json!(dn);
    }
    v
}

fn resource_query_json(q: &ResourceQuery) -> Value {
    json!({ "Type": q.type_, "Query": q.query })
}

fn group_configuration_json(g: &ResourceGroup) -> Value {
    json!({
        "Configuration": g.configuration,
        "Status": "UPDATE_COMPLETE",
    })
}

fn resource_identifier_json(arn: &str) -> Value {
    let mut v = json!({ "ResourceArn": arn });
    if let Some(t) = resource_type_from_arn(arn) {
        v["ResourceType"] = json!(t);
    }
    v
}

fn account_settings_json(s: &AccountSettings) -> Value {
    let desired = s
        .desired_status
        .clone()
        .unwrap_or_else(|| "INACTIVE".into());
    json!({
        "GroupLifecycleEventsDesiredStatus": desired,
        "GroupLifecycleEventsStatus": desired,
    })
}

fn tag_sync_task_json(t: &TagSyncTask) -> Value {
    json!({
        "GroupArn": t.group_arn,
        "GroupName": t.group_name,
        "TaskArn": t.task_arn,
        "TagKey": t.tag_key,
        "TagValue": t.tag_value,
        "ResourceQuery": t.resource_query.as_ref().map(resource_query_json),
        "RoleArn": t.role_arn,
        "Status": t.status,
        "CreatedAt": t.created_at.timestamp() as f64,
    })
}

// ---------------------------------------------------------------------------
// Parsing + helpers
// ---------------------------------------------------------------------------

fn parse_json(body: &[u8]) -> Result<Value, AwsServiceError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| bad_request(&format!("invalid request body: {e}")))
}

fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad_request(&format!("{field} is required.")))
}

fn opt_string(body: &Value, field: &str) -> Option<String> {
    body.get(field).and_then(|v| v.as_str()).map(String::from)
}

/// GetGroup/GetGroupQuery/etc. accept the group as `GroupName` (legacy) or
/// `Group` (name or ARN).
fn group_key(body: &Value) -> Result<String, AwsServiceError> {
    group_key_field(body, "GroupName", "Group")
}

fn group_key_field(body: &Value, a: &str, b: &str) -> Result<String, AwsServiceError> {
    body.get(a)
        .or_else(|| body.get(b))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| bad_request("A group name or ARN is required."))
}

fn parse_resource_query(v: Option<&Value>) -> Result<Option<ResourceQuery>, AwsServiceError> {
    let Some(q) = v else { return Ok(None) };
    if q.is_null() {
        return Ok(None);
    }
    let type_ = q
        .get("Type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| bad_request("ResourceQuery.Type is required."))?;
    if type_ != "TAG_FILTERS_1_0" && type_ != "CLOUDFORMATION_STACK_1_0" {
        return Err(bad_request(
            "ResourceQuery.Type must be TAG_FILTERS_1_0 or CLOUDFORMATION_STACK_1_0.",
        ));
    }
    let query = q
        .get("Query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| bad_request("ResourceQuery.Query is required."))?;
    // Query must itself be valid JSON.
    serde_json::from_str::<Value>(query)
        .map_err(|e| bad_request(&format!("ResourceQuery.Query is not valid JSON: {e}")))?;
    Ok(Some(ResourceQuery {
        type_: type_.to_string(),
        query: query.to_string(),
    }))
}

fn parse_tags(v: Option<&Value>) -> BTreeMap<String, String> {
    v.and_then(|t| t.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn validate_group_name(name: &str) -> Result<(), AwsServiceError> {
    if name.len() > 300
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(bad_request(
            "Group name must be 1-300 chars of [A-Za-z0-9._-].",
        ));
    }
    Ok(())
}

fn validate_max_results(body: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = body.get("MaxResults") {
        let n = v
            .as_i64()
            .ok_or_else(|| bad_request("MaxResults must be an integer."))?;
        if !(1..=50).contains(&n) {
            return Err(bad_request("MaxResults must be between 1 and 50."));
        }
    }
    Ok(())
}

/// `NextToken` @length is 0..=8192.
fn validate_next_token(body: &Value) -> Result<(), AwsServiceError> {
    if let Some(t) = body.get("NextToken").and_then(|v| v.as_str()) {
        if t.len() > 8192 {
            return Err(bad_request("NextToken must be at most 8192 characters."));
        }
    }
    Ok(())
}

/// Fold `@httpQuery`-bound `maxResults`/`nextToken` (used by ListGroups) into
/// the body view so the shared validation + pagination helpers see them.
fn inject_query_pagination(req: &AwsRequest, body: &mut Value) {
    if body.get("MaxResults").is_none() {
        if let Some(v) = req.query_params.get("maxResults") {
            body["MaxResults"] = v
                .parse::<i64>()
                .map(|n| json!(n))
                .unwrap_or_else(|_| json!(v));
        }
    }
    if body.get("NextToken").is_none() {
        if let Some(v) = req.query_params.get("nextToken") {
            body["NextToken"] = json!(v);
        }
    }
}

/// Opaque zero-based-offset pagination. A never-issued NextToken yields an
/// empty terminal page rather than restarting page one.
fn paginate(items: Vec<Value>, body: &Value) -> (Vec<Value>, Option<String>) {
    let max = body
        .get("MaxResults")
        .and_then(|v| v.as_i64())
        .map(|n| n as usize)
        .unwrap_or(50);
    let start = match body.get("NextToken").and_then(|v| v.as_str()) {
        Some(t) => match t.parse::<usize>() {
            Ok(n) => n,
            Err(_) => return (Vec::new(), None),
        },
        None => 0,
    };
    let total = items.len();
    if start >= total {
        return (Vec::new(), None);
    }
    let end = start.saturating_add(max).min(total);
    let next = (end < total).then(|| end.to_string());
    (items[start..end].to_vec(), next)
}

fn decode(seg: &str) -> String {
    percent_decode_str(seg).decode_utf8_lossy().into_owned()
}

fn group_arn(region: &str, account: &str, name: &str) -> String {
    let uid = uuid::Uuid::new_v4().simple().to_string();
    format!("arn:aws:resource-groups:{region}:{account}:group/{name}/{uid}")
}

fn tag_sync_task_arn(region: &str, account: &str, name: &str) -> String {
    let uid = uuid::Uuid::new_v4().simple().to_string();
    format!("arn:aws:resource-groups:{region}:{account}:group/{name}/{uid}")
}

/// Best-effort `AWS::Service::Type` from an ARN. Returns `None` when the ARN
/// shape isn't recognized rather than fabricating a type.
fn resource_type_from_arn(arn: &str) -> Option<String> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" {
        return None;
    }
    let service = parts[2];
    if service.is_empty() {
        return None;
    }
    let resource_seg = parts[5];
    let restype = resource_seg
        .split(['/', ':'])
        .next()
        .filter(|s| !s.is_empty())?;
    Some(format!(
        "AWS::{}::{}",
        titlecase(service),
        titlecase(restype)
    ))
}

fn titlecase(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(what: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFoundException",
        format!("The specified group or resource '{what}' was not found."),
    )
}
