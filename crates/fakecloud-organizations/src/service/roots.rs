//! `OrganizationsService` `roots` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn list_roots(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let policy_types: Vec<Value> = org
            .list_policy_type_statuses()
            .into_iter()
            .filter(|(_, status)| status == "ENABLED")
            .map(|(t, status)| json!({"Type": t, "Status": status}))
            .collect();
        let root = json!({
            "Id": org.root_id,
            "Arn": org.root_arn,
            "Name": org.root_name,
            "PolicyTypes": policy_types,
        });
        Ok(AwsResponse::ok_json(json!({ "Roots": [root] })))
    }

    pub(super) fn list_parents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let child_id = required_str(&body, "ChildId")?.to_string();
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        let parents = match org.parent_of(&child_id) {
            Some((id, kind)) => vec![json!({"Id": id, "Type": kind})],
            None => Vec::new(),
        };
        Ok(AwsResponse::ok_json(json!({ "Parents": parents })))
    }

    pub(super) fn list_children(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let parent_id = required_str(&body, "ParentId")?.to_string();
        let child_type = required_str(&body, "ChildType")?.to_string();
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        let children: Vec<Value> = org
            .list_children(&parent_id, &child_type)
            .into_iter()
            .map(|id| json!({"Id": id, "Type": child_type}))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "Children": children })))
    }
}
