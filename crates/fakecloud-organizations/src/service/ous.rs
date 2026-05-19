//! `OrganizationsService` `ous` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn create_organizational_unit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let parent_id = required_str(&body, "ParentId")?;
        let name = required_str(&body, "Name")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        let ou = org.create_ou(parent_id, name).map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationalUnit": ou_payload(&ou) }),
        ))
    }

    pub(super) fn update_organizational_unit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ou_id = required_str(&body, "OrganizationalUnitId")?;
        let new_name = required_str(&body, "Name")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        let ou = org.rename_ou(ou_id, new_name).map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationalUnit": ou_payload(&ou) }),
        ))
    }

    pub(super) fn delete_organizational_unit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ou_id = required_str(&body, "OrganizationalUnitId")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        org.delete_ou(ou_id).map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(Value::Null))
    }

    pub(super) fn describe_organizational_unit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ou_id = required_str(&body, "OrganizationalUnitId")?;
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let ou = org.ous.get(ou_id).ok_or_else(|| {
            org_error_to_aws(OrgError::OrganizationalUnitNotFound(ou_id.to_string()))
        })?;
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationalUnit": ou_payload(ou) }),
        ))
    }

    pub(super) fn list_organizational_units_for_parent(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let parent_id = required_str(&body, "ParentId")?;
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        if parent_id != org.root_id && !org.ous.contains_key(parent_id) {
            return Err(org_error_to_aws(OrgError::ParentNotFound(
                parent_id.to_string(),
            )));
        }
        let children: Vec<Value> = org
            .ous
            .values()
            .filter(|ou| ou.parent_id == parent_id)
            .map(ou_payload)
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationalUnits": children }),
        ))
    }
}
