//! `OrganizationsService` `service_access` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn enable_aws_service_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let principal = required_str(&body, "ServicePrincipal")?.to_string();
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.enable_aws_service_access(&principal);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn disable_aws_service_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let principal = required_str(&body, "ServicePrincipal")?.to_string();
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.disable_aws_service_access(&principal)
            .map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_aws_service_access_for_organization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (max_results, next_token) = parse_list_pagination(&body)?;
        let guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_ref().expect("management gate proved Some");
        let entries: Vec<Value> = org
            .list_trusted_services()
            .into_iter()
            .map(|(svc, enabled_at)| {
                json!({
                    "ServicePrincipal": svc,
                    "DateEnabled": enabled_at.timestamp() as f64,
                })
            })
            .collect();
        let (page, token) = paginate(&entries, next_token.as_deref(), max_results);
        let mut body = json!({ "EnabledServicePrincipals": page });
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(body))
    }
}
