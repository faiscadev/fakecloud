//! `OrganizationsService` `handshakes` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn accept_handshake(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_handshake(req, "ACCEPTED")
    }

    pub(super) fn decline_handshake(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_handshake(req, "DECLINED")
    }

    pub(super) fn cancel_handshake(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_handshake(req, "CANCELED")
    }

    pub(super) fn transition_handshake(
        &self,
        req: &AwsRequest,
        new_state: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "HandshakeId")?.to_string();
        let mut guard = self.state.write();
        // Handshake transitions are handshake-scoped, not org-scoped: with no
        // org there are no handshakes, so the id can't be found. These ops
        // don't declare AWSOrganizationsNotInUseException.
        let org = guard.as_mut().ok_or_else(|| {
            org_error_to_aws(crate::state::OrgError::HandshakeNotFound(id.clone()))
        })?;

        // AcceptHandshake / DeclineHandshake belong to the *target*
        // account; CancelHandshake belongs to the *source* (management)
        // account. Enforce party-correctness so test harnesses catch
        // misuse before AWS would.
        //
        // ENABLE_ALL_FEATURES / APPROVE_ALL_FEATURES handshakes are
        // org-wide and any member account can accept/decline their copy;
        // they don't have a single target_account_id, so we skip the
        // party gate for them.
        let handshake = org.handshakes.get(&id).ok_or_else(|| {
            org_error_to_aws(crate::state::OrgError::HandshakeNotFound(id.clone()))
        })?;
        let org_wide_action = matches!(
            handshake.action.as_str(),
            "ENABLE_ALL_FEATURES" | "APPROVE_ALL_FEATURES"
        );
        let allowed = if org_wide_action {
            // For org-wide handshakes only require membership.
            true
        } else {
            match new_state {
                "ACCEPTED" | "DECLINED" => req.account_id == handshake.target_account_id,
                "CANCELED" => req.account_id == handshake.source_account_id,
                _ => false,
            }
        };
        if !allowed {
            return Err(org_error_to_aws(
                crate::state::OrgError::InvalidHandshakeParty(req.account_id.clone()),
            ));
        }
        let updated = org
            .resolve_handshake(&id, new_state)
            .map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(
            json!({ "Handshake": handshake_payload(&updated) }),
        ))
    }

    pub(super) fn describe_handshake(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "HandshakeId")?.to_string();
        let guard = self.state.read();
        // DescribeHandshake is handshake-scoped; with no org the id can't be
        // found. It doesn't declare AWSOrganizationsNotInUseException.
        let handshake = guard
            .as_ref()
            .and_then(|org| org.handshakes.get(&id))
            .ok_or_else(|| {
                org_error_to_aws(crate::state::OrgError::HandshakeNotFound(id.clone()))
            })?;
        Ok(AwsResponse::ok_json(
            json!({ "Handshake": handshake_payload(handshake) }),
        ))
    }

    pub(super) fn list_handshakes_for_organization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filter = parse_handshake_filter(&body)?;
        let (max_results, next_token) = parse_list_pagination(&body)?;

        let guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_ref().expect("management gate proved Some");
        let filtered: Vec<Value> = org
            .list_handshakes(None)
            .into_iter()
            .filter(|h| handshake_matches_filter(h, &filter))
            .map(|h| handshake_payload(&h))
            .collect();
        let (page, token) = paginate(&filtered, next_token.as_deref(), max_results);
        let mut body = json!({ "Handshakes": page });
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(body))
    }
}
