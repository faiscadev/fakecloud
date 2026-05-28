//! `OrganizationsService` billing-responsibility-transfer family:
//! `InviteOrganizationToTransferResponsibility`,
//! `DescribeResponsibilityTransfer`, `UpdateResponsibilityTransfer`,
//! `TerminateResponsibilityTransfer`, and the inbound/outbound list ops.

use super::*;
use crate::state::{random_id, ResponsibilityTransfer};
use chrono::DateTime;

/// The only transfer type AWS Organizations currently defines.
const TRANSFER_TYPE_BILLING: &str = "BILLING";

fn transfer_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResponsibilityTransferNotFoundException",
        format!("No responsibility transfer was found with id {id}."),
    )
}

fn invalid_input(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}

/// Validate the `Type` field against the `ResponsibilityTransferType`
/// enum. Only `BILLING` is defined today.
fn require_transfer_type(body: &Value) -> Result<String, AwsServiceError> {
    let t = required_str(body, "Type")?;
    if t != TRANSFER_TYPE_BILLING {
        return Err(invalid_input(&format!(
            "Type must be one of [BILLING], got {t}"
        )));
    }
    Ok(t.to_string())
}

fn transfer_payload(t: &ResponsibilityTransfer) -> Value {
    let mut obj = json!({
        "Arn": t.arn,
        "Name": t.name,
        "Id": t.id,
        "Type": t.transfer_type,
        "Status": t.status,
        "Source": {
            "ManagementAccountId": t.source_management_account_id,
            "ManagementAccountEmail": t.source_management_account_email,
        },
        "Target": {
            "ManagementAccountId": t.target_management_account_id,
            "ManagementAccountEmail": t.target_management_account_email,
        },
        "StartTimestamp": t.start_timestamp.timestamp() as f64,
    });
    if let Some(end) = t.end_timestamp {
        obj["EndTimestamp"] = json!(end.timestamp() as f64);
    }
    if let Some(h) = &t.active_handshake_id {
        obj["ActiveHandshakeId"] = json!(h);
    }
    obj
}

impl OrganizationsService {
    pub(super) fn invite_organization_to_transfer_responsibility(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let transfer_type = require_transfer_type(&body)?;
        let source_name = required_str(&body, "SourceName")?.to_string();
        // StartTimestamp is required; accept either an epoch number or an
        // ISO-8601 string and fall back to "now" if the SDK omitted it.
        let start = body
            .get("StartTimestamp")
            .and_then(json_to_datetime)
            .unwrap_or_else(Utc::now);
        let target_obj = body
            .get("Target")
            .ok_or_else(|| invalid_input("Target is required"))?;
        let target_id = target_obj
            .get("Id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_input("Target.Id is required"))?
            .to_string();
        let target_kind = target_obj
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("ACCOUNT");
        let notes = body
            .get("Notes")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");

        // The invited party is identified by account id or email; record
        // whichever the caller supplied as the target management account.
        let (target_account_id, target_email) = if target_kind == "EMAIL" {
            (target_id.clone(), target_id.clone())
        } else {
            (target_id.clone(), format!("{target_id}@example.com"))
        };

        let now = Utc::now();
        // The transfer rides on a handshake the invited org accepts.
        let handshake_id = format!("h-{}", random_id(32));
        let handshake_arn = format!(
            "arn:aws:organizations::{}:handshake/{}/transfer/{}",
            org.management_account_id, org.org_id, handshake_id
        );
        let handshake = crate::state::Handshake {
            id: handshake_id.clone(),
            arn: handshake_arn,
            action: "TRANSFER_RESPONSIBILITY".to_string(),
            state: "OPEN".to_string(),
            requested_timestamp: now,
            expiration_timestamp: now + chrono::Duration::days(15),
            source_account_id: org.management_account_id.clone(),
            target_account_id: target_account_id.clone(),
            target_email: Some(target_email.clone()),
            target_kind: target_kind.to_string(),
            notes,
            organization_id: org.org_id.clone(),
        };
        org.handshakes
            .insert(handshake_id.clone(), handshake.clone());

        let transfer_id = format!("rt-{}", random_id(32));
        let transfer_arn = format!(
            "arn:aws:organizations::{}:responsibilitytransfer/{}/{}",
            org.management_account_id, org.org_id, transfer_id
        );
        let transfer = ResponsibilityTransfer {
            id: transfer_id.clone(),
            arn: transfer_arn,
            name: source_name,
            transfer_type,
            status: "REQUESTED".to_string(),
            direction: "OUTBOUND".to_string(),
            source_management_account_id: org.management_account_id.clone(),
            source_management_account_email: org.management_account_email.clone(),
            target_management_account_id: target_account_id,
            target_management_account_email: target_email,
            start_timestamp: start,
            end_timestamp: None,
            active_handshake_id: Some(handshake_id),
        };
        org.responsibility_transfers
            .insert(transfer_id, transfer.clone());

        Ok(AwsResponse::ok_json(
            json!({ "Handshake": handshake_payload(&handshake) }),
        ))
    }

    pub(super) fn describe_responsibility_transfer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?.to_string();
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        let transfer = org
            .responsibility_transfers
            .get(&id)
            .ok_or_else(|| transfer_not_found(&id))?;
        Ok(AwsResponse::ok_json(
            json!({ "ResponsibilityTransfer": transfer_payload(transfer) }),
        ))
    }

    pub(super) fn update_responsibility_transfer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?.to_string();
        let name = required_str(&body, "Name")?.to_string();
        let mut guard = self.state.write();
        let org = guard.as_mut().ok_or_else(organizations_not_in_use)?;
        let transfer = org
            .responsibility_transfers
            .get_mut(&id)
            .ok_or_else(|| transfer_not_found(&id))?;
        transfer.name = name;
        let snapshot = transfer.clone();
        Ok(AwsResponse::ok_json(
            json!({ "ResponsibilityTransfer": transfer_payload(&snapshot) }),
        ))
    }

    pub(super) fn terminate_responsibility_transfer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = required_str(&body, "Id")?.to_string();
        let end = body
            .get("EndTimestamp")
            .and_then(json_to_datetime)
            .unwrap_or_else(Utc::now);
        let mut guard = self.state.write();
        let org = guard.as_mut().ok_or_else(organizations_not_in_use)?;
        let transfer = org
            .responsibility_transfers
            .get_mut(&id)
            .ok_or_else(|| transfer_not_found(&id))?;
        // Only a still-pending transfer can be terminated.
        if transfer.status == "WITHDRAWN" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResponsibilityTransferAlreadyInStatusException",
                "The responsibility transfer is already withdrawn.",
            ));
        }
        if transfer.status != "REQUESTED" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidResponsibilityTransferTransitionException",
                format!(
                    "A responsibility transfer in status {} cannot be terminated.",
                    transfer.status
                ),
            ));
        }
        transfer.status = "WITHDRAWN".to_string();
        transfer.end_timestamp = Some(end);
        transfer.active_handshake_id = None;
        let snapshot = transfer.clone();
        // Cancel the riding handshake too.
        if let Some(hid) = &snapshot.active_handshake_id {
            if let Some(h) = org.handshakes.get_mut(hid) {
                h.state = "CANCELED".to_string();
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "ResponsibilityTransfer": transfer_payload(&snapshot) }),
        ))
    }

    pub(super) fn list_inbound_responsibility_transfers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.list_responsibility_transfers(req, "INBOUND")
    }

    pub(super) fn list_outbound_responsibility_transfers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.list_responsibility_transfers(req, "OUTBOUND")
    }

    fn list_responsibility_transfers(
        &self,
        req: &AwsRequest,
        direction: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // `Type` is required on both list ops.
        let transfer_type = require_transfer_type(&body)?;
        let (max_results, next_token) = parse_list_pagination(&body)?;
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        let filtered: Vec<Value> = org
            .responsibility_transfers
            .values()
            .filter(|t| t.direction == direction && t.transfer_type == transfer_type)
            .map(transfer_payload)
            .collect();
        let (page, token) = paginate(&filtered, next_token.as_deref(), max_results);
        let mut out = json!({ "ResponsibilityTransfers": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }
}

/// Parse a JSON timestamp that may arrive as an epoch number (seconds,
/// possibly fractional) or an ISO-8601 string.
fn json_to_datetime(v: &Value) -> Option<DateTime<Utc>> {
    if let Some(secs) = v.as_f64() {
        return DateTime::from_timestamp(secs as i64, 0);
    }
    if let Some(s) = v.as_str() {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc));
    }
    None
}
