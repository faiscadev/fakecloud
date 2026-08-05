//! Account Management restJson1 dispatch + operation handlers.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    default_region_status, AlternateContact, PendingEmailUpdate, SharedAccountState,
    ALTERNATE_CONTACT_TYPES, REGIONS,
};

/// Every operation name in the AWS Account Smithy model.
pub const ACCOUNT_ACTIONS: &[&str] = &[
    "AcceptPrimaryEmailUpdate",
    "DeleteAlternateContact",
    "DisableRegion",
    "EnableRegion",
    "GetAccountInformation",
    "GetAlternateContact",
    "GetContactInformation",
    "GetGovCloudAccountInformation",
    "GetPrimaryEmail",
    "GetPrimaryEmailUpdateStatus",
    "GetRegionOptStatus",
    "ListRegions",
    "PutAccountName",
    "PutAlternateContact",
    "PutContactInformation",
    "StartPrimaryEmailUpdate",
];

/// A resolved route (each op is a distinct `POST /<verb>` path in restJson1).
enum Route {
    AcceptPrimaryEmailUpdate,
    DeleteAlternateContact,
    DisableRegion,
    EnableRegion,
    GetAccountInformation,
    GetAlternateContact,
    GetContactInformation,
    GetGovCloudAccountInformation,
    GetPrimaryEmail,
    GetPrimaryEmailUpdateStatus,
    GetRegionOptStatus,
    ListRegions,
    PutAccountName,
    PutAlternateContact,
    PutContactInformation,
    StartPrimaryEmailUpdate,
}

impl Route {
    /// Whether the op mutates persisted state (region settle-on-read counts, so
    /// the settled ENABLING->ENABLED transition survives a restart).
    fn mutates(&self) -> bool {
        matches!(
            self,
            Route::AcceptPrimaryEmailUpdate
                | Route::DeleteAlternateContact
                | Route::DisableRegion
                | Route::EnableRegion
                | Route::GetRegionOptStatus
                | Route::ListRegions
                | Route::PutAccountName
                | Route::PutAlternateContact
                | Route::PutContactInformation
                | Route::StartPrimaryEmailUpdate
        )
    }
}

pub struct AccountService {
    state: SharedAccountState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl AccountService {
    pub fn new(state: SharedAccountState) -> Self {
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
            (&Method::POST, ["acceptPrimaryEmailUpdate"]) => Some(Route::AcceptPrimaryEmailUpdate),
            (&Method::POST, ["deleteAlternateContact"]) => Some(Route::DeleteAlternateContact),
            (&Method::POST, ["disableRegion"]) => Some(Route::DisableRegion),
            (&Method::POST, ["enableRegion"]) => Some(Route::EnableRegion),
            (&Method::POST, ["getAccountInformation"]) => Some(Route::GetAccountInformation),
            (&Method::POST, ["getAlternateContact"]) => Some(Route::GetAlternateContact),
            (&Method::POST, ["getContactInformation"]) => Some(Route::GetContactInformation),
            (&Method::POST, ["getGovCloudAccountInformation"]) => {
                Some(Route::GetGovCloudAccountInformation)
            }
            (&Method::POST, ["getPrimaryEmail"]) => Some(Route::GetPrimaryEmail),
            (&Method::POST, ["getPrimaryEmailUpdateStatus"]) => {
                Some(Route::GetPrimaryEmailUpdateStatus)
            }
            (&Method::POST, ["getRegionOptStatus"]) => Some(Route::GetRegionOptStatus),
            (&Method::POST, ["listRegions"]) => Some(Route::ListRegions),
            (&Method::POST, ["putAccountName"]) => Some(Route::PutAccountName),
            (&Method::POST, ["putAlternateContact"]) => Some(Route::PutAlternateContact),
            (&Method::POST, ["putContactInformation"]) => Some(Route::PutContactInformation),
            (&Method::POST, ["startPrimaryEmailUpdate"]) => Some(Route::StartPrimaryEmailUpdate),
            _ => None,
        }
    }

    // --- alternate contacts ------------------------------------------------

    fn put_alternate_contact(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let contact_type = require_contact_type(&body)?;
        let contact = AlternateContact {
            name: require_str_max(&body, "Name", 64)?.to_string(),
            title: require_str_max(&body, "Title", 50)?.to_string(),
            email_address: require_str_max(&body, "EmailAddress", 254)?.to_string(),
            phone_number: require_str_max(&body, "PhoneNumber", 25)?.to_string(),
            contact_type: contact_type.clone(),
        };
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        st.alternate_contacts.insert(contact_type, contact);
        Ok(empty_ok())
    }

    fn get_alternate_contact(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let contact_type = require_contact_type(&body)?;
        let accounts = self.state.read();
        let contact = accounts
            .get(&account)
            .and_then(|s| s.alternate_contacts.get(&contact_type))
            .ok_or_else(|| {
                not_found(&format!(
                    "No {contact_type} alternate contact found for this account."
                ))
            })?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "AlternateContact": alternate_contact_json(contact) }),
        ))
    }

    fn delete_alternate_contact(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let contact_type = require_contact_type(&body)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        if st.alternate_contacts.remove(&contact_type).is_none() {
            return Err(not_found(&format!(
                "No {contact_type} alternate contact found for this account."
            )));
        }
        Ok(empty_ok())
    }

    // --- contact information ----------------------------------------------

    fn put_contact_information(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let info = body
            .get("ContactInformation")
            .filter(|v| v.is_object())
            .ok_or_else(|| validation("ContactInformation is required."))?;
        // FullName, AddressLine1, City, CountryCode, PhoneNumber, PostalCode are
        // required members of the ContactInformation shape.
        for field in [
            "FullName",
            "AddressLine1",
            "City",
            "CountryCode",
            "PhoneNumber",
            "PostalCode",
        ] {
            if info
                .get(field)
                .and_then(Value::as_str)
                .unwrap_or("")
                .is_empty()
            {
                return Err(validation(&format!(
                    "ContactInformation.{field} is required."
                )));
            }
        }
        let info = info.clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        st.contact_information = Some(info);
        Ok(empty_ok())
    }

    fn get_contact_information(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let accounts = self.state.read();
        let info = accounts
            .get(&account)
            .and_then(|s| s.contact_information.clone())
            .ok_or_else(|| not_found("No contact information found for this account."))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "ContactInformation": info }),
        ))
    }

    // --- account information ----------------------------------------------

    fn get_account_information(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        let mut out = json!({
            "AccountId": account,
            "AccountCreatedDate": st.created_date.timestamp(),
            "AccountState": st.account_state,
        });
        if let Some(name) = &st.account_name {
            out["AccountName"] = json!(name);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn put_account_name(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let name = require_str_max(&body, "AccountName", 50)?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        st.account_name = Some(name);
        Ok(empty_ok())
    }

    fn get_gov_cloud_account_information(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let standard = require_str(&body, "StandardAccountId")?.to_string();
        validate_account_id(&standard)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&standard);
        // A GovCloud account id is deterministically derived from the standard id
        // so the pairing is stable across calls.
        let gov_id = format!("9{}", &standard[1..]);
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "GovCloudAccountId": gov_id, "AccountState": st.account_state }),
        ))
    }

    // --- primary email -----------------------------------------------------

    fn get_primary_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = require_account(&body)?;
        let accounts = self.state.read();
        let email = accounts
            .get(&account)
            .map(|s| s.primary_email.clone())
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| default_primary_email(&account));
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "PrimaryEmail": email }),
        ))
    }

    fn get_primary_email_update_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = require_account(&body)?;
        let accounts = self.state.read();
        let st = accounts
            .get(&account)
            .ok_or_else(|| not_found("No primary email update in progress for this account."))?;
        // A change awaiting AcceptPrimaryEmailUpdate is PENDING; once accepted the
        // primary email is set and the last update reports ACCEPTED. With neither,
        // no update has ever been started for the account.
        let status = if st.pending_email_update.is_some() {
            "PENDING"
        } else if !st.primary_email.is_empty() {
            "ACCEPTED"
        } else {
            return Err(not_found(
                "No primary email update in progress for this account.",
            ));
        };
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Status": status, "UpdatedAt": st.created_date.timestamp() }),
        ))
    }

    fn start_primary_email_update(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = require_account(&body)?;
        let email = require_str(&body, "PrimaryEmail")?.to_string();
        validate_email(&email)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        // A fixed OTP keeps the flow testable; the real service emails a code.
        st.pending_email_update = Some(PendingEmailUpdate {
            email,
            otp: "000000".to_string(),
        });
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "Status": "PENDING" }),
        ))
    }

    fn accept_primary_email_update(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = require_account(&body)?;
        let email = require_str(&body, "PrimaryEmail")?.to_string();
        let otp = require_str(&body, "Otp")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        match &st.pending_email_update {
            Some(pending) if pending.email == email && pending.otp == otp => {
                st.primary_email = email;
                st.pending_email_update = None;
                Ok(AwsResponse::json_value(
                    StatusCode::OK,
                    json!({ "Status": "ACCEPTED" }),
                ))
            }
            _ => Err(validation(
                "The one-time password or email address does not match the pending update.",
            )),
        }
    }

    // --- regions -----------------------------------------------------------

    fn get_region_opt_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let region = require_str(&body, "RegionName")?.to_string();
        default_region_status(&region)
            .ok_or_else(|| validation(&format!("Region {region} is not valid.")))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        settle_region(st.region_opt_status.get_mut(&region));
        let status = region_status(st.region_opt_status.get(&region), &region);
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "RegionName": region, "RegionOptStatus": status }),
        ))
    }

    fn list_regions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let filter: Vec<String> = body
            .get("RegionOptStatusContains")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let max_results = match body.get("MaxResults") {
            Some(v) => {
                let n = v
                    .as_i64()
                    .ok_or_else(|| validation("MaxResults must be an integer."))?;
                if !(1..=50).contains(&n) {
                    return Err(validation("MaxResults must be between 1 and 50."));
                }
                n as usize
            }
            None => 50,
        };
        let start = match body.get("NextToken").and_then(Value::as_str) {
            None | Some("") => 0,
            Some(t) => t
                .parse::<usize>()
                .map_err(|_| validation("The NextToken provided is invalid."))?,
        };

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        // Settle any in-flight transitions so the listing reflects them.
        for (name, _) in REGIONS {
            settle_region(st.region_opt_status.get_mut(*name));
        }
        let all: Vec<Value> = REGIONS
            .iter()
            .map(|(name, _)| {
                let status = region_status(st.region_opt_status.get(*name), name);
                json!({ "RegionName": name, "RegionOptStatus": status })
            })
            .filter(|r| {
                filter.is_empty() || filter.iter().any(|f| r["RegionOptStatus"] == f.as_str())
            })
            .collect();
        let total = all.len();
        let end = start.saturating_add(max_results).min(total);
        let page = if start >= total {
            Vec::new()
        } else {
            all[start..end].to_vec()
        };
        let mut out = json!({ "Regions": page });
        if end < total {
            out["NextToken"] = json!(end.to_string());
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn enable_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_region(req, true)
    }

    fn disable_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_region(req, false)
    }

    fn set_region(&self, req: &AwsRequest, enable: bool) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let account = target_account(&body, req)?;
        let region = require_str(&body, "RegionName")?.to_string();
        let opt_in = REGIONS
            .iter()
            .find(|(r, _)| *r == region)
            .map(|(_, o)| *o)
            .ok_or_else(|| validation(&format!("Region {region} is not valid.")))?;
        // Only opt-in regions can be enabled/disabled; the always-on regions are
        // ENABLED_BY_DEFAULT and cannot be toggled.
        if !opt_in {
            return Err(validation(&format!(
                "Region {region} cannot be enabled or disabled."
            )));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        st.region_opt_status.insert(
            region,
            if enable { "ENABLING" } else { "DISABLING" }.to_string(),
        );
        Ok(empty_ok())
    }
}

#[async_trait]
impl AwsService for AccountService {
    fn service_name(&self) -> &str {
        "account"
    }

    fn supported_actions(&self) -> &[&str] {
        ACCOUNT_ACTIONS
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
            Route::AcceptPrimaryEmailUpdate => self.accept_primary_email_update(&req),
            Route::DeleteAlternateContact => self.delete_alternate_contact(&req),
            Route::DisableRegion => self.disable_region(&req),
            Route::EnableRegion => self.enable_region(&req),
            Route::GetAccountInformation => self.get_account_information(&req),
            Route::GetAlternateContact => self.get_alternate_contact(&req),
            Route::GetContactInformation => self.get_contact_information(&req),
            Route::GetGovCloudAccountInformation => self.get_gov_cloud_account_information(&req),
            Route::GetPrimaryEmail => self.get_primary_email(&req),
            Route::GetPrimaryEmailUpdateStatus => self.get_primary_email_update_status(&req),
            Route::GetRegionOptStatus => self.get_region_opt_status(&req),
            Route::ListRegions => self.list_regions(&req),
            Route::PutAccountName => self.put_account_name(&req),
            Route::PutAlternateContact => self.put_alternate_contact(&req),
            Route::PutContactInformation => self.put_contact_information(&req),
            Route::StartPrimaryEmailUpdate => self.start_primary_email_update(&req),
        };
        if mutates && result.is_ok() {
            self.persist().await;
        }
        result
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn alternate_contact_json(c: &AlternateContact) -> Value {
    json!({
        "Name": c.name,
        "Title": c.title,
        "EmailAddress": c.email_address,
        "PhoneNumber": c.phone_number,
        "AlternateContactType": c.contact_type,
    })
}

/// Promote a settling region status one step: ENABLING->ENABLED,
/// DISABLING->DISABLED. Other values are left untouched.
fn settle_region(status: Option<&mut String>) {
    if let Some(s) = status {
        if s == "ENABLING" {
            *s = "ENABLED".to_string();
        } else if s == "DISABLING" {
            *s = "DISABLED".to_string();
        }
    }
}

/// The reported opt status for a region: an explicit override if present, else
/// the region's default.
fn region_status(override_status: Option<&String>, region: &str) -> String {
    override_status.cloned().unwrap_or_else(|| {
        default_region_status(region)
            .unwrap_or("DISABLED")
            .to_string()
    })
}

fn default_primary_email(account: &str) -> String {
    format!("root+{account}@fakecloud.example.com")
}

fn empty_ok() -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, json!({}))
}

fn parse_json(body: &[u8]) -> Result<Value, AwsServiceError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| validation(&format!("Invalid request body: {e}")))
}

fn require_str<'a>(body: &'a Value, key: &str) -> Result<&'a str, AwsServiceError> {
    body.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation(&format!("{key} is required.")))
}

/// Require a string member and enforce its model `max` length.
fn require_str_max<'a>(body: &'a Value, key: &str, max: usize) -> Result<&'a str, AwsServiceError> {
    let s = require_str(body, key)?;
    if s.chars().count() > max {
        return Err(validation(&format!(
            "{key} must be at most {max} characters."
        )));
    }
    Ok(s)
}

/// Resolve a target account that the model marks `@required` (the primary-email
/// ops): `AccountId` must be present and well-formed.
fn require_account(body: &Value) -> Result<String, AwsServiceError> {
    let id = require_str(body, "AccountId")?;
    validate_account_id(id)?;
    Ok(id.to_string())
}

/// Resolve the account the request targets: the optional `AccountId` member
/// (used by an organization's management account to act on a member) or, absent
/// that, the caller's own account.
fn target_account(body: &Value, req: &AwsRequest) -> Result<String, AwsServiceError> {
    match body.get("AccountId").and_then(Value::as_str) {
        Some(id) if !id.is_empty() => {
            validate_account_id(id)?;
            Ok(id.to_string())
        }
        _ => Ok(req.account_id.clone()),
    }
}

fn validate_account_id(id: &str) -> Result<(), AwsServiceError> {
    if id.len() == 12 && id.chars().all(|c| c.is_ascii_digit()) {
        Ok(())
    } else {
        Err(validation(&format!("{id} is not a valid account id.")))
    }
}

fn require_contact_type(body: &Value) -> Result<String, AwsServiceError> {
    let t = require_str(body, "AlternateContactType")?;
    if ALTERNATE_CONTACT_TYPES.contains(&t) {
        Ok(t.to_string())
    } else {
        Err(validation(&format!(
            "AlternateContactType must be one of BILLING, OPERATIONS, SECURITY (got {t})."
        )))
    }
}

fn validate_email(email: &str) -> Result<(), AwsServiceError> {
    if email.contains('@') && email.len() >= 6 {
        Ok(())
    } else {
        Err(validation("The email address provided is not valid."))
    }
}

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> AccountService {
        AccountService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "111122223333",
            "us-east-1",
            "",
        ))))
    }

    fn req(path: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "account".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "111122223333".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: path.to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn body_of(svc: &AccountService, path: &str, b: Value) -> Value {
        let resp = svc.handle(req(path, b)).await.unwrap();
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    async fn err(svc: &AccountService, path: &str, b: Value) -> AwsServiceError {
        svc.handle(req(path, b)).await.err().unwrap()
    }

    #[tokio::test]
    async fn alternate_contact_round_trip() {
        let s = svc();
        s.handle(req(
            "/putAlternateContact",
            json!({
                "AlternateContactType": "BILLING",
                "Name": "Fin Ops",
                "Title": "Controller",
                "EmailAddress": "billing@example.com",
                "PhoneNumber": "+1-555-0100"
            }),
        ))
        .await
        .unwrap();
        let v = body_of(
            &s,
            "/getAlternateContact",
            json!({"AlternateContactType":"BILLING"}),
        )
        .await;
        assert_eq!(v["AlternateContact"]["Name"], "Fin Ops");
        assert_eq!(v["AlternateContact"]["AlternateContactType"], "BILLING");
        // Delete then get -> 404.
        s.handle(req(
            "/deleteAlternateContact",
            json!({"AlternateContactType":"BILLING"}),
        ))
        .await
        .unwrap();
        let e = err(
            &s,
            "/getAlternateContact",
            json!({"AlternateContactType":"BILLING"}),
        )
        .await;
        assert_eq!(e.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn alternate_contact_type_and_length_validation() {
        let s = svc();
        let e = err(
            &s,
            "/getAlternateContact",
            json!({"AlternateContactType":"NOPE"}),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
        let e = err(
            &s,
            "/putAlternateContact",
            json!({
                "AlternateContactType": "SECURITY",
                "Name": "x".repeat(65),
                "Title": "T",
                "EmailAddress": "a@b.com",
                "PhoneNumber": "1"
            }),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
    }

    #[tokio::test]
    async fn contact_information_round_trip() {
        let s = svc();
        let info = json!({
            "FullName": "Acme Inc",
            "AddressLine1": "1 Main St",
            "City": "Seattle",
            "CountryCode": "US",
            "PhoneNumber": "+1-555-0100",
            "PostalCode": "98101"
        });
        s.handle(req(
            "/putContactInformation",
            json!({"ContactInformation": info}),
        ))
        .await
        .unwrap();
        let v = body_of(&s, "/getContactInformation", json!({})).await;
        assert_eq!(v["ContactInformation"]["City"], "Seattle");
        // Missing required field -> validation error.
        let e = err(
            &s,
            "/putContactInformation",
            json!({"ContactInformation": {"FullName": "x"}}),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
    }

    #[tokio::test]
    async fn account_name_and_information() {
        let s = svc();
        s.handle(req("/putAccountName", json!({"AccountName": "Prod"})))
            .await
            .unwrap();
        let v = body_of(&s, "/getAccountInformation", json!({})).await;
        assert_eq!(v["AccountName"], "Prod");
        assert_eq!(v["AccountId"], "111122223333");
        assert_eq!(v["AccountState"], "ACTIVE");
        // Over-long name -> validation.
        let e = err(
            &s,
            "/putAccountName",
            json!({"AccountName": "x".repeat(51)}),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
    }

    #[tokio::test]
    async fn primary_email_update_flow_requires_account_id() {
        let s = svc();
        // AccountId is @required for the primary-email ops.
        let e = err(&s, "/getPrimaryEmail", json!({})).await;
        assert_eq!(e.code(), "ValidationException");

        let acct = json!({"AccountId": "111122223333"});
        let v = body_of(
            &s,
            "/startPrimaryEmailUpdate",
            json!({"AccountId":"111122223333","PrimaryEmail":"new@example.com"}),
        )
        .await;
        assert_eq!(v["Status"], "PENDING");
        // Wrong OTP -> validation.
        let e = err(
            &s,
            "/acceptPrimaryEmailUpdate",
            json!({"AccountId":"111122223333","PrimaryEmail":"new@example.com","Otp":"999999"}),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
        // Correct OTP settles the change.
        let v = body_of(
            &s,
            "/acceptPrimaryEmailUpdate",
            json!({"AccountId":"111122223333","PrimaryEmail":"new@example.com","Otp":"000000"}),
        )
        .await;
        assert_eq!(v["Status"], "ACCEPTED");
        let v = body_of(&s, "/getPrimaryEmail", acct).await;
        assert_eq!(v["PrimaryEmail"], "new@example.com");
    }

    #[tokio::test]
    async fn region_opt_in_flow() {
        let s = svc();
        // Opt-in region defaults to DISABLED.
        let v = body_of(
            &s,
            "/getRegionOptStatus",
            json!({"RegionName":"af-south-1"}),
        )
        .await;
        assert_eq!(v["RegionOptStatus"], "DISABLED");
        // Enable -> settles to ENABLED on the next read.
        s.handle(req("/enableRegion", json!({"RegionName":"af-south-1"})))
            .await
            .unwrap();
        let v = body_of(
            &s,
            "/getRegionOptStatus",
            json!({"RegionName":"af-south-1"}),
        )
        .await;
        assert_eq!(v["RegionOptStatus"], "ENABLED");
        // A default region reports ENABLED_BY_DEFAULT and cannot be toggled.
        let v = body_of(&s, "/getRegionOptStatus", json!({"RegionName":"us-east-1"})).await;
        assert_eq!(v["RegionOptStatus"], "ENABLED_BY_DEFAULT");
        let e = err(&s, "/enableRegion", json!({"RegionName":"us-east-1"})).await;
        assert_eq!(e.code(), "ValidationException");
        // Unknown region -> validation.
        let e = err(
            &s,
            "/getRegionOptStatus",
            json!({"RegionName":"xx-nowhere-9"}),
        )
        .await;
        assert_eq!(e.code(), "ValidationException");
    }

    #[tokio::test]
    async fn list_regions_filters_and_paginates() {
        let s = svc();
        let v = body_of(
            &s,
            "/listRegions",
            json!({"RegionOptStatusContains":["ENABLED_BY_DEFAULT"]}),
        )
        .await;
        let regions = v["Regions"].as_array().unwrap();
        assert!(!regions.is_empty());
        assert!(regions
            .iter()
            .all(|r| r["RegionOptStatus"] == "ENABLED_BY_DEFAULT"));
        // Pagination: MaxResults caps the page and yields a NextToken.
        let v = body_of(&s, "/listRegions", json!({"MaxResults": 5})).await;
        assert_eq!(v["Regions"].as_array().unwrap().len(), 5);
        assert!(v["NextToken"].is_string());
    }

    #[tokio::test]
    async fn gov_cloud_account_information() {
        let s = svc();
        let v = body_of(
            &s,
            "/getGovCloudAccountInformation",
            json!({"StandardAccountId":"111122223333"}),
        )
        .await;
        assert_eq!(v["GovCloudAccountId"], "911122223333");
        assert_eq!(v["AccountState"], "ACTIVE");
    }

    #[test]
    fn service_name_and_actions() {
        let s = svc();
        assert_eq!(s.service_name(), "account");
        assert_eq!(s.supported_actions().len(), 15);
    }
}
