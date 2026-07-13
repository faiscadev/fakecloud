//! Endpoint + user handlers: get/update/delete endpoints, batch upsert,
//! attribute removal, and the per-user endpoint reads.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{accepted, copy_many, not_found, not_found_app, ok, Ctx, PinpointService};
use crate::shared;

/// Scalar `EndpointResponse` members echoed from the request, plus the nested
/// `User` (identical `EndpointUser` shape on request + response, so it's safe to
/// round-trip verbatim for the user-endpoint reads).
const ENDPOINT_MEMBERS: &[&str] = &[
    "Address",
    "ChannelType",
    "EndpointStatus",
    "OptOut",
    "RequestId",
    "CohortId",
    "EffectiveDate",
    "Attributes",
    "Demographic",
    "Location",
    "Metrics",
    "User",
];

impl PinpointService {
    pub(super) fn get_endpoint(
        &self,
        ctx: &Ctx,
        app_id: &str,
        eid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.endpoints
                .get(eid)
                .cloned()
                .ok_or_else(|| not_found_endpoint(eid))
        })
        .and_then(ok)
    }

    pub(super) fn update_endpoint(
        &self,
        ctx: &Ctx,
        app_id: &str,
        eid: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let record = build_endpoint(app_id, eid, body);
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.endpoints.insert(eid.to_string(), record);
        accepted(json!({ "Message": "Accepted", "RequestID": shared::hex_id() }))
    }

    pub(super) fn delete_endpoint(
        &self,
        ctx: &Ctx,
        app_id: &str,
        eid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        match app.endpoints.remove(eid) {
            Some(rec) => Ok(AwsResponse::json_value(http::StatusCode::ACCEPTED, rec)),
            None => Err(not_found_endpoint(eid)),
        }
    }

    pub(super) fn update_endpoints_batch(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let items = body
            .get("Item")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        for item in &items {
            let eid = item
                .get("Id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(shared::hex_id);
            let record = build_endpoint(app_id, &eid, item);
            app.endpoints.insert(eid, record);
        }
        accepted(json!({ "Message": "Accepted", "RequestID": shared::hex_id() }))
    }

    pub(super) fn remove_attributes(
        &self,
        ctx: &Ctx,
        app_id: &str,
        attr_type: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `AttributeType` selects which endpoint sub-map the blacklisted keys are
        // stripped from, mirroring AWS: custom attributes + metrics live at the
        // endpoint top level, user attributes under `User.UserAttributes`.
        let field = match attr_type {
            "endpoint-custom-metrics" => "Metrics",
            "endpoint-user-attributes" => "UserAttributes",
            _ => "Attributes",
        };
        let blacklist: Vec<String> = body
            .get("Blacklist")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();

        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        for endpoint in app.endpoints.values_mut() {
            let target = if field == "UserAttributes" {
                endpoint
                    .get_mut("User")
                    .and_then(|u| u.get_mut("UserAttributes"))
            } else {
                endpoint.get_mut(field)
            };
            if let Some(Value::Object(map)) = target {
                for key in &blacklist {
                    map.remove(key);
                }
            }
        }
        ok(json!({
            "ApplicationId": app_id,
            "AttributeType": attr_type,
            "Attributes": blacklist,
        }))
    }

    pub(super) fn get_user_endpoints(
        &self,
        ctx: &Ctx,
        app_id: &str,
        user_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            let item: Vec<Value> = app
                .endpoints
                .values()
                .filter(|e| endpoint_user_id(e) == Some(user_id))
                .cloned()
                .collect();
            Ok(json!({ "Item": item }))
        })
        .and_then(ok)
    }

    pub(super) fn delete_user_endpoints(
        &self,
        ctx: &Ctx,
        app_id: &str,
        user_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        let matched: Vec<String> = app
            .endpoints
            .iter()
            .filter(|(_, e)| endpoint_user_id(e) == Some(user_id))
            .map(|(k, _)| k.clone())
            .collect();
        let mut removed = Vec::new();
        for k in matched {
            if let Some(e) = app.endpoints.remove(&k) {
                removed.push(e);
            }
        }
        ok(json!({ "Item": removed }))
    }

    pub(super) fn get_in_app_messages(
        &self,
        ctx: &Ctx,
        app_id: &str,
        _eid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |_| {
            Ok(json!({ "InAppMessageCampaigns": [] }))
        })
        .and_then(ok)
    }
}

fn not_found_endpoint(eid: &str) -> AwsServiceError {
    not_found(&format!("Endpoint with id '{eid}' does not exist."))
}

fn endpoint_user_id(endpoint: &Value) -> Option<&str> {
    endpoint.get("User")?.get("UserId")?.as_str()
}

fn build_endpoint(app_id: &str, eid: &str, body: &Value) -> Value {
    let mut out = Map::new();
    out.insert("Id".into(), json!(eid));
    out.insert("ApplicationId".into(), json!(app_id));
    out.insert("CreationDate".into(), json!(shared::now_iso()));
    copy_many(&mut out, body, ENDPOINT_MEMBERS);
    Value::Object(out)
}
