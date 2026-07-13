//! Message-sending, OTP, phone-validation, event-ingest and event-stream
//! handlers.
//!
//! There is no real delivery: the send operations validate their input and
//! return a structurally-correct response with a per-address delivery-status
//! entry, but transmit nothing to any provider.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{accepted, not_found, ok, str_field, Ctx, PinpointService};
use crate::shared;

impl PinpointService {
    pub(super) fn send_messages(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = ctx;
        let mut result = Map::new();
        for key in address_and_endpoint_keys(body) {
            result.insert(
                key,
                json!({
                    "DeliveryStatus": "SUCCESSFUL",
                    "StatusCode": 200,
                    "StatusMessage": "Message accepted (fakecloud does not deliver).",
                    "MessageId": shared::hex_id(),
                }),
            );
        }
        ok(json!({
            "ApplicationId": app_id,
            "RequestId": shared::hex_id(),
            "Result": result,
        }))
    }

    pub(super) fn send_users_messages(
        &self,
        ctx: &Ctx,
        app_id: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = ctx;
        ok(json!({
            "ApplicationId": app_id,
            "RequestId": shared::hex_id(),
        }))
    }

    pub(super) fn send_otp_message(
        &self,
        ctx: &Ctx,
        app_id: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = ctx;
        ok(json!({
            "ApplicationId": app_id,
            "RequestId": shared::hex_id(),
        }))
    }

    pub(super) fn verify_otp_message(
        &self,
        ctx: &Ctx,
        _app_id: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = ctx;
        ok(json!({ "Valid": true }))
    }

    pub(super) fn phone_number_validate(
        &self,
        _ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let number = str_field(body, "PhoneNumber");
        ok(json!({
            "OriginalPhoneNumber": number,
            "PhoneType": "MOBILE",
            "PhoneTypeCode": 0,
        }))
    }

    pub(super) fn put_events(
        &self,
        _ctx: &Ctx,
        _app_id: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        accepted(json!({ "Results": {} }))
    }

    pub(super) fn put_event_stream(
        &self,
        ctx: &Ctx,
        app_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let record = json!({
            "ApplicationId": app_id,
            "DestinationStreamArn": str_field(body, "DestinationStreamArn"),
            "RoleArn": str_field(body, "RoleArn"),
            "LastModifiedDate": shared::now_iso(),
            "LastUpdatedBy": ctx.account,
        });
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.event_stream = Some(record.clone());
        ok(record)
    }

    pub(super) fn get_event_stream(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.event_stream
                .clone()
                .ok_or_else(|| not_found("No event stream is configured for this application."))
        })
        .and_then(ok)
    }

    pub(super) fn delete_event_stream(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        match app.event_stream.take() {
            Some(rec) => ok(rec),
            None => Err(not_found(
                "No event stream is configured for this application.",
            )),
        }
    }
}

/// The union of the `Addresses` and `Endpoints` keys of a `MessageRequest`.
fn address_and_endpoint_keys(body: &Value) -> Vec<String> {
    let mut keys = Vec::new();
    for field in ["Addresses", "Endpoints"] {
        if let Some(obj) = body.get(field).and_then(Value::as_object) {
            keys.extend(obj.keys().cloned());
        }
    }
    keys
}
