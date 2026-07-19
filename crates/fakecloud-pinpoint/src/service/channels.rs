//! Platform channel handlers (ADM, APNS + sandbox/VoIP variants, Baidu, Email,
//! GCM, SMS, Voice) plus the aggregate `GetChannels` read.
//!
//! Each channel response projects a set of members universal to every
//! `*ChannelResponse` shape (so the projection is model-valid for any channel),
//! carrying the platform-specific `Platform` value and the `Enabled` flag echoed
//! from the update request. Baidu additionally carries the required
//! `Credential`.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{copy_many, not_found, ok, Ctx, PinpointService};
use crate::shared;

impl PinpointService {
    pub(super) fn get_channel(
        &self,
        ctx: &Ctx,
        app_id: &str,
        channel: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            app.channels
                .get(channel)
                .cloned()
                .ok_or_else(|| not_found_channel(channel))
        })
        .and_then(ok)
    }

    pub(super) fn update_channel(
        &self,
        ctx: &Ctx,
        app_id: &str,
        channel: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let record = build_channel(app_id, channel, body);
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        app.channels.insert(channel.to_string(), record.clone());
        ok(record)
    }

    pub(super) fn delete_channel(
        &self,
        ctx: &Ctx,
        app_id: &str,
        channel: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let app = guard
            .get_mut(&ctx.account)
            .and_then(|d| d.apps.get_mut(app_id))
            .ok_or_else(|| super::not_found_app(app_id))?;
        match app.channels.remove(channel) {
            Some(mut rec) => {
                if let Some(obj) = rec.as_object_mut() {
                    obj.insert("Enabled".into(), json!(false));
                }
                ok(rec)
            }
            None => Err(not_found_channel(channel)),
        }
    }

    pub(super) fn get_channels(
        &self,
        ctx: &Ctx,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_app(&ctx.account, app_id, |app| {
            let mut channels = Map::new();
            for (key, rec) in &app.channels {
                channels.insert(
                    shared::channel_platform(key).to_string(),
                    generic_channel(rec),
                );
            }
            Ok(json!({ "Channels": channels }))
        })
        .and_then(ok)
    }
}

/// Project a platform channel record onto the generic `ChannelResponse` shape
/// used as the value type of `ChannelsResponse.Channels` (no `Platform` /
/// `Credential` members).
fn generic_channel(rec: &Value) -> Value {
    const MEMBERS: &[&str] = &[
        "ApplicationId",
        "CreationDate",
        "Enabled",
        "HasCredential",
        "Id",
        "IsArchived",
        "LastModifiedBy",
        "LastModifiedDate",
        "Version",
    ];
    let mut out = Map::new();
    super::copy_many(&mut out, rec, MEMBERS);
    Value::Object(out)
}

fn not_found_channel(channel: &str) -> AwsServiceError {
    not_found(&format!(
        "Channel '{channel}' is not configured for this application."
    ))
}

/// Non-secret config members shared between a `*ChannelRequest` and its
/// `*ChannelResponse`. Secrets (ApiKey / Certificate / PrivateKey / SecretKey /
/// ClientSecret / ServiceJson / ...) are deliberately NOT echoed — a channel
/// response summarizes credentials via `HasCredential` / `Credential`, so
/// blanket-copying the request would leak secrets and add members absent from
/// the response shape.
const CHANNEL_CONFIG: &[&str] = &[
    "ConfigurationSet",
    "FromAddress",
    "Identity",
    "RoleArn",
    "OrchestrationSendingRoleArn",
    "SenderId",
    "ShortCode",
    "DefaultAuthenticationMethod",
];

fn build_channel(app_id: &str, channel: &str, body: &Value) -> Value {
    let now = shared::now_iso();
    let enabled = body.get("Enabled").and_then(Value::as_bool).unwrap_or(true);
    let mut out = Map::new();
    out.insert("Id".into(), json!(shared::hex_id()));
    out.insert("ApplicationId".into(), json!(app_id));
    out.insert("Platform".into(), json!(shared::channel_platform(channel)));
    out.insert("CreationDate".into(), json!(now));
    out.insert("LastModifiedDate".into(), json!(now));
    out.insert("Enabled".into(), json!(enabled));
    out.insert("HasCredential".into(), json!(true));
    out.insert("IsArchived".into(), json!(false));
    out.insert("Version".into(), json!(1));
    // Echo the non-secret channel config the client wrote. Previously dropped:
    // Get*Channel returned only Enabled, losing FromAddress / SenderId / etc.
    copy_many(&mut out, body, CHANNEL_CONFIG);
    // `Credential` is a response member for channels whose request supplies an
    // `ApiKey` (Baidu/GCM); it is required by `BaiduChannelResponse`.
    if matches!(channel, "baidu" | "gcm") {
        let cred = body.get("ApiKey").and_then(Value::as_str).unwrap_or("");
        out.insert("Credential".into(), json!(cred));
    }
    Value::Object(out)
}
