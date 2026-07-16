//! Resource-specific handlers that the generic engine cannot express: ARN-keyed
//! tagging, position configurations (a `Put`/`Get` pair keyed by resource
//! identifier), resource positions (a raw `@httpPayload` GeoJSON blob stored
//! per resource), and the wireless-device import-task lookup (which falls back
//! to the wireless-device store for the model's cross-resource read pairing).

use std::collections::HashMap;

use http::{HeaderMap, StatusCode};
use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::OpMeta;

use super::{
    build_output, mint_arn, mint_uuid, now_epoch, ok_json, query_get, resource_type, storage_key,
    Ctx, IotWirelessService,
};

type Handled = Result<Option<(AwsResponse, bool)>, AwsServiceError>;

#[allow(clippy::too_many_arguments)]
pub(super) fn dispatch(
    svc: &IotWirelessService,
    meta: &'static OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    _headers: &HeaderMap,
    raw_body: &[u8],
    body: &Map<String, Value>,
) -> Handled {
    match meta.op {
        "TagResource" => Ok(Some(tag_resource(svc, ctx, query, body))),
        "UntagResource" => Ok(Some(untag_resource(svc, ctx, query))),
        "ListTagsForResource" => Ok(Some(list_tags(svc, ctx, query))),

        "PutPositionConfiguration" => Ok(Some(put_position_configuration(svc, ctx, labels, body))),
        "UpdateResourcePosition" => Ok(Some(update_resource_position(svc, ctx, labels, raw_body))),
        "GetResourcePosition" => Ok(Some(get_resource_position(svc, ctx, labels)?)),

        "GetWirelessDeviceImportTask" => Ok(Some(get_wireless_device_import_task(
            svc, meta, ctx, labels,
        )?)),

        // ---- per-resource log levels (finding 1) ----
        // `PutResourceLogLevel` writes `resources["log-levels"][ResourceIdentifier]`;
        // `GetResourceLogLevel` (Verb::Get) reads it through the generic engine,
        // 404'ing when never written. `ResetResourceLogLevel` deletes it.
        "PutResourceLogLevel" => Ok(Some(put_resource_log_level(svc, ctx, labels, query, body))),
        "ResetResourceLogLevel" => Ok(Some(reset_resource_log_level(svc, ctx, labels))),

        // ---- account-scoped singleton configurations (finding 2) ----
        "UpdateMetricConfiguration" => Ok(Some(update_singleton(
            svc,
            ctx,
            "metric-configuration",
            body,
        ))),
        "GetMetricConfiguration" => Ok(Some(get_singleton(
            svc,
            ctx,
            meta,
            "metric-configuration",
            metric_configuration_default(),
        ))),
        "UpdateLogLevelsByResourceTypes" => Ok(Some(update_singleton(
            svc,
            ctx,
            "log-levels-by-resource-types",
            body,
        ))),
        "GetLogLevelsByResourceTypes" => Ok(Some(get_singleton(
            svc,
            ctx,
            meta,
            "log-levels-by-resource-types",
            log_levels_default(),
        ))),
        "UpdateEventConfigurationByResourceTypes" => Ok(Some(update_singleton(
            svc,
            ctx,
            "event-configurations-resource-types",
            body,
        ))),
        "GetEventConfigurationByResourceTypes" => Ok(Some(get_singleton(
            svc,
            ctx,
            meta,
            "event-configurations-resource-types",
            Value::Object(Map::new()),
        ))),

        // ---- Action ops that mint / return output identifiers (finding 3) ----
        "SendDataToWirelessDevice" | "SendDataToMulticastGroup" => Ok(Some(send_data(labels))),
        "StartWirelessDeviceImportTask" | "StartSingleWirelessDeviceImportTask" => {
            Ok(Some(start_import_task(svc, ctx, body)))
        }
        "CreateWirelessGatewayTask" => {
            Ok(Some(create_wireless_gateway_task(svc, ctx, labels, body)))
        }
        "GetWirelessGatewayTask" => Ok(Some(get_wireless_gateway_task(svc, meta, ctx, labels)?)),
        "DeleteWirelessGatewayTask" => Ok(Some(delete_wireless_gateway_task(svc, ctx, labels))),
        "TestWirelessDevice" => Ok(Some(test_wireless_device())),
        "GetServiceEndpoint" => Ok(Some(get_service_endpoint(ctx, query))),
        "GetPositionEstimate" => Ok(Some(get_position_estimate())),
        "GetMetrics" => Ok(Some(get_metrics())),
        "GetWirelessDeviceStatistics" => Ok(Some(wireless_device_statistics(labels))),
        "GetWirelessGatewayStatistics" => Ok(Some(wireless_gateway_statistics(labels))),

        // ---- association membership edges (finding 4) ----
        "AssociateWirelessDeviceWithMulticastGroup" => Ok(Some(associate(
            svc,
            ctx,
            &multicast_devices_key(labels),
            body,
            "WirelessDeviceId",
        ))),
        "AssociateWirelessDeviceWithFuotaTask" => Ok(Some(associate(
            svc,
            ctx,
            &fuota_devices_key(labels),
            body,
            "WirelessDeviceId",
        ))),
        "AssociateMulticastGroupWithFuotaTask" => Ok(Some(associate(
            svc,
            ctx,
            &fuota_multicast_key(labels),
            body,
            "MulticastGroupId",
        ))),
        "DisassociateWirelessDeviceFromMulticastGroup" => Ok(Some(disassociate(
            svc,
            ctx,
            &multicast_devices_key(labels),
            labels.get("WirelessDeviceId").map(String::as_str),
        ))),
        "DisassociateWirelessDeviceFromFuotaTask" => Ok(Some(disassociate(
            svc,
            ctx,
            &fuota_devices_key(labels),
            labels.get("WirelessDeviceId").map(String::as_str),
        ))),
        "DisassociateMulticastGroupFromFuotaTask" => Ok(Some(disassociate(
            svc,
            ctx,
            &fuota_multicast_key(labels),
            labels.get("MulticastGroupId").map(String::as_str),
        ))),
        // Wireless device/gateway <-> IoT thing. Sets ThingArn/ThingName on
        // the stored record so GetWirelessDevice/GetWirelessGateway reflect it.
        "AssociateWirelessDeviceWithThing" => Ok(Some(associate_thing(
            svc,
            ctx,
            "wireless-devices",
            labels.get("Id").map(String::as_str),
            body,
        ))),
        "DisassociateWirelessDeviceFromThing" => Ok(Some(disassociate_thing(
            svc,
            ctx,
            "wireless-devices",
            labels.get("Id").map(String::as_str),
        ))),
        "AssociateWirelessGatewayWithThing" => Ok(Some(associate_thing(
            svc,
            ctx,
            "wireless-gateways",
            labels.get("Id").map(String::as_str),
            body,
        ))),
        "DisassociateWirelessGatewayFromThing" => Ok(Some(disassociate_thing(
            svc,
            ctx,
            "wireless-gateways",
            labels.get("Id").map(String::as_str),
        ))),

        // Wireless gateway <-> IoT certificate.
        "AssociateWirelessGatewayWithCertificate" => Ok(Some(associate_gateway_certificate(
            svc,
            ctx,
            labels.get("Id").map(String::as_str),
            body,
        ))),
        "DisassociateWirelessGatewayFromCertificate" => Ok(Some(disassociate_thing_field(
            svc,
            ctx,
            "wireless-gateways",
            labels.get("Id").map(String::as_str),
            &["IotCertificateId"],
        ))),
        "GetWirelessGatewayCertificate" => {
            Ok(Some(get_wireless_gateway_certificate(svc, ctx, labels)))
        }

        // AWS-account <-> Sidewalk partner account.
        "AssociateAwsAccountWithPartnerAccount" => {
            Ok(Some(associate_partner_account(svc, ctx, body)))
        }
        "DisassociateAwsAccountFromPartnerAccount" => Ok(Some(disassociate_partner_account(
            svc,
            ctx,
            labels.get("PartnerAccountId").map(String::as_str),
        ))),

        "ListMulticastGroupsByFuotaTask" => {
            Ok(Some(list_multicast_groups_by_fuota_task(svc, ctx, labels)))
        }

        // ---- multicast-group LoRaWAN session lifecycle ----
        // Start persists the requested LoRaWAN session under `multicast-sessions`
        // keyed by group id; Get projects it; Cancel clears it.
        "StartMulticastGroupSession" => Ok(Some(start_multicast_session(svc, ctx, labels, body))),
        "GetMulticastGroupSession" => Ok(Some(get_multicast_session(svc, ctx, labels))),
        "CancelMulticastGroupSession" => Ok(Some(cancel_multicast_session(svc, ctx, labels))),

        // ---- FUOTA task session start (transitions the task's Status) ----
        "StartFuotaTask" => Ok(Some(start_fuota_task(svc, ctx, labels))),

        // ---- bulk device <-> multicast-group membership ----
        // The bulk ops select devices by a tag query we do not model; the
        // faithful approximation associates / disassociates every wireless device
        // registered in the account, persisting to the same `multicast-devices`
        // relation the single-device variant writes.
        "StartBulkAssociateWirelessDeviceWithMulticastGroup" => {
            Ok(Some(bulk_multicast_membership(svc, ctx, labels, true)))
        }
        "StartBulkDisassociateWirelessDeviceFromMulticastGroup" => {
            Ok(Some(bulk_multicast_membership(svc, ctx, labels, false)))
        }

        // ---- low-frequency mutators with a concrete state effect ----
        "DeleteQueuedMessages" => Ok(Some(delete_queued_messages(svc, ctx, labels))),
        "DeregisterWirelessDevice" => Ok(Some(deregister_wireless_device(svc, ctx, labels))),
        "ResetAllResourceLogLevels" => Ok(Some(reset_all_resource_log_levels(svc, ctx))),

        _ => Ok(None),
    }
}

// ---------- tags ----------

fn tag_resource(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let entry = data.tags.entry(arn).or_default();
    if let Some(Value::Array(tags)) = body.get("Tags") {
        for t in tags {
            let k = t
                .get("Key")
                .or_else(|| t.get("key"))
                .and_then(Value::as_str);
            let v = t
                .get("Value")
                .or_else(|| t.get("value"))
                .and_then(Value::as_str);
            if let Some(k) = k {
                entry.insert(k.to_string(), v.unwrap_or("").to_string());
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn untag_resource(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(entry) = data.tags.get_mut(&arn) {
        // `TagKeys` is a `@httpQuery` list: each key arrives as a repeated
        // query parameter of the same name.
        for (k, v) in query {
            if k == "tagKeys" {
                entry.remove(v);
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn list_tags(
    svc: &IotWirelessService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("");
    let g = svc.state.read();
    let tags: Vec<Value> = g
        .get(&ctx.account)
        .and_then(|d| d.tags.get(arn))
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect()
        })
        .unwrap_or_default();
    (ok_json(json!({ "Tags": tags })), false)
}

// ---------- position configurations ----------

fn put_position_configuration(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let key = labels
        .get("ResourceIdentifier")
        .cloned()
        .unwrap_or_default();
    let mut record = body.clone();
    record.insert("ResourceIdentifier".to_string(), Value::String(key.clone()));
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("position-configurations", &key, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

// ---------- resource positions (raw @httpPayload GeoJSON blob) ----------

fn resource_position_key(labels: &HashMap<String, String>) -> String {
    format!(
        "resource-position:{}",
        labels
            .get("ResourceIdentifier")
            .map(String::as_str)
            .unwrap_or_default()
    )
}

fn update_resource_position(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    raw_body: &[u8],
) -> (AwsResponse, bool) {
    let key = resource_position_key(labels);
    let payload = String::from_utf8_lossy(raw_body).into_owned();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.blobs.insert(key, payload);
    (ok_json(Value::Object(Map::new())), true)
}

fn get_resource_position(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let key = resource_position_key(labels);
    let g = svc.state.read();
    let payload = g.get(&ctx.account).and_then(|d| d.blobs.get(&key)).cloned();
    // A resource whose position was never `UpdateResourcePosition`'d has no
    // stored GeoJSON; the model declares `ResourceNotFoundException` for it.
    let Some(payload) = payload else {
        let id = labels
            .get("ResourceIdentifier")
            .map(String::as_str)
            .unwrap_or_default();
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No position is set for resource '{id}'."),
        ));
    };
    // `GeoJsonPayload` is an `@httpPayload` blob: the response body IS the raw
    // stored GeoJSON, not a JSON envelope.
    Ok((
        AwsResponse::json(StatusCode::OK, payload.into_bytes()),
        false,
    ))
}

// ---------- wireless-device import task ----------

/// The Smithy round-trip heuristic pairs `UpdateWirelessDevice` (which writes a
/// `wireless-devices` record) with `GetWirelessDeviceImportTask` by name
/// overlap. Read the import-task store first, then fall back to the
/// wireless-device store so that cross-resource pairing resolves.
fn get_wireless_device_import_task(
    svc: &IotWirelessService,
    meta: &OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let key = storage_key(meta, labels);
    let rtype = resource_type(meta);
    let g = svc.state.read();
    let record = g.get(&ctx.account).and_then(|d| {
        d.get_resource(&rtype, &key)
            .or_else(|| d.get_resource("wireless-devices", &key))
            .cloned()
    });
    match record {
        Some(record) => Ok((ok_json(build_output(meta, &record)), false)),
        None => Err(super::engine::not_found(meta, &key)),
    }
}

// ---------- per-resource log levels (finding 1) ----------

fn put_resource_log_level(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let id = labels
        .get("ResourceIdentifier")
        .cloned()
        .unwrap_or_default();
    let mut record = Map::new();
    record.insert("ResourceIdentifier".to_string(), Value::String(id.clone()));
    if let Some(rt) = query_get(query, "resourceType") {
        record.insert("ResourceType".to_string(), Value::String(rt.to_string()));
    }
    if let Some(level) = body.get("LogLevel") {
        record.insert("LogLevel".to_string(), level.clone());
    }
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("log-levels", &id, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

fn reset_resource_log_level(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels
        .get("ResourceIdentifier")
        .cloned()
        .unwrap_or_default();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.remove_resource("log-levels", &id);
    (ok_json(Value::Object(Map::new())), true)
}

// ---------- account-scoped singleton configurations (finding 2) ----------

fn update_singleton(
    svc: &IotWirelessService,
    ctx: &Ctx,
    key: &str,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_singleton(key, Value::Object(body.clone()));
    (ok_json(Value::Object(Map::new())), true)
}

fn get_singleton(
    svc: &IotWirelessService,
    ctx: &Ctx,
    meta: &OpMeta,
    key: &str,
    default: Value,
) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let stored = g
        .get(&ctx.account)
        .and_then(|d| d.get_singleton(key).cloned());
    // Project the stored config (or an AWS-plausible default when never set)
    // onto the operation's output members.
    let source = stored.unwrap_or(default);
    (ok_json(build_output(meta, &source)), false)
}

fn metric_configuration_default() -> Value {
    json!({ "SummaryMetric": { "Status": "Disabled" } })
}

fn log_levels_default() -> Value {
    json!({
        "DefaultLogLevel": "INFO",
        "WirelessGatewayLogOptions": [],
        "WirelessDeviceLogOptions": [],
        "FuotaTaskLogOptions": [],
    })
}

// ---------- Action ops that mint / return output members (finding 3) ----------

/// A fresh UUID-shaped id seeded by the wall clock so repeated calls differ.
fn mint_transient_id(prefix: &str, seed: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    mint_uuid(&format!("{prefix}:{seed}:{nanos}"))
}

/// `SendDataToWirelessDevice` / `SendDataToMulticastGroup`: there is no live
/// radio plane, so no downlink is delivered, but AWS returns a `MessageId` for
/// the enqueued downlink. Mint and return one (transient — AWS does not expose
/// a read-back for it).
fn send_data(labels: &HashMap<String, String>) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or_default();
    let message_id = mint_transient_id("downlink", id);
    (ok_json(json!({ "MessageId": message_id })), false)
}

/// `StartWirelessDeviceImportTask` / `StartSingleWirelessDeviceImportTask`:
/// mint `Id` + `Arn`, and persist an import-task record so
/// `GetWirelessDeviceImportTask` / `DeleteWirelessDeviceImportTask` resolve.
fn start_import_task(
    svc: &IotWirelessService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let seq = data.next_seq();
    let id = mint_uuid(&format!("{}:import-task:{seq}", ctx.account));
    let arn = mint_arn(ctx, "wireless_device_import_task", &id);

    let mut record = Map::new();
    record.insert("Id".to_string(), Value::String(id.clone()));
    record.insert("Arn".to_string(), Value::String(arn.clone()));
    if let Some(dest) = body.get("DestinationName") {
        record.insert("DestinationName".to_string(), dest.clone());
    }
    if let Some(pos) = body.get("Positioning") {
        record.insert("Positioning".to_string(), pos.clone());
    }
    if let Some(sw) = body.get("Sidewalk") {
        record.insert("Sidewalk".to_string(), sw.clone());
    }
    record.insert("CreationTime".to_string(), now_epoch());
    record.insert(
        "Status".to_string(),
        Value::String("INITIALIZING".to_string()),
    );
    record.insert("InitializedImportedDeviceCount".to_string(), Value::from(0));
    record.insert("PendingImportedDeviceCount".to_string(), Value::from(0));
    record.insert("OnboardedImportedDeviceCount".to_string(), Value::from(0));
    record.insert("FailedImportedDeviceCount".to_string(), Value::from(0));

    data.put_resource("wireless_device_import_task", &id, Value::Object(record));
    (ok_json(json!({ "Id": id, "Arn": arn })), true)
}

/// `CreateWirelessGatewayTask`: persist a task record keyed by the gateway id so
/// `GetWirelessGatewayTask` / `DeleteWirelessGatewayTask` resolve, and return
/// the task-definition id + status.
fn create_wireless_gateway_task(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let gateway_id = labels.get("Id").cloned().unwrap_or_default();
    let def_id = body
        .get("WirelessGatewayTaskDefinitionId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let status = "QUEUED".to_string();

    let mut record = Map::new();
    record.insert(
        "WirelessGatewayId".to_string(),
        Value::String(gateway_id.clone()),
    );
    record.insert(
        "WirelessGatewayTaskDefinitionId".to_string(),
        Value::String(def_id.clone()),
    );
    record.insert("Status".to_string(), Value::String(status.clone()));

    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource(
        "wireless-gateways/tasks",
        &gateway_id,
        Value::Object(record),
    );
    (
        ok_json(json!({
            "WirelessGatewayTaskDefinitionId": def_id,
            "Status": status,
        })),
        true,
    )
}

fn get_wireless_gateway_task(
    svc: &IotWirelessService,
    meta: &OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let gateway_id = labels.get("Id").cloned().unwrap_or_default();
    let g = svc.state.read();
    let record = g.get(&ctx.account).and_then(|d| {
        d.get_resource("wireless-gateways/tasks", &gateway_id)
            .cloned()
    });
    match record {
        Some(record) => Ok((ok_json(build_output(meta, &record)), false)),
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No task is queued for wireless gateway '{gateway_id}'."),
        )),
    }
}

fn delete_wireless_gateway_task(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let gateway_id = labels.get("Id").cloned().unwrap_or_default();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.remove_resource("wireless-gateways/tasks", &gateway_id);
    (ok_json(Value::Object(Map::new())), true)
}

/// `TestWirelessDevice`: no radio plane, so no test uplink is generated, but AWS
/// returns a human-readable `Result` string acknowledging the request.
fn test_wireless_device() -> (AwsResponse, bool) {
    (
        ok_json(json!({ "Result": "Test message sent successfully to the wireless device." })),
        false,
    )
}

/// `GetServiceEndpoint`: AWS returns fixed-shape endpoint metadata for the
/// configuration-and-update-server (CUPS) or LoRaWAN-network-server (LNS)
/// protocol. There is no per-account state; the endpoint is derived from the
/// account/region and the trust anchor is a representative PEM chain.
fn get_service_endpoint(ctx: &Ctx, query: &[(String, String)]) -> (AwsResponse, bool) {
    let service_type = query_get(query, "serviceType").unwrap_or("CUPS");
    let host = if service_type.eq_ignore_ascii_case("LNS") {
        format!(
            "wss://{}.lns.lorawan.{}.amazonaws.com:443",
            ctx.account, ctx.region
        )
    } else {
        format!(
            "https://{}.cups.lorawan.{}.amazonaws.com:443",
            ctx.account, ctx.region
        )
    };
    let server_trust = "-----BEGIN CERTIFICATE-----\n\
        MIIBkTCB+wIJAKb1x2b3c4d5MA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBmlv\n\
        dHdscjAeFw0yMDAxMDEwMDAwMDBaFw0zMDAxMDEwMDAwMDBaMBExDzANBgNVBAMM\n\
        BmlvdHdscjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQDFakeCApemRepresentat1ve\n\
        TrustAnchorForIoTWirelessServiceEndpointEmulationOnlyNotReal00AgMB\n\
        AAEwDQYJKoZIhvcNAQELBQADQQAfakeSignatureBytesForEmulationPurposes\n\
        OnlyDoNotUseInProductionAsThisIsNotARealCertificate000000000000\n\
        -----END CERTIFICATE-----\n";
    (
        ok_json(json!({
            "ServiceType": service_type,
            "ServiceEndpoint": host,
            "ServerTrust": server_trust,
        })),
        false,
    )
}

/// `GetPositionEstimate`: the estimate is server-computed from the supplied
/// measurements. With no positioning engine, return a well-formed GeoJSON
/// `FeatureCollection` (an `@httpPayload` blob) representing a single estimated
/// point rather than an empty `{}`.
fn get_position_estimate() -> (AwsResponse, bool) {
    let geojson = json!({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {
                "horizontalAccuracy": 50.0,
                "verticalAccuracy": 30.0,
                "horizontalConfidenceLevel": 0.68,
                "country": "USA",
                "state": "WA",
                "city": "Seattle",
                "timestamp": "2020-03-05T13:31:34.842Z"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-122.3321, 47.6062, 50.0]
            }
        }]
    });
    let bytes = serde_json::to_vec(&geojson).unwrap_or_else(|_| b"{}".to_vec());
    (AwsResponse::json(StatusCode::OK, bytes), false)
}

/// `GetMetrics`: aggregated summary-metric analytics with no persisted
/// time-series behind them; return a well-formed (empty) result list.
fn get_metrics() -> (AwsResponse, bool) {
    (ok_json(json!({ "SummaryMetricQueryResults": [] })), false)
}

/// `GetWirelessDeviceStatistics`: radio-plane telemetry with no live device;
/// return a well-formed record echoing the addressed device id.
fn wireless_device_statistics(labels: &HashMap<String, String>) -> (AwsResponse, bool) {
    let id = labels
        .get("WirelessDeviceId")
        .map(String::as_str)
        .unwrap_or_default();
    (ok_json(json!({ "WirelessDeviceId": id })), false)
}

/// `GetWirelessGatewayStatistics`: as above, for a wireless gateway.
fn wireless_gateway_statistics(labels: &HashMap<String, String>) -> (AwsResponse, bool) {
    let id = labels
        .get("WirelessGatewayId")
        .map(String::as_str)
        .unwrap_or_default();
    (ok_json(json!({ "WirelessGatewayId": id })), false)
}

// ---------- association membership edges (finding 4) ----------

fn multicast_devices_key(labels: &HashMap<String, String>) -> String {
    format!(
        "multicast-devices:{}",
        labels.get("Id").map(String::as_str).unwrap_or_default()
    )
}

fn fuota_devices_key(labels: &HashMap<String, String>) -> String {
    format!(
        "fuota-devices:{}",
        labels.get("Id").map(String::as_str).unwrap_or_default()
    )
}

fn fuota_multicast_key(labels: &HashMap<String, String>) -> String {
    format!(
        "fuota-multicast:{}",
        labels.get("Id").map(String::as_str).unwrap_or_default()
    )
}

/// Persist a membership edge (the associated member id is a required body
/// member). The op has no output members.
fn associate(
    svc: &IotWirelessService,
    ctx: &Ctx,
    key: &str,
    body: &Map<String, Value>,
    member_field: &str,
) -> (AwsResponse, bool) {
    if let Some(member) = body.get(member_field).and_then(Value::as_str) {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.add_relation(key, member);
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// Remove a membership edge (the member id is a URI path label).
fn disassociate(
    svc: &IotWirelessService,
    ctx: &Ctx,
    key: &str,
    member: Option<&str>,
) -> (AwsResponse, bool) {
    if let Some(member) = member {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.remove_relation(key, member);
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// Set `ThingArn` (+ derived `ThingName`) on a stored wireless device/gateway
/// record so the matching Get reflects the association.
fn associate_thing(
    svc: &IotWirelessService,
    ctx: &Ctx,
    rtype: &str,
    id: Option<&str>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let (Some(id), Some(thing_arn)) = (id, body.get("ThingArn").and_then(Value::as_str)) else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    let thing_name = thing_arn
        .rsplit_once("thing/")
        .map(|(_, n)| n)
        .unwrap_or("");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(record) = data.get_resource(rtype, id).cloned() {
        let mut record = record;
        if let Some(obj) = record.as_object_mut() {
            obj.insert("ThingArn".to_string(), json!(thing_arn));
            obj.insert("ThingName".to_string(), json!(thing_name));
        }
        data.put_resource(rtype, id, record);
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// Clear `ThingArn`/`ThingName` on a wireless device/gateway record.
fn disassociate_thing(
    svc: &IotWirelessService,
    ctx: &Ctx,
    rtype: &str,
    id: Option<&str>,
) -> (AwsResponse, bool) {
    disassociate_thing_field(svc, ctx, rtype, id, &["ThingArn", "ThingName"])
}

/// Remove the named fields from a stored record (used by the disassociate ops).
fn disassociate_thing_field(
    svc: &IotWirelessService,
    ctx: &Ctx,
    rtype: &str,
    id: Option<&str>,
    fields: &[&str],
) -> (AwsResponse, bool) {
    if let Some(id) = id {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        if let Some(mut record) = data.get_resource(rtype, id).cloned() {
            if let Some(obj) = record.as_object_mut() {
                for f in fields {
                    obj.remove(*f);
                }
            }
            data.put_resource(rtype, id, record);
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// Store `IotCertificateId` on the wireless-gateway record.
fn associate_gateway_certificate(
    svc: &IotWirelessService,
    ctx: &Ctx,
    id: Option<&str>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let (Some(id), Some(cert)) = (id, body.get("IotCertificateId").and_then(Value::as_str)) else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(mut record) = data.get_resource("wireless-gateways", id).cloned() {
        if let Some(obj) = record.as_object_mut() {
            obj.insert("IotCertificateId".to_string(), json!(cert));
        }
        data.put_resource("wireless-gateways", id, record);
    }
    (ok_json(json!({ "IotCertificateId": cert })), true)
}

/// GetWirelessGatewayCertificate (a Verb::Action, so not served by the generic
/// engine): project the cert id stored on the gateway record.
fn get_wireless_gateway_certificate(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let g = svc.state.read();
    let cert = g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("wireless-gateways", id))
        .and_then(|r| r.get("IotCertificateId"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    (ok_json(json!({ "IotCertificateId": cert })), false)
}

/// A deterministic 64-character hex digest of an input, standing in for the
/// SHA-256 fingerprint AWS reports for a Sidewalk app-server private key. Built
/// from eight salted FNV-1a folds so the same key always maps to the same
/// digest without pulling in a crypto dependency.
fn fingerprint_hex(input: &str) -> String {
    let mut out = String::with_capacity(64);
    for salt in 0u8..8 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        hash ^= salt as u64;
        for b in input.bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        out.push_str(&format!("{hash:016x}"));
    }
    out
}

/// Persist a Sidewalk partner-account association keyed by its AmazonId so
/// GetPartnerAccount / ListPartnerAccounts (generic reads) reflect it.
fn associate_partner_account(
    svc: &IotWirelessService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let sidewalk = body.get("Sidewalk").cloned().unwrap_or(json!({}));
    let amazon_id = sidewalk
        .get("AmazonId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    // AWS returns a fingerprint of the app-server private key, never the key
    // itself. Derive a deterministic 64-hex digest so Get/List round-trip.
    let private_key = sidewalk
        .get("AppServerPrivateKey")
        .and_then(Value::as_str)
        .unwrap_or("");
    let fingerprint = fingerprint_hex(private_key);
    // The SidewalkAccountInfoWithFingerprint the reads project carries the
    // AmazonId + Fingerprint (never the private key).
    let sidewalk_with_fp = json!({
        "AmazonId": amazon_id,
        "Fingerprint": fingerprint,
    });
    // Store the projected list-element members (AmazonId/Fingerprint/Arn) at the
    // top level so the generic ListPartnerAccounts projection finds them, and
    // keep the nested Sidewalk object for GetPartnerAccount.
    let record = json!({
        "AmazonId": amazon_id,
        "Fingerprint": fingerprint,
        "Sidewalk": sidewalk_with_fp,
        "PartnerAccountId": amazon_id,
        "PartnerType": "Sidewalk",
    });
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("partner-accounts", &amazon_id, record.clone());
    (ok_json(record), true)
}

/// Remove a partner-account association.
fn disassociate_partner_account(
    svc: &IotWirelessService,
    ctx: &Ctx,
    id: Option<&str>,
) -> (AwsResponse, bool) {
    if let Some(id) = id {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.resources
            .get_mut("partner-accounts")
            .map(|m| m.remove(id));
    }
    (ok_json(Value::Object(Map::new())), true)
}

// ---------- multicast-group LoRaWAN session ----------

/// `StartMulticastGroupSession`: persist the requested LoRaWAN session so
/// `GetMulticastGroupSession` reflects it. AWS returns no output members.
fn start_multicast_session(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let lorawan = body.get("LoRaWAN").cloned().unwrap_or_else(|| json!({}));
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("multicast-sessions", id, json!({ "LoRaWAN": lorawan }));
    (ok_json(Value::Object(Map::new())), true)
}

/// `GetMulticastGroupSession`: project the stored LoRaWAN session. A group with
/// no active session returns an empty (shape-valid) body.
fn get_multicast_session(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let g = svc.state.read();
    let lorawan = g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("multicast-sessions", id))
        .and_then(|r| r.get("LoRaWAN"))
        .cloned();
    match lorawan {
        Some(l) => (ok_json(json!({ "LoRaWAN": l })), false),
        None => (ok_json(Value::Object(Map::new())), false),
    }
}

/// `CancelMulticastGroupSession`: clear the stored LoRaWAN session.
fn cancel_multicast_session(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.remove_resource("multicast-sessions", id);
    (ok_json(Value::Object(Map::new())), true)
}

/// `StartFuotaTask`: transition the addressed FUOTA task into an in-session
/// status so `GetFuotaTask` reflects it. No-op when the task does not exist.
fn start_fuota_task(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(mut record) = data.get_resource("fuota-tasks", id).cloned() {
        if let Some(obj) = record.as_object_mut() {
            obj.insert("Status".to_string(), json!("In_FuotaSession"));
        }
        data.put_resource("fuota-tasks", id, record);
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// `StartBulkAssociate/DisassociateWirelessDeviceWithMulticastGroup`: the tag
/// query is not modelled, so associate / disassociate every wireless device in
/// the account, writing to the same `multicast-devices` relation the
/// single-device variant uses.
fn bulk_multicast_membership(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    associate: bool,
) -> (AwsResponse, bool) {
    let key = multicast_devices_key(labels);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let device_ids: Vec<String> = data
        .resources
        .get("wireless-devices")
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    for device in device_ids {
        if associate {
            data.add_relation(&key, &device);
        } else {
            data.remove_relation(&key, &device);
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// `DeleteQueuedMessages`: purge the addressed device's persisted downlink queue
/// store (idempotent — there is no live radio plane enqueuing messages).
fn delete_queued_messages(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Id").map(String::as_str).unwrap_or("");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.remove_resource("wireless-devices/data", id);
    (ok_json(Value::Object(Map::new())), true)
}

/// `DeregisterWirelessDevice`: persist the deregistered state on the device
/// record rather than accepting-and-discarding the call.
fn deregister_wireless_device(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let id = labels.get("Identifier").map(String::as_str).unwrap_or("");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(mut record) = data.get_resource("wireless-devices", id).cloned() {
        if let Some(obj) = record.as_object_mut() {
            obj.insert("DeviceRegistrationState".to_string(), json!("Deregistered"));
        }
        data.put_resource("wireless-devices", id, record);
    }
    (ok_json(Value::Object(Map::new())), true)
}

/// `ResetAllResourceLogLevels`: clear every per-resource log level written by
/// `PutResourceLogLevel`.
fn reset_all_resource_log_levels(svc: &IotWirelessService, ctx: &Ctx) -> (AwsResponse, bool) {
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.resources.remove("log-levels");
    (ok_json(Value::Object(Map::new())), true)
}

/// `ListMulticastGroupsByFuotaTask`: read the FUOTA-task -> multicast-group
/// edges persisted by `AssociateMulticastGroupWithFuotaTask`.
fn list_multicast_groups_by_fuota_task(
    svc: &IotWirelessService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let key = fuota_multicast_key(labels);
    let g = svc.state.read();
    let groups: Vec<Value> = g
        .get(&ctx.account)
        .map(|d| d.list_relation(&key))
        .unwrap_or_default()
        .into_iter()
        .map(|id| json!({ "Id": id }))
        .collect();
    (ok_json(json!({ "MulticastGroupList": groups })), false)
}
