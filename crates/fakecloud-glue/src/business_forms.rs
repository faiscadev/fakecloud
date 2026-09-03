//! Glue business-catalog form types, attachments, iterable forms, and the
//! data-catalog export configuration.
//!
//! Iterable forms are not their own resource: an asset carries a `Forms` map of
//! form name -> `{ FormTypeId, Content }`, and a form whose content is a JSON
//! array is iterable, one item per array element. `ListIterableForms` and
//! `BatchGetIterableForms` read those items back out, which is why neither the
//! model nor this module has a create-item operation.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, invalid_input, new_id, now_ts, paginate_body, req_str};
use crate::service::GlueService;
use crate::state::GlueState;

/// Key for a glossary-term association or attachment scoped to one item of an
/// iterable form. Asset-level associations key on the bare asset id, so the
/// separator keeps the two spaces from colliding.
pub(crate) fn item_key(asset_id: &str, form: &str, item: &str) -> String {
    format!("{asset_id}\u{0}{form}\u{0}{item}")
}

/// Key for an attachment. Attachments hang off either an asset or one item of
/// an iterable form, and are named uniquely within that scope.
pub(crate) fn attachment_key(
    asset_id: &str,
    form: Option<&str>,
    item: Option<&str>,
    name: &str,
) -> String {
    match (form, item) {
        (Some(f), Some(i)) => format!("{}\u{0}{name}", item_key(asset_id, f, i)),
        _ => format!("{asset_id}\u{0}{name}"),
    }
}

fn opt_str<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(Value::as_str)
}

/// The items of an asset's iterable form, in content order. An absent form, or
/// one whose content is not a JSON array, has no items.
fn iterable_items(state: &GlueState, asset_id: &str, form: &str) -> Vec<Value> {
    let Some(asset) = state.assets.get(asset_id) else {
        return Vec::new();
    };
    let Some(entry) = asset.get("Forms").and_then(|f| f.get(form)) else {
        return Vec::new();
    };
    // Content is a JSON document carried as a string by the model.
    let parsed = match entry.get("Content") {
        Some(Value::String(s)) => serde_json::from_str::<Value>(s.as_str()).ok(),
        other => other.cloned(),
    };
    parsed
        .as_ref()
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// An item's identity: `ItemId` when present, else `ItemName`. Callers address
/// items by either, so both resolve to the same element.
fn item_matches(item: &Value, identifier: &str) -> bool {
    item.get("ItemId").and_then(Value::as_str) == Some(identifier)
        || item.get("ItemName").and_then(Value::as_str) == Some(identifier)
}

fn item_id_of(item: &Value) -> Option<&str> {
    item.get("ItemId")
        .or_else(|| item.get("ItemName"))
        .and_then(Value::as_str)
}

impl GlueService {
    // ---- form types ----

    /// Put is an upsert keyed by name: re-putting a name keeps its id, which is
    /// what makes the operation safe to retry.
    pub(crate) fn put_form_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let schema = req_str(&body, "Schema")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let id = state
            .form_types
            .values()
            .find(|f| f["Name"].as_str() == Some(name.as_str()))
            .and_then(|f| f["Id"].as_str().map(str::to_string))
            .unwrap_or_else(new_id);
        let stored = json!({ "Id": id, "Name": name, "Schema": schema });
        state.form_types.insert(id, stored.clone());
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn get_form_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let f = state
            .form_types
            .get(id)
            .ok_or_else(|| entity_not_found(format!("Form type {id} not found")))?;
        Ok(AwsResponse::ok_json(f.clone()))
    }

    pub(crate) fn delete_form_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);

        // A form type still shaping an asset form, an asset type, or an
        // attachment cannot be removed: that is what ConflictException is for.
        let used_by_asset = state.assets.values().any(|a| {
            a.get("Forms")
                .and_then(Value::as_object)
                .is_some_and(|forms| {
                    forms
                        .values()
                        .any(|e| e.get("FormTypeId").and_then(Value::as_str) == Some(id.as_str()))
                })
        });
        let used_by_type = state
            .asset_types
            .values()
            .any(|t| t["FormTypeId"].as_str() == Some(id.as_str()));
        let used_by_attachment = state
            .attachments
            .values()
            .any(|a| a["FormTypeId"].as_str() == Some(id.as_str()));
        if used_by_asset || used_by_type || used_by_attachment {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::CONFLICT,
                "ConflictException",
                format!("Form type {id} is still in use"),
            ));
        }
        // DeleteFormType declares no EntityNotFoundException, so removing one
        // that is already gone succeeds.
        state.form_types.remove(&id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_form_types(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let items: Vec<Value> = state
            .form_types
            .values()
            .map(|f| json!({ "Id": f["Id"], "Name": f["Name"] }))
            .collect();
        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    // ---- attachments ----

    pub(crate) fn put_attachment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_id = req_str(&body, "AssetIdentifier")?.to_string();
        let name = req_str(&body, "AttachmentName")?.to_string();
        let content = req_str(&body, "Content")?.to_string();
        let form_type_id = req_str(&body, "FormTypeId")?.to_string();
        let form = opt_str(&body, "IterableFormName").map(str::to_string);
        let item = opt_str(&body, "ItemIdentifier").map(str::to_string);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if !state.assets.contains_key(&asset_id) {
            return Err(entity_not_found(format!("Asset {asset_id} not found")));
        }
        if !state.form_types.contains_key(&form_type_id) {
            return Err(entity_not_found(format!(
                "Form type {form_type_id} not found"
            )));
        }
        // An item-scoped attachment needs that item to exist.
        if let (Some(f), Some(i)) = (form.as_deref(), item.as_deref()) {
            if !iterable_items(state, &asset_id, f)
                .iter()
                .any(|it| item_matches(it, i))
            {
                return Err(entity_not_found(format!("Item {i} not found in form {f}")));
            }
        }

        let key = attachment_key(&asset_id, form.as_deref(), item.as_deref(), &name);
        let stored = json!({
            "AssetIdentifier": asset_id,
            "IterableFormName": form,
            "ItemIdentifier": item,
            "AttachmentName": name,
            "FormTypeId": form_type_id,
            "Content": content,
        });
        state.attachments.insert(key, stored.clone());

        let mut out = json!({
            "AssetIdentifier": stored["AssetIdentifier"],
            "AttachmentName": stored["AttachmentName"],
            "FormTypeId": stored["FormTypeId"],
        });
        for f in ["IterableFormName", "ItemIdentifier"] {
            if !stored[f].is_null() {
                out[f] = stored[f].clone();
            }
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn delete_attachment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_id = req_str(&body, "AssetIdentifier")?.to_string();
        let name = req_str(&body, "AttachmentName")?.to_string();
        let form = opt_str(&body, "IterableFormName").map(str::to_string);
        let item = opt_str(&body, "ItemIdentifier").map(str::to_string);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if !state.assets.contains_key(&asset_id) {
            return Err(entity_not_found(format!("Asset {asset_id} not found")));
        }
        // The attachment itself may already be gone; only the asset is checked,
        // matching how the other business-catalog deletes behave.
        state.attachments.remove(&attachment_key(
            &asset_id,
            form.as_deref(),
            item.as_deref(),
            &name,
        ));

        let mut out = json!({ "AssetIdentifier": asset_id });
        if let Some(f) = form {
            out["IterableFormName"] = json!(f);
        }
        if let Some(i) = item {
            out["ItemIdentifier"] = json!(i);
        }
        Ok(AwsResponse::ok_json(out))
    }

    // ---- iterable forms ----

    pub(crate) fn list_iterable_forms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_id = req_str(&body, "AssetIdentifier")?.to_string();
        let form = req_str(&body, "IterableFormName")?.to_string();

        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        if !state.assets.contains_key(&asset_id) {
            return Err(entity_not_found(format!("Asset {asset_id} not found")));
        }
        let items: Vec<Value> = iterable_items(state, &asset_id, &form)
            .iter()
            .map(|it| {
                let mut out = json!({});
                for f in ["ItemId", "ItemName", "Description"] {
                    if let Some(v) = it.get(f).filter(|v| !v.is_null()) {
                        out[f] = v.clone();
                    }
                }
                if let Some(id) = item_id_of(it) {
                    if let Some(terms) = state
                        .asset_glossary_terms
                        .get(&item_key(&asset_id, &form, id))
                    {
                        if !terms.is_empty() {
                            out["GlossaryTerms"] = json!(terms);
                        }
                    }
                }
                out
            })
            .collect();
        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    /// Batch reads report per-item failures in `Errors` rather than failing the
    /// whole call, so an unknown item id does not hide the ones that resolved.
    pub(crate) fn batch_get_iterable_forms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_id = req_str(&body, "AssetIdentifier")?.to_string();
        let form = req_str(&body, "IterableFormName")?.to_string();
        let wanted: Vec<String> = body
            .get("ItemIdentifiers")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if wanted.is_empty() {
            return Err(invalid_input("ItemIdentifiers must not be empty"));
        }

        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        if !state.assets.contains_key(&asset_id) {
            return Err(entity_not_found(format!("Asset {asset_id} not found")));
        }
        let present = iterable_items(state, &asset_id, &form);

        let mut items = Vec::new();
        let mut errors = Vec::new();
        for id in &wanted {
            let Some(found) = present.iter().find(|it| item_matches(it, id)) else {
                errors.push(json!({
                    "ItemIdentifier": id,
                    "Code": "EntityNotFoundException",
                    "Message": format!("Item {id} not found in form {form}"),
                }));
                continue;
            };
            let mut out = json!({});
            for f in ["ItemId", "ItemName"] {
                if let Some(v) = found.get(f).filter(|v| !v.is_null()) {
                    out[f] = v.clone();
                }
            }
            if let Some(forms) = found.get("Forms").filter(|v| v.is_object()) {
                out["Forms"] = forms.clone();
            }
            let item_id = item_id_of(found).unwrap_or(id.as_str());
            if let Some(terms) = state
                .asset_glossary_terms
                .get(&item_key(&asset_id, &form, item_id))
            {
                if !terms.is_empty() {
                    out["GlossaryTerms"] = json!(terms);
                }
            }
            // Attachments on this item are reported as a form map, keyed by
            // attachment name, which is how they are addressed on write.
            let prefix = format!("{}\u{0}", item_key(&asset_id, &form, item_id));
            let attached: serde_json::Map<String, Value> = state
                .attachments
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, a)| {
                    (
                        a["AttachmentName"].as_str().unwrap_or_default().to_string(),
                        json!({ "FormTypeId": a["FormTypeId"], "Content": a["Content"] }),
                    )
                })
                .collect();
            if !attached.is_empty() {
                out["Attachments"] = Value::Object(attached);
            }
            items.push(out);
        }

        let mut out = json!({ "Items": items });
        if !errors.is_empty() {
            out["Errors"] = json!(errors);
        }
        Ok(AwsResponse::ok_json(out))
    }

    // ---- data catalog export configuration ----

    pub(crate) fn get_data_catalog_export_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let cfg = state.data_catalog_export.clone().ok_or_else(|| {
            entity_not_found("No data catalog export configuration exists for this account")
        })?;
        Ok(AwsResponse::ok_json(cfg))
    }

    pub(crate) fn put_data_catalog_export_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let setting = req_str(&body, "ExportSetting")?.to_string();
        if setting != "ENABLED" && setting != "DISABLED" {
            return Err(invalid_input(
                "ExportSetting must be one of ENABLED, DISABLED",
            ));
        }
        let encryption = body
            .get("EncryptionConfiguration")
            .filter(|v| v.is_object());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let now = now_ts();
        let created = state
            .data_catalog_export
            .as_ref()
            .and_then(|c| c["CreatedAt"].as_f64())
            .unwrap_or(now);
        // The export reaches its steady state immediately; there is no
        // background transition to observe through ENABLING/DISABLING here.
        let mut cfg = json!({
            "ExportSetting": setting,
            "Status": setting,
            "CreatedAt": created,
            "UpdatedAt": now,
            "S3TableBucketArn": format!(
                "arn:aws:s3tables:{}:{}:bucket/glue-data-catalog",
                req.region, req.account_id
            ),
        });
        if let Some(e) = encryption {
            cfg["EncryptionConfiguration"] = e.clone();
        }
        state.data_catalog_export = Some(cfg.clone());

        let mut out = json!({ "ExportSetting": cfg["ExportSetting"] });
        if let Some(e) = encryption {
            out["EncryptionConfiguration"] = e.clone();
        }
        Ok(AwsResponse::ok_json(out))
    }
}

/// Whether an item exists in an asset's iterable form. Used by the glossary
/// term association path, which can scope a term to a single item.
pub(crate) fn iterable_item_exists(
    state: &GlueState,
    asset_id: &str,
    form: &str,
    item: &str,
) -> bool {
    iterable_items(state, asset_id, form)
        .iter()
        .any(|it| item_matches(it, item))
}
