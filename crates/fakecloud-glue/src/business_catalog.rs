//! Glue's business catalog: glossaries and their terms, asset types, and the
//! assets those types describe.
//!
//! The families are linked, and the handlers keep the links real: a term
//! belongs to a glossary and is listed by it, an asset names an asset type
//! that has to exist, and glossary terms associate to assets so `GetAsset`
//! reports them. Deleting a parent takes its children with it rather than
//! leaving them pointing at something gone.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, invalid_input, new_id, now_ts, paginate_body, req_str};
use crate::service::GlueService;

fn opt_str(body: &Value, field: &str) -> Option<String> {
    body.get(field).and_then(Value::as_str).map(str::to_string)
}

fn str_list(body: &Value, field: &str) -> Vec<String> {
    body.get(field)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

impl GlueService {
    // ---- glossaries ----

    pub(crate) fn create_glossary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let description = opt_str(&body, "Description");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // Glossary names are unique within an account, so a repeat create is
        // a conflict rather than a second glossary with the same name.
        if state
            .glossaries
            .values()
            .any(|g| g["Name"].as_str() == Some(name.as_str()))
        {
            return Err(crate::common::already_exists(format!(
                "Glossary {name} already exists"
            )));
        }
        let id = new_id();
        let mut stored = json!({ "Id": id, "Name": name });
        if let Some(d) = &description {
            stored["Description"] = json!(d);
        }
        state.glossaries.insert(id.clone(), stored.clone());
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn get_glossary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let g = accounts
            .get(&req.account_id)
            .and_then(|s| s.glossaries.get(id))
            .ok_or_else(|| entity_not_found(format!("Glossary {id} not found")))?;
        Ok(AwsResponse::ok_json(g.clone()))
    }

    pub(crate) fn update_glossary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in ["Name", "Description"] {
            if let Some(v) = body.get(f).filter(|v| !v.is_null()) {
                updates.push((f, v.clone()));
            }
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        crate::generic::update_merge(&mut state.glossaries, &id, "Glossary", updates)?;
        let stored = state
            .glossaries
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({}));
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn delete_glossary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // These deletes declare no EntityNotFoundException in the Smithy
        // model, so removing an absent glossary is a no-op rather than an error.
        state.glossaries.remove(&id);
        // Terms belong to the glossary; they go with it, and any asset
        // association to those terms goes too.
        let removed: Vec<String> = state
            .glossary_terms
            .iter()
            .filter(|(_, t)| t["GlossaryId"].as_str() == Some(id.as_str()))
            .map(|(k, _)| k.clone())
            .collect();
        for term_id in &removed {
            state.glossary_terms.remove(term_id);
        }
        for terms in state.asset_glossary_terms.values_mut() {
            terms.retain(|t| !removed.contains(t));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_glossaries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.glossaries.values().cloned().collect())
            .unwrap_or_default();
        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    // ---- glossary terms ----

    pub(crate) fn create_glossary_term(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let glossary_id = req_str(&body, "GlossaryIdentifier")?.to_string();
        let name = req_str(&body, "Name")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // A term cannot exist outside a glossary.
        if !state.glossaries.contains_key(&glossary_id) {
            return Err(entity_not_found(format!(
                "Glossary {glossary_id} not found"
            )));
        }
        let id = new_id();
        let mut stored = json!({ "Id": id, "GlossaryId": glossary_id, "Name": name });
        for f in ["ShortDescription", "LongDescription"] {
            if let Some(v) = body.get(f).filter(|v| !v.is_null()) {
                stored[f] = v.clone();
            }
        }
        state.glossary_terms.insert(id.clone(), stored.clone());
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn get_glossary_term(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let t = accounts
            .get(&req.account_id)
            .and_then(|s| s.glossary_terms.get(id))
            .ok_or_else(|| entity_not_found(format!("Glossary term {id} not found")))?;
        Ok(AwsResponse::ok_json(t.clone()))
    }

    pub(crate) fn update_glossary_term(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in ["Name", "ShortDescription", "LongDescription"] {
            if let Some(v) = body.get(f).filter(|v| !v.is_null()) {
                updates.push((f, v.clone()));
            }
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        crate::generic::update_merge(&mut state.glossary_terms, &id, "Glossary term", updates)?;
        let stored = state
            .glossary_terms
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({}));
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn delete_glossary_term(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // These deletes declare no EntityNotFoundException in the Smithy
        // model, so removing an absent term is a no-op rather than an error.
        state.glossary_terms.remove(&id);
        // A deleted term must stop showing up on the assets it was attached to.
        for terms in state.asset_glossary_terms.values_mut() {
            terms.retain(|t| t != &id);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_glossary_terms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let glossary_id = req_str(&body, "GlossaryIdentifier")?.to_string();
        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // ListGlossaryTerms declares no EntityNotFoundException, so an
        // unknown glossary yields an empty page instead of an error.
        let items: Vec<Value> = state
            .glossary_terms
            .values()
            .filter(|t| t["GlossaryId"].as_str() == Some(glossary_id.as_str()))
            .cloned()
            .collect();
        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    /// Shared body for Associate/Disassociate: both take an asset and a list
    /// of term ids and report the asset's resulting term list.
    fn change_glossary_terms(
        &self,
        req: &AwsRequest,
        associate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_id = req_str(&body, "AssetIdentifier")?.to_string();
        let term_ids = str_list(&body, "GlossaryTermIdentifiers");
        if term_ids.is_empty() {
            return Err(invalid_input("GlossaryTermIdentifiers must not be empty"));
        }

        // Terms attach to the asset itself, or to one item of an iterable form
        // on it when the request names both.
        let form = body
            .get("IterableFormName")
            .and_then(Value::as_str)
            .map(str::to_string);
        let item = body
            .get("ItemIdentifier")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if !state.assets.contains_key(&asset_id) {
            return Err(entity_not_found(format!("Asset {asset_id} not found")));
        }
        let scope = match (form.as_deref(), item.as_deref()) {
            (Some(f), Some(i)) => {
                if !crate::business_forms::iterable_item_exists(state, &asset_id, f, i) {
                    return Err(entity_not_found(format!("Item {i} not found in form {f}")));
                }
                crate::business_forms::item_key(&asset_id, f, i)
            }
            _ => asset_id.clone(),
        };
        // Associating a term that does not exist would leave the asset
        // pointing at nothing, so every id is checked first.
        if associate {
            for t in &term_ids {
                if !state.glossary_terms.contains_key(t) {
                    return Err(entity_not_found(format!("Glossary term {t} not found")));
                }
            }
        }
        let entry = state.asset_glossary_terms.entry(scope).or_default();
        for t in &term_ids {
            if associate {
                if !entry.contains(t) {
                    entry.push(t.clone());
                }
            } else {
                entry.retain(|e| e != t);
            }
        }
        let current = entry.clone();
        let mut out = json!({
            "AssetIdentifier": asset_id,
            "GlossaryTerms": current,
        });
        if let Some(f) = form {
            out["IterableFormName"] = json!(f);
        }
        if let Some(i) = item {
            out["ItemIdentifier"] = json!(i);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn associate_glossary_terms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.change_glossary_terms(req, true)
    }

    pub(crate) fn disassociate_glossary_terms(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.change_glossary_terms(req, false)
    }

    // ---- asset types ----

    pub(crate) fn put_asset_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let forms = body
            .get("Forms")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| invalid_input("Forms is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // Put is upsert-by-name: a second Put with the same name replaces the
        // type's forms rather than minting a second id.
        let id = state
            .asset_types
            .iter()
            .find(|(_, t)| t["Name"].as_str() == Some(name.as_str()))
            .map(|(k, _)| k.clone())
            .unwrap_or_else(new_id);
        let stored = json!({ "Id": id, "Name": name, "Forms": forms });
        state.asset_types.insert(id.clone(), stored.clone());
        Ok(AwsResponse::ok_json(stored))
    }

    pub(crate) fn get_asset_type(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let t = accounts
            .get(&req.account_id)
            .and_then(|s| s.asset_types.get(id))
            .ok_or_else(|| entity_not_found(format!("Asset type {id} not found")))?;
        Ok(AwsResponse::ok_json(t.clone()))
    }

    pub(crate) fn delete_asset_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // Assets name their type, so a type still in use cannot be removed.
        if state
            .assets
            .values()
            .any(|a| a["AssetTypeId"].as_str() == Some(id.as_str()))
        {
            return Err(invalid_input(format!(
                "Asset type {id} is in use by one or more assets"
            )));
        }
        // These deletes declare no EntityNotFoundException in the Smithy
        // model, so removing an absent asset type is a no-op rather than an error.
        state.asset_types.remove(&id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn list_asset_types(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.asset_types
                    .values()
                    .map(|t| json!({ "Id": t["Id"], "Name": t["Name"] }))
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    // ---- assets ----

    pub(crate) fn put_asset(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let asset_type_id = req_str(&body, "AssetTypeId")?.to_string();
        let identifier = req_str(&body, "Identifier")?.to_string();
        let name = req_str(&body, "Name")?.to_string();
        let forms = body
            .get("Forms")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| invalid_input("Forms is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // An asset's type has to exist — its forms are what give the asset
        // its shape.
        if !state.asset_types.contains_key(&asset_type_id) {
            return Err(entity_not_found(format!(
                "Asset type {asset_type_id} not found"
            )));
        }
        let now = now_ts();
        // Put is an upsert: an existing asset keeps its creation time.
        let created_at = state
            .assets
            .get(&identifier)
            .and_then(|a| a["CreatedAt"].as_f64())
            .unwrap_or(now);
        let mut stored = json!({
            "Id": identifier,
            "Name": name,
            "AssetTypeId": asset_type_id,
            "Forms": forms,
            "CreatedAt": created_at,
            "UpdatedAt": now,
        });
        if let Some(d) = body.get("Description").filter(|v| !v.is_null()) {
            stored["Description"] = d.clone();
        }
        state.assets.insert(identifier.clone(), stored.clone());

        let mut out = json!({
            "Id": identifier,
            "Name": stored["Name"],
            "CreatedAt": created_at,
            "Forms": stored["Forms"],
        });
        if let Some(d) = stored.get("Description") {
            out["Description"] = d.clone();
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn get_asset(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?;
        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let a = state
            .assets
            .get(id)
            .ok_or_else(|| entity_not_found(format!("Asset {id} not found")))?;
        let mut out = a.clone();
        // The terms associated to this asset are reported here, which is what
        // makes AssociateGlossaryTerms observable.
        if let Some(terms) = state.asset_glossary_terms.get(id) {
            if !terms.is_empty() {
                out["GlossaryTerms"] = json!(terms);
            }
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn delete_asset(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "Identifier")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // These deletes declare no EntityNotFoundException in the Smithy
        // model, so removing an absent asset is a no-op rather than an error.
        state.assets.remove(&id);
        // Item-scoped associations and attachments are keyed by a composite
        // that starts with the asset id, so they go with the asset.
        let prefix = format!("{id}\u{0}");
        state.asset_glossary_terms.remove(&id);
        state
            .asset_glossary_terms
            .retain(|k, _| !k.starts_with(&prefix));
        state.attachments.retain(|k, _| !k.starts_with(&prefix));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn search_assets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let search_text = opt_str(&body, "SearchText")
            .unwrap_or_default()
            .to_lowercase();
        if let Some(sort) = body.get("Sort") {
            if let Some(order) = sort.get("Order").and_then(Value::as_str) {
                if !matches!(order, "ASCENDING" | "DESCENDING") {
                    return Err(invalid_input(format!(
                        "Sort.Order has an invalid value '{order}'"
                    )));
                }
            }
        }

        let accounts = self.state.read();
        let empty = Default::default();
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut items: Vec<Value> = state
            .assets
            .values()
            // A real substring match over the asset's name and description —
            // an empty SearchText matches everything, as AWS does.
            .filter(|a| {
                if search_text.is_empty() {
                    return true;
                }
                let name = a["Name"].as_str().unwrap_or_default().to_lowercase();
                let desc = a["Description"].as_str().unwrap_or_default().to_lowercase();
                name.contains(&search_text) || desc.contains(&search_text)
            })
            .map(|a| {
                let mut item = json!({
                    "Id": a["Id"],
                    "AssetName": a["Name"],
                    "AssetTypeId": a["AssetTypeId"],
                });
                if let Some(d) = a.get("Description") {
                    item["AssetDescription"] = d.clone();
                }
                if let Some(u) = a.get("UpdatedAt") {
                    item["UpdatedAt"] = u.clone();
                }
                item
            })
            .collect();

        // Sorting is by asset name; DESCENDING reverses it.
        let descending = body
            .get("Sort")
            .and_then(|s| s.get("Order"))
            .and_then(Value::as_str)
            == Some("DESCENDING");
        items.sort_by(|a, b| {
            let (x, y) = (
                a["AssetName"].as_str().unwrap_or_default(),
                b["AssetName"].as_str().unwrap_or_default(),
            );
            if descending {
                y.cmp(x)
            } else {
                x.cmp(y)
            }
        });

        let (page, token) = paginate_body(&req.action, &body, items)?;
        let mut out = json!({ "Items": page });
        if let Some(t) = token {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }
}
