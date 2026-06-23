//! `LambdaService` `aliases` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Aliases ──

    pub(super) fn alias_key(function: &str, alias: &str) -> String {
        format!("{function}:{alias}")
    }

    /// Render an alias in the AWS wire shape (PascalCase keys). The stored
    /// `FunctionAlias` uses snake_case Rust field names for persistence, so it
    /// must not be serialized directly into a response — the AWS SDK would not
    /// find `AliasArn`/`Name`/etc and would parse an empty object.
    fn alias_json(a: &FunctionAlias) -> Value {
        let mut o = json!({
            "AliasArn": a.alias_arn,
            "Name": a.name,
            "FunctionVersion": a.function_version,
            "Description": a.description,
            "RevisionId": a.revision_id,
        });
        if let Some(ref rc) = a.routing_config {
            o["RoutingConfig"] = rc.clone();
        }
        o
    }

    pub(super) fn create_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        // Smithy Alias.name @length(1, 128). Reject early so we don't
        // mint aliases that GetAlias / UpdateAlias / DeleteAlias would
        // refuse later.
        if name.is_empty() || name.chars().count() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Alias name must be 1..128 chars",
            ));
        }
        let version = body["FunctionVersion"]
            .as_str()
            .unwrap_or("$LATEST")
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        let alias_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}:{}",
            state.region, state.account_id, function_name, name
        );
        let alias = FunctionAlias {
            alias_arn: alias_arn.clone(),
            name: name.clone(),
            function_version: version,
            description: body["Description"].as_str().unwrap_or("").to_string(),
            revision_id: id_from_time("rev-"),
            routing_config: body
                .get("RoutingConfig")
                .filter(|rc| {
                    rc.get("AdditionalVersionWeights")
                        .and_then(|w| w.as_object())
                        .is_some_and(|w| !w.is_empty())
                })
                .cloned(),
        };
        state
            .aliases
            .insert(Self::alias_key(function_name, &name), alias.clone());
        ok(Self::alias_json(&alias))
    }

    pub(super) fn get_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        if alias_name.is_empty() {
            return Err(missing("Name"));
        }
        if alias_name.chars().count() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Alias name exceeds the 128-character maximum",
            ));
        }
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .aliases
                .get(&Self::alias_key(function_name, &alias_name))
                .map(|a| ok(Self::alias_json(a)))
                .unwrap_or_else(|| Err(not_found("Alias", &alias_name)))
        })
    }

    pub(super) fn list_aliases(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let marker = req.query_params.get("Marker").map(String::as_str);
        let max_items = crate::service::marker_page_size(req);
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let prefix = format!("{function_name}:");
            let aliases: Vec<FunctionAlias> = state
                .aliases
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.clone())
                .collect();
            let (page, next_marker) =
                crate::service::paginate_marker(aliases, marker, max_items, |a| a.name.clone());
            let page_json: Vec<Value> = page.iter().map(Self::alias_json).collect();
            ok(json!({"Aliases": page_json, "NextMarker": next_marker}))
        })
    }

    pub(super) fn update_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = Self::alias_key(function_name, &alias_name);
        let alias = state
            .aliases
            .get_mut(&key)
            .ok_or_else(|| not_found("Alias", &alias_name))?;
        if let Some(v) = body["FunctionVersion"].as_str() {
            alias.function_version = v.to_string();
        }
        if let Some(d) = body["Description"].as_str() {
            alias.description = d.to_string();
        }
        // UpdateAlias replaces the routing config wholesale: an absent or empty
        // `RoutingConfig.AdditionalVersionWeights` clears it (Terraform removes
        // the block by sending an empty config). Keeping the old weights would
        // leave the alias's routing config "still present" after removal.
        alias.routing_config = body
            .get("RoutingConfig")
            .filter(|rc| {
                rc.get("AdditionalVersionWeights")
                    .and_then(|w| w.as_object())
                    .is_some_and(|w| !w.is_empty())
            })
            .cloned();
        alias.revision_id = id_from_time("rev-");
        ok(Self::alias_json(alias))
    }

    pub(super) fn delete_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        if alias_name.is_empty() {
            return Err(missing("Name"));
        }
        // Smithy `Alias.length 1..128`.
        if alias_name.chars().count() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Alias name exceeds the 128-character maximum",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // `DeleteAlias` is idempotent on AWS — no `ResourceNotFoundException`
        // is declared on the operation. Removing without error matches
        // the live API.
        state
            .aliases
            .remove(&Self::alias_key(function_name, &alias_name));
        empty()
    }
}
