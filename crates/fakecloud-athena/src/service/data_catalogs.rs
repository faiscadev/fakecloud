//! `AthenaService` `data_catalogs` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_data_catalog(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CatalogNameString @length(min:1, max:256).
        let name = validate_required_string_len(&body, "Name", 1, 256)?;
        let cat_type = require_str(&body, "Type")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let parameters = body
            .get("Parameters")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let connection_type = body
            .get("ConnectionType")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.data_catalogs.contains_key(&name) {
            return Err(invalid_request(format!(
                "DataCatalog {name} already exists"
            )));
        }
        let cat = DataCatalog {
            name: name.clone(),
            description,
            cat_type,
            parameters,
            status: "CREATE_COMPLETE".to_string(),
            connection_type,
            error: None,
        };
        let arn = datacatalog_arn(&req.account_id, &req.region, &name);
        account.data_catalogs.insert(name, cat);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_data_catalog(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cat = account
            .data_catalogs
            .get(&name)
            .ok_or_else(|| invalid_request(format!("DataCatalog {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "DataCatalog": data_catalog_json(cat),
        })))
    }

    pub(super) fn list_data_catalogs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxDataCatalogsCount @range(2,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 2, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<DataCatalog> = account.data_catalogs.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate_checked(&all, next_token.as_deref(), max_results)
            .map_err(|_| invalid_request("Invalid NextToken"))?;
        let summaries: Vec<Value> = page
            .iter()
            .map(|c| {
                json!({
                    "CatalogName": c.name,
                    "Type": c.cat_type,
                    "Status": c.status,
                })
            })
            .collect();
        let mut response = json!({ "DataCatalogsSummary": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_data_catalog(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let cat_type = require_str(&body, "Type")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cat = account
            .data_catalogs
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("DataCatalog {name} not found")))?;
        cat.cat_type = cat_type;
        if description.is_some() {
            cat.description = description;
        }
        if let Some(p) = body.get("Parameters").and_then(Value::as_object) {
            cat.parameters = p
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_data_catalog(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CatalogNameString @length(1,256).
        let name = validate_required_string_len(&body, "Name", 1, 256)?;
        if name == "AwsDataCatalog" {
            return Err(invalid_request("Cannot delete the default AwsDataCatalog"));
        }
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.data_catalogs.remove(&name).is_none() {
            return Err(invalid_request(format!("DataCatalog {name} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "DataCatalog": {
                "Name": "",
                "Type": "",
                "Status": "DELETE_COMPLETE",
            }
        })))
    }
}
