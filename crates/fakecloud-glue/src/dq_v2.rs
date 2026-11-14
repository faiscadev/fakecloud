//! Data quality results V2: PublishDataQualityResultV2 stores a result
//! produced by an evaluation engine; GetDataQualityResultV2,
//! BatchGetDataQualityResultV2 and ListDataQualityResultsV2 read them back.

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, invalid_input, new_id, now_ts, req_present, req_str};
use crate::service::GlueService;

/// Request members a published result carries back on reads.
const STORED_MEMBERS: &[&str] = &[
    "Dimensions",
    "Tags",
    "ExecutionStartedOn",
    "ExecutionCompletedOn",
    "Score",
    "RuleResults",
    "AnalyzerResults",
    "Observations",
    "AggregatedMetrics",
    "Statistics",
    "RunConfiguration",
    "EvaluationContext",
    "Descriptors",
    "RulesetName",
    "DataQualitySecurityConfiguration",
    "Telemetry",
];

/// Members of `GetDataQualityResultV2Response`.
const GET_MEMBERS: &[&str] = &[
    "ResultId",
    "DataSource",
    "ProfileId",
    "Dimensions",
    "Tags",
    "ExecutionStartedOn",
    "ExecutionCompletedOn",
    "ResultPublishedOn",
    "Score",
    "RuleResults",
    "AnalyzerResults",
    "Observations",
    "EvaluationContext",
    "Descriptors",
    "AggregatedMetrics",
    "RulesetName",
];

/// Members of `DataQualityResultsV2Description` (ListDataQualityResultsV2).
const DESCRIPTION_MEMBERS: &[&str] = &[
    "ResultId",
    "DataSource",
    "ResultPublishedOn",
    "Dimensions",
    "EvaluationContext",
    "Descriptors",
];

fn project(stored: &Value, members: &[&str]) -> Value {
    let mut out = Map::new();
    for m in members {
        if let Some(v) = stored.get(*m).filter(|v| !v.is_null()) {
            out.insert((*m).to_string(), v.clone());
        }
    }
    Value::Object(out)
}

/// A `{String: String}` map member with a bounded entry count.
fn validate_string_map(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let Some(v) = body.get(field).filter(|v| !v.is_null()) else {
        return Ok(());
    };
    let obj = v
        .as_object()
        .ok_or_else(|| invalid_input(format!("{field} must be a map.")))?;
    if obj.len() < min || obj.len() > max {
        return Err(invalid_input(format!(
            "{field} must have between {min} and {max} entries."
        )));
    }
    for (k, v) in obj {
        let ok = |s: &str| (1..=255).contains(&s.chars().count());
        if !ok(k) || !v.as_str().is_some_and(ok) {
            return Err(invalid_input(format!(
                "{field} keys and values must be 1-255 characters."
            )));
        }
    }
    Ok(())
}

fn validate_list_len(body: &Value, field: &str, max: usize) -> Result<(), AwsServiceError> {
    match body.get(field) {
        Some(Value::Array(a)) if a.len() > max => Err(invalid_input(format!(
            "{field} must have at most {max} entries."
        ))),
        Some(Value::Array(_)) | Some(Value::Null) | None => Ok(()),
        Some(_) => Err(invalid_input(format!("{field} must be a list."))),
    }
}

/// The table a result describes, from the database/table/catalog names among
/// its descriptors (keys matched case-insensitively).
fn data_source_from_descriptors(descriptors: &Value) -> Option<Value> {
    let obj = descriptors.as_object()?;
    let find = |name: &str| {
        obj.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.as_str())
            .map(str::to_owned)
    };
    let db = find("DatabaseName")?;
    let table = find("TableName")?;
    let mut glue_table = json!({ "DatabaseName": db, "TableName": table });
    if let Some(cat) = find("CatalogId") {
        glue_table["CatalogId"] = json!(cat);
    }
    Some(json!({ "GlueTable": glue_table }))
}

/// Whether a result's table matches a `DataSource` filter, the filter's
/// catalog defaulting to the caller's account.
fn data_source_matches(filter: &Value, stored: &Value, account_id: &str) -> bool {
    let Some(want) = filter
        .get("GlueTable")
        .or_else(|| filter.get("DataQualityGlueTable"))
    else {
        return true;
    };
    let Some(have) = stored.get("DataSource").and_then(|d| d.get("GlueTable")) else {
        return false;
    };
    let field = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
    let catalog = |v: &Value| field(v, "CatalogId").unwrap_or_else(|| account_id.to_string());
    field(want, "DatabaseName") == field(have, "DatabaseName")
        && field(want, "TableName") == field(have, "TableName")
        && catalog(want) == catalog(have)
}

fn matches_filter(stored: &Value, filter: &Value, account_id: &str) -> bool {
    if let Some(ds) = filter.get("DataSource").filter(|v| !v.is_null()) {
        if !data_source_matches(ds, stored, account_id) {
            return false;
        }
    }
    let published = stored["ResultPublishedOn"].as_f64().unwrap_or(0.0);
    if let Some(after) = filter["PublishedAfter"].as_f64() {
        if published <= after {
            return false;
        }
    }
    if let Some(before) = filter["PublishedBefore"].as_f64() {
        if published >= before {
            return false;
        }
    }
    if let Some(ctx) = filter["EvaluationContext"].as_str() {
        if stored["EvaluationContext"].as_str() != Some(ctx) {
            return false;
        }
    }
    if let Some(dims) = filter["Dimensions"].as_object() {
        if !dims
            .iter()
            .all(|(k, v)| stored["Dimensions"].get(k) == Some(v))
        {
            return false;
        }
    }
    true
}

impl GlueService {
    pub(crate) fn publish_data_quality_result_v2(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_present(&body, "Dimensions")?;
        req_present(&body, "Score")?;
        req_present(&body, "RuleResults")?;
        req_str(&body, "EvaluationContext")?;
        validate_string_map(&body, "Dimensions", 1, 10)?;
        validate_string_map(&body, "Tags", 1, 5)?;
        validate_string_map(&body, "Descriptors", 0, 10)?;
        validate_list_len(&body, "RuleResults", 2000)?;
        validate_list_len(&body, "AnalyzerResults", 2000)?;
        validate_list_len(&body, "Observations", 50)?;
        let score = body["Score"]
            .as_f64()
            .ok_or_else(|| invalid_input("Score must be a number."))?;
        if !(0.0..=1.0).contains(&score) {
            return Err(invalid_input("Score must be between 0.0 and 1.0."));
        }
        if let Some(t) = body["ClientToken"].as_str() {
            if !t
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return Err(invalid_input(
                    "ClientToken may only contain letters, digits, '_' and '-'.",
                ));
            }
        }

        let mut stored = project(&body, STORED_MEMBERS);
        if let Some(ds) = body
            .get("Descriptors")
            .and_then(data_source_from_descriptors)
        {
            stored["DataSource"] = ds;
        }
        if let Some(profile) = body["Statistics"].as_array().and_then(|stats| {
            stats
                .iter()
                .find_map(|s| s.get("ProfileId").and_then(Value::as_str))
        }) {
            stored["ProfileId"] = json!(profile);
        }

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id, &req.region);
        let token = body["ClientToken"].as_str().map(str::to_owned);
        if let Some(t) = &token {
            if let Some(prior) = st.dq_results_v2_tokens.get(t) {
                // A retry with the same token returns the result it created;
                // the same token with a different request is a mismatch.
                if prior["Request"] == stored {
                    return Ok(AwsResponse::ok_json(
                        json!({ "ResultId": prior["ResultId"] }),
                    ));
                }
                return Err(fakecloud_core::service::AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "IdempotentParameterMismatchException",
                    format!("ClientToken {t} was already used with different parameters."),
                ));
            }
        }
        let result_id = format!("dqresult-{}", new_id());
        if let Some(t) = token {
            st.dq_results_v2_tokens.insert(
                t,
                json!({ "ResultId": result_id, "Request": stored.clone() }),
            );
        }
        stored["ResultId"] = json!(result_id);
        stored["ResultPublishedOn"] = json!(now_ts());
        st.dq_results_v2.insert(result_id.clone(), stored);
        Ok(AwsResponse::ok_json(json!({ "ResultId": result_id })))
    }

    pub(crate) fn get_data_quality_result_v2(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "ResultId")?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|s| s.dq_results_v2.get(id))
            .ok_or_else(|| entity_not_found(format!("Data quality result {id} not found")))?;
        Ok(AwsResponse::ok_json(project(r, GET_MEMBERS)))
    }

    pub(crate) fn batch_get_data_quality_result_v2(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids = req_present(&body, "ResultIds")?
            .as_array()
            .cloned()
            .ok_or_else(|| invalid_input("ResultIds must be a list."))?;
        if ids.is_empty() || ids.len() > 100 {
            return Err(invalid_input(
                "ResultIds must have between 1 and 100 entries.",
            ));
        }
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.dq_results_v2);
        let mut results = Vec::new();
        let mut not_found = Vec::new();
        for i in &ids {
            let Some(id) = i.as_str() else { continue };
            match store.and_then(|m| m.get(id)) {
                // DataQualityResultV2Item is the Get response without ProfileId.
                Some(v) => {
                    let mut item = project(v, GET_MEMBERS);
                    if let Some(o) = item.as_object_mut() {
                        o.remove("ProfileId");
                    }
                    results.push(item);
                }
                None => not_found.push(json!(id)),
            }
        }
        let mut out = json!({ "Results": results });
        if !not_found.is_empty() {
            out["ResultsNotFound"] = json!(not_found);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn list_data_quality_results_v2(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filter = body.get("Filter").cloned().unwrap_or(Value::Null);
        let accounts = self.state.read();
        let mut matched: Vec<&Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.dq_results_v2
                    .values()
                    .filter(|r| filter.is_null() || matches_filter(r, &filter, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        // Most recently published first.
        matched.sort_by(|a, b| {
            let t = |v: &Value| v["ResultPublishedOn"].as_f64().unwrap_or(0.0);
            t(b).total_cmp(&t(a))
                .then_with(|| b["ResultId"].as_str().cmp(&a["ResultId"].as_str()))
        });
        let items: Vec<Value> = matched
            .into_iter()
            .map(|r| project(r, DESCRIPTION_MEMBERS))
            .collect();
        let (page, token) = crate::common::paginate_body(&req.action, &body, items)?;
        let mut resp = json!({ "Results": page });
        if let Some(t) = token {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }
}
