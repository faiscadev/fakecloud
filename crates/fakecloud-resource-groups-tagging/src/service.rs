//! Resource Groups Tagging API (`tagging`) awsJson1.1 dispatch + handlers.
//!
//! The tagging API reports and mutates tags across every service in an
//! account/region. Reads aggregate every registered [`TagProvider`] (each
//! service's live tag state) plus this service's own store of tags applied to
//! arbitrary ARNs through `TagResources`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::tag_index::{TagProviderRegistry, TaggedResource};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::SharedResourceGroupsTaggingState;

/// Every operation name in the Resource Groups Tagging API Smithy model.
pub const RESOURCE_GROUPS_TAGGING_ACTIONS: &[&str] = &[
    "DescribeReportCreation",
    "GetComplianceSummary",
    "GetResources",
    "GetTagKeys",
    "GetTagValues",
    "ListRequiredTags",
    "StartReportCreation",
    "TagResources",
    "UntagResources",
];

/// `GetResources` ResourceARNList / ResourcesPerPage cap (Smithy length max).
const MAX_RESOURCES_PER_PAGE: i64 = 100;
/// `TagResources` / `UntagResources` ResourceARNList cap (Smithy length max).
const MAX_TAG_UNTAG_ARNS: usize = 20;
/// `TagResources` Tags / `UntagResources` TagKeys cap (Smithy length max).
const MAX_TAGS: usize = 50;

pub struct ResourceGroupsTaggingService {
    state: SharedResourceGroupsTaggingState,
    registry: TagProviderRegistry,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ResourceGroupsTaggingService {
    pub fn new(
        state: SharedResourceGroupsTaggingState,
        registry: TagProviderRegistry,
        snapshot_store: Option<Arc<dyn SnapshotStore>>,
    ) -> Self {
        Self {
            state,
            registry,
            snapshot_store,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// All resources visible to the tagging API for this account/region:
    /// every registered provider's resources, with API-applied tags overlaid,
    /// plus API-only ARNs (tagged through `TagResources` but owned by no
    /// modelled service).
    fn visible_resources(&self, req: &AwsRequest) -> Vec<TaggedResource> {
        let mut by_arn: BTreeMap<String, TaggedResource> = self
            .registry
            .resources(&req.account_id, Some(&req.region))
            .into_iter()
            .map(|r| (r.arn.clone(), r))
            .collect();

        let accounts = self.state.read();
        if let Some(st) = accounts.get(&req.account_id) {
            for (arn, tags) in &st.api_tags {
                // API tags that belong to a region other than the request
                // region (and are not global) are not visible here.
                let region = arn.split(':').nth(3).unwrap_or("");
                if !region.is_empty() && region != req.region {
                    continue;
                }
                let entry = by_arn.entry(arn.clone()).or_insert_with(|| {
                    TaggedResource::new(arn.clone(), resource_type_from_arn(arn), BTreeMap::new())
                });
                for (k, v) in tags {
                    entry.tags.insert(k.clone(), v.clone());
                }
            }
        }
        by_arn.into_values().collect()
    }

    fn get_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_token(&body, "PaginationToken")?;
        let per_page = match body.get("ResourcesPerPage").and_then(Value::as_i64) {
            Some(n) if !(1..=MAX_RESOURCES_PER_PAGE).contains(&n) => {
                return Err(invalid_param(&format!(
                    "ResourcesPerPage must be between 1 and {MAX_RESOURCES_PER_PAGE}."
                )));
            }
            Some(n) => n as usize,
            None => MAX_RESOURCES_PER_PAGE as usize,
        };
        let tags_per_page = match body.get("TagsPerPage").and_then(Value::as_i64) {
            Some(n) if n < 1 => {
                return Err(invalid_param("TagsPerPage must be at least 1."));
            }
            Some(n) => Some(n as usize),
            None => None,
        };

        let arn_list = string_list(body.get("ResourceARNList"));
        // AWS: ResourceARNList is mutually exclusive with the filter and
        // pagination parameters.
        if !arn_list.is_empty()
            && (body.get("TagFilters").is_some()
                || body.get("ResourceTypeFilters").is_some()
                || body.get("ResourcesPerPage").is_some()
                || body.get("TagsPerPage").is_some()
                || body
                    .get("PaginationToken")
                    .and_then(Value::as_str)
                    .is_some_and(|t| !t.is_empty()))
        {
            return Err(invalid_param(
                "ResourceARNList cannot be used together with TagFilters, \
                 ResourceTypeFilters, ResourcesPerPage, TagsPerPage, or PaginationToken.",
            ));
        }
        if arn_list.len() > MAX_RESOURCES_PER_PAGE as usize {
            return Err(invalid_param(&format!(
                "ResourceARNList can contain at most {MAX_RESOURCES_PER_PAGE} ARNs."
            )));
        }
        let arn_filter: BTreeSet<String> = arn_list.into_iter().collect();
        let type_filters = string_list(body.get("ResourceTypeFilters"));
        let tag_filters = parse_tag_filters(body.get("TagFilters"))?;

        let mut resources: Vec<TaggedResource> = self
            .visible_resources(req)
            .into_iter()
            .filter(|r| arn_filter.is_empty() || arn_filter.contains(&r.arn))
            .filter(|r| type_filters.is_empty() || type_matches(&r.resource_type, &type_filters))
            .filter(|r| tag_filters.iter().all(|f| f.matches(&r.tags)))
            .collect();
        resources.sort_by(|a, b| a.arn.cmp(&b.arn));

        let start = decode_token(body.get("PaginationToken"));
        let Some(start) = start else {
            // Unparseable token -> terminal empty page, never a 4xx.
            return Ok(json_ok(json!({
                "ResourceTagMappingList": [],
                "PaginationToken": "",
            })));
        };
        let total = resources.len();
        // Page ends when either the resource count OR (if TagsPerPage is set)
        // the accumulated tag count is reached — whichever comes first — always
        // emitting at least one resource so progress is guaranteed.
        let mut end = start;
        let mut tags_acc = 0usize;
        while end < total {
            let next_tags = resources[end].tags.len();
            let hit_resource_cap = end - start >= per_page;
            let hit_tag_cap =
                tags_per_page.is_some_and(|cap| end > start && tags_acc + next_tags > cap);
            if hit_resource_cap || hit_tag_cap {
                break;
            }
            tags_acc += next_tags;
            end += 1;
        }
        let page = if start < total {
            &resources[start..end]
        } else {
            &[]
        };
        let next = if end < total {
            end.to_string()
        } else {
            String::new()
        };

        let list: Vec<Value> = page
            .iter()
            .map(|r| {
                json!({
                    "ResourceARN": r.arn,
                    "Tags": tags_to_pairs(&r.tags),
                })
            })
            .collect();
        Ok(json_ok(json!({
            "ResourceTagMappingList": list,
            "PaginationToken": next,
        })))
    }

    fn get_tag_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_token(&body, "PaginationToken")?;
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for r in self.visible_resources(req) {
            keys.extend(r.tags.keys().cloned());
        }
        let all: Vec<String> = keys.into_iter().collect();
        let (page, next) = paginate_strings(all, decode_token(body.get("PaginationToken")));
        Ok(json_ok(json!({ "PaginationToken": next, "TagKeys": page })))
    }

    fn get_tag_values(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_token(&body, "PaginationToken")?;
        let key = require_str(&body, "Key")?;
        if key.is_empty() || key.len() > 128 {
            return Err(invalid_param("Key must be between 1 and 128 characters."));
        }
        let mut values: BTreeSet<String> = BTreeSet::new();
        for r in self.visible_resources(req) {
            if let Some(v) = r.tags.get(key) {
                values.insert(v.clone());
            }
        }
        let all: Vec<String> = values.into_iter().collect();
        let (page, next) = paginate_strings(all, decode_token(body.get("PaginationToken")));
        Ok(json_ok(
            json!({ "PaginationToken": next, "TagValues": page }),
        ))
    }

    fn tag_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let arns = string_list(body.get("ResourceARNList"));
        if arns.is_empty() {
            return Err(invalid_param(
                "ResourceARNList must contain at least one ARN.",
            ));
        }
        if arns.len() > MAX_TAG_UNTAG_ARNS {
            return Err(invalid_param(&format!(
                "ResourceARNList can contain at most {MAX_TAG_UNTAG_ARNS} ARNs."
            )));
        }
        let tags = parse_tag_map(body.get("Tags"))?;
        if tags.is_empty() {
            return Err(invalid_param("Tags must contain at least one tag."));
        }
        if tags.len() > MAX_TAGS {
            return Err(invalid_param(&format!(
                "Tags can contain at most {MAX_TAGS} entries."
            )));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for arn in &arns {
            let entry = st.api_tags.entry(arn.clone()).or_default();
            for (k, v) in &tags {
                entry.insert(k.clone(), v.clone());
            }
        }
        Ok(json_ok(json!({ "FailedResourcesMap": {} })))
    }

    fn untag_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let arns = string_list(body.get("ResourceARNList"));
        if arns.is_empty() {
            return Err(invalid_param(
                "ResourceARNList must contain at least one ARN.",
            ));
        }
        if arns.len() > MAX_TAG_UNTAG_ARNS {
            return Err(invalid_param(&format!(
                "ResourceARNList can contain at most {MAX_TAG_UNTAG_ARNS} ARNs."
            )));
        }
        let keys = string_list(body.get("TagKeys"));
        if keys.is_empty() {
            return Err(invalid_param("TagKeys must contain at least one key."));
        }
        if keys.len() > MAX_TAGS {
            return Err(invalid_param(&format!(
                "TagKeys can contain at most {MAX_TAGS} entries."
            )));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for arn in &arns {
            if let Some(entry) = st.api_tags.get_mut(arn) {
                for k in &keys {
                    entry.remove(k);
                }
                if entry.is_empty() {
                    st.api_tags.remove(arn);
                }
            }
        }
        Ok(json_ok(json!({ "FailedResourcesMap": {} })))
    }

    /// Compliance is evaluated against Organizations tag policies. With no tag
    /// policy in effect, no resource is non-compliant, so the summary is empty.
    fn get_compliance_summary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body, 1, 1000)?;
        validate_token(&body, "PaginationToken")?;
        Ok(json_ok(json!({ "SummaryList": [], "PaginationToken": "" })))
    }

    fn start_report_creation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        let bucket = require_str(&body, "S3Bucket")?.to_string();
        if bucket.len() < 3 || bucket.len() > 63 {
            return Err(invalid_param(
                "S3Bucket must be between 3 and 63 characters.",
            ));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.report.status = Some("SUCCEEDED".to_string());
        st.report.s3_location = Some(format!("s3://{bucket}/"));
        st.report.error_message = None;
        Ok(json_ok(json!({})))
    }

    fn describe_report_creation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        parse_json(&req.body)?;
        let accounts = self.state.read();
        let report = accounts.get(&req.account_id).map(|s| s.report.clone());
        let mut out = serde_json::Map::new();
        if let Some(r) = report {
            if let Some(s) = r.status {
                out.insert("Status".into(), json!(s));
            }
            if let Some(loc) = r.s3_location {
                out.insert("S3Location".into(), json!(loc));
            }
            if let Some(err) = r.error_message {
                out.insert("ErrorMessage".into(), json!(err));
            }
        }
        Ok(json_ok(Value::Object(out)))
    }

    /// Required tags come from Organizations tag policies. Without one, none
    /// are required.
    fn list_required_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_json(&req.body)?;
        validate_max_results(&body, 1, 200)?;
        validate_token(&body, "NextToken")?;
        Ok(json_ok(json!({ "RequiredTags": [] })))
    }
}

#[async_trait]
impl AwsService for ResourceGroupsTaggingService {
    fn service_name(&self) -> &str {
        "tagging"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = matches!(
            request.action.as_str(),
            "TagResources" | "UntagResources" | "StartReportCreation"
        );
        let result = match request.action.as_str() {
            "GetResources" => self.get_resources(&request),
            "GetTagKeys" => self.get_tag_keys(&request),
            "GetTagValues" => self.get_tag_values(&request),
            "TagResources" => self.tag_resources(&request),
            "UntagResources" => self.untag_resources(&request),
            "GetComplianceSummary" => self.get_compliance_summary(&request),
            "StartReportCreation" => self.start_report_creation(&request),
            "DescribeReportCreation" => self.describe_report_creation(&request),
            "ListRequiredTags" => self.list_required_tags(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        RESOURCE_GROUPS_TAGGING_ACTIONS
    }
}

// ----- helpers -----

fn json_ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

fn invalid_param(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

/// Pagination tokens (`PaginationToken` / `NextToken`) have a max length of
/// 2048 across every op that accepts one.
fn validate_token(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    if let Some(t) = body.get(field).and_then(Value::as_str) {
        if t.len() > 2048 {
            return Err(invalid_param(&format!(
                "{field} must be at most 2048 characters."
            )));
        }
    }
    Ok(())
}

/// Validate a `MaxResults` against an inclusive `[min, max]` range.
fn validate_max_results(body: &Value, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if let Some(n) = body.get("MaxResults").and_then(Value::as_i64) {
        if !(min..=max).contains(&n) {
            return Err(invalid_param(&format!(
                "MaxResults must be between {min} and {max}."
            )));
        }
    }
    Ok(())
}

fn parse_json(body: &[u8]) -> Result<Value, AwsServiceError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| invalid_param(&format!("malformed request body: {e}")))
}

fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_param(&format!("{field} is required.")))
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tag_map(v: Option<&Value>) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let Some(obj) = v else {
        return Ok(BTreeMap::new());
    };
    let obj = obj
        .as_object()
        .ok_or_else(|| invalid_param("Tags must be a map of string to string."))?;
    let mut out = BTreeMap::new();
    for (k, val) in obj {
        let val = val
            .as_str()
            .ok_or_else(|| invalid_param("Tag values must be strings."))?;
        out.insert(k.clone(), val.to_string());
    }
    Ok(out)
}

/// A parsed `TagFilter`: a key that must be present, and optionally a set of
/// acceptable values.
struct TagFilter {
    key: String,
    values: Vec<String>,
}

impl TagFilter {
    fn matches(&self, tags: &BTreeMap<String, String>) -> bool {
        match tags.get(&self.key) {
            None => false,
            Some(v) => self.values.is_empty() || self.values.contains(v),
        }
    }
}

fn parse_tag_filters(v: Option<&Value>) -> Result<Vec<TagFilter>, AwsServiceError> {
    let Some(arr) = v else {
        return Ok(Vec::new());
    };
    let arr = arr
        .as_array()
        .ok_or_else(|| invalid_param("TagFilters must be a list."))?;
    let mut out = Vec::new();
    for f in arr {
        let Some(key) = f.get("Key").and_then(Value::as_str) else {
            // A filter with no key matches nothing meaningful; AWS ignores it.
            continue;
        };
        out.push(TagFilter {
            key: key.to_string(),
            values: string_list(f.get("Values")),
        });
    }
    Ok(out)
}

fn tags_to_pairs(tags: &BTreeMap<String, String>) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect()
}

/// A pagination token is the next start index encoded as a decimal string.
/// An empty/absent token starts at 0; an unparseable token yields `None` so
/// callers can return a terminal empty page (never a 4xx).
fn decode_token(v: Option<&Value>) -> Option<usize> {
    match v.and_then(Value::as_str) {
        None | Some("") => Some(0),
        Some(s) => s.parse::<usize>().ok(),
    }
}

fn paginate_strings(all: Vec<String>, start: Option<usize>) -> (Vec<String>, String) {
    let Some(start) = start else {
        return (Vec::new(), String::new());
    };
    let total = all.len();
    if start >= total {
        return (Vec::new(), String::new());
    }
    let end = start
        .saturating_add(MAX_RESOURCES_PER_PAGE as usize)
        .min(total);
    let next = if end < total {
        end.to_string()
    } else {
        String::new()
    };
    (all[start..end].to_vec(), next)
}

/// `service:resourceType` from an ARN, per the tagging API's resource-type
/// convention. Returns just the service when there's no meaningful sub-type.
/// A [`TagProvider`] over the tagging API's own `api_tags` store, so that ARNs
/// tagged via `TagResources` (which no modelled service owns) are visible
/// through the shared [`TagProviderRegistry`] to every reader -- notably
/// Resource Groups tag-query resolution. Register this once at startup.
pub struct ApiTagProvider {
    state: SharedResourceGroupsTaggingState,
}

impl ApiTagProvider {
    pub fn new(state: SharedResourceGroupsTaggingState) -> Self {
        Self { state }
    }
}

impl fakecloud_core::tag_index::TagProvider for ApiTagProvider {
    fn tagged_resources(&self, account_id: &str) -> Vec<fakecloud_core::tag_index::TaggedResource> {
        let accounts = self.state.read();
        accounts
            .get(account_id)
            .map(|st| {
                st.api_tags
                    .iter()
                    .map(|(arn, tags)| {
                        fakecloud_core::tag_index::TaggedResource::new(
                            arn.clone(),
                            resource_type_from_arn(arn),
                            tags.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn resource_type_from_arn(arn: &str) -> String {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" {
        return String::new();
    }
    let service = parts[2];
    let resource_seg = parts[5];
    let restype = resource_seg.split(['/', ':']).next().unwrap_or("");
    if restype.is_empty() || restype == resource_seg && !resource_seg.contains('/') {
        // resource-id only, no type segment (e.g. `arn:aws:s3:::bucket`).
        service.to_string()
    } else {
        format!("{service}:{restype}")
    }
}

/// A resource type matches a filter when the filter equals the full
/// `service:type`, or names just the `service` (AWS treats `ec2` as matching
/// every `ec2:*`).
fn type_matches(resource_type: &str, filters: &[String]) -> bool {
    let service = resource_type.split(':').next().unwrap_or(resource_type);
    filters.iter().any(|f| f == resource_type || f == service)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_type_from_arn_cases() {
        assert_eq!(
            resource_type_from_arn("arn:aws:ec2:us-east-1:123456789012:instance/i-1"),
            "ec2:instance"
        );
        assert_eq!(resource_type_from_arn("arn:aws:s3:::my-bucket"), "s3");
        assert_eq!(
            resource_type_from_arn("arn:aws:dynamodb:us-east-1:123456789012:table/t"),
            "dynamodb:table"
        );
    }

    #[test]
    fn type_matches_service_and_full() {
        assert!(type_matches("ec2:instance", &["ec2".to_string()]));
        assert!(type_matches("ec2:instance", &["ec2:instance".to_string()]));
        assert!(!type_matches("ec2:instance", &["s3".to_string()]));
    }

    #[test]
    fn tag_filter_matches() {
        let mut tags = BTreeMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        let key_only = TagFilter {
            key: "env".into(),
            values: vec![],
        };
        assert!(key_only.matches(&tags));
        let val = TagFilter {
            key: "env".into(),
            values: vec!["prod".into()],
        };
        assert!(val.matches(&tags));
        let miss = TagFilter {
            key: "env".into(),
            values: vec!["dev".into()],
        };
        assert!(!miss.matches(&tags));
    }

    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::tag_index::TagProviderRegistry;
    use parking_lot::RwLock;

    fn svc() -> ResourceGroupsTaggingService {
        let state = Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )));
        ResourceGroupsTaggingService::new(state, TagProviderRegistry::new(), None)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "tagging".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn emsg(r: Result<AwsResponse, AwsServiceError>) -> String {
        match r {
            Ok(_) => panic!("expected an error, got Ok"),
            Err(e) => e.message(),
        }
    }

    #[test]
    fn tag_resources_rejects_over_limit_arns() {
        let arns: Vec<String> = (0..21)
            .map(|i| format!("arn:aws:x:us-east-1:0:t/{i}"))
            .collect();
        let msg = emsg(svc().tag_resources(&req(
            "TagResources",
            json!({ "ResourceARNList": arns, "Tags": { "k": "v" } }),
        )));
        assert!(msg.contains("at most 20"), "{msg}");
    }

    #[test]
    fn tag_resources_rejects_over_limit_tags() {
        let mut tags = serde_json::Map::new();
        for i in 0..51 {
            tags.insert(format!("k{i}"), json!("v"));
        }
        let msg = emsg(svc().tag_resources(&req(
            "TagResources",
            json!({ "ResourceARNList": ["arn:aws:x:us-east-1:0:t/a"], "Tags": tags }),
        )));
        assert!(msg.contains("at most 50"), "{msg}");
    }

    #[test]
    fn get_resources_rejects_arnlist_with_filters() {
        let msg = emsg(svc().get_resources(&req(
            "GetResources",
            json!({
                "ResourceARNList": ["arn:aws:x:us-east-1:0:t/a"],
                "ResourceTypeFilters": ["ec2"],
            }),
        )));
        assert!(msg.contains("cannot be used together"), "{msg}");
    }

    #[test]
    fn tag_then_get_and_untag_roundtrip() {
        let s = svc();
        let arn = "arn:aws:custom:us-east-1:000000000000:thing/abc";
        s.tag_resources(&req(
            "TagResources",
            json!({ "ResourceARNList": [arn], "Tags": { "stage": "prod" } }),
        ))
        .unwrap();
        let got = s.get_resources(&req("GetResources", json!({}))).unwrap();
        let body: Value = serde_json::from_slice(got.body.expect_bytes()).unwrap();
        assert_eq!(body["ResourceTagMappingList"][0]["ResourceARN"], arn);
        // Region filtering: a resource whose ARN region differs is excluded.
        let other_region = s
            .get_resources(&AwsRequest {
                region: "eu-west-1".into(),
                ..req("GetResources", json!({}))
            })
            .unwrap();
        let ob: Value = serde_json::from_slice(other_region.body.expect_bytes()).unwrap();
        assert!(ob["ResourceTagMappingList"].as_array().unwrap().is_empty());
        // Untag.
        s.untag_resources(&req(
            "UntagResources",
            json!({ "ResourceARNList": [arn], "TagKeys": ["stage"] }),
        ))
        .unwrap();
        let after = s.get_resources(&req("GetResources", json!({}))).unwrap();
        let ab: Value = serde_json::from_slice(after.body.expect_bytes()).unwrap();
        assert!(ab["ResourceTagMappingList"].as_array().unwrap().is_empty());
    }

    #[test]
    fn get_resources_paginates_by_tags_per_page() {
        let s = svc();
        // Two resources, 2 tags each. TagsPerPage=2 -> one resource per page.
        for arn in ["arn:aws:x:us-east-1:0:t/a", "arn:aws:x:us-east-1:0:t/b"] {
            s.tag_resources(&req(
                "TagResources",
                json!({ "ResourceARNList": [arn], "Tags": { "k1": "v", "k2": "v" } }),
            ))
            .unwrap();
        }
        let p1 = s
            .get_resources(&req("GetResources", json!({ "TagsPerPage": 2 })))
            .unwrap();
        let b1: Value = serde_json::from_slice(p1.body.expect_bytes()).unwrap();
        assert_eq!(b1["ResourceTagMappingList"].as_array().unwrap().len(), 1);
        assert_eq!(b1["PaginationToken"], "1");
    }
}
