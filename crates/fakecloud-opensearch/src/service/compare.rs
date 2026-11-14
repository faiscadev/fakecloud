//! `CompareDomainConfiguration`: diff the metadata (aliases, templates,
//! ingest pipelines, cluster settings, plugin configuration, ...) of two
//! domains, read live from their backing clusters.
//!
//! Each category is read from both clusters through the same REST API a
//! client would use. A category that cannot be read from either domain (no
//! backing cluster, the plugin that owns it is not installed or not enabled,
//! the cluster refused the request) is reported with status `error`, as AWS
//! reports a category it could not read.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{body, not_found_generic, ok, req_str, validation};

/// The categories the comparison understands, in the model's order.
pub(super) const CATEGORIES: &[&str] = &[
    "aliases",
    "alerting_monitors",
    "cluster_settings",
    "component_templates",
    "ingest_pipelines",
    "ism_policies",
    "security_action_groups",
    "security_internal_users",
    "security_role_mappings",
    "security_roles",
    "templates",
];

/// `DiffItemNameList` / `DivergentItemList` hold at most this many names.
const MAX_LISTED_ITEMS: usize = 1000;
/// `DiffFieldValue` is at most this many characters.
const MAX_FIELD_VALUE: usize = 10240;
/// `DiffFieldPath` is at most this many characters.
const MAX_FIELD_PATH: usize = 512;
/// `DiffItemName` is at most this many characters.
const MAX_ITEM_NAME: usize = 256;

/// Fields whose values are bookkeeping of the cluster that stores the item
/// (timestamps), not configuration: two copies of one definition differ
/// there by construction.
const VOLATILE_FIELDS: &[&str] = &["last_update_time", "last_updated_time", "enabled_time"];

/// Fields compared without revealing their values (`Policy: redacted`).
const REDACTED_FIELDS: &[&str] = &["hash", "password"];

/// The items of one category, keyed by item name.
type Items = BTreeMap<String, Value>;

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client")
    })
}

impl super::OpenSearchService {
    pub(super) async fn compare_domain_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let source_arn = req_str(&b, "SourceDomainArn")?;
        let target_arn = req_str(&b, "TargetDomainArn")?;
        let categories = parse_categories(b.get("Categories"))?;

        // Where each domain's cluster answers, resolved before any I/O so no
        // lock is held across it.
        let (source_url, target_url) = {
            let accounts = self.state.read();
            let find = |arn: &str| {
                accounts
                    .get(&req.account_id)
                    .and_then(|st| st.domains.values().find(|d| d.arn == arn && !d.deleted))
                    .map(|d| d.reachable_url())
                    .ok_or_else(|| not_found_generic(&format!("Domain not found: {arn}")))
            };
            (find(&source_arn)?, find(&target_arn)?)
        };

        let mut results = Map::new();
        for category in &categories {
            let source = match &source_url {
                Some(url) => read_category(url, category).await,
                None => Err("the source domain has no running cluster".to_string()),
            };
            let target = match &target_url {
                Some(url) => read_category(url, category).await,
                None => Err("the target domain has no running cluster".to_string()),
            };
            let result = match (source, target) {
                (Ok(s), Ok(t)) => diff_items(&s, &t),
                (Err(e), _) | (_, Err(e)) => {
                    tracing::debug!(category, error = %e, "category could not be compared");
                    error_result()
                }
            };
            results.insert(category.clone(), result);
        }
        Ok(ok(json!({
            "SourceDomainArn": source_arn,
            "TargetDomainArn": target_arn,
            "Categories": results,
        })))
    }
}

/// Validate `Categories` (1-20 names of 1-128 characters, each a supported
/// category); duplicates collapse.
fn parse_categories(v: Option<&Value>) -> Result<Vec<String>, AwsServiceError> {
    let list = v
        .and_then(Value::as_array)
        .ok_or_else(|| validation("Categories is required."))?;
    if list.is_empty() || list.len() > 20 {
        return Err(validation(
            "Categories must contain between 1 and 20 categories.",
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for item in list {
        let name = item
            .as_str()
            .ok_or_else(|| validation("Categories must contain strings."))?;
        if name.is_empty() || name.chars().count() > 128 {
            return Err(validation(
                "Each category must be between 1 and 128 characters.",
            ));
        }
        if !CATEGORIES.contains(&name) {
            return Err(validation(format!(
                "Unsupported category '{name}'. Supported categories: {}.",
                CATEGORIES.join(", ")
            )));
        }
        if !out.iter().any(|c| c == name) {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

fn error_result() -> Value {
    json!({
        "Status": "error",
        "InSync": 0,
        "SourceOnly": 0,
        "TargetOnly": 0,
        "Diverged": 0,
    })
}

/// Read one category's items from the cluster at `base`.
async fn read_category(base: &str, category: &str) -> Result<Items, String> {
    let security = |api: &str| format!("/_plugins/_security/api/{api}");
    let items = match category {
        "aliases" => aliases(get(base, "/_alias").await?),
        "alerting_monitors" => monitors(
            post(
                base,
                "/_plugins/_alerting/monitors/_search",
                json!({"size": MAX_LISTED_ITEMS, "query": {"match_all": {}}}),
            )
            .await?,
        ),
        "cluster_settings" => {
            cluster_settings(get(base, "/_cluster/settings?flat_settings=true").await?)
        }
        "component_templates" => named_list(
            get(base, "/_component_template").await?,
            "component_templates",
            "component_template",
        ),
        "ingest_pipelines" => keyed(get(base, "/_ingest/pipeline").await?),
        "ism_policies" => ism_policies(get(base, "/_plugins/_ism/policies?size=1000").await?),
        "security_action_groups" => keyed(get(base, &security("actiongroups")).await?),
        "security_internal_users" => keyed(get(base, &security("internalusers")).await?),
        "security_role_mappings" => keyed(get(base, &security("rolesmapping")).await?),
        "security_roles" => keyed(get(base, &security("roles")).await?),
        "templates" => named_list(
            get(base, "/_index_template").await?,
            "index_templates",
            "index_template",
        ),
        other => return Err(format!("unsupported category {other}")),
    };
    Ok(items
        .into_iter()
        .filter(|(name, _)| !name.starts_with('.'))
        .map(|(name, mut v)| {
            strip_volatile(&mut v);
            (name, v)
        })
        .collect())
}

async fn get(base: &str, path: &str) -> Result<Value, String> {
    let resp = client()
        .get(format!("{base}{path}"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    read_json(resp).await
}

async fn post(base: &str, path: &str, payload: Value) -> Result<Value, String> {
    let resp = client()
        .post(format!("{base}{path}"))
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    read_json(resp).await
}

/// A 2xx JSON body, or an empty category when the cluster reports the
/// store holding it does not exist yet (nothing was ever written there).
async fn read_json(resp: reqwest::Response) -> Result<Value, String> {
    let status = resp.status();
    let text = resp.text().await.map_err(|e| e.to_string())?;
    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    if status.is_success() {
        return Ok(v);
    }
    if status.as_u16() == 404 && is_nothing_stored(&v) {
        return Ok(json!({}));
    }
    Err(format!("HTTP {status}: {text}"))
}

fn is_nothing_stored(v: &Value) -> bool {
    if v.as_object().is_some_and(Map::is_empty) {
        return true;
    }
    let kind = v
        .pointer("/error/type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    matches!(
        kind,
        "index_not_found_exception" | "resource_not_found_exception"
    )
}

/// `GET /_alias`: `{index: {aliases: {alias: def}}}` regrouped per alias as
/// `{alias: {index: def}}`.
fn aliases(v: Value) -> Items {
    let mut out = Items::new();
    for (index, entry) in v.as_object().into_iter().flatten() {
        if index.starts_with('.') {
            continue;
        }
        for (alias, def) in entry
            .get("aliases")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let slot = out.entry(alias.clone()).or_insert_with(|| json!({}));
            slot[index] = def.clone();
        }
    }
    out
}

/// Alerting monitor search hits keyed by monitor name (workflows, stored in
/// the same index, are not monitors).
fn monitors(v: Value) -> Items {
    let mut out = Items::new();
    for hit in v
        .pointer("/hits/hits")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(src) = hit.get("_source") else {
            continue;
        };
        if src.get("workflow").is_some() {
            continue;
        }
        let monitor = src.get("monitor").unwrap_or(src);
        if let Some(name) = monitor.get("name").and_then(Value::as_str) {
            out.insert(name.to_string(), monitor.clone());
        }
    }
    out
}

/// `GET /_cluster/settings?flat_settings=true`: one item per setting, whose
/// fields are the scopes (`persistent`, `transient`) it is set in.
fn cluster_settings(v: Value) -> Items {
    let mut out = Items::new();
    for scope in ["persistent", "transient"] {
        for (key, value) in v
            .get(scope)
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let slot = out.entry(key.clone()).or_insert_with(|| json!({}));
            slot[scope] = value.clone();
        }
    }
    out
}

/// `{list_key: [{name, def_key}]}` (index and component templates).
fn named_list(v: Value, list_key: &str, def_key: &str) -> Items {
    v.get(list_key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let name = e.get("name")?.as_str()?.to_string();
            Some((name, e.get(def_key).cloned().unwrap_or(Value::Null)))
        })
        .collect()
}

/// `{name: def}` (ingest pipelines, security configuration).
fn keyed(v: Value) -> Items {
    v.as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// `GET /_plugins/_ism/policies`: `{policies: [{_id, policy}]}`.
fn ism_policies(v: Value) -> Items {
    v.get("policies")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let name = e.get("_id")?.as_str()?.to_string();
            Some((name, e.get("policy").cloned().unwrap_or(Value::Null)))
        })
        .collect()
}

fn strip_volatile(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.retain(|k, _| !VOLATILE_FIELDS.contains(&k.as_str()));
            m.values_mut().for_each(strip_volatile);
        }
        Value::Array(a) => a.iter_mut().for_each(strip_volatile),
        _ => {}
    }
}

/// Every leaf of `v` keyed by its path (`a.b[0].c`); an empty object or
/// array is a leaf of its own.
fn flatten(v: &Value, path: String, out: &mut BTreeMap<String, Value>) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            for (k, child) in m {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                flatten(child, p, out);
            }
        }
        Value::Array(a) if !a.is_empty() => {
            for (i, child) in a.iter().enumerate() {
                flatten(child, format!("{path}[{i}]"), out);
            }
        }
        _ => {
            out.insert(path, v.clone());
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn render_value(v: &Value) -> String {
    let s = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    truncate(&s, MAX_FIELD_VALUE)
}

fn is_redacted(path: &str) -> bool {
    let leaf = path.rsplit(['.', ']']).next().unwrap_or(path);
    REDACTED_FIELDS.contains(&leaf)
}

/// The fields in which two copies of an item differ.
fn divergent_fields(source: &Value, target: &Value) -> Vec<Value> {
    let (mut s, mut t) = (BTreeMap::new(), BTreeMap::new());
    flatten(source, String::new(), &mut s);
    flatten(target, String::new(), &mut t);
    let paths: BTreeSet<&String> = s.keys().chain(t.keys()).collect();
    paths
        .into_iter()
        .filter(|p| s.get(*p) != t.get(*p))
        .map(|p| {
            let mut field = json!({ "Path": truncate(p, MAX_FIELD_PATH) });
            if is_redacted(p) {
                field["Policy"] = json!("redacted");
            } else {
                if let Some(v) = s.get(p) {
                    field["SourceValue"] = json!(render_value(v));
                }
                if let Some(v) = t.get(p) {
                    field["TargetValue"] = json!(render_value(v));
                }
            }
            field
        })
        .collect()
}

/// Compare one category's items on both domains.
fn diff_items(source: &Items, target: &Items) -> Value {
    let mut in_sync = 0usize;
    let mut source_only = Vec::new();
    let mut target_only = Vec::new();
    let mut diverged = Vec::new();
    let names: BTreeSet<&String> = source.keys().chain(target.keys()).collect();
    for name in names {
        match (source.get(name), target.get(name)) {
            (Some(s), Some(t)) if s == t => in_sync += 1,
            (Some(s), Some(t)) => diverged.push(json!({
                "Name": truncate(name, MAX_ITEM_NAME),
                "Fields": divergent_fields(s, t),
            })),
            (Some(_), None) => source_only.push(json!(truncate(name, MAX_ITEM_NAME))),
            (None, Some(_)) => target_only.push(json!(truncate(name, MAX_ITEM_NAME))),
            (None, None) => {}
        }
    }
    let counts = (source_only.len(), target_only.len(), diverged.len());
    let truncated = [counts.0, counts.1, counts.2]
        .iter()
        .any(|n| *n > MAX_LISTED_ITEMS);
    source_only.truncate(MAX_LISTED_ITEMS);
    target_only.truncate(MAX_LISTED_ITEMS);
    diverged.truncate(MAX_LISTED_ITEMS);
    json!({
        "Status": "compared",
        "InSync": in_sync,
        "SourceOnly": counts.0,
        "TargetOnly": counts.1,
        "Diverged": counts.2,
        "Items": {
            "SourceOnly": source_only,
            "TargetOnly": target_only,
            "Diverged": diverged,
        },
        "Truncated": truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(v: Value) -> Items {
        keyed(v)
    }

    #[test]
    fn diff_counts_and_names_every_kind_of_difference() {
        let source = items(json!({
            "same": {"processors": [{"set": {"field": "a", "value": 1}}]},
            "changed": {"description": "x", "processors": [{"set": {"field": "a"}}]},
            "only-src": {},
        }));
        let target = items(json!({
            "same": {"processors": [{"set": {"field": "a", "value": 1}}]},
            "changed": {"description": "y", "processors": [{"set": {"field": "b"}}], "extra": true},
            "only-tgt": {},
        }));
        let r = diff_items(&source, &target);
        assert_eq!(r["Status"], "compared");
        assert_eq!(r["InSync"], 1);
        assert_eq!(r["SourceOnly"], 1);
        assert_eq!(r["TargetOnly"], 1);
        assert_eq!(r["Diverged"], 1);
        assert_eq!(r["Truncated"], false);
        assert_eq!(r["Items"]["SourceOnly"], json!(["only-src"]));
        assert_eq!(r["Items"]["TargetOnly"], json!(["only-tgt"]));
        let item = &r["Items"]["Diverged"][0];
        assert_eq!(item["Name"], "changed");
        assert_eq!(
            item["Fields"],
            json!([
                {"Path": "description", "SourceValue": "x", "TargetValue": "y"},
                {"Path": "extra", "TargetValue": "true"},
                {"Path": "processors[0].set.field", "SourceValue": "a", "TargetValue": "b"},
            ])
        );
    }

    #[test]
    fn secrets_are_compared_but_not_revealed() {
        let source = items(json!({"u": {"hash": "aaa", "backend_roles": ["r1"]}}));
        let target = items(json!({"u": {"hash": "bbb", "backend_roles": ["r1"]}}));
        let r = diff_items(&source, &target);
        assert_eq!(
            r["Items"]["Diverged"][0]["Fields"],
            json!([{"Path": "hash", "Policy": "redacted"}])
        );
    }

    #[test]
    fn listings_are_capped_but_counts_stay_complete() {
        let source: Items = (0..1005).map(|i| (format!("p{i:04}"), json!({}))).collect();
        let r = diff_items(&source, &Items::new());
        assert_eq!(r["SourceOnly"], 1005);
        assert_eq!(r["Items"]["SourceOnly"].as_array().unwrap().len(), 1000);
        assert_eq!(r["Truncated"], true);
    }

    #[test]
    fn category_shapes_are_read_into_named_items() {
        let a = aliases(json!({
            "logs-1": {"aliases": {"logs": {}, "w": {"is_write_index": true}}},
            "logs-2": {"aliases": {"logs": {}}},
            ".kibana_1": {"aliases": {".kibana": {}}},
        }));
        assert_eq!(a.keys().collect::<Vec<_>>(), ["logs", "w"]);
        assert_eq!(a["logs"], json!({"logs-1": {}, "logs-2": {}}));

        let s = cluster_settings(json!({
            "persistent": {"cluster.max_shards_per_node": "2000"},
            "transient": {"cluster.max_shards_per_node": "3000", "a.b": "1"},
        }));
        assert_eq!(
            s["cluster.max_shards_per_node"],
            json!({"persistent": "2000", "transient": "3000"})
        );
        assert_eq!(s["a.b"], json!({"transient": "1"}));

        let t = named_list(
            json!({"index_templates": [{"name": "t1", "index_template": {"index_patterns": ["a*"]}}]}),
            "index_templates",
            "index_template",
        );
        assert_eq!(t["t1"], json!({"index_patterns": ["a*"]}));

        let m = monitors(json!({"hits": {"hits": [
            {"_id": "1", "_source": {"monitor": {"name": "cpu", "enabled": true}}},
            {"_id": "2", "_source": {"workflow": {"name": "wf"}}},
        ]}}));
        assert_eq!(m.keys().collect::<Vec<_>>(), ["cpu"]);

        let p = ism_policies(json!({"policies": [{"_id": "hot-warm", "policy": {"states": []}}]}));
        assert_eq!(p["hot-warm"], json!({"states": []}));
    }

    #[test]
    fn bookkeeping_timestamps_do_not_count_as_differences() {
        let mut a = json!({"policy_id": "p", "last_updated_time": 1, "ism_template": [{"last_updated_time": 5}]});
        let mut b = json!({"policy_id": "p", "last_updated_time": 2, "ism_template": [{"last_updated_time": 9}]});
        strip_volatile(&mut a);
        strip_volatile(&mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn categories_are_validated_against_the_supported_set() {
        assert_eq!(
            parse_categories(Some(&json!(["templates", "aliases", "templates"]))).unwrap(),
            ["templates", "aliases"]
        );
        for bad in [json!([]), json!(["nope"]), json!([""]), json!("templates")] {
            assert!(parse_categories(Some(&bad)).is_err(), "{bad}");
        }
        let err = parse_categories(Some(&json!(["nope"]))).unwrap_err();
        assert!(format!("{err:?}").contains("ingest_pipelines"));
    }

    #[test]
    fn a_missing_store_reads_as_an_empty_category() {
        assert!(is_nothing_stored(&json!({})));
        assert!(is_nothing_stored(
            &json!({"error": {"type": "index_not_found_exception"}, "status": 404})
        ));
        assert!(!is_nothing_stored(
            &json!({"error": "no handler found for uri", "status": 400})
        ));
    }
}
