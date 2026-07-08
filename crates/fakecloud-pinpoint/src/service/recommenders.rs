//! Recommender-configuration handlers (a global, persisted resource family).

use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use super::{copy_many, created, not_found, ok, paginate, str_field, Ctx, PinpointService};
use crate::shared;

/// Scalar `RecommenderConfigurationResponse` members echoed from the request.
const RECOMMENDER_SCALARS: &[&str] = &[
    "Name",
    "Description",
    "RecommendationProviderIdType",
    "RecommendationTransformerUri",
    "RecommendationsDisplayName",
    "RecommendationsPerMessage",
    "Attributes",
];

impl PinpointService {
    pub(super) fn create_recommender(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::hex_id();
        let record = build_recommender(&id, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.recommenders.insert(id, record.clone());
        created(record)
    }

    pub(super) fn get_recommender(
        &self,
        ctx: &Ctx,
        rid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        guard
            .get(&ctx.account)
            .and_then(|d| d.recommenders.get(rid))
            .cloned()
            .ok_or_else(|| not_found_recommender(rid))
            .and_then(ok)
    }

    pub(super) fn update_recommender(
        &self,
        ctx: &Ctx,
        rid: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.recommenders.contains_key(rid) {
            return Err(not_found_recommender(rid));
        }
        let created_date = data
            .recommenders
            .get(rid)
            .and_then(|r| r.get("CreationDate").cloned())
            .unwrap_or_else(|| json!(shared::now_iso()));
        let mut record = build_recommender(rid, body);
        if let Some(obj) = record.as_object_mut() {
            obj.insert("CreationDate".into(), created_date);
        }
        data.recommenders.insert(rid.to_string(), record.clone());
        ok(record)
    }

    pub(super) fn delete_recommender(
        &self,
        ctx: &Ctx,
        rid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.recommenders.remove(rid) {
            Some(rec) => ok(rec),
            None => Err(not_found_recommender(rid)),
        }
    }

    pub(super) fn get_recommenders(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.recommenders.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(items, q)?;
        let mut out = Map::new();
        out.insert("Item".into(), json!(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }
}

fn not_found_recommender(rid: &str) -> AwsServiceError {
    not_found(&format!(
        "Recommender configuration '{rid}' does not exist."
    ))
}

fn build_recommender(id: &str, body: &Value) -> Value {
    let now = shared::now_iso();
    let mut out = Map::new();
    out.insert("Id".into(), json!(id));
    out.insert("CreationDate".into(), json!(now));
    out.insert("LastModifiedDate".into(), json!(now));
    out.insert(
        "RecommendationProviderRoleArn".into(),
        json!(str_field(body, "RecommendationProviderRoleArn")),
    );
    out.insert(
        "RecommendationProviderUri".into(),
        json!(str_field(body, "RecommendationProviderUri")),
    );
    copy_many(&mut out, body, RECOMMENDER_SCALARS);
    Value::Object(out)
}
