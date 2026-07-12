//! Data Catalog assets.
//!
//! Only `UpdateAsset` is currently modelled by AWS's public Glue definition in
//! this crate's scope, and it operates on a pre-existing asset (there is no
//! `CreateAsset`/`PutAsset` handler yet), so an update against an unknown
//! identifier returns `EntityNotFoundException`, matching AWS. The `assets`
//! store is name-keyed so that once seeding operations land the update path
//! round-trips without further changes.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{now_ts, req_str};
use crate::generic;
use crate::service::GlueService;

impl GlueService {
    pub(crate) fn update_asset(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identifier = req_str(&body, "Identifier")?.to_string();

        // Only the fields the caller supplies are updated.
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in ["Name", "Description"] {
            if let Some(v) = body.get(f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        let updated_at = now_ts();
        updates.push(("UpdatedAt", json!(updated_at)));

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut state.assets, &identifier, "Asset", updates)?;

        let stored = state
            .assets
            .get(&identifier)
            .cloned()
            .unwrap_or_else(|| json!({}));

        let mut resp = json!({
            "Id": identifier,
            "UpdatedAt": updated_at,
        });
        if let Some(name) = stored.get("Name").filter(|v| !v.is_null()) {
            resp["Name"] = name.clone();
        }
        if let Some(desc) = stored.get("Description").filter(|v| !v.is_null()) {
            resp["Description"] = desc.clone();
        }
        Ok(AwsResponse::ok_json(resp))
    }
}
