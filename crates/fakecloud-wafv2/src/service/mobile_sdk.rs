//! `Wafv2Service` `mobile_sdk` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn generate_mobile_sdk_release_url(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let platform = require_str(&body, "Platform")?;
        validate_enum(&platform, &["IOS", "ANDROID"], "Platform")?;
        let release = require_str_len(&body, "ReleaseVersion", 1, 64)?;
        Ok(AwsResponse::ok_json(json!({
            "Url": format!("https://wafv2-mobile-sdk.{}.amazonaws.com/{}/{}.zip", req.region, platform, release),
        })))
    }

    pub(super) fn get_mobile_sdk_release(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let platform = require_str(&body, "Platform")?;
        validate_enum(&platform, &["IOS", "ANDROID"], "Platform")?;
        let release = require_str_len(&body, "ReleaseVersion", 1, 64)?;
        Ok(AwsResponse::ok_json(json!({
            "MobileSdkRelease": {
                "ReleaseVersion": release,
                "Timestamp": Utc::now().timestamp() as f64,
                "ReleaseNotes": format!("fakecloud {platform} SDK release {release}"),
                "Tags": [],
            },
        })))
    }

    pub(super) fn list_mobile_sdk_releases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let platform = require_str(&body, "Platform")?;
        validate_enum(&platform, &["IOS", "ANDROID"], "Platform")?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        let limit = body.get("Limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let next_marker = body.get("NextMarker").and_then(Value::as_str).unwrap_or("");
        let all = vec![
            json!({"ReleaseVersion": "1.0.0", "Timestamp": Utc::now().timestamp() as f64}),
            json!({"ReleaseVersion": "1.1.0", "Timestamp": Utc::now().timestamp() as f64}),
        ];
        let start = if next_marker.is_empty() {
            0
        } else {
            all.iter()
                .position(|r| r.get("ReleaseVersion").and_then(Value::as_str) == Some(next_marker))
                .map(|p| p + 1)
                .unwrap_or(0)
        };
        let page: Vec<Value> = all.iter().skip(start).take(limit).cloned().collect();
        let next = if start + page.len() < all.len() {
            page.last()
                .and_then(|r| r.get("ReleaseVersion"))
                .and_then(Value::as_str)
                .map(str::to_string)
        } else {
            None
        };
        let mut out = json!({ "ReleaseSummaries": page });
        if let Some(n) = next {
            out["NextMarker"] = json!(n);
        }
        Ok(AwsResponse::ok_json(out))
    }
}
