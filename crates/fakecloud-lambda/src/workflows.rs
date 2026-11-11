//! Lambda Capacity Providers (2025-11-30 API): customer-managed compute
//! pools functions can run in. Durable executions (2025-12-01 API) live in
//! `crate::durable`.

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::extras::check_len;
use crate::state::{CapacityProvider, SharedLambdaState};

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn arn_for_capacity_provider(region: &str, account: &str, name: &str) -> String {
    Arn::regional(
        "lambda",
        region,
        account,
        &format!("capacity-provider:{name}"),
    )
    .to_string()
}

fn capacity_provider_json(cp: &CapacityProvider) -> Value {
    json!({
        "CapacityProviderArn": cp.arn,
        "State": cp.state,
        "VpcConfig": cp.vpc_config,
        "PermissionsConfig": cp.permissions_config,
        "InstanceRequirements": cp.instance_requirements,
        "CapacityProviderScalingConfig": cp.scaling_config,
        "KmsKeyArn": cp.kms_key_arn,
        "LastModified": cp.last_modified.to_rfc3339(),
    })
}

pub(crate) fn create_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let cp = create_capacity_provider_in(state, &req.account_id, &req.region, body)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&cp) }),
    ))
}

/// `CreateCapacityProvider` in `account`/`region` from a request-shaped
/// `body`, shared with CloudFormation's `AWS::Lambda::CapacityProvider`.
pub fn create_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    body: &Value,
) -> Result<CapacityProvider, AwsServiceError> {
    let name = body["CapacityProviderName"]
        .as_str()
        .ok_or_else(|| validation("CapacityProviderName is required"))?
        .to_string();
    check_len("CapacityProviderName", &name, 1, 140)?;
    let vpc = body
        .get("VpcConfig")
        .filter(|v| !v.is_null())
        .ok_or_else(|| validation("VpcConfig is required"))?
        .clone();
    let perms = body
        .get("PermissionsConfig")
        .filter(|v| !v.is_null())
        .ok_or_else(|| validation("PermissionsConfig is required"))?
        .clone();

    let arn = arn_for_capacity_provider(region, account, &name);

    let mut accts = state.write();
    let s = accts.regional_mut(account, region);
    if s.capacity_providers.contains_key(&name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            format!("Capacity provider {name} already exists"),
        ));
    }

    let tags = body["Tags"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let cp = CapacityProvider {
        name: name.clone(),
        arn,
        state: "Active".to_string(),
        vpc_config: vpc,
        permissions_config: perms,
        instance_requirements: body.get("InstanceRequirements").cloned(),
        scaling_config: body.get("CapacityProviderScalingConfig").cloned(),
        kms_key_arn: body["KmsKeyArn"].as_str().map(String::from),
        tags,
        last_modified: Utc::now(),
    };
    s.capacity_providers.insert(name, cp.clone());
    Ok(cp)
}

pub(crate) fn get_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let name = crate::capacity::capacity_provider_name_from_label(name, &req.region)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let cp = s.capacity_providers.get(name).ok_or_else(|| {
        crate::capacity::provider_not_found(&arn_for_capacity_provider(
            &req.region,
            &req.account_id,
            name,
        ))
    })?;

    Ok(AwsResponse::ok_json(
        json!({ "CapacityProvider": capacity_provider_json(cp) }),
    ))
}

pub(crate) fn list_capacity_providers(
    state: &SharedLambdaState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(s) = req.query_params.get("MaxItems") {
        let n: i64 = s.parse().map_err(|_| validation("MaxItems must be int"))?;
        if !(1..=50).contains(&n) {
            return Err(validation(format!("MaxItems must be in [1,50], got {n}")));
        }
    }
    if let Some(st) = req.query_params.get("State") {
        if !matches!(st.as_str(), "Pending" | "Active" | "Failed" | "Deleting") {
            return Err(validation(format!("State enum invalid: {st}")));
        }
    }
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let providers: Vec<Value> = s
        .capacity_providers
        .values()
        .map(capacity_provider_json)
        .collect();

    Ok(AwsResponse::ok_json(json!({
        "CapacityProviders": providers,
    })))
}

pub(crate) fn update_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let cp = update_capacity_provider_in(state, &req.account_id, &req.region, name, body)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&cp) }),
    ))
}

/// `UpdateCapacityProvider` of the provider `name` (a name or ARN) in
/// `account`/`region`, shared with CloudFormation.
pub fn update_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    name: &str,
    body: &Value,
) -> Result<CapacityProvider, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label, region)?;
    let mut accts = state.write();
    let s = accts.regional_mut(account, region);
    let cp = s
        .capacity_providers
        .get_mut(name)
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    if let Some(v) = body.get("VpcConfig").filter(|v| !v.is_null()) {
        cp.vpc_config = v.clone();
    }
    if let Some(v) = body.get("PermissionsConfig").filter(|v| !v.is_null()) {
        cp.permissions_config = v.clone();
    }
    if let Some(v) = body.get("InstanceRequirements") {
        cp.instance_requirements = if v.is_null() { None } else { Some(v.clone()) };
    }
    if let Some(v) = body.get("CapacityProviderScalingConfig") {
        cp.scaling_config = if v.is_null() { None } else { Some(v.clone()) };
    }
    if let Some(v) = body["KmsKeyArn"].as_str() {
        cp.kms_key_arn = Some(v.to_string());
    }
    cp.last_modified = Utc::now();
    Ok(cp.clone())
}

pub(crate) fn delete_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let deleted = delete_capacity_provider_in(state, &req.account_id, &req.region, name)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&deleted) }),
    ))
}

/// `DeleteCapacityProvider` of the provider `name` (a name or ARN) in
/// `account`/`region`, shared with CloudFormation. A provider still used by
/// a function version is refused with `ResourceConflictException`.
pub fn delete_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    name: &str,
) -> Result<CapacityProvider, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label, region)?;
    let mut accts = state.write();
    let arn = accts
        .regional(account, region)
        .and_then(|s| s.capacity_providers.get(name))
        .map(|cp| cp.arn.clone())
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    // A capacity provider cannot be deleted while function versions use it.
    let in_use = crate::capacity::versions_using(&accts, &arn);
    if !in_use.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            format!(
                "Capacity provider {arn} can't be deleted because it's in use by {} function version(s). Delete the function versions that use it first.",
                in_use.len()
            ),
        ));
    }
    let s = accts.regional_mut(account, region);
    let mut deleted = s
        .capacity_providers
        .remove(name)
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    deleted.state = "Deleting".to_string();
    deleted.last_modified = Utc::now();
    Ok(deleted)
}

pub(crate) fn list_function_versions_by_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label, &req.region)?;
    // `MaxItems` (1..50) is range-checked by the dispatcher.
    let max_items = req
        .query_params
        .get("MaxItems")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(50);
    let accts = state.read();
    let cp = accts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.capacity_providers.get(name))
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    let versions = crate::capacity::versions_using(&accts, &cp.arn);
    // `Marker` is the ARN of the last version of the previous page.
    let start = match req.query_params.get("Marker").filter(|m| !m.is_empty()) {
        Some(marker) => versions
            .iter()
            .position(|v| v.function_arn.as_str() > marker.as_str())
            .unwrap_or(versions.len()),
        None => 0,
    };
    let page: Vec<_> = versions.iter().skip(start).take(max_items).collect();
    let mut out = json!({
        "CapacityProviderArn": cp.arn,
        "FunctionVersions": page
            .iter()
            .map(|v| json!({"FunctionArn": v.function_arn, "State": v.state}))
            .collect::<Vec<_>>(),
    });
    if start + page.len() < versions.len() {
        if let Some(last) = page.last() {
            out["NextMarker"] = json!(last.function_arn);
        }
    }
    Ok(AwsResponse::ok_json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SharedLambdaState;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared() -> SharedLambdaState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "lambda".to_string(),
            action: "x".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_then_get_capacity_provider_roundtrips_state() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {"SubnetIds": ["sub"], "SecurityGroupIds": ["sg"]},
            "PermissionsConfig": {"RoleArn": "arn:aws:iam::123:role/r"}
        });
        let resp = create_capacity_provider(&s, &req(), &body).unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
        let got = get_capacity_provider(&s, &req(), "cp1").unwrap();
        assert_eq!(got.status, StatusCode::OK);
    }

    #[test]
    fn create_capacity_provider_requires_vpc_and_perms() {
        let s = shared();
        let err = create_capacity_provider(&s, &req(), &json!({"CapacityProviderName": "x"}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_capacity_provider_duplicate_is_conflict() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {},
            "PermissionsConfig": {}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        let err = create_capacity_provider(&s, &req(), &body).err().unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn delete_capacity_provider_removes_state() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {},
            "PermissionsConfig": {}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        delete_capacity_provider(&s, &req(), "cp1").unwrap();
        let err = get_capacity_provider(&s, &req(), "cp1").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn update_capacity_provider_patches_fields_and_bumps_last_modified() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {"SubnetIds": []},
            "PermissionsConfig": {"RoleArn": "old"}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        let prev_mod = s
            .read()
            .regional("123456789012", "us-east-1")
            .unwrap()
            .capacity_providers["cp1"]
            .last_modified;
        std::thread::sleep(std::time::Duration::from_millis(2));
        update_capacity_provider(
            &s,
            &req(),
            "cp1",
            &json!({"PermissionsConfig": {"RoleArn": "new"}}),
        )
        .unwrap();
        let state = s.read();
        let cp = &state
            .regional("123456789012", "us-east-1")
            .unwrap()
            .capacity_providers["cp1"];
        assert_eq!(cp.permissions_config["RoleArn"], "new");
        assert!(cp.last_modified > prev_mod);
    }

    #[test]
    fn capacity_provider_arn_is_aws_shaped_and_addresses_the_provider() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {},
            "PermissionsConfig": {}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        let arn = s
            .read()
            .regional("123456789012", "us-east-1")
            .unwrap()
            .capacity_providers["cp1"]
            .arn
            .clone();
        assert_eq!(
            arn,
            "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp1"
        );
        // `CapacityProviderName` also takes the provider's ARN.
        let got = get_capacity_provider(&s, &req(), &arn).unwrap();
        assert_eq!(got.status, StatusCode::OK);
    }

    #[test]
    fn restored_slash_form_capacity_provider_arn_reads_back_in_aws_form() {
        let legacy = json!({
            "name": "cp1",
            "arn": "arn:aws:lambda:us-east-1:123456789012:capacity-provider/cp1",
            "state": "Active",
            "vpc_config": {},
            "permissions_config": {},
            "instance_requirements": null,
            "scaling_config": null,
            "kms_key_arn": null,
            "tags": {},
            "last_modified": "2026-01-01T00:00:00Z"
        });
        let cp: CapacityProvider = serde_json::from_value(legacy).unwrap();
        assert_eq!(
            cp.arn,
            "arn:aws:lambda:us-east-1:123456789012:capacity-provider:cp1"
        );
    }
}
