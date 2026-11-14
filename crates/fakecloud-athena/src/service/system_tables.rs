//! `AthenaService` system table configuration family
//! (Create/Get/Update/Delete SystemTableConfiguration, ListSystemTableStatuses).

use std::sync::LazyLock;

use regex::Regex;

use super::*;
use crate::state::SystemTableConfiguration;

/// Smithy `SystemTableName` enum.
const SYSTEM_TABLE_NAMES: &[&str] = &["ALL"];

/// Smithy `KmsKeyArn` @pattern.
static KMS_KEY_ARN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^arn:aws[a-z\-]*:kms:[a-z0-9\-]+:\d{12}:(key/(mrk-)?[0-9a-f\-]+|alias/[a-zA-Z0-9/_\-]+)$",
    )
    .expect("valid KmsKeyArn regex")
});

fn require_table_name(body: &Value) -> Result<String, AwsServiceError> {
    let name = require_str(body, "TableName")?;
    if !SYSTEM_TABLE_NAMES.contains(&name.as_str()) {
        return Err(invalid_request(format!(
            "TableName value {name:?} is not a valid enum value"
        )));
    }
    Ok(name)
}

fn optional_kms_key_arn(body: &Value) -> Result<Option<String>, AwsServiceError> {
    validate_opt_string_len(body, "KmsKeyArn", 20, 2048)?;
    let Some(arn) = body.get("KmsKeyArn").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !KMS_KEY_ARN_RE.is_match(arn) {
        return Err(invalid_request(format!(
            "KmsKeyArn {arn:?} is not a valid KMS key ARN"
        )));
    }
    Ok(Some(arn.to_string()))
}

fn configuration_json(cfg: &SystemTableConfiguration) -> Value {
    let mut obj = json!({
        "TableName": cfg.table_name,
        "CreatedAt": cfg.created_at.timestamp_millis() as f64 / 1000.0,
        "LastModifiedAt": cfg.last_modified_at.timestamp_millis() as f64 / 1000.0,
    });
    if let Some(k) = &cfg.kms_key_arn {
        obj["KmsKeyArn"] = json!(k);
    }
    obj
}

fn not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("System table configuration {name} does not exist"),
        vec![("ResourceName".to_string(), name.to_string())],
    )
}

impl AthenaService {
    pub(super) fn create_system_table_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ClientRequestToken targets IdempotencyToken @length(32,128).
        validate_opt_string_len(&body, "ClientRequestToken", 32, 128)?;
        let token = body
            .get("ClientRequestToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let table_name = require_table_name(&body)?;
        let kms_key_arn = optional_kms_key_arn(&body)?;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if let Some(existing) = account.system_table_configurations.get(&table_name) {
            // A retry carrying the same idempotency token returns the
            // configuration it created instead of failing.
            if token.is_some() && existing.client_request_token == token {
                return Ok(AwsResponse::ok_json(configuration_json(existing)));
            }
            return Err(invalid_request(format!(
                "System table configuration {table_name} already exists"
            )));
        }
        let now = Utc::now();
        let cfg = SystemTableConfiguration {
            table_name: table_name.clone(),
            kms_key_arn,
            created_at: now,
            last_modified_at: now,
            client_request_token: token,
            tags,
        };
        let out = configuration_json(&cfg);
        account.system_table_configurations.insert(table_name, cfg);
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn get_system_table_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let table_name = require_table_name(&body)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cfg = account
            .system_table_configurations
            .get(&table_name)
            .ok_or_else(|| not_found(&table_name))?;
        Ok(AwsResponse::ok_json(configuration_json(cfg)))
    }

    pub(super) fn update_system_table_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let table_name = require_table_name(&body)?;
        let kms_key_arn = optional_kms_key_arn(&body)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cfg = account
            .system_table_configurations
            .get_mut(&table_name)
            .ok_or_else(|| not_found(&table_name))?;
        // KmsKeyArn is the only mutable setting; omitting it reverts the
        // system tables to Athena-managed encryption.
        cfg.kms_key_arn = kms_key_arn;
        cfg.last_modified_at = Utc::now();
        Ok(AwsResponse::ok_json(configuration_json(cfg)))
    }

    pub(super) fn delete_system_table_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let table_name = require_table_name(&body)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        // Idempotent: deleting an absent configuration is a no-op.
        account.system_table_configurations.remove(&table_name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_system_table_statuses(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxSystemTableStatusesCount @range(1,50);
        // NextToken targets Token @length(1,1024).
        let max = validate_max_results(&body, 1, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let all = account.system_table_statuses();
        let (page, next) =
            paginate_checked(&all, body.get("NextToken").and_then(Value::as_str), max)
                .map_err(|_| invalid_request("Invalid NextToken"))?;
        let mut resp = json!({ "Items": page });
        if let Some(t) = next {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "athena".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: Default::default(),
            query_params: Default::default(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(svc: &AthenaService, action: &str, body: Value) -> Result<Value, AwsServiceError> {
        let r = req(action, body);
        let resp = match action {
            "CreateSystemTableConfiguration" => svc.create_system_table_configuration(&r),
            "GetSystemTableConfiguration" => svc.get_system_table_configuration(&r),
            "UpdateSystemTableConfiguration" => svc.update_system_table_configuration(&r),
            "DeleteSystemTableConfiguration" => svc.delete_system_table_configuration(&r),
            "ListSystemTableStatuses" => svc.list_system_table_statuses(&r),
            "GetWorkGroup" => svc.get_work_group(&r),
            other => panic!("unexpected {other}"),
        }?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
    }

    const KEY: &str = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab";

    #[test]
    fn system_table_configuration_lifecycle_drives_statuses() {
        let svc = AthenaService::default();
        let statuses = call(&svc, "ListSystemTableStatuses", json!({})).unwrap();
        assert_eq!(
            statuses["Items"],
            json!([{"TableName": "QUERY_EXECUTIONS", "Enabled": false}])
        );
        let err = call(
            &svc,
            "GetSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ResourceNotFoundException");

        let created = call(
            &svc,
            "CreateSystemTableConfiguration",
            json!({"TableName": "ALL", "KmsKeyArn": KEY}),
        )
        .unwrap();
        assert_eq!(created["TableName"], "ALL");
        assert_eq!(created["KmsKeyArn"], KEY);

        let wg = call(&svc, "GetWorkGroup", json!({"WorkGroup": "primary"})).unwrap();
        assert_eq!(
            wg["WorkGroup"]["Configuration"]["SystemTableStatuses"],
            json!([{"TableName": "QUERY_EXECUTIONS", "Enabled": true}])
        );

        let dup = call(
            &svc,
            "CreateSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap_err();
        assert_eq!(dup.code(), "InvalidRequestException");

        let updated = call(
            &svc,
            "UpdateSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap();
        assert!(updated.get("KmsKeyArn").is_none());
        let got = call(
            &svc,
            "GetSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap();
        assert!(got.get("KmsKeyArn").is_none());
        assert_eq!(got["CreatedAt"], created["CreatedAt"]);

        call(
            &svc,
            "DeleteSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap();
        let statuses = call(&svc, "ListSystemTableStatuses", json!({})).unwrap();
        assert_eq!(statuses["Items"][0]["Enabled"], false);
        let err = call(
            &svc,
            "UpdateSystemTableConfiguration",
            json!({"TableName": "ALL"}),
        )
        .unwrap_err();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[test]
    fn create_is_idempotent_on_client_request_token() {
        let svc = AthenaService::default();
        let token = "a".repeat(32);
        let first = call(
            &svc,
            "CreateSystemTableConfiguration",
            json!({"TableName": "ALL", "ClientRequestToken": token}),
        )
        .unwrap();
        let again = call(
            &svc,
            "CreateSystemTableConfiguration",
            json!({"TableName": "ALL", "ClientRequestToken": token}),
        )
        .unwrap();
        assert_eq!(first, again);
    }

    #[test]
    fn rejects_invalid_input() {
        let svc = AthenaService::default();
        for body in [
            json!({}),
            json!({"TableName": "QUERY_EXECUTIONS"}),
            json!({"TableName": "ALL", "KmsKeyArn": "arn:aws:s3:::not-a-kms-key-arn"}),
            json!({"TableName": "ALL", "ClientRequestToken": "short"}),
        ] {
            let err = call(&svc, "CreateSystemTableConfiguration", body).unwrap_err();
            assert_eq!(err.code(), "InvalidRequestException");
        }
        let err = call(&svc, "ListSystemTableStatuses", json!({"MaxResults": 51})).unwrap_err();
        assert_eq!(err.code(), "InvalidRequestException");
    }
}
