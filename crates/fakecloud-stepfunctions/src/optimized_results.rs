//! The results of optimized integrations, as recorded from AWS: the SDK metadata
//! most of them report, and the shapes of nested Step Functions executions.

use serde_json::{json, Map, Value};

use crate::sdk_protocol::{DateStyle, SdkService};

/// How a `.sync` Task presents the description of a finished child execution.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncForm {
    /// `.sync`: `Input` and `Output` stay escaped JSON strings.
    Escaped,
    /// `.sync:2`: `Input` and `Output` are JSON.
    Json,
}

/// Parse an escaped-JSON member in place; text that is not JSON stays as is.
fn parse_json_member(result: &mut Map<String, Value>, member: &str) {
    let parsed = result
        .get(member)
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str::<Value>(text).ok());
    if let Some(parsed) = parsed {
        result.insert(member.to_string(), parsed);
    }
}

fn http_date() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// Add the metadata of the SDK call behind an integration: its response headers,
/// status and request id.
fn add_metadata(result: &mut Value, request_id: &str, headers: Vec<(&str, String)>) {
    let mut all = Map::new();
    let mut single = Map::new();
    for (name, value) in headers {
        all.insert(name.to_string(), json!([value]));
        single.insert(name.to_string(), json!(value));
    }
    if !result.is_object() {
        *result = json!({});
    }
    result["SdkHttpMetadata"] = json!({
        "AllHttpHeaders": all,
        "HttpHeaders": single,
        "HttpStatusCode": 200,
    });
    result["SdkResponseMetadata"] = json!({ "RequestId": request_id });
}

/// A JSON-protocol service's headers: the casing of `connection` varies by
/// service, as recorded.
fn json_service_metadata(result: &mut Value, connection: &str, length: usize, content_type: &str) {
    let request_id = uuid::Uuid::new_v4().to_string();
    add_metadata(
        result,
        &request_id,
        vec![
            ("x-amzn-RequestId", request_id.clone()),
            (connection, "keep-alive".to_string()),
            ("Content-Length", length.to_string()),
            ("Date", http_date()),
            ("Content-Type", content_type.to_string()),
        ],
    );
}

/// `lambda:invoke`: `result` already holds the invocation's envelope.
pub(crate) fn lambda_invoke_metadata(result: &mut Value) {
    let request_id = uuid::Uuid::new_v4().to_string();
    let length = result["Payload"].to_string().len().to_string();
    let hex = uuid::Uuid::new_v4().simple().to_string();
    let trace = format!(
        "Root=1-{:08x}-{};Parent={};Sampled=0;Lineage=1:{}:0",
        chrono::Utc::now().timestamp(),
        &hex[..24],
        &hex[8..24],
        &hex[24..32]
    );
    add_metadata(
        result,
        &request_id,
        vec![
            ("X-Amz-Executed-Version", "$LATEST".to_string()),
            ("x-amzn-Remapped-Content-Length", "0".to_string()),
            ("Connection", "keep-alive".to_string()),
            ("x-amzn-RequestId", request_id.clone()),
            ("Content-Length", length),
            ("Date", http_date()),
            ("X-Amzn-Trace-Id", trace),
            ("Content-Type", "application/json".to_string()),
        ],
    );
}

/// `sqs:sendMessage`.
pub(crate) fn sqs_metadata(result: &mut Value) {
    let length = result.to_string().len();
    json_service_metadata(result, "connection", length, "application/x-amz-json-1.0");
}

/// `sns:publish`, whose response is a fixed-size XML document.
pub(crate) fn sns_metadata(result: &mut Value) {
    json_service_metadata(result, "connection", 294, "text/xml");
}

/// `dynamodb:*`, with its own request id form and a body checksum.
pub(crate) fn dynamodb_metadata(result: &mut Value) {
    let body = result.to_string();
    let hex = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let request_id = hex[..52].to_ascii_uppercase();
    add_metadata(
        result,
        &request_id,
        vec![
            ("Server", "Server".to_string()),
            ("x-amzn-requestid", request_id.clone()),
            ("Connection", "keep-alive".to_string()),
            ("x-amz-crc32", crc32fast::hash(body.as_bytes()).to_string()),
            ("Content-Length", body.len().to_string()),
            ("Date", http_date()),
            ("Content-Type", "application/x-amz-json-1.0".to_string()),
        ],
    );
}

/// `glue:startJobRun`, which also echoes the job's name.
pub(crate) fn glue_start_job_run_result(
    service: &SdkService,
    response: &Value,
    job_name: &Value,
) -> Value {
    let mut result = service.result_json("StartJobRun", response, DateStyle::EpochMillis);
    let request_id = uuid::Uuid::new_v4().to_string();
    add_metadata(
        &mut result,
        &request_id,
        vec![
            ("Cache-Control", "no-cache".to_string()),
            ("Connection", "keep-alive".to_string()),
            ("x-amzn-RequestId", request_id.clone()),
            ("Content-Length", response.to_string().len().to_string()),
            ("Date", http_date()),
            ("Content-Type", "application/x-amz-json-1.1".to_string()),
        ],
    );
    result["JobName"] = job_name.clone();
    result
}

/// The result of `states:startExecution`.
pub(crate) fn start_execution_result(service: &SdkService, response: &Value) -> Value {
    let mut result = service.result_json("StartExecution", response, DateStyle::EpochMillis);
    json_service_metadata(
        &mut result,
        "connection",
        response.to_string().len(),
        "application/x-amz-json-1.0",
    );
    result
}

/// The cause a `.sync[:2]` Task fails with when its child did: the child's
/// description as JSON text, for both forms.
pub(crate) fn sync_failure_cause(service: &SdkService, described: &Value) -> String {
    let Value::Object(mut result) =
        service.result_json("DescribeExecution", described, DateStyle::EpochMillis)
    else {
        return described.to_string();
    };
    result.remove("BillingDetails");
    result.insert("InputDetails".to_string(), json!({"Included": true}));
    result.insert("RedriveCount".to_string(), json!(0));
    result.insert("RedriveStatus".to_string(), json!("REDRIVABLE"));
    Value::Object(result).to_string()
}

/// The result of `states:startExecution.sync[:2]` for a child that succeeded.
pub(crate) fn sync_execution_result(
    service: &SdkService,
    described: &Value,
    form: SyncForm,
) -> Value {
    let Value::Object(mut result) =
        service.result_json("DescribeExecution", described, DateStyle::EpochMillis)
    else {
        return described.clone();
    };
    result.remove("BillingDetails");
    result.insert("InputDetails".to_string(), json!({"Included": true}));
    result.insert("OutputDetails".to_string(), json!({"Included": true}));
    result.insert("RedriveCount".to_string(), json!(0));
    result.insert("RedriveStatus".to_string(), json!("NOT_REDRIVABLE"));
    result.insert(
        "RedriveStatusReason".to_string(),
        json!("Execution is SUCCEEDED and cannot be redriven"),
    );
    if form == SyncForm::Json {
        parse_json_member(&mut result, "Input");
        parse_json_member(&mut result, "Output");
    }
    Value::Object(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sfn() -> std::sync::Arc<SdkService> {
        crate::sdk_protocol::service("sfn").unwrap()
    }

    fn describe_response() -> Value {
        json!({
            "executionArn": "e",
            "stateMachineArn": "s",
            "name": "n",
            "startDate": 10.5,
            "stopDate": 11,
            "status": "SUCCEEDED",
            "input": "{\"a\":1}",
            "output": "{\"b\":2}",
            "redriveCount": 0,
            "billingDetails": {"billedMemoryUsedInMB": 64}
        })
    }

    /// The exact key set AWS returned for `nested_sync`.
    #[test]
    fn sync_result_matches_the_recording() {
        let result = sync_execution_result(&sfn(), &describe_response(), SyncForm::Escaped);
        let mut keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "ExecutionArn",
                "Input",
                "InputDetails",
                "Name",
                "Output",
                "OutputDetails",
                "RedriveCount",
                "RedriveStatus",
                "RedriveStatusReason",
                "StartDate",
                "StateMachineArn",
                "Status",
                "StopDate"
            ]
        );
        assert_eq!(result["StartDate"], 10_500);
        assert_eq!(result["Input"], "{\"a\":1}");
        assert_eq!(result["Output"], "{\"b\":2}");
        assert_eq!(result["RedriveStatus"], "NOT_REDRIVABLE");
    }

    #[test]
    fn sync2_result_parses_input_and_output() {
        let result = sync_execution_result(&sfn(), &describe_response(), SyncForm::Json);
        assert_eq!(result["Input"], json!({"a": 1}));
        assert_eq!(result["Output"], json!({"b": 2}));
    }

    #[test]
    fn sync2_leaves_non_json_output_alone() {
        let result = sync_execution_result(&sfn(), &json!({"output": "not json"}), SyncForm::Json);
        assert_eq!(result["Output"], "not json");
    }

    #[test]
    fn start_execution_result_carries_sdk_metadata() {
        let result = start_execution_result(
            &sfn(),
            &json!({"executionArn": "e", "startDate": 1794229080.058}),
        );
        assert_eq!(result["ExecutionArn"], "e");
        assert_eq!(result["StartDate"], 1794229080058i64);
        assert_eq!(result["SdkHttpMetadata"]["HttpStatusCode"], 200);
        let id = &result["SdkResponseMetadata"]["RequestId"];
        assert_eq!(
            &result["SdkHttpMetadata"]["HttpHeaders"]["x-amzn-RequestId"],
            id
        );
        assert_eq!(
            &result["SdkHttpMetadata"]["AllHttpHeaders"]["x-amzn-RequestId"][0],
            id
        );
    }

    /// The exact key set AWS returned for `sync_child_fail`.
    #[test]
    fn sync_failure_cause_matches_the_recording() {
        let cause = sync_failure_cause(
            &sfn(),
            &json!({
                "executionArn": "e",
                "stateMachineArn": "s",
                "name": "n",
                "startDate": 10.5,
                "stopDate": 11,
                "status": "FAILED",
                "input": "{}",
                "error": "ChildBoom",
                "cause": "child cause",
                "redriveCount": 0,
                "billingDetails": {"billedMemoryUsedInMB": 64}
            }),
        );
        let cause: Value = serde_json::from_str(&cause).unwrap();
        let mut keys: Vec<&str> = cause
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "Cause",
                "Error",
                "ExecutionArn",
                "Input",
                "InputDetails",
                "Name",
                "RedriveCount",
                "RedriveStatus",
                "StartDate",
                "StateMachineArn",
                "Status",
                "StopDate"
            ]
        );
        assert_eq!(cause["RedriveStatus"], "REDRIVABLE");
        assert_eq!(cause["StartDate"], 10_500);
        assert_eq!(cause["Input"], "{}");
        assert_eq!(cause["Error"], "ChildBoom");
    }

    #[test]
    fn dynamodb_metadata_has_its_own_id_form_and_checksum() {
        let mut result = json!({"Item": {"k": {"S": "a"}}});
        let body = result.to_string();
        dynamodb_metadata(&mut result);
        let headers = &result["SdkHttpMetadata"]["HttpHeaders"];
        assert_eq!(headers["Server"], "Server");
        assert_eq!(
            headers["x-amz-crc32"],
            crc32fast::hash(body.as_bytes()).to_string()
        );
        let id = headers["x-amzn-requestid"].as_str().unwrap();
        assert_eq!(id.len(), 52);
        assert_eq!(result["SdkResponseMetadata"]["RequestId"], id);
        assert_eq!(result["Item"]["k"]["S"], "a");
    }

    #[test]
    fn lambda_metadata_describes_the_payload() {
        let mut result =
            json!({"ExecutedVersion": "$LATEST", "Payload": {"ok": true}, "StatusCode": 200});
        lambda_invoke_metadata(&mut result);
        let headers = &result["SdkHttpMetadata"]["AllHttpHeaders"];
        assert_eq!(headers["Content-Length"][0], "11");
        assert_eq!(headers["X-Amz-Executed-Version"][0], "$LATEST");
        assert!(headers["X-Amzn-Trace-Id"][0]
            .as_str()
            .unwrap()
            .starts_with("Root=1-"));
    }

    #[test]
    fn glue_result_echoes_the_job_name() {
        let glue = crate::sdk_protocol::service("glue").unwrap();
        let result = glue_start_job_run_result(&glue, &json!({"JobRunId": "jr_1"}), &json!("job"));
        assert_eq!(result["JobRunId"], "jr_1");
        assert_eq!(result["JobName"], "job");
        assert_eq!(
            result["SdkHttpMetadata"]["HttpHeaders"]["Content-Type"],
            "application/x-amz-json-1.1"
        );
    }
}
