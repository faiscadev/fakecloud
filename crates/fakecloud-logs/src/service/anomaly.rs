use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::LogsService;
use chrono::Utc;

use crate::state::{AnomalyDetector, LogAnomaly};

impl LogsService {
    // ---- Anomaly Detectors ----

    pub(crate) fn create_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("detectorName", body["detectorName"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "evaluationFrequency",
            &body["evaluationFrequency"],
            &[
                "ONE_MIN",
                "FIVE_MIN",
                "TEN_MIN",
                "FIFTEEN_MIN",
                "THIRTY_MIN",
                "ONE_HOUR",
            ],
        )?;
        validate_optional_string_length("filterPattern", body["filterPattern"].as_str(), 0, 1024)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;
        validate_optional_range_i64(
            "anomalyVisibilityTime",
            body["anomalyVisibilityTime"].as_i64(),
            7,
            90,
        )?;

        let log_group_arn_list = body["logGroupArnList"]
            .as_array()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupArnList is required",
                )
            })?
            .iter()
            .map(|v| {
                v.as_str().map(|s| s.to_string()).ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "logGroupArnList elements must be strings",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let detector_name = body["detectorName"].as_str().unwrap_or("").to_string();
        let evaluation_frequency = body["evaluationFrequency"].as_str().map(|s| s.to_string());
        let filter_pattern = body["filterPattern"].as_str().map(|s| s.to_string());
        let anomaly_visibility_time = body["anomalyVisibilityTime"].as_i64();
        let tags = body["tags"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let detector_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:anomaly-detector:{}",
            req.region, state.account_id, detector_id
        );

        let detector = AnomalyDetector {
            detector_name: detector_name.clone(),
            arn: arn.clone(),
            log_group_arn_list,
            evaluation_frequency,
            filter_pattern,
            anomaly_visibility_time,
            creation_time: now,
            last_modified_time: now,
            enabled: true,
            tags,
        };

        state.anomaly_detectors.insert(arn.clone(), detector);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalyDetectorArn": arn })).unwrap(),
        ))
    }

    pub(crate) fn get_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let detector = state.anomaly_detectors.get(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            )
        })?;

        let mut result = json!({
            "anomalyDetectorArn": detector.arn,
            "detectorName": detector.detector_name,
            "logGroupArnList": detector.log_group_arn_list,
            "creationTimeStamp": detector.creation_time,
            "lastModifiedTimeStamp": detector.last_modified_time,
            "anomalyDetectorStatus": if detector.enabled { "TRAINING" } else { "PAUSED" },
        });
        if let Some(ref f) = detector.evaluation_frequency {
            result["evaluationFrequency"] = json!(f);
        }
        if let Some(ref f) = detector.filter_pattern {
            result["filterPattern"] = json!(f);
        }
        if let Some(t) = detector.anomaly_visibility_time {
            result["anomalyVisibilityTime"] = json!(t);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn delete_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.anomaly_detectors.remove(arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_log_anomaly_detectors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length(
            "filterLogGroupArn",
            body["filterLogGroupArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let filter_log_group_arn = body["filterLogGroupArn"].as_str();
        let _limit = body["limit"].as_i64().unwrap_or(50);

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let detectors: Vec<Value> = state
            .anomaly_detectors
            .values()
            .filter(|d| {
                filter_log_group_arn.is_none_or(|arn| d.log_group_arn_list.iter().any(|a| a == arn))
            })
            .map(|d| {
                let mut obj = json!({
                    "anomalyDetectorArn": d.arn,
                    "detectorName": d.detector_name,
                    "logGroupArnList": d.log_group_arn_list,
                    "creationTimeStamp": d.creation_time,
                    "lastModifiedTimeStamp": d.last_modified_time,
                    "anomalyDetectorStatus": if d.enabled { "TRAINING" } else { "PAUSED" },
                });
                if let Some(ref f) = d.evaluation_frequency {
                    obj["evaluationFrequency"] = json!(f);
                }
                if let Some(ref f) = d.filter_pattern {
                    obj["filterPattern"] = json!(f);
                }
                if let Some(t) = d.anomaly_visibility_time {
                    obj["anomalyVisibilityTime"] = json!(t);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalyDetectors": detectors })).unwrap(),
        ))
    }

    pub(crate) fn update_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;
        validate_optional_enum_value(
            "evaluationFrequency",
            &body["evaluationFrequency"],
            &[
                "ONE_MIN",
                "FIVE_MIN",
                "TEN_MIN",
                "FIFTEEN_MIN",
                "THIRTY_MIN",
                "ONE_HOUR",
            ],
        )?;
        let enabled = body["enabled"].as_bool().unwrap_or(true);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let detector = state.anomaly_detectors.get_mut(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Anomaly detector not found: {arn}"),
            )
        })?;

        detector.enabled = enabled;
        if let Some(f) = body["evaluationFrequency"].as_str() {
            detector.evaluation_frequency = Some(f.to_string());
        }
        if let Some(f) = body["filterPattern"].as_str() {
            detector.filter_pattern = Some(f.to_string());
        }
        if let Some(t) = body["anomalyVisibilityTime"].as_i64() {
            detector.anomaly_visibility_time = Some(t);
        }
        detector.last_modified_time = Utc::now().timestamp_millis();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_anomalies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length(
            "anomalyDetectorArn",
            body["anomalyDetectorArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value(
            "suppressionState",
            &body["suppressionState"],
            &["SUPPRESSED", "UNSUPPRESSED"],
        )?;
        let detector_filter = body["anomalyDetectorArn"].as_str();
        let suppression = body["suppressionState"].as_str();

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let anomalies: Vec<Value> = state
            .anomalies
            .values()
            .filter(|a| {
                detector_filter.is_none_or(|d| a.anomaly_detector_arn == d)
                    && suppression.is_none_or(|s| match s {
                        "SUPPRESSED" => a.suppressed,
                        "UNSUPPRESSED" => !a.suppressed,
                        _ => true,
                    })
            })
            .map(|a| {
                json!({
                    "anomalyId": a.anomaly_id,
                    "anomalyDetectorArn": a.anomaly_detector_arn,
                    "logGroupArnList": a.log_group_arn_list,
                    "patternId": a.pattern_id,
                    "patternString": a.pattern_string,
                    "firstSeen": a.first_seen,
                    "lastSeen": a.last_seen,
                    "priority": a.priority,
                    "state": a.state,
                    "active": !a.suppressed,
                    "suppressed": a.suppressed,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalies": anomalies })).unwrap(),
        ))
    }

    pub(crate) fn update_anomaly(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("anomalyDetectorArn", &body["anomalyDetectorArn"])?;
        validate_optional_string_length(
            "anomalyDetectorArn",
            body["anomalyDetectorArn"].as_str(),
            1,
            2048,
        )?;
        validate_optional_string_length("anomalyId", body["anomalyId"].as_str(), 36, 36)?;
        validate_optional_string_length("patternId", body["patternId"].as_str(), 32, 32)?;
        validate_optional_enum_value(
            "suppressionType",
            &body["suppressionType"],
            &["LIMITED", "INFINITE"],
        )?;
        let anomaly_id = body["anomalyId"].as_str();
        let suppress = !body["suppressionType"].is_null();
        if let Some(id) = anomaly_id {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(a) = state.anomalies.get_mut(id) {
                a.suppressed = suppress;
            }
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // -- Import tasks --

    /// Admin: inject a synthetic anomaly so tests can exercise
    /// ListAnomalies / UpdateAnomaly without running real detection.
    /// Called from the `/_fakecloud/logs/anomalies/inject` endpoint.
    pub fn inject_anomaly(
        &self,
        account_id: &str,
        _region: &str,
        anomaly_detector_arn: String,
        log_group_arns: Vec<String>,
        pattern_string: String,
        priority: Option<String>,
    ) -> String {
        let now = Utc::now().timestamp_millis();
        let anomaly_id = uuid::Uuid::new_v4().to_string();
        let pattern_id = format!("{:032x}", uuid::Uuid::new_v4().as_u128());
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.anomalies.insert(
            anomaly_id.clone(),
            LogAnomaly {
                anomaly_id: anomaly_id.clone(),
                anomaly_detector_arn,
                log_group_arn_list: log_group_arns,
                pattern_id,
                pattern_string,
                first_seen: now,
                last_seen: now,
                priority: priority.unwrap_or_else(|| "MEDIUM".to_string()),
                state: "ACTIVE".to_string(),
                suppressed: false,
            },
        );
        anomaly_id
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Anomaly detectors ----

    #[test]
    fn anomaly_detector_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateLogAnomalyDetector",
            json!({
                "logGroupArnList": ["arn:aws:logs:us-east-1:123456789012:log-group:test:*"],
                "detectorName": "my-detector",
            }),
        );
        let resp = svc.create_log_anomaly_detector(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["anomalyDetectorArn"].as_str().unwrap().to_string();

        let req = make_request(
            "GetLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn }),
        );
        let resp = svc.get_log_anomaly_detector(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["detectorName"], "my-detector");

        let req = make_request("ListLogAnomalyDetectors", json!({}));
        let resp = svc.list_log_anomaly_detectors(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["anomalyDetectors"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn, "enabled": false }),
        );
        svc.update_log_anomaly_detector(&req).unwrap();

        let req = make_request(
            "DeleteLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn }),
        );
        svc.delete_log_anomaly_detector(&req).unwrap();
    }

    #[test]
    fn list_anomalies_returns_injected_entries() {
        let svc = make_service();
        let id = svc.inject_anomaly(
            "123456789012",
            "us-east-1",
            "arn:aws:logs:us-east-1:123456789012:anomaly-detector:abc".to_string(),
            vec!["arn:aws:logs:us-east-1:123456789012:log-group:test".to_string()],
            "ERROR pattern".to_string(),
            None,
        );

        let req = make_request("ListAnomalies", json!({}));
        let resp = svc.list_anomalies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arr = body["anomalies"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["anomalyId"], id);
        assert_eq!(arr[0]["patternString"], "ERROR pattern");
        assert_eq!(arr[0]["suppressed"], false);

        let req = make_request(
            "UpdateAnomaly",
            json!({
                "anomalyDetectorArn": "arn:aws:logs:us-east-1:123456789012:anomaly-detector:abc",
                "anomalyId": id,
                "suppressionType": "INFINITE",
            }),
        );
        svc.update_anomaly(&req).unwrap();

        let req = make_request("ListAnomalies", json!({"suppressionState": "SUPPRESSED"}));
        let resp = svc.list_anomalies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["anomalies"].as_array().unwrap().len(), 1);
    }
}
