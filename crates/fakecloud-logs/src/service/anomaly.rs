use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, LogsService};
use chrono::Utc;

use crate::state::AnomalyDetector;

impl LogsService {
    // ---- Anomaly Detectors ----

    pub(crate) fn create_log_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
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

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let detector_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:logs:{}:{}:anomaly-detector:{}",
            state.region, state.account_id, detector_id
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
        let body = body_json(req);
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let state = self.state.read();
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
        let body = body_json(req);
        let arn = body["anomalyDetectorArn"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "anomalyDetectorArn is required",
            )
        })?;

        let mut state = self.state.write();
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
        let body = body_json(req);
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

        let state = self.state.read();
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
        let body = body_json(req);
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

        let mut state = self.state.write();
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
        let body = body_json(req);
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
        // Stub: return empty anomalies list
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "anomalies": [] })).unwrap(),
        ))
    }

    pub(crate) fn update_anomaly(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        // No-op stub
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // -- Import tasks --
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["anomalyDetectorArn"].as_str().unwrap().to_string();

        let req = make_request(
            "GetLogAnomalyDetector",
            json!({ "anomalyDetectorArn": &arn }),
        );
        let resp = svc.get_log_anomaly_detector(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["detectorName"], "my-detector");

        let req = make_request("ListLogAnomalyDetectors", json!({}));
        let resp = svc.list_log_anomaly_detectors(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
}
