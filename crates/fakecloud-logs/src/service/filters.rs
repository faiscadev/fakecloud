use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, matches_filter_pattern, validation_error, LogsService};
use chrono::Utc;

use crate::state::{MetricFilter, MetricTransformation, SubscriptionFilter};

impl LogsService {
    // ---- Subscription Filters ----

    pub(crate) fn put_subscription_filter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let filter_name = body["filterName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "filterName is required",
                )
            })?
            .to_string();
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("").to_string();
        let destination_arn = body["destinationArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "destinationArn is required",
                )
            })?
            .to_string();
        let role_arn = body["roleArn"].as_str().map(|s| s.to_string());
        let distribution = body["distribution"]
            .as_str()
            .unwrap_or("ByLogStream")
            .to_string();

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_string_length("filterName", &filter_name, 1, 512)?;
        validate_optional_string_length("filterPattern", Some(&filter_pattern), 0, 1024)?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        // Check if updating existing filter
        if let Some(existing) = group
            .subscription_filters
            .iter_mut()
            .find(|f| f.filter_name == filter_name)
        {
            existing.filter_pattern = filter_pattern;
            existing.destination_arn = destination_arn;
            existing.role_arn = role_arn;
            existing.distribution = distribution;
            return Ok(AwsResponse::json(StatusCode::OK, "{}"));
        }

        // Max 2 subscription filters per log group
        if group.subscription_filters.len() >= 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                "Resource limit exceeded.",
            ));
        }

        let now = Utc::now().timestamp_millis();
        group.subscription_filters.push(SubscriptionFilter {
            filter_name,
            log_group_name: log_group_name.to_string(),
            filter_pattern,
            destination_arn,
            role_arn,
            distribution,
            creation_time: now,
        });

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_subscription_filters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length(
            "filterNamePrefix",
            body["filterNamePrefix"].as_str(),
            1,
            512,
        )?;

        let state = self.state.read();
        let group = state.log_groups.get(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        let filters: Vec<Value> = group
            .subscription_filters
            .iter()
            .map(|f| {
                let mut obj = json!({
                    "filterName": f.filter_name,
                    "logGroupName": f.log_group_name,
                    "filterPattern": f.filter_pattern,
                    "destinationArn": f.destination_arn,
                    "distribution": f.distribution,
                    "creationTime": f.creation_time,
                });
                if let Some(ref arn) = f.role_arn {
                    obj["roleArn"] = json!(arn);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "subscriptionFilters": filters })).unwrap(),
        ))
    }

    pub(crate) fn delete_subscription_filter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let filter_name = body["filterName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterName is required",
            )
        })?;

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_string_length("filterName", filter_name, 1, 512)?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(log_group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        })?;

        let idx = group
            .subscription_filters
            .iter()
            .position(|f| f.filter_name == filter_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified subscription filter does not exist.",
                )
            })?;

        group.subscription_filters.remove(idx);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Metric Filters ----

    pub(crate) fn put_metric_filter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name = body["filterName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "filterName is required",
                )
            })?
            .to_string();
        validate_required("filterPattern", &body["filterPattern"])?;
        let filter_pattern = body["filterPattern"].as_str().unwrap_or("").to_string();
        let log_group_name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();

        validate_string_length("filterName", &filter_name, 1, 512)?;
        validate_string_length("logGroupName", &log_group_name, 1, 512)?;
        validate_optional_string_length("filterPattern", Some(&filter_pattern), 0, 1024)?;
        validate_optional_string_length(
            "fieldSelectionCriteria",
            body["fieldSelectionCriteria"].as_str(),
            0,
            2000,
        )?;

        let transformations_json = body["metricTransformations"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "metricTransformations is required",
            )
        })?;

        // Validate max 1 transformation
        if transformations_json.len() > 1 {
            return Err(validation_error(
                "metricTransformations",
                &format!("{}", transformations_json.len()),
                "Member must have length less than or equal to 1",
            ));
        }

        let transformations: Vec<MetricTransformation> = transformations_json
            .iter()
            .map(|t| MetricTransformation {
                metric_name: t["metricName"].as_str().unwrap_or("").to_string(),
                metric_namespace: t["metricNamespace"].as_str().unwrap_or("").to_string(),
                metric_value: t["metricValue"].as_str().unwrap_or("").to_string(),
                default_value: t["defaultValue"].as_f64(),
            })
            .collect();

        let now = Utc::now().timestamp_millis();

        let mut state = self.state.write();

        // Update existing or add new
        if let Some(existing) = state
            .metric_filters
            .iter_mut()
            .find(|f| f.filter_name == filter_name && f.log_group_name == log_group_name)
        {
            existing.filter_pattern = filter_pattern;
            existing.metric_transformations = transformations;
        } else {
            state.metric_filters.push(MetricFilter {
                filter_name,
                filter_pattern,
                log_group_name,
                metric_transformations: transformations,
                creation_time: now,
            });
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_metric_filters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name_prefix = body["filterNamePrefix"].as_str();
        let log_group_name = body["logGroupName"].as_str();
        let metric_name = body["metricName"].as_str();
        let metric_namespace = body["metricNamespace"].as_str();

        validate_optional_string_length("filterNamePrefix", filter_name_prefix, 1, 512)?;
        validate_optional_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length("metricName", metric_name, 0, 255)?;
        validate_optional_string_length("metricNamespace", metric_namespace, 0, 255)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let filters: Vec<Value> = state
            .metric_filters
            .iter()
            .filter(|f| {
                if let Some(prefix) = filter_name_prefix {
                    if !f.filter_name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(lg) = log_group_name {
                    if f.log_group_name != lg {
                        return false;
                    }
                }
                if let Some(mn) = metric_name {
                    if !f.metric_transformations.iter().any(|t| t.metric_name == mn) {
                        return false;
                    }
                }
                if let Some(ns) = metric_namespace {
                    if !f
                        .metric_transformations
                        .iter()
                        .any(|t| t.metric_namespace == ns)
                    {
                        return false;
                    }
                }
                true
            })
            .map(|f| {
                let transformations: Vec<Value> = f
                    .metric_transformations
                    .iter()
                    .map(|t| {
                        let mut obj = json!({
                            "metricName": t.metric_name,
                            "metricNamespace": t.metric_namespace,
                            "metricValue": t.metric_value,
                        });
                        if let Some(dv) = t.default_value {
                            obj["defaultValue"] = json!(dv);
                        }
                        obj
                    })
                    .collect();

                json!({
                    "filterName": f.filter_name,
                    "filterPattern": f.filter_pattern,
                    "logGroupName": f.log_group_name,
                    "metricTransformations": transformations,
                    "creationTime": f.creation_time,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "metricFilters": filters })).unwrap(),
        ))
    }

    pub(crate) fn delete_metric_filter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_name = body["filterName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterName is required",
            )
        })?;
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("filterName", filter_name, 1, 512)?;
        validate_string_length("logGroupName", log_group_name, 1, 512)?;

        let mut state = self.state.write();
        let idx = state
            .metric_filters
            .iter()
            .position(|f| f.filter_name == filter_name && f.log_group_name == log_group_name);

        if let Some(i) = idx {
            state.metric_filters.remove(i);
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn test_metric_filter(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let filter_pattern = body["filterPattern"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "filterPattern is required",
            )
        })?;
        validate_string_length("filterPattern", filter_pattern, 0, 1024)?;
        let log_event_messages = body["logEventMessages"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEventMessages is required",
            )
        })?;

        let matches: Vec<Value> = log_event_messages
            .iter()
            .enumerate()
            .filter(|(_, msg)| {
                let msg_str = msg.as_str().unwrap_or("");
                matches_filter_pattern(filter_pattern, msg_str)
            })
            .map(|(i, msg)| {
                json!({
                    "eventNumber": i + 1,
                    "eventMessage": msg,
                    "extractedValues": {},
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "matches": matches })).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Subscription filters ----

    #[test]
    fn subscription_filter_lifecycle() {
        let svc = make_service();
        create_group(&svc, "sub-grp");

        let req = make_request(
            "PutSubscriptionFilter",
            json!({
                "logGroupName": "sub-grp",
                "filterName": "my-filter",
                "filterPattern": "ERROR",
                "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
            }),
        );
        svc.put_subscription_filter(&req).unwrap();

        let req = make_request(
            "DescribeSubscriptionFilters",
            json!({ "logGroupName": "sub-grp" }),
        );
        let resp = svc.describe_subscription_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let filters = body["subscriptionFilters"].as_array().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0]["filterName"], "my-filter");
        assert_eq!(filters[0]["filterPattern"], "ERROR");
        assert_eq!(
            filters[0]["destinationArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        );
        assert_eq!(filters[0]["distribution"], "ByLogStream");

        // Delete
        let req = make_request(
            "DeleteSubscriptionFilter",
            json!({ "logGroupName": "sub-grp", "filterName": "my-filter" }),
        );
        svc.delete_subscription_filter(&req).unwrap();

        let req = make_request(
            "DescribeSubscriptionFilters",
            json!({ "logGroupName": "sub-grp" }),
        );
        let resp = svc.describe_subscription_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["subscriptionFilters"].as_array().unwrap().is_empty());
    }

    #[test]
    fn subscription_filter_update_existing() {
        let svc = make_service();
        create_group(&svc, "sub-upd");

        let req = make_request(
            "PutSubscriptionFilter",
            json!({
                "logGroupName": "sub-upd",
                "filterName": "f1",
                "filterPattern": "",
                "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:old",
            }),
        );
        svc.put_subscription_filter(&req).unwrap();

        // Update the same filter
        let req = make_request(
            "PutSubscriptionFilter",
            json!({
                "logGroupName": "sub-upd",
                "filterName": "f1",
                "filterPattern": "WARN",
                "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:new",
            }),
        );
        svc.put_subscription_filter(&req).unwrap();

        let req = make_request(
            "DescribeSubscriptionFilters",
            json!({ "logGroupName": "sub-upd" }),
        );
        let resp = svc.describe_subscription_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let filters = body["subscriptionFilters"].as_array().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0]["filterPattern"], "WARN");
    }

    #[test]
    fn subscription_filter_limit_exceeded() {
        let svc = make_service();
        create_group(&svc, "sub-limit");

        for i in 0..2 {
            let req = make_request(
                "PutSubscriptionFilter",
                json!({
                    "logGroupName": "sub-limit",
                    "filterName": format!("f{i}"),
                    "filterPattern": "",
                    "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
                }),
            );
            svc.put_subscription_filter(&req).unwrap();
        }

        // Third should fail
        let req = make_request(
            "PutSubscriptionFilter",
            json!({
                "logGroupName": "sub-limit",
                "filterName": "f2",
                "filterPattern": "",
                "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            }),
        );
        assert!(svc.put_subscription_filter(&req).is_err());
    }

    #[test]
    fn delete_subscription_filter_nonexistent_errors() {
        let svc = make_service();
        create_group(&svc, "sub-del");

        let req = make_request(
            "DeleteSubscriptionFilter",
            json!({ "logGroupName": "sub-del", "filterName": "nope" }),
        );
        assert!(svc.delete_subscription_filter(&req).is_err());
    }

    // ---- Metric filters ----

    #[test]
    fn metric_filter_lifecycle() {
        let svc = make_service();
        create_group(&svc, "mf-grp");

        let req = make_request(
            "PutMetricFilter",
            json!({
                "logGroupName": "mf-grp",
                "filterName": "err-count",
                "filterPattern": "ERROR",
                "metricTransformations": [{
                    "metricName": "ErrorCount",
                    "metricNamespace": "MyApp",
                    "metricValue": "1",
                }],
            }),
        );
        svc.put_metric_filter(&req).unwrap();

        // Describe by log group
        let req = make_request("DescribeMetricFilters", json!({ "logGroupName": "mf-grp" }));
        let resp = svc.describe_metric_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let filters = body["metricFilters"].as_array().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0]["filterName"], "err-count");
        assert_eq!(
            filters[0]["metricTransformations"][0]["metricName"],
            "ErrorCount"
        );

        // Describe by metric name
        let req = make_request(
            "DescribeMetricFilters",
            json!({ "metricName": "ErrorCount", "metricNamespace": "MyApp" }),
        );
        let resp = svc.describe_metric_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["metricFilters"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(
            "DeleteMetricFilter",
            json!({ "logGroupName": "mf-grp", "filterName": "err-count" }),
        );
        svc.delete_metric_filter(&req).unwrap();

        let req = make_request("DescribeMetricFilters", json!({ "logGroupName": "mf-grp" }));
        let resp = svc.describe_metric_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["metricFilters"].as_array().unwrap().is_empty());
    }

    #[test]
    fn metric_filter_update_existing() {
        let svc = make_service();
        create_group(&svc, "mf-upd");

        let req = make_request(
            "PutMetricFilter",
            json!({
                "logGroupName": "mf-upd",
                "filterName": "mf1",
                "filterPattern": "ERROR",
                "metricTransformations": [{
                    "metricName": "M1",
                    "metricNamespace": "NS",
                    "metricValue": "1",
                }],
            }),
        );
        svc.put_metric_filter(&req).unwrap();

        // Update same filter
        let req = make_request(
            "PutMetricFilter",
            json!({
                "logGroupName": "mf-upd",
                "filterName": "mf1",
                "filterPattern": "WARN",
                "metricTransformations": [{
                    "metricName": "M1",
                    "metricNamespace": "NS",
                    "metricValue": "1",
                    "defaultValue": 0.0,
                }],
            }),
        );
        svc.put_metric_filter(&req).unwrap();

        let req = make_request("DescribeMetricFilters", json!({ "logGroupName": "mf-upd" }));
        let resp = svc.describe_metric_filters(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let filters = body["metricFilters"].as_array().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0]["filterPattern"], "WARN");
        assert_eq!(filters[0]["metricTransformations"][0]["defaultValue"], 0.0);
    }
}
