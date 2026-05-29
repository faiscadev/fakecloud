//! OTel enrichment toggle, alarm contributors, and metric widget image.

use fakecloud_core::query::optional_query_param;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{missing_param, not_found, xml_response, CloudWatchService};

/// A tiny, valid 1x1 transparent PNG (raw bytes). GetMetricWidgetImage
/// returns a `MetricWidgetImage` blob; this is a deterministic placeholder
/// image rather than a stubbed string.
const TINY_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
    0x42, 0x60, 0x82,
];

impl CloudWatchService {
    pub(crate) fn get_otel_enrichment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let running = state
            .get(&req.account_id)
            .map(|a| a.otel_enrichment_running)
            .unwrap_or(false);
        let status = if running { "RUNNING" } else { "STOPPED" };
        let inner = format!("<Status>{status}</Status>");
        Ok(xml_response("GetOTelEnrichment", &inner, &req.request_id))
    }

    pub(crate) fn start_otel_enrichment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        state.get_or_create(&req.account_id).otel_enrichment_running = true;
        Ok(xml_response("StartOTelEnrichment", "", &req.request_id))
    }

    pub(crate) fn stop_otel_enrichment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        state.get_or_create(&req.account_id).otel_enrichment_running = false;
        Ok(xml_response("StopOTelEnrichment", "", &req.request_id))
    }

    pub(crate) fn describe_alarm_contributors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // AlarmName is required and the op declares ResourceNotFoundException;
        // reject missing / unknown alarms with that declared error.
        let alarm_name = optional_query_param(req, "AlarmName")
            .ok_or_else(|| not_found("Alarm does not exist"))?;
        let state = self.state.read();
        let exists = state
            .get(&req.account_id)
            .map(|a| {
                a.alarms_in(&req.region)
                    .map(|m| m.contains_key(&alarm_name))
                    .unwrap_or(false)
                    || a.composite_alarms_in(&req.region)
                        .map(|m| m.contains_key(&alarm_name))
                        .unwrap_or(false)
            })
            .unwrap_or(false);
        if !exists {
            return Err(not_found(format!("Alarm {alarm_name} does not exist")));
        }
        // No live contributor evaluation; return an empty contributor list.
        let inner = String::from("<AlarmContributors/>");
        Ok(xml_response(
            "DescribeAlarmContributors",
            &inner,
            &req.request_id,
        ))
    }

    pub(crate) fn get_metric_widget_image(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // No declared errors, but the omitted-required negative variant still
        // expects a 4xx (accepted as AnyError). A well-formed request returns a
        // deterministic image.
        if optional_query_param(req, "MetricWidget").is_none() {
            return Err(missing_param("MetricWidget"));
        }
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(TINY_PNG);
        let inner = format!("<MetricWidgetImage>{b64}</MetricWidgetImage>");
        Ok(xml_response(
            "GetMetricWidgetImage",
            &inner,
            &req.request_id,
        ))
    }
}
