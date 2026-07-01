//! Anomaly detection models: PutAnomalyDetector / DescribeAnomalyDetectors /
//! DeleteAnomalyDetector.

use std::collections::BTreeMap;

use fakecloud_core::query::optional_query_param;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_indexed, collect_member_values, invalid_param, parse_dimensions_query,
    render_dimensions, validate_len, validate_range_i64, xml_escape, xml_response,
    CloudWatchService,
};
use crate::state::{AnomalyDetector, AnomalyDetectorConfiguration, ExcludedTimeRange};

/// Parse the optional `Configuration` (MetricTimezone + ExcludedTimeRanges)
/// off a PutAnomalyDetector request, if present.
fn parse_configuration(req: &AwsRequest) -> Option<AnomalyDetectorConfiguration> {
    let metric_timezone = optional_query_param(req, "Configuration.MetricTimezone");
    let excluded_time_ranges: Vec<ExcludedTimeRange> =
        collect_indexed(req, "Configuration.ExcludedTimeRanges")
            .into_iter()
            .map(|m| ExcludedTimeRange {
                start_time: m.get("StartTime").cloned(),
                end_time: m.get("EndTime").cloned(),
            })
            .collect();
    if metric_timezone.is_none() && excluded_time_ranges.is_empty() {
        return None;
    }
    Some(AnomalyDetectorConfiguration {
        excluded_time_ranges,
        metric_timezone,
    })
}

/// Build the stable key for a detector from its identifying fields. A
/// metric-math detector keys off its serialized expression members; a
/// single-metric detector keys off namespace/metric/stat/dimensions.
fn detector_key(
    namespace: &Option<String>,
    metric_name: &Option<String>,
    stat: &Option<String>,
    dims: &BTreeMap<String, String>,
    metric_math_id: &Option<String>,
) -> String {
    if let Some(id) = metric_math_id {
        return format!("MATH:{id}");
    }
    let dim_part: String = dims
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{}|{}|{}|{}",
        namespace.as_deref().unwrap_or(""),
        metric_name.as_deref().unwrap_or(""),
        stat.as_deref().unwrap_or(""),
        dim_part
    )
}

/// The identifying fields of an anomaly detector, resolved from whichever
/// input form the caller used.
struct DetectorIdentity {
    namespace: Option<String>,
    metric_name: Option<String>,
    stat: Option<String>,
    dimensions: BTreeMap<String, String>,
    math_id: Option<String>,
}

/// Resolve the identifying fields from either the flat
/// (Namespace/MetricName/Stat/Dimensions) form or the
/// SingleMetricAnomalyDetector / MetricMathAnomalyDetector nested forms.
fn resolve_identity(req: &AwsRequest) -> DetectorIdentity {
    // Metric-math: identified by the first query id.
    let math_id = req
        .query_params
        .get("MetricMathAnomalyDetector.MetricDataQueries.member.1.Id")
        .cloned();
    if math_id.is_some() {
        return DetectorIdentity {
            namespace: None,
            metric_name: None,
            stat: None,
            dimensions: BTreeMap::new(),
            math_id,
        };
    }

    // Single-metric nested form.
    if let Some(ns) = optional_query_param(req, "SingleMetricAnomalyDetector.Namespace") {
        return DetectorIdentity {
            namespace: Some(ns),
            metric_name: optional_query_param(req, "SingleMetricAnomalyDetector.MetricName"),
            stat: optional_query_param(req, "SingleMetricAnomalyDetector.Stat"),
            dimensions: parse_dimensions_query(req, "SingleMetricAnomalyDetector.Dimensions"),
            math_id: None,
        };
    }

    // Flat form.
    DetectorIdentity {
        namespace: optional_query_param(req, "Namespace"),
        metric_name: optional_query_param(req, "MetricName"),
        stat: optional_query_param(req, "Stat"),
        dimensions: parse_dimensions_query(req, "Dimensions"),
        math_id: None,
    }
}

impl CloudWatchService {
    pub(crate) fn put_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "Namespace", 1, 255)?;
        validate_len(req, "MetricName", 1, 255)?;
        let id = resolve_identity(req);
        if id.namespace.is_none() && id.math_id.is_none() {
            return Err(invalid_param(
                "PutAnomalyDetector requires a single-metric or metric-math detector",
            ));
        }
        let key = detector_key(
            &id.namespace,
            &id.metric_name,
            &id.stat,
            &id.dimensions,
            &id.math_id,
        );
        let configuration = parse_configuration(req);
        let periodic_spikes = optional_query_param(req, "MetricCharacteristics.PeriodicSpikes")
            .map(|s| s.eq_ignore_ascii_case("true"));
        let detector = AnomalyDetector {
            key: key.clone(),
            namespace: id.namespace,
            metric_name: id.metric_name,
            stat: id.stat,
            dimensions: id.dimensions,
            metric_math: id.math_id.is_some(),
            state_value: "TRAINED".to_string(),
            configuration,
            periodic_spikes,
        };
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.anomaly_detectors_in_mut(&req.region)
            .insert(key, detector);
        Ok(xml_response("PutAnomalyDetector", "", &req.request_id))
    }

    pub(crate) fn describe_anomaly_detectors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "Namespace", 1, 255)?;
        validate_len(req, "MetricName", 1, 255)?;
        validate_range_i64(req, "MaxResults", 1, i64::MAX)?;
        let filter_ns = optional_query_param(req, "Namespace");
        let filter_metric = optional_query_param(req, "MetricName");
        let filter_dims = parse_dimensions_query(req, "Dimensions");
        let types = collect_member_values(req, "AnomalyDetectorTypes");
        let want_math = types.iter().any(|t| t == "METRIC_MATH");
        let want_single = types.is_empty() || types.iter().any(|t| t == "SINGLE_METRIC");

        let state = self.state.read();
        let mut inner = String::from("<AnomalyDetectors>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(detectors) = acct.anomaly_detectors_in(&req.region) {
                for d in detectors.values() {
                    if d.metric_math && !want_math {
                        continue;
                    }
                    if !d.metric_math && !want_single {
                        continue;
                    }
                    if let Some(ns) = filter_ns.as_ref() {
                        if d.namespace.as_ref() != Some(ns) {
                            continue;
                        }
                    }
                    if let Some(m) = filter_metric.as_ref() {
                        if d.metric_name.as_ref() != Some(m) {
                            continue;
                        }
                    }
                    if !filter_dims.is_empty() && d.dimensions != filter_dims {
                        continue;
                    }
                    inner.push_str(&render_detector(d));
                }
            }
        }
        inner.push_str("</AnomalyDetectors>");
        Ok(xml_response(
            "DescribeAnomalyDetectors",
            &inner,
            &req.request_id,
        ))
    }

    pub(crate) fn delete_anomaly_detector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = resolve_identity(req);
        if id.namespace.is_none() && id.math_id.is_none() {
            return Err(invalid_param(
                "DeleteAnomalyDetector requires a single-metric or metric-math detector",
            ));
        }
        let key = detector_key(
            &id.namespace,
            &id.metric_name,
            &id.stat,
            &id.dimensions,
            &id.math_id,
        );
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let removed = acct.anomaly_detectors_in_mut(&req.region).remove(&key);
        if removed.is_none() {
            return Err(crate::service::not_found(
                "No anomaly detector found for the specified metric",
            ));
        }
        Ok(xml_response("DeleteAnomalyDetector", "", &req.request_id))
    }
}

fn render_detector(d: &AnomalyDetector) -> String {
    let mut s = String::from("<member>");
    if let Some(ns) = &d.namespace {
        s.push_str(&format!("<Namespace>{}</Namespace>", xml_escape(ns)));
    }
    if let Some(m) = &d.metric_name {
        s.push_str(&format!("<MetricName>{}</MetricName>", xml_escape(m)));
    }
    s.push_str(&render_dimensions(&d.dimensions));
    if let Some(stat) = &d.stat {
        s.push_str(&format!("<Stat>{}</Stat>", xml_escape(stat)));
    }
    s.push_str(&format!(
        "<StateValue>{}</StateValue>",
        xml_escape(&d.state_value)
    ));
    if d.metric_math {
        s.push_str("<MetricMathAnomalyDetector><MetricDataQueries/></MetricMathAnomalyDetector>");
    } else {
        s.push_str("<SingleMetricAnomalyDetector>");
        if let Some(ns) = &d.namespace {
            s.push_str(&format!("<Namespace>{}</Namespace>", xml_escape(ns)));
        }
        if let Some(m) = &d.metric_name {
            s.push_str(&format!("<MetricName>{}</MetricName>", xml_escape(m)));
        }
        s.push_str(&render_dimensions(&d.dimensions));
        if let Some(stat) = &d.stat {
            s.push_str(&format!("<Stat>{}</Stat>", xml_escape(stat)));
        }
        s.push_str("</SingleMetricAnomalyDetector>");
    }
    if let Some(cfg) = &d.configuration {
        s.push_str("<Configuration>");
        if !cfg.excluded_time_ranges.is_empty() {
            s.push_str("<ExcludedTimeRanges>");
            for r in &cfg.excluded_time_ranges {
                s.push_str("<member>");
                if let Some(st) = &r.start_time {
                    s.push_str(&format!("<StartTime>{}</StartTime>", xml_escape(st)));
                }
                if let Some(et) = &r.end_time {
                    s.push_str(&format!("<EndTime>{}</EndTime>", xml_escape(et)));
                }
                s.push_str("</member>");
            }
            s.push_str("</ExcludedTimeRanges>");
        }
        if let Some(tz) = &cfg.metric_timezone {
            s.push_str(&format!(
                "<MetricTimezone>{}</MetricTimezone>",
                xml_escape(tz)
            ));
        }
        s.push_str("</Configuration>");
    }
    if let Some(ps) = d.periodic_spikes {
        s.push_str(&format!(
            "<MetricCharacteristics><PeriodicSpikes>{ps}</PeriodicSpikes></MetricCharacteristics>"
        ));
    }
    s.push_str("</member>");
    s
}
