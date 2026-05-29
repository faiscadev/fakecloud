//! Metric streams: Put/Get/List/Delete + Start/Stop (control plane only;
//! no Firehose data-plane delivery).

use chrono::Utc;

use fakecloud_core::query::required_query_param;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_indexed, collect_member_values, missing_param, not_found, validate_enum, validate_len,
    validate_range_i64, xml_escape, xml_response, CloudWatchService,
};
use crate::state::{MetricStream, MetricStreamFilter};

fn parse_filters(req: &AwsRequest, prefix: &str) -> Vec<MetricStreamFilter> {
    let members = collect_indexed(req, prefix);
    members
        .into_iter()
        .map(|m| {
            let mut metric_names = Vec::new();
            // MetricNames.member.N within this filter member.
            let mut idx = 1;
            loop {
                let key = format!("MetricNames.member.{idx}");
                match m.get(&key) {
                    Some(v) => {
                        metric_names.push(v.clone());
                        idx += 1;
                    }
                    None => break,
                }
            }
            MetricStreamFilter {
                namespace: m.get("Namespace").cloned(),
                metric_names,
            }
        })
        .collect()
}

fn render_filters(tag: &str, filters: &[MetricStreamFilter]) -> String {
    let mut s = format!("<{tag}>");
    for f in filters {
        s.push_str("<member>");
        if let Some(ns) = &f.namespace {
            s.push_str(&format!("<Namespace>{}</Namespace>", xml_escape(ns)));
        }
        s.push_str("<MetricNames>");
        for n in &f.metric_names {
            s.push_str(&format!("<member>{}</member>", xml_escape(n)));
        }
        s.push_str("</MetricNames>");
        s.push_str("</member>");
    }
    s.push_str(&format!("</{tag}>"));
    s
}

impl CloudWatchService {
    pub(crate) fn put_metric_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "Name", 1, 255)?;
        validate_len(req, "FirehoseArn", 1, 1024)?;
        validate_len(req, "RoleArn", 1, 1024)?;
        // The current Smithy model uses JSON / OPEN_TELEMETRY_*; older SDK
        // builds (e.g. aws-sdk-cloudwatch 1.61.0) serialise the lowercase
        // legacy wire values. Accept both so the model-driven probe and SDK
        // clients agree.
        validate_enum(
            req,
            "OutputFormat",
            &[
                "JSON",
                "OPEN_TELEMETRY_0_7",
                "OPEN_TELEMETRY_1_0",
                "json",
                "opentelemetry0.7",
                "opentelemetry1.0",
            ],
        )?;
        let name = required_query_param(req, "Name")?;
        let firehose_arn = required_query_param(req, "FirehoseArn")?;
        let role_arn = required_query_param(req, "RoleArn")?;
        let output_format = required_query_param(req, "OutputFormat")?;
        let include_filters = parse_filters(req, "IncludeFilters");
        let exclude_filters = parse_filters(req, "ExcludeFilters");
        let include_linked =
            fakecloud_core::query::optional_query_param(req, "IncludeLinkedAccountsMetrics")
                .map(|s| s.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:metric-stream/{name}",
            req.region, req.account_id
        );
        let now = Utc::now();

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let streams = acct.metric_streams_in_mut(&req.region);
        let creation_date = streams.get(&name).map(|s| s.creation_date).unwrap_or(now);
        let stream = MetricStream {
            name: name.clone(),
            arn: arn.clone(),
            firehose_arn,
            role_arn,
            output_format,
            state: "running".to_string(),
            include_filters,
            exclude_filters,
            include_linked_accounts_metrics: include_linked,
            creation_date,
            last_update_date: now,
        };
        streams.insert(name, stream);
        let inner = format!("<Arn>{}</Arn>", xml_escape(&arn));
        Ok(xml_response("PutMetricStream", &inner, &req.request_id))
    }

    pub(crate) fn get_metric_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "Name")?;
        let state = self.state.read();
        let stream = state
            .get(&req.account_id)
            .and_then(|a| a.metric_streams_in(&req.region))
            .and_then(|m| m.get(&name))
            .cloned()
            .ok_or_else(|| not_found(format!("Metric stream {name} does not exist")))?;

        let mut inner = format!("<Arn>{}</Arn>", xml_escape(&stream.arn));
        inner.push_str(&format!("<Name>{}</Name>", xml_escape(&stream.name)));
        inner.push_str(&render_filters("IncludeFilters", &stream.include_filters));
        inner.push_str(&render_filters("ExcludeFilters", &stream.exclude_filters));
        inner.push_str(&format!(
            "<FirehoseArn>{}</FirehoseArn>",
            xml_escape(&stream.firehose_arn)
        ));
        inner.push_str(&format!(
            "<RoleArn>{}</RoleArn>",
            xml_escape(&stream.role_arn)
        ));
        inner.push_str(&format!("<State>{}</State>", xml_escape(&stream.state)));
        inner.push_str(&format!(
            "<CreationDate>{}</CreationDate>",
            stream
                .creation_date
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        ));
        inner.push_str(&format!(
            "<LastUpdateDate>{}</LastUpdateDate>",
            stream
                .last_update_date
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        ));
        inner.push_str(&format!(
            "<OutputFormat>{}</OutputFormat>",
            xml_escape(&stream.output_format)
        ));
        inner.push_str(&format!(
            "<IncludeLinkedAccountsMetrics>{}</IncludeLinkedAccountsMetrics>",
            stream.include_linked_accounts_metrics
        ));
        Ok(xml_response("GetMetricStream", &inner, &req.request_id))
    }

    pub(crate) fn list_metric_streams(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i64(req, "MaxResults", 1, 500)?;
        let state = self.state.read();
        let mut inner = String::from("<Entries>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(streams) = acct.metric_streams_in(&req.region) {
                for s in streams.values() {
                    inner.push_str("<member>");
                    inner.push_str(&format!("<Arn>{}</Arn>", xml_escape(&s.arn)));
                    inner.push_str(&format!(
                        "<CreationDate>{}</CreationDate>",
                        s.creation_date
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    ));
                    inner.push_str(&format!(
                        "<LastUpdateDate>{}</LastUpdateDate>",
                        s.last_update_date
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    ));
                    inner.push_str(&format!("<Name>{}</Name>", xml_escape(&s.name)));
                    inner.push_str(&format!(
                        "<FirehoseArn>{}</FirehoseArn>",
                        xml_escape(&s.firehose_arn)
                    ));
                    inner.push_str(&format!("<State>{}</State>", xml_escape(&s.state)));
                    inner.push_str(&format!(
                        "<OutputFormat>{}</OutputFormat>",
                        xml_escape(&s.output_format)
                    ));
                    inner.push_str("</member>");
                }
            }
        }
        inner.push_str("</Entries>");
        Ok(xml_response("ListMetricStreams", &inner, &req.request_id))
    }

    pub(crate) fn delete_metric_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "Name", 1, 255)?;
        let name = required_query_param(req, "Name")?;
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.metric_streams_in_mut(&req.region).remove(&name);
        Ok(xml_response("DeleteMetricStream", "", &req.request_id))
    }

    pub(crate) fn start_metric_streams(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.set_stream_state(req, "running", "StartMetricStreams")
    }

    pub(crate) fn stop_metric_streams(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.set_stream_state(req, "stopped", "StopMetricStreams")
    }

    fn set_stream_state(
        &self,
        req: &AwsRequest,
        new_state: &str,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = collect_member_values(req, "Names");
        if names.is_empty() {
            return Err(missing_param("Names"));
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let streams = acct.metric_streams_in_mut(&req.region);
        for name in &names {
            if let Some(s) = streams.get_mut(name) {
                s.state = new_state.to_string();
                s.last_update_date = Utc::now();
            }
        }
        Ok(xml_response(action, "", &req.request_id))
    }
}
