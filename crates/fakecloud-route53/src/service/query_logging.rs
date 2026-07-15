//! Route53 `query_logging` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn create_query_logging_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateQueryLoggingConfigRequest =
            xml_io::from_xml_root(&req.body).map_err(|e| {
                invalid_argument(format!("invalid CreateQueryLoggingConfigRequest XML: {e}"))
            })?;
        if cfg.hosted_zone_id.is_empty() || cfg.cloud_watch_logs_log_group_arn.is_empty() {
            return Err(invalid_argument(
                "HostedZoneId and CloudWatchLogsLogGroupArn are required",
            ));
        }
        let zone_id = strip_zone_prefix(&cfg.hosted_zone_id);
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if let Some(zone) = account.hosted_zones.get(&zone_id) {
            if zone.private_zone {
                return Err(invalid_argument(
                    "Query logging is only supported for public hosted zones",
                ));
            }
        } else {
            return Err(no_such_hosted_zone(&zone_id));
        }
        // One config per zone.
        if account
            .query_logging_configs
            .values()
            .any(|c| c.hosted_zone_id == zone_id)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "QueryLoggingConfigAlreadyExists",
                format!("A query logging config already exists for zone {}", zone_id),
            ));
        }
        let id = Uuid::new_v4().to_string();
        let stored = StoredQueryLoggingConfig {
            id: id.clone(),
            hosted_zone_id: zone_id,
            cloud_watch_logs_log_group_arn: cfg.cloud_watch_logs_log_group_arn,
        };
        account
            .query_logging_configs
            .insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<CreateQueryLoggingConfigResponse xmlns=\"{NS}\">"
        ));
        push_query_logging_config(&mut body, &stored);
        body.push_str("</CreateQueryLoggingConfigResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/queryloggingconfig/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn get_query_logging_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let state = self.state.read();
        let cfg = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.query_logging_configs.get(&id).cloned())
            .ok_or_else(|| no_such_query_logging_config(&id))?;
        drop(state);
        let mut body = String::with_capacity(256);
        body.push_str(XML_DECL);
        body.push_str(&format!("<GetQueryLoggingConfigResponse xmlns=\"{NS}\">"));
        push_query_logging_config(&mut body, &cfg);
        body.push_str("</GetQueryLoggingConfigResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_query_logging_config(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_query_logging_config(&id))?;
        if account.query_logging_configs.remove(&id).is_none() {
            return Err(no_such_query_logging_config(&id));
        }
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!(
            "<DeleteQueryLoggingConfigResponse xmlns=\"{NS}\"/>"
        ));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_query_logging_configs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "hostedzoneid",
                    min: 0,
                    max: 32,
                },
                QueryConstraint::StrLen {
                    key: "nexttoken",
                    min: 0,
                    max: 1024,
                },
                MAX_RESULTS_CONSTRAINT,
            ],
        )?;
        let zone_filter = req.query_params.get("hostedzoneid").cloned();
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut configs: Vec<StoredQueryLoggingConfig> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.query_logging_configs.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        if let Some(zid) = zone_filter {
            let z = strip_zone_prefix(&zid);
            configs.retain(|c| c.hosted_zone_id == z);
        }
        configs.sort_by(|a, b| a.id.cmp(&b.id));
        // Resume after the inbound NextToken (the id of the last config
        // returned on the previous page). Previously the token was accepted
        // but never applied, so paging re-returned page 1 forever.
        let start = match req.query_params.get("nexttoken") {
            Some(t) => configs
                .iter()
                .position(|c| c.id.as_str() > t.as_str())
                .unwrap_or(configs.len()),
            None => 0,
        };
        let remaining = &configs[start..];
        let slice: Vec<&StoredQueryLoggingConfig> = remaining.iter().take(max_items).collect();
        // Token is the last id emitted; the next request resumes strictly
        // after it (`id > token`). Emitting the first excluded id would skip
        // it under strict-greater resume.
        let next = if slice.len() < remaining.len() {
            slice.last().map(|c| c.id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListQueryLoggingConfigsResponse xmlns=\"{NS}\">"));
        body.push_str("<QueryLoggingConfigs>");
        for c in &slice {
            push_query_logging_config(&mut body, c);
        }
        body.push_str("</QueryLoggingConfigs>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListQueryLoggingConfigsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
