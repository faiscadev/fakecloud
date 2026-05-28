//! Contributor Insights rules: Put/Describe/Delete/Enable/Disable, report,
//! and managed rules.

use fakecloud_core::query::{optional_query_param, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_member_values, invalid_param, not_found, validate_len, validate_range_i64, xml_escape,
    xml_response, CloudWatchService,
};
use crate::state::{InsightRule, ManagedRule};

const DEFAULT_SCHEMA: &str = "{\"Name\":\"CloudWatchLogRule\",\"Version\":1}";

impl CloudWatchService {
    pub(crate) fn put_insight_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "RuleName", 1, 128)?;
        validate_len(req, "RuleDefinition", 1, 8192)?;
        validate_len(req, "RuleState", 1, 32)?;
        let name = required_query_param(req, "RuleName")?;
        let definition = required_query_param(req, "RuleDefinition")?;
        let rule_state = optional_query_param(req, "RuleState").unwrap_or_else(|| "ENABLED".into());
        if rule_state != "ENABLED" && rule_state != "DISABLED" {
            return Err(invalid_param("RuleState must be ENABLED or DISABLED"));
        }
        let apply_on_transformed_logs = optional_query_param(req, "ApplyOnTransformedLogs")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let rule = InsightRule {
            name: name.clone(),
            state: rule_state,
            schema: DEFAULT_SCHEMA.to_string(),
            definition,
            managed: false,
            apply_on_transformed_logs,
        };
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.insight_rules_in_mut(&req.region).insert(name, rule);
        Ok(xml_response("PutInsightRule", "", &req.request_id))
    }

    pub(crate) fn describe_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_range_i64(req, "MaxResults", 1, 500)?;
        let state = self.state.read();
        let mut inner = String::from("<InsightRules>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(rules) = acct.insight_rules_in(&req.region) {
                for rule in rules.values() {
                    inner.push_str(&render_insight_rule(rule));
                }
            }
        }
        inner.push_str("</InsightRules>");
        Ok(xml_response(
            "DescribeInsightRules",
            &inner,
            &req.request_id,
        ))
    }

    pub(crate) fn delete_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = collect_member_values(req, "RuleNames");
        if names.is_empty() {
            return Err(crate::service::missing_param("RuleNames"));
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let rules = acct.insight_rules_in_mut(&req.region);
        for name in &names {
            rules.remove(name);
        }
        Ok(xml_response(
            "DeleteInsightRules",
            "<Failures/>",
            &req.request_id,
        ))
    }

    pub(crate) fn enable_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.toggle_insight_rules(req, "ENABLED", "EnableInsightRules")
    }

    pub(crate) fn disable_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.toggle_insight_rules(req, "DISABLED", "DisableInsightRules")
    }

    fn toggle_insight_rules(
        &self,
        req: &AwsRequest,
        new_state: &str,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = collect_member_values(req, "RuleNames");
        if names.is_empty() {
            return Err(crate::service::missing_param("RuleNames"));
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let rules = acct.insight_rules_in_mut(&req.region);
        for name in &names {
            if let Some(rule) = rules.get_mut(name) {
                rule.state = new_state.to_string();
            }
        }
        Ok(xml_response(action, "<Failures/>", &req.request_id))
    }

    pub(crate) fn get_insight_rule_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "RuleName")?;
        required_query_param(req, "StartTime")?;
        required_query_param(req, "EndTime")?;
        let _period = required_query_param(req, "Period")?
            .parse::<i64>()
            .map_err(|_| invalid_param("Period must be an integer"))?;
        let metrics = collect_member_values(req, "Metrics");

        let state = self.state.read();
        let exists = state
            .get(&req.account_id)
            .and_then(|a| a.insight_rules_in(&req.region))
            .map(|r| r.contains_key(&name))
            .unwrap_or(false);
        if !exists {
            return Err(not_found(format!("Insight rule {name} does not exist")));
        }

        let mut inner = String::from("<KeyLabels/>");
        inner.push_str("<AggregationStatistic>Sum</AggregationStatistic>");
        inner.push_str("<AggregateValue>0.0</AggregateValue>");
        inner.push_str("<ApproximateUniqueCount>0</ApproximateUniqueCount>");
        inner.push_str("<Contributors/>");
        inner.push_str("<MetricDatapoints/>");
        // Echo requested metrics back so callers can confirm the report shape;
        // the metric list itself isn't part of the output but the wire stays
        // well-formed regardless of how many metrics were requested.
        let _ = metrics;
        Ok(xml_response(
            "GetInsightRuleReport",
            &inner,
            &req.request_id,
        ))
    }

    pub(crate) fn put_managed_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let members = crate::service::collect_indexed(req, "ManagedRules");
        if members.is_empty() {
            return Err(crate::service::missing_param("ManagedRules"));
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        for m in members {
            let template = m
                .get("TemplateName")
                .cloned()
                .ok_or_else(|| invalid_param("ManagedRules.member.N.TemplateName is required"))?;
            let arn = m
                .get("ResourceARN")
                .cloned()
                .ok_or_else(|| invalid_param("ManagedRules.member.N.ResourceARN is required"))?;
            let bucket = acct
                .managed_rules_in_mut(&req.region)
                .entry(arn.clone())
                .or_default();
            // De-duplicate by template name per resource.
            if !bucket.iter().any(|r| r.template_name == template) {
                bucket.push(ManagedRule {
                    template_name: template,
                    resource_arn: arn,
                });
            }
        }
        Ok(xml_response(
            "PutManagedInsightRules",
            "<Failures/>",
            &req.request_id,
        ))
    }

    pub(crate) fn list_managed_insight_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "ResourceARN", 1, 1024)?;
        validate_range_i64(req, "MaxResults", 1, 500)?;
        let resource_arn = required_query_param(req, "ResourceARN")?;
        let state = self.state.read();
        let mut inner = String::from("<ManagedRules>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(map) = acct.managed_rules_in(&req.region) {
                if let Some(rules) = map.get(&resource_arn) {
                    for r in rules {
                        inner.push_str("<member>");
                        inner.push_str(&format!(
                            "<TemplateName>{}</TemplateName>",
                            xml_escape(&r.template_name)
                        ));
                        inner.push_str(&format!(
                            "<ResourceARN>{}</ResourceARN>",
                            xml_escape(&r.resource_arn)
                        ));
                        inner.push_str(
                            "<RuleState><RuleName>ManagedRule</RuleName><State>ENABLED</State></RuleState>",
                        );
                        inner.push_str("</member>");
                    }
                }
            }
        }
        inner.push_str("</ManagedRules>");
        Ok(xml_response(
            "ListManagedInsightRules",
            &inner,
            &req.request_id,
        ))
    }
}

fn render_insight_rule(rule: &InsightRule) -> String {
    let mut s = String::from("<member>");
    s.push_str(&format!("<Name>{}</Name>", xml_escape(&rule.name)));
    s.push_str(&format!("<State>{}</State>", xml_escape(&rule.state)));
    s.push_str(&format!("<Schema>{}</Schema>", xml_escape(&rule.schema)));
    s.push_str(&format!(
        "<Definition>{}</Definition>",
        xml_escape(&rule.definition)
    ));
    s.push_str(&format!("<ManagedRule>{}</ManagedRule>", rule.managed));
    s.push_str(&format!(
        "<ApplyOnTransformedLogs>{}</ApplyOnTransformedLogs>",
        rule.apply_on_transformed_logs
    ));
    s.push_str("</member>");
    s
}
