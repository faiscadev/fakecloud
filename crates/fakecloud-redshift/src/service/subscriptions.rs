//! Event subscriptions and the read-only event / event-category catalog.

use chrono::Utc;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::helpers::*;
use super::RedshiftService;
use crate::state::EventSubscription;

fn render_subscription(s: &EventSubscription) -> String {
    let source_ids: String = s
        .source_ids
        .iter()
        .map(|id| tag_elem("SourceId", id))
        .collect();
    let categories: String = s
        .event_categories
        .iter()
        .map(|c| tag_elem("EventCategory", c))
        .collect();
    format!(
        "<CustSubscriptionId>{name}</CustSubscriptionId><CustomerAwsId>{aws_id}</CustomerAwsId>\
         <SnsTopicArn>{topic}</SnsTopicArn><Status>{status}</Status>\
         <SubscriptionCreationTime>{created}</SubscriptionCreationTime>{source_type}\
         <SourceIdsList>{source_ids}</SourceIdsList><EventCategoriesList>{categories}</EventCategoriesList>\
         {severity}<Enabled>{enabled}</Enabled>{tags}",
        name = xml_escape(&s.subscription_name),
        aws_id = xml_escape(&s.customer_aws_id),
        topic = xml_escape(&s.sns_topic_arn),
        status = xml_escape(&s.status),
        created = s.subscription_creation_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        source_type = opt_elem("SourceType", s.source_type.as_deref()),
        severity = opt_elem("Severity", s.severity.as_deref()),
        enabled = s.enabled,
        tags = render_tags(&s.tags),
    )
}

impl RedshiftService {
    pub(super) fn create_event_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "SubscriptionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.event_subscriptions.contains_key(&name) {
            return Err(subscription_already_exists(&name));
        }
        let sub = EventSubscription {
            subscription_name: name.clone(),
            customer_aws_id: req.account_id.clone(),
            sns_topic_arn: param(req, "SnsTopicArn").unwrap_or_default(),
            status: "active".to_string(),
            subscription_creation_time: Utc::now(),
            source_type: param(req, "SourceType"),
            source_ids: member_list(req, "SourceIds", "SourceId"),
            event_categories: member_list(req, "EventCategories", "EventCategory"),
            severity: param(req, "Severity"),
            enabled: bool_param(req, "Enabled").unwrap_or(true),
            tags: parse_tags(req),
        };
        acct.event_subscriptions.insert(name, sub.clone());
        Ok(xml_resp(
            "CreateEventSubscription",
            format!(
                "<EventSubscription>{}</EventSubscription>",
                render_subscription(&sub)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_event_subscriptions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<EventSubscription> = match (param(req, "SubscriptionName"), acct) {
            (Some(n), Some(a)) => match a.event_subscriptions.get(&n) {
                Some(s) => vec![s.clone()],
                None => return Err(subscription_not_found(&n)),
            },
            (Some(n), None) => return Err(subscription_not_found(&n)),
            (None, Some(a)) => a.event_subscriptions.values().cloned().collect(),
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|s| {
                format!(
                    "<EventSubscription>{}</EventSubscription>",
                    render_subscription(s)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeEventSubscriptions",
            format!(
                "{}<EventSubscriptionsList>{inner}</EventSubscriptionsList>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_event_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "SubscriptionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let sub = acct
            .event_subscriptions
            .get_mut(&name)
            .ok_or_else(|| subscription_not_found(&name))?;
        if let Some(v) = param(req, "SnsTopicArn") {
            sub.sns_topic_arn = v;
        }
        if let Some(v) = param(req, "SourceType") {
            sub.source_type = Some(v);
        }
        if let Some(v) = param(req, "Severity") {
            sub.severity = Some(v);
        }
        if let Some(v) = bool_param(req, "Enabled") {
            sub.enabled = v;
        }
        let source_ids = member_list(req, "SourceIds", "SourceId");
        if !source_ids.is_empty() {
            sub.source_ids = source_ids;
        }
        let cats = member_list(req, "EventCategories", "EventCategory");
        if !cats.is_empty() {
            sub.event_categories = cats;
        }
        let out = render_subscription(sub);
        Ok(xml_resp(
            "ModifyEventSubscription",
            format!("<EventSubscription>{out}</EventSubscription>"),
            &req.request_id,
        ))
    }

    pub(super) fn delete_event_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "SubscriptionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.event_subscriptions.remove(&name).is_none() {
            return Err(subscription_not_found(&name));
        }
        Ok(xml_metadata_only(
            "DeleteEventSubscription",
            &req.request_id,
        ))
    }

    pub(super) fn describe_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // No event history is generated in the mock; return an empty, well-formed list.
        Ok(xml_resp(
            "DescribeEvents",
            "<Events/>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_event_categories(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source_types = [
            (
                "cluster",
                &[
                    "configuration",
                    "management",
                    "monitoring",
                    "security",
                    "pending",
                ][..],
            ),
            ("cluster-parameter-group", &["configuration"][..]),
            ("cluster-security-group", &["configuration"][..]),
            ("cluster-snapshot", &["management"][..]),
            ("scheduled-action", &["management"][..]),
        ];
        let filter = param(req, "SourceType");
        let inner: String = source_types
            .iter()
            .filter(|(st, _)| filter.as_deref().map(|f| f == *st).unwrap_or(true))
            .map(|(st, cats)| {
                let events: String = cats
                    .iter()
                    .map(|c| {
                        format!(
                            "<EventInfoMap><EventId>REDSHIFT-EVENT-1000</EventId><EventCategories><EventCategory>{}</EventCategory></EventCategories><EventDescription>Redshift {} event</EventDescription><Severity>INFO</Severity></EventInfoMap>",
                            xml_escape(c),
                            xml_escape(c)
                        )
                    })
                    .collect();
                format!(
                    "<EventCategoriesMap><SourceType>{}</SourceType><Events>{events}</Events></EventCategoriesMap>",
                    xml_escape(st)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeEventCategories",
            format!("<EventCategoriesMapList>{inner}</EventCategoriesMapList>"),
            &req.request_id,
        ))
    }
}
