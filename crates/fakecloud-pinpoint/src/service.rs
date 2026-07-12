//! Amazon Pinpoint (`pinpoint`) restJson1 dispatch + shared handler helpers.
//!
//! Requests are routed to an operation by HTTP method + `@http` URI path under
//! the `/v1` prefix; path labels are captured positionally (percent-decoded, so
//! an ARN-shaped `ResourceArn` label survives with its slashes/colons intact)
//! and query parameters are read from the raw query string. State is
//! account-partitioned and persisted. The per-resource-family handlers live in
//! the sibling submodules (`apps`, `campaigns`, `segments`, `endpoints`,
//! `channels`, `messaging`, `jobs`, `journeys`, `templates`, `recommenders`,
//! `tags`); this module owns routing, the `AwsService` glue, and the small set
//! of shared response / error / projection helpers.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{App, SharedPinpointState};

mod apps;
mod campaigns;
mod channels;
mod endpoints;
mod jobs;
mod journeys;
mod messaging;
mod recommenders;
mod segments;
mod tags;
mod templates;

#[cfg(test)]
mod tests;

/// Every operation name in the Amazon Pinpoint Smithy model (122).
pub const PINPOINT_ACTIONS: &[&str] = &[
    "CreateApp",
    "CreateCampaign",
    "CreateEmailTemplate",
    "CreateExportJob",
    "CreateImportJob",
    "CreateInAppTemplate",
    "CreateJourney",
    "CreatePushTemplate",
    "CreateRecommenderConfiguration",
    "CreateSegment",
    "CreateSmsTemplate",
    "CreateVoiceTemplate",
    "DeleteAdmChannel",
    "DeleteApnsChannel",
    "DeleteApnsSandboxChannel",
    "DeleteApnsVoipChannel",
    "DeleteApnsVoipSandboxChannel",
    "DeleteApp",
    "DeleteBaiduChannel",
    "DeleteCampaign",
    "DeleteEmailChannel",
    "DeleteEmailTemplate",
    "DeleteEndpoint",
    "DeleteEventStream",
    "DeleteGcmChannel",
    "DeleteInAppTemplate",
    "DeleteJourney",
    "DeletePushTemplate",
    "DeleteRecommenderConfiguration",
    "DeleteSegment",
    "DeleteSmsChannel",
    "DeleteSmsTemplate",
    "DeleteUserEndpoints",
    "DeleteVoiceChannel",
    "DeleteVoiceTemplate",
    "GetAdmChannel",
    "GetApnsChannel",
    "GetApnsSandboxChannel",
    "GetApnsVoipChannel",
    "GetApnsVoipSandboxChannel",
    "GetApp",
    "GetApplicationDateRangeKpi",
    "GetApplicationSettings",
    "GetApps",
    "GetBaiduChannel",
    "GetCampaign",
    "GetCampaignActivities",
    "GetCampaignDateRangeKpi",
    "GetCampaignVersion",
    "GetCampaignVersions",
    "GetCampaigns",
    "GetChannels",
    "GetEmailChannel",
    "GetEmailTemplate",
    "GetEndpoint",
    "GetEventStream",
    "GetExportJob",
    "GetExportJobs",
    "GetGcmChannel",
    "GetImportJob",
    "GetImportJobs",
    "GetInAppMessages",
    "GetInAppTemplate",
    "GetJourney",
    "GetJourneyDateRangeKpi",
    "GetJourneyExecutionActivityMetrics",
    "GetJourneyExecutionMetrics",
    "GetJourneyRunExecutionActivityMetrics",
    "GetJourneyRunExecutionMetrics",
    "GetJourneyRuns",
    "GetPushTemplate",
    "GetRecommenderConfiguration",
    "GetRecommenderConfigurations",
    "GetSegment",
    "GetSegmentExportJobs",
    "GetSegmentImportJobs",
    "GetSegmentVersion",
    "GetSegmentVersions",
    "GetSegments",
    "GetSmsChannel",
    "GetSmsTemplate",
    "GetUserEndpoints",
    "GetVoiceChannel",
    "GetVoiceTemplate",
    "ListJourneys",
    "ListTagsForResource",
    "ListTemplateVersions",
    "ListTemplates",
    "PhoneNumberValidate",
    "PutEventStream",
    "PutEvents",
    "RemoveAttributes",
    "SendMessages",
    "SendOTPMessage",
    "SendUsersMessages",
    "TagResource",
    "UntagResource",
    "UpdateAdmChannel",
    "UpdateApnsChannel",
    "UpdateApnsSandboxChannel",
    "UpdateApnsVoipChannel",
    "UpdateApnsVoipSandboxChannel",
    "UpdateApplicationSettings",
    "UpdateBaiduChannel",
    "UpdateCampaign",
    "UpdateEmailChannel",
    "UpdateEmailTemplate",
    "UpdateEndpoint",
    "UpdateEndpointsBatch",
    "UpdateGcmChannel",
    "UpdateInAppTemplate",
    "UpdateJourney",
    "UpdateJourneyState",
    "UpdatePushTemplate",
    "UpdateRecommenderConfiguration",
    "UpdateSegment",
    "UpdateSmsChannel",
    "UpdateSmsTemplate",
    "UpdateTemplateActiveVersion",
    "UpdateVoiceChannel",
    "UpdateVoiceTemplate",
    "VerifyOTPMessage",
];

pub struct PinpointService {
    state: SharedPinpointState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl PinpointService {
    pub fn new(state: SharedPinpointState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        let l = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();

        // Channel keys that map to a single `channels/<key>` route.
        let channel_key = |seg: &str| -> Option<&'static str> {
            Some(match seg {
                "adm" => "adm",
                "apns" => "apns",
                "apns_sandbox" => "apns_sandbox",
                "apns_voip" => "apns_voip",
                "apns_voip_sandbox" => "apns_voip_sandbox",
                "baidu" => "baidu",
                "email" => "email",
                "gcm" => "gcm",
                "sms" => "sms",
                "voice" => "voice",
                _ => return None,
            })
        };
        let channel_action = |key: &str, verb: &str| -> &'static str {
            match (key, verb) {
                ("adm", "get") => "GetAdmChannel",
                ("adm", "put") => "UpdateAdmChannel",
                ("adm", "del") => "DeleteAdmChannel",
                ("apns", "get") => "GetApnsChannel",
                ("apns", "put") => "UpdateApnsChannel",
                ("apns", "del") => "DeleteApnsChannel",
                ("apns_sandbox", "get") => "GetApnsSandboxChannel",
                ("apns_sandbox", "put") => "UpdateApnsSandboxChannel",
                ("apns_sandbox", "del") => "DeleteApnsSandboxChannel",
                ("apns_voip", "get") => "GetApnsVoipChannel",
                ("apns_voip", "put") => "UpdateApnsVoipChannel",
                ("apns_voip", "del") => "DeleteApnsVoipChannel",
                ("apns_voip_sandbox", "get") => "GetApnsVoipSandboxChannel",
                ("apns_voip_sandbox", "put") => "UpdateApnsVoipSandboxChannel",
                ("apns_voip_sandbox", "del") => "DeleteApnsVoipSandboxChannel",
                ("baidu", "get") => "GetBaiduChannel",
                ("baidu", "put") => "UpdateBaiduChannel",
                ("baidu", "del") => "DeleteBaiduChannel",
                ("email", "get") => "GetEmailChannel",
                ("email", "put") => "UpdateEmailChannel",
                ("email", "del") => "DeleteEmailChannel",
                ("gcm", "get") => "GetGcmChannel",
                ("gcm", "put") => "UpdateGcmChannel",
                ("gcm", "del") => "DeleteGcmChannel",
                ("sms", "get") => "GetSmsChannel",
                ("sms", "put") => "UpdateSmsChannel",
                ("sms", "del") => "DeleteSmsChannel",
                ("voice", "get") => "GetVoiceChannel",
                ("voice", "put") => "UpdateVoiceChannel",
                _ => "DeleteVoiceChannel",
            }
        };
        // Template type segment for the typed template routes.
        let template_type = |seg: &str| -> Option<&'static str> {
            Some(match seg {
                "email" => "EMAIL",
                "inapp" => "INAPP",
                "push" => "PUSH",
                "sms" => "SMS",
                "voice" => "VOICE",
                _ => return None,
            })
        };
        let template_action = |ttype: &str, verb: &str| -> &'static str {
            match (ttype, verb) {
                ("EMAIL", "post") => "CreateEmailTemplate",
                ("EMAIL", "get") => "GetEmailTemplate",
                ("EMAIL", "put") => "UpdateEmailTemplate",
                ("EMAIL", "del") => "DeleteEmailTemplate",
                ("INAPP", "post") => "CreateInAppTemplate",
                ("INAPP", "get") => "GetInAppTemplate",
                ("INAPP", "put") => "UpdateInAppTemplate",
                ("INAPP", "del") => "DeleteInAppTemplate",
                ("PUSH", "post") => "CreatePushTemplate",
                ("PUSH", "get") => "GetPushTemplate",
                ("PUSH", "put") => "UpdatePushTemplate",
                ("PUSH", "del") => "DeletePushTemplate",
                ("SMS", "post") => "CreateSmsTemplate",
                ("SMS", "get") => "GetSmsTemplate",
                ("SMS", "put") => "UpdateSmsTemplate",
                ("SMS", "del") => "DeleteSmsTemplate",
                ("VOICE", "post") => "CreateVoiceTemplate",
                ("VOICE", "get") => "GetVoiceTemplate",
                ("VOICE", "put") => "UpdateVoiceTemplate",
                _ => "DeleteVoiceTemplate",
            }
        };

        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            // ---- apps ----
            ["v1", "apps"] if post => ("CreateApp", vec![]),
            ["v1", "apps"] if get => ("GetApps", vec![]),
            ["v1", "apps", id] if get => ("GetApp", l(&[id])),
            ["v1", "apps", id] if del => ("DeleteApp", l(&[id])),
            ["v1", "apps", id, "settings"] if get => ("GetApplicationSettings", l(&[id])),
            ["v1", "apps", id, "settings"] if put => ("UpdateApplicationSettings", l(&[id])),
            ["v1", "apps", id, "kpis", "daterange", kpi] if get => {
                ("GetApplicationDateRangeKpi", l(&[id, kpi]))
            }
            ["v1", "apps", id, "attributes", at] if put => ("RemoveAttributes", l(&[id, at])),

            // ---- campaigns ----
            ["v1", "apps", id, "campaigns"] if post => ("CreateCampaign", l(&[id])),
            ["v1", "apps", id, "campaigns"] if get => ("GetCampaigns", l(&[id])),
            ["v1", "apps", id, "campaigns", cid] if get => ("GetCampaign", l(&[id, cid])),
            ["v1", "apps", id, "campaigns", cid] if put => ("UpdateCampaign", l(&[id, cid])),
            ["v1", "apps", id, "campaigns", cid] if del => ("DeleteCampaign", l(&[id, cid])),
            ["v1", "apps", id, "campaigns", cid, "activities"] if get => {
                ("GetCampaignActivities", l(&[id, cid]))
            }
            ["v1", "apps", id, "campaigns", cid, "kpis", "daterange", kpi] if get => {
                ("GetCampaignDateRangeKpi", l(&[id, cid, kpi]))
            }
            ["v1", "apps", id, "campaigns", cid, "versions"] if get => {
                ("GetCampaignVersions", l(&[id, cid]))
            }
            ["v1", "apps", id, "campaigns", cid, "versions", ver] if get => {
                ("GetCampaignVersion", l(&[id, cid, ver]))
            }

            // ---- channels ----
            ["v1", "apps", id, "channels"] if get => ("GetChannels", l(&[id])),
            ["v1", "apps", id, "channels", c] if get && channel_key(c).is_some() => {
                (channel_action(channel_key(c).unwrap(), "get"), l(&[id]))
            }
            ["v1", "apps", id, "channels", c] if put && channel_key(c).is_some() => {
                (channel_action(channel_key(c).unwrap(), "put"), l(&[id]))
            }
            ["v1", "apps", id, "channels", c] if del && channel_key(c).is_some() => {
                (channel_action(channel_key(c).unwrap(), "del"), l(&[id]))
            }

            // ---- endpoints / users ----
            ["v1", "apps", id, "endpoints"] if put => ("UpdateEndpointsBatch", l(&[id])),
            ["v1", "apps", id, "endpoints", eid] if get => ("GetEndpoint", l(&[id, eid])),
            ["v1", "apps", id, "endpoints", eid] if put => ("UpdateEndpoint", l(&[id, eid])),
            ["v1", "apps", id, "endpoints", eid] if del => ("DeleteEndpoint", l(&[id, eid])),
            ["v1", "apps", id, "endpoints", eid, "inappmessages"] if get => {
                ("GetInAppMessages", l(&[id, eid]))
            }
            ["v1", "apps", id, "users", uid] if get => ("GetUserEndpoints", l(&[id, uid])),
            ["v1", "apps", id, "users", uid] if del => ("DeleteUserEndpoints", l(&[id, uid])),

            // ---- events / event stream ----
            ["v1", "apps", id, "events"] if post => ("PutEvents", l(&[id])),
            ["v1", "apps", id, "eventstream"] if post => ("PutEventStream", l(&[id])),
            ["v1", "apps", id, "eventstream"] if get => ("GetEventStream", l(&[id])),
            ["v1", "apps", id, "eventstream"] if del => ("DeleteEventStream", l(&[id])),

            // ---- jobs ----
            ["v1", "apps", id, "jobs", "export"] if post => ("CreateExportJob", l(&[id])),
            ["v1", "apps", id, "jobs", "export"] if get => ("GetExportJobs", l(&[id])),
            ["v1", "apps", id, "jobs", "export", jid] if get => ("GetExportJob", l(&[id, jid])),
            ["v1", "apps", id, "jobs", "import"] if post => ("CreateImportJob", l(&[id])),
            ["v1", "apps", id, "jobs", "import"] if get => ("GetImportJobs", l(&[id])),
            ["v1", "apps", id, "jobs", "import", jid] if get => ("GetImportJob", l(&[id, jid])),

            // ---- journeys ----
            ["v1", "apps", id, "journeys"] if post => ("CreateJourney", l(&[id])),
            ["v1", "apps", id, "journeys"] if get => ("ListJourneys", l(&[id])),
            ["v1", "apps", id, "journeys", jid] if get => ("GetJourney", l(&[id, jid])),
            ["v1", "apps", id, "journeys", jid] if put => ("UpdateJourney", l(&[id, jid])),
            ["v1", "apps", id, "journeys", jid] if del => ("DeleteJourney", l(&[id, jid])),
            ["v1", "apps", id, "journeys", jid, "state"] if put => {
                ("UpdateJourneyState", l(&[id, jid]))
            }
            ["v1", "apps", id, "journeys", jid, "kpis", "daterange", kpi] if get => {
                ("GetJourneyDateRangeKpi", l(&[id, jid, kpi]))
            }
            ["v1", "apps", id, "journeys", jid, "runs"] if get => ("GetJourneyRuns", l(&[id, jid])),
            ["v1", "apps", id, "journeys", jid, "execution-metrics"] if get => {
                ("GetJourneyExecutionMetrics", l(&[id, jid]))
            }
            ["v1", "apps", id, "journeys", jid, "activities", act, "execution-metrics"] if get => {
                ("GetJourneyExecutionActivityMetrics", l(&[id, jid, act]))
            }
            ["v1", "apps", id, "journeys", jid, "runs", run, "execution-metrics"] if get => {
                ("GetJourneyRunExecutionMetrics", l(&[id, jid, run]))
            }
            ["v1", "apps", id, "journeys", jid, "runs", run, "activities", act, "execution-metrics"]
                if get =>
            {
                (
                    "GetJourneyRunExecutionActivityMetrics",
                    l(&[id, jid, act, run]),
                )
            }

            // ---- messaging ----
            ["v1", "apps", id, "messages"] if post => ("SendMessages", l(&[id])),
            ["v1", "apps", id, "otp"] if post => ("SendOTPMessage", l(&[id])),
            ["v1", "apps", id, "users-messages"] if post => ("SendUsersMessages", l(&[id])),
            ["v1", "apps", id, "verify-otp"] if post => ("VerifyOTPMessage", l(&[id])),

            // ---- segments ----
            ["v1", "apps", id, "segments"] if post => ("CreateSegment", l(&[id])),
            ["v1", "apps", id, "segments"] if get => ("GetSegments", l(&[id])),
            ["v1", "apps", id, "segments", sid] if get => ("GetSegment", l(&[id, sid])),
            ["v1", "apps", id, "segments", sid] if put => ("UpdateSegment", l(&[id, sid])),
            ["v1", "apps", id, "segments", sid] if del => ("DeleteSegment", l(&[id, sid])),
            ["v1", "apps", id, "segments", sid, "jobs", "export"] if get => {
                ("GetSegmentExportJobs", l(&[id, sid]))
            }
            ["v1", "apps", id, "segments", sid, "jobs", "import"] if get => {
                ("GetSegmentImportJobs", l(&[id, sid]))
            }
            ["v1", "apps", id, "segments", sid, "versions"] if get => {
                ("GetSegmentVersions", l(&[id, sid]))
            }
            ["v1", "apps", id, "segments", sid, "versions", ver] if get => {
                ("GetSegmentVersion", l(&[id, sid, ver]))
            }

            // ---- phone ----
            ["v1", "phone", "number", "validate"] if post => ("PhoneNumberValidate", vec![]),

            // ---- recommenders ----
            ["v1", "recommenders"] if post => ("CreateRecommenderConfiguration", vec![]),
            ["v1", "recommenders"] if get => ("GetRecommenderConfigurations", vec![]),
            ["v1", "recommenders", rid] if get => ("GetRecommenderConfiguration", l(&[rid])),
            ["v1", "recommenders", rid] if put => ("UpdateRecommenderConfiguration", l(&[rid])),
            ["v1", "recommenders", rid] if del => ("DeleteRecommenderConfiguration", l(&[rid])),

            // ---- tags ----
            ["v1", "tags", arn] if get => ("ListTagsForResource", l(&[arn])),
            ["v1", "tags", arn] if post => ("TagResource", l(&[arn])),
            ["v1", "tags", arn] if del => ("UntagResource", l(&[arn])),

            // ---- templates ----
            ["v1", "templates"] if get => ("ListTemplates", vec![]),
            ["v1", "templates", name, t] if post && template_type(t).is_some() => (
                template_action(template_type(t).unwrap(), "post"),
                l(&[name]),
            ),
            ["v1", "templates", name, t] if get && template_type(t).is_some() => (
                template_action(template_type(t).unwrap(), "get"),
                l(&[name]),
            ),
            ["v1", "templates", name, t] if put && template_type(t).is_some() => (
                template_action(template_type(t).unwrap(), "put"),
                l(&[name]),
            ),
            ["v1", "templates", name, t] if del && template_type(t).is_some() => (
                template_action(template_type(t).unwrap(), "del"),
                l(&[name]),
            ),
            ["v1", "templates", name, ttype, "active-version"] if put => {
                ("UpdateTemplateActiveVersion", l(&[name, ttype]))
            }
            ["v1", "templates", name, ttype, "versions"] if get => {
                ("ListTemplateVersions", l(&[name, ttype]))
            }

            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for PinpointService {
    fn service_name(&self) -> &str {
        "pinpoint"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if is_mutating(action) && success {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        PINPOINT_ACTIONS
    }
}

/// Operations that mutate persisted state on success (so a snapshot is taken).
fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Update")
        || action.starts_with("Delete")
        || action.starts_with("Put")
        || action == "TagResource"
        || action == "UntagResource"
        || action == "RemoveAttributes"
}

/// Per-request account + region context.
pub(crate) struct Ctx {
    pub account: String,
    pub region: String,
}

impl PinpointService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req)?;
        for label in labels {
            if label.is_empty() || (label.starts_with('{') && label.ends_with('}')) {
                return Err(bad_request(
                    "The request failed because it is missing a required path parameter.",
                ));
            }
        }
        crate::validate::validate_input(action, &body)?;
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        match action {
            // apps
            "CreateApp" => self.create_app(&ctx, &body),
            "GetApp" => self.get_app(&ctx, a(0)),
            "DeleteApp" => self.delete_app(&ctx, a(0)),
            "GetApps" => self.get_apps(&ctx, &q),
            "GetApplicationSettings" => self.get_application_settings(&ctx, a(0)),
            "UpdateApplicationSettings" => self.update_application_settings(&ctx, a(0), &body),
            "GetApplicationDateRangeKpi" => self.get_application_kpi(&ctx, a(0), a(1)),
            // campaigns
            "CreateCampaign" => self.create_campaign(&ctx, a(0), &body),
            "GetCampaign" => self.get_campaign(&ctx, a(0), a(1)),
            "UpdateCampaign" => self.update_campaign(&ctx, a(0), a(1), &body),
            "DeleteCampaign" => self.delete_campaign(&ctx, a(0), a(1)),
            "GetCampaigns" => self.get_campaigns(&ctx, a(0), &q),
            "GetCampaignVersions" => self.get_campaign_versions(&ctx, a(0), a(1), &q),
            "GetCampaignVersion" => self.get_campaign_version(&ctx, a(0), a(1), a(2)),
            "GetCampaignActivities" => self.get_campaign_activities(&ctx, a(0), a(1), &q),
            "GetCampaignDateRangeKpi" => self.get_campaign_kpi(&ctx, a(0), a(1), a(2)),
            // segments
            "CreateSegment" => self.create_segment(&ctx, a(0), &body),
            "GetSegment" => self.get_segment(&ctx, a(0), a(1)),
            "UpdateSegment" => self.update_segment(&ctx, a(0), a(1), &body),
            "DeleteSegment" => self.delete_segment(&ctx, a(0), a(1)),
            "GetSegments" => self.get_segments(&ctx, a(0), &q),
            "GetSegmentVersions" => self.get_segment_versions(&ctx, a(0), a(1), &q),
            "GetSegmentVersion" => self.get_segment_version(&ctx, a(0), a(1), a(2)),
            "GetSegmentImportJobs" => self.get_segment_import_jobs(&ctx, a(0), a(1), &q),
            "GetSegmentExportJobs" => self.get_segment_export_jobs(&ctx, a(0), a(1), &q),
            // endpoints / users
            "GetEndpoint" => self.get_endpoint(&ctx, a(0), a(1)),
            "UpdateEndpoint" => self.update_endpoint(&ctx, a(0), a(1), &body),
            "DeleteEndpoint" => self.delete_endpoint(&ctx, a(0), a(1)),
            "UpdateEndpointsBatch" => self.update_endpoints_batch(&ctx, a(0), &body),
            "RemoveAttributes" => self.remove_attributes(&ctx, a(0), a(1), &body),
            "GetUserEndpoints" => self.get_user_endpoints(&ctx, a(0), a(1)),
            "DeleteUserEndpoints" => self.delete_user_endpoints(&ctx, a(0), a(1)),
            "GetInAppMessages" => self.get_in_app_messages(&ctx, a(0), a(1)),
            // channels
            "GetChannels" => self.get_channels(&ctx, a(0)),
            "GetAdmChannel"
            | "GetApnsChannel"
            | "GetApnsSandboxChannel"
            | "GetApnsVoipChannel"
            | "GetApnsVoipSandboxChannel"
            | "GetBaiduChannel"
            | "GetEmailChannel"
            | "GetGcmChannel"
            | "GetSmsChannel"
            | "GetVoiceChannel" => self.get_channel(&ctx, a(0), channel_key_for(action)),
            "UpdateAdmChannel"
            | "UpdateApnsChannel"
            | "UpdateApnsSandboxChannel"
            | "UpdateApnsVoipChannel"
            | "UpdateApnsVoipSandboxChannel"
            | "UpdateBaiduChannel"
            | "UpdateEmailChannel"
            | "UpdateGcmChannel"
            | "UpdateSmsChannel"
            | "UpdateVoiceChannel" => {
                self.update_channel(&ctx, a(0), channel_key_for(action), &body)
            }
            "DeleteAdmChannel"
            | "DeleteApnsChannel"
            | "DeleteApnsSandboxChannel"
            | "DeleteApnsVoipChannel"
            | "DeleteApnsVoipSandboxChannel"
            | "DeleteBaiduChannel"
            | "DeleteEmailChannel"
            | "DeleteGcmChannel"
            | "DeleteSmsChannel"
            | "DeleteVoiceChannel" => self.delete_channel(&ctx, a(0), channel_key_for(action)),
            // messaging
            "SendMessages" => self.send_messages(&ctx, a(0), &body),
            "SendUsersMessages" => self.send_users_messages(&ctx, a(0), &body),
            "SendOTPMessage" => self.send_otp_message(&ctx, a(0), &body),
            "VerifyOTPMessage" => self.verify_otp_message(&ctx, a(0), &body),
            "PhoneNumberValidate" => self.phone_number_validate(&ctx, &body),
            "PutEvents" => self.put_events(&ctx, a(0), &body),
            "PutEventStream" => self.put_event_stream(&ctx, a(0), &body),
            "GetEventStream" => self.get_event_stream(&ctx, a(0)),
            "DeleteEventStream" => self.delete_event_stream(&ctx, a(0)),
            // jobs
            "CreateImportJob" => self.create_import_job(&ctx, a(0), &body),
            "GetImportJob" => self.get_import_job(&ctx, a(0), a(1)),
            "GetImportJobs" => self.get_import_jobs(&ctx, a(0), &q),
            "CreateExportJob" => self.create_export_job(&ctx, a(0), &body),
            "GetExportJob" => self.get_export_job(&ctx, a(0), a(1)),
            "GetExportJobs" => self.get_export_jobs(&ctx, a(0), &q),
            // journeys
            "CreateJourney" => self.create_journey(&ctx, a(0), &body),
            "GetJourney" => self.get_journey(&ctx, a(0), a(1)),
            "UpdateJourney" => self.update_journey(&ctx, a(0), a(1), &body),
            "UpdateJourneyState" => self.update_journey_state(&ctx, a(0), a(1), &body),
            "DeleteJourney" => self.delete_journey(&ctx, a(0), a(1)),
            "ListJourneys" => self.list_journeys(&ctx, a(0), &q),
            "GetJourneyRuns" => self.get_journey_runs(&ctx, a(0), a(1), &q),
            "GetJourneyDateRangeKpi" => self.get_journey_kpi(&ctx, a(0), a(1), a(2)),
            "GetJourneyExecutionMetrics" => self.get_journey_execution_metrics(&ctx, a(0), a(1)),
            "GetJourneyExecutionActivityMetrics" => {
                self.get_journey_execution_activity_metrics(&ctx, a(0), a(1), a(2))
            }
            "GetJourneyRunExecutionMetrics" => {
                self.get_journey_run_execution_metrics(&ctx, a(0), a(1), a(2))
            }
            "GetJourneyRunExecutionActivityMetrics" => {
                self.get_journey_run_execution_activity_metrics(&ctx, a(0), a(1), a(2), a(3))
            }
            // templates
            "CreateEmailTemplate"
            | "CreateInAppTemplate"
            | "CreatePushTemplate"
            | "CreateSmsTemplate"
            | "CreateVoiceTemplate" => {
                self.create_template(&ctx, a(0), template_type_for(action), &body)
            }
            "GetEmailTemplate" | "GetInAppTemplate" | "GetPushTemplate" | "GetSmsTemplate"
            | "GetVoiceTemplate" => self.get_template(&ctx, a(0), template_type_for(action), &q),
            "UpdateEmailTemplate"
            | "UpdateInAppTemplate"
            | "UpdatePushTemplate"
            | "UpdateSmsTemplate"
            | "UpdateVoiceTemplate" => {
                self.update_template(&ctx, a(0), template_type_for(action), &body, &q)
            }
            "DeleteEmailTemplate"
            | "DeleteInAppTemplate"
            | "DeletePushTemplate"
            | "DeleteSmsTemplate"
            | "DeleteVoiceTemplate" => self.delete_template(&ctx, a(0), template_type_for(action)),
            "ListTemplates" => self.list_templates(&ctx, &q),
            "ListTemplateVersions" => self.list_template_versions(&ctx, a(0), a(1), &q),
            "UpdateTemplateActiveVersion" => {
                self.update_template_active_version(&ctx, a(0), a(1), &body)
            }
            // recommenders
            "CreateRecommenderConfiguration" => self.create_recommender(&ctx, &body),
            "GetRecommenderConfiguration" => self.get_recommender(&ctx, a(0)),
            "UpdateRecommenderConfiguration" => self.update_recommender(&ctx, a(0), &body),
            "DeleteRecommenderConfiguration" => self.delete_recommender(&ctx, a(0)),
            "GetRecommenderConfigurations" => self.get_recommenders(&ctx, &q),
            // tags
            "TagResource" => self.tag_resource(&ctx, a(0), &body),
            "UntagResource" => self.untag_resource(&ctx, a(0), &q),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, a(0)),
            _ => Err(AwsServiceError::action_not_implemented("pinpoint", action)),
        }
    }
}

/// The canonical channel key implied by a channel action name.
fn channel_key_for(action: &str) -> &'static str {
    match action {
        a if a.contains("AdmChannel") => "adm",
        a if a.contains("ApnsVoipSandboxChannel") => "apns_voip_sandbox",
        a if a.contains("ApnsVoipChannel") => "apns_voip",
        a if a.contains("ApnsSandboxChannel") => "apns_sandbox",
        a if a.contains("ApnsChannel") => "apns",
        a if a.contains("BaiduChannel") => "baidu",
        a if a.contains("EmailChannel") => "email",
        a if a.contains("GcmChannel") => "gcm",
        a if a.contains("SmsChannel") => "sms",
        _ => "voice",
    }
}

/// The template type implied by a typed-template action name.
fn template_type_for(action: &str) -> &'static str {
    match action {
        a if a.contains("Email") => "EMAIL",
        a if a.contains("InApp") => "INAPP",
        a if a.contains("Push") => "PUSH",
        a if a.contains("Sms") => "SMS",
        _ => "VOICE",
    }
}

// ===================== shared helpers =====================

impl PinpointService {
    /// Look up an application read-only, erroring `NotFoundException` when
    /// absent.
    pub(crate) fn with_app<T>(
        &self,
        account: &str,
        app_id: &str,
        f: impl FnOnce(&App) -> Result<T, AwsServiceError>,
    ) -> Result<T, AwsServiceError> {
        let guard = self.state.read();
        let app = guard
            .get(account)
            .and_then(|d| d.apps.get(app_id))
            .ok_or_else(|| not_found_app(app_id))?;
        f(app)
    }
}

pub(crate) fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

pub(crate) fn created(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::CREATED, v))
}

pub(crate) fn accepted(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::ACCEPTED, v))
}

pub(crate) fn no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("The request body is malformed: {e}")))
}

pub(crate) fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

pub(crate) fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

pub(crate) fn not_found_app(app_id: &str) -> AwsServiceError {
    not_found(&format!("Application with id '{app_id}' does not exist."))
}

pub(crate) fn str_field(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Copy a member from `src` into `out` verbatim when present and non-null.
pub(crate) fn copy_present(out: &mut Map<String, Value>, src: &Value, key: &str) {
    if let Some(v) = src.get(key) {
        if !v.is_null() {
            out.insert(key.to_string(), v.clone());
        }
    }
}

/// Copy each listed member from `src` into `out` when present and non-null.
pub(crate) fn copy_many(out: &mut Map<String, Value>, src: &Value, keys: &[&str]) {
    for k in keys {
        copy_present(out, src, k);
    }
}

pub(crate) fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

pub(crate) fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Paginate wire objects with Pinpoint's `PageSize` / `Token` (or `NextToken`)
/// query params, returning the page plus an opaque continuation token.
pub(crate) fn paginate(
    items: Vec<Value>,
    q: &[(String, String)],
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let max = match query_one(q, "PageSize") {
        Some(v) => {
            let n: i64 = v
                .parse()
                .map_err(|_| bad_request("PageSize must be an integer."))?;
            if n <= 0 {
                return Err(bad_request("PageSize must be a positive integer."));
            }
            n as usize
        }
        None => usize::MAX,
    };
    let token = query_one(q, "Token").or_else(|| query_one(q, "NextToken"));
    let start = token.and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    let end = start.saturating_add(max).min(items.len());
    let page: Vec<Value> = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    Ok((page, next))
}
