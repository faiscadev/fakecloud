//! HSM objects, snapshot copy (grants/enable/retention), snapshot schedules,
//! scheduled actions, usage limits, logging, resource policies, and tags.

use chrono::Utc;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::helpers::*;
use super::RedshiftService;
use crate::state::{
    HsmClientCertificate, HsmConfiguration, LoggingStatus, ScheduledAction, SnapshotCopyGrant,
    SnapshotCopyStatus, SnapshotSchedule, UsageLimit,
};

fn render_hsm_client_certificate(c: &HsmClientCertificate) -> String {
    format!(
        "<HsmClientCertificateIdentifier>{id}</HsmClientCertificateIdentifier>\
         <HsmClientCertificatePublicKey>{key}</HsmClientCertificatePublicKey>{tags}",
        id = xml_escape(&c.hsm_client_certificate_identifier),
        key = xml_escape(&c.hsm_client_certificate_public_key),
        tags = render_tags(&c.tags),
    )
}

fn render_hsm_configuration(c: &HsmConfiguration) -> String {
    format!(
        "<HsmConfigurationIdentifier>{id}</HsmConfigurationIdentifier><Description>{desc}</Description>\
         <HsmIpAddress>{ip}</HsmIpAddress><HsmPartitionName>{part}</HsmPartitionName>{tags}",
        id = xml_escape(&c.hsm_configuration_identifier),
        desc = xml_escape(&c.description),
        ip = xml_escape(&c.hsm_ip_address),
        part = xml_escape(&c.hsm_partition_name),
        tags = render_tags(&c.tags),
    )
}

fn render_snapshot_copy_grant(g: &SnapshotCopyGrant) -> String {
    format!(
        "<SnapshotCopyGrantName>{name}</SnapshotCopyGrantName><KmsKeyId>{kms}</KmsKeyId>{tags}",
        name = xml_escape(&g.snapshot_copy_grant_name),
        kms = xml_escape(&g.kms_key_id),
        tags = render_tags(&g.tags),
    )
}

fn render_snapshot_schedule(s: &SnapshotSchedule) -> String {
    let defs: String = s
        .schedule_definitions
        .iter()
        .map(|d| tag_elem("ScheduleDefinition", d))
        .collect();
    let clusters: String = s
        .associated_clusters
        .iter()
        .map(|c| {
            format!(
                "<ClusterAssociatedToSchedule><ClusterIdentifier>{}</ClusterIdentifier><ScheduleAssociationState>ACTIVE</ScheduleAssociationState></ClusterAssociatedToSchedule>",
                xml_escape(c)
            )
        })
        .collect();
    format!(
        "<ScheduleIdentifier>{id}</ScheduleIdentifier><ScheduleDescription>{desc}</ScheduleDescription>\
         <ScheduleDefinitions>{defs}</ScheduleDefinitions>\
         <AssociatedClusterCount>{count}</AssociatedClusterCount>\
         <AssociatedClusters>{clusters}</AssociatedClusters>{tags}",
        id = xml_escape(&s.schedule_identifier),
        desc = xml_escape(&s.schedule_description),
        count = s.associated_cluster_count,
        tags = render_tags(&s.tags),
    )
}

fn render_scheduled_action(a: &ScheduledAction) -> String {
    let target = a
        .target_action
        .as_ref()
        .map(|t| format!("<TargetAction>{t}</TargetAction>"))
        .unwrap_or_default();
    format!(
        "<ScheduledActionName>{name}</ScheduledActionName>{target}<Schedule>{schedule}</Schedule>\
         <IamRole>{iam}</IamRole><ScheduledActionDescription>{desc}</ScheduledActionDescription>\
         <State>{state}</State>{start}{end}",
        name = xml_escape(&a.scheduled_action_name),
        schedule = xml_escape(&a.schedule),
        iam = xml_escape(&a.iam_role),
        desc = xml_escape(&a.scheduled_action_description),
        state = xml_escape(&a.state),
        start = a
            .start_time
            .map(|t| format!(
                "<StartTime>{}</StartTime>",
                t.format("%Y-%m-%dT%H:%M:%S%.3fZ")
            ))
            .unwrap_or_default(),
        end = a
            .end_time
            .map(|t| format!("<EndTime>{}</EndTime>", t.format("%Y-%m-%dT%H:%M:%S%.3fZ")))
            .unwrap_or_default(),
    )
}

fn render_usage_limit(u: &UsageLimit) -> String {
    format!(
        "<UsageLimitId>{id}</UsageLimitId><ClusterIdentifier>{cluster}</ClusterIdentifier>\
         <FeatureType>{feature}</FeatureType><LimitType>{ltype}</LimitType><Amount>{amount}</Amount>\
         <Period>{period}</Period><BreachAction>{action}</BreachAction>{tags}",
        id = xml_escape(&u.usage_limit_id),
        cluster = xml_escape(&u.cluster_identifier),
        feature = xml_escape(&u.feature_type),
        ltype = xml_escape(&u.limit_type),
        amount = u.amount,
        period = xml_escape(&u.period),
        action = xml_escape(&u.breach_action),
        tags = render_tags(&u.tags),
    )
}

fn render_logging(l: &LoggingStatus) -> String {
    // `LogTypeList` uses the default `member` wrapper.
    let log_exports = if l.log_exports.is_empty() {
        String::new()
    } else {
        let inner: String = l
            .log_exports
            .iter()
            .map(|e| tag_elem("member", e))
            .collect();
        format!("<LogExports>{inner}</LogExports>")
    };
    format!(
        "<LoggingEnabled>{enabled}</LoggingEnabled>{bucket}{prefix}{dest}{log_exports}{time}",
        enabled = l.logging_enabled,
        bucket = opt_elem("BucketName", l.bucket_name.as_deref()),
        prefix = opt_elem("S3KeyPrefix", l.s3_key_prefix.as_deref()),
        dest = opt_elem("LogDestinationType", l.log_destination_type.as_deref()),
        time = l
            .last_successful_delivery_time
            .map(|t| format!(
                "<LastSuccessfulDeliveryTime>{}</LastSuccessfulDeliveryTime>",
                t.format("%Y-%m-%dT%H:%M:%S%.3fZ")
            ))
            .unwrap_or_default(),
    )
}

impl RedshiftService {
    // ── HSM client certificates ───────────────────────────────────
    pub(super) fn create_hsm_client_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "HsmClientCertificateIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.hsm_client_certificates.contains_key(&id) {
            return Err(hsm_client_certificate_already_exists(&id));
        }
        let cert = HsmClientCertificate {
            hsm_client_certificate_identifier: id.clone(),
            hsm_client_certificate_public_key: format!(
                "-----BEGIN CERTIFICATE-----\nFAKECLOUD{}\n-----END CERTIFICATE-----",
                Uuid::new_v4().simple()
            ),
            tags: parse_tags(req),
        };
        acct.hsm_client_certificates.insert(id, cert.clone());
        Ok(xml_resp(
            "CreateHsmClientCertificate",
            format!(
                "<HsmClientCertificate>{}</HsmClientCertificate>",
                render_hsm_client_certificate(&cert)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_hsm_client_certificates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<HsmClientCertificate> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.hsm_client_certificates.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|c| {
                format!(
                    "<HsmClientCertificate>{}</HsmClientCertificate>",
                    render_hsm_client_certificate(c)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeHsmClientCertificates",
            format!(
                "{}<HsmClientCertificates>{inner}</HsmClientCertificates>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_hsm_client_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "HsmClientCertificateIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.hsm_client_certificates.remove(&id).is_none() {
            return Err(hsm_client_certificate_not_found(&id));
        }
        Ok(xml_metadata_only(
            "DeleteHsmClientCertificate",
            &req.request_id,
        ))
    }

    // ── HSM configurations ────────────────────────────────────────
    pub(super) fn create_hsm_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "HsmConfigurationIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.hsm_configurations.contains_key(&id) {
            return Err(hsm_configuration_already_exists(&id));
        }
        let cfg = HsmConfiguration {
            hsm_configuration_identifier: id.clone(),
            description: param(req, "Description").unwrap_or_default(),
            hsm_ip_address: param(req, "HsmIpAddress").unwrap_or_default(),
            hsm_partition_name: param(req, "HsmPartitionName").unwrap_or_default(),
            tags: parse_tags(req),
        };
        acct.hsm_configurations.insert(id, cfg.clone());
        Ok(xml_resp(
            "CreateHsmConfiguration",
            format!(
                "<HsmConfiguration>{}</HsmConfiguration>",
                render_hsm_configuration(&cfg)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_hsm_configurations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<HsmConfiguration> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.hsm_configurations.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|c| {
                format!(
                    "<HsmConfiguration>{}</HsmConfiguration>",
                    render_hsm_configuration(c)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeHsmConfigurations",
            format!(
                "{}<HsmConfigurations>{inner}</HsmConfigurations>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_hsm_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "HsmConfigurationIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.hsm_configurations.remove(&id).is_none() {
            return Err(hsm_configuration_not_found(&id));
        }
        Ok(xml_metadata_only("DeleteHsmConfiguration", &req.request_id))
    }

    // ── Snapshot copy grants ──────────────────────────────────────
    pub(super) fn create_snapshot_copy_grant(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "SnapshotCopyGrantName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshot_copy_grants.contains_key(&name) {
            return Err(snapshot_copy_grant_already_exists(&name));
        }
        let g = SnapshotCopyGrant {
            snapshot_copy_grant_name: name.clone(),
            kms_key_id: param_or(req, "KmsKeyId", "default"),
            tags: parse_tags(req),
        };
        acct.snapshot_copy_grants.insert(name, g.clone());
        Ok(xml_resp(
            "CreateSnapshotCopyGrant",
            format!(
                "<SnapshotCopyGrant>{}</SnapshotCopyGrant>",
                render_snapshot_copy_grant(&g)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_snapshot_copy_grants(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<SnapshotCopyGrant> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.snapshot_copy_grants.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|g| {
                format!(
                    "<SnapshotCopyGrant>{}</SnapshotCopyGrant>",
                    render_snapshot_copy_grant(g)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeSnapshotCopyGrants",
            format!(
                "{}<SnapshotCopyGrants>{inner}</SnapshotCopyGrants>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_snapshot_copy_grant(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "SnapshotCopyGrantName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshot_copy_grants.remove(&name).is_none() {
            return Err(snapshot_copy_grant_not_found(&name));
        }
        Ok(xml_metadata_only(
            "DeleteSnapshotCopyGrant",
            &req.request_id,
        ))
    }

    // ── Snapshot copy (per-cluster) ───────────────────────────────
    pub(super) fn enable_snapshot_copy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_snapshot_copy(req, "EnableSnapshotCopy")
    }

    pub(super) fn disable_snapshot_copy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_snapshot_copy(req, "DisableSnapshotCopy")
    }

    pub(super) fn modify_snapshot_copy_retention_period(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_snapshot_copy(req, "ModifySnapshotCopyRetentionPeriod")
    }

    fn cluster_snapshot_copy(
        &self,
        req: &AwsRequest,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        match action {
            "EnableSnapshotCopy" => {
                c.snapshot_copy = Some(SnapshotCopyStatus {
                    destination_region: param(req, "DestinationRegion").unwrap_or_default(),
                    retention_period: long_param(req, "RetentionPeriod").unwrap_or(7),
                    manual_snapshot_retention_period: int_param(
                        req,
                        "ManualSnapshotRetentionPeriod",
                    )
                    .unwrap_or(-1),
                    snapshot_copy_grant_name: param(req, "SnapshotCopyGrantName"),
                });
            }
            "DisableSnapshotCopy" => {
                c.snapshot_copy = None;
            }
            "ModifySnapshotCopyRetentionPeriod" => {
                if let Some(sc) = c.snapshot_copy.as_mut() {
                    if let Some(r) = long_param(req, "RetentionPeriod") {
                        sc.retention_period = r;
                    }
                    if bool_param(req, "Manual").unwrap_or(false) {
                        sc.manual_snapshot_retention_period =
                            long_param(req, "RetentionPeriod").unwrap_or(-1) as i32;
                    }
                }
            }
            _ => {}
        }
        let cluster = c.clone();
        Ok(xml_resp(
            action,
            format!(
                "<Cluster>{}</Cluster>",
                super::clusters::render_cluster(&cluster)
            ),
            &req.request_id,
        ))
    }

    // ── Snapshot schedules ────────────────────────────────────────
    pub(super) fn create_snapshot_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ScheduleIdentifier")
            .unwrap_or_else(|| format!("schedule-{}", fakecloud_core::ids::short_id(8)));
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshot_schedules.contains_key(&id) {
            return Err(snapshot_schedule_already_exists(&id));
        }
        let s = SnapshotSchedule {
            schedule_identifier: id.clone(),
            schedule_description: param(req, "ScheduleDescription").unwrap_or_default(),
            schedule_definitions: member_list(req, "ScheduleDefinitions", "ScheduleDefinition"),
            tags: parse_tags(req),
            associated_cluster_count: 0,
            associated_clusters: Vec::new(),
        };
        acct.snapshot_schedules.insert(id, s.clone());
        Ok(xml_resp(
            "CreateSnapshotSchedule",
            render_snapshot_schedule(&s),
            &req.request_id,
        ))
    }

    pub(super) fn describe_snapshot_schedules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let schedule_filter = param(req, "ScheduleIdentifier");
        let cluster_filter = param(req, "ClusterIdentifier");
        let guard = self.state.read();
        let all: Vec<SnapshotSchedule> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.snapshot_schedules
                    .values()
                    .filter(|s| {
                        schedule_filter
                            .as_ref()
                            .map(|f| &s.schedule_identifier == f)
                            .unwrap_or(true)
                    })
                    .filter(|s| {
                        cluster_filter
                            .as_ref()
                            .map(|c| s.associated_clusters.contains(c))
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|s| {
                format!(
                    "<SnapshotSchedule>{}</SnapshotSchedule>",
                    render_snapshot_schedule(s)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeSnapshotSchedules",
            format!(
                "{}<SnapshotSchedules>{inner}</SnapshotSchedules>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_snapshot_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ScheduleIdentifier").unwrap_or_default();
        let defs = member_list(req, "ScheduleDefinitions", "ScheduleDefinition");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let s = acct
            .snapshot_schedules
            .get_mut(&id)
            .ok_or_else(|| snapshot_schedule_not_found(&id))?;
        if !defs.is_empty() {
            s.schedule_definitions = defs;
        }
        Ok(xml_resp(
            "ModifySnapshotSchedule",
            render_snapshot_schedule(s),
            &req.request_id,
        ))
    }

    pub(super) fn delete_snapshot_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ScheduleIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshot_schedules.remove(&id).is_none() {
            return Err(snapshot_schedule_not_found(&id));
        }
        Ok(xml_metadata_only("DeleteSnapshotSchedule", &req.request_id))
    }

    // ── Scheduled actions ─────────────────────────────────────────
    pub(super) fn create_scheduled_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ScheduledActionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.scheduled_actions.contains_key(&name) {
            return Err(scheduled_action_already_exists(&name));
        }
        let a = ScheduledAction {
            scheduled_action_name: name.clone(),
            target_action: extract_target_action(req),
            schedule: param(req, "Schedule").unwrap_or_default(),
            iam_role: param(req, "IamRole").unwrap_or_default(),
            scheduled_action_description: param(req, "ScheduledActionDescription")
                .unwrap_or_default(),
            state: if bool_param(req, "Enable").unwrap_or(true) {
                "ACTIVE".to_string()
            } else {
                "DISABLED".to_string()
            },
            start_time: None,
            end_time: None,
            enable: bool_param(req, "Enable").unwrap_or(true),
        };
        acct.scheduled_actions.insert(name, a.clone());
        Ok(xml_resp(
            "CreateScheduledAction",
            render_scheduled_action(&a),
            &req.request_id,
        ))
    }

    pub(super) fn describe_scheduled_actions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<ScheduledAction> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.scheduled_actions.values().cloned().collect())
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|a| {
                format!(
                    "<ScheduledAction>{}</ScheduledAction>",
                    render_scheduled_action(a)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeScheduledActions",
            format!(
                "{}<ScheduledActions>{inner}</ScheduledActions>",
                render_marker(next)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn modify_scheduled_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ScheduledActionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let a = acct
            .scheduled_actions
            .get_mut(&name)
            .ok_or_else(|| scheduled_action_not_found(&name))?;
        if let Some(s) = param(req, "Schedule") {
            a.schedule = s;
        }
        if let Some(r) = param(req, "IamRole") {
            a.iam_role = r;
        }
        if let Some(d) = param(req, "ScheduledActionDescription") {
            a.scheduled_action_description = d;
        }
        if let Some(e) = bool_param(req, "Enable") {
            a.enable = e;
            a.state = if e {
                "ACTIVE".to_string()
            } else {
                "DISABLED".to_string()
            };
        }
        if let Some(t) = extract_target_action(req) {
            a.target_action = Some(t);
        }
        Ok(xml_resp(
            "ModifyScheduledAction",
            render_scheduled_action(a),
            &req.request_id,
        ))
    }

    pub(super) fn delete_scheduled_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = param(req, "ScheduledActionName").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.scheduled_actions.remove(&name).is_none() {
            return Err(scheduled_action_not_found(&name));
        }
        Ok(xml_metadata_only("DeleteScheduledAction", &req.request_id))
    }

    // ── Usage limits ──────────────────────────────────────────────
    pub(super) fn create_usage_limit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster = param(req, "ClusterIdentifier").unwrap_or_default();
        let feature = param(req, "FeatureType").unwrap_or_default();
        let limit_type = param(req, "LimitType").unwrap_or_default();
        let id = format!("usagelimit-{}", fakecloud_core::ids::short_id(12));
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        // Reject duplicate feature/limit-type on the same cluster (AWS behavior).
        if acct.usage_limits.values().any(|u| {
            u.cluster_identifier == cluster
                && u.feature_type == feature
                && u.limit_type == limit_type
        }) {
            return Err(usage_limit_already_exists(&feature));
        }
        let u = UsageLimit {
            usage_limit_id: id.clone(),
            cluster_identifier: cluster,
            feature_type: feature,
            limit_type,
            amount: long_param(req, "Amount").unwrap_or(0),
            period: param_or(req, "Period", "monthly"),
            breach_action: param_or(req, "BreachAction", "log"),
            tags: parse_tags(req),
        };
        acct.usage_limits.insert(id, u.clone());
        Ok(xml_resp(
            "CreateUsageLimit",
            render_usage_limit(&u),
            &req.request_id,
        ))
    }

    pub(super) fn describe_usage_limits(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster_filter = param(req, "ClusterIdentifier");
        let id_filter = param(req, "UsageLimitId");
        let guard = self.state.read();
        let all: Vec<UsageLimit> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.usage_limits
                    .values()
                    .filter(|u| {
                        cluster_filter
                            .as_ref()
                            .map(|c| &u.cluster_identifier == c)
                            .unwrap_or(true)
                            && id_filter
                                .as_ref()
                                .map(|i| &u.usage_limit_id == i)
                                .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|u| format!("<member>{}</member>", render_usage_limit(u)))
            .collect();
        Ok(xml_resp(
            "DescribeUsageLimits",
            format!("{}<UsageLimits>{inner}</UsageLimits>", render_marker(next)),
            &req.request_id,
        ))
    }

    pub(super) fn modify_usage_limit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "UsageLimitId").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let u = acct
            .usage_limits
            .get_mut(&id)
            .ok_or_else(|| usage_limit_not_found(&id))?;
        if let Some(a) = long_param(req, "Amount") {
            u.amount = a;
        }
        if let Some(b) = param(req, "BreachAction") {
            u.breach_action = b;
        }
        Ok(xml_resp(
            "ModifyUsageLimit",
            render_usage_limit(u),
            &req.request_id,
        ))
    }

    pub(super) fn delete_usage_limit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "UsageLimitId").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.usage_limits.remove(&id).is_none() {
            return Err(usage_limit_not_found(&id));
        }
        Ok(xml_metadata_only("DeleteUsageLimit", &req.request_id))
    }

    // ── Logging (per-cluster) ─────────────────────────────────────
    pub(super) fn enable_logging(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if !acct.clusters.contains_key(&id) {
            return Err(cluster_not_found(&id));
        }
        let status = LoggingStatus {
            logging_enabled: true,
            bucket_name: param(req, "BucketName"),
            s3_key_prefix: param(req, "S3KeyPrefix"),
            log_destination_type: param(req, "LogDestinationType"),
            log_exports: member_list(req, "LogExports", "member"),
            last_successful_delivery_time: Some(Utc::now()),
        };
        acct.logging.insert(id, status.clone());
        Ok(xml_resp(
            "EnableLogging",
            render_logging(&status),
            &req.request_id,
        ))
    }

    pub(super) fn disable_logging(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if !acct.clusters.contains_key(&id) {
            return Err(cluster_not_found(&id));
        }
        acct.logging.remove(&id);
        let status = LoggingStatus {
            logging_enabled: false,
            bucket_name: None,
            s3_key_prefix: None,
            log_destination_type: None,
            log_exports: Vec::new(),
            last_successful_delivery_time: None,
        };
        Ok(xml_resp(
            "DisableLogging",
            render_logging(&status),
            &req.request_id,
        ))
    }

    pub(super) fn describe_logging_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let guard = self.state.read();
        let acct = guard
            .accounts
            .get(&req.account_id)
            .ok_or_else(|| cluster_not_found(&id))?;
        if !acct.clusters.contains_key(&id) {
            return Err(cluster_not_found(&id));
        }
        let status = acct.logging.get(&id).cloned().unwrap_or(LoggingStatus {
            logging_enabled: false,
            bucket_name: None,
            s3_key_prefix: None,
            log_destination_type: None,
            log_exports: Vec::new(),
            last_successful_delivery_time: None,
        });
        Ok(xml_resp(
            "DescribeLoggingStatus",
            render_logging(&status),
            &req.request_id,
        ))
    }

    // ── Resource policies ─────────────────────────────────────────
    pub(super) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "ResourceArn").unwrap_or_default();
        let policy = param(req, "Policy").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        acct.resource_policies.insert(arn.clone(), policy.clone());
        Ok(xml_resp(
            "PutResourcePolicy",
            format!(
                "<ResourcePolicy><ResourceArn>{}</ResourceArn><Policy>{}</Policy></ResourcePolicy>",
                xml_escape(&arn),
                xml_escape(&policy)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "ResourceArn").unwrap_or_default();
        let guard = self.state.read();
        let policy = guard
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.resource_policies.get(&arn))
            .cloned()
            .ok_or_else(|| resource_not_found(format!("Resource policy for {arn} not found.")))?;
        Ok(xml_resp(
            "GetResourcePolicy",
            format!(
                "<ResourcePolicy><ResourceArn>{}</ResourceArn><Policy>{}</Policy></ResourcePolicy>",
                xml_escape(&arn),
                xml_escape(&policy)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "ResourceArn").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.resource_policies.remove(&arn).is_none() {
            return Err(resource_not_found(format!(
                "Resource policy for {arn} not found."
            )));
        }
        Ok(xml_metadata_only("DeleteResourcePolicy", &req.request_id))
    }

    // ── Tags ──────────────────────────────────────────────────────
    pub(super) fn create_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "ResourceName").unwrap_or_default();
        let new_tags = parse_tags(req);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let tags = resolve_tags_mut(acct, &arn)
            .ok_or_else(|| resource_not_found(format!("Cannot find resource with ARN: {arn}")))?;
        // Upsert: a repeated key replaces the previous value, matching AWS.
        for nt in new_tags {
            if let Some(existing) = tags.iter_mut().find(|t| t.key == nt.key) {
                existing.value = nt.value;
            } else {
                tags.push(nt);
            }
        }
        Ok(xml_metadata_only("CreateTags", &req.request_id))
    }

    pub(super) fn delete_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = param(req, "ResourceName").unwrap_or_default();
        let keys = member_list(req, "TagKeys", "TagKey");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let tags = resolve_tags_mut(acct, &arn)
            .ok_or_else(|| resource_not_found(format!("Cannot find resource with ARN: {arn}")))?;
        tags.retain(|t| !keys.contains(&t.key));
        Ok(xml_metadata_only("DeleteTags", &req.request_id))
    }

    pub(super) fn describe_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Aggregate tags across all resource families for this account. AWS lets
        // the caller narrow by `ResourceName` (a specific ARN) and/or
        // `ResourceType`; honour both.
        let name_filter = param(req, "ResourceName");
        let type_filter = param(req, "ResourceType");
        let guard = self.state.read();
        let mut rows = String::new();
        if let Some(acct) = guard.accounts.get(&req.account_id) {
            let region = &req.region;
            let account_id = &req.account_id;
            let mut push = |resource_type: &str, name: &str, tags: &[crate::state::Tag]| {
                let arn = Arn::new(
                    "redshift",
                    region,
                    account_id,
                    &format!("{resource_type}:{name}"),
                )
                .to_string();
                if name_filter.as_ref().is_some_and(|n| n != &arn) {
                    return;
                }
                if type_filter.as_ref().is_some_and(|t| t != resource_type) {
                    return;
                }
                for t in tags {
                    rows.push_str(&format!(
                        "<TaggedResource><Tag><Key>{}</Key><Value>{}</Value></Tag><ResourceName>{}</ResourceName><ResourceType>{}</ResourceType></TaggedResource>",
                        xml_escape(&t.key),
                        xml_escape(&t.value),
                        xml_escape(&arn),
                        xml_escape(resource_type)
                    ));
                }
            };
            for c in acct.clusters.values() {
                push("cluster", &c.cluster_identifier, &c.tags);
            }
            for s in acct.snapshots.values() {
                push("snapshot", &s.snapshot_identifier, &s.tags);
            }
            for g in acct.parameter_groups.values() {
                push("parametergroup", &g.parameter_group_name, &g.tags);
            }
            for g in acct.subnet_groups.values() {
                push("subnetgroup", &g.cluster_subnet_group_name, &g.tags);
            }
            for g in acct.security_groups.values() {
                push("securitygroup", &g.cluster_security_group_name, &g.tags);
            }
            for g in acct.snapshot_copy_grants.values() {
                push("snapshotcopygrant", &g.snapshot_copy_grant_name, &g.tags);
            }
            for s in acct.snapshot_schedules.values() {
                push("snapshotschedule", &s.schedule_identifier, &s.tags);
            }
            for h in acct.hsm_client_certificates.values() {
                push(
                    "hsmclientcertificate",
                    &h.hsm_client_certificate_identifier,
                    &h.tags,
                );
            }
            for h in acct.hsm_configurations.values() {
                push("hsmconfiguration", &h.hsm_configuration_identifier, &h.tags);
            }
            for s in acct.event_subscriptions.values() {
                push("eventsubscription", &s.subscription_name, &s.tags);
            }
            for u in acct.usage_limits.values() {
                push("usagelimit", &u.usage_limit_id, &u.tags);
            }
        }
        Ok(xml_resp(
            "DescribeTags",
            format!("<TaggedResources>{rows}</TaggedResources>"),
            &req.request_id,
        ))
    }
}

/// Resolve an ARN of the form `arn:aws:redshift:<region>:<account>:<type>:<name>`
/// to a mutable handle on that resource's tag list, so CreateTags/DeleteTags
/// mutate real state. Returns `None` for an unparseable ARN or an unknown /
/// missing resource.
fn resolve_tags_mut<'a>(
    acct: &'a mut crate::state::RedshiftState,
    arn: &str,
) -> Option<&'a mut Vec<crate::state::Tag>> {
    // Split off the `<type>:<name>` resource portion after the account field.
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    if parts.len() < 7 || parts[2] != "redshift" {
        return None;
    }
    let resource_type = parts[5];
    let name = parts[6];
    match resource_type {
        "cluster" => acct.clusters.get_mut(name).map(|c| &mut c.tags),
        "snapshot" => {
            // Snapshot ARNs are `snapshot:<cluster>/<snapshot>`; key on the id.
            let snap_id = name.rsplit('/').next().unwrap_or(name);
            acct.snapshots.get_mut(snap_id).map(|s| &mut s.tags)
        }
        "parametergroup" => acct.parameter_groups.get_mut(name).map(|g| &mut g.tags),
        "subnetgroup" => acct.subnet_groups.get_mut(name).map(|g| &mut g.tags),
        "securitygroup" => acct.security_groups.get_mut(name).map(|g| &mut g.tags),
        "snapshotcopygrant" => acct.snapshot_copy_grants.get_mut(name).map(|g| &mut g.tags),
        "snapshotschedule" => acct.snapshot_schedules.get_mut(name).map(|s| &mut s.tags),
        "hsmclientcertificate" => acct
            .hsm_client_certificates
            .get_mut(name)
            .map(|h| &mut h.tags),
        "hsmconfiguration" => acct.hsm_configurations.get_mut(name).map(|h| &mut h.tags),
        "eventsubscription" => acct.event_subscriptions.get_mut(name).map(|s| &mut s.tags),
        "usagelimit" => acct.usage_limits.get_mut(name).map(|u| &mut u.tags),
        _ => None,
    }
}

/// Serialize the `TargetAction` union back out compactly. Redshift's target
/// action is one of ResizeCluster / PauseCluster / ResumeCluster; we round-trip
/// whichever sub-key the caller supplied.
fn extract_target_action(req: &AwsRequest) -> Option<String> {
    for variant in ["ResizeCluster", "PauseCluster", "ResumeCluster"] {
        let ci_key = format!("TargetAction.{variant}.ClusterIdentifier");
        if let Some(ci) = req.query_params.get(&ci_key) {
            return Some(format!(
                "<{variant}><ClusterIdentifier>{}</ClusterIdentifier></{variant}>",
                xml_escape(ci)
            ));
        }
    }
    None
}
