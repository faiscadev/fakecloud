//! AWS Config (`config`) awsJson1_1 service — configuration recorder, config
//! items, rules with real evaluation, compliance, conformance packs,
//! aggregators, remediation, retention, stored queries and resource
//! evaluations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::SharedLambdaState;
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_config_snapshot;
use crate::state::*;
use crate::validate::{self, CrossServiceStates};

pub const SUPPORTED_ACTIONS: &[&str] = &[
    "AssociateResourceTypes",
    "BatchGetAggregateResourceConfig",
    "BatchGetResourceConfig",
    "DeleteAggregationAuthorization",
    "DeleteConfigRule",
    "DeleteConfigurationAggregator",
    "DeleteConfigurationRecorder",
    "DeleteConformancePack",
    "DeleteDeliveryChannel",
    "DeleteEvaluationResults",
    "DeleteOrganizationConfigRule",
    "DeleteOrganizationConformancePack",
    "DeletePendingAggregationRequest",
    "DeleteRemediationConfiguration",
    "DeleteRemediationExceptions",
    "DeleteResourceConfig",
    "DeleteRetentionConfiguration",
    "DeleteServiceLinkedConfigurationRecorder",
    "DeleteStoredQuery",
    "DeliverConfigSnapshot",
    "DescribeAggregateComplianceByConfigRules",
    "DescribeAggregateComplianceByConformancePacks",
    "DescribeAggregationAuthorizations",
    "DescribeComplianceByConfigRule",
    "DescribeComplianceByResource",
    "DescribeConfigRuleEvaluationStatus",
    "DescribeConfigRules",
    "DescribeConfigurationAggregatorSourcesStatus",
    "DescribeConfigurationAggregators",
    "DescribeConfigurationRecorderStatus",
    "DescribeConfigurationRecorders",
    "DescribeConformancePackCompliance",
    "DescribeConformancePackStatus",
    "DescribeConformancePacks",
    "DescribeDeliveryChannelStatus",
    "DescribeDeliveryChannels",
    "DescribeOrganizationConfigRuleStatuses",
    "DescribeOrganizationConfigRules",
    "DescribeOrganizationConformancePackStatuses",
    "DescribeOrganizationConformancePacks",
    "DescribePendingAggregationRequests",
    "DescribeRemediationConfigurations",
    "DescribeRemediationExceptions",
    "DescribeRemediationExecutionStatus",
    "DescribeRetentionConfigurations",
    "DisassociateResourceTypes",
    "GetAggregateComplianceDetailsByConfigRule",
    "GetAggregateConfigRuleComplianceSummary",
    "GetAggregateConformancePackComplianceSummary",
    "GetAggregateDiscoveredResourceCounts",
    "GetAggregateResourceConfig",
    "GetComplianceDetailsByConfigRule",
    "GetComplianceDetailsByResource",
    "GetComplianceSummaryByConfigRule",
    "GetComplianceSummaryByResourceType",
    "GetConformancePackComplianceDetails",
    "GetConformancePackComplianceSummary",
    "GetCustomRulePolicy",
    "GetDiscoveredResourceCounts",
    "GetOrganizationConfigRuleDetailedStatus",
    "GetOrganizationConformancePackDetailedStatus",
    "GetOrganizationCustomRulePolicy",
    "GetResourceConfigHistory",
    "GetResourceEvaluationSummary",
    "GetStoredQuery",
    "ListAggregateDiscoveredResources",
    "ListConfigurationRecorders",
    "ListConformancePackComplianceScores",
    "ListDiscoveredResources",
    "ListResourceEvaluations",
    "ListStoredQueries",
    "ListTagsForResource",
    "PutAggregationAuthorization",
    "PutConfigRule",
    "PutConfigurationAggregator",
    "PutConfigurationRecorder",
    "PutConformancePack",
    "PutDeliveryChannel",
    "PutEvaluations",
    "PutExternalEvaluation",
    "PutOrganizationConfigRule",
    "PutOrganizationConformancePack",
    "PutRemediationConfigurations",
    "PutRemediationExceptions",
    "PutResourceConfig",
    "PutRetentionConfiguration",
    "PutServiceLinkedConfigurationRecorder",
    "PutStoredQuery",
    "SelectAggregateResourceConfig",
    "SelectResourceConfig",
    "StartConfigRulesEvaluation",
    "StartConfigurationRecorder",
    "StartRemediationExecution",
    "StartResourceEvaluation",
    "StopConfigurationRecorder",
    "TagResource",
    "UntagResource",
];

/// Actions that mutate persisted state and trigger a snapshot write.
const MUTATING_ACTIONS: &[&str] = &[
    "AssociateResourceTypes",
    "DeleteAggregationAuthorization",
    "DeleteConfigRule",
    "DeleteConfigurationAggregator",
    "DeleteConfigurationRecorder",
    "DeleteConformancePack",
    "DeleteDeliveryChannel",
    "DeleteEvaluationResults",
    "DeleteOrganizationConfigRule",
    "DeleteOrganizationConformancePack",
    "DeletePendingAggregationRequest",
    "DeleteRemediationConfiguration",
    "DeleteRemediationExceptions",
    "DeleteResourceConfig",
    "DeleteRetentionConfiguration",
    "DeleteServiceLinkedConfigurationRecorder",
    "DeleteStoredQuery",
    "DisassociateResourceTypes",
    "PutAggregationAuthorization",
    "PutConfigRule",
    "PutConfigurationAggregator",
    "PutConfigurationRecorder",
    "PutConformancePack",
    "PutDeliveryChannel",
    "PutEvaluations",
    "PutExternalEvaluation",
    "PutOrganizationConfigRule",
    "PutOrganizationConformancePack",
    "PutRemediationConfigurations",
    "PutRemediationExceptions",
    "PutResourceConfig",
    "PutRetentionConfiguration",
    "PutServiceLinkedConfigurationRecorder",
    "PutStoredQuery",
    "StartConfigRulesEvaluation",
    "StartConfigurationRecorder",
    "StartRemediationExecution",
    "StartResourceEvaluation",
    "StopConfigurationRecorder",
    "TagResource",
    "UntagResource",
];

pub struct ConfigService {
    state: SharedConfigState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    cross: CrossServiceStates,
    lambda_state: Option<SharedLambdaState>,
    container_runtime: Option<Arc<ContainerRuntime>>,
    /// Monotonic counter bumped whenever recorded items or rules change. Lets
    /// managed-rule evaluation skip recomputing when nothing changed since the
    /// last evaluation (avoids re-running the full evaluation on every poll).
    state_version: Arc<AtomicU64>,
    /// The `state_version` at which managed-rule evaluation last ran, tracked
    /// per account. A single global counter would let one account's evaluation
    /// suppress every other account's — a second account whose rules never
    /// evaluated would report stale / INSUFFICIENT_DATA forever. Keyed by
    /// account id so each account re-evaluates independently.
    last_eval_version: Arc<Mutex<HashMap<String, u64>>>,
}

impl ConfigService {
    pub fn new(state: SharedConfigState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            cross: CrossServiceStates::default(),
            lambda_state: None,
            container_runtime: None,
            state_version: Arc::new(AtomicU64::new(1)),
            last_eval_version: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_cross_service(mut self, cross: CrossServiceStates) -> Self {
        self.cross = cross;
        self
    }

    pub fn with_lambda(
        mut self,
        lambda_state: SharedLambdaState,
        runtime: Option<Arc<ContainerRuntime>>,
    ) -> Self {
        self.lambda_state = Some(lambda_state);
        self.container_runtime = runtime;
        self
    }

    pub fn shared_state(&self) -> SharedConfigState {
        Arc::clone(&self.state)
    }

    async fn save_snapshot(&self) {
        save_config_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_config_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Whether at least one recorder is running for the account.
    fn is_recording(&self, account_id: &str) -> bool {
        let st = self.state.read();
        st.account(account_id)
            .map(|a| a.recorders.values().any(|r| r.recording))
            .unwrap_or(false)
    }

    /// Bump the state version so cached managed-rule evaluation is recomputed.
    fn bump_version(&self) {
        self.state_version.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the live cross-service resources into recorded config items
    /// when a recorder is running. Discovery runs under the other services'
    /// read locks; the config write lock (which serializes config reads) is
    /// taken only when discovery actually yields a change.
    fn sync_recording(&self, account_id: &str, region: &str) {
        if !self.is_recording(account_id) {
            return;
        }
        // Discover under the cross-service read locks (no config lock held).
        let discovered = validate::discover_all(&self.cross, account_id, region);
        // Read-only check: skip the write lock entirely if nothing changed.
        let needs = {
            let st = self.state.read();
            match st.account(account_id) {
                Some(acc) => validate::sync_would_change(acc, &discovered),
                None => !discovered.is_empty(),
            }
        };
        if !needs {
            return;
        }
        {
            let mut st = self.state.write();
            let account = st.account_mut(account_id);
            validate::apply_recorded_items(account, discovered, account_id);
        }
        self.bump_version();
    }

    /// Run managed-rule evaluation for every AWS-managed rule and store the
    /// results — but only when recorded items or rules have changed since the
    /// last evaluation. Custom / policy rule results are left as recorded by
    /// `PutEvaluations` / `PutExternalEvaluation`.
    fn ensure_evaluated(&self, account_id: &str) {
        let version = self.state_version.load(Ordering::Relaxed);
        if self.last_eval_version.lock().get(account_id).copied() == Some(version) {
            return;
        }
        let mut st = self.state.write();
        let account = st.account_mut(account_id);
        let rules: Vec<(String, String, Value, Value)> = account
            .rules
            .values()
            .filter_map(|r| {
                let owner = r.source.get("Owner").and_then(Value::as_str).unwrap_or("");
                if owner != "AWS" {
                    return None;
                }
                let sid = r
                    .source
                    .get("SourceIdentifier")
                    .and_then(Value::as_str)?
                    .to_string();
                let params: Value = r
                    .input_parameters
                    .as_deref()
                    .and_then(|p| serde_json::from_str(p).ok())
                    .unwrap_or(Value::Null);
                let scope = r.scope.clone().unwrap_or(Value::Null);
                Some((r.name.clone(), sid, params, scope))
            })
            .collect();
        for (rule_name, sid, params, scope) in rules {
            let outcomes = validate::evaluate_managed_rule(&sid, &params, &scope, account);
            let entry = account.evaluations.entry(rule_name.clone()).or_default();
            entry.clear();
            for o in &outcomes {
                let key = resource_key(&o.resource_type, &o.resource_id);
                entry.insert(key, validate::outcome_to_result(&rule_name, o));
            }
            if let Some(rule) = account.rules.get_mut(&rule_name) {
                let now = Utc::now();
                rule.last_successful_evaluation_time = Some(now);
                rule.last_successful_invocation_time = Some(now);
                if rule.first_activated_time.is_none() {
                    rule.first_activated_time = Some(now);
                }
            }
        }
        drop(st);
        self.last_eval_version
            .lock()
            .insert(account_id.to_string(), version);
    }
}

impl Default for ConfigService {
    fn default() -> Self {
        Self::new(Arc::new(parking_lot::RwLock::new(ConfigAccounts::new())))
    }
}

#[async_trait]
impl AwsService for ConfigService {
    fn service_name(&self) -> &str {
        "config"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let account = account_id(&req);
        let region = region(&req);
        let body = req.json_body();
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());

        // Enforce the Smithy model's input constraints (required / length /
        // range / enum) exactly as AWS does, before any business logic.
        if let Err(msg) = crate::model_validate::validate_input(&req.action, &body) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                msg,
            ));
        }

        // Actions that read discovered resources refresh the recording first.
        const RECORDING_READS: &[&str] = &[
            "GetResourceConfigHistory",
            "BatchGetResourceConfig",
            "BatchGetAggregateResourceConfig",
            "ListDiscoveredResources",
            "ListAggregateDiscoveredResources",
            "GetDiscoveredResourceCounts",
            "GetAggregateDiscoveredResourceCounts",
            "GetResourceConfig",
            "GetAggregateResourceConfig",
            "SelectResourceConfig",
            "SelectAggregateResourceConfig",
            "StartConfigRulesEvaluation",
            "DescribeComplianceByConfigRule",
            "DescribeComplianceByResource",
            "GetComplianceDetailsByConfigRule",
            "GetComplianceDetailsByResource",
            "GetComplianceSummaryByConfigRule",
            "GetComplianceSummaryByResourceType",
            "DescribeConfigRuleEvaluationStatus",
        ];
        if RECORDING_READS.contains(&req.action.as_str()) {
            self.sync_recording(&account, &region);
        }

        let result = match req.action.as_str() {
            // ── Recorders ──
            "PutConfigurationRecorder" => self.put_configuration_recorder(&account, &region, &body),
            "DescribeConfigurationRecorders" => {
                self.describe_configuration_recorders(&account, &body)
            }
            "DescribeConfigurationRecorderStatus" => {
                self.describe_configuration_recorder_status(&account, &body)
            }
            "DeleteConfigurationRecorder" => self.delete_configuration_recorder(&account, &body),
            "StartConfigurationRecorder" => self.set_recording(&account, &body, true),
            "StopConfigurationRecorder" => self.set_recording(&account, &body, false),
            "ListConfigurationRecorders" => self.list_configuration_recorders(&account, &body),
            "PutServiceLinkedConfigurationRecorder" => {
                self.put_service_linked_recorder(&account, &region, &body)
            }
            "DeleteServiceLinkedConfigurationRecorder" => {
                self.delete_service_linked_recorder(&account, &body)
            }
            "AssociateResourceTypes" => self.associate_resource_types(&account, &body, true),
            "DisassociateResourceTypes" => self.associate_resource_types(&account, &body, false),
            // ── Delivery channels ──
            "PutDeliveryChannel" => self.put_delivery_channel(&account, &body),
            "DescribeDeliveryChannels" => self.describe_delivery_channels(&account, &body),
            "DescribeDeliveryChannelStatus" => {
                self.describe_delivery_channel_status(&account, &body)
            }
            "DeleteDeliveryChannel" => self.delete_delivery_channel(&account, &body),
            "DeliverConfigSnapshot" => self.deliver_config_snapshot(&account, &body),
            // ── Config items ──
            "PutResourceConfig" => self.put_resource_config(&account, &region, &body),
            "DeleteResourceConfig" => self.delete_resource_config(&account, &body),
            "GetResourceConfigHistory" => self.get_resource_config_history(&account, &body),
            "BatchGetResourceConfig" => self.batch_get_resource_config(&account, &body),
            "ListDiscoveredResources" => self.list_discovered_resources(&account, &body),
            "GetDiscoveredResourceCounts" => self.get_discovered_resource_counts(&account, &body),
            // ── Config rules ──
            "PutConfigRule" => self.put_config_rule(&account, &region, &body),
            "DescribeConfigRules" => self.describe_config_rules(&account, &body),
            "DeleteConfigRule" => self.delete_config_rule(&account, &body),
            "DescribeConfigRuleEvaluationStatus" => {
                self.ensure_evaluated(&account);
                self.describe_config_rule_evaluation_status(&account, &body)
            }
            "StartConfigRulesEvaluation" => {
                self.start_config_rules_evaluation(&account, &region, &body)
                    .await
            }
            "PutEvaluations" => self.put_evaluations(&account, &body),
            "PutExternalEvaluation" => self.put_external_evaluation(&account, &body),
            "DeleteEvaluationResults" => self.delete_evaluation_results(&account, &body),
            "GetCustomRulePolicy" => self.get_custom_rule_policy(&account, &body),
            // ── Compliance ──
            "DescribeComplianceByConfigRule" => {
                self.ensure_evaluated(&account);
                self.describe_compliance_by_config_rule(&account, &body)
            }
            "DescribeComplianceByResource" => {
                self.ensure_evaluated(&account);
                self.describe_compliance_by_resource(&account, &body)
            }
            "GetComplianceDetailsByConfigRule" => {
                self.ensure_evaluated(&account);
                self.get_compliance_details_by_config_rule(&account, &body)
            }
            "GetComplianceDetailsByResource" => {
                self.ensure_evaluated(&account);
                self.get_compliance_details_by_resource(&account, &body)
            }
            "GetComplianceSummaryByConfigRule" => {
                self.ensure_evaluated(&account);
                self.get_compliance_summary_by_config_rule(&account)
            }
            "GetComplianceSummaryByResourceType" => {
                self.ensure_evaluated(&account);
                self.get_compliance_summary_by_resource_type(&account, &body)
            }
            // ── Remediation ──
            "PutRemediationConfigurations" => {
                self.put_remediation_configurations(&account, &region, &body)
            }
            "DescribeRemediationConfigurations" => {
                self.describe_remediation_configurations(&account, &body)
            }
            "DeleteRemediationConfiguration" => {
                self.delete_remediation_configuration(&account, &body)
            }
            "PutRemediationExceptions" => self.put_remediation_exceptions(&account, &body),
            "DescribeRemediationExceptions" => {
                self.describe_remediation_exceptions(&account, &body)
            }
            "DeleteRemediationExceptions" => self.delete_remediation_exceptions(&account, &body),
            "StartRemediationExecution" => self.start_remediation_execution(&account, &body),
            "DescribeRemediationExecutionStatus" => {
                self.describe_remediation_execution_status(&account, &body)
            }
            // ── Conformance packs ──
            "PutConformancePack" => self.put_conformance_pack(&account, &region, &body),
            "DescribeConformancePacks" => self.describe_conformance_packs(&account, &body),
            "DeleteConformancePack" => self.delete_conformance_pack(&account, &body),
            "DescribeConformancePackStatus" => {
                self.describe_conformance_pack_status(&account, &region, &body)
            }
            "DescribeConformancePackCompliance" => {
                self.ensure_evaluated(&account);
                self.describe_conformance_pack_compliance(&account, &body)
            }
            "GetConformancePackComplianceSummary" => {
                self.ensure_evaluated(&account);
                self.get_conformance_pack_compliance_summary(&account, &body)
            }
            "GetConformancePackComplianceDetails" => {
                self.ensure_evaluated(&account);
                self.get_conformance_pack_compliance_details(&account, &body)
            }
            "ListConformancePackComplianceScores" => {
                self.ensure_evaluated(&account);
                self.list_conformance_pack_compliance_scores(&account)
            }
            // ── Organization rules/packs ──
            "PutOrganizationConfigRule" => {
                self.put_organization_config_rule(&account, &region, &body)
            }
            "DescribeOrganizationConfigRules" => {
                self.describe_organization_config_rules(&account, &body)
            }
            "DeleteOrganizationConfigRule" => self.delete_organization_config_rule(&account, &body),
            "DescribeOrganizationConfigRuleStatuses" => {
                self.describe_organization_config_rule_statuses(&account, &body)
            }
            "GetOrganizationConfigRuleDetailedStatus" => {
                self.get_organization_config_rule_detailed_status(&account, &body)
            }
            "GetOrganizationCustomRulePolicy" => {
                self.get_organization_custom_rule_policy(&account, &body)
            }
            "PutOrganizationConformancePack" => {
                self.put_organization_conformance_pack(&account, &region, &body)
            }
            "DescribeOrganizationConformancePacks" => {
                self.describe_organization_conformance_packs(&account, &body)
            }
            "DeleteOrganizationConformancePack" => {
                self.delete_organization_conformance_pack(&account, &body)
            }
            "DescribeOrganizationConformancePackStatuses" => {
                self.describe_organization_conformance_pack_statuses(&account, &body)
            }
            "GetOrganizationConformancePackDetailedStatus" => {
                self.get_organization_conformance_pack_detailed_status(&account, &body)
            }
            // ── Aggregators ──
            "PutConfigurationAggregator" => {
                self.put_configuration_aggregator(&account, &region, &body)
            }
            "DescribeConfigurationAggregators" => {
                self.describe_configuration_aggregators(&account, &body)
            }
            "DeleteConfigurationAggregator" => {
                self.delete_configuration_aggregator(&account, &body)
            }
            "DescribeConfigurationAggregatorSourcesStatus" => {
                self.describe_configuration_aggregator_sources_status(&account, &body)
            }
            "PutAggregationAuthorization" => {
                self.put_aggregation_authorization(&account, &region, &body)
            }
            "DescribeAggregationAuthorizations" => {
                self.describe_aggregation_authorizations(&account, &body)
            }
            "DeleteAggregationAuthorization" => {
                self.delete_aggregation_authorization(&account, &body)
            }
            "DeletePendingAggregationRequest" => Ok(AwsResponse::ok_json(json!({}))),
            "DescribePendingAggregationRequests" => Ok(AwsResponse::ok_json(
                json!({ "PendingAggregationRequests": [] }),
            )),
            "BatchGetAggregateResourceConfig" => {
                self.batch_get_aggregate_resource_config(&account, &body)
            }
            "GetAggregateResourceConfig" => self.get_aggregate_resource_config(&account, &body),
            "ListAggregateDiscoveredResources" => {
                self.list_aggregate_discovered_resources(&account, &body)
            }
            "GetAggregateDiscoveredResourceCounts" => {
                self.get_aggregate_discovered_resource_counts(&account, &body)
            }
            "DescribeAggregateComplianceByConfigRules" => {
                self.ensure_evaluated(&account);
                self.describe_aggregate_compliance_by_config_rules(&account, &body)
            }
            "DescribeAggregateComplianceByConformancePacks" => {
                self.describe_aggregate_compliance_by_conformance_packs(&account, &body)
            }
            "GetAggregateComplianceDetailsByConfigRule" => {
                self.ensure_evaluated(&account);
                self.get_aggregate_compliance_details_by_config_rule(&account, &body)
            }
            "GetAggregateConfigRuleComplianceSummary" => {
                self.ensure_evaluated(&account);
                self.get_aggregate_config_rule_compliance_summary(&account, &body)
            }
            "GetAggregateConformancePackComplianceSummary" => {
                self.get_aggregate_conformance_pack_compliance_summary(&account, &body)
            }
            "SelectAggregateResourceConfig" => self.select_resource_config(&account, &body, true),
            // ── Retention ──
            "PutRetentionConfiguration" => self.put_retention_configuration(&account, &body),
            "DescribeRetentionConfigurations" => {
                self.describe_retention_configurations(&account, &body)
            }
            "DeleteRetentionConfiguration" => self.delete_retention_configuration(&account, &body),
            // ── Stored queries ──
            "PutStoredQuery" => self.put_stored_query(&account, &region, &body),
            "GetStoredQuery" => self.get_stored_query(&account, &body),
            "DeleteStoredQuery" => self.delete_stored_query(&account, &body),
            "ListStoredQueries" => self.list_stored_queries(&account, &body),
            // ── Resource evaluations ──
            "StartResourceEvaluation" => self.start_resource_evaluation(&account, &body),
            "GetResourceEvaluationSummary" => self.get_resource_evaluation_summary(&account, &body),
            "ListResourceEvaluations" => self.list_resource_evaluations(&account, &body),
            // ── Select ──
            "SelectResourceConfig" => self.select_resource_config(&account, &body, false),
            // ── Tags ──
            "TagResource" => self.tag_resource(&account, &body),
            "UntagResource" => self.untag_resource(&account, &body),
            "ListTagsForResource" => self.list_tags_for_resource(&account, &body),
            other => Err(AwsServiceError::action_not_implemented("config", other)),
        };

        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            // A successful mutation may change recorded items or rules, so
            // managed-rule evaluation must recompute on the next read.
            self.bump_version();
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Recorders ───────────────────────────────────────────────────────────

impl ConfigService {
    fn put_configuration_recorder(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rec = body
            .get("ConfigurationRecorder")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid("ConfigurationRecorder is required"))?;
        let name = rec
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let role_arn = rec
            .get("roleARN")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let existing = acc.recorders.get(&name);
        let recorder = ConfigurationRecorder {
            name: name.clone(),
            role_arn,
            recording_group: rec.get("recordingGroup").cloned(),
            recording_mode: rec.get("recordingMode").cloned(),
            arn: existing.and_then(|e| e.arn.clone()),
            service_principal: existing.and_then(|e| e.service_principal.clone()),
            recording: existing.map(|e| e.recording).unwrap_or(false),
            last_start_time: existing.and_then(|e| e.last_start_time),
            last_stop_time: existing.and_then(|e| e.last_stop_time),
            last_status: existing
                .map(|e| e.last_status.clone())
                .unwrap_or_else(|| "PENDING".into()),
            last_status_change_time: Some(Utc::now()),
        };
        let _ = region;
        acc.recorders.insert(name, recorder);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn recorder_json(r: &ConfigurationRecorder) -> Value {
        let mut v = json!({
            "name": r.name,
            "roleARN": r.role_arn,
        });
        if let Some(g) = &r.recording_group {
            v["recordingGroup"] = g.clone();
        }
        if let Some(m) = &r.recording_mode {
            v["recordingMode"] = m.clone();
        }
        if let Some(a) = &r.arn {
            v["arn"] = json!(a);
        }
        if let Some(sp) = &r.service_principal {
            v["servicePrincipal"] = json!(sp);
        }
        v
    }

    fn describe_configuration_recorders(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigurationRecorderNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.recorders
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(Self::recorder_json)
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            for n in &names {
                let found = st
                    .account(account)
                    .map(|a| a.recorders.contains_key(n))
                    .unwrap_or(false);
                if !found {
                    return Err(no_such(
                        "NoSuchConfigurationRecorderException",
                        format!(
                            "Cannot find configuration recorder with the specified name '{n}'."
                        ),
                    ));
                }
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "ConfigurationRecorders": list }),
        ))
    }

    fn describe_configuration_recorder_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigurationRecorderNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.recorders
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(|r| {
                        json!({
                            "name": r.name,
                            "lastStartTime": r.last_start_time.map(|t| t.timestamp() as f64),
                            "lastStopTime": r.last_stop_time.map(|t| t.timestamp() as f64),
                            "recording": r.recording,
                            "lastStatus": r.last_status,
                            "lastStatusChangeTime": r.last_status_change_time.map(|t| t.timestamp() as f64),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ConfigurationRecordersStatus": list }),
        ))
    }

    fn delete_configuration_recorder(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationRecorderName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.recorders.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchConfigurationRecorderException",
                format!("Cannot find configuration recorder with the specified name '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn set_recording(
        &self,
        account: &str,
        body: &Value,
        recording: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationRecorderName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let rec = acc.recorders.get_mut(&name).ok_or_else(|| {
            no_such(
                "NoSuchConfigurationRecorderException",
                format!("Cannot find configuration recorder with the specified name '{name}'."),
            )
        })?;
        rec.recording = recording;
        let now = Utc::now();
        if recording {
            rec.last_start_time = Some(now);
            rec.last_status = "SUCCESS".into();
        } else {
            rec.last_stop_time = Some(now);
            rec.last_status = "PENDING".into();
        }
        rec.last_status_change_time = Some(now);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_configuration_recorders(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.recorders
                    .values()
                    .map(|r| {
                        let mut v = json!({ "name": r.name, "recordingScope": "INTERNAL" });
                        if let Some(arn) = &r.arn {
                            v["arn"] = json!(arn);
                        }
                        if let Some(sp) = &r.service_principal {
                            v["servicePrincipal"] = json!(sp);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "ConfigurationRecorderSummaries",
            list,
            body,
            "NextToken",
        ))
    }

    fn put_service_linked_recorder(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sp = body
            .get("ServicePrincipal")
            .and_then(Value::as_str)
            .unwrap_or("config-conforms.amazonaws.com")
            .to_string();
        let name = format!(
            "AWSConfigurationRecorderForServiceLinkedConfigurationRecorder-{}",
            &Uuid::new_v4().to_string()[..8]
        );
        let arn = format!("arn:aws:config:{region}:{account}:configuration-recorder/{name}");
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        acc.recorders.insert(
            name.clone(),
            ConfigurationRecorder {
                name: name.clone(),
                role_arn: format!("arn:aws:iam::{account}:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig"),
                recording_group: None,
                recording_mode: None,
                arn: Some(arn.clone()),
                service_principal: Some(sp),
                recording: true,
                last_start_time: Some(Utc::now()),
                last_stop_time: None,
                last_status: "SUCCESS".into(),
                last_status_change_time: Some(Utc::now()),
            },
        );
        Ok(AwsResponse::ok_json(json!({ "Arn": arn, "Name": name })))
    }

    fn delete_service_linked_recorder(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sp = body
            .get("ServicePrincipal")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let name = acc
            .recorders
            .values()
            .find(|r| r.service_principal.as_deref() == Some(sp.as_str()))
            .map(|r| r.name.clone());
        let name = name.ok_or_else(|| {
            no_such(
                "NoSuchConfigurationRecorderException",
                "No service-linked configuration recorder for that principal.",
            )
        })?;
        let arn = acc
            .recorders
            .get(&name)
            .and_then(|r| r.arn.clone())
            .unwrap_or_default();
        acc.recorders.remove(&name);
        Ok(AwsResponse::ok_json(json!({ "Arn": arn, "Name": name })))
    }

    fn associate_resource_types(
        &self,
        account: &str,
        body: &Value,
        associate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationRecorderArn")
            .or_else(|_| require_str(body, "ConfigurationRecorderName"))?;
        let types = string_list(body, "ResourceTypes");
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        // Match by ARN or by name.
        let rec = acc
            .recorders
            .values_mut()
            .find(|r| r.arn.as_deref() == Some(name.as_str()) || r.name == name)
            .ok_or_else(|| {
                no_such(
                    "NoSuchConfigurationRecorderException",
                    "Cannot find the specified configuration recorder.",
                )
            })?;
        let mut group = rec.recording_group.clone().unwrap_or_else(|| json!({}));
        let current = group
            .get("resourceTypes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut set: Vec<String> = current
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        if associate {
            for t in types {
                if !set.contains(&t) {
                    set.push(t);
                }
            }
        } else {
            set.retain(|t| !types.contains(t));
        }
        group["allSupported"] = json!(false);
        group["resourceTypes"] = json!(set);
        rec.recording_group = Some(group);
        let out = Self::recorder_json(rec);
        Ok(AwsResponse::ok_json(
            json!({ "ConfigurationRecorder": out }),
        ))
    }
}

// ─── Delivery channels ────────────────────────────────────────────────────

impl ConfigService {
    fn put_delivery_channel(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ch = body
            .get("DeliveryChannel")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid("DeliveryChannel is required"))?;
        let name = ch
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let bucket = ch
            .get("s3BucketName")
            .and_then(Value::as_str)
            .map(String::from);
        match &bucket {
            None => {
                return Err(no_such(
                    "NoSuchBucketException",
                    "Cannot find a S3 bucket with an empty bucket name.",
                ));
            }
            Some(name) => {
                // Validate the referenced bucket actually exists (matches AWS,
                // which rejects a delivery channel pointing at a missing
                // bucket). Skipped when S3 is not wired.
                if validate::s3_bucket_exists(&self.cross, name) == Some(false) {
                    return Err(no_such(
                        "NoSuchBucketException",
                        format!("Cannot find a S3 bucket with the bucket name '{name}'."),
                    ));
                }
            }
        }
        let channel = DeliveryChannel {
            name: name.clone(),
            s3_bucket_name: bucket,
            s3_key_prefix: ch
                .get("s3KeyPrefix")
                .and_then(Value::as_str)
                .map(String::from),
            s3_kms_key_arn: ch
                .get("s3KmsKeyArn")
                .and_then(Value::as_str)
                .map(String::from),
            sns_topic_arn: ch
                .get("snsTopicARN")
                .and_then(Value::as_str)
                .map(String::from),
            config_snapshot_delivery_properties: ch
                .get("configSnapshotDeliveryProperties")
                .cloned(),
            last_status: "SUCCESS".into(),
        };
        let mut st = self.state.write();
        st.account_mut(account)
            .delivery_channels
            .insert(name, channel);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn channel_json(c: &DeliveryChannel) -> Value {
        let mut v = json!({ "name": c.name });
        if let Some(b) = &c.s3_bucket_name {
            v["s3BucketName"] = json!(b);
        }
        if let Some(p) = &c.s3_key_prefix {
            v["s3KeyPrefix"] = json!(p);
        }
        if let Some(k) = &c.s3_kms_key_arn {
            v["s3KmsKeyArn"] = json!(k);
        }
        if let Some(s) = &c.sns_topic_arn {
            v["snsTopicARN"] = json!(s);
        }
        if let Some(p) = &c.config_snapshot_delivery_properties {
            v["configSnapshotDeliveryProperties"] = p.clone();
        }
        v
    }

    fn describe_delivery_channels(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "DeliveryChannelNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.delivery_channels
                    .values()
                    .filter(|c| names.is_empty() || names.contains(&c.name))
                    .map(Self::channel_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "DeliveryChannels": list })))
    }

    fn describe_delivery_channel_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "DeliveryChannelNames");
        let now = Utc::now().timestamp() as f64;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.delivery_channels
                    .values()
                    .filter(|c| names.is_empty() || names.contains(&c.name))
                    .map(|c| {
                        json!({
                            "name": c.name,
                            "configSnapshotDeliveryInfo": { "lastStatus": "SUCCESS", "lastSuccessfulTime": now },
                            "configHistoryDeliveryInfo": { "lastStatus": "SUCCESS", "lastSuccessfulTime": now },
                            "configStreamDeliveryInfo": { "lastStatus": "SUCCESS", "lastStatusChangeTime": now },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "DeliveryChannelsStatus": list }),
        ))
    }

    fn delete_delivery_channel(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "DeliveryChannelName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.delivery_channels.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchDeliveryChannelException",
                format!("Cannot find delivery channel with specified name '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn deliver_config_snapshot(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "deliveryChannelName")?;
        let st = self.state.read();
        let exists = st
            .account(account)
            .map(|a| a.delivery_channels.contains_key(&name))
            .unwrap_or(false);
        if !exists {
            return Err(no_such(
                "NoSuchDeliveryChannelException",
                format!("Cannot find delivery channel with specified name '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(
            json!({ "configSnapshotId": Uuid::new_v4().to_string() }),
        ))
    }
}

// ─── Config items ─────────────────────────────────────────────────────────

impl ConfigService {
    fn config_item_json(ci: &ConfigurationItem) -> Value {
        json!({
            "version": ci.version,
            "accountId": ci.account_id,
            "configurationItemCaptureTime": ci.configuration_item_capture_time.timestamp() as f64,
            "configurationItemStatus": ci.configuration_item_status,
            "configurationStateId": ci.configuration_state_id,
            "arn": ci.arn,
            "resourceType": ci.resource_type,
            "resourceId": ci.resource_id,
            "resourceName": ci.resource_name,
            "awsRegion": ci.aws_region,
            "availabilityZone": ci.availability_zone,
            "resourceCreationTime": ci.resource_creation_time.map(|t| t.timestamp() as f64),
            "tags": ci.tags,
            "relatedEvents": [],
            "relationships": [],
            "configuration": ci.configuration,
            "supplementaryConfiguration": ci.supplementary_configuration,
        })
    }

    fn base_config_item_json(ci: &ConfigurationItem) -> Value {
        json!({
            "version": ci.version,
            "accountId": ci.account_id,
            "configurationItemCaptureTime": ci.configuration_item_capture_time.timestamp() as f64,
            "configurationItemStatus": ci.configuration_item_status,
            "configurationStateId": ci.configuration_state_id,
            "arn": ci.arn,
            "resourceType": ci.resource_type,
            "resourceId": ci.resource_id,
            "resourceName": ci.resource_name,
            "awsRegion": ci.aws_region,
            "availabilityZone": ci.availability_zone,
            "resourceCreationTime": ci.resource_creation_time.map(|t| t.timestamp() as f64),
            "configuration": ci.configuration,
            "supplementaryConfiguration": ci.supplementary_configuration,
        })
    }

    fn put_resource_config(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_type = require_str(body, "ResourceType")?;
        let resource_id = require_str(body, "ResourceId")?;
        require_str(body, "SchemaVersionId")?;
        let configuration = require_str(body, "Configuration")?;
        let resource_name = body
            .get("ResourceName")
            .and_then(Value::as_str)
            .map(String::from);
        let tags: std::collections::BTreeMap<String, String> = body
            .get("Tags")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let now = Utc::now();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let key = resource_key(&resource_type, &resource_id);
        let history = acc.config_items.entry(key).or_default();
        let state_id = (history.len() as u64 + 1).to_string();
        history.push(ConfigurationItem {
            version: "1.3".into(),
            account_id: account.to_string(),
            configuration_item_capture_time: now,
            configuration_item_status: if history.is_empty() {
                "ResourceDiscovered".into()
            } else {
                "OK".into()
            },
            configuration_state_id: state_id,
            arn: format!(
                "arn:aws:config:{region}:{account}:resource/{resource_type}/{resource_id}"
            ),
            resource_type,
            resource_id,
            resource_name,
            aws_region: region.to_string(),
            availability_zone: "Not Applicable".into(),
            resource_creation_time: Some(now),
            tags,
            configuration,
            supplementary_configuration: Default::default(),
            externally_recorded: true,
        });
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_resource_config(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_type = require_str(body, "ResourceType")?;
        let resource_id = require_str(body, "ResourceId")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let key = resource_key(&resource_type, &resource_id);
        if let Some(history) = acc.config_items.get_mut(&key) {
            if let Some(last) = history.last().cloned() {
                let state_id = (history.len() as u64 + 1).to_string();
                history.push(ConfigurationItem {
                    configuration_item_capture_time: Utc::now(),
                    configuration_item_status: "ResourceDeleted".into(),
                    configuration_state_id: state_id,
                    ..last
                });
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_resource_config_history(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_type = require_str(body, "resourceType")?;
        let resource_id = require_str(body, "resourceId")?;
        // Optional time window (epoch seconds). `laterTime` is the inclusive
        // upper bound, `earlierTime` the inclusive lower bound.
        let later = body.get("laterTime").and_then(Value::as_f64);
        let earlier = body.get("earlierTime").and_then(Value::as_f64);
        // Default order is Reverse (newest first), matching AWS.
        let forward = body
            .get("chronologicalOrder")
            .and_then(Value::as_str)
            .map(|o| o.eq_ignore_ascii_case("Forward"))
            .unwrap_or(false);
        let st = self.state.read();
        let key = resource_key(&resource_type, &resource_id);
        let history = st.account(account).and_then(|a| a.config_items.get(&key));
        let Some(history) = history else {
            return Err(no_such(
                "ResourceNotDiscoveredException",
                format!("Resource {resource_id} of type {resource_type} is not discovered."),
            ));
        };
        // Stored oldest-first; select those inside the window.
        let mut selected: Vec<&ConfigurationItem> = history
            .iter()
            .filter(|ci| {
                let ts = ci.configuration_item_capture_time.timestamp() as f64;
                later.map(|l| ts <= l).unwrap_or(true) && earlier.map(|e| ts >= e).unwrap_or(true)
            })
            .collect();
        if !forward {
            selected.reverse();
        }
        let items: Vec<Value> = selected.into_iter().map(Self::config_item_json).collect();
        let (page, next) = paginate(items, body);
        let mut out = json!({ "configurationItems": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn batch_get_resource_config(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let keys = body
            .get("resourceKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let st = self.state.read();
        let acc = st.account(account);
        let mut items = Vec::new();
        let mut unprocessed = Vec::new();
        for k in keys {
            let rt = k
                .get("resourceType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let rid = k
                .get("resourceId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let key = resource_key(rt, rid);
            match acc.and_then(|a| a.config_items.get(&key)).and_then(|h| {
                h.iter()
                    .rev()
                    .find(|ci| !ci.configuration_item_status.starts_with("ResourceDeleted"))
            }) {
                Some(ci) => items.push(Self::base_config_item_json(ci)),
                None => unprocessed.push(k),
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "baseConfigurationItems": items, "unprocessedResourceKeys": unprocessed }),
        ))
    }

    fn list_discovered_resources(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_type = require_str(body, "resourceType")?;
        let ids = string_list(body, "resourceIds");
        let include_deleted = body
            .get("includeDeletedResources")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let st = self.state.read();
        let mut list = Vec::new();
        if let Some(acc) = st.account(account) {
            for (key, history) in &acc.config_items {
                let Some((rt, _)) = key.split_once('\u{1}') else {
                    continue;
                };
                if rt != resource_type {
                    continue;
                }
                let Some(ci) = history.last() else { continue };
                let deleted = ci.configuration_item_status.starts_with("ResourceDeleted");
                if deleted && !include_deleted {
                    continue;
                }
                if !ids.is_empty() && !ids.contains(&ci.resource_id) {
                    continue;
                }
                let mut ident = json!({
                    "resourceType": ci.resource_type,
                    "resourceId": ci.resource_id,
                    "resourceName": ci.resource_name,
                });
                if deleted {
                    ident["resourceDeletionTime"] =
                        json!(ci.configuration_item_capture_time.timestamp() as f64);
                }
                list.push(ident);
            }
        }
        let (page, next) = paginate(list, body);
        let mut out = json!({ "resourceIdentifiers": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn get_discovered_resource_counts(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = string_list(body, "resourceTypes");
        let st = self.state.read();
        let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
        if let Some(acc) = st.account(account) {
            for (key, history) in &acc.config_items {
                let Some((rt, _)) = key.split_once('\u{1}') else {
                    continue;
                };
                if !filter.is_empty() && !filter.contains(&rt.to_string()) {
                    continue;
                }
                let deleted = history
                    .last()
                    .map(|ci| ci.configuration_item_status.starts_with("ResourceDeleted"))
                    .unwrap_or(true);
                if deleted {
                    continue;
                }
                *counts.entry(rt.to_string()).or_insert(0) += 1;
            }
        }
        let total: i64 = counts.values().sum();
        let list: Vec<Value> = counts
            .into_iter()
            .map(|(t, c)| json!({ "resourceType": t, "count": c }))
            .collect();
        let (page, next) = paginate(list, body);
        let mut out = json!({ "totalDiscoveredResources": total, "resourceCounts": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }
}

// ─── Config rules + evaluation ─────────────────────────────────────────────

impl ConfigService {
    fn put_config_rule(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule = body
            .get("ConfigRule")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid("ConfigRule is required"))?;
        let name = rule
            .get("ConfigRuleName")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("ConfigRuleName is required"))?
            .to_string();
        let source = rule
            .get("Source")
            .cloned()
            .ok_or_else(|| invalid("Source is required"))?;
        let owner = source
            .get("Owner")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !["AWS", "CUSTOM_LAMBDA", "CUSTOM_POLICY"].contains(&owner) {
            return Err(invalid(format!("Invalid Source.Owner '{owner}'")));
        }
        if owner == "CUSTOM_LAMBDA"
            && source
                .get("SourceIdentifier")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .is_empty()
        {
            return Err(invalid(
                "SourceIdentifier (Lambda ARN) is required for CUSTOM_LAMBDA rules",
            ));
        }
        let now = Utc::now();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let existing = acc.rules.get(&name);
        let arn = existing.map(|r| r.arn.clone()).unwrap_or_else(|| {
            format!(
                "arn:aws:config:{region}:{account}:config-rule/config-rule-{}",
                short_id()
            )
        });
        let rule_id = existing
            .map(|r| r.rule_id.clone())
            .unwrap_or_else(|| format!("config-rule-{}", short_id()));
        let cfg_rule = ConfigRule {
            name: name.clone(),
            arn,
            rule_id,
            description: rule
                .get("Description")
                .and_then(Value::as_str)
                .map(String::from),
            scope: rule.get("Scope").cloned(),
            source,
            input_parameters: rule
                .get("InputParameters")
                .and_then(Value::as_str)
                .map(String::from),
            maximum_execution_frequency: rule
                .get("MaximumExecutionFrequency")
                .and_then(Value::as_str)
                .map(String::from),
            state: "ACTIVE".into(),
            created_by: rule
                .get("CreatedBy")
                .and_then(Value::as_str)
                .map(String::from),
            evaluation_modes: rule.get("EvaluationModes").cloned(),
            last_updated: now,
            first_activated_time: existing.and_then(|r| r.first_activated_time),
            last_successful_evaluation_time: existing
                .and_then(|r| r.last_successful_evaluation_time),
            last_successful_invocation_time: existing
                .and_then(|r| r.last_successful_invocation_time),
        };
        acc.rules.insert(name.clone(), cfg_rule);
        let arn_for_tags = acc.rules[&name].arn.clone();
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(arn_for_tags).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn rule_json(r: &ConfigRule) -> Value {
        let mut v = json!({
            "ConfigRuleName": r.name,
            "ConfigRuleArn": r.arn,
            "ConfigRuleId": r.rule_id,
            "Source": r.source,
            "ConfigRuleState": r.state,
        });
        if let Some(d) = &r.description {
            v["Description"] = json!(d);
        }
        if let Some(s) = &r.scope {
            v["Scope"] = s.clone();
        }
        if let Some(p) = &r.input_parameters {
            v["InputParameters"] = json!(p);
        }
        if let Some(f) = &r.maximum_execution_frequency {
            v["MaximumExecutionFrequency"] = json!(f);
        }
        if let Some(c) = &r.created_by {
            v["CreatedBy"] = json!(c);
        }
        if let Some(m) = &r.evaluation_modes {
            v["EvaluationModes"] = m.clone();
        }
        v
    }

    fn describe_config_rules(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigRuleNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.rules
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(Self::rule_json)
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            for n in &names {
                if !st
                    .account(account)
                    .map(|a| a.rules.contains_key(n))
                    .unwrap_or(false)
                {
                    return Err(no_such(
                        "NoSuchConfigRuleException",
                        format!("The ConfigRule '{n}' provided in the request is invalid."),
                    ));
                }
            }
        }
        Ok(paged_response("ConfigRules", list, body, "NextToken"))
    }

    fn delete_config_rule(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.rules.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchConfigRuleException",
                format!("The ConfigRule '{name}' provided in the request is invalid."),
            ));
        }
        acc.evaluations.remove(&name);
        acc.remediation_configs.remove(&name);
        acc.remediation_executions
            .retain(|_, e| e.config_rule_name != name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_config_rule_evaluation_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigRuleNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.rules
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(|r| {
                        json!({
                            "ConfigRuleName": r.name,
                            "ConfigRuleArn": r.arn,
                            "ConfigRuleId": r.rule_id,
                            "LastSuccessfulInvocationTime": r.last_successful_invocation_time.map(|t| t.timestamp() as f64),
                            "LastSuccessfulEvaluationTime": r.last_successful_evaluation_time.map(|t| t.timestamp() as f64),
                            "FirstActivatedTime": r.first_activated_time.map(|t| t.timestamp() as f64),
                            "FirstEvaluationStarted": r.first_activated_time.is_some(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "ConfigRulesEvaluationStatus",
            list,
            body,
            "NextToken",
        ))
    }

    async fn start_config_rules_evaluation(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.ensure_evaluated(account);
        // For custom-Lambda rules, invoke the referenced function.
        let names = string_list(body, "ConfigRuleNames");
        let custom_rules: Vec<(String, String, Option<String>)> = {
            let st = self.state.read();
            st.account(account)
                .map(|a| {
                    a.rules
                        .values()
                        .filter(|r| names.is_empty() || names.contains(&r.name))
                        .filter_map(|r| {
                            let owner = r.source.get("Owner").and_then(Value::as_str).unwrap_or("");
                            if owner != "CUSTOM_LAMBDA" {
                                return None;
                            }
                            let lambda_arn = r
                                .source
                                .get("SourceIdentifier")
                                .and_then(Value::as_str)?
                                .to_string();
                            Some((r.name.clone(), lambda_arn, r.input_parameters.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        for (rule_name, lambda_arn, input_parameters) in custom_rules {
            self.invoke_custom_rule(
                account,
                region,
                &rule_name,
                &lambda_arn,
                input_parameters.as_deref(),
            )
            .await;
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    /// Invoke a custom-Lambda config rule's function with a Config invoking
    /// event. If the function returns an array of evaluations synchronously,
    /// record them; otherwise the function is expected to call PutEvaluations
    /// with the supplied resultToken.
    async fn invoke_custom_rule(
        &self,
        account: &str,
        region: &str,
        rule_name: &str,
        lambda_arn: &str,
        input_parameters: Option<&str>,
    ) {
        let (Some(lambda_state), Some(runtime)) =
            (self.lambda_state.clone(), self.container_runtime.clone())
        else {
            return;
        };
        let func_name = lambda_arn
            .rsplit(':')
            .next()
            .unwrap_or(lambda_arn)
            .to_string();
        let result_token = format!("{rule_name}#{}", Uuid::new_v4());
        let event = custom_rule_event(account, region, rule_name, input_parameters, &result_token);
        // Resolve the Lambda in the rule's own account (not just the default
        // account) so a custom rule in a non-default account finds its function.
        let resolved = {
            let accounts = lambda_state.read();
            accounts
                .get(account)
                .and_then(|state| state.functions.get(&func_name).cloned())
        };
        let Some(func) = resolved else {
            tracing::warn!(function = %func_name, account = %account, "Config custom rule Lambda not found");
            return;
        };
        let payload = event.to_string().into_bytes();
        match runtime.invoke(&func, &payload, &[]).await {
            Ok(resp) => {
                // If the function returned evaluations directly, record them.
                if let Ok(v) = serde_json::from_slice::<Value>(&resp) {
                    if let Some(arr) = v
                        .as_array()
                        .or_else(|| v.get("evaluations").and_then(Value::as_array))
                    {
                        self.record_evaluations(account, rule_name, arr);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(function = %func_name, error = %e, "Config custom rule Lambda invocation failed")
            }
        }
    }

    fn record_evaluations(&self, account: &str, rule_name: &str, evals: &[Value]) {
        let now = Utc::now();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let entry = acc.evaluations.entry(rule_name.to_string()).or_default();
        for e in evals {
            let rt = e
                .get("ComplianceResourceType")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let rid = e
                .get("ComplianceResourceId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let ct = e
                .get("ComplianceType")
                .and_then(Value::as_str)
                .unwrap_or("INSUFFICIENT_DATA")
                .to_string();
            let key = resource_key(&rt, &rid);
            entry.insert(
                key,
                EvaluationResult {
                    resource_type: rt,
                    resource_id: rid,
                    rule_name: rule_name.to_string(),
                    compliance_type: ct,
                    annotation: e
                        .get("Annotation")
                        .and_then(Value::as_str)
                        .map(String::from),
                    result_recorded_time: now,
                    config_rule_invoked_time: now,
                    ordering_timestamp: now,
                },
            );
        }
        if let Some(rule) = acc.rules.get_mut(rule_name) {
            rule.last_successful_evaluation_time = Some(now);
            rule.last_successful_invocation_time = Some(now);
            if rule.first_activated_time.is_none() {
                rule.first_activated_time = Some(now);
            }
        }
    }

    fn put_evaluations(&self, account: &str, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let token = require_str(body, "ResultToken")?;
        let rule_name = token.split('#').next().unwrap_or(&token).to_string();
        let evals = body
            .get("Evaluations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let test_mode = body
            .get("TestMode")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !test_mode {
            let now = Utc::now();
            let mut st = self.state.write();
            let acc = st.account_mut(account);
            let entry = acc.evaluations.entry(rule_name.clone()).or_default();
            for e in &evals {
                let rt = e
                    .get("ComplianceResourceType")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let rid = e
                    .get("ComplianceResourceId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let ct = e
                    .get("ComplianceType")
                    .and_then(Value::as_str)
                    .unwrap_or("INSUFFICIENT_DATA")
                    .to_string();
                let key = resource_key(&rt, &rid);
                entry.insert(
                    key,
                    EvaluationResult {
                        resource_type: rt,
                        resource_id: rid,
                        rule_name: rule_name.clone(),
                        compliance_type: ct,
                        annotation: e
                            .get("Annotation")
                            .and_then(Value::as_str)
                            .map(String::from),
                        result_recorded_time: now,
                        config_rule_invoked_time: now,
                        ordering_timestamp: now,
                    },
                );
            }
        }
        Ok(AwsResponse::ok_json(json!({ "FailedEvaluations": [] })))
    }

    fn put_external_evaluation(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let e = body
            .get("ExternalEvaluation")
            .cloned()
            .ok_or_else(|| invalid("ExternalEvaluation is required"))?;
        let rt = e
            .get("ComplianceResourceType")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let rid = e
            .get("ComplianceResourceId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let ct = e
            .get("ComplianceType")
            .and_then(Value::as_str)
            .unwrap_or("INSUFFICIENT_DATA")
            .to_string();
        let now = Utc::now();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let key = resource_key(&rt, &rid);
        acc.evaluations
            .entry(rule_name.clone())
            .or_default()
            .insert(
                key,
                EvaluationResult {
                    resource_type: rt,
                    resource_id: rid,
                    rule_name,
                    compliance_type: ct,
                    annotation: e
                        .get("Annotation")
                        .and_then(Value::as_str)
                        .map(String::from),
                    result_recorded_time: now,
                    config_rule_invoked_time: now,
                    ordering_timestamp: now,
                },
            );
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_evaluation_results(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let mut st = self.state.write();
        st.account_mut(account).evaluations.remove(&name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_custom_rule_policy(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let st = self.state.read();
        let policy = st
            .account(account)
            .and_then(|a| a.rules.get(&name))
            .and_then(|r| {
                r.source
                    .pointer("/CustomPolicyDetails/PolicyText")
                    .and_then(Value::as_str)
                    .map(String::from)
            });
        Ok(AwsResponse::ok_json(json!({ "PolicyText": policy })))
    }
}

// ─── Compliance ─────────────────────────────────────────────────────────────

/// Fold a set of per-resource compliance types into a single rule/resource
/// verdict: NON_COMPLIANT if any is, else COMPLIANT if any is, else
/// INSUFFICIENT_DATA.
fn fold_compliance<'a>(types: impl Iterator<Item = &'a str>) -> String {
    let mut any_compliant = false;
    let mut any_noncompliant = false;
    for t in types {
        match t {
            "NON_COMPLIANT" => any_noncompliant = true,
            "COMPLIANT" => any_compliant = true,
            _ => {}
        }
    }
    if any_noncompliant {
        "NON_COMPLIANT".into()
    } else if any_compliant {
        "COMPLIANT".into()
    } else {
        "INSUFFICIENT_DATA".into()
    }
}

impl ConfigService {
    fn describe_compliance_by_config_rule(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigRuleNames");
        let filter = string_list(body, "ComplianceTypes");
        let st = self.state.read();
        let mut list = Vec::new();
        if let Some(acc) = st.account(account) {
            for rule in acc.rules.values() {
                if !names.is_empty() && !names.contains(&rule.name) {
                    continue;
                }
                let results = acc.evaluations.get(&rule.name);
                let compliance = results
                    .map(|m| fold_compliance(m.values().map(|r| r.compliance_type.as_str())))
                    .unwrap_or_else(|| "INSUFFICIENT_DATA".into());
                if !filter.is_empty() && !filter.contains(&compliance) {
                    continue;
                }
                list.push(json!({
                    "ConfigRuleName": rule.name,
                    "Compliance": { "ComplianceType": compliance },
                }));
            }
        }
        Ok(paged_response(
            "ComplianceByConfigRules",
            list,
            body,
            "NextToken",
        ))
    }

    fn describe_compliance_by_resource(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rtype = body.get("ResourceType").and_then(Value::as_str);
        let rid = body.get("ResourceId").and_then(Value::as_str);
        let filter = string_list(body, "ComplianceTypes");
        let st = self.state.read();
        // Aggregate per resource across all rules.
        let mut per_resource: std::collections::BTreeMap<(String, String), Vec<String>> =
            std::collections::BTreeMap::new();
        if let Some(acc) = st.account(account) {
            for results in acc.evaluations.values() {
                for r in results.values() {
                    if r.resource_type.is_empty() {
                        continue;
                    }
                    if let Some(t) = rtype {
                        if r.resource_type != t {
                            continue;
                        }
                    }
                    if let Some(i) = rid {
                        if r.resource_id != i {
                            continue;
                        }
                    }
                    per_resource
                        .entry((r.resource_type.clone(), r.resource_id.clone()))
                        .or_default()
                        .push(r.compliance_type.clone());
                }
            }
        }
        let mut list = Vec::new();
        for ((rt, ri), types) in per_resource {
            let compliance = fold_compliance(types.iter().map(String::as_str));
            if !filter.is_empty() && !filter.contains(&compliance) {
                continue;
            }
            list.push(json!({
                "ResourceType": rt,
                "ResourceId": ri,
                "Compliance": { "ComplianceType": compliance },
            }));
        }
        Ok(paged_response(
            "ComplianceByResources",
            list,
            body,
            "NextToken",
        ))
    }

    fn get_compliance_details_by_config_rule(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let filter = string_list(body, "ComplianceTypes");
        let st = self.state.read();
        let results: Vec<Value> = st
            .account(account)
            .and_then(|a| a.evaluations.get(&name))
            .map(|m| {
                m.values()
                    .filter(|r| filter.is_empty() || filter.contains(&r.compliance_type))
                    .map(|r| Self::evaluation_result_json(r, &name))
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "EvaluationResults",
            results,
            body,
            "NextToken",
        ))
    }

    fn get_compliance_details_by_resource(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rtype = body
            .get("ResourceType")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let rid = body
            .get("ResourceId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let filter = string_list(body, "ComplianceTypes");
        let st = self.state.read();
        let mut results = Vec::new();
        if let Some(acc) = st.account(account) {
            for (rule_name, m) in &acc.evaluations {
                for r in m.values() {
                    if r.resource_type == rtype
                        && r.resource_id == rid
                        && (filter.is_empty() || filter.contains(&r.compliance_type))
                    {
                        results.push(Self::evaluation_result_json(r, rule_name));
                    }
                }
            }
        }
        Ok(paged_response(
            "EvaluationResults",
            results,
            body,
            "NextToken",
        ))
    }

    fn evaluation_result_json(r: &EvaluationResult, rule_name: &str) -> Value {
        json!({
            "EvaluationResultIdentifier": {
                "EvaluationResultQualifier": {
                    "ConfigRuleName": rule_name,
                    "ResourceType": r.resource_type,
                    "ResourceId": r.resource_id,
                },
                "OrderingTimestamp": r.ordering_timestamp.timestamp() as f64,
            },
            "ComplianceType": r.compliance_type,
            "ResultRecordedTime": r.result_recorded_time.timestamp() as f64,
            "ConfigRuleInvokedTime": r.config_rule_invoked_time.timestamp() as f64,
            "Annotation": r.annotation,
        })
    }

    fn get_compliance_summary_by_config_rule(
        &self,
        account: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let mut compliant = 0;
        let mut noncompliant = 0;
        if let Some(acc) = st.account(account) {
            for rule in acc.rules.values() {
                let c = acc
                    .evaluations
                    .get(&rule.name)
                    .map(|m| fold_compliance(m.values().map(|r| r.compliance_type.as_str())))
                    .unwrap_or_else(|| "INSUFFICIENT_DATA".into());
                match c.as_str() {
                    "COMPLIANT" => compliant += 1,
                    "NON_COMPLIANT" => noncompliant += 1,
                    _ => {}
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "ComplianceSummary": {
                "CompliantResourceCount": { "CappedCount": compliant, "CapExceeded": false },
                "NonCompliantResourceCount": { "CappedCount": noncompliant, "CapExceeded": false },
                "ComplianceSummaryTimestamp": Utc::now().timestamp() as f64,
            }
        })))
    }

    fn get_compliance_summary_by_resource_type(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let types = string_list(body, "ResourceTypes");
        let st = self.state.read();
        // Collect the folded compliance per resource, grouped by resource type.
        // A resource type is COMPLIANT/NON_COMPLIANT once per distinct resource
        // (folded across every rule that evaluated it).
        let mut per_type_resources: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, Vec<String>>,
        > = std::collections::BTreeMap::new();
        if let Some(acc) = st.account(account) {
            for m in acc.evaluations.values() {
                for r in m.values() {
                    if r.resource_type.is_empty() {
                        continue;
                    }
                    if !types.is_empty() && !types.contains(&r.resource_type) {
                        continue;
                    }
                    per_type_resources
                        .entry(r.resource_type.clone())
                        .or_default()
                        .entry(r.resource_id.clone())
                        .or_default()
                        .push(r.compliance_type.clone());
                }
            }
        }
        let now = Utc::now().timestamp() as f64;
        let summary_json = |resource_type: &str,
                            resources: &std::collections::BTreeMap<String, Vec<String>>|
         -> Value {
            let mut compliant = 0;
            let mut noncompliant = 0;
            for c in resources.values() {
                match fold_compliance(c.iter().map(String::as_str)).as_str() {
                    "COMPLIANT" => compliant += 1,
                    "NON_COMPLIANT" => noncompliant += 1,
                    _ => {}
                }
            }
            let summary = json!({
                "CompliantResourceCount": { "CappedCount": compliant, "CapExceeded": false },
                "NonCompliantResourceCount": { "CappedCount": noncompliant, "CapExceeded": false },
                "ComplianceSummaryTimestamp": now,
            });
            if resource_type.is_empty() {
                json!({ "ComplianceSummary": summary })
            } else {
                json!({ "ResourceType": resource_type, "ComplianceSummary": summary })
            }
        };
        let list: Vec<Value> = if types.is_empty() {
            // No filter: one aggregated summary over all resource types, with no
            // ResourceType label (matches AWS).
            let mut all: std::collections::BTreeMap<String, Vec<String>> =
                std::collections::BTreeMap::new();
            for resources in per_type_resources.values() {
                for (rid, c) in resources {
                    all.entry(rid.clone())
                        .or_default()
                        .extend(c.iter().cloned());
                }
            }
            vec![summary_json("", &all)]
        } else {
            // One summary per requested resource type (empty when the type has
            // no evaluated resources).
            types
                .iter()
                .map(|t| {
                    let empty = std::collections::BTreeMap::new();
                    let resources = per_type_resources.get(t).unwrap_or(&empty);
                    summary_json(t, resources)
                })
                .collect()
        };
        Ok(AwsResponse::ok_json(json!({
            "ComplianceSummariesByResourceType": list
        })))
    }
}

// ─── Remediation ────────────────────────────────────────────────────────────

impl ConfigService {
    fn put_remediation_configurations(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let configs = body
            .get("RemediationConfigurations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut failed = Vec::new();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        for c in &configs {
            let rule_name = match c.get("ConfigRuleName").and_then(Value::as_str) {
                Some(n) => n.to_string(),
                None => {
                    failed.push(json!({ "FailureMessage": "ConfigRuleName is required", "FailedItems": [c] }));
                    continue;
                }
            };
            acc.remediation_configs.insert(
                rule_name.clone(),
                RemediationConfiguration {
                    config_rule_name: rule_name.clone(),
                    arn: format!("arn:aws:config:{region}:{account}:remediation-configuration/{rule_name}/{}", short_id()),
                    target_type: c.get("TargetType").and_then(Value::as_str).unwrap_or("SSM_DOCUMENT").to_string(),
                    target_id: c.get("TargetId").and_then(Value::as_str).unwrap_or_default().to_string(),
                    target_version: c.get("TargetVersion").and_then(Value::as_str).map(String::from),
                    parameters: c.get("Parameters").cloned(),
                    resource_type: c.get("ResourceType").and_then(Value::as_str).map(String::from),
                    automatic: c.get("Automatic").and_then(Value::as_bool).unwrap_or(false),
                    execution_controls: c.get("ExecutionControls").cloned(),
                    maximum_automatic_attempts: c.get("MaximumAutomaticAttempts").and_then(Value::as_i64),
                    retry_attempt_seconds: c.get("RetryAttemptSeconds").and_then(Value::as_i64),
                    created_by_service: None,
                },
            );
        }
        Ok(AwsResponse::ok_json(json!({ "FailedBatches": failed })))
    }

    fn remediation_json(r: &RemediationConfiguration) -> Value {
        let mut v = json!({
            "ConfigRuleName": r.config_rule_name,
            "TargetType": r.target_type,
            "TargetId": r.target_id,
            "Arn": r.arn,
            "Automatic": r.automatic,
        });
        if let Some(t) = &r.target_version {
            v["TargetVersion"] = json!(t);
        }
        if let Some(p) = &r.parameters {
            v["Parameters"] = p.clone();
        }
        if let Some(rt) = &r.resource_type {
            v["ResourceType"] = json!(rt);
        }
        if let Some(e) = &r.execution_controls {
            v["ExecutionControls"] = e.clone();
        }
        if let Some(m) = r.maximum_automatic_attempts {
            v["MaximumAutomaticAttempts"] = json!(m);
        }
        if let Some(s) = r.retry_attempt_seconds {
            v["RetryAttemptSeconds"] = json!(s);
        }
        v
    }

    fn describe_remediation_configurations(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigRuleNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.remediation_configs
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.config_rule_name))
                    .map(Self::remediation_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "RemediationConfigurations": list }),
        ))
    }

    fn delete_remediation_configuration(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.remediation_configs.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchRemediationConfigurationException",
                "No RemediationConfiguration for rule exists.",
            ));
        }
        acc.remediation_executions
            .retain(|_, e| e.config_rule_name != name);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_remediation_exceptions(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let keys = body
            .get("ResourceKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let message = body
            .get("Message")
            .and_then(Value::as_str)
            .map(String::from);
        // ExpirationTime is a request-level field applied to every supplied key.
        let expiration_time = body
            .get("ExpirationTime")
            .and_then(Value::as_f64)
            .and_then(|s| DateTime::from_timestamp(s as i64, 0));
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        for k in &keys {
            let rt = k
                .get("ResourceType")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let rid = k
                .get("ResourceId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let key = format!("{rule_name}\u{1}{rt}\u{1}{rid}");
            acc.remediation_exceptions.insert(
                key,
                RemediationException {
                    config_rule_name: rule_name.clone(),
                    resource_type: rt,
                    resource_id: rid,
                    message: message.clone(),
                    expiration_time,
                },
            );
        }
        Ok(AwsResponse::ok_json(json!({ "FailedBatches": [] })))
    }

    fn describe_remediation_exceptions(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.remediation_exceptions
                    .values()
                    .filter(|e| e.config_rule_name == rule_name)
                    .map(|e| {
                        let mut v = json!({
                            "ConfigRuleName": e.config_rule_name,
                            "ResourceType": e.resource_type,
                            "ResourceId": e.resource_id,
                            "Message": e.message,
                        });
                        if let Some(exp) = e.expiration_time {
                            v["ExpirationTime"] = json!(exp.timestamp() as f64);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "RemediationExceptions",
            list,
            body,
            "NextToken",
        ))
    }

    fn delete_remediation_exceptions(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let keys = body
            .get("ResourceKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        for k in &keys {
            let rt = k
                .get("ResourceType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let rid = k
                .get("ResourceId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            acc.remediation_exceptions
                .remove(&format!("{rule_name}\u{1}{rt}\u{1}{rid}"));
        }
        Ok(AwsResponse::ok_json(json!({ "FailedBatches": [] })))
    }

    fn start_remediation_execution(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let keys = body
            .get("ResourceKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let now = Utc::now();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if !acc.remediation_configs.contains_key(&rule_name) {
            return Err(no_such(
                "NoSuchRemediationConfigurationException",
                "No RemediationConfiguration for rule exists.",
            ));
        }
        // Record an execution per resource key. The SSM Automation the target
        // document would drive is not simulated here, so the execution is
        // recorded as QUEUED (accepted, outcome unknown) rather than a
        // fabricated SUCCEEDED.
        let mut failed = Vec::new();
        for k in &keys {
            let (Some(rt), Some(rid)) = (
                k.get("resourceType")
                    .or_else(|| k.get("ResourceType"))
                    .and_then(Value::as_str),
                k.get("resourceId")
                    .or_else(|| k.get("ResourceId"))
                    .and_then(Value::as_str),
            ) else {
                failed.push(k.clone());
                continue;
            };
            let key = format!("{rule_name}\u{1}{rt}\u{1}{rid}");
            acc.remediation_executions.insert(
                key,
                RemediationExecutionStatus {
                    config_rule_name: rule_name.clone(),
                    resource_type: rt.to_string(),
                    resource_id: rid.to_string(),
                    state: "QUEUED".into(),
                    invocation_time: now,
                    last_updated_time: now,
                },
            );
        }
        Ok(AwsResponse::ok_json(json!({ "FailedItems": failed })))
    }

    fn describe_remediation_execution_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rule_name = require_str(body, "ConfigRuleName")?;
        let keys = body
            .get("ResourceKeys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // Restrict to the requested resource keys, when any were supplied.
        let wanted: Vec<(String, String)> = keys
            .iter()
            .filter_map(|k| {
                let rt = k
                    .get("resourceType")
                    .or_else(|| k.get("ResourceType"))
                    .and_then(Value::as_str)?;
                let rid = k
                    .get("resourceId")
                    .or_else(|| k.get("ResourceId"))
                    .and_then(Value::as_str)?;
                Some((rt.to_string(), rid.to_string()))
            })
            .collect();
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.remediation_executions
                    .values()
                    .filter(|e| e.config_rule_name == rule_name)
                    .filter(|e| {
                        wanted.is_empty()
                            || wanted
                                .iter()
                                .any(|(rt, rid)| *rt == e.resource_type && *rid == e.resource_id)
                    })
                    .map(|e| {
                        let inv = e.invocation_time.timestamp() as f64;
                        let upd = e.last_updated_time.timestamp() as f64;
                        json!({
                            "ResourceKey": { "resourceType": e.resource_type, "resourceId": e.resource_id },
                            "State": e.state,
                            "StepDetails": [{ "Name": "remediate", "State": e.state, "StartTime": inv }],
                            "InvocationTime": inv,
                            "LastUpdatedTime": upd,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "RemediationExecutionStatuses",
            list,
            body,
            "NextToken",
        ))
    }
}

// ─── Conformance packs ──────────────────────────────────────────────────────

/// Extract the `ConfigRuleName`s declared by a conformance-pack template body.
/// Used to scope conformance-pack compliance to the pack's own rules. Both JSON
/// and YAML CloudFormation templates are parsed (conformance-pack templates are
/// most commonly authored in YAML); a template with no parseable rule names
/// yields an empty list (compliance then aggregates over all account rules).
fn extract_pack_rule_names(template: Option<&str>) -> Vec<String> {
    let Some(t) = template else { return Vec::new() };
    // Try JSON first, then YAML (a superset that also accepts JSON, but JSON is
    // cheaper and the common case for programmatically-generated templates).
    let v: Value = serde_json::from_str(t)
        .or_else(|_| serde_yaml::from_str::<Value>(t))
        .unwrap_or(Value::Null);
    let Some(resources) = v.get("Resources").and_then(Value::as_object) else {
        return Vec::new();
    };
    resources
        .values()
        .filter(|r| r.get("Type").and_then(Value::as_str) == Some("AWS::Config::ConfigRule"))
        .filter_map(|r| {
            r.pointer("/Properties/ConfigRuleName")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .collect()
}

impl ConfigService {
    fn put_conformance_pack(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConformancePackName")?;
        let template_body = body
            .get("TemplateBody")
            .and_then(Value::as_str)
            .map(String::from);
        let template_s3_uri = body
            .get("TemplateS3Uri")
            .and_then(Value::as_str)
            .map(String::from);
        if template_body.is_none()
            && template_s3_uri.is_none()
            && body.get("TemplateSSMDocumentDetails").is_none()
        {
            return Err(invalid(
                "Either TemplateBody, TemplateS3Uri, or TemplateSSMDocumentDetails is required",
            ));
        }
        let now = Utc::now();
        let arn = format!(
            "arn:aws:config:{region}:{account}:conformance-pack/{name}-{}",
            short_id()
        );
        let id = format!("conformance-pack-{}", short_id());
        let rule_names = extract_pack_rule_names(template_body.as_deref());
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let existing = acc.conformance_packs.get(&name);
        let pack = ConformancePack {
            name: name.clone(),
            arn: existing.map(|p| p.arn.clone()).unwrap_or(arn.clone()),
            id: existing.map(|p| p.id.clone()).unwrap_or(id),
            delivery_s3_bucket: body
                .get("DeliveryS3Bucket")
                .and_then(Value::as_str)
                .map(String::from),
            delivery_s3_key_prefix: body
                .get("DeliveryS3KeyPrefix")
                .and_then(Value::as_str)
                .map(String::from),
            input_parameters: body
                .get("ConformancePackInputParameters")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            template_body,
            template_s3_uri,
            template_ssm_document_details: body.get("TemplateSSMDocumentDetails").cloned(),
            last_update_requested_time: now,
            created_by: None,
            rule_names,
        };
        let out_arn = pack.arn.clone();
        acc.conformance_packs.insert(name, pack);
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(out_arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "ConformancePackArn": out_arn }),
        ))
    }

    fn describe_conformance_packs(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConformancePackNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.conformance_packs
                    .values()
                    .filter(|p| names.is_empty() || names.contains(&p.name))
                    .map(|p| {
                        json!({
                            "ConformancePackName": p.name,
                            "ConformancePackArn": p.arn,
                            "ConformancePackId": p.id,
                            "DeliveryS3Bucket": p.delivery_s3_bucket,
                            "DeliveryS3KeyPrefix": p.delivery_s3_key_prefix,
                            "ConformancePackInputParameters": p.input_parameters,
                            "LastUpdateRequestedTime": p.last_update_requested_time.timestamp() as f64,
                            "TemplateSSMDocumentDetails": p.template_ssm_document_details,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            for n in &names {
                if !st
                    .account(account)
                    .map(|a| a.conformance_packs.contains_key(n))
                    .unwrap_or(false)
                {
                    return Err(no_such(
                        "NoSuchConformancePackException",
                        format!("Cannot find conformance pack with name '{n}'."),
                    ));
                }
            }
        }
        Ok(paged_response(
            "ConformancePackDetails",
            list,
            body,
            "NextToken",
        ))
    }

    fn delete_conformance_pack(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConformancePackName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.conformance_packs.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchConformancePackException",
                format!("Cannot find conformance pack with name '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_conformance_pack_status(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConformancePackNames");
        let now = Utc::now().timestamp() as f64;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.conformance_packs
                    .values()
                    .filter(|p| names.is_empty() || names.contains(&p.name))
                    .map(|p| {
                        json!({
                            "ConformancePackName": p.name,
                            "ConformancePackId": p.id,
                            "ConformancePackArn": p.arn,
                            "ConformancePackState": "CREATE_COMPLETE",
                            "StackArn": format!("arn:aws:cloudformation:{region}:{account}:stack/awsconfigconforms-{}/{}", p.name, short_id()),
                            "LastUpdateRequestedTime": now,
                            "LastUpdateCompletedTime": now,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "ConformancePackStatusDetails",
            list,
            body,
            "NextToken",
        ))
    }

    /// Rule names that belong to a pack (its own declared rules, or all account
    /// rules when the template did not declare any parseable ones).
    fn pack_rule_names(acc: &AccountState, pack: &ConformancePack) -> Vec<String> {
        if pack.rule_names.is_empty() {
            acc.rules.keys().cloned().collect()
        } else {
            pack.rule_names
                .iter()
                .filter(|n| acc.rules.contains_key(*n))
                .cloned()
                .collect()
        }
    }

    fn describe_conformance_pack_compliance(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConformancePackName")?;
        let st = self.state.read();
        let acc = st.account(account).ok_or_else(|| {
            no_such(
                "NoSuchConformancePackException",
                "No such conformance pack.",
            )
        })?;
        let pack = acc.conformance_packs.get(&name).ok_or_else(|| {
            no_such(
                "NoSuchConformancePackException",
                format!("Cannot find conformance pack with name '{name}'."),
            )
        })?;
        let list: Vec<Value> = Self::pack_rule_names(acc, pack)
            .iter()
            .map(|rule| {
                let c = acc
                    .evaluations
                    .get(rule)
                    .map(|m| fold_compliance(m.values().map(|r| r.compliance_type.as_str())))
                    .unwrap_or_else(|| "INSUFFICIENT_DATA".into());
                json!({ "ConfigRuleName": rule, "ComplianceType": c, "Controls": [] })
            })
            .collect();
        let (page, next) = paginate(list, body);
        let mut out =
            json!({ "ConformancePackName": name, "ConformancePackRuleComplianceList": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn get_conformance_pack_compliance_summary(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConformancePackNames");
        let st = self.state.read();
        let acc = st.account(account);
        let list: Vec<Value> = acc
            .map(|a| {
                a.conformance_packs
                    .values()
                    .filter(|p| names.is_empty() || names.contains(&p.name))
                    .map(|p| {
                        let rules = Self::pack_rule_names(a, p);
                        let noncompliant = rules.iter().any(|r| {
                            a.evaluations.get(r).map(|m| m.values().any(|e| e.compliance_type == "NON_COMPLIANT")).unwrap_or(false)
                        });
                        json!({
                            "ConformancePackName": p.name,
                            "ConformancePackComplianceStatus": if noncompliant { "NON_COMPLIANT" } else { "COMPLIANT" },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ConformancePackComplianceSummaryList": list }),
        ))
    }

    fn get_conformance_pack_compliance_details(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConformancePackName")?;
        let st = self.state.read();
        let acc = st.account(account).ok_or_else(|| {
            no_such(
                "NoSuchConformancePackException",
                "No such conformance pack.",
            )
        })?;
        let pack = acc.conformance_packs.get(&name).ok_or_else(|| {
            no_such(
                "NoSuchConformancePackException",
                format!("Cannot find conformance pack with name '{name}'."),
            )
        })?;
        let mut results = Vec::new();
        for rule in Self::pack_rule_names(acc, pack) {
            if let Some(m) = acc.evaluations.get(&rule) {
                for r in m.values() {
                    results.push(json!({
                        "ConfigRuleInvokedTime": r.config_rule_invoked_time.timestamp() as f64,
                        "ResultRecordedTime": r.result_recorded_time.timestamp() as f64,
                        "ComplianceType": r.compliance_type,
                        "EvaluationResultIdentifier": {
                            "EvaluationResultQualifier": {
                                "ConfigRuleName": rule,
                                "ResourceType": r.resource_type,
                                "ResourceId": r.resource_id,
                            },
                            "OrderingTimestamp": r.ordering_timestamp.timestamp() as f64,
                        },
                    }));
                }
            }
        }
        let (page, next) = paginate(results, body);
        let mut out =
            json!({ "ConformancePackName": name, "ConformancePackRuleEvaluationResults": page });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn list_conformance_pack_compliance_scores(
        &self,
        account: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.conformance_packs
                    .values()
                    .map(|p| {
                        let rules = Self::pack_rule_names(a, p);
                        let total = rules.len().max(1);
                        let compliant = rules
                            .iter()
                            .filter(|r| {
                                a.evaluations
                                    .get(*r)
                                    .map(|m| {
                                        !m.values().any(|e| e.compliance_type == "NON_COMPLIANT")
                                    })
                                    .unwrap_or(true)
                            })
                            .count();
                        let score = (compliant as f64 / total as f64) * 100.0;
                        json!({
                            "Score": format!("{score:.2}"),
                            "ConformancePackName": p.name,
                            "LastUpdatedTime": p.last_update_requested_time.timestamp() as f64,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ConformancePackComplianceScores": list }),
        ))
    }
}

// ─── Organization rules / packs ─────────────────────────────────────────────

impl ConfigService {
    fn put_organization_config_rule(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConfigRuleName")?;
        let arn = format!(
            "arn:aws:config:{region}:{account}:organization-config-rule/{name}-{}",
            short_id()
        );
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let out_arn = acc
            .org_rules
            .get(&name)
            .map(|r| r.arn.clone())
            .unwrap_or(arn);
        acc.org_rules.insert(
            name.clone(),
            OrganizationConfigRule {
                name,
                arn: out_arn.clone(),
                managed_rule_metadata: body.get("OrganizationManagedRuleMetadata").cloned(),
                custom_rule_metadata: body.get("OrganizationCustomRuleMetadata").cloned(),
                custom_policy_rule_metadata: body
                    .get("OrganizationCustomPolicyRuleMetadata")
                    .cloned(),
                excluded_accounts: string_list(body, "ExcludedAccounts"),
                last_update_time: Utc::now(),
            },
        );
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(out_arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConfigRuleArn": out_arn }),
        ))
    }

    fn describe_organization_config_rules(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "OrganizationConfigRuleNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.org_rules
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(|r| {
                        json!({
                            "OrganizationConfigRuleName": r.name,
                            "OrganizationConfigRuleArn": r.arn,
                            "OrganizationManagedRuleMetadata": r.managed_rule_metadata,
                            "OrganizationCustomRuleMetadata": r.custom_rule_metadata,
                            "OrganizationCustomPolicyRuleMetadata": r.custom_policy_rule_metadata,
                            "ExcludedAccounts": r.excluded_accounts,
                            "LastUpdateTime": r.last_update_time.timestamp() as f64,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConfigRules": list }),
        ))
    }

    fn delete_organization_config_rule(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConfigRuleName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.org_rules.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchOrganizationConfigRuleException",
                format!("Cannot find organization config rule '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_organization_config_rule_statuses(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "OrganizationConfigRuleNames");
        let now = Utc::now().timestamp() as f64;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.org_rules
                    .values()
                    .filter(|r| names.is_empty() || names.contains(&r.name))
                    .map(|r| {
                        json!({ "OrganizationConfigRuleName": r.name, "OrganizationRuleStatus": "CREATE_SUCCESSFUL", "LastUpdateTime": now })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConfigRuleStatuses": list }),
        ))
    }

    fn get_organization_config_rule_detailed_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConfigRuleName")?;
        let st = self.state.read();
        if !st
            .account(account)
            .map(|a| a.org_rules.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(no_such(
                "NoSuchOrganizationConfigRuleException",
                format!("Cannot find organization config rule '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConfigRuleDetailedStatus": [] }),
        ))
    }

    fn get_organization_custom_rule_policy(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConfigRuleName")?;
        let st = self.state.read();
        let policy = st
            .account(account)
            .and_then(|a| a.org_rules.get(&name))
            .and_then(|r| r.custom_policy_rule_metadata.as_ref())
            .and_then(|m| {
                m.get("PolicyText")
                    .and_then(Value::as_str)
                    .map(String::from)
            });
        Ok(AwsResponse::ok_json(json!({ "PolicyText": policy })))
    }

    fn put_organization_conformance_pack(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConformancePackName")?;
        let arn = format!(
            "arn:aws:config:{region}:{account}:organization-conformance-pack/{name}-{}",
            short_id()
        );
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let out_arn = acc
            .org_conformance_packs
            .get(&name)
            .map(|p| p.arn.clone())
            .unwrap_or(arn);
        acc.org_conformance_packs.insert(
            name.clone(),
            OrganizationConformancePack {
                name,
                arn: out_arn.clone(),
                delivery_s3_bucket: body
                    .get("DeliveryS3Bucket")
                    .and_then(Value::as_str)
                    .map(String::from),
                delivery_s3_key_prefix: body
                    .get("DeliveryS3KeyPrefix")
                    .and_then(Value::as_str)
                    .map(String::from),
                input_parameters: body
                    .get("ConformancePackInputParameters")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                template_body: body
                    .get("TemplateBody")
                    .and_then(Value::as_str)
                    .map(String::from),
                template_s3_uri: body
                    .get("TemplateS3Uri")
                    .and_then(Value::as_str)
                    .map(String::from),
                excluded_accounts: string_list(body, "ExcludedAccounts"),
                last_update_time: Utc::now(),
            },
        );
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(out_arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConformancePackArn": out_arn }),
        ))
    }

    fn describe_organization_conformance_packs(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "OrganizationConformancePackNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.org_conformance_packs
                    .values()
                    .filter(|p| names.is_empty() || names.contains(&p.name))
                    .map(|p| {
                        json!({
                            "OrganizationConformancePackName": p.name,
                            "OrganizationConformancePackArn": p.arn,
                            "DeliveryS3Bucket": p.delivery_s3_bucket,
                            "DeliveryS3KeyPrefix": p.delivery_s3_key_prefix,
                            "ConformancePackInputParameters": p.input_parameters,
                            "ExcludedAccounts": p.excluded_accounts,
                            "LastUpdateTime": p.last_update_time.timestamp() as f64,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConformancePacks": list }),
        ))
    }

    fn delete_organization_conformance_pack(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConformancePackName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.org_conformance_packs.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchOrganizationConformancePackException",
                format!("Cannot find organization conformance pack '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_organization_conformance_pack_statuses(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "OrganizationConformancePackNames");
        let now = Utc::now().timestamp() as f64;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.org_conformance_packs
                    .values()
                    .filter(|p| names.is_empty() || names.contains(&p.name))
                    .map(|p| json!({ "OrganizationConformancePackName": p.name, "Status": "CREATE_SUCCESSFUL", "LastUpdateTime": now }))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConformancePackStatuses": list }),
        ))
    }

    fn get_organization_conformance_pack_detailed_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "OrganizationConformancePackName")?;
        let st = self.state.read();
        if !st
            .account(account)
            .map(|a| a.org_conformance_packs.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(no_such(
                "NoSuchOrganizationConformancePackException",
                format!("Cannot find organization conformance pack '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(
            json!({ "OrganizationConformancePackDetailedStatuses": [] }),
        ))
    }
}

// ─── Aggregators ────────────────────────────────────────────────────────────

impl ConfigService {
    fn put_configuration_aggregator(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationAggregatorName")?;
        let now = Utc::now();
        let arn = format!(
            "arn:aws:config:{region}:{account}:config-aggregator/config-aggregator-{}",
            short_id()
        );
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let existing = acc.aggregators.get(&name);
        let agg = ConfigurationAggregator {
            name: name.clone(),
            arn: existing.map(|a| a.arn.clone()).unwrap_or(arn),
            account_aggregation_sources: body
                .get("AccountAggregationSources")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            organization_aggregation_source: body.get("OrganizationAggregationSource").cloned(),
            creation_time: existing.map(|a| a.creation_time).unwrap_or(now),
            last_updated_time: now,
            created_by: None,
        };
        let out = Self::aggregator_json(&agg);
        let arn_for_tags = agg.arn.clone();
        acc.aggregators.insert(name, agg);
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(arn_for_tags).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "ConfigurationAggregator": out }),
        ))
    }

    fn aggregator_json(a: &ConfigurationAggregator) -> Value {
        json!({
            "ConfigurationAggregatorName": a.name,
            "ConfigurationAggregatorArn": a.arn,
            "AccountAggregationSources": a.account_aggregation_sources,
            "OrganizationAggregationSource": a.organization_aggregation_source,
            "CreationTime": a.creation_time.timestamp() as f64,
            "LastUpdatedTime": a.last_updated_time.timestamp() as f64,
        })
    }

    fn describe_configuration_aggregators(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let names = string_list(body, "ConfigurationAggregatorNames");
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.aggregators
                    .values()
                    .filter(|g| names.is_empty() || names.contains(&g.name))
                    .map(Self::aggregator_json)
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            for n in &names {
                if !st
                    .account(account)
                    .map(|a| a.aggregators.contains_key(n))
                    .unwrap_or(false)
                {
                    return Err(no_such(
                        "NoSuchConfigurationAggregatorException",
                        format!("The configuration aggregator '{n}' does not exist."),
                    ));
                }
            }
        }
        Ok(paged_response(
            "ConfigurationAggregators",
            list,
            body,
            "NextToken",
        ))
    }

    fn delete_configuration_aggregator(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationAggregatorName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.aggregators.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchConfigurationAggregatorException",
                format!("The configuration aggregator '{name}' does not exist."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_configuration_aggregator_sources_status(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigurationAggregatorName")?;
        let now = Utc::now().timestamp() as f64;
        let st = self.state.read();
        let agg = st.account(account).and_then(|a| a.aggregators.get(&name));
        let Some(agg) = agg else {
            return Err(no_such(
                "NoSuchConfigurationAggregatorException",
                format!("The configuration aggregator '{name}' does not exist."),
            ));
        };
        let mut list = Vec::new();
        for src in &agg.account_aggregation_sources {
            let region = src
                .get("AwsRegions")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_str)
                .unwrap_or("us-east-1");
            let accounts = src
                .get("AccountIds")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for acct in accounts {
                list.push(json!({
                    "SourceId": acct,
                    "SourceType": "ACCOUNT",
                    "AwsRegion": region,
                    "LastUpdateStatus": "SUCCEEDED",
                    "LastUpdateTime": now,
                }));
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "AggregatedSourceStatusList": list }),
        ))
    }

    fn put_aggregation_authorization(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let authorized_account = require_str(body, "AuthorizedAccountId")?;
        let authorized_region = require_str(body, "AuthorizedAwsRegion")?;
        let now = Utc::now();
        let arn = format!("arn:aws:config:{region}:{account}:aggregation-authorization/{authorized_account}/{authorized_region}");
        let key = format!("{authorized_account}\u{1}{authorized_region}");
        let auth = AggregationAuthorization {
            arn: arn.clone(),
            authorized_account_id: authorized_account,
            authorized_aws_region: authorized_region,
            creation_time: now,
        };
        let out = Self::auth_json(&auth);
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        acc.aggregation_authorizations.insert(key, auth);
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        let _ = out;
        Ok(AwsResponse::ok_json(
            json!({ "AggregationAuthorization": Self::auth_json_by_arn(&arn) }),
        ))
    }

    fn auth_json(a: &AggregationAuthorization) -> Value {
        json!({
            "AggregationAuthorizationArn": a.arn,
            "AuthorizedAccountId": a.authorized_account_id,
            "AuthorizedAwsRegion": a.authorized_aws_region,
            "CreationTime": a.creation_time.timestamp() as f64,
        })
    }

    fn auth_json_by_arn(arn: &str) -> Value {
        json!({ "AggregationAuthorizationArn": arn })
    }

    fn describe_aggregation_authorizations(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.aggregation_authorizations
                    .values()
                    .map(Self::auth_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "AggregationAuthorizations",
            list,
            body,
            "NextToken",
        ))
    }

    fn delete_aggregation_authorization(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let authorized_account = require_str(body, "AuthorizedAccountId")?;
        let authorized_region = require_str(body, "AuthorizedAwsRegion")?;
        let key = format!("{authorized_account}\u{1}{authorized_region}");
        let mut st = self.state.write();
        st.account_mut(account)
            .aggregation_authorizations
            .remove(&key);
        Ok(AwsResponse::ok_json(json!({})))
    }

    /// Aggregators aggregate over the same recorded items (single-account,
    /// single-region deployment) — real aggregation across the account's own
    /// recorded state.
    fn batch_get_aggregate_resource_config(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let keys = body
            .get("ResourceIdentifiers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let st = self.state.read();
        let acc = st.account(account);
        let mut items = Vec::new();
        let mut unprocessed = Vec::new();
        for k in keys {
            let rt = k
                .get("ResourceType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let rid = k
                .get("ResourceId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let key = resource_key(rt, rid);
            match acc
                .and_then(|a| a.config_items.get(&key))
                .and_then(|h| h.last())
            {
                Some(ci) => items.push(Self::config_item_json(ci)),
                None => unprocessed.push(k),
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "BaseConfigurationItems": items, "UnprocessedResourceIdentifiers": unprocessed }),
        ))
    }

    fn get_aggregate_resource_config(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ident = body
            .get("ResourceIdentifier")
            .cloned()
            .unwrap_or(Value::Null);
        let rt = ident
            .get("ResourceType")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let rid = ident
            .get("ResourceId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let st = self.state.read();
        let key = resource_key(rt, rid);
        let ci = st
            .account(account)
            .and_then(|a| a.config_items.get(&key))
            .and_then(|h| h.last());
        match ci {
            Some(ci) => Ok(AwsResponse::ok_json(
                json!({ "ConfigurationItem": Self::config_item_json(ci) }),
            )),
            None => Err(no_such(
                "ResourceNotDiscoveredException",
                "Resource is not discovered by the aggregator.",
            )),
        }
    }

    fn list_aggregate_discovered_resources(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_type = require_str(body, "ResourceType")?;
        let st = self.state.read();
        let mut list = Vec::new();
        if let Some(acc) = st.account(account) {
            for (key, history) in &acc.config_items {
                let Some((rt, _)) = key.split_once('\u{1}') else {
                    continue;
                };
                if rt != resource_type {
                    continue;
                }
                let Some(ci) = history.last() else { continue };
                if ci.configuration_item_status.starts_with("ResourceDeleted") {
                    continue;
                }
                list.push(json!({
                    "SourceAccountId": account,
                    "SourceRegion": ci.aws_region,
                    "ResourceId": ci.resource_id,
                    "ResourceType": ci.resource_type,
                    "ResourceName": ci.resource_name,
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({ "ResourceIdentifiers": list })))
    }

    fn get_aggregate_discovered_resource_counts(
        &self,
        account: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
        if let Some(acc) = st.account(account) {
            for (key, history) in &acc.config_items {
                let Some((rt, _)) = key.split_once('\u{1}') else {
                    continue;
                };
                if history
                    .last()
                    .map(|ci| ci.configuration_item_status.starts_with("ResourceDeleted"))
                    .unwrap_or(true)
                {
                    continue;
                }
                *counts.entry(rt.to_string()).or_insert(0) += 1;
            }
        }
        let total: i64 = counts.values().sum();
        let list: Vec<Value> = counts
            .into_iter()
            .map(|(t, c)| json!({ "ResourceType": t, "Count": c }))
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "TotalDiscoveredResources": total, "GroupedResourceCounts": list }),
        ))
    }

    fn describe_aggregate_compliance_by_config_rules(
        &self,
        account: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let mut list = Vec::new();
        if let Some(acc) = st.account(account) {
            for rule in acc.rules.values() {
                let c = acc
                    .evaluations
                    .get(&rule.name)
                    .map(|m| fold_compliance(m.values().map(|r| r.compliance_type.as_str())))
                    .unwrap_or_else(|| "INSUFFICIENT_DATA".into());
                list.push(json!({
                    "ConfigRuleName": rule.name,
                    "Compliance": { "ComplianceType": c },
                    "AccountId": account,
                    "AwsRegion": "us-east-1",
                }));
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "AggregateComplianceByConfigRules": list }),
        ))
    }

    fn describe_aggregate_compliance_by_conformance_packs(
        &self,
        account: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.conformance_packs
                    .values()
                    .map(|p| {
                        let rules = Self::pack_rule_names(a, p);
                        let noncompliant = rules.iter().any(|r| a.evaluations.get(r).map(|m| m.values().any(|e| e.compliance_type == "NON_COMPLIANT")).unwrap_or(false));
                        json!({
                            "ConformancePackName": p.name,
                            "Compliance": { "ComplianceType": if noncompliant { "NON_COMPLIANT" } else { "COMPLIANT" } },
                            "AccountId": account,
                            "AwsRegion": "us-east-1",
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "AggregateComplianceByConformancePacks": list }),
        ))
    }

    fn get_aggregate_compliance_details_by_config_rule(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "ConfigRuleName")?;
        let st = self.state.read();
        let results: Vec<Value> = st
            .account(account)
            .and_then(|a| a.evaluations.get(&name))
            .map(|m| {
                m.values()
                    .map(|r| {
                        json!({
                            "AccountId": account,
                            "AwsRegion": "us-east-1",
                            "ComplianceType": r.compliance_type,
                            "ResultRecordedTime": r.result_recorded_time.timestamp() as f64,
                            "ConfigRuleInvokedTime": r.config_rule_invoked_time.timestamp() as f64,
                            "Annotation": r.annotation,
                            "EvaluationResultIdentifier": {
                                "EvaluationResultQualifier": {
                                    "ConfigRuleName": name,
                                    "ResourceType": r.resource_type,
                                    "ResourceId": r.resource_id,
                                },
                                "OrderingTimestamp": r.ordering_timestamp.timestamp() as f64,
                            },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "AggregateEvaluationResults": results }),
        ))
    }

    fn get_aggregate_config_rule_compliance_summary(
        &self,
        account: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let mut compliant = 0;
        let mut noncompliant = 0;
        if let Some(acc) = st.account(account) {
            for rule in acc.rules.values() {
                match acc
                    .evaluations
                    .get(&rule.name)
                    .map(|m| fold_compliance(m.values().map(|r| r.compliance_type.as_str())))
                    .unwrap_or_else(|| "INSUFFICIENT_DATA".into())
                    .as_str()
                {
                    "COMPLIANT" => compliant += 1,
                    "NON_COMPLIANT" => noncompliant += 1,
                    _ => {}
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "GroupByKey": "ACCOUNT_ID",
            "AggregateComplianceCounts": [{
                "GroupName": account,
                "ComplianceSummary": {
                    "CompliantResourceCount": { "CappedCount": compliant, "CapExceeded": false },
                    "NonCompliantResourceCount": { "CappedCount": noncompliant, "CapExceeded": false },
                    "ComplianceSummaryTimestamp": Utc::now().timestamp() as f64,
                }
            }]
        })))
    }

    fn get_aggregate_conformance_pack_compliance_summary(
        &self,
        account: &str,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let mut compliant = 0;
        let mut noncompliant = 0;
        if let Some(acc) = st.account(account) {
            for p in acc.conformance_packs.values() {
                let rules = Self::pack_rule_names(acc, p);
                let nc = rules.iter().any(|r| {
                    acc.evaluations
                        .get(r)
                        .map(|m| m.values().any(|e| e.compliance_type == "NON_COMPLIANT"))
                        .unwrap_or(false)
                });
                if nc {
                    noncompliant += 1;
                } else {
                    compliant += 1;
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "GroupByKey": "ACCOUNT_ID",
            "AggregateConformancePackComplianceSummaries": [{
                "GroupName": account,
                "ComplianceSummary": {
                    "CompliantConformancePackCount": compliant,
                    "NonCompliantConformancePackCount": noncompliant,
                }
            }]
        })))
    }
}

// ─── Retention / stored queries / resource evaluations / select / tags ──────

impl ConfigService {
    fn put_retention_configuration(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let days = body
            .get("RetentionPeriodInDays")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid("RetentionPeriodInDays is required"))?;
        if !(30..=2557).contains(&days) {
            return Err(invalid("RetentionPeriodInDays must be between 30 and 2557"));
        }
        let mut st = self.state.write();
        st.account_mut(account).retention_configurations.insert(
            "default".into(),
            RetentionConfiguration {
                name: "default".into(),
                retention_period_in_days: days,
            },
        );
        Ok(AwsResponse::ok_json(
            json!({ "RetentionConfiguration": { "Name": "default", "RetentionPeriodInDays": days } }),
        ))
    }

    fn describe_retention_configurations(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| a.retention_configurations.values().map(|r| json!({ "Name": r.name, "RetentionPeriodInDays": r.retention_period_in_days })).collect())
            .unwrap_or_default();
        Ok(paged_response(
            "RetentionConfigurations",
            list,
            body,
            "NextToken",
        ))
    }

    fn delete_retention_configuration(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "RetentionConfigurationName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.retention_configurations.remove(&name).is_none() {
            return Err(no_such(
                "NoSuchRetentionConfigurationException",
                format!("Cannot find retention configuration with the specified name '{name}'."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_stored_query(
        &self,
        account: &str,
        region: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let q = body
            .get("StoredQuery")
            .cloned()
            .ok_or_else(|| invalid("StoredQuery is required"))?;
        let name = q
            .get("QueryName")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("QueryName is required"))?
            .to_string();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let existing = acc.stored_queries.get(&name);
        let id = existing
            .map(|s| s.id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let arn = existing.map(|s| s.arn.clone()).unwrap_or_else(|| {
            format!("arn:aws:config:{region}:{account}:stored-query/{name}/{id}")
        });
        acc.stored_queries.insert(
            name.clone(),
            StoredQuery {
                id: id.clone(),
                name,
                arn: arn.clone(),
                description: q
                    .get("Description")
                    .and_then(Value::as_str)
                    .map(String::from),
                expression: q
                    .get("Expression")
                    .and_then(Value::as_str)
                    .map(String::from),
            },
        );
        let _ = id;
        let tags = parse_create_tags(body);
        if !tags.is_empty() {
            let entry = acc.tags.entry(arn.clone()).or_default();
            for (k, v) in tags {
                entry.insert(k, v);
            }
        }
        Ok(AwsResponse::ok_json(json!({ "QueryArn": arn })))
    }

    fn get_stored_query(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "QueryName")?;
        let st = self.state.read();
        let q = st
            .account(account)
            .and_then(|a| a.stored_queries.get(&name));
        let Some(q) = q else {
            return Err(no_such(
                "ResourceNotFoundException",
                format!("The stored query '{name}' does not exist."),
            ));
        };
        Ok(AwsResponse::ok_json(json!({
            "StoredQuery": {
                "QueryId": q.id,
                "QueryArn": q.arn,
                "QueryName": q.name,
                "Description": q.description,
                "Expression": q.expression,
            }
        })))
    }

    fn delete_stored_query(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_str(body, "QueryName")?;
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if acc.stored_queries.remove(&name).is_none() {
            return Err(no_such(
                "ResourceNotFoundException",
                format!("The stored query '{name}' does not exist."),
            ));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_stored_queries(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| a.stored_queries.values().map(|q| json!({ "QueryId": q.id, "QueryArn": q.arn, "QueryName": q.name, "Description": q.description })).collect())
            .unwrap_or_default();
        Ok(paged_response(
            "StoredQueryMetadata",
            list,
            body,
            "NextToken",
        ))
    }

    fn start_resource_evaluation(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mode = body
            .get("EvaluationMode")
            .and_then(Value::as_str)
            .unwrap_or("DETECTIVE")
            .to_string();
        let details = body.get("ResourceDetails").cloned();
        // Evaluate the supplied resource configuration against the account's
        // managed rules for real, rather than storing a canned COMPLIANT.
        let (rtype, rid, config_str) = details
            .as_ref()
            .map(|d| {
                let rt = d
                    .get("ResourceType")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let ri = d
                    .get("ResourceId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                // ResourceConfiguration is a JSON document string in the AWS
                // API; tolerate a raw object too.
                let cfg = match d.get("ResourceConfiguration") {
                    Some(Value::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => "null".to_string(),
                };
                (rt, ri, cfg)
            })
            .unwrap_or_default();
        let id = Uuid::new_v4().to_string();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let evaluation_results = if rtype.is_empty() || rid.is_empty() {
            Vec::new()
        } else {
            validate::evaluate_proactive(acc, &rtype, &rid, &config_str)
        };
        acc.resource_evaluations.insert(
            id.clone(),
            ResourceEvaluation {
                resource_evaluation_id: id.clone(),
                evaluation_mode: mode,
                time_stamp: Utc::now(),
                status: "SUCCEEDED".into(),
                resource_details: details,
                evaluation_context: body.get("EvaluationContext").cloned(),
                evaluation_results,
            },
        );
        Ok(AwsResponse::ok_json(json!({ "ResourceEvaluationId": id })))
    }

    fn get_resource_evaluation_summary(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_str(body, "ResourceEvaluationId")?;
        let st = self.state.read();
        let ev = st
            .account(account)
            .and_then(|a| a.resource_evaluations.get(&id));
        let Some(ev) = ev else {
            return Err(no_such(
                "ResourceNotFoundException",
                format!("ResourceEvaluationId '{id}' does not exist."),
            ));
        };
        let resource_type = ev
            .resource_details
            .as_ref()
            .and_then(|d| d.get("ResourceType"))
            .cloned()
            .unwrap_or(Value::Null);
        let resource_id = ev
            .resource_details
            .as_ref()
            .and_then(|d| d.get("ResourceId"))
            .cloned()
            .unwrap_or(Value::Null);
        // Real compliance folded from the recorded per-rule outcomes. No
        // applicable managed rule -> INSUFFICIENT_DATA, never a canned pass.
        let compliance = if ev.evaluation_results.is_empty() {
            "INSUFFICIENT_DATA".to_string()
        } else {
            fold_compliance(
                ev.evaluation_results
                    .iter()
                    .map(|r| r.compliance_type.as_str()),
            )
        };
        Ok(AwsResponse::ok_json(json!({
            "ResourceEvaluationId": ev.resource_evaluation_id,
            "EvaluationMode": ev.evaluation_mode,
            "EvaluationStatus": { "Status": ev.status },
            "EvaluationStartTimestamp": ev.time_stamp.timestamp() as f64,
            "Compliance": compliance,
            "ResourceDetails": { "ResourceId": resource_id, "ResourceType": resource_type },
        })))
    }

    fn list_resource_evaluations(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .map(|a| {
                a.resource_evaluations
                    .values()
                    .map(|e| json!({ "ResourceEvaluationId": e.resource_evaluation_id, "EvaluationMode": e.evaluation_mode, "EvaluationStartTimestamp": e.time_stamp.timestamp() as f64 }))
                    .collect()
            })
            .unwrap_or_default();
        Ok(paged_response(
            "ResourceEvaluations",
            list,
            body,
            "NextToken",
        ))
    }

    fn select_resource_config(
        &self,
        account: &str,
        body: &Value,
        aggregate: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let expr = require_str(body, "Expression")?;
        let st = self.state.read();
        let empty = AccountState::default();
        let acc = st.account(account).unwrap_or(&empty);
        let rows = validate::run_select(&expr, acc)
            .map_err(|e| invalid(format!("Invalid SelectResourceConfig expression: {e}")))?;
        let results: Vec<Value> = rows.iter().map(|r| json!(r.to_string())).collect();
        let (page, next) = paginate(results, body);
        let mut out = json!({
            "Results": page,
            "QueryInfo": { "SelectFields": select_fields(&expr) },
        });
        if let Some(t) = next {
            out["NextToken"] = json!(t);
        }
        if aggregate {
            let _ = out.as_object_mut();
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn tag_resource(&self, account: &str, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_str(body, "ResourceArn")?;
        let tags = body
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        let entry = acc.tags.entry(arn).or_default();
        for t in tags {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                entry.insert(k.to_string(), v.to_string());
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, account: &str, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_str(body, "ResourceArn")?;
        let keys = string_list(body, "TagKeys");
        let mut st = self.state.write();
        let acc = st.account_mut(account);
        if let Some(entry) = acc.tags.get_mut(&arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(
        &self,
        account: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = require_str(body, "ResourceArn")?;
        let st = self.state.read();
        let list: Vec<Value> = st
            .account(account)
            .and_then(|a| a.tags.get(&arn))
            .map(|m| {
                m.iter()
                    .map(|(k, v)| json!({ "Key": k, "Value": v }))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Tags": list })))
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Parse a Put* request's create-time `Tags` (a `TagsList` of `{Key, Value}`)
/// into `(key, value)` pairs. Config Put ops accept Tags at create time but
/// historically dropped them, so ListTagsForResource returned nothing until a
/// follow-up TagResource (bug-hunt).
fn parse_create_tags(body: &Value) -> Vec<(String, String)> {
    body.get("Tags")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    Some((
                        t.get("Key").and_then(Value::as_str)?.to_string(),
                        t.get("Value").and_then(Value::as_str)?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn account_id(req: &AwsRequest) -> String {
    if req.account_id.is_empty() {
        "000000000000".to_string()
    } else {
        req.account_id.clone()
    }
}

fn region(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

fn short_id() -> String {
    fakecloud_core::ids::short_id(6)
}

fn invalid(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn no_such(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg)
}

fn require_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or_else(|| invalid(format!("{field} is required")))
}

fn string_list(body: &Value, field: &str) -> Vec<String> {
    body.get(field)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Read the page-size cap from a request body, tolerating the several field
/// spellings AWS Config uses across operations (`Limit`, `MaxResults`, and the
/// lowercase `limit` on the resource-config data-plane ops).
fn page_limit(body: &Value) -> Option<usize> {
    ["Limit", "MaxResults", "limit"]
        .iter()
        .find_map(|k| body.get(*k).and_then(Value::as_i64))
        .filter(|l| *l > 0)
        .map(|l| l as usize)
}

/// Read the continuation token, tolerating both `NextToken` (control plane) and
/// `nextToken` (data plane) spellings.
fn page_token(body: &Value) -> Option<usize> {
    ["NextToken", "nextToken"]
        .iter()
        .find_map(|k| body.get(*k).and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .and_then(decode_page_token)
}

/// Tokens are an opaque offset into the full ordered result list. They are
/// encoded so callers treat them as opaque; decoding a malformed token yields
/// `None` (the page then restarts from the beginning rather than erroring).
fn encode_page_token(offset: usize) -> String {
    format!("cfg:{offset}")
}

fn decode_page_token(token: &str) -> Option<usize> {
    token.strip_prefix("cfg:").and_then(|n| n.parse().ok())
}

/// Slice `items` to a single page given the request's `Limit`/`NextToken`.
/// Returns the page and the token for the next page (`None` when exhausted).
fn paginate(items: Vec<Value>, body: &Value) -> (Vec<Value>, Option<String>) {
    let total = items.len();
    let start = page_token(body).unwrap_or(0).min(total);
    let end = match page_limit(body) {
        Some(limit) => start.saturating_add(limit).min(total),
        None => total,
    };
    let next = if end < total {
        Some(encode_page_token(end))
    } else {
        None
    };
    (
        items.into_iter().skip(start).take(end - start).collect(),
        next,
    )
}

/// Build a paginated list response: the list under `field`, plus a continuation
/// token under `token_field` (`NextToken` for the control plane, `nextToken`
/// for the data plane) when more items remain.
fn paged_response(field: &str, items: Vec<Value>, body: &Value, token_field: &str) -> AwsResponse {
    let (page, next) = paginate(items, body);
    let mut out = json!({ field: page });
    if let Some(t) = next {
        out[token_field] = json!(t);
    }
    AwsResponse::ok_json(out)
}

/// Build the invoking event handed to a custom-Lambda config rule. The rule's
/// stored `InputParameters` are passed through verbatim as `ruleParameters` (a
/// JSON string), exactly as AWS Config does, defaulting to `"{}"` when the rule
/// declared none — a custom rule that reads its parameters must see them.
fn custom_rule_event(
    account: &str,
    region: &str,
    rule_name: &str,
    input_parameters: Option<&str>,
    result_token: &str,
) -> Value {
    let invoking_event = json!({
        "awsAccountId": account,
        "configRuleName": rule_name,
        "messageType": "ScheduledNotification",
        "notificationCreationTime": Utc::now().to_rfc3339(),
    })
    .to_string();
    let rule_parameters = input_parameters
        .filter(|p| !p.is_empty())
        .map(String::from)
        .unwrap_or_else(|| "{}".to_string());
    json!({
        "invokingEvent": invoking_event,
        "ruleParameters": rule_parameters,
        "resultToken": result_token,
        "configRuleArn": format!("arn:aws:config:{region}:{account}:config-rule/{rule_name}"),
        "configRuleName": rule_name,
        "accountId": account,
    })
}

/// Best-effort extraction of the SELECT field names for the `QueryInfo`.
fn select_fields(expr: &str) -> Vec<Value> {
    let lower = expr.to_ascii_lowercase();
    let Some(start) = lower.find("select ") else {
        return Vec::new();
    };
    let after = &expr[start + 7..];
    let end = after
        .to_ascii_lowercase()
        .find(" where ")
        .unwrap_or(after.len());
    after[..end]
        .split(',')
        .map(|s| json!({ "Name": s.trim() }))
        .collect()
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn custom_rule_event_passes_stored_input_parameters() {
        let params = r#"{"maxAccessKeyAge":"90"}"#;
        let event = custom_rule_event(
            "111122223333",
            "eu-west-1",
            "my-rule",
            Some(params),
            "my-rule#tok",
        );
        // ruleParameters must carry the rule's stored InputParameters verbatim,
        // not a hardcoded "{}".
        assert_eq!(event["ruleParameters"], json!(params));
        assert_eq!(event["configRuleName"], json!("my-rule"));
        assert_eq!(event["accountId"], json!("111122223333"));
        assert_eq!(event["resultToken"], json!("my-rule#tok"));
        // The event's configRuleArn must carry the rule's region so it matches
        // the ARN DescribeConfigRules returns, not a hardcoded us-east-1.
        assert_eq!(
            event["configRuleArn"],
            json!("arn:aws:config:eu-west-1:111122223333:config-rule/my-rule")
        );
    }

    #[test]
    fn custom_rule_event_defaults_empty_parameters_to_empty_object() {
        let none = custom_rule_event("111122223333", "us-west-2", "r", None, "r#t");
        assert_eq!(none["ruleParameters"], json!("{}"));
        let empty = custom_rule_event("111122223333", "us-west-2", "r", Some(""), "r#t");
        assert_eq!(empty["ruleParameters"], json!("{}"));
    }

    #[test]
    fn paginate_slices_and_emits_token() {
        let items: Vec<Value> = (0..5).map(|i| json!(i)).collect();
        let first_req = json!({ "Limit": 2 });
        let (page, next) = paginate(items.clone(), &first_req);
        assert_eq!(page, vec![json!(0), json!(1)]);
        let token = next.expect("more items remain -> token");

        let second_req = json!({ "Limit": 2, "NextToken": token });
        let (page2, next2) = paginate(items.clone(), &second_req);
        assert_eq!(page2, vec![json!(2), json!(3)]);
        let token2 = next2.expect("still more");

        let third_req = json!({ "Limit": 2, "NextToken": token2 });
        let (page3, next3) = paginate(items, &third_req);
        assert_eq!(page3, vec![json!(4)]);
        assert!(next3.is_none(), "last page has no token");
    }

    #[test]
    fn paginate_no_limit_returns_all() {
        let items: Vec<Value> = (0..3).map(|i| json!(i)).collect();
        let (page, next) = paginate(items.clone(), &json!({}));
        assert_eq!(page, items);
        assert!(next.is_none());
    }

    #[test]
    fn extract_pack_rule_names_parses_yaml_template() {
        let yaml = r#"
Resources:
  MyRule:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: yaml-declared-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED
"#;
        let names = extract_pack_rule_names(Some(yaml));
        assert_eq!(names, vec!["yaml-declared-rule".to_string()]);
    }
}
