//! EC2 instance alarm actions (`arn:aws:automate:<region>:ec2:stop` and
//! friends), performed through the EC2 service itself so the instance goes
//! through exactly the lifecycle a `StopInstances` / `TerminateInstances` /
//! `RebootInstances` call gives it (state change, backing container,
//! persistence), and Systems Manager OpsItem alarm actions, opened through
//! the SSM service's `CreateOpsItem`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use axum::http::{HeaderMap, Method};
use bytes::Bytes;

use fakecloud_cloudwatch::alarm_actions::{ActionGrant, AlarmActionAuthorizer, GrantPrincipal};
use fakecloud_cloudwatch::{
    AlarmOpsItem, Ec2AlarmActionHook, Ec2InstanceAction, OpsItemAlarmActionHook,
};
use fakecloud_core::auth::{
    ConditionContext, IamAction, IamPolicyEvaluator, InternalCaller, Principal, PrincipalType,
    ResourcePolicyProvider,
};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_core::service::AwsRequest;
use fakecloud_ssm::SharedSsmState;

/// Evaluates alarm action targets' resource policies (a Lambda function's
/// policy, an SNS topic's policy) with the same providers and evaluator
/// request authorization uses.
pub struct AlarmActionPolicies {
    provider: Arc<dyn ResourcePolicyProvider>,
    evaluator: Arc<dyn IamPolicyEvaluator>,
}

impl AlarmActionPolicies {
    pub fn new(
        provider: Arc<dyn ResourcePolicyProvider>,
        evaluator: Arc<dyn IamPolicyEvaluator>,
    ) -> Self {
        Self {
            provider,
            evaluator,
        }
    }
}

impl AlarmActionAuthorizer for AlarmActionPolicies {
    fn allows(&self, grant: &ActionGrant<'_>) -> bool {
        let Some(policy) = self
            .provider
            .resource_policy(grant.service, grant.resource_arn)
        else {
            return false;
        };
        let (principal, service_keys) = match grant.principal {
            GrantPrincipal::Service(service) => {
                let caller = InternalCaller::Service {
                    service: service.to_string(),
                    source_arn: grant.source_arn.to_string(),
                    source_account: grant.source_account.to_string(),
                };
                (caller.principal(), caller.condition_keys())
            }
            GrantPrincipal::Account(account) => {
                let partition = fakecloud_aws::arn::partition_of(grant.resource_arn);
                let mut keys = std::collections::BTreeMap::new();
                keys.insert("aws:SourceOwner".to_string(), vec![account.to_string()]);
                keys.insert("aws:SourceAccount".to_string(), vec![account.to_string()]);
                keys.insert(
                    "aws:SourceArn".to_string(),
                    vec![grant.source_arn.to_string()],
                );
                (
                    Principal {
                        arn: format!("arn:{partition}:iam::{account}:root"),
                        user_id: account.to_string(),
                        account_id: account.to_string(),
                        principal_type: PrincipalType::Root,
                        source_identity: None,
                        tags: None,
                    },
                    keys,
                )
            }
        };
        let context = ConditionContext {
            service_keys,
            ..Default::default()
        };
        let action = IamAction {
            service: grant.service,
            action: grant.action,
            resource: grant.resource_arn.to_string(),
        };
        self.evaluator
            .evaluate_resource_policy_only(&principal, &action, &context, Some(&policy))
            .is_allow()
    }
}

/// Dispatches EC2 alarm actions to the registered EC2 service. The registry
/// is filled in once every service is registered.
pub struct Ec2AlarmActions {
    registry: Arc<OnceLock<Arc<ServiceRegistry>>>,
}

impl Ec2AlarmActions {
    pub fn new(registry: Arc<OnceLock<Arc<ServiceRegistry>>>) -> Self {
        Self { registry }
    }
}

impl Ec2AlarmActionHook for Ec2AlarmActions {
    fn perform(
        &self,
        account_id: &str,
        region: &str,
        instance_id: &str,
        action: Ec2InstanceAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let service = self.registry.get().and_then(|r| r.get("ec2").cloned());
        let operation = action.operation();
        let mut query_params = HashMap::new();
        query_params.insert("Action".to_string(), operation.to_string());
        query_params.insert("Version".to_string(), "2016-11-15".to_string());
        query_params.insert("InstanceId.1".to_string(), instance_id.to_string());
        let request = internal_request(
            "ec2",
            operation,
            account_id,
            region,
            query_params,
            Bytes::new(),
            HeaderMap::new(),
            true,
        );
        Box::pin(async move {
            let service = service.ok_or_else(|| "EC2 is not available".to_string())?;
            service
                .handle(request)
                .await
                .map(|_| ())
                .map_err(|e| format!("{}: {}", e.code(), e.message()))
        })
    }
}

/// A request a hook makes to another service on CloudWatch's behalf.
#[allow(clippy::too_many_arguments)]
fn internal_request(
    service: &str,
    action: &str,
    account_id: &str,
    region: &str,
    query_params: HashMap<String, String>,
    body: Bytes,
    headers: HeaderMap,
    is_query_protocol: bool,
) -> AwsRequest {
    AwsRequest {
        service: service.to_string(),
        action: action.to_string(),
        region: region.to_string(),
        account_id: account_id.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers,
        query_params,
        body,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol,
        access_key_id: None,
        principal: None,
    }
}

/// Opens OpsItems for Systems Manager alarm actions through the registered
/// SSM service, so they are stored and persisted like any `CreateOpsItem`.
/// An alarm opens no new OpsItem while one with its deduplication string is
/// unresolved.
pub struct SsmOpsItemAlarmActions {
    registry: Arc<OnceLock<Arc<ServiceRegistry>>>,
    ssm_state: SharedSsmState,
}

impl SsmOpsItemAlarmActions {
    pub fn new(registry: Arc<OnceLock<Arc<ServiceRegistry>>>, ssm_state: SharedSsmState) -> Self {
        Self {
            registry,
            ssm_state,
        }
    }

    /// The unresolved OpsItem carrying `dedup_string`, if any.
    fn open_duplicate(&self, account_id: &str, region: &str, dedup_string: &str) -> Option<String> {
        let accounts = self.ssm_state.read();
        let state = accounts.regional(account_id, region)?;
        state
            .ops_items
            .values()
            .find(|item| {
                item.status != "Resolved"
                    && item
                        .operational_data
                        .get("/aws/dedup")
                        .and_then(|v| v["Value"].as_str())
                        .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
                        .is_some_and(|v| v["dedupString"].as_str() == Some(dedup_string))
            })
            .map(|item| item.ops_item_id.clone())
    }
}

impl OpsItemAlarmActionHook for SsmOpsItemAlarmActions {
    fn create(
        &self,
        item: AlarmOpsItem,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send>> {
        if self
            .open_duplicate(&item.account_id, &item.region, &item.dedup_string)
            .is_some()
        {
            return Box::pin(async { Ok(None) });
        }
        let service = self.registry.get().and_then(|r| r.get("ssm").cloned());
        let mut body = serde_json::json!({
            "Title": item.title,
            "Source": item.source,
            "Description": item.description,
            "Severity": item.severity,
            "OperationalData": item.operational_data,
        });
        if let Some(c) = &item.category {
            body["Category"] = serde_json::json!(c);
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-target",
            axum::http::HeaderValue::from_static("AmazonSSM.CreateOpsItem"),
        );
        headers.insert(
            "content-type",
            axum::http::HeaderValue::from_static("application/x-amz-json-1.1"),
        );
        let request = internal_request(
            "ssm",
            "CreateOpsItem",
            &item.account_id,
            &item.region,
            HashMap::new(),
            Bytes::from(body.to_string()),
            headers,
            false,
        );
        Box::pin(async move {
            let service = service.ok_or_else(|| "Systems Manager is not available".to_string())?;
            let response = service
                .handle(request)
                .await
                .map_err(|e| format!("{}: {}", e.code(), e.message()))?;
            let id = serde_json::from_slice::<serde_json::Value>(response.body.expect_bytes())
                .ok()
                .and_then(|v| v["OpsItemId"].as_str().map(str::to_string));
            Ok(id)
        })
    }
}
