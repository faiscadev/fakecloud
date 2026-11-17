//! EC2 instance alarm actions (`arn:aws:automate:<region>:ec2:stop` and
//! friends), performed through the EC2 service itself so the instance goes
//! through exactly the lifecycle a `StopInstances` / `TerminateInstances` /
//! `RebootInstances` call gives it (state change, backing container,
//! persistence).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use axum::http::{HeaderMap, Method};
use bytes::Bytes;

use fakecloud_cloudwatch::alarm_actions::{ActionGrant, AlarmActionAuthorizer, GrantPrincipal};
use fakecloud_cloudwatch::{Ec2AlarmActionHook, Ec2InstanceAction};
use fakecloud_core::auth::{
    ConditionContext, IamAction, IamPolicyEvaluator, InternalCaller, Principal, PrincipalType,
    ResourcePolicyProvider,
};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_core::service::AwsRequest;

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
        let request = AwsRequest {
            service: "ec2".to_string(),
            action: operation.to_string(),
            region: region.to_string(),
            account_id: account_id.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        };
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
