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

use fakecloud_cloudwatch::{Ec2AlarmActionHook, Ec2InstanceAction};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_core::service::AwsRequest;

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
