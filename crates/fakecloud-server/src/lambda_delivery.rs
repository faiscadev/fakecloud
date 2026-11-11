//! Implements the `LambdaDelivery` trait for real Lambda execution via containers.

use std::sync::Arc;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::{DurableHub, SharedLambdaState};

/// Invokes Lambda functions using the container runtime.
pub struct LambdaDeliveryImpl {
    lambda_state: SharedLambdaState,
    runtime: Arc<ContainerRuntime>,
    durable: DurableHub,
}

impl LambdaDeliveryImpl {
    pub fn new(
        lambda_state: SharedLambdaState,
        runtime: Arc<ContainerRuntime>,
        durable: DurableHub,
    ) -> Self {
        Self {
            lambda_state,
            runtime,
            durable,
        }
    }
}

impl LambdaDelivery for LambdaDeliveryImpl {
    fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
        // The function lives in the account and region its ARN names; a bare
        // name (no ARN) means the default account in the server's region.
        let (account_id, region, function_name, resolved) = {
            let accounts = self.lambda_state.read();
            let (account, region, name) = fakecloud_lambda::function_location(
                function_arn,
                accounts.default_account_id(),
                accounts.region(),
            );
            let resolved =
                fakecloud_lambda::resolve_invocable(&accounts, function_arn, account, region);
            (
                account.to_string(),
                region.to_string(),
                name.to_string(),
                resolved,
            )
        };
        let (func, layer_zips) = match resolved {
            Some((func, zips)) => (Some(func), zips),
            None => (None, Vec::new()),
        };

        let runtime = self.runtime.clone();
        let payload = payload.to_string();
        let lambda_state = self.lambda_state.clone();
        let function_arn = function_arn.to_string();
        let durable = self.durable.clone();

        Box::pin(async move {
            let func = func.ok_or_else(|| format!("Function not found: {function_name}"))?;

            // Record invocation regardless of whether code exists
            {
                let mut accounts = lambda_state.write();
                let state = accounts.regional_mut(&account_id, &region);
                state.invocations.push(fakecloud_lambda::LambdaInvocation {
                    function_arn: function_arn.clone(),
                    payload: payload.clone(),
                    timestamp: chrono::Utc::now(),
                    source: "aws:lambda:delivery".to_string(),
                });
            }

            // A durable function runs as a durable execution; the delivery
            // waits for it to end, as an event source mapping does.
            if let Some(result) =
                fakecloud_lambda::invoke_durable_for_delivery(&durable, &function_arn, &payload)
                    .await
            {
                return result;
            }
            if func.code_zip.is_none() && func.package_type != "Image" {
                return Err(format!(
                    "Function {function_name} has no deployment package"
                ));
            }
            runtime
                .invoke(&func, payload.as_bytes(), &layer_zips)
                .await
                .map_err(|e| format!("Lambda invocation failed: {e}"))
        })
    }
}
