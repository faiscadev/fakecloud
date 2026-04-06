//! Implements the `LambdaDelivery` trait for real Lambda execution via containers.

use std::sync::Arc;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::state::SharedLambdaState;

/// Invokes Lambda functions using the container runtime.
pub struct LambdaDeliveryImpl {
    lambda_state: SharedLambdaState,
    runtime: Arc<ContainerRuntime>,
}

impl LambdaDeliveryImpl {
    pub fn new(lambda_state: SharedLambdaState, runtime: Arc<ContainerRuntime>) -> Self {
        Self {
            lambda_state,
            runtime,
        }
    }
}

impl LambdaDelivery for LambdaDeliveryImpl {
    fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
        // Extract function name from ARN: arn:aws:lambda:region:account:function:name[:qualifier]
        let function_name = {
            let parts: Vec<&str> = function_arn.split(':').collect();
            if parts.len() >= 7 && parts[5] == "function" {
                parts[6].to_string()
            } else {
                // Fallback: treat the whole thing as a function name
                function_arn.to_string()
            }
        };

        let func = {
            let state = self.lambda_state.read();
            state.functions.get(&function_name).cloned()
        };

        let runtime = self.runtime.clone();
        let payload = payload.to_string();
        let lambda_state = self.lambda_state.clone();
        let function_arn = function_arn.to_string();

        Box::pin(async move {
            let func = func.ok_or_else(|| format!("Function not found: {function_name}"))?;

            // Record invocation regardless of whether code exists
            {
                let mut state = lambda_state.write();
                state
                    .invocations
                    .push(fakecloud_lambda::state::LambdaInvocation {
                        function_arn: function_arn.clone(),
                        payload: payload.clone(),
                        timestamp: chrono::Utc::now(),
                        source: "aws:lambda:delivery".to_string(),
                    });
            }

            if func.code_zip.is_none() {
                return Err(format!(
                    "Function {function_name} has no deployment package"
                ));
            }
            runtime
                .invoke(&func, payload.as_bytes())
                .await
                .map_err(|e| format!("Lambda invocation failed: {e}"))
        })
    }
}
