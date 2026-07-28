//! Implements the `LambdaDelivery` trait for real Lambda execution via containers.

use std::sync::Arc;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::SharedLambdaState;

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

        // Extract account ID from ARN, falling back to the default account
        let account_id = {
            let parts: Vec<&str> = function_arn.split(':').collect();
            let parsed = if parts.len() >= 5 { parts[4] } else { "" };
            if parsed.is_empty() {
                self.lambda_state.read().default_account_id().to_string()
            } else {
                parsed.to_string()
            }
        };

        let (func, layer_zips) = {
            let accounts = self.lambda_state.read();
            match accounts
                .get(&account_id)
                .and_then(|state| state.functions.get(&function_name).cloned())
            {
                Some(func) => {
                    let mut layer_zips: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
                    for attached in &func.layers {
                        if let Some(bytes) =
                            fakecloud_lambda::extras::parse_layer_version_arn(&attached.arn)
                                .and_then(|(acct, name, ver)| {
                                    accounts
                                        .get(&acct)
                                        .and_then(|s| s.layers.get(&name))
                                        .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                                        .and_then(|v| v.code_zip.clone())
                                })
                        {
                            layer_zips.push(bytes);
                        }
                    }
                    (Some(func), layer_zips)
                }
                None => (None, Vec::new()),
            }
        };

        let runtime = self.runtime.clone();
        let payload = payload.to_string();
        let lambda_state = self.lambda_state.clone();
        let function_arn = function_arn.to_string();

        Box::pin(async move {
            let func = func.ok_or_else(|| format!("Function not found: {function_name}"))?;

            // Record invocation regardless of whether code exists
            {
                let mut accounts = lambda_state.write();
                let state = accounts.get_or_create(&account_id);
                state.invocations.push(fakecloud_lambda::LambdaInvocation {
                    function_arn: function_arn.clone(),
                    payload: payload.clone(),
                    timestamp: chrono::Utc::now(),
                    source: "aws:lambda:delivery".to_string(),
                });
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
