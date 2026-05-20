//! `CognitoService` `compromised` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    /// CompromisedCredentialsRiskConfiguration enforcement.
    /// Returns `Err(NotAuthorizedException)` when the pool's risk
    /// config has `Actions.EventAction = BLOCK` and the password
    /// matches a known compromised hash.
    pub(crate) fn evaluate_compromised_credentials(
        &self,
        account_id: &str,
        pool_id: &str,
        client_id: &str,
        password: &str,
    ) -> Result<(), AwsServiceError> {
        use sha2::{Digest, Sha256};
        let accounts = self.state.read();
        let Some(state) = accounts.get(account_id) else {
            return Ok(());
        };
        // Risk config keyed by (pool, client) with a fallback to (pool, "").
        let pool_key = format!("{pool_id}:{client_id}");
        let pool_default = format!("{pool_id}:");
        let cfg = state
            .risk_configurations
            .get(&pool_key)
            .or_else(|| state.risk_configurations.get(&pool_default));
        let Some(cfg) = cfg else {
            return Ok(());
        };
        let action = cfg["CompromisedCredentialsRiskConfiguration"]["Actions"]["EventAction"]
            .as_str()
            .unwrap_or("");
        if !action.eq_ignore_ascii_case("BLOCK") {
            return Ok(());
        }
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        if state.compromised_password_hashes.contains(&hash) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Password has been found in a previous data breach. \
                 This password cannot be used. Please use a different password.",
            ));
        }
        Ok(())
    }
}
