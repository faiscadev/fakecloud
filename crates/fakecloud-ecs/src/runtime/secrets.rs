//! `EcsRuntime` `secrets` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcsRuntime {
    /// Resolve a `secrets[].valueFrom` reference to the actual secret
    /// payload. Supports SecretsManager secret ARNs and SSM parameter
    /// ARNs; returns `None` when the referenced state isn't wired or
    /// the lookup misses.
    pub(super) fn resolve_secret(&self, account_id: &str, value_from: &str) -> Option<String> {
        if value_from.contains(":secret:") {
            let state = self.secretsmanager_state.as_ref()?;
            let accounts = state.read();
            let sm = accounts.get(account_id)?;
            // ARN shape: arn:aws:secretsmanager:<region>:<acct>:secret:<name>-<6char>
            // ECS also allows trailing `:json-key:version-stage:version-id`
            // (https://docs.aws.amazon.com/secretsmanager/latest/userguide/retrieving-secrets_ecs.html).
            // Strip those trailing segments before parsing the secret name.
            let mut arn_tail = value_from.rsplit(":secret:").next()?;
            // Drop any of the up-to-3 trailing colon-separated optional
            // selectors (json-key, version-stage, version-id) that ECS
            // tacks on after the secret name.
            for _ in 0..3 {
                if let Some((head, _)) = arn_tail.rsplit_once(':') {
                    arn_tail = head;
                } else {
                    break;
                }
            }
            // Stored key is the secret name (no suffix). Strip the
            // AWS-generated 6-char suffix when comparing.
            let name = arn_tail
                .rsplit_once('-')
                .map(|(n, _)| n)
                .unwrap_or(arn_tail);
            let secret = sm.secrets.get(name).or_else(|| sm.secrets.get(arn_tail))?;
            let version_id = secret.current_version_id.as_ref()?;
            let v = secret.versions.get(version_id)?;
            return v.secret_string.clone();
        }
        if value_from.contains(":parameter") {
            let state = self.ssm_state.as_ref()?;
            let accounts = state.read();
            let ssm = accounts.get(account_id)?;
            // ARN shape: arn:aws:ssm:<region>:<acct>:parameter/<name>
            // Parameters are stored keyed by name (with leading slash)
            // or without, depending on how they were created. Try both.
            let after = value_from.rsplit(":parameter").next()?;
            let name_with_slash = after.trim_start_matches('/');
            return ssm
                .parameters
                .get(&format!("/{name_with_slash}"))
                .or_else(|| ssm.parameters.get(name_with_slash))
                .map(|p| p.value.clone());
        }
        None
    }
}
