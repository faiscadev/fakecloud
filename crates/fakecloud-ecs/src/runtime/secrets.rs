//! Resolution of container `secrets[].valueFrom` references at task start.
//!
//! ECS reads each reference with the task execution role, the way the
//! container agent does:
//!
//! - A Secrets Manager ARN, optionally followed by
//!   `:json-key:version-stage:version-id`
//!   (`arn:aws:secretsmanager:<region>:<account>:secret:<name>-AbCdEf:password:AWSPREVIOUS:`),
//!   is read with `GetSecretValue` (full or partial ARN, in the ARN's
//!   account and region). A `json-key` extracts that field from the JSON
//!   secret, rendered as the agent renders it (see [`json_field_text`]).
//! - Anything else is an SSM parameter name or ARN, read with
//!   `GetParameters` and decryption (a name in the task's account, an ARN in
//!   its own account).
//!
//! A reference that does not resolve stops the task with ECS's
//! `ResourceInitializationError`.

use super::*;

/// A parsed `secrets[].valueFrom`.
#[derive(Debug, PartialEq, Eq)]
enum SecretReference<'a> {
    SecretsManager {
        /// The secret's full or partial ARN, without the selectors.
        secret_id: String,
        json_key: Option<&'a str>,
        version_stage: Option<&'a str>,
        version_id: Option<&'a str>,
    },
    /// An SSM parameter name or ARN.
    Parameter(&'a str),
}

/// Split a `valueFrom` into the store it names and its selectors. Like the
/// ECS agent, a Secrets Manager ARN's resource is either `secret:<id>` or
/// `secret:<id>:<json-key>:<version-stage>:<version-id>` (empty selectors
/// are unset); any other shape is rejected.
fn parse_value_from(value_from: &str) -> Result<SecretReference<'_>, String> {
    if fakecloud_aws::arn::arn_resource(value_from, "secretsmanager").is_none() {
        return Ok(SecretReference::Parameter(value_from));
    }
    // arn:<partition>:secretsmanager:<region>:<account>:<resource>
    let parts: Vec<&str> = value_from.split(':').collect();
    let resource = parts.get(5..).unwrap_or_default();
    let well_formed = matches!(resource.len(), 2 | 5) && resource[0] == "secret";
    if !well_formed {
        return Err(format!(
            "unable to retrieve secret from asm: trying to retrieve secret with value \
             {value_from} resulted in error: an invalid ARN format for the AWS Secrets Manager \
             secret was specified. Specify a valid ARN and try again."
        ));
    }
    let selector = |i: usize| resource.get(i).copied().filter(|s| !s.is_empty());
    Ok(SecretReference::SecretsManager {
        secret_id: parts[..7].join(":"),
        json_key: selector(2),
        version_stage: selector(3),
        version_id: selector(4),
    })
}

/// A JSON secret field as the ECS agent injects it: Go's `fmt.Sprintf("%v")`
/// of the value `encoding/json` decodes, so a string is injected as-is, a
/// number in Go's shortest `%g` form, `null` as `<nil>`, an object as
/// `map[k:v ...]` (sorted keys) and an array as `[v ...]`.
fn json_field_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "<nil>".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => go_float_text(n.as_f64().unwrap_or_default()),
        serde_json::Value::Array(items) => {
            let body: Vec<String> = items.iter().map(json_field_text).collect();
            format!("[{}]", body.join(" "))
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let body: Vec<String> = entries
                .into_iter()
                .map(|(k, v)| format!("{k}:{}", json_field_text(v)))
                .collect();
            format!("map[{}]", body.join(" "))
        }
    }
}

/// Go's `%v` of a `float64` (`strconv.FormatFloat(f, 'g', -1, 64)`): the
/// shortest round-tripping digits, in exponent form (`1e+08`, `1.5e-07`)
/// when the decimal exponent is below -4 or at least 6, fixed otherwise.
fn go_float_text(f: f64) -> String {
    // Rust's `{:e}` gives the shortest round-tripping digits: `1.5e-7`.
    let sci = format!("{f:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exp.abs())
    } else {
        // Fixed notation with exactly the shortest digits.
        let digits = mantissa.trim_start_matches('-').replace('.', "");
        let negative = mantissa.starts_with('-');
        let point = exp + 1; // digits before the decimal point
        let body = if point <= 0 {
            format!("0.{}{digits}", "0".repeat((-point) as usize))
        } else if point as usize >= digits.len() {
            format!("{digits}{}", "0".repeat(point as usize - digits.len()))
        } else {
            let (int, frac) = digits.split_at(point as usize);
            format!("{int}.{frac}")
        };
        if negative {
            format!("-{body}")
        } else {
            body
        }
    }
}

impl EcsRuntime {
    /// Resolve a `secrets[].valueFrom` reference to the value injected into
    /// the container, reading as `account_id` in `region` (the task's account
    /// and region): a name resolves in the task's region, a full ARN in the
    /// region it names (how ECS reaches a secret or parameter in another
    /// region). The error becomes the task's `stoppedReason`.
    pub(super) fn resolve_secret(
        &self,
        account_id: &str,
        region: &str,
        value_from: &str,
    ) -> Result<String, RuntimeError> {
        self.resolve_secret_value(account_id, region, value_from)
            .map_err(RuntimeError::SecretRetrieval)
    }

    fn resolve_secret_value(
        &self,
        account_id: &str,
        region: &str,
        value_from: &str,
    ) -> Result<String, String> {
        match parse_value_from(value_from)? {
            SecretReference::SecretsManager {
                secret_id,
                json_key,
                version_stage,
                version_id,
            } => self
                .read_asm_secret(
                    account_id,
                    region,
                    &secret_id,
                    json_key,
                    version_stage,
                    version_id,
                )
                .map_err(|e| format!("unable to retrieve secret from asm: {e}")),
            SecretReference::Parameter(name) => {
                self.read_ssm_parameter(account_id, region, name).map_err(|e| {
                    format!(
                        "unable to retrieve secrets from ssm: fetching secret data from SSM \
                         Parameter Store: {e}"
                    )
                })
            }
        }
    }

    /// `GetSecretValue` + json-key extraction, with the agent's errors.
    fn read_asm_secret(
        &self,
        account_id: &str,
        region: &str,
        secret_id: &str,
        json_key: Option<&str>,
        version_stage: Option<&str>,
        version_id: Option<&str>,
    ) -> Result<String, String> {
        let not_found = |detail: &str| {
            format!(
                "ResourceNotFoundException: The task can't retrieve the secret with ARN \
                 '{secret_id}' from AWS Secrets Manager. Check whether the secret exists in the \
                 specified Region: ResourceNotFoundException: {detail}"
            )
        };
        let state = self
            .secretsmanager_state
            .as_ref()
            .ok_or_else(|| not_found("Secrets Manager can't find the specified secret."))?;
        let value = fakecloud_secretsmanager::value::read_secret_value(
            state,
            self.kms_hook.as_deref(),
            account_id,
            region,
            secret_id,
            version_id,
            version_stage,
        )
        .map_err(|e| {
            if e.code() == "ResourceNotFoundException" {
                not_found(&e.message())
            } else {
                format!("secret {secret_id}: {}: {}", e.code(), e.message())
            }
        })?;
        // A binary secret has no SecretString: the agent injects it as the
        // empty string, and cannot read a json key out of it.
        let secret_string = value.secret_string.unwrap_or_default();
        let Some(key) = json_key else {
            return Ok(secret_string);
        };
        let fields: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&secret_string).map_err(|e| {
                format!("secret {secret_id}: secret value is not a JSON object: {e}")
            })?;
        fields.get(key).map(json_field_text).ok_or_else(|| {
            format!("retrieved secret from Secrets Manager did not contain json key {key}")
        })
    }

    /// `GetParameters` with decryption, with the agent's errors.
    fn read_ssm_parameter(
        &self,
        account_id: &str,
        region: &str,
        name: &str,
    ) -> Result<String, String> {
        let invalid = || format!("invalid parameters: {name}");
        let state = self.ssm_state.as_ref().ok_or_else(invalid)?;
        fakecloud_ssm::read_parameter_value(
            state,
            self.kms_hook.as_deref(),
            account_id,
            region,
            name,
        )
            .map(|p| p.value)
            .map_err(|e| {
                if e.code() == "ParameterNotFound" {
                    invalid()
                } else {
                    format!("{}: {}", e.code(), e.message())
                }
            })
    }
}

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
