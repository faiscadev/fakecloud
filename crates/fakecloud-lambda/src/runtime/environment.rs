//! The environment a function's runtime process starts with.
//!
//! Real Lambda starts every execution environment with a set of reserved
//! variables (region, execution-role credentials, function metadata) that
//! function code and the AWS SDKs rely on: an SDK client constructed with no
//! region or credentials fails before it sends anything. Those keys cannot be
//! set by the function itself (`CreateFunction` rejects them, see
//! [`reserved_keys_in`]), so they always carry the platform's values.
//!
//! On top of that, fakecloud adds `AWS_ENDPOINT_URL` pointing back at the
//! server, the one deliberate deviation from AWS: on real Lambda the SDK's
//! default endpoints are correct, here the handler would otherwise call real
//! AWS. It is not reserved, so a function that sets its own keeps it.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use fakecloud_core::auth::SessionCredentials;

use super::env_rewrite::rewrite_localhost_envs;
use crate::state::LambdaFunction;

/// Environment variable keys Lambda reserves for itself. A function
/// configuration that sets any of them is rejected with
/// `InvalidParameterValueException`.
pub const RESERVED_ENV_KEYS: &[&str] = &[
    "_HANDLER",
    "_X_AMZN_TRACE_ID",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "AWS_EXECUTION_ENV",
    "AWS_LAMBDA_FUNCTION_NAME",
    "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
    "AWS_LAMBDA_FUNCTION_VERSION",
    "AWS_LAMBDA_INITIALIZATION_TYPE",
    "AWS_LAMBDA_LOG_GROUP_NAME",
    "AWS_LAMBDA_LOG_STREAM_NAME",
    "AWS_ACCESS_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_LAMBDA_RUNTIME_API",
    "LAMBDA_TASK_ROOT",
    "LAMBDA_RUNTIME_DIR",
];

/// The reserved keys present in a function's environment, in key order.
pub fn reserved_keys_in(environment: &BTreeMap<String, String>) -> Vec<&str> {
    environment
        .keys()
        .map(String::as_str)
        .filter(|k| RESERVED_ENV_KEYS.contains(k))
        .collect()
}

/// The `InvalidParameterValueException` message Lambda returns for a
/// configuration that sets reserved keys, or `None` when it sets none.
pub fn reserved_keys_message(environment: &BTreeMap<String, String>) -> Option<String> {
    let reserved = reserved_keys_in(environment);
    if reserved.is_empty() {
        return None;
    }
    Some(format!(
        "Lambda was unable to configure your environment variables because the environment \
         variables you have provided contains reserved keys that are currently not supported \
         for modification. Reserved keys used in this request: {}",
        reserved.join(", ")
    ))
}

/// Region from a function ARN (`arn:<partition>:lambda:<region>:<account>:function:<name>`).
/// Function ARNs are always minted with a region; `us-east-1` only covers a
/// malformed value.
pub fn region_from_function_arn(arn: &str) -> &str {
    arn.split(':')
        .nth(3)
        .filter(|r| !r.is_empty())
        .unwrap_or("us-east-1")
}

/// Log group the function writes to: `LoggingConfig.LogGroup` when set,
/// otherwise Lambda's default `/aws/lambda/<function-name>`.
fn log_group_name(func: &LambdaFunction) -> String {
    func.logging_config
        .as_ref()
        .and_then(|c| c["LogGroup"].as_str())
        .filter(|g| !g.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("/aws/lambda/{}", func.function_name))
}

/// Lambda's per-execution-environment log stream name:
/// `YYYY/MM/DD/[<version>]<32 hex>`.
fn log_stream_name(version: &str, started_at: DateTime<Utc>, instance_id: &str) -> String {
    format!("{}/[{version}]{instance_id}", started_at.format("%Y/%m/%d"))
}

/// `AWS_EXECUTION_ENV` for a managed runtime (`AWS_Lambda_python3.12`). Custom
/// runtimes (`provided*`) and container images set none, as on AWS.
fn execution_env(func: &LambdaFunction) -> Option<String> {
    if func.package_type == "Image"
        || func.runtime.is_empty()
        || func.runtime.starts_with("provided")
    {
        return None;
    }
    Some(format!("AWS_Lambda_{}", func.runtime))
}

/// The environment keys that carry the execution role's credentials. Backends
/// keep their values off command lines and out of Pod specs.
pub const CREDENTIAL_ENV_KEYS: [&str; 4] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];

/// The credential variables for `creds`, keyed by [`CREDENTIAL_ENV_KEYS`].
pub fn credential_envs(creds: &SessionCredentials) -> [(&'static str, String); 4] {
    let [akid, legacy_akid, secret, token] = CREDENTIAL_ENV_KEYS;
    [
        (akid, creds.access_key_id.clone()),
        (legacy_akid, creds.access_key_id.clone()),
        (secret, creds.secret_access_key.clone()),
        (token, creds.session_token.clone()),
    ]
}

/// Assemble the full environment for one execution environment of `func`.
///
/// Returns each key exactly once, resolved by precedence: fakecloud's
/// endpoint override, then the function's own variables (with
/// `localhost`/`127.0.0.1` URLs rewritten to `rewrite_host`, since inside the
/// container those name the container itself), then the reserved variables,
/// which always win. `endpoint_url` is how this backend's containers reach the
/// fakecloud server, and `runtime_api` (`host:port`) where the instance's
/// runtime reaches the Runtime API fakecloud serves for it
/// (`AWS_LAMBDA_RUNTIME_API`; with it set the base images' entrypoint runs the
/// runtime directly instead of the bundled Runtime Interface Emulator).
/// `credentials` are the execution role's session credentials; `None` leaves
/// them unset.
pub fn function_environment(
    func: &LambdaFunction,
    endpoint_url: &str,
    rewrite_host: &str,
    runtime_api: &str,
    credentials: Option<&SessionCredentials>,
) -> Vec<(String, String)> {
    let started_at = Utc::now();
    let instance_id = uuid::Uuid::new_v4().simple().to_string();
    let region = region_from_function_arn(&func.function_arn);

    let mut env: BTreeMap<String, String> = BTreeMap::new();
    env.insert("AWS_ENDPOINT_URL".into(), endpoint_url.to_string());
    for (key, value) in rewrite_localhost_envs(&func.environment, rewrite_host) {
        if !RESERVED_ENV_KEYS.contains(&key.as_str()) {
            env.insert(key, value);
        }
    }

    let mut reserved: Vec<(&str, String)> = vec![
        ("AWS_REGION", region.to_string()),
        ("AWS_DEFAULT_REGION", region.to_string()),
        ("AWS_LAMBDA_FUNCTION_NAME", func.function_name.clone()),
        ("AWS_LAMBDA_FUNCTION_VERSION", func.version.clone()),
        (
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
            func.memory_size.to_string(),
        ),
        ("AWS_LAMBDA_LOG_GROUP_NAME", log_group_name(func)),
        (
            "AWS_LAMBDA_LOG_STREAM_NAME",
            log_stream_name(&func.version, started_at, &instance_id),
        ),
        ("AWS_LAMBDA_INITIALIZATION_TYPE", "on-demand".to_string()),
        ("AWS_LAMBDA_RUNTIME_API", runtime_api.to_string()),
    ];
    if let Some(exec_env) = execution_env(func) {
        reserved.push(("AWS_EXECUTION_ENV", exec_env));
    }
    if let Some(creds) = credentials {
        reserved.extend(credential_envs(creds));
    }
    for (key, value) in reserved {
        env.insert(key.to_string(), value);
    }
    env.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn func() -> LambdaFunction {
        serde_json::from_value(serde_json::json!({
            "function_name": "probe",
            "function_arn": "arn:aws:lambda:eu-west-2:123456789012:function:probe",
            "runtime": "python3.12",
            "role": "arn:aws:iam::123456789012:role/r",
            "handler": "index.handler",
            "description": "",
            "timeout": 7,
            "memory_size": 256,
            "code_sha256": "sha",
            "code_size": 1,
            "version": "$LATEST",
            "last_modified": "2020-01-01T00:00:00Z",
            "tags": {},
            "environment": {},
            "architectures": ["x86_64"],
            "package_type": "Zip",
            "code_zip": null,
            "policy": null
        }))
        .expect("build test LambdaFunction")
    }

    fn creds() -> SessionCredentials {
        SessionCredentials {
            access_key_id: "FSIAEXAMPLE".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expiration: Utc::now(),
            account_id: "123456789012".into(),
        }
    }

    fn get<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
        env.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    #[test]
    fn carries_the_reserved_lambda_environment() {
        let env = function_environment(
            &func(),
            "http://host.docker.internal:4566",
            "host.docker.internal",
            "fakecloud-runtime-abc:4566",
            Some(&creds()),
        );
        assert_eq!(
            get(&env, "AWS_ENDPOINT_URL"),
            Some("http://host.docker.internal:4566")
        );
        assert_eq!(get(&env, "AWS_REGION"), Some("eu-west-2"));
        assert_eq!(get(&env, "AWS_DEFAULT_REGION"), Some("eu-west-2"));
        assert_eq!(get(&env, "AWS_ACCESS_KEY_ID"), Some("FSIAEXAMPLE"));
        assert_eq!(get(&env, "AWS_ACCESS_KEY"), Some("FSIAEXAMPLE"));
        assert_eq!(get(&env, "AWS_SECRET_ACCESS_KEY"), Some("secret"));
        assert_eq!(get(&env, "AWS_SESSION_TOKEN"), Some("token"));
        assert_eq!(get(&env, "AWS_LAMBDA_FUNCTION_NAME"), Some("probe"));
        assert_eq!(get(&env, "AWS_LAMBDA_FUNCTION_VERSION"), Some("$LATEST"));
        assert_eq!(get(&env, "AWS_LAMBDA_FUNCTION_MEMORY_SIZE"), Some("256"));
        assert_eq!(
            get(&env, "AWS_LAMBDA_RUNTIME_API"),
            Some("fakecloud-runtime-abc:4566")
        );
        assert_eq!(get(&env, "AWS_LAMBDA_FUNCTION_TIMEOUT"), None);
        assert_eq!(
            get(&env, "AWS_LAMBDA_LOG_GROUP_NAME"),
            Some("/aws/lambda/probe")
        );
        assert_eq!(
            get(&env, "AWS_LAMBDA_INITIALIZATION_TYPE"),
            Some("on-demand")
        );
        assert_eq!(
            get(&env, "AWS_EXECUTION_ENV"),
            Some("AWS_Lambda_python3.12")
        );
        let stream = get(&env, "AWS_LAMBDA_LOG_STREAM_NAME").unwrap();
        let (date, rest) = stream.split_at(10);
        assert_eq!(date.matches('/').count(), 2, "{stream}");
        let hex = rest.strip_prefix("/[$LATEST]").expect(stream);
        assert!(
            hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()),
            "{stream}"
        );
    }

    #[test]
    fn every_key_appears_once() {
        let mut f = func();
        f.environment
            .insert("AWS_ENDPOINT_URL".into(), "http://localhost:9999".into());
        let env = function_environment(&f, "http://h:4566", "h", "rt:4566", Some(&creds()));
        let mut keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        let total = keys.len();
        keys.dedup();
        assert_eq!(keys.len(), total);
    }

    #[test]
    fn function_can_override_the_endpoint_but_not_reserved_keys() {
        let mut f = func();
        f.environment
            .insert("AWS_ENDPOINT_URL".into(), "http://localhost:9999".into());
        f.environment
            .insert("AWS_REGION".into(), "ap-south-1".into());
        f.environment.insert("APP_MODE".into(), "test".into());
        let env = function_environment(&f, "http://h:4566", "h", "rt:4566", Some(&creds()));
        // Its own endpoint wins, rewritten off `localhost` like any URL.
        assert_eq!(get(&env, "AWS_ENDPOINT_URL"), Some("http://h:9999"));
        assert_eq!(get(&env, "AWS_REGION"), Some("eu-west-2"));
        assert_eq!(get(&env, "AWS_LAMBDA_RUNTIME_API"), Some("rt:4566"));
        assert_eq!(get(&env, "APP_MODE"), Some("test"));
    }

    #[test]
    fn no_credentials_without_an_issuer() {
        let env = function_environment(&func(), "http://h:4566", "h", "rt:4566", None);
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            assert_eq!(get(&env, key), None, "{key}");
        }
    }

    #[test]
    fn version_log_group_and_execution_env_follow_the_configuration() {
        let mut f = func();
        f.version = "3".into();
        f.logging_config = Some(serde_json::json!({"LogGroup": "/custom/group"}));
        let env = function_environment(&f, "http://h:4566", "h", "rt:4566", None);
        assert_eq!(get(&env, "AWS_LAMBDA_FUNCTION_VERSION"), Some("3"));
        assert_eq!(
            get(&env, "AWS_LAMBDA_LOG_GROUP_NAME"),
            Some("/custom/group")
        );
        assert!(get(&env, "AWS_LAMBDA_LOG_STREAM_NAME")
            .unwrap()
            .contains("/[3]"));

        f.runtime = "provided.al2023".into();
        let env = function_environment(&f, "http://h:4566", "h", "rt:4566", None);
        assert_eq!(get(&env, "AWS_EXECUTION_ENV"), None);

        f.runtime = String::new();
        f.package_type = "Image".into();
        let env = function_environment(&f, "http://h:4566", "h", "rt:4566", None);
        assert_eq!(get(&env, "AWS_EXECUTION_ENV"), None);
    }

    #[test]
    fn reserved_keys_are_detected() {
        let mut env = BTreeMap::new();
        env.insert("APP".to_string(), "x".to_string());
        env.insert("AWS_REGION".to_string(), "x".to_string());
        env.insert("AWS_SESSION_TOKEN".to_string(), "x".to_string());
        env.insert("AWS_ENDPOINT_URL".to_string(), "x".to_string());
        assert_eq!(
            reserved_keys_in(&env),
            vec!["AWS_REGION", "AWS_SESSION_TOKEN"]
        );
    }

    #[test]
    fn region_is_read_from_the_function_arn() {
        assert_eq!(
            region_from_function_arn("arn:aws:lambda:eu-west-2:123456789012:function:f"),
            "eu-west-2"
        );
        assert_eq!(region_from_function_arn("not-an-arn"), "us-east-1");
    }
}
