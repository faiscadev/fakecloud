//! Rewrite `localhost`/`127.0.0.1` URLs in function environment values
//! to a backend-specific host alias, so functions running inside a
//! container/pod can still reach the fakecloud server on the host (or
//! the in-cluster fakecloud service).
//!
//! Docker rewrites to `host.docker.internal`; the Kubernetes backend
//! rewrites to the cluster-internal fakecloud service host derived from
//! `FAKECLOUD_K8S_SELF_URL`.

use std::collections::BTreeMap;

/// Rewrite `localhost` and `127.0.0.1` URLs in each value to use
/// `target_host` instead. Touches only the host portion of `http(s)://`
/// URLs; other occurrences of the word "localhost" in env values pass
/// through unchanged.
pub fn rewrite_localhost_envs(
    env: &BTreeMap<String, String>,
    target_host: &str,
) -> Vec<(String, String)> {
    env.iter()
        .map(|(k, v)| (k.clone(), rewrite_value(v, target_host)))
        .collect()
}

fn rewrite_value(value: &str, target_host: &str) -> String {
    value
        .replace("http://127.0.0.1:", &format!("http://{target_host}:"))
        .replace("https://127.0.0.1:", &format!("https://{target_host}:"))
        .replace("http://localhost:", &format!("http://{target_host}:"))
        .replace("https://localhost:", &format!("https://{target_host}:"))
}

/// The standard AWS environment a Lambda gets, plus the endpoint override
/// that keeps SDK calls inside fakecloud.
///
/// Real Lambda injects region and execution-role credentials, and function code
/// relies on them: an SDK client constructed with no region or credentials
/// fails outright. fakecloud injected none of them, so handler code that called
/// AWS did nothing useful — CDK's `BucketDeployment` reported success having
/// copied no files.
///
/// `AWS_ENDPOINT_URL` is the one deliberate deviation from AWS. On real Lambda
/// it is absent and the SDK's default endpoints are correct; here the container
/// must be pointed back at fakecloud on the host, or the handler reaches out to
/// real AWS instead. `host` is the backend's host alias, since `localhost`
/// inside the container is the container itself.
///
/// The function's own environment is applied after these, so a function that
/// sets any of them keeps its value.
pub fn default_aws_envs(host: &str, port: u16, region: &str) -> Vec<(String, String)> {
    [
        ("AWS_ENDPOINT_URL", format!("http://{host}:{port}")),
        ("AWS_REGION", region.to_string()),
        ("AWS_DEFAULT_REGION", region.to_string()),
        ("AWS_ACCESS_KEY_ID", "test".to_string()),
        ("AWS_SECRET_ACCESS_KEY", "test".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// Region from a function ARN (`arn:aws:lambda:<region>:<account>:function:<n>`),
/// falling back to `us-east-1` as the AWS SDKs do when none is configured.
pub fn region_from_function_arn(arn: &str) -> &str {
    arn.split(':')
        .nth(3)
        .filter(|r| !r.is_empty())
        .unwrap_or("us-east-1")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rewrites_http_localhost() {
        let out = rewrite_localhost_envs(
            &env(&[("FAKECLOUD_URL", "http://localhost:4566")]),
            "host.docker.internal",
        );
        assert_eq!(
            out,
            vec![(
                "FAKECLOUD_URL".to_string(),
                "http://host.docker.internal:4566".to_string()
            )]
        );
    }

    #[test]
    fn rewrites_http_127() {
        let out = rewrite_localhost_envs(
            &env(&[("X", "https://127.0.0.1:4566/path")]),
            "fakecloud.default.svc.cluster.local",
        );
        assert_eq!(
            out[0].1,
            "https://fakecloud.default.svc.cluster.local:4566/path"
        );
    }

    #[test]
    fn leaves_non_url_localhost_alone() {
        // The word "localhost" appearing outside an http(s):// prefix
        // is not rewritten — env values can legitimately contain it
        // (e.g. SMTP `EHLO localhost`).
        let out = rewrite_localhost_envs(
            &env(&[("MSG", "connect to localhost soon"), ("EMPTY", "")]),
            "host.docker.internal",
        );
        assert_eq!(out[1].1, "connect to localhost soon");
        assert_eq!(out[0].1, "");
    }

    #[test]
    fn rewrites_multiple_urls_in_one_value() {
        let out = rewrite_localhost_envs(
            &env(&[("URLS", "http://localhost:4566 http://127.0.0.1:4566")]),
            "h",
        );
        assert_eq!(out[0].1, "http://h:4566 http://h:4566");
    }

    #[test]
    fn default_envs_point_the_sdk_at_fakecloud_on_the_host() {
        let envs = default_aws_envs("host.docker.internal", 4566, "eu-west-2");
        let get = |k: &str| {
            envs.iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        };
        // Not `localhost`: inside the container that is the container itself.
        assert_eq!(
            get("AWS_ENDPOINT_URL").as_deref(),
            Some("http://host.docker.internal:4566")
        );
        assert_eq!(get("AWS_REGION").as_deref(), Some("eu-west-2"));
        assert_eq!(get("AWS_DEFAULT_REGION").as_deref(), Some("eu-west-2"));
        // Real Lambda supplies execution-role credentials; an SDK client with
        // none fails before it ever reaches the endpoint.
        assert!(get("AWS_ACCESS_KEY_ID").is_some());
        assert!(get("AWS_SECRET_ACCESS_KEY").is_some());
    }

    #[test]
    fn function_environment_overrides_the_defaults() {
        // Emitted defaults-first so a later `-e` wins, matching docker's
        // last-one-wins semantics.
        let defaults = default_aws_envs("host.docker.internal", 4566, "us-east-1");
        let user = rewrite_localhost_envs(
            &env(&[("AWS_ENDPOINT_URL", "http://localhost:9999")]),
            "host.docker.internal",
        );
        let merged: Vec<(String, String)> = defaults.into_iter().chain(user).collect();
        let last = merged
            .iter()
            .rfind(|(k, _)| k == "AWS_ENDPOINT_URL")
            .expect("endpoint present");
        assert_eq!(last.1, "http://host.docker.internal:9999");
    }

    #[test]
    fn region_is_read_from_the_function_arn() {
        assert_eq!(
            region_from_function_arn("arn:aws:lambda:eu-west-2:123456789012:function:f"),
            "eu-west-2"
        );
        assert_eq!(region_from_function_arn("not-an-arn"), "us-east-1");
        assert_eq!(
            region_from_function_arn("arn:aws:lambda::1:function:f"),
            "us-east-1"
        );
    }
}
