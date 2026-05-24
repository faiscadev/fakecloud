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
}
