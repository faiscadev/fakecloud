//! AWS ECR URI recognition + translation to the local fakecloud OCI v2 endpoint.
//!
//! Shared between ECS and Lambda container runtimes: when a task definition
//! or Lambda function references `<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`,
//! fakecloud can't pull from AWS — there is no AWS account. Instead we
//! translate the URI to `127.0.0.1:<server-port>/<repo>:<tag>` and pull
//! from fakecloud's own OCI v2 registry (which is just another route on
//! the same HTTP server). Docker treats `127.0.0.1:<port>` as an insecure
//! registry automatically on both Linux and Docker Desktop, so no daemon
//! config is required.

/// Detect whether `image` is an AWS private-ECR URI. Match shape:
/// `<any>.dkr.ecr.<any>.amazonaws.com/<path>[:<tag>|@sha256:<digest>]`.
/// The registry host is anchored so arbitrary-path substrings like
/// `docker.io/user/.dkr.ecr.whatever.amazonaws.com/bar` don't get
/// misclassified.
pub fn is_aws_ecr_uri(image: &str) -> bool {
    let Some((registry, _path)) = image.split_once('/') else {
        return false;
    };
    registry.contains(".dkr.ecr.") && registry.ends_with(".amazonaws.com")
}

/// If `image` is an AWS private-ECR URI, return the local-registry URI
/// that fakecloud's OCI v2 endpoint serves the same content at. Returns
/// `None` for any other reference (public ECR, Docker Hub, etc.) — those
/// go straight to the upstream daemon.
///
/// Docker's localhost-registry behaviour means the daemon on both Linux
/// and macOS Docker Desktop accepts `127.0.0.1:<port>` over plain HTTP.
pub fn translate_to_local(image: &str, server_port: u16) -> Option<String> {
    translate_to_local_at(image, "127.0.0.1", server_port)
}

/// Like [`translate_to_local`] but lets the caller pick the host the
/// rewritten URI should point at. Lambda's Kubernetes backend needs
/// this — inside a cluster, the in-cluster fakecloud Service hostname
/// (not `127.0.0.1`) is the only address Lambda Pods can reach.
pub fn translate_to_local_at(image: &str, host: &str, server_port: u16) -> Option<String> {
    if !is_aws_ecr_uri(image) {
        return None;
    }
    let (_registry, path) = image.split_once('/')?;
    if path.is_empty() {
        return None;
    }
    Some(format!("{host}:{server_port}/{path}"))
}

/// Is `image` a digest-pinned reference (`repo@sha256:...`)? Docker's
/// `tag` subcommand cannot target a digest ref, so the runtime must
/// skip retagging and run the container under the local URI directly.
pub fn is_digest_ref(image: &str) -> bool {
    image.contains("@sha256:")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_private_ecr_uri() {
        assert!(is_aws_ecr_uri(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag"
        ));
        assert!(is_aws_ecr_uri(
            "123456789012.dkr.ecr.eu-west-2.amazonaws.com/team/svc:v1"
        ));
        assert!(!is_aws_ecr_uri("public.ecr.aws/lambda/python:3.12"));
        assert!(!is_aws_ecr_uri("docker.io/library/alpine:3.20"));
        assert!(!is_aws_ecr_uri("alpine:3.20"));
        // Host anchoring — path containing the tokens must NOT match.
        assert!(!is_aws_ecr_uri(
            "docker.io/team/evil.dkr.ecr.us-east-1.amazonaws.com/repo:tag"
        ));
    }

    #[test]
    fn detects_digest_ref() {
        assert!(is_digest_ref(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo@sha256:abc"
        ));
        assert!(!is_digest_ref(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag"
        ));
    }

    #[test]
    fn translates_private_ecr_uri() {
        assert_eq!(
            translate_to_local(
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
                4566
            ),
            Some("127.0.0.1:4566/repo:tag".to_string())
        );
        assert_eq!(
            translate_to_local(
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/svc:v1",
                8080
            ),
            Some("127.0.0.1:8080/team/svc:v1".to_string())
        );
        assert_eq!(
            translate_to_local(
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo@sha256:abc",
                4566
            ),
            Some("127.0.0.1:4566/repo@sha256:abc".to_string())
        );
        assert_eq!(translate_to_local("alpine:3.20", 4566), None);
        assert_eq!(
            translate_to_local("public.ecr.aws/lambda/python:3.12", 4566),
            None
        );
    }

    #[test]
    fn rejects_empty_path() {
        assert_eq!(
            translate_to_local("123456789012.dkr.ecr.us-east-1.amazonaws.com/", 4566),
            None
        );
    }

    #[test]
    fn translate_to_local_at_overrides_host() {
        assert_eq!(
            translate_to_local_at(
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
                "fakecloud.fakecloud.svc.cluster.local",
                4566
            ),
            Some("fakecloud.fakecloud.svc.cluster.local:4566/repo:tag".to_string())
        );
    }

    #[test]
    fn translate_to_local_at_returns_none_for_non_ecr() {
        assert_eq!(translate_to_local_at("alpine:3.20", "anything", 4566), None);
    }
}
