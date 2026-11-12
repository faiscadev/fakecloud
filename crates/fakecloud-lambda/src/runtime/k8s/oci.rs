//! The entrypoint and command of a container image, read from its registry.
//!
//! The k8s backend runs every function through the extension launcher, which
//! then runs the image's own entrypoint and command; the kubelet pulls the
//! image itself, so fakecloud reads them from the image config over the OCI
//! distribution API (anonymous bearer tokens for public registries, basic
//! auth for fakecloud's own registry).

use std::time::Duration;

const ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
application/vnd.docker.distribution.manifest.list.v2+json, \
application/vnd.oci.image.manifest.v1+json, \
application/vnd.docker.distribution.manifest.v2+json";

/// A parsed image reference.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ImageRef {
    pub registry: String,
    pub repository: String,
    pub reference: String,
}

/// Split `image` into registry, repository and tag or digest, with Docker's
/// defaults (`docker.io`, `library/`, `latest`).
pub(crate) fn parse_image(image: &str) -> ImageRef {
    let (name, reference) = match image.split_once('@') {
        Some((n, d)) => (n.to_string(), d.to_string()),
        None => {
            let slash = image.rfind('/').map_or(0, |i| i + 1);
            match image[slash..].rfind(':') {
                Some(i) => (
                    image[..slash + i].to_string(),
                    image[slash + i + 1..].to_string(),
                ),
                None => (image.to_string(), "latest".to_string()),
            }
        }
    };
    let (registry, repository) = match name.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            (first.to_string(), rest.to_string())
        }
        _ => ("registry-1.docker.io".to_string(), name.clone()),
    };
    let repository = if registry == "registry-1.docker.io" && !repository.contains('/') {
        format!("library/{repository}")
    } else {
        repository
    };
    ImageRef {
        registry,
        repository,
        reference,
    }
}

/// The image's `(Entrypoint, Cmd)` for `arch` (`amd64`/`arm64`). `plain_http`
/// registries (fakecloud's own) are spoken to over HTTP with `basic_auth`.
pub(crate) async fn image_command(
    image: &str,
    arch: &str,
    plain_http: bool,
    basic_auth: Option<&str>,
) -> Result<(Vec<String>, Vec<String>), String> {
    let r = parse_image(image);
    let scheme = if plain_http { "http" } else { "https" };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let mut auth: Option<String> = basic_auth.map(|b| format!("Basic {b}"));
    let base = format!("{scheme}://{}/v2/{}", r.registry, r.repository);

    let get = |url: String, auth: Option<String>, accept: Option<&'static str>| {
        let client = client.clone();
        async move {
            let mut req = client.get(&url);
            if let Some(a) = auth {
                req = req.header("Authorization", a);
            }
            if let Some(a) = accept {
                req = req.header("Accept", a);
            }
            req.send().await.map_err(|e| e.to_string())
        }
    };

    let manifest_url = format!("{base}/manifests/{}", r.reference);
    let mut resp = get(manifest_url.clone(), auth.clone(), Some(ACCEPT)).await?;
    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        let challenge = resp
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let token = bearer_token(&client, &challenge, &r.repository).await?;
        auth = Some(format!("Bearer {token}"));
        resp = get(manifest_url, auth.clone(), Some(ACCEPT)).await?;
    }
    let mut manifest: serde_json::Value = check(resp).await?;
    if let Some(list) = manifest.get("manifests").and_then(|m| m.as_array()) {
        let digest = list
            .iter()
            .find(|m| {
                m.pointer("/platform/os").and_then(|v| v.as_str()) == Some("linux")
                    && m.pointer("/platform/architecture").and_then(|v| v.as_str()) == Some(arch)
            })
            .or_else(|| list.first())
            .and_then(|m| m.get("digest"))
            .and_then(|d| d.as_str())
            .ok_or("image index lists no manifest")?
            .to_string();
        manifest = check(
            get(
                format!("{base}/manifests/{digest}"),
                auth.clone(),
                Some(ACCEPT),
            )
            .await?,
        )
        .await?;
    }
    let config_digest = manifest
        .pointer("/config/digest")
        .and_then(|d| d.as_str())
        .ok_or("manifest names no config")?;
    let config: serde_json::Value =
        check(get(format!("{base}/blobs/{config_digest}"), auth, None).await?).await?;
    let list = |key: &str| -> Vec<String> {
        config
            .pointer(&format!("/config/{key}"))
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    Ok((list("Entrypoint"), list("Cmd")))
}

async fn check(resp: reqwest::Response) -> Result<serde_json::Value, String> {
    if !resp.status().is_success() {
        return Err(format!("registry answered {}", resp.status()));
    }
    resp.json().await.map_err(|e| e.to_string())
}

/// An anonymous pull token from a `Bearer realm=...,service=...` challenge.
async fn bearer_token(
    client: &reqwest::Client,
    challenge: &str,
    repository: &str,
) -> Result<String, String> {
    let params = challenge
        .strip_prefix("Bearer ")
        .ok_or("registry asked for an unsupported authentication")?;
    let field = |name: &str| {
        params.split(',').find_map(|p| {
            let (k, v) = p.trim().split_once('=')?;
            (k == name).then(|| v.trim_matches('"').to_string())
        })
    };
    let realm = field("realm").ok_or("no token realm")?;
    let mut url = reqwest::Url::parse(&realm).map_err(|e| e.to_string())?;
    {
        let mut q = url.query_pairs_mut();
        if let Some(service) = field("service") {
            q.append_pair("service", &service);
        }
        q.append_pair("scope", &format!("repository:{repository}:pull"));
    }
    let v: serde_json::Value =
        check(client.get(url).send().await.map_err(|e| e.to_string())?).await?;
    v.get("token")
        .or_else(|| v.get("access_token"))
        .and_then(|t| t.as_str())
        .map(str::to_string)
        .ok_or_else(|| "token endpoint returned no token".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_references_parse_with_docker_defaults() {
        assert_eq!(
            parse_image("public.ecr.aws/lambda/python:3.12"),
            ImageRef {
                registry: "public.ecr.aws".into(),
                repository: "lambda/python".into(),
                reference: "3.12".into()
            }
        );
        assert_eq!(
            parse_image("busybox"),
            ImageRef {
                registry: "registry-1.docker.io".into(),
                repository: "library/busybox".into(),
                reference: "latest".into()
            }
        );
        assert_eq!(
            parse_image("fakecloud.svc:4566/repo/app@sha256:abc"),
            ImageRef {
                registry: "fakecloud.svc:4566".into(),
                repository: "repo/app".into(),
                reference: "sha256:abc".into()
            }
        );
    }
}
