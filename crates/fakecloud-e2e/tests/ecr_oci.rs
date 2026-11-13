//! ECR Batch 3: OCI v2 Distribution HTTP protocol.
//!
//! Exercises `/v2/` via raw reqwest to make sure a real docker client's
//! request shape works (Basic auth, chunked uploads, manifest push).

mod helpers;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use helpers::TestServer;
use sha2::{Digest, Sha256};

fn auth_header() -> String {
    format!("Basic {}", B64.encode(b"AWS:test-token"))
}

fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

#[tokio::test]
async fn api_version_probe() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/v2/", server.endpoint()))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{:?}", resp.status());
    assert_eq!(
        resp.headers()
            .get("docker-distribution-api-version")
            .and_then(|v| v.to_str().ok()),
        Some("registry/2.0"),
    );
}

#[tokio::test]
async fn api_version_probe_without_auth_is_challenged() {
    // Docker's two-phase login does an unauthenticated `GET /v2/`
    // first, expects 401 + WWW-Authenticate, then retries with Basic.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/v2/", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key("www-authenticate"));
}

/// The registry challenge is the registry's own, under `--iam strict` and on
/// an ECR host too: ECR's API is awsJson, so an unsigned REST request to ECR
/// is a registry call, never refused for missing AWS credentials.
#[tokio::test]
async fn api_version_probe_is_challenged_under_strict_on_ecr_hosts() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    for host in [
        None,
        Some("ecr.us-east-1.amazonaws.com"),
        Some("ecr.us-east-1.localhost.localstack.cloud"),
        Some("123456789012.dkr.ecr.us-east-1.amazonaws.com"),
    ] {
        let mut req = reqwest::Client::new().get(format!("{}/v2/", server.endpoint()));
        if let Some(host) = host {
            req = req.header("host", host);
        }
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED, "{host:?}");
        assert!(resp.headers().contains_key("www-authenticate"), "{host:?}");
    }
}

#[tokio::test]
async fn api_version_probe_with_bad_credentials_is_rejected() {
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/v2/", server.endpoint()))
        .header("Authorization", "Basic bm9wZTpub3Q=") // user="nope"
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key("www-authenticate"));
}

#[tokio::test]
async fn blob_upload_cancel_rejects_cross_repo_upload_id() {
    let server = TestServer::start().await;
    let aws = server.ecr_client().await;
    aws.create_repository()
        .repository_name("owner-repo")
        .send()
        .await
        .unwrap();
    aws.create_repository()
        .repository_name("thief-repo")
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let start = http
        .post(format!(
            "{}/v2/owner-repo/blobs/uploads/",
            server.endpoint()
        ))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    let uuid = start
        .headers()
        .get("docker-upload-uuid")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    // Different repo must not be able to cancel owner-repo's upload.
    let resp = http
        .delete(format!(
            "{}/v2/thief-repo/blobs/uploads/{uuid}",
            server.endpoint()
        ))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    // Owner still sees its upload (HEAD-equivalent check: PATCH succeeds).
    let patched = http
        .patch(format!(
            "{}/v2/owner-repo/blobs/uploads/{uuid}",
            server.endpoint()
        ))
        .header("Authorization", auth_header())
        .body(b"still-ours".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), reqwest::StatusCode::ACCEPTED);
}

#[tokio::test]
async fn docker_push_pull_round_trip() {
    let server = TestServer::start().await;
    let aws = server.ecr_client().await;
    aws.create_repository()
        .repository_name("oci-repo")
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("{}/v2/oci-repo", server.endpoint());

    // Start a blob upload.
    let start = http
        .post(format!("{base}/blobs/uploads/"))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(start.status(), reqwest::StatusCode::ACCEPTED);
    let upload_location = start
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("location")
        .to_string();
    let upload_uuid = start
        .headers()
        .get("docker-upload-uuid")
        .and_then(|v| v.to_str().ok())
        .expect("docker-upload-uuid")
        .to_string();
    assert!(upload_location.contains(&upload_uuid));

    let layer_bytes = b"fakecloud layer blob".to_vec();
    let layer_digest = sha256_digest(&layer_bytes);

    // Upload the blob in chunks using PATCH.
    let patch_url = format!("{}{}", server.endpoint(), upload_location);
    let patched = http
        .patch(&patch_url)
        .header("Authorization", auth_header())
        .body(layer_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), reqwest::StatusCode::ACCEPTED);

    // Finalize via PUT ?digest=...
    let put_url = format!(
        "{}{}?digest={}",
        server.endpoint(),
        upload_location,
        layer_digest
    );
    let finished = http
        .put(&put_url)
        .header("Authorization", auth_header())
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert_eq!(finished.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        finished
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok()),
        Some(layer_digest.as_str()),
    );

    // HEAD + GET blob.
    let head = http
        .head(format!("{base}/blobs/{layer_digest}"))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert!(head.status().is_success());
    let get = http
        .get(format!("{base}/blobs/{layer_digest}"))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert!(get.status().is_success());
    assert_eq!(get.bytes().await.unwrap().to_vec(), layer_bytes);

    // Push manifest referencing the layer.
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": 0,
            "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "size": layer_bytes.len(),
                "digest": layer_digest,
            }
        ]
    });
    let manifest_body = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = sha256_digest(&manifest_body);

    let push = http
        .put(format!("{base}/manifests/v1"))
        .header("Authorization", auth_header())
        .header(
            "Content-Type",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .body(manifest_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(push.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        push.headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok()),
        Some(manifest_digest.as_str()),
    );

    // Pull manifest by tag.
    let pulled = http
        .get(format!("{base}/manifests/v1"))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert!(pulled.status().is_success());
    assert_eq!(pulled.bytes().await.unwrap().to_vec(), manifest_body);

    // List tags.
    let tags = http
        .get(format!("{base}/tags/list"))
        .header("Authorization", auth_header())
        .send()
        .await
        .unwrap();
    assert!(tags.status().is_success());
    let body: serde_json::Value = tags.json().await.unwrap();
    assert_eq!(body["name"], "oci-repo");
    assert_eq!(body["tags"], serde_json::json!(["v1"]));

    // Same image is now visible through the AWS SDK too.
    let desc = aws
        .describe_images()
        .repository_name("oci-repo")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.image_details().len(), 1);
    assert_eq!(
        desc.image_details()[0].image_digest(),
        Some(manifest_digest.as_str()),
    );
}

#[tokio::test]
async fn get_authorization_token_returns_basic_credentials() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;
    let resp = client.get_authorization_token().send().await.unwrap();
    let data = resp.authorization_data();
    assert_eq!(data.len(), 1);
    let token = data[0].authorization_token().unwrap();
    let decoded = String::from_utf8(B64.decode(token).unwrap()).unwrap();
    assert!(decoded.starts_with("AWS:"), "{decoded}");
}

#[tokio::test]
async fn put_manifest_with_mismatched_digest_reference_still_succeeds_on_tag() {
    // Reference can be either a tag (stored as tag) or a sha256:digest
    // (stored by digest). Pushing with a random tag name is a normal flow.
    let server = TestServer::start().await;
    let aws = server.ecr_client().await;
    aws.create_repository()
        .repository_name("tag-push-repo")
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let manifest = serde_json::json!({"schemaVersion": 2, "layers": []});
    let body = serde_json::to_vec(&manifest).unwrap();
    let resp = http
        .put(format!(
            "{}/v2/tag-push-repo/manifests/release-2024",
            server.endpoint()
        ))
        .header("Authorization", auth_header())
        .header(
            "Content-Type",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let listed = aws
        .list_images()
        .repository_name("tag-push-repo")
        .send()
        .await
        .unwrap();
    let tags: Vec<String> = listed
        .image_ids()
        .iter()
        .filter_map(|id| id.image_tag().map(|s| s.to_string()))
        .collect();
    assert_eq!(tags, vec!["release-2024".to_string()]);
}
