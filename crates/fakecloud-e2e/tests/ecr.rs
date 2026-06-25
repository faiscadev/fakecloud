//! ECR Batch 1: repository CRUD, policy, tagging.

mod helpers;

use base64::Engine;
use helpers::TestServer;

#[tokio::test]
async fn create_describe_list_delete_repository() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    let created = client
        .create_repository()
        .repository_name("batch1-repo")
        .send()
        .await
        .expect("create_repository");
    let repo = created.repository().expect("repository");
    assert_eq!(repo.repository_name(), Some("batch1-repo"));
    assert_eq!(repo.image_tag_mutability().unwrap().as_str(), "MUTABLE");
    let uri = repo.repository_uri().unwrap();
    assert!(uri.ends_with("/batch1-repo"), "unexpected uri: {uri}");

    // DescribeRepositories without filter returns everything.
    let all = client
        .describe_repositories()
        .send()
        .await
        .expect("describe_repositories");
    assert_eq!(all.repositories().len(), 1);

    // With repositoryNames filter returns the matching one.
    let filtered = client
        .describe_repositories()
        .repository_names("batch1-repo")
        .send()
        .await
        .expect("describe filtered");
    assert_eq!(filtered.repositories().len(), 1);

    // Delete returns the repository snapshot.
    let deleted = client
        .delete_repository()
        .repository_name("batch1-repo")
        .send()
        .await
        .expect("delete_repository");
    assert_eq!(
        deleted.repository().and_then(|r| r.repository_name()),
        Some("batch1-repo")
    );

    // And subsequent describe 404s.
    let err = client
        .describe_repositories()
        .repository_names("batch1-repo")
        .send()
        .await
        .expect_err("should error after delete");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("RepositoryNotFound"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn delete_repository_with_images_requires_force() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    client
        .create_repository()
        .repository_name("nonempty-repo")
        .send()
        .await
        .expect("create_repository");

    let manifest = r#"{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":7023,"digest":"sha256:dummy"},"layers":[]}"#;
    client
        .put_image()
        .repository_name("nonempty-repo")
        .image_manifest(manifest)
        .image_tag("v1")
        .send()
        .await
        .expect("put_image");

    // Without force, a repository holding images must not delete (bug-hunt
    // 2026-06-24, 1.13 — force was accepted but ignored).
    let err = client
        .delete_repository()
        .repository_name("nonempty-repo")
        .send()
        .await
        .expect_err("delete without force should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("RepositoryNotEmpty"),
        "unexpected error: {msg}"
    );

    // The repository must still exist.
    let still = client
        .describe_repositories()
        .repository_names("nonempty-repo")
        .send()
        .await
        .expect("repo still present");
    assert_eq!(still.repositories().len(), 1);

    // With force=true the delete succeeds.
    client
        .delete_repository()
        .repository_name("nonempty-repo")
        .force(true)
        .send()
        .await
        .expect("force delete");
    let err = client
        .describe_repositories()
        .repository_names("nonempty-repo")
        .send()
        .await
        .expect_err("gone after force delete");
    assert!(format!("{err:?}").contains("RepositoryNotFound"));
}

#[tokio::test]
async fn create_repository_with_options() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    use aws_sdk_ecr::types::{ImageScanningConfiguration, ImageTagMutability, Tag};

    client
        .create_repository()
        .repository_name("immutable-repo")
        .image_tag_mutability(ImageTagMutability::Immutable)
        .image_scanning_configuration(
            ImageScanningConfiguration::builder()
                .scan_on_push(true)
                .build(),
        )
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .expect("create with options");

    let resp = client
        .describe_repositories()
        .repository_names("immutable-repo")
        .send()
        .await
        .expect("describe");
    let repo = resp.repositories().first().expect("one repo");
    assert_eq!(repo.image_tag_mutability().unwrap().as_str(), "IMMUTABLE");
    assert!(repo.image_scanning_configuration().unwrap().scan_on_push());
}

#[tokio::test]
async fn repository_policy_round_trip() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    client
        .create_repository()
        .repository_name("policy-repo")
        .send()
        .await
        .expect("create");

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Sid":"Allow","Effect":"Allow","Principal":{"AWS":"*"},"Action":"ecr:GetDownloadUrlForLayer"}]}"#;

    client
        .set_repository_policy()
        .repository_name("policy-repo")
        .policy_text(policy)
        .send()
        .await
        .expect("set_repository_policy");

    let fetched = client
        .get_repository_policy()
        .repository_name("policy-repo")
        .send()
        .await
        .expect("get_repository_policy");
    assert_eq!(fetched.policy_text(), Some(policy));

    client
        .delete_repository_policy()
        .repository_name("policy-repo")
        .send()
        .await
        .expect("delete_repository_policy");

    let err = client
        .get_repository_policy()
        .repository_name("policy-repo")
        .send()
        .await
        .expect_err("policy gone");
    assert!(format!("{err:?}").contains("RepositoryPolicyNotFound"));
}

#[tokio::test]
async fn put_image_tag_mutability_and_scanning() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    client
        .create_repository()
        .repository_name("mutability-repo")
        .send()
        .await
        .expect("create");

    use aws_sdk_ecr::types::{ImageScanningConfiguration, ImageTagMutability};

    let r1 = client
        .put_image_tag_mutability()
        .repository_name("mutability-repo")
        .image_tag_mutability(ImageTagMutability::Immutable)
        .send()
        .await
        .expect("put_image_tag_mutability");
    assert_eq!(r1.image_tag_mutability().unwrap().as_str(), "IMMUTABLE");

    let r2 = client
        .put_image_scanning_configuration()
        .repository_name("mutability-repo")
        .image_scanning_configuration(
            ImageScanningConfiguration::builder()
                .scan_on_push(true)
                .build(),
        )
        .send()
        .await
        .expect("put_image_scanning_configuration");
    assert!(r2.image_scanning_configuration().unwrap().scan_on_push());
}

#[tokio::test]
async fn tag_resource_round_trip() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    let created = client
        .create_repository()
        .repository_name("tagged-repo")
        .send()
        .await
        .expect("create");
    let arn = created
        .repository()
        .expect("repository")
        .repository_arn()
        .expect("arn")
        .to_string();

    use aws_sdk_ecr::types::Tag;

    client
        .tag_resource()
        .resource_arn(&arn)
        .tags(
            Tag::builder()
                .key("team")
                .value("platform")
                .build()
                .unwrap(),
        )
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .expect("tag_resource");

    let listed = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    let mut kv: Vec<(String, String)> = listed
        .tags()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    kv.sort();
    assert_eq!(
        kv,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ]
    );

    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag_resource");
    let after = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags after");
    assert_eq!(after.tags().len(), 1);
    assert_eq!(after.tags()[0].key(), "team");
}

#[tokio::test]
async fn start_image_scan_transitions_to_complete() {
    use std::time::Duration;
    let server = TestServer::start().await;
    let client = server.ecr_client().await;
    client
        .create_repository()
        .repository_name("scan-transition-repo")
        .send()
        .await
        .unwrap();

    // Push a minimal manifest so DescribeImageScanFindings can find an image.
    let http = reqwest::Client::new();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("AWS:test")
    );
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": 0,
            "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        },
        "layers": []
    });
    http.put(format!(
        "{}/v2/scan-transition-repo/manifests/v1",
        server.endpoint()
    ))
    .header("Authorization", &auth)
    .header(
        "Content-Type",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    .body(serde_json::to_vec(&manifest).unwrap())
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap();

    use aws_sdk_ecr::types::ImageIdentifier;
    let scan = client
        .start_image_scan()
        .repository_name("scan-transition-repo")
        .image_id(ImageIdentifier::builder().image_tag("v1").build())
        .send()
        .await
        .unwrap();
    assert_eq!(
        scan.image_scan_status()
            .and_then(|s| s.status())
            .map(|s| s.as_str()),
        Some("IN_PROGRESS")
    );

    // Background scanner finishes shortly (synthetic fallback when Trivy
    // is unavailable; real findings when it is). Poll up to 30s.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut final_status = String::new();
    while std::time::Instant::now() < deadline {
        let resp = client
            .describe_image_scan_findings()
            .repository_name("scan-transition-repo")
            .image_id(ImageIdentifier::builder().image_tag("v1").build())
            .send()
            .await
            .unwrap();
        let status = resp
            .image_scan_status()
            .and_then(|s| s.status())
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        if status == "COMPLETE" {
            final_status = status;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(final_status, "COMPLETE", "scan never reached COMPLETE");
}

#[tokio::test]
async fn duplicate_create_returns_already_exists() {
    let server = TestServer::start().await;
    let client = server.ecr_client().await;

    client
        .create_repository()
        .repository_name("dup-repo")
        .send()
        .await
        .expect("first create");

    let err = client
        .create_repository()
        .repository_name("dup-repo")
        .send()
        .await
        .expect_err("duplicate should fail");
    assert!(format!("{err:?}").contains("RepositoryAlreadyExists"));
}
