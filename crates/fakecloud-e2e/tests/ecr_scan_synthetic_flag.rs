//! ECR `DescribeImageScanFindings` returns the AWS-shaped response.
//! When the optional Trivy CLI is not installed, fakecloud still returns
//! a well-formed empty findings result so client-side plumbing can be
//! exercised end-to-end. Asserted via raw HTTP so the assertion holds
//! whether or not the test environment has Trivy on PATH.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn describe_image_scan_findings_returns_well_formed_response() {
    let server = TestServer::start().await;
    let ecr = server.ecr_client().await;

    ecr.create_repository()
        .repository_name("scan-shape-repo")
        .send()
        .await
        .unwrap();

    // Push a minimal empty manifest via OCI v2 so there's an image to
    // reference — `DescribeImageScanFindings` requires an existing
    // imageId (digest or tag).
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
    let body = serde_json::to_vec(&manifest).unwrap();
    http.put(format!(
        "{}/v2/scan-shape-repo/manifests/v1",
        server.endpoint()
    ))
    .header("Authorization", &auth)
    .header(
        "Content-Type",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
    .body(body)
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap();

    // A never-scanned image now returns ScanNotFoundException (matching AWS),
    // so kick off a scan first. With no Trivy on PATH the scan still completes
    // with an empty, well-formed findings result — which is what this test
    // asserts the shape of.
    ecr.start_image_scan()
        .repository_name("scan-shape-repo")
        .image_id(
            aws_sdk_ecr::types::ImageIdentifier::builder()
                .image_tag("v1")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Hit the AWS JSON surface via raw HTTP.
    let resp = http
        .post(server.endpoint())
        .header(
            "X-Amz-Target",
            "AmazonEC2ContainerRegistry_V20150921.DescribeImageScanFindings",
        )
        .header("Content-Type", "application/x-amz-json-1.1")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260424/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=0000",
        )
        .body(
            serde_json::json!({
                "repositoryName": "scan-shape-repo",
                "imageId": { "imageTag": "v1" }
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status = {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["imageScanFindings"]["findings"].is_array());
    assert!(body["imageScanFindings"]["findingSeverityCounts"].is_object());
    // Verify *absence* of the `isSynthetic` field — `is_null()` would
    // accept an explicit `null`, which is a different shape than what
    // AWS returns. Use the parent object's key set instead.
    let scan_findings = body["imageScanFindings"]
        .as_object()
        .expect("imageScanFindings is an object");
    assert!(
        !scan_findings.contains_key("isSynthetic"),
        "isSynthetic was a fakecloud-only marker that has been removed; AWS's \
         DescribeImageScanFindings returns no such field, so the key must be absent",
    );
}

use base64::Engine;
