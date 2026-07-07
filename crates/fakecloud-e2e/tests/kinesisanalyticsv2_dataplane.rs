//! Amazon Managed Service for Apache Flink (kinesisanalyticsv2) DATA-PLANE E2E:
//! proves that starting a Flink-flavor application spawns a REAL Apache Flink
//! job in a Docker container, not a formatted-but-dead dashboard URL.
//!
//! The test obtains a genuine Flink job JAR by copying one of the `flink:1.19`
//! image's bundled examples (`StateMachineExample.jar`, which runs
//! continuously) out of the image, uploads it into a fakecloud S3 bucket, then:
//!
//!   1. CreateApplication (FLINK-1_19, code = that S3 object) + StartApplication
//!   2. poll DescribeApplication until RUNNING — which only happens once the
//!      REAL Flink job reaches RUNNING on the spawned session cluster
//!   3. CreateApplicationPresignedUrl returns the reachable dashboard; querying
//!      the live Flink REST `/jobs` through it shows exactly one RUNNING job
//!      (the round trip that proves the data plane actually works)
//!   4. StopApplication -> poll until READY; the backing container is torn down
//!      (which only happens after the real job is canceled) — asserted by the
//!      absence of any leftover container.
//!
//! Gated on Docker + `FAKECLOUD_E2E_FLINK=1` (the heavy Flink container). It
//! runs ONLY in the dedicated, resourced `flink-runtime` CI job; in the shared
//! partition the flag is unset and it skips loudly. In that CI job a missing
//! Docker hard-fails rather than silently skipping.

mod helpers;

use std::time::Duration;

use aws_sdk_kinesisanalyticsv2::types::{
    ApplicationCodeConfiguration, ApplicationConfiguration, CodeContent, CodeContentType,
    RuntimeEnvironment, S3ContentLocation, UrlType,
};
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

const FLINK_IMAGE: &str = "flink:1.19";
const EXAMPLE_JAR: &str = "/opt/flink/examples/streaming/StateMachineExample.jar";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    // The real Flink session cluster (a JVM JobManager + TaskManager) is heavy
    // and strains the shared E2E partition's runner, so this suite runs ONLY in
    // the dedicated, resourced `flink-runtime` CI job, which sets
    // FAKECLOUD_E2E_FLINK=1. In the shared partition the flag is unset and we
    // skip loudly (by design). In the dedicated CI job a missing Docker
    // hard-fails rather than silently skipping.
    if std::env::var("FAKECLOUD_E2E_FLINK").as_deref() != Ok("1") {
        eprintln!(
            "Skipping {test}: FAKECLOUD_E2E_FLINK!=1 (runs in the dedicated flink-runtime CI job)"
        );
        return false;
    }
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in the flink-runtime CI job");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

/// Copy the bundled StateMachineExample JAR out of the Flink image so we can
/// upload it into S3 as a genuine, runnable Flink job.
fn extract_example_jar() -> Vec<u8> {
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--entrypoint",
            "cat",
            FLINK_IMAGE,
            EXAMPLE_JAR,
        ])
        .output()
        .expect("run docker to extract the example jar");
    assert!(
        out.status.success(),
        "failed to extract {EXAMPLE_JAR} from {FLINK_IMAGE}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.len() > 1_000_000,
        "example jar looks too small ({} bytes)",
        out.stdout.len()
    );
    out.stdout
}

/// Whether any backing Flink container for `app_arn` is still present.
fn leftover_container(app_arn: &str) -> bool {
    let out = std::process::Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=fakecloud-flink={app_arn}"),
        ])
        .output()
        .expect("docker ps");
    !String::from_utf8_lossy(&out.stdout).trim().is_empty()
}

async fn ka2_client(server: &TestServer) -> aws_sdk_kinesisanalyticsv2::Client {
    aws_sdk_kinesisanalyticsv2::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn flink_application_runs_a_real_job_in_a_container() {
    if !require_docker_or_skip("flink_application_runs_a_real_job_in_a_container") {
        return;
    }

    let server = TestServer::start().await;
    let ka2 = ka2_client(&server).await;
    let s3 = server.s3_client().await;
    let http = reqwest::Client::new();

    // 1. Publish a genuine Flink job JAR into a fakecloud S3 bucket.
    let bucket = "fc-flink-code";
    let key = "jobs/StateMachineExample.jar";
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create S3 bucket");
    let jar = extract_example_jar();
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(jar))
        .send()
        .await
        .expect("put jar object");

    // 2. CreateApplication pointing at that S3 object, then StartApplication.
    let role = "arn:aws:iam::000000000000:role/service-role/kinesis-analytics";
    let code_cfg = ApplicationConfiguration::builder()
        .application_code_configuration(
            ApplicationCodeConfiguration::builder()
                .code_content(
                    CodeContent::builder()
                        .s3_content_location(
                            S3ContentLocation::builder()
                                .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                                .file_key(key)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .code_content_type(CodeContentType::Zipfile)
                .build()
                .unwrap(),
        )
        .build();

    let app_name = "fc-flink-dataplane";
    let created = ka2
        .create_application()
        .application_name(app_name)
        .runtime_environment(RuntimeEnvironment::from("FLINK_1_19"))
        .service_execution_role(role)
        .application_configuration(code_cfg)
        .send()
        .await
        .expect("create application");
    let app_arn = created
        .application_detail()
        .map(|d| d.application_arn().to_string())
        .expect("application arn");

    ka2.start_application()
        .application_name(app_name)
        .send()
        .await
        .expect("start application");

    // 3. Poll DescribeApplication until RUNNING. This only flips once the REAL
    //    Flink job reaches RUNNING on the spawned cluster (container pull + boot
    //    + TaskManager registration + job submission), so allow a wide window.
    let deadline = std::time::Instant::now() + Duration::from_secs(360);
    loop {
        let d = ka2
            .describe_application()
            .application_name(app_name)
            .send()
            .await
            .expect("describe application");
        let status = d
            .application_detail()
            .map(|a| a.application_status().as_str().to_string())
            .unwrap_or_default();
        if status == "RUNNING" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "application did not reach RUNNING within the deadline (last status: {status})"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // 4. The presigned URL is the REAL reachable Flink dashboard/REST base.
    //    Querying its /jobs endpoint must show exactly one RUNNING job — the
    //    round trip that proves the data plane genuinely works.
    let presigned = ka2
        .create_application_presigned_url()
        .application_name(app_name)
        .url_type(UrlType::FlinkDashboardUrl)
        .send()
        .await
        .expect("create presigned url");
    let dashboard = presigned
        .authorized_url()
        .expect("authorized url")
        .to_string();
    assert!(
        !dashboard.contains("amazonaws.com"),
        "a running Flink app must return a reachable dashboard, got {dashboard}"
    );

    let jobs: serde_json::Value = http
        .get(format!("{dashboard}/jobs"))
        .send()
        .await
        .expect("GET /jobs on the live Flink cluster")
        .json()
        .await
        .expect("parse /jobs");
    let running: Vec<&serde_json::Value> = jobs["jobs"]
        .as_array()
        .expect("jobs array")
        .iter()
        .filter(|j| j["status"] == "RUNNING")
        .collect();
    assert_eq!(
        running.len(),
        1,
        "exactly one RUNNING Flink job expected on the live cluster, got {jobs}"
    );

    // 5. StopApplication -> poll until READY; the container is torn down only
    //    after the real job is canceled, so no container may be left behind.
    ka2.stop_application()
        .application_name(app_name)
        .send()
        .await
        .expect("stop application");

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let d = ka2
            .describe_application()
            .application_name(app_name)
            .send()
            .await
            .expect("describe application");
        let status = d
            .application_detail()
            .map(|a| a.application_status().as_str().to_string())
            .unwrap_or_default();
        if status == "READY" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "application did not return to READY after stop (last status: {status})"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(
        !leftover_container(&app_arn),
        "the backing Flink container must be torn down on stop (no leftover)"
    );
}
