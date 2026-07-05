//! AWS CodeBuild `StartBuild` runs the buildspec for REAL in a Docker
//! container and settles `buildStatus` off the real per-command exit codes —
//! the wedge against a status-flip stub that fabricates `SUCCEEDED` with no
//! container. A build whose `build` phase runs a failing command must settle
//! `FAILED`, and the real per-phase breakdown must be present.
//!
//! Docker-gated: requires a working docker daemon (mirrors the Batch/ECS
//! real-runtime tests). Panics in CI if docker is missing, skips locally.

mod helpers;

use std::time::Duration;

use aws_sdk_codebuild::types::{
    ArtifactsType, ComputeType, EnvironmentType, ProjectArtifacts, ProjectEnvironment,
    ProjectSource, SourceType, StatusType,
};
use helpers::TestServer;

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
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

/// Create a CodeBuild project named `name` with an inline `buildspec` and no
/// real source/artifacts. The environment image (`aws/codebuild/standard:7.0`)
/// is an AWS-curated image that isn't publicly pullable, so the backend maps it
/// to a small runnable Ubuntu — the point of the test is that the commands run.
async fn create_project(client: &aws_sdk_codebuild::Client, name: &str, buildspec: &str) {
    client
        .create_project()
        .name(name)
        .source(
            ProjectSource::builder()
                .r#type(SourceType::NoSource)
                .buildspec(buildspec)
                .build()
                .unwrap(),
        )
        .artifacts(
            ProjectArtifacts::builder()
                .r#type(ArtifactsType::NoArtifacts)
                .build()
                .unwrap(),
        )
        .environment(
            ProjectEnvironment::builder()
                .r#type(EnvironmentType::LinuxContainer)
                .image("aws/codebuild/standard:7.0")
                .compute_type(ComputeType::BuildGeneral1Small)
                .build()
                .unwrap(),
        )
        .service_role("arn:aws:iam::000000000000:role/codebuild-service-role")
        .send()
        .await
        .expect("create project");
}

/// Poll `BatchGetBuilds` until the build is complete or the deadline elapses.
/// Returns the settled build.
async fn wait_complete(
    client: &aws_sdk_codebuild::Client,
    build_id: &str,
) -> aws_sdk_codebuild::types::Build {
    for _ in 0..180 {
        let out = client
            .batch_get_builds()
            .ids(build_id)
            .send()
            .await
            .expect("batch get builds");
        if let Some(build) = out.builds().first() {
            if build.build_complete() {
                return build.clone();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("build {build_id} never completed");
}

/// Extract the status of a specific phase type from a build's real breakdown.
fn phase_status(build: &aws_sdk_codebuild::types::Build, phase: &str) -> Option<StatusType> {
    build
        .phases()
        .iter()
        .find(|p| p.phase_type().map(|t| t.as_str()) == Some(phase))
        .and_then(|p| p.phase_status().cloned())
}

#[tokio::test]
async fn start_build_runs_real_buildspec_and_succeeds() {
    if !require_docker_or_skip("start_build_runs_real_buildspec_and_succeeds") {
        return;
    }
    let s = TestServer::start().await;
    let cb = aws_sdk_codebuild::Client::new(&s.aws_config().await);

    // A real command that writes a file and reads it back. If the container
    // never ran, the phase breakdown below would not exist.
    let spec = "version: 0.2\n\
                phases:\n  \
                install:\n    commands:\n      - echo installing\n  \
                build:\n    commands:\n      - echo hello > out.txt\n      - cat out.txt\n  \
                post_build:\n    commands:\n      - echo done\n";
    create_project(&cb, "e2e-ok", spec).await;

    let start = cb
        .start_build()
        .project_name("e2e-ok")
        .send()
        .await
        .expect("start build");
    let build_id = start.build_value().unwrap().id().unwrap().to_string();
    // StartBuild returns immediately, IN_PROGRESS (non-blocking handler).
    assert_eq!(
        start.build_value().unwrap().build_status(),
        Some(&StatusType::InProgress)
    );

    let build = wait_complete(&cb, &build_id).await;
    assert_eq!(
        build.build_status(),
        Some(&StatusType::Succeeded),
        "real build should succeed; phases: {:?}",
        build.phases()
    );

    // The REAL per-phase breakdown must be present (not a fabricated one): the
    // buildspec phases actually ran in the container.
    assert_eq!(phase_status(&build, "INSTALL"), Some(StatusType::Succeeded));
    assert_eq!(phase_status(&build, "BUILD"), Some(StatusType::Succeeded));
    assert_eq!(
        phase_status(&build, "POST_BUILD"),
        Some(StatusType::Succeeded)
    );
    // Provisioning + a COMPLETED terminal phase are part of the real breakdown.
    assert!(build
        .phases()
        .iter()
        .any(|p| p.phase_type().map(|t| t.as_str()) == Some("COMPLETED")));

    // Logs point at a real CloudWatch group/stream a client can read.
    let logs = build.logs().expect("logs location");
    assert!(logs.group_name().is_some(), "log group set: {logs:?}");
}

#[tokio::test]
async fn start_build_failing_command_settles_failed() {
    if !require_docker_or_skip("start_build_failing_command_settles_failed") {
        return;
    }
    let s = TestServer::start().await;
    let cb = aws_sdk_codebuild::Client::new(&s.aws_config().await);

    // The build phase runs a failing command. A stub that fabricates success
    // would still report SUCCEEDED; a real container run reports FAILED.
    let spec = "version: 0.2\n\
                phases:\n  \
                build:\n    commands:\n      - echo about to fail\n      - exit 1\n  \
                post_build:\n    commands:\n      - echo post always runs\n";
    create_project(&cb, "e2e-bad", spec).await;

    let start = cb
        .start_build()
        .project_name("e2e-bad")
        .send()
        .await
        .expect("start build");
    let build_id = start.build_value().unwrap().id().unwrap().to_string();

    let build = wait_complete(&cb, &build_id).await;
    assert_eq!(
        build.build_status(),
        Some(&StatusType::Failed),
        "failing command must fail the build; phases: {:?}",
        build.phases()
    );
    assert_eq!(phase_status(&build, "BUILD"), Some(StatusType::Failed));
    // post_build still runs even after build fails (AWS semantics).
    assert!(
        phase_status(&build, "POST_BUILD").is_some(),
        "post_build must run even after build failure"
    );
}

#[tokio::test]
async fn restart_fails_in_flight_build_instead_of_zombie() {
    if !require_docker_or_skip("restart_fails_in_flight_build_instead_of_zombie") {
        return;
    }
    // A long build is in flight when the server restarts. Its background task
    // and backing container don't survive the restart, so without reconcile it
    // would hang IN_PROGRESS forever. It must come back terminal (FAILED).
    //
    // Use `start_full` with the persistent flags (rather than `start_persistent`,
    // which force-disables the container CLI) so the REAL backend runs and the
    // build is genuinely in flight across the restart.
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let mut server = TestServer::start_full(
        &[],
        &["--storage-mode", "persistent", "--data-path", &data_path],
    )
    .await;
    let cb = aws_sdk_codebuild::Client::new(&server.aws_config().await);

    let spec = "version: 0.2\nphases:\n  build:\n    commands:\n      - sleep 120\n";
    create_project(&cb, "e2e-restart", spec).await;
    let start = cb
        .start_build()
        .project_name("e2e-restart")
        .send()
        .await
        .expect("start build");
    let build_id = start.build_value().unwrap().id().unwrap().to_string();

    server.restart().await;
    let cb = aws_sdk_codebuild::Client::new(&server.aws_config().await);
    let out = cb
        .batch_get_builds()
        .ids(&build_id)
        .send()
        .await
        .expect("batch get builds");
    let build = out.builds().first().expect("build present after restart");
    assert_eq!(
        build.build_status(),
        Some(&StatusType::Failed),
        "in-flight build must be failed by restart reconcile, not left a zombie"
    );
}

#[tokio::test]
async fn cross_phase_shell_state_persists() {
    if !require_docker_or_skip("cross_phase_shell_state_persists") {
        return;
    }
    let s = TestServer::start().await;
    let cb = aws_sdk_codebuild::Client::new(&s.aws_config().await);

    // pre_build exports a var and changes directory; build relies on BOTH
    // surviving into the next phase. If each phase ran in its own shell (the
    // bug), `$FCTAG` would be empty and the cwd would reset -> the assertions
    // would fail and the build FAIL. It must SUCCEED, proving one continuous
    // shell across phases (matching AWS).
    let spec = "version: 0.2\n\
                phases:\n  \
                pre_build:\n    commands:\n      - export FCTAG=cross-phase-ok\n      - cd /tmp\n  \
                build:\n    commands:\n      \
                - test \"$FCTAG\" = cross-phase-ok\n      \
                - test \"$(pwd)\" = /tmp\n";
    create_project(&cb, "e2e-crossphase", spec).await;

    let start = cb
        .start_build()
        .project_name("e2e-crossphase")
        .send()
        .await
        .expect("start build");
    let build_id = start.build_value().unwrap().id().unwrap().to_string();
    let build = wait_complete(&cb, &build_id).await;
    assert_eq!(
        build.build_status(),
        Some(&StatusType::Succeeded),
        "cross-phase env/cwd must persist; phases: {:?}",
        build.phases()
    );
    assert_eq!(phase_status(&build, "BUILD"), Some(StatusType::Succeeded));
}

#[tokio::test]
async fn glob_artifacts_upload_to_s3() {
    if !require_docker_or_skip("glob_artifacts_upload_to_s3") {
        return;
    }
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cb = aws_sdk_codebuild::Client::new(&cfg);
    let s3 = aws_sdk_s3::Client::new(&cfg);

    let bucket = "e2e-codebuild-artifacts";
    s3.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");

    // The build produces two files under target/; the artifact glob
    // `target/*.txt` must match ONLY the .txt (a literal-path bug matches
    // nothing and silently uploads zero). packaging NONE -> individual objects.
    let spec = "version: 0.2\n\
                phases:\n  \
                build:\n    commands:\n      \
                - mkdir -p target\n      \
                - echo artifact-body > target/result.txt\n      \
                - echo ignore > target/other.bin\n\
                artifacts:\n  files:\n    - 'target/*.txt'\n  name: myartifacts\n";
    cb.create_project()
        .name("e2e-artifacts")
        .source(
            ProjectSource::builder()
                .r#type(SourceType::NoSource)
                .buildspec(spec)
                .build()
                .unwrap(),
        )
        .artifacts(
            ProjectArtifacts::builder()
                .r#type(ArtifactsType::S3)
                .location(bucket)
                .name("myartifacts")
                .packaging(aws_sdk_codebuild::types::ArtifactPackaging::None)
                .build()
                .unwrap(),
        )
        .environment(
            ProjectEnvironment::builder()
                .r#type(EnvironmentType::LinuxContainer)
                .image("aws/codebuild/standard:7.0")
                .compute_type(ComputeType::BuildGeneral1Small)
                .build()
                .unwrap(),
        )
        .service_role("arn:aws:iam::000000000000:role/codebuild-service-role")
        .send()
        .await
        .expect("create project");

    let start = cb
        .start_build()
        .project_name("e2e-artifacts")
        .send()
        .await
        .expect("start build");
    let build_id = start.build_value().unwrap().id().unwrap().to_string();
    let build = wait_complete(&cb, &build_id).await;
    assert_eq!(
        build.build_status(),
        Some(&StatusType::Succeeded),
        "artifact build should succeed; phases: {:?}",
        build.phases()
    );

    // The matched .txt artifact was uploaded under <name>/<relpath>; the .bin
    // (not matched by the glob) must NOT be present.
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key("myartifacts/target/result.txt")
        .send()
        .await
        .expect("artifact object present in S3");
    let body = obj.body.collect().await.expect("read body").into_bytes();
    assert_eq!(&body[..], b"artifact-body\n");

    let missing = s3
        .get_object()
        .bucket(bucket)
        .key("myartifacts/target/other.bin")
        .send()
        .await;
    assert!(
        missing.is_err(),
        "non-matching file must not be uploaded as an artifact"
    );
}
