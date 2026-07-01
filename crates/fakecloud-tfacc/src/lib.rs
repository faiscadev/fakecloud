//! Upstream Terraform Provider acceptance-test harness.
//!
//! Each `#[tokio::test]` in `tests/acc.rs` spawns a fakecloud process, sets
//! `TF_ACC=1` plus `AWS_ENDPOINT_URL_<SERVICE>=…` env vars, and invokes a
//! single `TestAcc*` function from `hashicorp/terraform-provider-aws` via
//! `go test`. The upstream test does its own Terraform apply/plan/destroy
//! cycle and asserts on the returned resource state — giving us semantic
//! coverage (waiters, field presence, drift) that SDK-based tests miss.
//!
//! Prior art: `bblommers/localstack-terraform-test`. We invert the model to
//! an *allow*-list rather than a deny-list to match fakecloud's
//! parity-per-implemented-service invariant.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

pub mod allowlist;

pub use allowlist::{shard_deny_list, Service, Shard, SERVICES, SHARDS};

/// Pinned upstream provider tag. Bumping is a deliberate edit; newer tags
/// may add acc tests that assume fields fakecloud does not yet return.
pub const PROVIDER_TAG: &str = "v5.97.0";
pub const PROVIDER_REPO: &str = "https://github.com/hashicorp/terraform-provider-aws.git";

/// Panics with an actionable message if `go` or `terraform` are missing.
/// Called at the top of every tfacc test so the failure mode is loud:
/// running this crate is an opt-in signal that the caller wants the
/// upstream Terraform suite exercised, and silently skipping would just
/// hide regressions.
pub fn require_toolchain() {
    let missing: Vec<&str> = [("go", "go"), ("terraform", "terraform")]
        .into_iter()
        .filter(|(_, bin)| {
            Command::new(bin)
                .arg("version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| !s.success())
                .unwrap_or(true)
        })
        .map(|(name, _)| name)
        .collect();
    if !missing.is_empty() {
        panic!(
            "fakecloud-tfacc requires {} on PATH. Install them (or run a \
             different test crate) before exercising the upstream Terraform \
             acceptance suite.",
            missing.join(" and "),
        );
    }
}

/// Idempotently clone + patch the upstream provider into `target/tfacc/`.
///
/// Returns the absolute path to the provider source tree, ready for
/// `go test ./internal/service/<svc>/`.
///
/// On Go ≥ 1.24 the upstream `go.mod` needs its `godebug tlskyber=0`
/// directive stripped (the pragma was removed in 1.24). We apply the strip
/// unconditionally — it's harmless on 1.23.
pub fn setup_provider_source() -> std::io::Result<PathBuf> {
    let target = provider_dir();
    if !target.exists() {
        let parent = target.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("provider_dir() has no parent: {}", target.display()),
            )
        })?;
        std::fs::create_dir_all(parent)?;
        let status = Command::new("git")
            .args([
                "clone",
                "--depth",
                "1",
                "--branch",
                PROVIDER_TAG,
                PROVIDER_REPO,
                &target.display().to_string(),
            ])
            .status()?;
        if !status.success() {
            return Err(std::io::Error::other(format!(
                "failed to clone {PROVIDER_REPO}@{PROVIDER_TAG}"
            )));
        }
    }
    strip_godebug(&target.join("go.mod"))?;
    Ok(target)
}

fn provider_dir() -> PathBuf {
    // target/tfacc/terraform-provider-aws — sibling to target/debug
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .join("..")
        .join("..")
        .join("target")
        .join("tfacc")
        .join(format!("terraform-provider-aws-{PROVIDER_TAG}"))
}

fn strip_godebug(go_mod: &Path) -> std::io::Result<()> {
    let contents = std::fs::read_to_string(go_mod)?;
    if !contents.contains("godebug tlskyber") {
        return Ok(());
    }
    // Preserve the original line endings (including whether the file has a
    // trailing newline) by splitting on `\n` rather than `lines()`, which
    // silently swallows the final empty element.
    let stripped: String = contents
        .split_inclusive('\n')
        .filter(|line| !line.trim_start().starts_with("godebug tlskyber"))
        .collect();
    std::fs::write(go_mod, stripped)
}

/// Thin newtype over `fakecloud_testkit::TestServer`. Delegates the
/// lifecycle (graceful SIGTERM, container sweep on drop) to testkit so
/// tfacc stops leaking Docker containers when a Go acceptance test panics.
pub struct TestServer(fakecloud_testkit::TestServer);

impl TestServer {
    pub async fn start() -> Self {
        Self(fakecloud_testkit::TestServer::start().await)
    }

    pub fn port(&self) -> u16 {
        self.0.port()
    }

    pub fn endpoint(&self) -> String {
        self.0.endpoint().to_string()
    }
}

/// Runs every `TestAcc*` test for a service — minus deny-listed names —
/// against a running fakecloud instance.
///
/// We intentionally run the whole service at once rather than one Go test
/// per Rust test: provider process startup dominates per-test time, and
/// `go test -skip` lets us exclude unsupportable tests cheaply.
pub struct GoTestRunner<'a> {
    pub provider_root: &'a Path,
    pub endpoint: String,
}

/// Go `-parallel` degree for a service's `go test` invocation. Defaults to 4;
/// a few background-loop + waiter-heavy services drop to 2 so their lifecycle
/// settle tasks don't starve for CPU under four concurrent terraform applies on
/// a 2-core CI runner. `pipes` runs a 500ms source-poll loop alongside
/// create/import/update/delete waiters and is the clearest such case.
fn parallelism_for(service: &str) -> u32 {
    match service {
        "pipes" => 2,
        _ => 4,
    }
}

impl<'a> GoTestRunner<'a> {
    pub fn run_service(&self, service: &Service) -> GoTestResult {
        self.run_go_tests(service.name, service.run_regex, service.deny)
    }

    /// Run the `go test` slice for a single matrix shard. Merges the
    /// shard's `extra_deny` with its owning service's deny-list so
    /// sibling shards of the same service don't race on the same test.
    pub fn run_shard(&self, shard: &Shard) -> GoTestResult {
        let deny = shard_deny_list(shard);
        self.run_go_tests(shard.service, shard.run_regex, &deny)
    }

    fn run_go_tests(&self, service: &str, run_regex: &str, deny: &[&str]) -> GoTestResult {
        let service_path = format!("./internal/service/{service}/");
        let skip_re = if deny.is_empty() {
            String::new()
        } else {
            format!("^({})$", deny.join("|"))
        };

        // `-parallel N` lets Go's test runner execute up to N `t.Parallel()`
        // subtests concurrently within a single `go test` invocation. We use 4
        // (rather than 8 or runner core count) because some upstream tests
        // poll fakecloud aggressively under parallel load and can starve the
        // request loop, surfacing as suite-wide hangs. CI fan-out across
        // services is handled by the GitHub Actions matrix, so wall time
        // scales with the slowest single service, not their sum.
        //
        // A few services drive a fakecloud-side background loop plus multi-step
        // create/update/delete lifecycle waiters, and on a constrained 2-core CI
        // runner four of those in parallel saturate the CPU enough that the
        // lifecycle settle tasks starve and the provider's delete/update waiters
        // time out (observed as a 30-minute stuck DELETING pipe). Those run at a
        // lower parallelism; see `parallelism_for`.
        let parallel = parallelism_for(service);
        let mut cmd = Command::new("go");
        let mut args: Vec<String> = vec![
            "test".into(),
            service_path,
            "-run".into(),
            run_regex.to_string(),
            "-v".into(),
            "-timeout".into(),
            "90m".into(),
            "-count=1".into(),
            "-parallel".into(),
            parallel.to_string(),
        ];
        if !skip_re.is_empty() {
            args.push("-skip".into());
            args.push(skip_re);
        }
        cmd.args(&args)
            .current_dir(self.provider_root)
            .env("TF_ACC", "1")
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_REGION", "us-east-1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Route every service we care about to the single fakecloud endpoint.
        // AWS SDK Go v2 honours AWS_ENDPOINT_URL_<SERVICE>; these override any
        // default endpoint lookup.
        for (key, _service_id) in ENDPOINT_ENV_VARS {
            cmd.env(key, &self.endpoint);
        }

        // Stream `go test` output live (echo each line to our own stdout/stderr
        // as it arrives) instead of buffering until exit. `go test -v` can run
        // for many minutes; if a single subtest hangs and CI kills the job on
        // its timeout, a buffered `cmd.output()` would discard everything and
        // the CI log would show no `=== RUN` / `--- PASS` lines — leaving the
        // culprit invisible (exactly the blind spot a pipes-shard hang hit).
        // Streaming means the last un-paired `=== RUN` in the job log names the
        // hung test even when the process is killed mid-run. We still collect
        // the bytes so `assert_pass` can parse the failure summary.
        let mut child = cmd.spawn().expect("spawn go test");
        let child_stdout = child.stdout.take().expect("piped stdout");
        let child_stderr = child.stderr.take().expect("piped stderr");
        let stdout_handle = std::thread::spawn(move || tee(child_stdout, false));
        let stderr_handle = std::thread::spawn(move || tee(child_stderr, true));
        let status = child.wait().expect("wait for go test");
        let stdout = stdout_handle.join().unwrap_or_default();
        let stderr = stderr_handle.join().unwrap_or_default();
        GoTestResult {
            success: status.success(),
            output: Output {
                status,
                stdout,
                stderr,
            },
        }
    }
}

/// Read a child pipe line by line, echoing each line to the parent's stdout
/// (or stderr) as it arrives so the output survives a timeout-kill, while
/// accumulating the raw bytes for later parsing.
fn tee<R: std::io::Read>(pipe: R, is_stderr: bool) -> Vec<u8> {
    let mut collected = Vec::new();
    let mut reader = BufReader::new(pipe);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {
                collected.extend_from_slice(&line);
                if is_stderr {
                    let _ = std::io::stderr().write_all(&line);
                    let _ = std::io::stderr().flush();
                } else {
                    let _ = std::io::stdout().write_all(&line);
                    let _ = std::io::stdout().flush();
                }
            }
            Err(_) => break,
        }
    }
    collected
}

pub struct GoTestResult {
    pub success: bool,
    pub output: Output,
}

impl GoTestResult {
    /// On failure, dump the full `go test` output to a stable path under
    /// `target/tfacc/logs/` and panic with the failing-test list plus
    /// the step-level error snippets. Acc-test failures usually cite a
    /// single line ("Step 1/4 error: Check failed: ..."), so we extract
    /// any line between `=== NAME` and `--- FAIL` that looks like a
    /// step error and include it inline in the panic message.
    pub fn assert_pass(self, service: &str) {
        if self.success {
            return;
        }
        let stdout = String::from_utf8_lossy(&self.output.stdout);
        let stderr = String::from_utf8_lossy(&self.output.stderr);
        let fails: Vec<String> = stdout
            .lines()
            .filter(|l| l.contains("--- FAIL:"))
            .map(|l| l.to_string())
            .collect();
        let step_errors: Vec<String> = stdout
            .lines()
            .filter(|l| l.contains("Step ") && l.contains(" error:"))
            .map(|l| l.trim().to_string())
            .collect();
        let combined = format!("--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");

        let log_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("tfacc")
            .join("logs");
        let _ = std::fs::create_dir_all(&log_dir);
        let log_path = log_dir.join(format!("{service}-failure.log"));
        let _ = std::fs::write(&log_path, &combined);

        let step_error_block = if step_errors.is_empty() {
            String::from("(no step-level error lines found in output)")
        } else {
            step_errors.join("\n")
        };

        panic!(
            "upstream TestAcc failures in service `{service}` ({} failed):\n{}\n\nStep errors:\n{}\n\nFull go test output: {}",
            fails.len(),
            fails.join("\n"),
            step_error_block,
            log_path.display(),
        );
    }
}

/// `(env_var, provider_service_id)` — set every entry to the fakecloud
/// endpoint so a single acc test can touch multiple services.
pub const ENDPOINT_ENV_VARS: &[(&str, &str)] = &[
    ("AWS_ENDPOINT_URL", "default"),
    ("AWS_ENDPOINT_URL_SQS", "sqs"),
    ("AWS_ENDPOINT_URL_SNS", "sns"),
    ("AWS_ENDPOINT_URL_S3", "s3"),
    ("AWS_ENDPOINT_URL_IAM", "iam"),
    ("AWS_ENDPOINT_URL_STS", "sts"),
    ("AWS_ENDPOINT_URL_SSM", "ssm"),
    ("AWS_ENDPOINT_URL_DYNAMODB", "dynamodb"),
    ("AWS_ENDPOINT_URL_LAMBDA", "lambda"),
    ("AWS_ENDPOINT_URL_SECRETSMANAGER", "secretsmanager"),
    ("AWS_ENDPOINT_URL_EVENTBRIDGE", "eventbridge"),
    ("AWS_ENDPOINT_URL_KMS", "kms"),
    ("AWS_ENDPOINT_URL_LOGS", "logs"),
    ("AWS_ENDPOINT_URL_KINESIS", "kinesis"),
    ("AWS_ENDPOINT_URL_RDS", "rds"),
    ("AWS_ENDPOINT_URL_ELASTICACHE", "elasticache"),
    ("AWS_ENDPOINT_URL_CLOUDFORMATION", "cloudformation"),
    ("AWS_ENDPOINT_URL_SESV2", "sesv2"),
    ("AWS_ENDPOINT_URL_SES", "ses"),
    ("AWS_ENDPOINT_URL_COGNITO_IDP", "cognitoidp"),
    ("AWS_ENDPOINT_URL_SFN", "sfn"),
    ("AWS_ENDPOINT_URL_APIGATEWAYV2", "apigatewayv2"),
    ("AWS_ENDPOINT_URL_BEDROCK", "bedrock"),
    ("AWS_ENDPOINT_URL_ROUTE53", "route53"),
    ("AWS_ENDPOINT_URL_ORGANIZATIONS", "organizations"),
    ("AWS_ENDPOINT_URL_ECR", "ecr"),
    ("AWS_ENDPOINT_URL_GLUE", "glue"),
    ("AWS_ENDPOINT_URL_CLOUDFRONT", "cloudfront"),
    ("AWS_ENDPOINT_URL_EC2", "ec2"),
    ("AWS_ENDPOINT_URL_ECS", "ecs"),
    ("AWS_ENDPOINT_URL_BATCH", "batch"),
    ("AWS_ENDPOINT_URL_COGNITO_IDENTITY", "cognito-identity"),
    ("AWS_ENDPOINT_URL_SCHEDULER", "scheduler"),
    ("AWS_ENDPOINT_URL_PIPES", "pipes"),
    ("AWS_ENDPOINT_URL_WAFV2", "wafv2"),
    ("AWS_ENDPOINT_URL_FIREHOSE", "firehose"),
    ("AWS_ENDPOINT_URL_CLOUDWATCH", "monitoring"),
    ("AWS_ENDPOINT_URL_API_GATEWAY", "apigateway"),
    (
        "AWS_ENDPOINT_URL_ELASTIC_LOAD_BALANCING_V2",
        "elasticloadbalancingv2",
    ),
    ("AWS_ENDPOINT_URL_BEDROCK_AGENT", "bedrock-agent"),
    (
        "AWS_ENDPOINT_URL_APPLICATION_AUTO_SCALING",
        "application-autoscaling",
    ),
];
