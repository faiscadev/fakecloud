#![allow(dead_code)]

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};

/// Guard that kills the child process on drop.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Guard for a container-backed server: asks it to shut down gracefully so it
/// runs every runtime's `stop_all()` and removes the containers it started,
/// and only kills it if it has not exited within the grace period.
struct GracefulChild(Child);

impl Drop for GracefulChild {
    fn drop(&mut self) {
        let terminated = ProcessCommand::new("kill")
            .arg("-TERM")
            .arg(self.0.id().to_string())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if terminated {
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            while std::time::Instant::now() < deadline {
                if let Ok(Some(_)) = self.0.try_wait() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
use std::time::Duration;

mod checksum;
mod generators;
mod lambda_seed;
mod probe;
mod report;
mod shape_validator;
mod smithy;

#[derive(Parser)]
#[command(name = "fakecloud-conformance", about = "AWS API conformance testing")]
struct Cli {
    /// Path to the aws-models directory
    #[arg(long, default_value = "aws-models")]
    models_dir: PathBuf,

    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Print all operations per service from the Smithy models
    Operations,
    /// Print model checksums for all operations
    Checksums,
    /// Run Level 1 auto-generated conformance probes
    Run {
        /// Only test these services (comma-separated)
        #[arg(long)]
        services: Option<String>,
        /// Output format: text or json
        #[arg(long, default_value = "text")]
        format: String,
        /// Connect to an already-running fakecloud at this endpoint
        #[arg(long)]
        endpoint: Option<String>,
    },
    /// Run Level 2 audit: check handwritten test coverage
    Audit,
    /// Check that each crate's vendored `model.json` still matches `aws-models/`
    VendoredModels,
    /// Check conformance results (fails on any failing variant or any drift from the baseline)
    Check {
        /// Path to conformance-baseline.json
        #[arg(long, default_value = "conformance-baseline.json")]
        baseline: PathBuf,
        /// Connect to an already-running fakecloud at this endpoint
        #[arg(long)]
        endpoint: Option<String>,
        /// Also write the full JSON report to this path
        #[arg(long)]
        json_out: Option<PathBuf>,
        /// Also write a compact markdown summary (suitable for $GITHUB_STEP_SUMMARY) to this path
        #[arg(long)]
        markdown_summary_out: Option<PathBuf>,
    },
    /// Update the baseline file with current conformance results
    UpdateBaseline {
        /// Path to conformance-baseline.json
        #[arg(long, default_value = "conformance-baseline.json")]
        baseline: PathBuf,
        /// Connect to an already-running fakecloud at this endpoint
        #[arg(long)]
        endpoint: Option<String>,
    },
}

/// A handful of service crates `include_str!` their own copy of the Smithy
/// model for model-driven input validation, because `cargo publish` only
/// packages files under the crate root and cannot reach `aws-models/`. Those
/// copies go stale silently every time the weekly refresh lands, so check them.
fn cmd_vendored_models(project_root: &std::path::Path, models_dir: &std::path::Path) -> bool {
    let crates_dir = project_root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates_dir) else {
        eprintln!("cannot read {}", crates_dir.display());
        return false;
    };
    let mut vendored: Vec<(String, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let copy = entry.path().join("model.json");
        if !copy.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let service = name.strip_prefix("fakecloud-").unwrap_or(&name).to_string();
        vendored.push((service, copy));
    }
    vendored.sort();

    println!("=== Vendored Smithy models ===");
    let mut stale = Vec::new();
    for (service, copy) in &vendored {
        let source = if models_dir.is_absolute() {
            models_dir.join(format!("{service}.json"))
        } else {
            project_root
                .join(models_dir)
                .join(format!("{service}.json"))
        };
        let a = match std::fs::read(copy) {
            Ok(a) => a,
            Err(e) => {
                println!("  [x] {service}: cannot read {} ({e})", copy.display());
                stale.push(service.clone());
                continue;
            }
        };
        // A crate whose directory suffix is not its model's file name (the
        // `elbv2` / `elasticloadbalancingv2` shape) lands here: name the
        // mismatch rather than reporting a stale copy.
        let b = match std::fs::read(&source) {
            Ok(b) => b,
            Err(e) => {
                println!(
                    "  [x] {service}: no model at {} ({e}) -- does the crate suffix match the aws-models file name?",
                    source.display()
                );
                stale.push(service.clone());
                continue;
            }
        };
        if a == b {
            println!("  [ok] {service}");
        } else {
            println!(
                "  [x] {service}: {} differs from {}",
                copy.display(),
                source.display()
            );
            stale.push(service.clone());
        }
    }

    if stale.is_empty() {
        println!(
            "\nPASS: all {} vendored models match aws-models",
            vendored.len()
        );
        return true;
    }
    println!(
        "\nFAIL: {} vendored model(s) stale: {}",
        stale.len(),
        stale.join(", ")
    );
    println!("Fix with: cp aws-models/<service>.json crates/fakecloud-<service>/model.json");
    false
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        CliCommand::Operations => cmd_operations(&cli.models_dir),
        CliCommand::Checksums => cmd_checksums(&cli.models_dir),
        CliCommand::Run {
            services,
            format,
            endpoint,
        } => cmd_run(&cli.models_dir, services, &format, endpoint),
        CliCommand::Audit => {
            let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..");
            let pass = fakecloud_conformance::audit::run_audit(&project_root);
            if !pass {
                std::process::exit(1);
            }
        }
        CliCommand::VendoredModels => {
            let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..");
            if !cmd_vendored_models(&project_root, &cli.models_dir) {
                std::process::exit(1);
            }
        }
        CliCommand::Check {
            baseline,
            endpoint,
            json_out,
            markdown_summary_out,
        } => cmd_check(
            &cli.models_dir,
            &baseline,
            endpoint,
            json_out.as_deref(),
            markdown_summary_out.as_deref(),
        ),
        CliCommand::UpdateBaseline { baseline, endpoint } => {
            cmd_update_baseline(&cli.models_dir, &baseline, endpoint)
        }
    }
}

fn cmd_operations(models_dir: &std::path::Path) {
    let models = load_models(models_dir);

    let mut total_ops = 0;
    for (service_name, model) in &models {
        let count = model.operations.len();
        total_ops += count;
        println!("{} ({} operations)", service_name, count);
        for op in &model.operations {
            let input_members = op
                .input_shape
                .as_ref()
                .and_then(|id| model.shapes.get(id))
                .map(|s| match &s.shape_type {
                    smithy::ShapeType::Structure { members } => members
                        .iter()
                        .filter(|m| m.required)
                        .map(|m| m.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    _ => String::new(),
                })
                .unwrap_or_default();

            if input_members.is_empty() {
                println!("  {}", op.name);
            } else {
                println!("  {} (required: {})", op.name, input_members);
            }
        }
        println!();
    }
    println!(
        "Total: {} operations across {} services",
        total_ops,
        models.len()
    );
}

fn cmd_checksums(models_dir: &std::path::Path) {
    let models = checksum::model_checksums(models_dir).unwrap_or_else(|e| {
        eprintln!("Error computing checksums: {}", e);
        std::process::exit(1);
    });

    for model in &models {
        println!("{}:", model.service_name);
        for (op, cs) in &model.operations {
            println!("  {}  {}", cs, op);
        }
        println!();
    }
}

/// Run probes and return the report data.
fn run_probes(
    models_dir: &std::path::Path,
    services_filter: Option<String>,
    endpoint: Option<String>,
) -> report::ConformanceReport {
    let models = load_models(models_dir);

    let filter: Option<Vec<String>> =
        services_filter.map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

    let (endpoint, _server) = if let Some(ep) = endpoint {
        (ep, None)
    } else {
        let (ep, child) = start_fakecloud();
        (ep, Some(ChildGuard(child)))
    };
    // Variants re-probed against a container-backed server (see
    // `needs_container_runtime`). Only when we own the sweep server: against
    // an operator-supplied endpoint we cannot choose its runtime.
    let may_reprobe = _server.is_some();
    let mut reprobe_cli: Option<Option<String>> = None;

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client");
    let seed_context = SeedContext::new(may_reprobe, caller_account(&client, &endpoint));

    // Cross-reference Smithy operations against fakecloud's own
    // `fn supported_actions()` lists. Operations not in a service's SUPPORTED
    // set are classified as NotImplemented without sending a probe. This
    // prevents fakecloud's grab-bag of unrouted-path responses
    // (NotFoundException, UnknownOperationException, execute-api stage errors,
    // generic-handler stubs, etc.) from being counted as "routed successes"
    // under the lenient 2xx-4xx = Pass rule.
    //
    // The SUPPORTED list is fakecloud's own source of truth for "we implement
    // this action." Treating it as authoritative gives honest per-service
    // coverage numbers and surfaces real feature gaps — the same numbers the
    // Level-2 audit uses. If a service routes actions without listing them
    // in SUPPORTED (a known under-reporting pattern in some older services),
    // the right fix is to update SUPPORTED, not to paper over in the probe.
    let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    let implemented_actions = fakecloud_conformance::audit::scan_implemented_actions(&project_root)
        .unwrap_or_else(|e| {
            eprintln!("Warning: failed to scan implemented actions: {e}");
            HashMap::new()
        });
    let implemented_tags = fakecloud_conformance::audit::audit_service_tags(&project_root);

    let mut all_results: HashMap<String, HashMap<String, Vec<probe::ProbeResult>>> = HashMap::new();
    let mut total_ops_per_service: HashMap<String, usize> = HashMap::new();

    for (service_name, model) in &models {
        if let Some(ref filter) = filter {
            if !filter.contains(service_name) {
                continue;
            }
        }

        total_ops_per_service.insert(service_name.clone(), model.operations.len());

        eprintln!(
            "Probing {} ({} operations)...",
            service_name,
            model.operations.len()
        );

        let mut service_results: HashMap<String, Vec<probe::ProbeResult>> = HashMap::new();

        // Union SUPPORTED lists across every audit mapping whose tag list
        // includes this Smithy service_name. Covers shared-crate services
        // (bedrock + bedrock-runtime share one SUPPORTED list across two
        // Smithy service keys).
        let supported_for_service: Option<Vec<String>> = {
            let mut combined: Vec<String> = Vec::new();
            let mut any = false;
            for (audit_key, tags) in &implemented_tags {
                if tags.iter().any(|t| t == service_name) {
                    if let Some(list) = implemented_actions.get(audit_key) {
                        combined.extend(list.iter().cloned());
                        any = true;
                    }
                }
            }
            if any {
                combined.sort();
                combined.dedup();
                Some(combined)
            } else {
                // Smithy service not yet in audit mapping — preserve the
                // legacy probe-and-classify behavior so nothing regresses.
                None
            }
        };

        for (op_index, op) in model.operations.iter().enumerate() {
            // Re-seed before every op so destructive ops (DeleteBucket /
            // DeleteObject) don't leave later ops staring at a missing
            // resource. Cheap: each call is a single keep-alive request
            // against the in-process server. Against an operator-supplied
            // endpoint the seed is torn down when `seed` drops at the end of
            // this iteration.
            let seed = seed_service_resources(
                &client,
                &endpoint,
                service_name,
                &op.name,
                &seed_context.for_operation(op_index),
            );
            let variants = seeded_variants(model, &op.name, &seed);

            // If fakecloud's SUPPORTED list is known and this op isn't in it,
            // short-circuit every variant to NotImplemented. Saves the
            // network round-trip and, more importantly, gives an honest
            // per-service coverage number.
            if let Some(supported) = supported_for_service.as_ref() {
                if !supported.iter().any(|s| s == &op.name) {
                    let results: Vec<probe::ProbeResult> = variants
                        .iter()
                        .map(|v| probe::ProbeResult {
                            variant_name: v.name.clone(),
                            status: probe::ProbeStatus::NotImplemented,
                            http_status: 0,
                            response_body: String::new(),
                            duration_ms: 0,
                        })
                        .collect();
                    let total = results.len();
                    eprintln!("  SKIP {} (0/{})", op.name, total);
                    service_results.insert(op.name.clone(), results);
                    continue;
                }
            }

            // Per-op seed for state one operation's documented `@examples`
            // assume. The guard undoes it when this iteration ends -- normally,
            // or by unwinding from a panicking variant thread -- so no other op
            // sees it.
            let _op_seed = seed_operation_state(&client, &endpoint, service_name, &op.name);

            // Get output shape for shape validation
            let output_shape_id = op.output_shape.as_deref();

            // Probe variants with bounded concurrency to avoid overwhelming
            // fakecloud. A round-trip variant writes and then reads back the
            // same resource, so it runs alone, after the others: a concurrent
            // variant writing that resource between its two steps would make
            // it read someone else's value.
            let max_concurrent = 8;
            let probe_one = |variant: &generators::TestVariant| {
                probe::probe_variant_with_model(
                    &client,
                    &endpoint,
                    service_name,
                    &op.name,
                    variant,
                    output_shape_id.map(|oid| (model, oid)),
                )
            };
            // An operation that removes the seeded resource (Lambda's
            // DeleteAlias, RemovePermission, ...) succeeds once: the first
            // variant removes it and every later one, concurrent or not,
            // only finds it gone. Each of its variants runs alone on a seed
            // of its own instead.
            let reseed_each = seed.reseed_each_variant;
            let (round_trips, concurrent): (Vec<usize>, Vec<usize>) =
                (0..variants.len()).partition(|&i| reseed_each || variants[i].followup.is_some());
            let mut slots: Vec<Option<probe::ProbeResult>> =
                variants.iter().map(|_| None).collect();
            for chunk in concurrent.chunks(max_concurrent) {
                std::thread::scope(|s| {
                    let handles: Vec<_> = chunk
                        .iter()
                        .map(|&i| {
                            let probe_one = &probe_one;
                            let variant = &variants[i];
                            (i, s.spawn(move || probe_one(variant)))
                        })
                        .collect();
                    for (i, h) in handles {
                        slots[i] = Some(h.join().unwrap());
                    }
                });
            }
            for i in round_trips {
                slots[i] = Some(if reseed_each {
                    let own = seed_service_resources(
                        &client,
                        &endpoint,
                        service_name,
                        &op.name,
                        &seed_context.for_operation(op_index),
                    );
                    let rebuilt =
                        rebind_variants(&[&variants[i]], seeded_variants(model, &op.name, &own));
                    probe_one(&rebuilt[0])
                } else {
                    probe_one(&variants[i])
                });
            }
            let mut op_results: Vec<probe::ProbeResult> = slots
                .into_iter()
                .map(|r| r.expect("every variant is probed"))
                .collect();

            if may_reprobe {
                let pending: Vec<usize> = op_results
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| needs_container_runtime(r))
                    .map(|(i, _)| i)
                    .collect();
                let cli = if pending.is_empty() {
                    None
                } else {
                    reprobe_cli
                        .get_or_insert_with(|| {
                            let cli = reprobe_container_cli(
                                std::env::var_os("FAKECLOUD_CONTAINER_CLI"),
                                fakecloud_core::container_net::detect_container_cli,
                            );
                            if cli.is_none() {
                                eprintln!(
                                    "  note: some variants need a container runtime and none \
                                     is available; they are reported as failures"
                                );
                            }
                            cli
                        })
                        .as_deref()
                };
                if let Some(cli) = cli {
                    for batch in reprobe_batches(service_name, &pending) {
                        let batch_variants: Vec<&generators::TestVariant> =
                            batch.iter().map(|&i| &variants[i]).collect();
                        for v in &batch_variants {
                            eprintln!(
                                "  RETRY {} {} against a {}-backed server",
                                op.name, v.name, cli
                            );
                        }
                        let results = probe_with_container_runtime(
                            cli,
                            &client,
                            &seed_context,
                            ServiceOperation {
                                service_name,
                                model,
                                op_name: &op.name,
                                op_index,
                                output_shape_id,
                            },
                            &batch_variants,
                        );
                        for (&i, r) in batch.iter().zip(results) {
                            op_results[i] = r;
                        }
                    }
                }
            }

            let passed = op_results
                .iter()
                .filter(|r| r.status == probe::ProbeStatus::Pass)
                .count();
            let total = op_results.len();
            let marker = if op_results
                .iter()
                .all(|r| r.status == probe::ProbeStatus::NotImplemented)
            {
                "SKIP"
            } else if passed == total {
                "OK"
            } else {
                "FAIL"
            };
            eprintln!("  {} {} ({}/{})", marker, op.name, passed, total);

            service_results.insert(op.name.clone(), op_results);
        }

        all_results.insert(service_name.clone(), service_results);
    }

    report::build_report(all_results, &total_ops_per_service)
}

fn cmd_run(
    models_dir: &std::path::Path,
    services_filter: Option<String>,
    format: &str,
    endpoint: Option<String>,
) {
    let report_data = run_probes(models_dir, services_filter, endpoint);
    match format {
        "json" => report::print_json_report(&report_data),
        _ => report::print_text_report(&report_data),
    }
}

/// The identifier values every generated variant of one operation is built
/// with, whether its documented `@examples` use them too, and what to remove
/// once the operation is done.
#[derive(Default)]
struct OperationSeedValues<'a> {
    /// Member name -> value, handed to the generators as `overrides`: every
    /// variant carries it, except one that exercises that member itself (a
    /// boundary, negative or id-form variant of `Bucket` keeps its own value).
    overrides: HashMap<String, serde_json::Value>,
    /// Which of the operation's documented examples also use these members.
    pin_examples: ExamplePins,
    /// Optional members every generated variant sends (with its `overrides`
    /// value) unless it sets the member itself: the generators only fill in
    /// required members, but some operations do nothing useful without an
    /// optional one (CreateEventSourceMapping without `EventSourceArn`).
    include: Vec<&'static str>,
    /// The operation removes what the seed created, so each variant gets a
    /// fresh seed and runs alone.
    reseed_each_variant: bool,
    /// Removes the seeded resources when dropped (operator endpoints only).
    teardown: Option<SeedTeardown<'a>>,
}

/// What every operation's seed shares: whether the probe owns the server,
/// the account the probe's credentials resolve to (resolved once, after the
/// server is up), and a token naming this run's resources on an
/// operator-supplied endpoint.
struct SeedContext {
    owns_server: bool,
    account: Option<String>,
    run_id: String,
}

impl SeedContext {
    fn new(owns_server: bool, account: Option<String>) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self {
            owns_server,
            account,
            run_id: format!(
                "{:08x}",
                (nanos as u64 ^ u64::from(std::process::id())) as u32
            ),
        }
    }

    fn for_operation(&self, op_index: usize) -> OperationSeedContext<'_> {
        OperationSeedContext {
            owns_server: self.owns_server,
            account: self.account.as_deref(),
            names: SeedNames::new(self.owns_server, &self.run_id, op_index),
        }
    }
}

/// [`SeedContext`] narrowed to one operation.
struct OperationSeedContext<'a> {
    owns_server: bool,
    account: Option<&'a str>,
    names: SeedNames,
}

/// The names one operation's seed creates.
///
/// On a server the probe owns these are fixed: the service is reset (or its
/// seed re-created) before every operation, so nothing carries over. Against
/// an operator-supplied endpoint nothing is reset, so each operation gets
/// names of its own: a bucket that already exists from an earlier operation
/// would keep that operation's changes and would never pick up this one's
/// creation-time setup (Object Lock can only be enabled when a bucket is
/// created).
#[derive(Debug, PartialEq)]
struct SeedNames {
    bucket: String,
    /// A bucket nothing creates: CreateBucket's variants create it.
    new_bucket: String,
    function: String,
    /// Lambda's seeded layer, event-source queue and capacity provider.
    layer: String,
    queue: String,
    capacity_provider: String,
}

impl SeedNames {
    fn new(owns_server: bool, run_id: &str, op_index: usize) -> Self {
        if owns_server {
            Self {
                bucket: S3_SEED_BUCKET.to_string(),
                new_bucket: S3_NEW_BUCKET.to_string(),
                function: LAMBDA_SEED_FUNCTION.to_string(),
                layer: lambda_seed::LAYER.to_string(),
                queue: lambda_seed::QUEUE.to_string(),
                capacity_provider: lambda_seed::CAPACITY_PROVIDER.to_string(),
            }
        } else {
            let unique = |base: &str| format!("{base}-{run_id}-{op_index}");
            Self {
                bucket: unique(S3_SEED_BUCKET),
                new_bucket: unique(S3_NEW_BUCKET),
                function: unique(LAMBDA_SEED_FUNCTION),
                layer: unique(lambda_seed::LAYER),
                queue: unique(lambda_seed::QUEUE),
                capacity_provider: unique(lambda_seed::CAPACITY_PROVIDER),
            }
        }
    }
}

/// The resources one operation's seed created that nothing else removes,
/// removed (best effort, in order) when dropped -- after the operation's
/// variants ran, or while unwinding from a panicking one.
struct SeedTeardown<'a> {
    client: &'a reqwest::blocking::Client,
    endpoint: &'a str,
    steps: Vec<TeardownStep>,
}

/// One resource [`SeedTeardown`] removes.
#[derive(Debug, Clone, PartialEq)]
enum TeardownStep {
    /// Empty and delete this bucket.
    S3Bucket(String),
    /// `DELETE` this Lambda path (already percent-encoded).
    Lambda(String),
    /// Delete the SQS queue at this URL.
    SqsQueue(String),
    /// Delete every event source mapping of this Lambda function: the seeded
    /// one, and those CreateEventSourceMapping's variants created.
    EventSourceMappingsOf(String),
}

impl Drop for SeedTeardown<'_> {
    fn drop(&mut self) {
        for step in &self.steps {
            match step {
                TeardownStep::S3Bucket(bucket) => {
                    remove_s3_bucket(self.client, self.endpoint, bucket)
                }
                TeardownStep::Lambda(path) => {
                    let _ = self
                        .client
                        .delete(format!("{}{path}", self.endpoint))
                        .header("Authorization", LAMBDA_SEED_AUTH)
                        .send();
                }
                TeardownStep::SqsQueue(url) => {
                    lambda_seed::delete_queue(self.client, self.endpoint, url)
                }
                TeardownStep::EventSourceMappingsOf(function) => {
                    lambda_seed::delete_event_source_mappings(self.client, self.endpoint, function)
                }
            }
        }
    }
}

/// One operation of one service, as the re-probe needs it.
struct ServiceOperation<'m> {
    service_name: &'m str,
    model: &'m smithy::ServiceModel,
    op_name: &'m str,
    op_index: usize,
    output_shape_id: Option<&'m str>,
}

/// Seed the resources an operation's variants address, and return the
/// identifiers to build them with.
///
/// Without this, success-path variants name whatever the generators make up
/// (`Bucket: "t"`, `FunctionName: "t"`), and the service answers every one of
/// them with its not-found error before any of the operation's own behaviour
/// runs. Those errors are modelled, so the variants pass without testing
/// anything. Seeded identifiers go in through the generators' `overrides`,
/// which keep a variant's intent: a variant about the identifier itself
/// (`negative_omit_Bucket`, `boundary_len_min_FunctionName_1`) still sends its
/// own value.
///
/// When the probe owns the server, every operation starts from the same
/// seeded state, whatever the previous one changed: S3 is reset and the
/// Lambda seed function re-created. Against an operator-supplied endpoint
/// nothing is reset; the seed uses names of its own (see [`SeedNames`]) and is
/// removed again when the returned value drops.
fn seed_service_resources<'a>(
    client: &'a reqwest::blocking::Client,
    endpoint: &'a str,
    service_name: &str,
    op_name: &str,
    ctx: &OperationSeedContext<'_>,
) -> OperationSeedValues<'a> {
    let mut seed = match service_name {
        "s3" => seed_s3(client, endpoint, op_name, ctx),
        "lambda" => lambda_seed::seed(client, endpoint, op_name, ctx),
        "secretsmanager" => {
            seed_secretsmanager(client, endpoint);
            OperationSeedValues::default()
        }
        _ => OperationSeedValues::default(),
    };
    // On a server the probe owns, S3 is reset before every operation; only
    // an operator-supplied endpoint keeps the buckets, so they are removed.
    if service_name == "s3" && !ctx.owns_server {
        seed.teardown = Some(SeedTeardown {
            client,
            endpoint,
            steps: vec![
                TeardownStep::S3Bucket(ctx.names.bucket.clone()),
                TeardownStep::S3Bucket(ctx.names.new_bucket.clone()),
            ],
        })
    }
    seed
}

/// The operation's variants, built with the seed's identifiers.
fn seeded_variants(
    model: &smithy::ServiceModel,
    op_name: &str,
    seed: &OperationSeedValues<'_>,
) -> Vec<generators::TestVariant> {
    let mut variants = generators::generate_all_variants(model, op_name, &seed.overrides);
    pin_example_identifiers(&mut variants, &seed.overrides, seed.pin_examples);
    include_members(&mut variants, &seed.include, &seed.overrides);
    variants
}

/// Add each `include` member (with its `overrides` value) to every generated
/// variant that does not set it. Documented examples keep their own inputs.
fn include_members(
    variants: &mut [generators::TestVariant],
    include: &[&str],
    overrides: &HashMap<String, serde_json::Value>,
) {
    for v in variants.iter_mut().filter(|v| {
        !matches!(
            v.strategy,
            generators::Strategy::Examples | generators::Strategy::ExamplesDiff
        )
    }) {
        if let Some(obj) = v.input.as_object_mut() {
            for member in include {
                if let (false, Some(value)) = (obj.contains_key(*member), overrides.get(*member)) {
                    obj.insert(member.to_string(), value.clone());
                }
            }
        }
    }
}

/// Which documented examples [`pin_example_identifiers`] rewrites.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum ExamplePins {
    /// Examples keep their documented identifiers.
    #[default]
    None,
    /// The example requests (`Examples`), not the output diffs: the diff
    /// compares the documented resource's output, configuration and all, so
    /// against a seed configured otherwise it reports the configuration, not
    /// the operation (Lambda's documented function has an environment and a
    /// KMS key; the seed function has neither).
    Requests,
    /// The example requests and their output diffs.
    All,
}

/// Replace the seeded identifiers in an operation's documented examples.
///
/// Example inputs name the resources of the documentation's own account
/// (`examplebucket`, `HappyFace.jpg`), which never exist on the server. Only
/// members the example sets are replaced, so an example keeps its shape.
fn pin_example_identifiers(
    variants: &mut [generators::TestVariant],
    overrides: &HashMap<String, serde_json::Value>,
    pins: ExamplePins,
) {
    for v in variants.iter_mut().filter(|v| match pins {
        ExamplePins::None => false,
        ExamplePins::Requests => v.strategy == generators::Strategy::Examples,
        ExamplePins::All => matches!(
            v.strategy,
            generators::Strategy::Examples | generators::Strategy::ExamplesDiff
        ),
    }) {
        if let Some(obj) = v.input.as_object_mut() {
            for (member, value) in overrides {
                if let Some(slot) = obj.get_mut(member) {
                    *slot = value.clone();
                }
            }
        }
    }
}

const S3_SEED_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
                            SignedHeaders=host;x-amz-date, Signature=00";
const S3_SEED_BUCKET: &str = "test-conformance-bucket";
const S3_SEED_KEY: &str = "test-key";
/// A bucket name no seed creates: CreateBucket's variants create it.
const S3_NEW_BUCKET: &str = "test-conformance-new-bucket";

/// The account the probe's credentials resolve to (the owner every
/// `ExpectedBucketOwner` check compares against), from STS.
fn caller_account(client: &reqwest::blocking::Client, endpoint: &str) -> Option<String> {
    let body = client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/sts/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=00",
        )
        .body("Action=GetCallerIdentity&Version=2011-06-15")
        .send()
        .ok()?
        .text()
        .ok()?;
    xml_text(&body, "Account")
}

/// The text of the first `<tag>` element in `xml`.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&format!("</{tag}>"))?;
    Some(xml[start..end].to_string())
}

/// Base64 MD5 of the seed object's body (`conformance-seed`): object-lock
/// buckets refuse a PUT without an integrity header.
const S3_SEED_BODY_MD5: &str = "e7d33v2nhROiMhKyoIEAVw==";

/// How the seeded bucket and object are set up for one operation, so its
/// success path has something to act on.
#[derive(Default)]
struct S3SeedState {
    /// The bucket is created with Object Lock (which turns on versioning).
    object_lock: bool,
    /// Versioning is enabled (PutObjectLockConfiguration requires it).
    versioning: bool,
    /// The object carries a GOVERNANCE retention and a legal hold, so the
    /// readers find them.
    retained: bool,
    /// The object is in GLACIER, so RestoreObject has something to restore.
    archived: bool,
}

impl S3SeedState {
    fn for_operation(op_name: &str) -> Self {
        match op_name {
            "GetObjectRetention" | "GetObjectLegalHold" => Self {
                object_lock: true,
                retained: true,
                ..Self::default()
            },
            "PutObjectRetention" | "PutObjectLegalHold" | "GetObjectLockConfiguration" => Self {
                object_lock: true,
                ..Self::default()
            },
            "PutObjectLockConfiguration" => Self {
                versioning: true,
                ..Self::default()
            },
            "RestoreObject" => Self {
                archived: true,
                ..Self::default()
            },
            _ => Self::default(),
        }
    }
}

/// S3: a bucket holding one object (`test-key`) with an in-progress multipart
/// upload of it. Variants address that bucket, that key and its version (`null`
/// in a bucket without versioning), that upload,
/// a copy source of that object, and the real owning account.
///
/// DeleteBucket gets an empty bucket, so its success path can delete it; a
/// non-empty one answers every variant with BucketNotEmpty. CreateBucket's
/// variants name a bucket that does not exist yet.
fn seed_s3<'a>(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    op_name: &str,
    ctx: &OperationSeedContext<'_>,
) -> OperationSeedValues<'a> {
    if ctx.owns_server {
        let _ = client
            .post(format!("{endpoint}/_fakecloud/reset/s3"))
            .send();
    }
    // Best-effort: a failed seed shows up as failing variants, which is where
    // it should be reported.
    let state = S3SeedState::for_operation(op_name);
    let seed_bucket = ctx.names.bucket.as_str();
    let bucket_url = format!("{endpoint}/{seed_bucket}");
    let mut create = client
        .put(&bucket_url)
        .header("Authorization", S3_SEED_AUTH);
    if state.object_lock {
        create = create.header("x-amz-bucket-object-lock-enabled", "true");
    }
    let _ = create.send();
    if state.versioning {
        let _ = client
            .put(format!("{bucket_url}?versioning"))
            .header("Authorization", S3_SEED_AUTH)
            .body(
                "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Status>Enabled</Status></VersioningConfiguration>",
            )
            .send();
    }
    let mut upload_id = None;
    let mut version_id = "null".to_string();
    if op_name != "DeleteBucket" {
        let object_url = format!("{bucket_url}/{S3_SEED_KEY}");
        let body = "conformance-seed";
        let mut put = client
            .put(&object_url)
            .header("Authorization", S3_SEED_AUTH)
            .header("Content-Type", "application/octet-stream")
            .header("Content-MD5", S3_SEED_BODY_MD5);
        if state.archived {
            put = put.header("x-amz-storage-class", "GLACIER");
        }
        if state.retained {
            // The per-operation reset removes it again, retention or not.
            put = put
                .header("x-amz-object-lock-mode", "GOVERNANCE")
                .header(
                    "x-amz-object-lock-retain-until-date",
                    "2099-01-01T00:00:00Z",
                )
                .header("x-amz-object-lock-legal-hold", "ON");
        }
        // The object's version: a versioned (or Object Lock) bucket names it
        // in `x-amz-version-id`; elsewhere it is the `null` version.
        version_id = put
            .body(body)
            .send()
            .ok()
            .and_then(|r| {
                r.headers()
                    .get("x-amz-version-id")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "null".to_string());
        upload_id = client
            .post(format!("{object_url}?uploads"))
            .header("Authorization", S3_SEED_AUTH)
            .send()
            .ok()
            .and_then(|r| r.text().ok())
            .and_then(|body| xml_text(&body, "UploadId"));
    }

    let s = |v: &str| serde_json::Value::String(v.to_string());
    let bucket = if op_name == "CreateBucket" {
        ctx.names.new_bucket.as_str()
    } else {
        seed_bucket
    };
    let mut overrides = HashMap::from([
        ("Bucket".to_string(), s(bucket)),
        ("Key".to_string(), s(S3_SEED_KEY)),
        ("VersionId".to_string(), s(&version_id)),
        (
            "CopySource".to_string(),
            s(&format!("{seed_bucket}/{S3_SEED_KEY}")),
        ),
    ]);
    if let Some(id) = upload_id {
        overrides.insert("UploadId".to_string(), s(&id));
    }
    if let Some(account) = ctx.account {
        overrides.insert("ExpectedBucketOwner".to_string(), s(account));
        overrides.insert("ExpectedSourceBucketOwner".to_string(), s(account));
    }
    OperationSeedValues {
        overrides,
        pin_examples: ExamplePins::All,
        include: Vec::new(),
        reseed_each_variant: false,
        teardown: None,
    }
}

/// Empty and delete `bucket`, best effort: abort its multipart uploads,
/// delete every object version and delete marker (lifting a legal hold and
/// bypassing a GOVERNANCE retention first), then the bucket itself.
fn remove_s3_bucket(client: &reqwest::blocking::Client, endpoint: &str, bucket: &str) {
    let Ok(base) = reqwest::Url::parse(endpoint) else {
        return;
    };
    let url = |key: Option<&str>, query: &[(&str, Option<&str>)]| {
        let mut u = base.clone();
        if let Ok(mut segs) = u.path_segments_mut() {
            segs.pop_if_empty().push(bucket);
            if let Some(key) = key {
                segs.push(key);
            }
        }
        {
            let mut q = u.query_pairs_mut();
            for (k, v) in query {
                match v {
                    Some(v) => q.append_pair(k, v),
                    None => q.append_key_only(k),
                };
            }
        }
        if query.is_empty() {
            u.set_query(None);
        }
        u
    };
    let list = |query: &str| {
        client
            .get(url(None, &[(query, None)]))
            .header("Authorization", S3_SEED_AUTH)
            .send()
            .ok()
            .filter(|r| r.status().is_success())
            .and_then(|r| r.text().ok())
    };
    // Each listing returns up to 1000 entries; a few rounds covers anything a
    // single operation's variants create.
    for _ in 0..10 {
        let Some(body) = list("uploads") else { break };
        let uploads: Vec<(String, String)> = xml_blocks(&body, "Upload")
            .filter_map(|u| {
                Some((
                    xml_text_decoded(u, "Key")?,
                    xml_text_decoded(u, "UploadId")?,
                ))
            })
            .collect();
        if uploads.is_empty() {
            break;
        }
        for (key, id) in uploads {
            let _ = client
                .delete(url(Some(&key), &[("uploadId", Some(&id))]))
                .header("Authorization", S3_SEED_AUTH)
                .send();
        }
    }
    for _ in 0..10 {
        let Some(body) = list("versions") else { break };
        let versions: Vec<(String, String, bool)> = xml_blocks(&body, "Version")
            .map(|v| (v, false))
            .chain(xml_blocks(&body, "DeleteMarker").map(|v| (v, true)))
            .filter_map(|(v, marker)| {
                Some((
                    xml_text_decoded(v, "Key")?,
                    xml_text_decoded(v, "VersionId")?,
                    marker,
                ))
            })
            .collect();
        if versions.is_empty() {
            break;
        }
        for (key, version, marker) in versions {
            if !marker {
                let _ = client
                    .put(url(
                        Some(&key),
                        &[("legal-hold", None), ("versionId", Some(&version))],
                    ))
                    .header("Authorization", S3_SEED_AUTH)
                    .body(
                        "<LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                         <Status>OFF</Status></LegalHold>",
                    )
                    .send();
            }
            let _ = client
                .delete(url(Some(&key), &[("versionId", Some(&version))]))
                .header("Authorization", S3_SEED_AUTH)
                .header("x-amz-bypass-governance-retention", "true")
                .send();
        }
    }
    let _ = client
        .delete(url(None, &[]))
        .header("Authorization", S3_SEED_AUTH)
        .send();
}

/// The contents of every `<tag>...</tag>` element in `xml`.
fn xml_blocks<'x>(xml: &'x str, tag: &str) -> impl Iterator<Item = &'x str> + 'x {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let end = start + rest[start..].find(&close)?;
        let block = &rest[start..end];
        rest = &rest[end + close.len()..];
        Some(block)
    })
}

/// [`xml_text`] with the XML entities S3 escapes in keys decoded.
fn xml_text_decoded(xml: &str, tag: &str) -> Option<String> {
    let raw = xml_text(xml, tag)?;
    Some(
        raw.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&#13;", "\r")
            .replace("&#10;", "\n")
            .replace("&amp;", "&"),
    )
}

const LAMBDA_SEED_FUNCTION: &str = "test-conformance-function";

/// Base64 of a zip holding `index.py`, a handler that echoes its event:
///
/// ```python
/// def handler(event, context):
///     return event
/// ```
const LAMBDA_SEED_ZIP: &str = "UEsDBBQAAAAAAAAAIVi0CHBpLgAAAC4AAAAIAAAAaW5kZXgucHlkZWYgaGFuZGxlcihldmVudCwgY29udGV4dCk6CiAgICByZXR1cm4gZXZlbnQKUEsBAhQDFAAAAAAAAAAhWLQIcGkuAAAALgAAAAgAAAAAAAAAAAAAAIABAAAAAGluZGV4LnB5UEsFBgAAAAABAAEANgAAAFQAAAAAAA==";

/// Pre-seed the secret used by the documented `@examples` for Secrets Manager
/// (`MyTestDatabaseSecret`) with both a description and a resource-based
/// policy. Without this seed, the `GetResourcePolicy` example diff fails
/// because the live response omits `ResourcePolicy` for secrets that have
/// none attached. Best-effort: ignores conflicts when the secret already
/// exists from a previous probe run.
fn seed_secretsmanager(client: &reqwest::blocking::Client, endpoint: &str) {
    let auth = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/secretsmanager/aws4_request, \
                SignedHeaders=host;x-amz-date, Signature=00";
    let send = |target: &str, body: &str| {
        let _ = client
            .post(endpoint)
            .header("Content-Type", "application/x-amz-json-1.1")
            .header("X-Amz-Target", format!("secretsmanager.{}", target))
            .header("Authorization", auth)
            .body(body.to_string())
            .send();
    };
    send(
        "CreateSecret",
        r#"{"Name":"MyTestDatabaseSecret","Description":"My test database secret","SecretString":"conformance-seed"}"#,
    );
    send(
        "PutResourcePolicy",
        r#"{"SecretId":"MyTestDatabaseSecret","ResourcePolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"*\"}]}"}"#,
    );
}

const LAMBDA_SEED_AUTH: &str =
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/lambda/aws4_request, \
                                SignedHeaders=host;x-amz-date, Signature=00";

/// State seeded for a single operation. Dropping it puts back what was there
/// before, so the seed cannot outlive its operation even if probing panics.
struct OperationSeedGuard<'a> {
    client: &'a reqwest::blocking::Client,
    endpoint: &'a str,
    seed: OperationSeed,
}

enum OperationSeed {
    /// `my-function`'s reserved concurrency before the seed (`None`: unset).
    LambdaReservedConcurrency(Option<i64>),
}

impl Drop for OperationSeedGuard<'_> {
    fn drop(&mut self) {
        match self.seed {
            OperationSeed::LambdaReservedConcurrency(previous) => {
                let url = lambda_example_concurrency_url(self.endpoint);
                let _ = match previous {
                    Some(n) => self
                        .client
                        .put(&url)
                        .header("Content-Type", "application/json")
                        .header("Authorization", LAMBDA_SEED_AUTH)
                        .body(format!(r#"{{"ReservedConcurrentExecutions":{n}}}"#))
                        .send(),
                    None => self
                        .client
                        .delete(&url)
                        .header("Authorization", LAMBDA_SEED_AUTH)
                        .send(),
                };
            }
        }
    }
}

fn lambda_example_concurrency_url(endpoint: &str) -> String {
    format!("{endpoint}/2017-10-31/functions/my-function/concurrency")
}

/// Seed state that exactly one operation's documented `@examples` assume,
/// right before that operation's variants run.
///
/// Lambda `GetFunctionConcurrency`: its example reads `my-function` (created
/// by the `CreateFunction` example) expecting `ReservedConcurrentExecutions:
/// 250`. Without a reservation AWS (and fakecloud) answer with an empty body,
/// and `DeleteFunctionConcurrency`'s example, which runs earlier, clears any
/// reservation. The previous setting is recorded so dropping the returned
/// guard leaves every other operation's view of the function unchanged. Best-effort:
/// when `my-function` doesn't exist the seed is a 404 and the restore a no-op.
fn seed_operation_state<'a>(
    client: &'a reqwest::blocking::Client,
    endpoint: &'a str,
    service_name: &str,
    op_name: &str,
) -> Option<OperationSeedGuard<'a>> {
    if (service_name, op_name) != ("lambda", "GetFunctionConcurrency") {
        return None;
    }
    let url = lambda_example_concurrency_url(endpoint);
    let current = client
        .get(&url)
        .header("Authorization", LAMBDA_SEED_AUTH)
        .send()
        .ok()
        .filter(|r| r.status().is_success())?;
    let previous = current
        .json::<serde_json::Value>()
        .ok()
        .and_then(|v| v["ReservedConcurrentExecutions"].as_i64());
    client
        .put(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", LAMBDA_SEED_AUTH)
        .body(r#"{"ReservedConcurrentExecutions":250}"#)
        .send()
        .ok()
        .filter(|r| r.status().is_success())?;
    Some(OperationSeedGuard {
        client,
        endpoint,
        seed: OperationSeed::LambdaReservedConcurrency(previous),
    })
}

fn load_models(models_dir: &std::path::Path) -> Vec<(String, smithy::ServiceModel)> {
    smithy::load_all_models(models_dir).unwrap_or_else(|e| {
        eprintln!("Error loading models: {}", e);
        std::process::exit(1);
    })
}

/// The container-CLI value the probe-sweep server should run with, given the
/// operator's current `FAKECLOUD_CONTAINER_CLI` (as `var_os`). Returns `Some`
/// sentinel binary name to force every container runtime onto its metadata-only
/// path when the operator hasn't pinned a CLI, and `None` (no override) when
/// they have. Kept pure so the precedence is unit-testable without spawning a
/// server. See `start_fakecloud` for why the sweep avoids real containers.
fn disabling_container_cli(current: Option<std::ffi::OsString>) -> Option<&'static str> {
    match current {
        Some(_) => None,
        None => Some("fakecloud-no-container-backend"),
    }
}

/// True when a variant failed only because the sweep server runs without a
/// container runtime: the operation refused with the runtime-required error
/// every container-backed service raises (RDS CreateDBInstance, for one, will
/// not report an instance it cannot back with a real database). Such a
/// variant says nothing about API conformance until it runs where a runtime
/// exists.
///
/// Only a 5xx refusal counts: the hint can also appear as data in a successful
/// response (an ECS task's `stoppedReason`), and a shape failure on such a
/// response is a real finding that a fresh server must not paper over.
fn needs_container_runtime(result: &probe::ProbeResult) -> bool {
    result.status != probe::ProbeStatus::Pass
        && result.http_status >= 500
        && result
            .response_body
            .contains(fakecloud_core::container_net::CONTAINER_RUNTIME_HINT)
}

/// The container CLI to re-probe runtime-dependent variants with. An operator
/// who pinned `FAKECLOUD_CONTAINER_CLI` already chose the sweep server's
/// runtime (a real CLI means nothing failed for lack of one; a bogus one means
/// they opted out), so only an unset variable triggers auto-detection.
fn reprobe_container_cli(
    current: Option<std::ffi::OsString>,
    detect: impl FnOnce() -> Option<String>,
) -> Option<String> {
    match current {
        Some(_) => None,
        None => detect(),
    }
}

/// Group the variants (indexes into the operation's list) that need a
/// container-backed server, one group per server.
///
/// By default every variant gets a fresh server, so variants cannot collide
/// on resource names the way they would on a shared one (two documented
/// examples creating the same DB instance identifier). Lambda's
/// runtime-dependent variants are invocations of the one seeded function and
/// create nothing, so an operation's variants share one server; a fresh
/// server and container cold start per Invoke variant is several minutes of
/// CI time for no isolation gain.
fn reprobe_batches(service_name: &str, pending: &[usize]) -> Vec<Vec<usize>> {
    if service_name == "lambda" {
        if pending.is_empty() {
            Vec::new()
        } else {
            vec![pending.to_vec()]
        }
    } else {
        pending.iter().map(|&i| vec![i]).collect()
    }
}

/// Probe `variants` against a fresh server that drives real containers, then
/// shut that server down gracefully so it removes what it started.
///
/// The fresh server is seeded the way the sweep server was, but what it
/// issued (an upload id, a version id) is its own, so the variants are
/// rebuilt from that seed's values and matched to `variants` by name.
fn probe_with_container_runtime(
    cli: &str,
    client: &reqwest::blocking::Client,
    seed_context: &SeedContext,
    op: ServiceOperation<'_>,
    variants: &[&generators::TestVariant],
) -> Vec<probe::ProbeResult> {
    let ServiceOperation {
        service_name,
        model,
        op_name,
        op_index,
        output_shape_id,
    } = op;
    let model_info = output_shape_id.map(|oid| (model, oid));
    let (endpoint, child) = match try_start_fakecloud(Some(cli), Duration::from_secs(60)) {
        Ok(started) => started,
        Err(e) => {
            eprintln!("  could not start a {cli}-backed server: {e}");
            return variants
                .iter()
                .map(|variant| probe::ProbeResult {
                    variant_name: variant.name.clone(),
                    status: probe::ProbeStatus::UnexpectedResult(format!(
                        "needs a container runtime, but a {cli}-backed server failed to start: {e}"
                    )),
                    http_status: 0,
                    response_body: String::new(),
                    duration_ms: 0,
                })
                .collect();
        }
    };
    let pid = child.id();
    let server = GracefulChild(child);
    let seed = seed_service_resources(
        client,
        &endpoint,
        service_name,
        op_name,
        &seed_context.for_operation(op_index),
    );
    let reseeded = rebind_variants(variants, seeded_variants(model, op_name, &seed));
    let results = reseeded
        .iter()
        .map(|variant| {
            probe::probe_variant_with_model(
                client,
                &endpoint,
                service_name,
                op_name,
                variant,
                model_info,
            )
        })
        .collect();
    // The operation may have started a container in the background (RDS
    // CreateDBInstance returns `creating` and runs the engine afterwards). A
    // container is only tracked for shutdown once its `run` returns, so let
    // the server's container-CLI children finish before asking it to stop.
    wait_for_child_processes(pid, Duration::from_secs(300));
    drop(server);
    remove_labelled_leftovers(cli, pid);
    results
}

/// `variants` as `rebuilt` (the same operation's variants generated again
/// from another seed) has them, matched by name. A variant `rebuilt` lacks is
/// kept as it was.
fn rebind_variants(
    variants: &[&generators::TestVariant],
    rebuilt: Vec<generators::TestVariant>,
) -> Vec<generators::TestVariant> {
    let mut by_name: HashMap<String, generators::TestVariant> =
        rebuilt.into_iter().map(|v| (v.name.clone(), v)).collect();
    variants
        .iter()
        .map(|v| by_name.remove(&v.name).unwrap_or_else(|| (*v).clone()))
        .collect()
}

fn wait_for_child_processes(pid: u32, limit: Duration) {
    let deadline = std::time::Instant::now() + limit;
    let mut quiet = 0;
    while std::time::Instant::now() < deadline && quiet < 4 {
        let busy = ProcessCommand::new("pgrep")
            .arg("-P")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        quiet = if busy { 0 } else { quiet + 1 };
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Remove any container or volume still carrying the stopped server's
/// ownership label. Graceful shutdown already removes what the server tracked;
/// this catches anything created too late for it to see.
fn remove_labelled_leftovers(cli: &str, pid: u32) {
    let label = format!("label=fakecloud-instance=fakecloud-{pid}");
    for (list, remove) in [
        (&["ps", "-aq", "--filter"][..], &["rm", "-f"][..]),
        (
            &["volume", "ls", "-q", "--filter"][..],
            &["volume", "rm", "-f"][..],
        ),
    ] {
        let Ok(out) = ProcessCommand::new(cli).args(list).arg(&label).output() else {
            continue;
        };
        let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .map(str::to_string)
            .collect();
        if !ids.is_empty() {
            let _ = ProcessCommand::new(cli)
                .args(remove)
                .args(&ids)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

/// Start the main sweep server (containers disabled, see `try_start_fakecloud`),
/// exiting the process if it cannot come up: nothing can be probed without it.
fn start_fakecloud() -> (String, Child) {
    try_start_fakecloud(None, Duration::from_secs(60)).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    })
}

/// Start a fakecloud server for the sweep. `container_cli` is `None` for the
/// main sweep server (containers disabled, see below) and `Some(cli)` for the
/// short-lived servers that re-probe runtime-dependent variants, which get a
/// longer `ready_within`: with a runtime enabled the server reaps stale
/// containers before it binds, which can take several bounded CLI calls.
fn try_start_fakecloud(
    container_cli: Option<&str>,
    ready_within: Duration,
) -> Result<(String, Child), String> {
    let port = find_available_port();
    let endpoint = format!("http://127.0.0.1:{}", port);

    let bin = find_binary();

    let mut cmd = ProcessCommand::new(&bin);
    cmd.arg("--addr")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--log-level")
        .arg("error")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // The probe sweep exercises every operation of every service for API-shape
    // conformance; it never asserts on runtime behaviour (that is e2e's job).
    // But container-backed services (EC2 RunInstances, Lambda Invoke, ECS
    // RunTask, RDS/ElastiCache create) would each shell out to Docker and spawn
    // real containers + per-subnet networks for thousands of probe variants.
    // On a CI runner that pulls base images and piles up containers until the
    // host starves and the runner loses contact with GitHub (~2h, the "lost
    // communication with the server" death introduced by EC2 VPC isolation in
    // #1745). Most synchronous API responses are identical with or without a
    // backing container, so we point the container-CLI probe at a binary that
    // does not exist: every runtime's detect_container_cli() returns None and
    // they serve their documented metadata-only path. The few operations that
    // refuse outright without a runtime are re-probed one variant at a time
    // against a container-backed server (`needs_container_runtime`), which
    // bounds the containers a sweep can start. An operator who genuinely
    // wants the sweep to drive real containers can still set
    // FAKECLOUD_CONTAINER_CLI explicitly to override this default.
    if let Some(cli) = container_cli {
        cmd.env("FAKECLOUD_CONTAINER_CLI", cli);
    } else if let Some(sentinel) =
        disabling_container_cli(std::env::var_os("FAKECLOUD_CONTAINER_CLI"))
    {
        cmd.env("FAKECLOUD_CONTAINER_CLI", sentinel);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to start fakecloud ({bin}): {e}"))?;

    let deadline = std::time::Instant::now() + ready_within;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            eprintln!("fakecloud started on {}", endpoint);
            return Ok((endpoint, child));
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("fakecloud exited during startup ({status})"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let _ = child.kill();
    let _ = child.wait();
    Err(format!(
        "fakecloud did not start within {} seconds",
        ready_within.as_secs()
    ))
}

fn find_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind to random port")
        .local_addr()
        .unwrap()
        .port()
}

fn find_binary() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let debug_path = format!("{}/../../target/debug/fakecloud", manifest_dir);
    let release_path = format!("{}/../../target/release/fakecloud", manifest_dir);

    if std::path::Path::new(&debug_path).exists() {
        return debug_path;
    }
    if std::path::Path::new(&release_path).exists() {
        return release_path;
    }

    eprintln!(
        "fakecloud binary not found. Run `cargo build` first.\nLooked in:\n  {}\n  {}",
        debug_path, release_path
    );
    std::process::exit(1);
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Baseline {
    variants_passed: usize,
    total_variants: usize,
    per_service: HashMap<String, ServiceBaseline>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ServiceBaseline {
    passed: usize,
    total: usize,
}

fn cmd_check(
    models_dir: &std::path::Path,
    baseline_path: &std::path::Path,
    endpoint: Option<String>,
    json_out: Option<&std::path::Path>,
    markdown_summary_out: Option<&std::path::Path>,
) {
    let baseline_content = std::fs::read_to_string(baseline_path).unwrap_or_else(|e| {
        eprintln!("Failed to read baseline {}: {}", baseline_path.display(), e);
        std::process::exit(1);
    });
    let baseline: Baseline = serde_json::from_str(&baseline_content).unwrap_or_else(|e| {
        eprintln!("Failed to parse baseline: {}", e);
        std::process::exit(1);
    });

    let report_data = run_probes(models_dir, None, endpoint);

    report::print_text_report(&report_data);

    if let Some(path) = json_out {
        let json = serde_json::to_string_pretty(&report_data).unwrap();
        if let Err(e) = std::fs::write(path, format!("{}\n", json)) {
            eprintln!("Failed to write JSON report {}: {}", path.display(), e);
        }
    }

    // Every implemented service is at 100%, so any failing variant is a
    // regression: report each failing operation.
    let mut regressions = failing_operations(&report_data);

    // Exact per-service ratchet against the baseline: a service can't keep a
    // clean 100% by losing variants, and a gain or a changed variant count
    // means the committed baseline (and every surface quoting it) is stale.
    // No margin by default; a local run can relax drops via
    // FAKECLOUD_CONFORMANCE_FLAKE_MARGIN.
    let flake_margin: usize = std::env::var("FAKECLOUD_CONFORMANCE_FLAKE_MARGIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let current_by_service: HashMap<&str, ServiceBaseline> = report_data
        .services
        .iter()
        .map(|svc| {
            let passed: usize = svc.operations.iter().map(|o| o.passed).sum();
            let total: usize = svc.operations.iter().map(|o| o.total_variants).sum();
            (svc.service_name.as_str(), ServiceBaseline { passed, total })
        })
        .collect();
    regressions.extend(baseline_drift(
        &baseline.per_service,
        &current_by_service,
        flake_margin,
    ));

    if let Some(path) = markdown_summary_out {
        let baseline_passed: HashMap<String, usize> = baseline
            .per_service
            .iter()
            .map(|(k, v)| (k.clone(), v.passed))
            .collect();
        let md = report::render_markdown_summary(
            &report_data,
            baseline.variants_passed,
            baseline.total_variants,
            &baseline_passed,
            &regressions,
        );
        if let Err(e) = std::fs::write(path, md) {
            eprintln!("Failed to write markdown summary {}: {}", path.display(), e);
        }
    }

    if regressions.is_empty() {
        println!("\nConformance check PASSED (every variant passes, results match the baseline exactly)");
        println!(
            "  baseline: {}/{} ({:.1}%)",
            baseline.variants_passed,
            baseline.total_variants,
            baseline.variants_passed as f64 / baseline.total_variants as f64 * 100.0,
        );
        println!(
            "  current:  {}/{} ({:.1}%)",
            report_data.summary.variants_passed,
            report_data.summary.total_variants,
            report_data.summary.variants_passed as f64 / report_data.summary.total_variants as f64
                * 100.0,
        );
    } else {
        eprintln!("\nConformance check FAILED:");
        for r in &regressions {
            eprintln!("  {}", r);
        }
        eprintln!("\nTo update the baseline after intentional changes:");
        eprintln!("  cargo run -p fakecloud-conformance -- update-baseline");
        std::process::exit(1);
    }
}

/// Every difference between the committed per-service baseline and this run,
/// one line per service, sorted by service name. Drops below
/// `baseline - flake_margin` are regressions; a gain, a changed variant total,
/// or a service missing from either side means the baseline is out of date.
fn baseline_drift(
    baseline: &HashMap<String, ServiceBaseline>,
    current: &HashMap<&str, ServiceBaseline>,
    flake_margin: usize,
) -> Vec<String> {
    let mut names: Vec<&str> = baseline
        .keys()
        .map(String::as_str)
        .chain(current.keys().copied())
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut drift = Vec::new();
    for name in names {
        match (baseline.get(name), current.get(name)) {
            (Some(b), None) => drift.push(format!(
                "{name}: in the baseline ({}/{}) but not probed",
                b.passed, b.total
            )),
            (None, Some(c)) => drift.push(format!(
                "{name}: probed ({}/{}) but not in the baseline",
                c.passed, c.total
            )),
            (Some(b), Some(c)) => {
                if c.passed < b.passed.saturating_sub(flake_margin) {
                    drift.push(format!(
                        "{name}: {} -> {} variants passing (lost {}; margin {flake_margin})",
                        b.passed,
                        c.passed,
                        b.passed - c.passed
                    ));
                } else if c.passed > b.passed {
                    drift.push(format!(
                        "{name}: {} -> {} variants passing (gained {}; baseline is stale)",
                        b.passed,
                        c.passed,
                        c.passed - b.passed
                    ));
                }
                if c.total != b.total {
                    drift.push(format!(
                        "{name}: {} -> {} variants generated (baseline is stale)",
                        b.total, c.total
                    ));
                }
            }
            (None, None) => {}
        }
    }
    drift
}

/// One line per operation with a failing variant: `service Op: passed/total`.
fn failing_operations(report_data: &report::ConformanceReport) -> Vec<String> {
    report_data
        .services
        .iter()
        .flat_map(|svc| {
            svc.operations
                .iter()
                // An op with no variants at all is as unchecked as a failing one.
                .filter(|op| op.passed < op.total_variants || op.total_variants == 0)
                .map(|op| {
                    format!(
                        "{} {}: {}/{} variants passing",
                        svc.service_name, op.name, op.passed, op.total_variants
                    )
                })
        })
        .collect()
}

fn cmd_update_baseline(
    models_dir: &std::path::Path,
    baseline_path: &std::path::Path,
    endpoint: Option<String>,
) {
    let report_data = run_probes(models_dir, None, endpoint);

    let mut per_service = HashMap::new();
    for svc in &report_data.services {
        let passed: usize = svc.operations.iter().map(|o| o.passed).sum();
        let total: usize = svc.operations.iter().map(|o| o.total_variants).sum();
        per_service.insert(svc.service_name.clone(), ServiceBaseline { passed, total });
    }

    let baseline = Baseline {
        variants_passed: report_data.summary.variants_passed,
        total_variants: report_data.summary.total_variants,
        per_service,
    };

    let json = serde_json::to_string_pretty(&baseline).unwrap();
    std::fs::write(baseline_path, format!("{}\n", json)).unwrap_or_else(|e| {
        eprintln!("Failed to write baseline: {}", e);
        std::process::exit(1);
    });

    println!("Baseline updated: {}", baseline_path.display());
    println!(
        "  {}/{} variants passing ({:.1}%)",
        baseline.variants_passed,
        baseline.total_variants,
        baseline.variants_passed as f64 / baseline.total_variants as f64 * 100.0,
    );
}

#[cfg(test)]
mod tests {
    fn sb(passed: usize, total: usize) -> ServiceBaseline {
        ServiceBaseline { passed, total }
    }

    #[test]
    fn baseline_matching_exactly_has_no_drift() {
        let baseline = HashMap::from([("sqs".to_string(), sb(10, 10))]);
        let current = HashMap::from([("sqs", sb(10, 10))]);
        assert!(baseline_drift(&baseline, &current, 0).is_empty());
    }

    #[test]
    fn every_baseline_difference_is_drift() {
        let baseline = HashMap::from([
            ("lost".to_string(), sb(10, 10)),
            ("gained".to_string(), sb(10, 10)),
            ("regenerated".to_string(), sb(10, 10)),
            ("gone".to_string(), sb(5, 5)),
        ]);
        let current = HashMap::from([
            ("lost", sb(8, 10)),
            ("gained", sb(11, 11)),
            ("regenerated", sb(10, 12)),
            ("new", sb(3, 3)),
        ]);
        assert_eq!(
            baseline_drift(&baseline, &current, 0),
            vec![
                "gained: 10 -> 11 variants passing (gained 1; baseline is stale)",
                "gained: 10 -> 11 variants generated (baseline is stale)",
                "gone: in the baseline (5/5) but not probed",
                "lost: 10 -> 8 variants passing (lost 2; margin 0)",
                "new: probed (3/3) but not in the baseline",
                "regenerated: 10 -> 12 variants generated (baseline is stale)",
            ]
        );
    }

    #[test]
    fn flake_margin_only_relaxes_drops() {
        let baseline = HashMap::from([
            ("flaky".to_string(), sb(10, 10)),
            ("gained".to_string(), sb(10, 11)),
        ]);
        let current = HashMap::from([("flaky", sb(8, 10)), ("gained", sb(11, 11))]);
        assert_eq!(
            baseline_drift(&baseline, &current, 2),
            vec!["gained: 10 -> 11 variants passing (gained 1; baseline is stale)"]
        );
    }

    #[test]
    fn every_failing_operation_is_a_check_failure() {
        let report_data = report::ConformanceReport {
            services: vec![
                report::ServiceReport {
                    service_name: "sqs".to_string(),
                    total_operations: 2,
                    operations: vec![op("SendMessage", 30, 30), op("DeleteMessageBatch", 21, 22)],
                },
                report::ServiceReport {
                    service_name: "kms".to_string(),
                    total_operations: 2,
                    operations: vec![op("Encrypt", 40, 40), op("Decrypt", 0, 0)],
                },
            ],
            summary: report::Summary {
                total_operations: 3,
                implemented: 3,
                passing: 2,
                failing: 1,
                not_implemented: 0,
                total_variants: 92,
                variants_passed: 91,
                variants_failed: 1,
            },
        };
        assert_eq!(
            failing_operations(&report_data),
            vec![
                "sqs DeleteMessageBatch: 21/22 variants passing".to_string(),
                "kms Decrypt: 0/0 variants passing".to_string(),
            ]
        );
    }
    use super::*;

    fn op(name: &str, passed: usize, total: usize) -> report::OperationReport {
        report::OperationReport {
            name: name.to_string(),
            total_variants: total,
            passed,
            failed: total - passed,
            not_implemented: false,
            crashes: 0,
            failures: Vec::new(),
        }
    }

    /// A one-request-per-connection HTTP stub that answers `GET` with
    /// `get_body` and everything else with an empty 200, recording each
    /// request as `"METHOD path body"`.
    fn concurrency_stub(
        get_body: &'static str,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut len = 0usize;
                loop {
                    let mut h = String::new();
                    reader.read_line(&mut h).unwrap();
                    if h == "\r\n" || h.is_empty() {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                let mut parts = line.split_whitespace();
                let (method, path) = (parts.next().unwrap(), parts.next().unwrap());
                log.lock().unwrap().push(format!(
                    "{method} {path} {}",
                    String::from_utf8_lossy(&body)
                ));
                let reply = if method == "GET" { get_body } else { "{}" };
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                )
                .unwrap();
            }
        });
        (endpoint, seen)
    }

    #[test]
    fn concurrency_seed_only_applies_to_get_function_concurrency() {
        let (endpoint, seen) = concurrency_stub("{}");
        let client = reqwest::blocking::Client::new();
        assert!(seed_operation_state(&client, &endpoint, "lambda", "GetFunction").is_none());
        assert!(seed_operation_state(&client, &endpoint, "s3", "GetFunctionConcurrency").is_none());
        assert!(seen.lock().unwrap().is_empty());
    }

    #[test]
    fn concurrency_seed_is_removed_when_none_was_set() {
        let (endpoint, seen) = concurrency_stub("{}");
        let client = reqwest::blocking::Client::new();
        let guard = seed_operation_state(&client, &endpoint, "lambda", "GetFunctionConcurrency")
            .expect("seeded");
        drop(guard);
        let seen = seen.lock().unwrap().clone();
        let path = "/2017-10-31/functions/my-function/concurrency";
        assert_eq!(
            seen,
            vec![
                format!("GET {path} "),
                format!(r#"PUT {path} {{"ReservedConcurrentExecutions":250}}"#),
                format!("DELETE {path} "),
            ]
        );
    }

    #[test]
    fn concurrency_seed_restores_the_previous_value_even_on_panic() {
        let (endpoint, seen) = concurrency_stub(r#"{"ReservedConcurrentExecutions":7}"#);
        let client = reqwest::blocking::Client::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard =
                seed_operation_state(&client, &endpoint, "lambda", "GetFunctionConcurrency")
                    .expect("seeded");
            panic!("a variant thread panicked");
        }));
        assert!(result.is_err());
        let last = seen.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            last,
            r#"PUT /2017-10-31/functions/my-function/concurrency {"ReservedConcurrentExecutions":7}"#
        );
    }

    #[test]
    fn sweep_disables_containers_when_cli_unset() {
        // Unset operator CLI -> inject the sentinel that no container CLI
        // resolves to, so the probe-sweep server serves metadata-only and
        // never spawns containers that would starve a CI runner.
        let sentinel = disabling_container_cli(None).expect("sentinel injected");
        assert_eq!(sentinel, "fakecloud-no-container-backend");
        // The sentinel must not resolve to a runnable binary, or a runtime's
        // `detect_container_cli()` would treat it as a live CLI and the sweep
        // would still spawn containers. This mirrors core's `cli_available`
        // liveness probe (`<cli> info` succeeds) without taking a dep on it.
        let resolves = ProcessCommand::new(sentinel)
            .arg("info")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(!resolves, "sentinel must not be a runnable container CLI");
    }

    #[test]
    fn sweep_respects_explicit_container_cli() {
        // An operator who pins FAKECLOUD_CONTAINER_CLI keeps it: no override.
        assert_eq!(
            disabling_container_cli(Some(std::ffi::OsString::from("docker"))),
            None
        );
        assert_eq!(
            disabling_container_cli(Some(std::ffi::OsString::from(""))),
            None
        );
    }

    fn result(status: probe::ProbeStatus, body: &str) -> probe::ProbeResult {
        probe::ProbeResult {
            variant_name: "v".to_string(),
            status,
            http_status: 503,
            response_body: body.to_string(),
            duration_ms: 0,
        }
    }

    #[test]
    fn example_identifiers_are_pinned_only_where_the_example_sets_them() {
        let variant = |strategy, input: serde_json::Value| generators::TestVariant {
            name: "v".to_string(),
            strategy,
            input,
            expectation: generators::Expectation::Success,
            expected_output: None,
            followup: None,
        };
        let mut vs = vec![
            variant(
                generators::Strategy::Examples,
                serde_json::json!({"Bucket": "examplebucket", "Key": "HappyFace.jpg"}),
            ),
            variant(
                generators::Strategy::ExamplesDiff,
                serde_json::json!({"Bucket": "examplebucket"}),
            ),
            variant(
                generators::Strategy::Negative,
                serde_json::json!({"Bucket": "t"}),
            ),
        ];
        let overrides = HashMap::from([
            ("Bucket".to_string(), serde_json::json!("seeded")),
            ("Key".to_string(), serde_json::json!("test-key")),
        ]);
        pin_example_identifiers(&mut vs, &overrides, ExamplePins::All);
        assert_eq!(
            vs[0].input,
            serde_json::json!({"Bucket": "seeded", "Key": "test-key"})
        );
        assert_eq!(vs[1].input, serde_json::json!({"Bucket": "seeded"}));
        // Generated variants already carry their overrides from the generators.
        assert_eq!(vs[2].input, serde_json::json!({"Bucket": "t"}));
    }

    #[test]
    fn request_pins_leave_the_example_output_diffs_alone() {
        let variant = |strategy| generators::TestVariant {
            name: "v".to_string(),
            strategy,
            input: serde_json::json!({"FunctionName": "my-function"}),
            expectation: generators::Expectation::Success,
            expected_output: None,
            followup: None,
        };
        let mut vs = vec![
            variant(generators::Strategy::Examples),
            variant(generators::Strategy::ExamplesDiff),
        ];
        let overrides = HashMap::from([("FunctionName".to_string(), serde_json::json!("seeded"))]);
        pin_example_identifiers(&mut vs, &overrides, ExamplePins::Requests);
        assert_eq!(vs[0].input["FunctionName"], "seeded");
        assert_eq!(vs[1].input["FunctionName"], "my-function");
        pin_example_identifiers(&mut vs, &overrides, ExamplePins::None);
        assert_eq!(vs[1].input["FunctionName"], "my-function");
    }

    #[test]
    fn included_members_are_added_where_a_variant_leaves_them_out() {
        let variant = |strategy, input: serde_json::Value| generators::TestVariant {
            name: "v".to_string(),
            strategy,
            input,
            expectation: generators::Expectation::Success,
            expected_output: None,
            followup: None,
        };
        let mut vs = vec![
            variant(
                generators::Strategy::Optionals,
                serde_json::json!({"FunctionName": "f"}),
            ),
            // A variant about the member keeps its own value.
            variant(
                generators::Strategy::Boundary,
                serde_json::json!({"FunctionName": "f", "EventSourceArn": ""}),
            ),
            variant(
                generators::Strategy::Examples,
                serde_json::json!({"FunctionName": "my-function"}),
            ),
        ];
        let overrides = HashMap::from([(
            "EventSourceArn".to_string(),
            serde_json::json!("arn:aws:sqs:us-east-1:123456789012:q"),
        )]);
        include_members(&mut vs, &["EventSourceArn"], &overrides);
        assert_eq!(
            vs[0].input["EventSourceArn"],
            "arn:aws:sqs:us-east-1:123456789012:q"
        );
        assert_eq!(vs[1].input["EventSourceArn"], "");
        assert!(vs[2].input.get("EventSourceArn").is_none());
    }

    #[test]
    fn s3_seed_state_per_operation() {
        assert!(S3SeedState::for_operation("GetObjectRetention").retained);
        assert!(S3SeedState::for_operation("PutObjectLegalHold").object_lock);
        assert!(S3SeedState::for_operation("PutObjectLockConfiguration").versioning);
        assert!(S3SeedState::for_operation("RestoreObject").archived);
        let plain = S3SeedState::for_operation("GetObject");
        assert!(!plain.object_lock && !plain.versioning && !plain.retained && !plain.archived);
    }

    #[test]
    fn seed_names_are_fixed_on_an_owned_server_and_unique_per_operation_elsewhere() {
        let owned = SeedNames::new(true, "run", 3);
        assert_eq!(owned, SeedNames::new(true, "other", 7));
        assert_eq!(owned.bucket, S3_SEED_BUCKET);
        assert_eq!(owned.function, LAMBDA_SEED_FUNCTION);
        let a = SeedNames::new(false, "0badf00d", 3);
        let b = SeedNames::new(false, "0badf00d", 4);
        assert_ne!(a.bucket, b.bucket);
        assert_ne!(a.function, b.function);
        assert_ne!(a.bucket, a.new_bucket);
        assert_ne!(a.layer, b.layer);
        assert_ne!(a.queue, b.queue);
        assert_ne!(a.capacity_provider, b.capacity_provider);
        // Still valid bucket and function names at the largest index.
        let big = SeedNames::new(false, "ffffffff", usize::MAX);
        assert!(big.bucket.len() <= 63 && big.new_bucket.len() <= 63);
        assert!(big.function.len() <= 64);
        assert!(big.layer.len() <= 140 && big.capacity_provider.len() <= 140);
        assert!(big.queue.len() <= 80);
    }

    #[test]
    fn rebind_variants_takes_the_rebuilt_inputs_by_name() {
        let variant = |name: &str, upload: &str| generators::TestVariant {
            name: name.to_string(),
            strategy: generators::Strategy::Examples,
            input: serde_json::json!({"UploadId": upload}),
            expectation: generators::Expectation::Success,
            expected_output: None,
            followup: None,
        };
        let old_a = variant("a", "old");
        let old_b = variant("b", "old");
        let rebound = rebind_variants(
            &[&old_a, &old_b],
            vec![variant("a", "new"), variant("c", "new")],
        );
        assert_eq!(rebound[0].input["UploadId"], "new");
        // Not in the rebuilt set: kept as it was.
        assert_eq!(rebound[1].input["UploadId"], "old");
    }

    #[test]
    fn xml_blocks_iterates_every_element() {
        let xml = "<R><Version><Key>a&amp;b</Key><VersionId>1</VersionId></Version>\
                   <DeleteMarker><Key>c</Key></DeleteMarker>\
                   <Version><Key>d</Key><VersionId>2</VersionId></Version></R>";
        let keys: Vec<String> = xml_blocks(xml, "Version")
            .filter_map(|v| xml_text_decoded(v, "Key"))
            .collect();
        assert_eq!(keys, vec!["a&b".to_string(), "d".to_string()]);
        assert_eq!(xml_blocks(xml, "Upload").count(), 0);
    }

    #[test]
    fn xml_text_reads_the_first_element() {
        assert_eq!(
            xml_text("<R><UploadId>abc</UploadId></R>", "UploadId").as_deref(),
            Some("abc")
        );
        assert_eq!(xml_text("<R/>", "UploadId"), None);
    }

    #[test]
    fn lambda_reprobes_share_one_server_others_get_one_each() {
        assert_eq!(reprobe_batches("lambda", &[1, 4, 7]), vec![vec![1, 4, 7]]);
        assert!(reprobe_batches("lambda", &[]).is_empty());
        assert_eq!(reprobe_batches("rds", &[2, 3]), vec![vec![2], vec![3]]);
    }

    #[test]
    fn runtime_required_failures_are_reprobed() {
        let body = format!(
            "<Error><Code>InsufficientDBInstanceCapacity</Code><Message>Docker/Podman is \
             required for RDS DB instances but is not available. {}</Message></Error>",
            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
        );
        assert!(needs_container_runtime(&result(
            probe::ProbeStatus::UnexpectedResult("Expected success, got HTTP 503".into()),
            &body,
        )));
        // A pass (the documented error variants expect exactly this refusal)
        // and an unrelated failure are left alone.
        assert!(!needs_container_runtime(&result(
            probe::ProbeStatus::Pass,
            &body
        )));
        // The hint as data in a 2xx body (an ECS task's stoppedReason) is a
        // real shape finding, not a missing runtime.
        let mut ok_body = result(probe::ProbeStatus::ShapeMismatch("x".into()), &body);
        ok_body.http_status = 200;
        assert!(!needs_container_runtime(&ok_body));
        assert!(!needs_container_runtime(&result(
            probe::ProbeStatus::UnexpectedResult("Expected success, got HTTP 400".into()),
            "<Error><Code>InvalidParameterValue</Code></Error>",
        )));
    }

    #[test]
    fn reprobe_detects_a_runtime_only_when_the_operator_pinned_none() {
        assert_eq!(
            reprobe_container_cli(None, || Some("docker".to_string())),
            Some("docker".to_string())
        );
        assert_eq!(reprobe_container_cli(None, || None), None);
        // A pinned CLI already decided the sweep server's runtime, so nothing
        // is re-probed (and detection never runs).
        assert_eq!(
            reprobe_container_cli(Some(std::ffi::OsString::from("false")), || {
                panic!("must not detect when the operator pinned a CLI")
            }),
            None
        );
    }
}
