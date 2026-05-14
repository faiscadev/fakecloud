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
use std::time::Duration;

mod checksum;
mod generators;
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
    /// Check conformance results against baseline (fails if coverage drops)
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
    let models = load_models(models_dir);

    for (service_name, model) in &models {
        println!("{}:", service_name);
        for op in &model.operations {
            match checksum::operation_checksum(model, &op.name) {
                Some(cs) => println!("  {}  {}", cs, op.name),
                None => println!("  ????????  {} (error)", op.name),
            }
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

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client");

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

        for op in &model.operations {
            let overrides = HashMap::new();
            let variants = generators::generate_all_variants(model, &op.name, &overrides);

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

            // Get output shape for shape validation
            let output_shape_id = op.output_shape.as_deref();

            // Probe variants with bounded concurrency to avoid overwhelming fakecloud
            let max_concurrent = 8;
            let op_results: Vec<probe::ProbeResult> = variants
                .chunks(max_concurrent)
                .flat_map(|chunk| {
                    std::thread::scope(|s| {
                        let handles: Vec<_> = chunk
                            .iter()
                            .map(|variant| {
                                let client = &client;
                                let endpoint = &endpoint;
                                let service_name = service_name.as_str();
                                let op_name = op.name.as_str();
                                let model_info = output_shape_id.map(|oid| (model, oid));
                                s.spawn(move || {
                                    probe::probe_variant_with_model(
                                        client,
                                        endpoint,
                                        service_name,
                                        op_name,
                                        variant,
                                        model_info,
                                    )
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .map(|h| h.join().unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

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

fn load_models(models_dir: &std::path::Path) -> Vec<(String, smithy::ServiceModel)> {
    smithy::load_all_models(models_dir).unwrap_or_else(|e| {
        eprintln!("Error loading models: {}", e);
        std::process::exit(1);
    })
}

fn start_fakecloud() -> (String, Child) {
    let port = find_available_port();
    let endpoint = format!("http://127.0.0.1:{}", port);

    let bin = find_binary();

    let child = ProcessCommand::new(&bin)
        .arg("--addr")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--log-level")
        .arg("error")
        // Pre-seed the canonical conformance probe identifiers
        // (`test` / `test-conformance-bucket` buckets + `test` / `test-key`
        // objects). Without these, every object-on-bucket variant for ops
        // whose Smithy model doesn't declare `NoSuchBucket` (CopyObject,
        // PutObject, RestoreObject, …) returns an undeclared 404 and the
        // probe correctly classifies it as a failure. The seeded bucket
        // is gated by an env var so production callers never see it.
        .env("FAKECLOUD_SEED_TEST_RESOURCES", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| {
            eprintln!("Failed to start fakecloud ({}): {}", bin, e);
            std::process::exit(1);
        });

    // Wait for server to be ready
    for _ in 0..50 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            eprintln!("fakecloud started on {}", endpoint);
            return (endpoint, child);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("fakecloud did not start within 5 seconds");
    std::process::exit(1);
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

    // Check per-service ratchet
    let mut regressions = Vec::new();

    // Build a map of current results for lookup
    let current_by_service: HashMap<&str, usize> = report_data
        .services
        .iter()
        .map(|svc| {
            let passed: usize = svc.operations.iter().map(|o| o.passed).sum();
            (svc.service_name.as_str(), passed)
        })
        .collect();

    for (svc_name, svc_baseline) in &baseline.per_service {
        let current_passed = current_by_service
            .get(svc_name.as_str())
            .copied()
            .unwrap_or(0);
        if current_passed < svc_baseline.passed {
            regressions.push(format!(
                "{}: {} → {} variants passing (was {}, lost {})",
                svc_name,
                svc_baseline.passed,
                current_passed,
                svc_baseline.passed,
                svc_baseline.passed - current_passed,
            ));
        }
    }

    // Check overall ratchet
    let current_total_passed = report_data.summary.variants_passed;
    if current_total_passed < baseline.variants_passed {
        regressions.push(format!(
            "overall: {} → {} variants passing (lost {})",
            baseline.variants_passed,
            current_total_passed,
            baseline.variants_passed - current_total_passed,
        ));
    }

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
        println!("\nConformance check PASSED (no regressions)");
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
        eprintln!("\nConformance check FAILED — coverage dropped:");
        for r in &regressions {
            eprintln!("  {}", r);
        }
        eprintln!("\nTo update the baseline after intentional changes:");
        eprintln!("  cargo run -p fakecloud-conformance -- update-baseline");
        std::process::exit(1);
    }
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
