//! Coverage report formatting (text + JSON).

use serde::Serialize;
use std::collections::HashMap;

use crate::probe::{ProbeResult, ProbeStatus};

#[derive(Debug, Serialize)]
pub struct ConformanceReport {
    pub services: Vec<ServiceReport>,
    pub summary: Summary,
}

#[derive(Debug, Serialize)]
pub struct ServiceReport {
    pub service_name: String,
    pub total_operations: usize,
    pub operations: Vec<OperationReport>,
}

#[derive(Debug, Serialize)]
pub struct OperationReport {
    pub name: String,
    pub total_variants: usize,
    pub passed: usize,
    pub failed: usize,
    pub not_implemented: bool,
    pub crashes: usize,
    pub failures: Vec<FailureDetail>,
}

#[derive(Debug, Serialize)]
pub struct FailureDetail {
    pub variant: String,
    pub status: String,
    pub http_status: u16,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub total_operations: usize,
    pub implemented: usize,
    pub passing: usize,
    pub failing: usize,
    pub not_implemented: usize,
    pub total_variants: usize,
    pub variants_passed: usize,
    pub variants_failed: usize,
}

/// Build a report from probe results grouped by service and operation.
pub fn build_report(
    results: HashMap<String, HashMap<String, Vec<ProbeResult>>>,
    total_ops_per_service: &HashMap<String, usize>,
) -> ConformanceReport {
    let mut services = Vec::new();
    let mut summary = Summary {
        total_operations: 0,
        implemented: 0,
        passing: 0,
        failing: 0,
        not_implemented: 0,
        total_variants: 0,
        variants_passed: 0,
        variants_failed: 0,
    };

    let mut service_names: Vec<_> = results.keys().cloned().collect();
    service_names.sort();

    for service_name in service_names {
        let ops = &results[&service_name];
        let total_ops = total_ops_per_service
            .get(&service_name)
            .copied()
            .unwrap_or(ops.len());
        summary.total_operations += total_ops;

        let mut operations = Vec::new();
        let mut op_names: Vec<_> = ops.keys().cloned().collect();
        op_names.sort();

        for op_name in op_names {
            let probe_results = &ops[&op_name];

            let not_implemented = probe_results.is_empty()
                || probe_results
                    .iter()
                    .all(|r| r.status == ProbeStatus::NotImplemented);

            let passed = probe_results
                .iter()
                .filter(|r| r.status == ProbeStatus::Pass)
                .count();
            let crashes = probe_results
                .iter()
                .filter(|r| matches!(r.status, ProbeStatus::Crash(_)))
                .count();
            let failed = probe_results.len() - passed;

            let failures: Vec<FailureDetail> = probe_results
                .iter()
                .filter(|r| r.status != ProbeStatus::Pass)
                .map(|r| FailureDetail {
                    variant: r.variant_name.clone(),
                    status: r.status.to_string(),
                    http_status: r.http_status,
                })
                .collect();

            summary.total_variants += probe_results.len();
            summary.variants_passed += passed;
            summary.variants_failed += failed;

            if not_implemented {
                summary.not_implemented += 1;
            } else if failed == 0 {
                summary.implemented += 1;
                summary.passing += 1;
            } else {
                summary.implemented += 1;
                summary.failing += 1;
            }

            operations.push(OperationReport {
                name: op_name,
                total_variants: probe_results.len(),
                passed,
                failed,
                not_implemented,
                crashes,
                failures,
            });
        }

        services.push(ServiceReport {
            service_name,
            total_operations: total_ops,
            operations,
        });
    }

    ConformanceReport { services, summary }
}

/// Print the report as text to stdout.
pub fn print_text_report(report: &ConformanceReport) {
    println!("=== FakeCloud Conformance Report ===\n");

    for service in &report.services {
        let implemented: Vec<_> = service
            .operations
            .iter()
            .filter(|o| !o.not_implemented)
            .collect();
        let passing = implemented.iter().filter(|o| o.failed == 0).count();

        println!(
            "{}: {}/{} operations handled, {}/{} fully passing",
            service.service_name,
            implemented.len(),
            service.total_operations,
            passing,
            implemented.len(),
        );

        for op in &service.operations {
            if op.not_implemented {
                println!("  [ ] {}", op.name);
            } else if op.failed == 0 {
                println!(
                    "  [✓] {} ({}/{} variants pass)",
                    op.name, op.passed, op.total_variants
                );
            } else {
                println!(
                    "  [✗] {} ({}/{} variants pass, {} crashes)",
                    op.name, op.passed, op.total_variants, op.crashes
                );
                for failure in &op.failures {
                    println!("      {} — {}", failure.variant, failure.status);
                }
            }
        }
        println!();
    }

    println!("=== Summary ===");
    println!(
        "Operations: {}/{} implemented ({:.1}%), {}/{} fully passing ({:.1}%)",
        report.summary.implemented,
        report.summary.total_operations,
        if report.summary.total_operations > 0 {
            report.summary.implemented as f64 / report.summary.total_operations as f64 * 100.0
        } else {
            0.0
        },
        report.summary.passing,
        report.summary.total_operations,
        if report.summary.total_operations > 0 {
            report.summary.passing as f64 / report.summary.total_operations as f64 * 100.0
        } else {
            0.0
        },
    );
    println!(
        "Variants: {}/{} passed ({:.1}%)",
        report.summary.variants_passed,
        report.summary.total_variants,
        if report.summary.total_variants > 0 {
            report.summary.variants_passed as f64 / report.summary.total_variants as f64 * 100.0
        } else {
            0.0
        },
    );
}

/// Build a compact markdown summary suitable for `$GITHUB_STEP_SUMMARY`
/// (max ~1MB). One row per service plus an overall row; per-variant
/// failure detail is intentionally omitted — the full report.json and
/// report.txt artifacts cover that.
pub fn render_markdown_summary(
    report: &ConformanceReport,
    baseline_passed_total: usize,
    baseline_total: usize,
    baseline_per_service: &HashMap<String, usize>,
    regressions: &[String],
) -> String {
    fn pct(num: usize, den: usize) -> String {
        if den == 0 {
            "—".to_string()
        } else {
            format!("{:.1}%", num as f64 / den as f64 * 100.0)
        }
    }

    let mut out = String::new();
    out.push_str("## Conformance Report\n\n");

    if regressions.is_empty() {
        out.push_str(
            "**Status:** PASSED (every variant passes, results match the baseline exactly)\n\n",
        );
    } else {
        out.push_str("**Status:** FAILED\n\n");
        for r in regressions {
            out.push_str(&format!("- {}\n", r));
        }
        out.push('\n');
    }

    let cur_passed = report.summary.variants_passed;
    let cur_total = report.summary.total_variants;
    out.push_str(&format!(
        "**Overall:** {}/{} variants pass ({}). Baseline: {}/{} ({}). Delta: {:+}.\n\n",
        cur_passed,
        cur_total,
        pct(cur_passed, cur_total),
        baseline_passed_total,
        baseline_total,
        pct(baseline_passed_total, baseline_total),
        cur_passed as i64 - baseline_passed_total as i64,
    ));

    out.push_str("| Service | Ops impl | Ops passing | Variants pass | Pct | Δ vs baseline |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|\n");

    for svc in &report.services {
        let implemented = svc.operations.iter().filter(|o| !o.not_implemented).count();
        let ops_passing = svc
            .operations
            .iter()
            .filter(|o| !o.not_implemented && o.failed == 0)
            .count();
        let v_passed: usize = svc.operations.iter().map(|o| o.passed).sum();
        let v_total: usize = svc.operations.iter().map(|o| o.total_variants).sum();
        let baseline = baseline_per_service
            .get(&svc.service_name)
            .copied()
            .unwrap_or(0);
        let delta = v_passed as i64 - baseline as i64;
        out.push_str(&format!(
            "| {} | {}/{} | {}/{} | {}/{} | {} | {:+} |\n",
            svc.service_name,
            implemented,
            svc.total_operations,
            ops_passing,
            implemented,
            v_passed,
            v_total,
            pct(v_passed, v_total),
            delta,
        ));
    }

    out
}

/// Print the report as JSON to stdout.
pub fn print_json_report(report: &ConformanceReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".to_string())
    );
}
