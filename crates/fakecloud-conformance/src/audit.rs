use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// One entry in the audit's service mapping.
///
/// * `service_name` — the logical audit key (unique per mapping entry).
/// * `source_files` — candidate paths to the crate's `supported_actions()` body.
/// * `test_service_tags` — the service tags we accept on `#[test_action("tag", "Action", ...)]`
///   when looking for coverage. Usually one tag (same as `service_name`), but services that
///   share one crate and one `supported_actions()` list across several AWS service tags
///   (e.g. `bedrock` control plane + `bedrock-runtime` data plane) supply multiple tags so
///   tests annotated under any of them count as coverage.
pub struct AuditMapping {
    pub service_name: String,
    pub source_files: Vec<PathBuf>,
    pub test_service_tags: Vec<String>,
}

/// Mapping from each audited crate to its `supported_actions()` source files and the
/// `#[test_action(...)]` service tags that count as coverage for it.
fn service_source_files(project_root: &Path) -> Vec<AuditMapping> {
    // (service_name, crate_suffix, source_files, test_service_tags)
    // For each service, try both `src/<name>.rs` and `src/<name>/mod.rs` to
    // handle services that have been split into sub-module directories.
    let mappings: &[(&str, &str, &[&str], &[&str])] = &[
        ("sqs", "sqs", &["service/mod.rs", "service.rs"], &["sqs"]),
        ("sns", "sns", &["service/mod.rs", "service.rs"], &["sns"]),
        (
            "events",
            "eventbridge",
            &["service/mod.rs", "service.rs"],
            &["events"],
        ),
        (
            "iam",
            "iam",
            &["iam_service/mod.rs", "iam_service.rs"],
            &["iam"],
        ),
        (
            "sts",
            "iam",
            &["sts_service/mod.rs", "sts_service.rs"],
            &["sts"],
        ),
        ("ssm", "ssm", &["service/mod.rs", "service.rs"], &["ssm"]),
        ("s3", "s3", &["service/mod.rs", "service.rs"], &["s3"]),
        (
            "dynamodb",
            "dynamodb",
            &["service/mod.rs", "service.rs"],
            &["dynamodb"],
        ),
        (
            "lambda",
            "lambda",
            &["service/mod.rs", "service.rs"],
            &["lambda"],
        ),
        (
            "secretsmanager",
            "secretsmanager",
            &["service.rs"],
            &["secretsmanager"],
        ),
        ("logs", "logs", &["service/mod.rs", "service.rs"], &["logs"]),
        ("kms", "kms", &["service.rs"], &["kms"]),
        (
            "cloudformation",
            "cloudformation",
            &["service.rs"],
            &["cloudformation"],
        ),
        ("ses", "ses", &["service/mod.rs", "service.rs"], &["ses"]),
        (
            "cognito-idp",
            "cognito",
            &["service/mod.rs", "service.rs"],
            &["cognito-idp"],
        ),
        (
            "cognito-identity",
            "cognito",
            &["service/identity_pools.rs"],
            &["cognito-identity"],
        ),
        ("rds", "rds", &["service.rs"], &["rds"]),
        ("glue", "glue", &["service.rs"], &["glue"]),
        ("kinesis", "kinesis", &["service.rs"], &["kinesis"]),
        ("firehose", "firehose", &["service.rs"], &["firehose"]),
        // CloudWatch Metrics & Alarms speaks awsQuery under the SigV4
        // service name `monitoring`. SUPPORTED_ACTIONS lives in service.rs;
        // op handlers are split across submodules but the audit only scans
        // the action list, so service.rs is sufficient.
        ("monitoring", "cloudwatch", &["service.rs"], &["monitoring"]),
        (
            "elasticache",
            "elasticache",
            &["service.rs"],
            &["elasticache"],
        ),
        // Step Functions tests historically tagged `sfn`; Smithy / service-map uses `states`.
        (
            "states",
            "stepfunctions",
            &["service/mod.rs", "service.rs"],
            &["states", "sfn"],
        ),
        ("scheduler", "scheduler", &["service.rs"], &["scheduler"]),
        // bedrock crate implements both control-plane (`bedrock`) and data-plane
        // (`bedrock-runtime`) AWS services from a single `supported_actions()` list.
        // Tests for data-plane ops are tagged `bedrock-runtime`, control-plane tests
        // are tagged `bedrock`; either counts.
        (
            "bedrock",
            "bedrock",
            &["service.rs"],
            &["bedrock", "bedrock-runtime"],
        ),
        // API Gateway v1 (REST APIs) and v2 (HTTP APIs) are separate AWS
        // services with different Smithy models but share one SigV4 service
        // name (`apigateway`). Each crate owns its own
        // `supported_actions()` list; tag tests strictly so v1 coverage
        // never double-counts as v2 coverage when an action name happens
        // to exist in both models. The v1 tag list also includes the
        // Smithy `service_name` (`apigatewayv1`) so the auto-probe runner
        // matches it to the v1 crate's `supported_actions()` for gating.
        (
            "apigateway",
            "apigateway",
            &["service.rs"],
            &["apigateway", "apigatewayv1"],
        ),
        (
            "apigatewayv2",
            "apigatewayv2",
            &["service.rs"],
            &["apigatewayv2"],
        ),
        ("ecr", "ecr", &["service.rs"], &["ecr"]),
        ("ecs", "ecs", &["service.rs"], &["ecs"]),
        // ELBv2's Smithy service_name is "elasticloadbalancing"; AWS SDK crate
        // is `elasticloadbalancingv2`. Tests tag `elbv2` for ergonomics.
        (
            "elasticloadbalancing",
            "elbv2",
            &["service.rs"],
            &["elbv2", "elasticloadbalancing", "elasticloadbalancingv2"],
        ),
        ("cloudfront", "cloudfront", &["service.rs"], &["cloudfront"]),
        ("route53", "route53", &["service.rs"], &["route53"]),
        ("acm", "acm", &["service.rs"], &["acm"]),
        (
            "application-autoscaling",
            "application-autoscaling",
            &["service.rs"],
            &["application-autoscaling"],
        ),
        ("wafv2", "wafv2", &["service.rs"], &["wafv2"]),
        ("athena", "athena", &["service.rs"], &["athena"]),
        (
            "organizations",
            "organizations",
            &["service/mod.rs", "service.rs"],
            &["organizations"],
        ),
    ];

    mappings
        .iter()
        .map(|(service, crate_suffix, files, tags)| {
            let paths: Vec<PathBuf> = files
                .iter()
                .map(|f| {
                    project_root
                        .join("crates")
                        .join(format!("fakecloud-{}", crate_suffix))
                        .join("src")
                        .join(f)
                })
                .collect();
            AuditMapping {
                service_name: service.to_string(),
                source_files: paths,
                test_service_tags: tags.iter().map(|t| t.to_string()).collect(),
            }
        })
        .collect()
}

/// Scan Rust source files to extract the list of actions from `supported_actions()` bodies.
///
/// Returns a map of service_name to the list of action name strings found.
pub fn scan_implemented_actions(
    project_root: &Path,
) -> Result<HashMap<String, Vec<String>>, String> {
    let re = Regex::new(r#""([^"]+)""#).unwrap();
    let mut result = HashMap::new();

    for mapping in service_source_files(project_root) {
        let mut actions = Vec::new();

        for path in &mapping.source_files {
            if !path.exists() {
                eprintln!(
                    "Warning: source file not found for {}: {}",
                    mapping.service_name,
                    path.display()
                );
                continue;
            }

            let content = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

            // Find the supported_actions() function body.
            // Two cases:
            // 1. Inline: fn supported_actions(&self) -> &[&str] { &[...] }
            // 2. Const reference: fn supported_actions(...) { CONST_NAME }
            //    where CONST_NAME: &[&str] = &[...] appears elsewhere in the file.
            if let Some(start) = content.find("fn supported_actions") {
                let after_fn = &content[start..];
                // Find the opening brace of the function body
                if let Some(brace_pos) = after_fn.find('{') {
                    let after_brace = &after_fn[brace_pos + 1..];
                    // Find the closing brace to limit our search to the function body
                    if let Some(close_brace_pos) = after_brace.find('}') {
                        let func_body_only = &after_brace[..close_brace_pos];
                        // Now find the &[ that starts the array literal within the function body
                        if let Some(bracket_start) = func_body_only.find("&[") {
                            let after_bracket = &func_body_only[bracket_start..];
                            // Find the matching ]
                            if let Some(bracket_end) = after_bracket.find(']') {
                                let body = &after_bracket[..bracket_end];
                                for cap in re.captures_iter(body) {
                                    actions.push(cap[1].to_string());
                                }
                            }
                        } else {
                            // Case 2: function body references a const.
                            // Extract the const name and search for its definition.
                            let func_body = func_body_only.trim();
                            // func_body should be a single identifier like "SUPPORTED_ACTIONS"
                            if func_body.chars().all(|c| c.is_alphanumeric() || c == '_') {
                                // Search for const CONST_NAME: &[&str] = &[...];
                                let const_pattern = format!("const {}: &[&str] = &[", func_body);
                                if let Some(const_start) = content.find(&const_pattern) {
                                    let after_const = &content[const_start + const_pattern.len()..];
                                    if let Some(bracket_end) = after_const.find("];") {
                                        let body = &after_const[..bracket_end];
                                        for cap in re.captures_iter(body) {
                                            actions.push(cap[1].to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if !actions.is_empty() {
            actions.sort();
            result.insert(mapping.service_name, actions);
        }
    }

    Ok(result)
}

/// Map each logical audit-service key to the list of `#[test_action("tag", ...)]` tags
/// that count as coverage for it. Derived from `service_source_files`.
pub fn audit_service_tags(project_root: &Path) -> HashMap<String, Vec<String>> {
    service_source_files(project_root)
        .into_iter()
        .map(|m| (m.service_name, m.test_service_tags))
        .collect()
}

/// Scan conformance test files for `#[test_action("service", "Action", ...)]` annotations.
///
/// Returns a map of service_name to the list of action names that have tests.
pub fn scan_test_annotations(project_root: &Path) -> Result<HashMap<String, Vec<String>>, String> {
    let tests_dir = project_root
        .join("crates")
        .join("fakecloud-conformance")
        .join("tests");

    if !tests_dir.exists() {
        return Ok(HashMap::new());
    }

    let re = Regex::new(r#"test_action\(\s*"([^"]+)",\s*"([^"]+)""#).unwrap();
    let mut result: HashMap<String, Vec<String>> = HashMap::new();

    for entry in walkdir(&tests_dir)? {
        if entry.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let content = std::fs::read_to_string(&entry)
            .map_err(|e| format!("Failed to read {}: {}", entry.display(), e))?;

        for cap in re.captures_iter(&content) {
            let service = cap[1].to_string();
            let action = cap[2].to_string();
            result.entry(service).or_default().push(action);
        }
    }

    // Sort and deduplicate
    for actions in result.values_mut() {
        actions.sort();
        actions.dedup();
    }

    Ok(result)
}

/// Simple recursive directory walk returning file paths.
fn walkdir(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read directory {}: {}", dir.display(), e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Directory entry error: {}", e))?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walkdir(&path)?);
        } else {
            files.push(path);
        }
    }

    Ok(files)
}

/// Run the Level 2 audit: cross-reference implemented actions with conformance test coverage.
///
/// Returns `true` if all implemented actions have tests, `false` otherwise.
pub fn run_audit(project_root: &Path) -> bool {
    let implemented = match scan_implemented_actions(project_root) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error scanning implemented actions: {}", e);
            return false;
        }
    };

    let covered = match scan_test_annotations(project_root) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error scanning test annotations: {}", e);
            return false;
        }
    };

    let tag_map = audit_service_tags(project_root);

    println!("=== Conformance Audit ===");
    println!();

    let mut total_implemented = 0;
    let mut total_missing = 0;

    // Sort services for deterministic output
    let mut services: Vec<&String> = implemented.keys().collect();
    services.sort();

    for service in &services {
        let actions = &implemented[*service];
        let tags = tag_map
            .get(*service)
            .cloned()
            .unwrap_or_else(|| vec![service.to_string()]);
        let covered_actions: Vec<String> = tags
            .iter()
            .flat_map(|tag| covered.get(tag).cloned().unwrap_or_default())
            .collect();

        let covered_count = actions
            .iter()
            .filter(|a| covered_actions.contains(a))
            .count();
        let total = actions.len();
        let missing_count = total - covered_count;

        total_implemented += total;
        total_missing += missing_count;

        println!(
            "{}: {}/{} implemented actions covered",
            service, covered_count, total
        );

        for action in actions {
            if covered_actions.contains(action) {
                println!("  [\u{2713}] {}", action);
            } else {
                println!("  [\u{2717}] {} (missing test)", action);
            }
        }
        println!();
    }

    println!("=== Result ===");

    if total_missing == 0 {
        println!(
            "PASS: all {} implemented actions have conformance tests",
            total_implemented
        );
        true
    } else {
        println!(
            "FAIL: {} implemented actions missing conformance tests",
            total_missing
        );
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn project_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
    }

    #[test]
    fn test_scan_implemented_actions() {
        let root = project_root();
        let actions = scan_implemented_actions(&root).unwrap();

        // SQS should have actions
        assert!(actions.contains_key("sqs"), "sqs should be present");
        let sqs = &actions["sqs"];
        assert!(sqs.contains(&"CreateQueue".to_string()));
        assert!(sqs.contains(&"SendMessage".to_string()));

        // IAM should have actions
        assert!(actions.contains_key("iam"), "iam should be present");
        let iam = &actions["iam"];
        assert!(iam.contains(&"CreateUser".to_string()));

        // STS should have actions
        assert!(actions.contains_key("sts"), "sts should be present");
        let sts = &actions["sts"];
        assert!(sts.contains(&"GetCallerIdentity".to_string()));
    }

    #[test]
    fn test_scan_test_annotations_empty() {
        let root = project_root();
        // With no test files yet, this should return empty or whatever exists
        let result = scan_test_annotations(&root);
        assert!(result.is_ok());
    }
}
