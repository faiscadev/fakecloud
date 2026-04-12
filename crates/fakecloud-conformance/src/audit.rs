use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Map from service_name (as used in service-map.json) to the list of source files
/// containing `supported_actions()`.
fn service_source_files(project_root: &Path) -> Vec<(String, Vec<PathBuf>)> {
    // service_name -> (crate_suffix, list of source filenames)
    // For each service, try both `src/<name>.rs` and `src/<name>/mod.rs` to
    // handle services that have been split into sub-module directories.
    let mappings: &[(&str, &str, &[&str])] = &[
        ("sqs", "sqs", &["service.rs"]),
        ("sns", "sns", &["service.rs"]),
        ("events", "eventbridge", &["service.rs"]),
        ("iam", "iam", &["iam_service/mod.rs", "iam_service.rs"]),
        ("sts", "iam", &["sts_service.rs"]),
        ("ssm", "ssm", &["service/mod.rs", "service.rs"]),
        ("s3", "s3", &["service/mod.rs", "service.rs"]),
        ("dynamodb", "dynamodb", &["service/mod.rs", "service.rs"]),
        ("lambda", "lambda", &["service.rs"]),
        ("secretsmanager", "secretsmanager", &["service.rs"]),
        ("logs", "logs", &["service/mod.rs", "service.rs"]),
        ("kms", "kms", &["service.rs"]),
        ("cloudformation", "cloudformation", &["service.rs"]),
        ("ses", "ses", &["service/mod.rs", "service.rs"]),
        ("cognito-idp", "cognito", &["service/mod.rs", "service.rs"]),
    ];

    mappings
        .iter()
        .map(|(service, crate_suffix, files)| {
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
            (service.to_string(), paths)
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

    for (service_name, source_files) in service_source_files(project_root) {
        let mut actions = Vec::new();

        for path in &source_files {
            if !path.exists() {
                eprintln!(
                    "Warning: source file not found for {}: {}",
                    service_name,
                    path.display()
                );
                continue;
            }

            let content = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

            // Find the supported_actions() function body.
            // The signature is: fn supported_actions(&self) -> &[&str] { &[...] }
            // We need to skip past the opening brace to find the array literal.
            if let Some(start) = content.find("fn supported_actions") {
                let after_fn = &content[start..];
                // Find the opening brace of the function body
                if let Some(brace_pos) = after_fn.find('{') {
                    let after_brace = &after_fn[brace_pos + 1..];
                    // Now find the &[ that starts the array literal
                    if let Some(bracket_start) = after_brace.find("&[") {
                        let after_bracket = &after_brace[bracket_start..];
                        // Find the matching ]
                        if let Some(bracket_end) = after_bracket.find(']') {
                            let body = &after_bracket[..bracket_end];
                            for cap in re.captures_iter(body) {
                                actions.push(cap[1].to_string());
                            }
                        }
                    }
                }
            }
        }

        if !actions.is_empty() {
            actions.sort();
            result.insert(service_name, actions);
        }
    }

    Ok(result)
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

    println!("=== Conformance Audit ===");
    println!();

    let mut total_implemented = 0;
    let mut total_missing = 0;

    // Sort services for deterministic output
    let mut services: Vec<&String> = implemented.keys().collect();
    services.sort();

    for service in &services {
        let actions = &implemented[*service];
        let covered_actions = covered.get(*service).cloned().unwrap_or_default();

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
