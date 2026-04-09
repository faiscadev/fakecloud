use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fmt;
use std::process::Command;

use serde_json::{json, Value};

const PACKAGE: &str = "fakecloud-e2e";
const USAGE: &str = "usage: e2e_nextest_partitions [matrix|check]";

#[derive(Clone, Copy)]
struct Partition {
    name: &'static str,
    filter: &'static str,
    partition: Option<&'static str>,
    install_podman: bool,
}

const PARTITIONS: [Partition; 5] = [
    Partition {
        name: "general-1",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:1/2"),
        install_podman: false,
    },
    Partition {
        name: "general-2",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:2/2"),
        install_podman: false,
    },
    Partition {
        name: "lambda-api",
        filter:
            "binary(lambda) and not test(lambda_invoke_docker) and not test(lambda_invoke_podman)",
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes",
        filter: "binary(lambda_invoke)",
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-container-clis",
        filter: "binary(lambda) and (test(lambda_invoke_docker) | test(lambda_invoke_podman))",
        partition: None,
        install_podman: true,
    },
];

#[derive(Debug)]
struct SimpleError(String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SimpleError {}

type DynError = Box<dyn std::error::Error>;

fn main() -> Result<(), DynError> {
    let Some(command) = env::args().nth(1) else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };

    match command.as_str() {
        "matrix" => emit_matrix(),
        "check" => check_partitions(&ShellNextestLister),
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

fn emit_matrix() -> Result<(), DynError> {
    let include: Vec<Value> = PARTITIONS
        .iter()
        .map(|partition| {
            json!({
                "name": partition.name,
                "filter": partition.filter,
                "partition": partition.partition.unwrap_or(""),
                "install_podman": partition.install_podman,
            })
        })
        .collect();
    println!("{}", serde_json::to_string(&json!({ "include": include }))?);
    Ok(())
}

trait NextestLister {
    fn list(
        &self,
        filter_expr: Option<&str>,
        partition: Option<&str>,
    ) -> Result<BTreeSet<String>, DynError>;
}

struct ShellNextestLister;

impl NextestLister for ShellNextestLister {
    fn list(
        &self,
        filter_expr: Option<&str>,
        partition: Option<&str>,
    ) -> Result<BTreeSet<String>, DynError> {
        let mut cmd = Command::new("cargo");
        cmd.args(["nextest", "list", "-p", PACKAGE, "--message-format", "json"]);
        if let Some(filter_expr) = filter_expr {
            cmd.args(["-E", filter_expr]);
        }
        if let Some(partition) = partition {
            cmd.args(["--partition", partition]);
        }

        let output = cmd.output()?;
        if !output.status.success() {
            return Err(SimpleError(format!(
                "cargo nextest list failed with status {}",
                output.status
            ))
            .into());
        }

        let stdout = String::from_utf8(output.stdout)?;
        let payload = parse_json_payload(&stdout)?;
        collect_matching_tests(&payload)
    }
}

fn check_partitions(lister: &dyn NextestLister) -> Result<(), DynError> {
    let expected = lister.list(None, None)?;
    let mut seen = HashMap::<String, &'static str>::new();
    let mut overlaps = Vec::<(String, &'static str, &'static str)>::new();
    let mut union = BTreeSet::<String>::new();

    println!(
        "checking {} nextest E2E partitions against {} discovered tests",
        PARTITIONS.len(),
        expected.len()
    );

    for partition in PARTITIONS {
        let tests = lister.list(Some(partition.filter), partition.partition)?;
        if tests.is_empty() {
            return Err(
                SimpleError(format!("partition {} selected no tests", partition.name)).into(),
            );
        }

        println!("{}: {} tests", partition.name, tests.len());
        for test in &tests {
            if let Some(previous) = seen.insert(test.clone(), partition.name) {
                overlaps.push((test.clone(), previous, partition.name));
            }
        }
        union.extend(tests);
    }

    let missing: Vec<_> = expected.difference(&union).cloned().collect();
    let extra: Vec<_> = union.difference(&expected).cloned().collect();

    if !overlaps.is_empty() {
        eprintln!("overlapping partition assignments detected:");
        for (test, first, second) in overlaps.into_iter().take(20) {
            eprintln!("  {test}: {first}, {second}");
        }
        return Err(SimpleError("partition overlap detected".into()).into());
    }

    if !missing.is_empty() {
        eprintln!("tests missing from partition definitions:");
        for test in missing.into_iter().take(20) {
            eprintln!("  {test}");
        }
        return Err(SimpleError("partition coverage drift detected".into()).into());
    }

    if !extra.is_empty() {
        eprintln!("partition definitions selected unexpected tests:");
        for test in extra.into_iter().take(20) {
            eprintln!("  {test}");
        }
        return Err(SimpleError("unexpected partition selections detected".into()).into());
    }

    println!("all non-ignored fakecloud-e2e tests are covered exactly once");
    Ok(())
}

fn parse_json_payload(stdout: &str) -> Result<Value, DynError> {
    for line in stdout.lines() {
        let line = line.trim();
        if line.starts_with('{') {
            return Ok(serde_json::from_str(line)?);
        }
    }
    Err(SimpleError("cargo nextest list did not emit JSON output".into()).into())
}

fn collect_matching_tests(payload: &Value) -> Result<BTreeSet<String>, DynError> {
    let suites = payload
        .get("rust-suites")
        .and_then(Value::as_object)
        .ok_or_else(|| SimpleError("missing rust-suites in nextest JSON output".into()))?;
    let mut tests = BTreeSet::new();

    for suite in suites.values() {
        let package_name = suite.get("package-name").and_then(Value::as_str);
        let kind = suite.get("kind").and_then(Value::as_str);
        if package_name != Some(PACKAGE) || kind != Some("test") {
            continue;
        }

        let binary_id = suite
            .get("binary-id")
            .and_then(Value::as_str)
            .ok_or_else(|| SimpleError("missing binary-id in nextest JSON output".into()))?;
        let Some(testcases) = suite.get("testcases").and_then(Value::as_object) else {
            continue;
        };

        for (test_name, testcase) in testcases {
            let status = testcase
                .get("filter-match")
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str);
            if status == Some("matches") {
                tests.insert(format!("{binary_id}::{test_name}"));
            }
        }
    }

    Ok(tests)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeLister {
        expected: BTreeSet<String>,
        responses: HashMap<(Option<&'static str>, Option<&'static str>), BTreeSet<String>>,
    }

    impl FakeLister {
        fn with_partitions(
            expected: &[&str],
            partitions: &[((Option<&'static str>, Option<&'static str>), &[&str])],
        ) -> Self {
            let responses = partitions
                .iter()
                .map(|(key, tests)| {
                    (
                        *key,
                        tests
                            .iter()
                            .map(|test| (*test).to_owned())
                            .collect::<BTreeSet<_>>(),
                    )
                })
                .collect();
            Self {
                expected: expected.iter().map(|test| (*test).to_owned()).collect(),
                responses,
            }
        }
    }

    impl NextestLister for FakeLister {
        fn list(
            &self,
            filter_expr: Option<&str>,
            partition: Option<&str>,
        ) -> Result<BTreeSet<String>, DynError> {
            if filter_expr.is_none() && partition.is_none() {
                return Ok(self.expected.clone());
            }
            self.responses
                .get(&(filter_expr, partition))
                .cloned()
                .ok_or_else(|| SimpleError("missing fake nextest response".into()).into())
        }
    }

    fn partition_key(name: &'static str) -> (Option<&'static str>, Option<&'static str>) {
        let partition = PARTITIONS
            .iter()
            .find(|partition| partition.name == name)
            .expect("partition exists");
        (Some(partition.filter), partition.partition)
    }

    #[test]
    fn matrix_output_includes_all_partitions() {
        let include = PARTITIONS
            .iter()
            .map(|partition| {
                json!({
                    "name": partition.name,
                    "filter": partition.filter,
                    "partition": partition.partition.unwrap_or(""),
                    "install_podman": partition.install_podman,
                })
            })
            .collect::<Vec<_>>();

        let payload = json!({ "include": include });
        assert_eq!(
            payload["include"].as_array().unwrap().len(),
            PARTITIONS.len()
        );
    }

    #[test]
    fn check_partitions_accepts_exact_coverage() {
        let lister = FakeLister::with_partitions(
            &["a", "b", "c", "d", "e"],
            &[
                (partition_key("general-1"), &["a"]),
                (partition_key("general-2"), &["b"]),
                (partition_key("lambda-api"), &["c"]),
                (partition_key("lambda-runtimes"), &["d"]),
                (partition_key("lambda-container-clis"), &["e"]),
            ],
        );

        assert!(check_partitions(&lister).is_ok());
    }

    #[test]
    fn check_partitions_rejects_missing_tests() {
        let lister = FakeLister::with_partitions(
            &["a", "b", "c", "d", "e", "missing"],
            &[
                (partition_key("general-1"), &["a"]),
                (partition_key("general-2"), &["b"]),
                (partition_key("lambda-api"), &["c"]),
                (partition_key("lambda-runtimes"), &["d"]),
                (partition_key("lambda-container-clis"), &["e"]),
            ],
        );

        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn check_partitions_rejects_overlaps() {
        let lister = FakeLister::with_partitions(
            &["a", "b", "c", "d"],
            &[
                (partition_key("general-1"), &["a"]),
                (partition_key("general-2"), &["a"]),
                (partition_key("lambda-api"), &["b"]),
                (partition_key("lambda-runtimes"), &["c"]),
                (partition_key("lambda-container-clis"), &["d"]),
            ],
        );

        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn check_partitions_rejects_empty_partition() {
        let lister = FakeLister::with_partitions(
            &["a", "b", "c", "d"],
            &[
                (partition_key("general-1"), &["a"]),
                (partition_key("general-2"), &["b"]),
                (partition_key("lambda-api"), &[]),
                (partition_key("lambda-runtimes"), &["c"]),
                (partition_key("lambda-container-clis"), &["d"]),
            ],
        );

        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn parse_json_payload_reads_first_json_line() {
        let payload = parse_json_payload("Compiling\n{\"rust-suites\":{}}\n").unwrap();
        assert_eq!(payload["rust-suites"], json!({}));
    }

    #[test]
    fn collect_matching_tests_filters_to_matching_fakecloud_e2e_tests() {
        let payload = json!({
            "rust-suites": {
                "suite-a": {
                    "package-name": "fakecloud-e2e",
                    "kind": "test",
                    "binary-id": "lambda",
                    "testcases": {
                        "kept": { "filter-match": { "status": "matches" } },
                        "skipped": { "filter-match": { "status": "ignored" } }
                    }
                },
                "suite-b": {
                    "package-name": "other",
                    "kind": "test",
                    "binary-id": "other",
                    "testcases": {
                        "ignored": { "filter-match": { "status": "matches" } }
                    }
                }
            }
        });

        let tests = collect_matching_tests(&payload).unwrap();
        assert_eq!(tests, BTreeSet::from(["lambda::kept".to_owned()]));
    }
}
