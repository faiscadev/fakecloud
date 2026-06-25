use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fmt;
use std::process::Command;

use serde_json::{json, Value};

const PACKAGE: &str = "fakecloud-e2e";
const USAGE: &str = "usage: e2e_nextest_partitions [matrix|check]";
const LAMBDA_RUNTIME_FAMILY_PARTITIONS: [&str; 6] = [
    "lambda-runtimes-python",
    "lambda-runtimes-nodejs",
    "lambda-runtimes-ruby",
    "lambda-runtimes-provided",
    "lambda-runtimes-java",
    "lambda-runtimes-dotnet",
];

const LAMBDA_RUNTIME_PYTHON_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_python3_11) | ",
    "test(test_invoke_python3_12) | ",
    "test(test_invoke_python3_13) | ",
    "test(test_invoke_python3_14) | ",
    "test(test_invoke_warm_start) | ",
    "test(test_invoke_with_payload) | ",
    "test(test_invoke_with_environment) | ",
    "test(test_invoke_no_code) | ",
    "test(invoke_with_log_type_tail_returns_log_result)",
    ")"
);
const LAMBDA_RUNTIME_NODEJS_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_nodejs18) | ",
    "test(test_invoke_nodejs20) | ",
    "test(test_invoke_nodejs22) | ",
    "test(test_invoke_nodejs24) | ",
    "test(test_invoke_with_response_stream_emits_payload_chunks_and_invoke_complete) | ",
    "test(test_invoke_with_response_stream_surfaces_handler_errors)",
    ")"
);
const LAMBDA_RUNTIME_RUBY_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_ruby3_3) | ",
    "test(test_invoke_ruby3_4)",
    ")"
);
const LAMBDA_RUNTIME_PROVIDED_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_provided_al2) | ",
    "test(test_invoke_provided_al2023)",
    ")"
);
const LAMBDA_RUNTIME_JAVA_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_java17) | ",
    "test(test_invoke_java21) | ",
    "test(test_invoke_java25)",
    ")"
);
const LAMBDA_RUNTIME_DOTNET_FILTER: &str = concat!(
    "binary(lambda_invoke)",
    " and (",
    "test(test_invoke_dotnet8) | ",
    "test(test_invoke_dotnet10)",
    ")"
);

#[derive(Clone, Copy)]
struct Partition {
    name: &'static str,
    filter: &'static str,
    partition: Option<&'static str>,
    install_podman: bool,
}

// Each E2E partition runs prebuilt binaries from a shared nextest archive (no
// per-partition compile), and every partition job waits on the single build
// anchor, so the workflow's wall-clock is `build + slowest partition`. The set
// below is sized so no single partition's test runtime dominates: the heavy
// "general" group fans out 8 ways (~55 min of total test execution), the two
// slowest lambda runtime families (python ~12 min, nodejs ~10 min) are hash-split
// in two, and the container-CLI tests split by runtime (docker vs podman) so the
// podman-only job is the only one paying the podman install.
const PARTITIONS: [Partition; 19] = [
    Partition {
        name: "general-1",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:1/8"),
        install_podman: false,
    },
    Partition {
        name: "general-2",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:2/8"),
        install_podman: false,
    },
    Partition {
        name: "general-3",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:3/8"),
        install_podman: false,
    },
    Partition {
        name: "general-4",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:4/8"),
        install_podman: false,
    },
    Partition {
        name: "general-5",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:5/8"),
        install_podman: false,
    },
    Partition {
        name: "general-6",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:6/8"),
        install_podman: false,
    },
    Partition {
        name: "general-7",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:7/8"),
        install_podman: false,
    },
    Partition {
        name: "general-8",
        filter: "package(fakecloud-e2e) and not binary(lambda) and not binary(lambda_invoke)",
        partition: Some("hash:8/8"),
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
        name: "lambda-runtimes-python-1",
        filter: LAMBDA_RUNTIME_PYTHON_FILTER,
        partition: Some("hash:1/2"),
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-python-2",
        filter: LAMBDA_RUNTIME_PYTHON_FILTER,
        partition: Some("hash:2/2"),
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-nodejs-1",
        filter: LAMBDA_RUNTIME_NODEJS_FILTER,
        partition: Some("hash:1/2"),
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-nodejs-2",
        filter: LAMBDA_RUNTIME_NODEJS_FILTER,
        partition: Some("hash:2/2"),
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-ruby",
        filter: LAMBDA_RUNTIME_RUBY_FILTER,
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-provided",
        filter: LAMBDA_RUNTIME_PROVIDED_FILTER,
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-java",
        filter: LAMBDA_RUNTIME_JAVA_FILTER,
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-runtimes-dotnet",
        filter: LAMBDA_RUNTIME_DOTNET_FILTER,
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-container-docker",
        filter: "binary(lambda) and test(lambda_invoke_docker)",
        partition: None,
        install_podman: false,
    },
    Partition {
        name: "lambda-container-podman",
        filter: "binary(lambda) and test(lambda_invoke_podman)",
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
    validate_partition_layout()?;
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
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SimpleError(format!(
                "cargo nextest list failed with status {}\n---stderr---\n{stderr}",
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
    validate_partition_layout()?;
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

fn validate_partition_layout() -> Result<(), DynError> {
    // Each runtime family must keep at least one dedicated partition. Families
    // whose runtime is slow enough may be hash-split into `<family>-1`,
    // `<family>-2`, ...; matching by prefix accepts both the single-partition
    // and the split form while still catching an accidentally-dropped family.
    for prefix in LAMBDA_RUNTIME_FAMILY_PARTITIONS {
        let matches = |name: &str| name == prefix || name.starts_with(&format!("{prefix}-"));
        if !PARTITIONS.iter().any(|partition| matches(partition.name)) {
            return Err(SimpleError(format!(
                "missing explicit lambda runtime partition {prefix}"
            ))
            .into());
        }
    }

    if PARTITIONS
        .iter()
        .any(|partition| partition.name == "lambda-runtimes")
    {
        return Err(SimpleError(
            "legacy lambda-runtimes partition must stay split by runtime family".into(),
        )
        .into());
    }

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

    type PartitionKey = (Option<&'static str>, Option<&'static str>);
    type PartitionCase = (PartitionKey, &'static [&'static str]);

    struct FakeLister {
        expected: BTreeSet<String>,
        responses: HashMap<PartitionKey, BTreeSet<String>>,
    }

    impl FakeLister {
        fn with_partitions(expected: &[&str], partitions: &[PartitionCase]) -> Self {
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
        validate_partition_layout().unwrap();
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

    // One synthetic test per real partition, in PARTITIONS order:
    // a..h = general-1..8, i = lambda-api, j/k = python-1/2, l/m = nodejs-1/2,
    // n = ruby, o = provided, p = java, q = dotnet, r = docker, s = podman.
    fn exact_coverage_cases() -> Vec<(PartitionKey, &'static [&'static str])> {
        vec![
            (partition_key("general-1"), &["a"]),
            (partition_key("general-2"), &["b"]),
            (partition_key("general-3"), &["c"]),
            (partition_key("general-4"), &["d"]),
            (partition_key("general-5"), &["e"]),
            (partition_key("general-6"), &["f"]),
            (partition_key("general-7"), &["g"]),
            (partition_key("general-8"), &["h"]),
            (partition_key("lambda-api"), &["i"]),
            (partition_key("lambda-runtimes-python-1"), &["j"]),
            (partition_key("lambda-runtimes-python-2"), &["k"]),
            (partition_key("lambda-runtimes-nodejs-1"), &["l"]),
            (partition_key("lambda-runtimes-nodejs-2"), &["m"]),
            (partition_key("lambda-runtimes-ruby"), &["n"]),
            (partition_key("lambda-runtimes-provided"), &["o"]),
            (partition_key("lambda-runtimes-java"), &["p"]),
            (partition_key("lambda-runtimes-dotnet"), &["q"]),
            (partition_key("lambda-container-docker"), &["r"]),
            (partition_key("lambda-container-podman"), &["s"]),
        ]
    }

    const ALL_TESTS: &[&str] = &[
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
        "s",
    ];

    #[test]
    fn check_partitions_accepts_exact_coverage() {
        let lister = FakeLister::with_partitions(ALL_TESTS, &exact_coverage_cases());
        assert!(check_partitions(&lister).is_ok());
    }

    #[test]
    fn check_partitions_rejects_missing_tests() {
        let mut expected = ALL_TESTS.to_vec();
        expected.push("missing");
        let lister = FakeLister::with_partitions(&expected, &exact_coverage_cases());
        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn check_partitions_rejects_overlaps() {
        // general-2 duplicates general-1's "a"; drop "b" from expected so the
        // only discrepancy is the overlap.
        let mut cases = exact_coverage_cases();
        cases[1].1 = &["a"];
        let expected: Vec<&str> = ALL_TESTS.iter().copied().filter(|t| *t != "b").collect();
        let lister = FakeLister::with_partitions(&expected, &cases);
        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn check_partitions_rejects_empty_partition() {
        // lambda-api selects nothing; drop its "i" from expected.
        let mut cases = exact_coverage_cases();
        cases[8].1 = &[];
        let expected: Vec<&str> = ALL_TESTS.iter().copied().filter(|t| *t != "i").collect();
        let lister = FakeLister::with_partitions(&expected, &cases);
        assert!(check_partitions(&lister).is_err());
    }

    #[test]
    fn lambda_runtime_family_partitions_are_explicit() {
        validate_partition_layout().unwrap();
        // Every runtime family is represented by at least one partition (some
        // families are hash-split into `<family>-1`/`-2`, so the partition count
        // is >= the number of families).
        for prefix in LAMBDA_RUNTIME_FAMILY_PARTITIONS {
            assert!(
                PARTITIONS.iter().any(|partition| partition.name == prefix
                    || partition.name.starts_with(&format!("{prefix}-"))),
                "no partition for runtime family {prefix}"
            );
        }
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
