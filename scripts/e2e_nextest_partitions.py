#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict, dataclass


PACKAGE = "fakecloud-e2e"


@dataclass(frozen=True)
class Partition:
    name: str
    filter: str
    partition: str = ""
    install_podman: bool = False


PARTITIONS = [
    Partition(
        name="general-1",
        filter=f"package({PACKAGE}) and not binary(lambda) and not binary(lambda_invoke)",
        partition="hash:1/2",
    ),
    Partition(
        name="general-2",
        filter=f"package({PACKAGE}) and not binary(lambda) and not binary(lambda_invoke)",
        partition="hash:2/2",
    ),
    Partition(
        name="lambda-api",
        filter="binary(lambda) and not test(lambda_invoke_docker) and not test(lambda_invoke_podman)",
    ),
    Partition(
        name="lambda-runtimes",
        filter="binary(lambda_invoke)",
    ),
    Partition(
        name="lambda-container-clis",
        filter="binary(lambda) and (test(lambda_invoke_docker) | test(lambda_invoke_podman))",
        install_podman=True,
    ),
]


def usage() -> int:
    print(f"usage: {sys.argv[0]} [matrix|check]", file=sys.stderr)
    return 2


def nextest_list(*, filter_expr: str | None = None, partition: str | None = None) -> set[str]:
    cmd = [
        "cargo",
        "nextest",
        "list",
        "-p",
        PACKAGE,
        "--message-format",
        "json",
    ]
    if filter_expr:
        cmd.extend(["-E", filter_expr])
    if partition:
        cmd.extend(["--partition", partition])

    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    payload = parse_json_payload(result.stdout)
    return collect_matching_tests(payload)


def parse_json_payload(stdout: str) -> dict:
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("cargo nextest list did not emit JSON output")


def collect_matching_tests(payload: dict) -> set[str]:
    tests: set[str] = set()
    for suite in payload.get("rust-suites", {}).values():
        if suite.get("package-name") != PACKAGE or suite.get("kind") != "test":
            continue
        binary_id = suite["binary-id"]
        for test_name, testcase in suite.get("testcases", {}).items():
            if testcase.get("filter-match", {}).get("status") != "matches":
                continue
            tests.add(f"{binary_id}::{test_name}")
    return tests


def emit_matrix() -> int:
    print(json.dumps({"include": [asdict(partition) for partition in PARTITIONS]}))
    return 0


def check_partitions() -> int:
    expected = nextest_list()
    seen: dict[str, str] = {}
    overlaps: list[tuple[str, str, str]] = []
    union: set[str] = set()

    print(f"checking {len(PARTITIONS)} nextest E2E partitions against {len(expected)} discovered tests")

    for partition in PARTITIONS:
        tests = nextest_list(filter_expr=partition.filter, partition=partition.partition or None)
        if not tests:
            print(f"partition {partition.name} selected no tests", file=sys.stderr)
            return 1

        print(f"{partition.name}: {len(tests)} tests")
        for test in sorted(tests):
            previous = seen.get(test)
            if previous is not None:
                overlaps.append((test, previous, partition.name))
                continue
            seen[test] = partition.name
        union.update(tests)

    missing = sorted(expected - union)
    extra = sorted(union - expected)

    if overlaps:
        print("overlapping partition assignments detected:", file=sys.stderr)
        for test, first, second in overlaps[:20]:
            print(f"  {test}: {first}, {second}", file=sys.stderr)
        return 1

    if missing:
        print("tests missing from partition definitions:", file=sys.stderr)
        for test in missing[:20]:
            print(f"  {test}", file=sys.stderr)
        return 1

    if extra:
        print("partition definitions selected unexpected tests:", file=sys.stderr)
        for test in extra[:20]:
            print(f"  {test}", file=sys.stderr)
        return 1

    print("all non-ignored fakecloud-e2e tests are covered exactly once")
    return 0


def main() -> int:
    if len(sys.argv) != 2:
        return usage()

    command = sys.argv[1]
    if command == "matrix":
        return emit_matrix()
    if command == "check":
        return check_partitions()
    return usage()


if __name__ == "__main__":
    raise SystemExit(main())
