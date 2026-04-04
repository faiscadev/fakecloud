#!/usr/bin/env python3
"""
Parse Moto per-service test logs and generate a compatibility report.

Reads from tests/compat/moto/results/*.log
Writes to tests/compat/moto/RESULTS.md and stdout.
"""

import re
import sys
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
OUTPUT_FILE = Path(__file__).parent / "RESULTS.md"

# Strip ANSI escape codes
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def parse_pytest_summary(log_path: Path) -> dict:
    """Parse a pytest log file for the summary line.

    Looks for lines like:
        111 failed, 46 passed, 15 skipped, 155 warnings in 17.74s
        = 10 passed in 5.67s =
        = no tests ran in 0.01s =
    """
    text = ANSI_RE.sub("", log_path.read_text(errors="replace"))
    result = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0, "total": 0}

    # Check for collection errors
    collection_errors = len(re.findall(r"ERROR collecting", text))
    if collection_errors:
        result["errors"] = collection_errors

    # Find the summary line (last one in the file)
    # Pytest summary: "X failed, Y passed, Z skipped, W warnings in N.NNs"
    # May or may not be wrapped in "=" characters
    summary_pattern = re.compile(
        r"(?:=+\s*)?((?:\d+\s+\w+(?:,\s*)?)+)\s+in\s+[\d.]+s(?:\s*\([\d:]+\))?\s*(?:=+)?$",
        re.MULTILINE,
    )
    matches = list(summary_pattern.finditer(text))
    if not matches:
        if "no tests ran" in text:
            return result
        if text.strip():
            result["errors"] = max(result["errors"], 1)
        return result

    summary = matches[-1].group(1)

    for key in ("passed", "failed", "skipped"):
        m = re.search(rf"(\d+) {key}", summary)
        if m:
            result[key] = int(m.group(1))

    # "error" or "errors"
    m = re.search(r"(\d+) errors?", summary)
    if m:
        result["errors"] = int(m.group(1))

    result["total"] = (
        result["passed"] + result["failed"] + result["skipped"] + result["errors"]
    )
    return result


def generate_report() -> str:
    if not RESULTS_DIR.exists():
        return "No results directory found. Run the test suite first.\n"

    log_files = sorted(RESULTS_DIR.glob("*.log"))
    # Exclude server.log
    log_files = [f for f in log_files if f.stem != "server"]
    if not log_files:
        return "No log files found. Run the test suite first.\n"

    rows = []
    totals = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0, "total": 0}

    for log_path in log_files:
        service = log_path.stem
        stats = parse_pytest_summary(log_path)
        rows.append((service, stats))
        for key in totals:
            totals[key] += stats[key]

    lines = []
    lines.append("# Moto Compatibility Report")
    lines.append("")
    if totals["total"] > 0:
        lines.append(
            f"**Total: {totals['passed']} passed / {totals['total']} total "
            f"({totals['passed'] / totals['total'] * 100:.1f}% pass rate)**"
        )
    else:
        lines.append("**No tests were executed.**")
    lines.append("")
    lines.append(
        "| Service | Passed | Failed | Errors | Skipped | Total | Pass Rate |"
    )
    lines.append(
        "|---------|--------|--------|--------|---------|-------|-----------|"
    )

    for service, stats in rows:
        total = stats["total"]
        if total > 0:
            rate = f"{stats['passed'] / total * 100:.0f}%"
        else:
            rate = "-"
        lines.append(
            f"| {service} | {stats['passed']} | {stats['failed']} | "
            f"{stats['errors']} | {stats['skipped']} | {total} | {rate} |"
        )

    lines.append("")
    lines.append(
        f"**{len(rows)} services tested.** "
        f"{sum(1 for _, s in rows if s['total'] > 0 and s['passed'] == s['total'])} "
        f"fully passing, "
        f"{sum(1 for _, s in rows if s['total'] > 0 and s['passed'] > 0 and s['passed'] < s['total'])} "
        f"partially passing, "
        f"{sum(1 for _, s in rows if s['total'] > 0 and s['passed'] == 0)} "
        f"fully failing."
    )
    lines.append("")

    return "\n".join(lines)


def main():
    report = generate_report()
    print(report)
    OUTPUT_FILE.write_text(report)
    print(f"\nReport saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
