#!/usr/bin/env python3
"""Regenerate the AWS-managed IAM policy catalog.

Source of truth: the `iann0036/iam-dataset` repo, which mirrors the AWS managed
policies (name, ARN, default version, and the verbatim policy *document*) and is
itself generated from `aws iam list-policies --scope AWS` + `get-policy-version`.
We read `aws/managedpolicies/<Name>.json` (each file is self-contained: arn,
name, version, createdate/updatedate, and the real `document`) and emit
`crates/fakecloud-iam/src/managed_policies/catalog.json` in fakecloud's schema.

Run:  python3 scripts/gen-managed-policies.py
(network: shallow-clones the dataset into a temp dir; no AWS creds needed.)
"""
import gzip
import json
import os
import subprocess
import sys
import tempfile

DATASET_REPO = "https://github.com/iann0036/iam-dataset.git"
# The catalog is gzip-compressed (~3.5 MB JSON -> ~350 KB) so embedding the full
# AWS managed-policy set keeps the fakecloud binary lean. fakecloud-iam
# decompresses it once at first use.
OUT = os.path.join(
    os.path.dirname(__file__),
    "..",
    "crates",
    "fakecloud-iam",
    "src",
    "managed_policies",
    "catalog.json.gz",
)


def derive_path(arn: str, name: str) -> str:
    # arn:aws:iam::aws:policy[/service-role|/aws-service-role]/<Name>
    marker = ":policy/"
    i = arn.find(marker)
    if i < 0:
        return "/"
    rest = arn[i + len(marker):]
    if rest.endswith(name):
        prefix = rest[: len(rest) - len(name)]
    else:
        prefix = rest.rsplit("/", 1)[0] + "/"
    return "/" + prefix if not prefix.startswith("/") else prefix


def norm_date(s: str) -> str:
    # iann0036 uses RFC3339 with +00:00; fakecloud's existing entries use ...Z.
    if not s:
        return "2015-01-01T00:00:00Z"
    return s.replace("+00:00", "Z")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--filter=blob:none",
             "--sparse", DATASET_REPO, tmp],
            check=True,
        )
        subprocess.run(["git", "-C", tmp, "sparse-checkout", "set",
                        "aws/managedpolicies"], check=True)
        src_dir = os.path.join(tmp, "aws", "managedpolicies")
        out = []
        for fn in sorted(os.listdir(src_dir)):
            if not fn.endswith(".json"):
                continue
            with open(os.path.join(src_dir, fn)) as f:
                d = json.load(f)
            name = d.get("name")
            arn = d.get("arn")
            doc = d.get("document")
            if not (name and arn and isinstance(doc, dict)):
                continue
            if ":aws:policy/" not in arn:  # AWS-managed only
                continue
            out.append({
                "name": name,
                "arn": arn,
                "path": derive_path(arn, name),
                "defaultVersionId": d.get("version") or "v1",
                "createDate": norm_date(d.get("createdate", "")),
                "description": d.get("description", "") or "",
                # store the document as a compact JSON string (matches schema)
                "document": json.dumps(doc, separators=(",", ":")),
            })

    out.sort(key=lambda p: p["arn"])
    payload = (json.dumps({"policies": out}, separators=(",", ":")) + "\n").encode()
    # Deterministic gzip (mtime=0, no filename) so re-running on unchanged input
    # produces an identical blob — no spurious git churn.
    with open(OUT, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0,
                           compresslevel=9) as f:
            f.write(payload)
    print(f"wrote {len(out)} policies -> {os.path.relpath(OUT)} "
          f"({len(payload)} bytes raw, {os.path.getsize(OUT)} bytes gzipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
