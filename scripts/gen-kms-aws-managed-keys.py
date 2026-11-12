#!/usr/bin/env python3
"""Regenerate the AWS-managed KMS key catalog from recorded AWS responses.

Source of truth: the recordings in
`crates/fakecloud-kms/tests/fixtures/aws-managed-keys/<service>.json`, one per
AWS-managed key (`alias/aws/<service>`) a real account holds: what
`aws kms describe-key --key-id alias/aws/<service>` and
`aws kms get-key-policy --key-id <key id> --policy-name default` print, with
the account id normalized to 123456789012 (see the README next to them for
how they were captured). fakecloud vendors the result as
`crates/fakecloud-kms/data/aws_managed_keys.json`, which
`fakecloud_kms::aws_managed` reads once at first use: each service's key
description and its key policy text exactly as AWS returns it (recorded in
us-east-1; the account, region and partition are substituted at runtime).

    python3 -I scripts/gen-kms-aws-managed-keys.py

Deterministic: services sorted by name, so an unchanged fixture set produces
an identical file. `predefined` is the list of aliases AWS lists in every
account before their service first uses them (kept here, not derived: the
recordings only show which aliases were listed without a target key).
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(ROOT, "crates/fakecloud-kms/tests/fixtures/aws-managed-keys")
OUT = os.path.join(ROOT, "crates/fakecloud-kms/data/aws_managed_keys.json")

# Listed by `ListAliases` in every account, with no `TargetKeyId` until the
# service first uses them (list-aliases-before.json shows them; aws/s3 and
# aws/ebs were already in use there, and are in the same set).
PREDEFINED = [
    "dynamodb",
    "ebs",
    "elasticfilesystem",
    "es",
    "glue",
    "kinesisvideo",
    "rds",
    "redshift",
    "s3",
    "ssm",
    "xray",
]


def main() -> int:
    keys = {}
    for name in sorted(os.listdir(FIXTURES)):
        path = os.path.join(FIXTURES, name)
        if not name.endswith(".json"):
            continue
        with open(path) as f:
            rec = json.load(f)
        if "get_key_policy" not in rec:
            continue
        service = rec["alias"].removeprefix("alias/aws/")
        keys[service] = {
            "description": rec["describe_key"]["Description"],
            "policy": rec["get_key_policy"]["Policy"],
        }
    missing = [s for s in PREDEFINED if s not in keys]
    if missing:
        print(f"predefined aliases without a recording: {missing}", file=sys.stderr)
        return 1
    out = {
        "_generated_by": "scripts/gen-kms-aws-managed-keys.py",
        "recorded_region": "us-east-1",
        "recorded_account": "123456789012",
        "predefined": PREDEFINED,
        "keys": keys,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {len(keys)} keys to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
