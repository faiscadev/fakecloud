#!/usr/bin/env python3
"""Regenerate the CloudFormation resource schema facts fakecloud vendors.

Source of truth: the CloudFormation registry. `DescribeType` returns every
public AWS resource type's schema, and four of its lists decide what a stack
update does to a resource: `createOnlyProperties` (changing one replaces the
resource), `conditionalCreateOnlyProperties` (changing one may; the crate's
allowlist says which do), `primaryIdentifier` and `readOnlyProperties` (which
properties name the resource). fakecloud vendors them as
`crates/fakecloud-cloudformation/data/resource_schema_facts.json.gz`, which
`fakecloud_cloudformation`'s `replacement` module decodes on first use. CI never
calls AWS.

Two steps:

  1. dump: call the registry and write the facts as JSON. Needs credentials
     allowed to call `cloudformation:ListTypes` and
     `cloudformation:DescribeType`. Schemas do not depend on the account; use
     us-east-1, the region the vendored file is taken from. Requires boto3.

       python3 scripts/gen-cfn-resource-schemas.py dump DUMP.json \\
           [--profile PROFILE] [--region us-east-1]

  2. build: turn a dump into the vendored file. Deterministic: types and paths
     sorted, gzip with no timestamp, so rebuilding an unchanged dump produces an
     identical file.

       python3 scripts/gen-cfn-resource-schemas.py build DUMP.json \\
           [--out crates/fakecloud-cloudformation/data/resource_schema_facts.json.gz]

`all` runs both.

Dump schema: {type: {createOnly, conditionalCreateOnly, primary, readOnly}},
property paths with `/` separators and the `/properties/` prefix removed.

Vendored schema (compact JSON, gzipped): {type: {"c": createOnly,
"o": conditionalCreateOnly, "p": primary members that are not read-only,
"r": 1 when a primary member is read-only}}, empty members left out.
"""
import argparse
import gzip
import io
import json
import os
import sys

DEFAULT_OUT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "crates",
    "fakecloud-cloudformation",
    "data",
    "resource_schema_facts.json.gz",
)


def strip(paths):
    return sorted(p.removeprefix("/properties/") for p in paths)


def dump(out_path, profile, region):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    session = boto3.Session(profile_name=profile, region_name=region)
    client = session.client(
        "cloudformation",
        config=Config(retries={"max_attempts": 15, "mode": "adaptive"}),
    )
    names = []
    token = None
    while True:
        kwargs = {
            "Visibility": "PUBLIC",
            "Type": "RESOURCE",
            "Filters": {"Category": "AWS_TYPES"},
            "MaxResults": 100,
        }
        if token:
            kwargs["NextToken"] = token
        page = client.list_types(**kwargs)
        names += [t["TypeName"] for t in page["TypeSummaries"]]
        token = page.get("NextToken")
        if not token:
            break
    facts = {}
    for name in sorted(set(names)):
        try:
            schema = json.loads(
                client.describe_type(Type="RESOURCE", TypeName=name)["Schema"]
            )
        except (ClientError, ValueError) as err:
            raise SystemExit(f"{name}: {err}")
        facts[name] = {
            "createOnly": strip(schema.get("createOnlyProperties", [])),
            "conditionalCreateOnly": strip(
                schema.get("conditionalCreateOnlyProperties", [])
            ),
            "primary": strip(schema.get("primaryIdentifier", [])),
            "readOnly": strip(schema.get("readOnlyProperties", [])),
        }
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(facts, f, indent=0, sort_keys=True)
    print(f"dumped {len(facts)} types -> {out_path}")


def compact(entry):
    read_only = set(entry.get("readOnly", []))
    primary = sorted(entry.get("primary", []))
    out = {}
    create_only = sorted(set(entry.get("createOnly", [])) - read_only)
    conditional = sorted(set(entry.get("conditionalCreateOnly", [])) - read_only)
    writable_primary = [p for p in primary if p not in read_only]
    if create_only:
        out["c"] = create_only
    if conditional:
        out["o"] = conditional
    if writable_primary:
        out["p"] = writable_primary
    if len(writable_primary) != len(primary):
        out["r"] = 1
    return out


def build(dump_path, out_path):
    with open(dump_path, encoding="utf-8") as f:
        facts = json.load(f)
    payload = (
        json.dumps(
            {name: compact(facts[name]) for name in sorted(facts)},
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9
    ) as f:
        f.write(payload)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(buf.getvalue())
    print(
        f"wrote {len(facts)} types -> {os.path.relpath(out_path)} "
        f"({len(payload)} bytes raw, {len(buf.getvalue())} bytes gzipped)"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("dump", "all"):
        cmd = sub.add_parser(name)
        cmd.add_argument("dump_path")
        cmd.add_argument("--profile")
        cmd.add_argument("--region", default="us-east-1")
        if name == "all":
            cmd.add_argument("--out", default=DEFAULT_OUT)
    cmd = sub.add_parser("build")
    cmd.add_argument("dump_path")
    cmd.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()
    if args.cmd in ("dump", "all"):
        dump(args.dump_path, args.profile, args.region)
    if args.cmd in ("build", "all"):
        build(args.dump_path, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
