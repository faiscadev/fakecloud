#!/usr/bin/env bash
#
# Tests for scripts/generate-ec2-instance-catalog.py. Hermetic: it builds from small
# local dumps, so it needs neither network nor AWS credentials.
#
#   bash scripts/tests/generate-ec2-instance-catalog-test.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT="$ROOT/scripts/generate-ec2-instance-catalog.py"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cd "$TMP"
mkdir dumps stale

python3 - <<'PY'
import json

def save(path, **doc):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f)

def offerings(*pairs):
    return [{"InstanceType": t, "Location": l} for t, l in pairs]

# `aws` output form: PascalCase, real JSON scalars.
def cli(name, dedicated, classes):
    return {
        "InstanceType": name,
        "DedicatedHostsSupported": dedicated,
        "SupportedUsageClasses": classes,
        "RebootMigrationSupport": "supported",
        "VCpuInfo": {"DefaultVCpus": 2},
        "EbsInfo": {
            "EbsOptimizedInfo": {"BaselineThroughputInMBps": 2375.0},
            "EbsCards": [{"EbsCardIndex": 0}],
        },
        "GpuInfo": {"Gpus": [{"Name": "Gaudi HL-205", "Workloads": ["ml-ai"]}]},
    }

# The raw API's form, as --profile writes it. `supportedInRegion` is per request.
raw_only_west = {
    "instanceType": "b.large",
    "rebootMigrationSupport": "unsupported",
    "supportedInRegion": "true",
}
unsupported = {
    "instanceType": "only-unsupported.large",
    "rebootMigrationSupport": "unsupported",
    "supportedInRegion": "false",
}

save("dumps/us-east-1.types.json", InstanceTypes=[cli("a.large", False, ["on-demand"])])
save("dumps/us-west-2.types.json", InstanceTypes=[cli("a.large", True, ["spot"]), raw_only_west])
# us-east-1 does not sell b.large or only-unsupported.large, and says different things
# about the former than the Region selling it does.
save("dumps/us-east-1.unsupported.json", InstanceTypes=[
    unsupported, cli("a.large", False, ["on-demand"]), dict(raw_only_west, supportedUsageClasses=""),
])
# GetInstanceTypesFromInstanceRequirements: a.large is never listed, c.large is listed
# though us-west-2 does not describe it.
save("dumps/us-east-1.requirements.json", InstanceTypes=[])
save("dumps/us-west-2.requirements.json", InstanceTypes=["b.large", "c.large"])
save("dumps/us-west-2.baseline.json", References={
    "a": ["b.large", "c.large"], "b": ["c.large"], "none": [],
})
save("dumps/us-east-1.baseline.json", References={"a": ["z.large"], "gone": []})
save("dumps/us-east-1.region.json", InstanceTypeOfferings=offerings(("a.large", "us-east-1")))
save(
    "dumps/us-west-2.region.json",
    InstanceTypeOfferings=offerings(("a.large", "us-west-2"), ("b.large", "us-west-2")),
)
save(
    "dumps/us-east-1.az.json",
    InstanceTypeOfferings=offerings(("a.large", "us-east-1c"), ("a.large", "us-east-1a")),
)
save(
    "dumps/us-west-2.az.json",
    InstanceTypeOfferings=offerings(
        ("a.large", "us-west-2b"),
        # A Local Zone is not one of the Region's zone letters.
        ("a.large", "us-west-2-lax-1a"),
        ("b.large", "us-west-2a"),
    ),
)
# A dump from a CLI too old to know current elements.
save("stale/us-east-1.types.json", InstanceTypes=[{"InstanceType": "a.large"}])
save("stale/us-east-1.region.json", InstanceTypeOfferings=[])
save("stale/us-east-1.az.json", InstanceTypeOfferings=[])
PY

# A bare file name has no parent directory to create.
python3 "$SCRIPT" --dumps dumps --out catalog.json.gz >/dev/null
test -s catalog.json.gz

# A parent that does not exist yet is created.
python3 "$SCRIPT" --dumps dumps --out new/dir/catalog.json.gz >/dev/null
test -s new/dir/catalog.json.gz

# The output is byte-for-byte reproducible.
cp catalog.json.gz first.json.gz
python3 "$SCRIPT" --dumps dumps --out catalog.json.gz >/dev/null
cmp first.json.gz catalog.json.gz

python3 - <<'PY'
import gzip
import json

with gzip.open("catalog.json.gz", "rt", encoding="utf-8") as f:
    doc = json.load(f)
assert doc["dump_regions"] == ["us-east-1", "us-west-2"], doc["dump_regions"]
assert doc["requirements_omitted"] == {"us-east-1": ["a.large"], "us-west-2": ["a.large"]}, doc
# us-west-2 has no unsupported listing here, so nothing is said of it.
assert doc["not_described"] == {}, doc["not_described"]
assert doc["requirements_extra"] == {"us-west-2": ["c.large"]}, doc["requirements_extra"]
# Each reference's families, merged across Regions; one listing nothing is left out.
assert doc["baseline_performance"] == {"a": ["b", "c", "z"], "b": ["c"]}, doc["baseline_performance"]
types = {t["name"]: t for t in doc["instance_types"]}
assert list(types) == sorted(types), list(types)

a = types["a.large"]
# Disagreeing Regions merge by union.
assert a["info"]["dedicatedHostsSupported"] == "true", a
assert a["info"]["supportedUsageClasses"] == ["on-demand", "spot"], a
# Member names become XML element names, leaves the verbatim text.
ebs = a["info"]["ebsInfo"]
assert ebs["ebsOptimizedInfo"]["baselineThroughputInMBps"] == "2375.0", ebs
assert ebs["ebsCardSet"] == [{"ebsCardIndex": "0"}], ebs
assert a["info"]["gpuInfo"]["gpus"][0]["workloadSet"] == ["ml-ai"], a
assert a["info"]["vCpuInfo"]["defaultVCpus"] == "2", a
# Sorted zone letters per Region, Local Zones left out.
assert a["offerings"] == {"us-east-1": "ac", "us-west-2": "b"}, a
# ...and each Region keeps its own value where it differs from the union.
assert a["overrides"] == {
    "us-east-1": {"dedicatedHostsSupported": "false", "supportedUsageClasses": ["on-demand"]},
    "us-west-2": {"supportedUsageClasses": ["spot"]},
}, a["overrides"]
# A Region's answer about a type it does not sell is kept for that Region too.
assert types["b.large"]["overrides"] == {"us-east-1": {"supportedUsageClasses": ""}}, types["b.large"]

# The raw form is taken as is, without the per-request element.
assert "supportedInRegion" not in types["b.large"]["info"], types["b.large"]
assert types["b.large"]["offerings"] == {"us-west-2": "a"}, types["b.large"]
# Only an unsupported listing describes it: known, sold nowhere.
assert types["only-unsupported.large"]["offerings"] == {}, types["only-unsupported.large"]
PY

# A refresh keeps what it cannot see: a Region not in the dumps, and a type the API
# no longer describes. A dumped Region's own offerings replace what was vendored.
python3 - <<'PY'
import gzip
import json

with gzip.open("catalog.json.gz", "rt", encoding="utf-8") as f:
    doc = json.load(f)
types = {t["name"]: t for t in doc["instance_types"]}
types["a.large"]["offerings"]["af-south-1"] = ""
types["a.large"]["offerings"]["us-east-1"] = "z"
types["b.large"]["offerings"]["us-east-1"] = "a"
types["retired.large"] = {"name": "retired.large", "info": {"instanceType": "retired.large"},
                          "offerings": {"af-south-1": ""}}
types["only-unsupported.large"]["offerings"] = {"af-south-1": ""}
types["only-unsupported.large"]["info"]["supportedUsageClasses"] = ["on-demand"]
doc["instance_types"] = list(types.values())
with gzip.open("previous.json.gz", "wt", encoding="utf-8") as f:
    json.dump(doc, f)
PY
python3 "$SCRIPT" --dumps dumps --out previous.json.gz | grep -q "kept retired.large"
python3 - <<'PY'
import gzip
import json

with gzip.open("previous.json.gz", "rt", encoding="utf-8") as f:
    types = {t["name"]: t for t in json.load(f)["instance_types"]}
expected = {"af-south-1": "", "us-east-1": "ac", "us-west-2": "b"}
assert types["a.large"]["offerings"] == expected, types["a.large"]
assert types["b.large"]["offerings"] == {"us-west-2": "a"}, types["b.large"]
assert types["retired.large"]["offerings"] == {"af-south-1": ""}, types["retired.large"]
# Sold only where it is carried over: it keeps the usage classes EC2 omits elsewhere.
carried = types["only-unsupported.large"]
assert carried["info"]["supportedUsageClasses"] == ["on-demand"], carried
PY

# A dump that lost elements to an old CLI is refused, not vendored.
if python3 "$SCRIPT" --dumps stale --out stale.json.gz 2>stale.err; then
    echo "a stale dump was accepted" >&2
    exit 1
fi
grep -q "rebootMigrationSupport" stale.err
test ! -e stale.json.gz

# A list against a string (a scalar in one Region, a list in another) merges without
# crashing, and dumps are written atomically.
python3 - "$SCRIPT" <<'PY'
import importlib.util
import json
import os
import sys

spec = importlib.util.spec_from_file_location("generator", sys.argv[1])
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)
seen = []
merged = generator.merge(["a"], "b", "x", lambda *conflict: seen.append(conflict))
assert merged == ["a"] and seen, (merged, seen)
assert generator.merge("true", "false", "x", lambda *_: None) == "true"
os.mkdir("atomic")
generator.write_dump("atomic", "f.json", "K", [1])
assert os.listdir("atomic") == ["f.json"], os.listdir("atomic")
with open("atomic/f.json", encoding="utf-8") as f:
    assert json.load(f) == {"K": [1]}
PY

# Regions must be there at all.
mkdir empty
if python3 "$SCRIPT" --dumps empty --out empty.json.gz 2>empty.err; then
    echo "an empty dump directory was accepted" >&2
    exit 1
fi

echo "generate-ec2-instance-catalog tests passed"
