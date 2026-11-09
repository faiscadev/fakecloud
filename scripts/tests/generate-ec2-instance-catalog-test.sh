#!/usr/bin/env bash
#
# Tests for scripts/generate-ec2-instance-catalog.py. Hermetic: it reads a small
# local --input, so it never downloads the dataset.
#
#   bash scripts/tests/generate-ec2-instance-catalog-test.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT="$ROOT/scripts/generate-ec2-instance-catalog.py"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cd "$TMP"

python3 - <<'PY'
import json

def row(name, family):
    return {
        "instance_type": name, "family": family, "vCPU": 4, "memory": 8, "arch": ["x86_64"],
        "generation": "current", "network_performance": "Up to 10 Gigabit",
        "linux_virtualization_types": [], "GPU": 0, "FPGA": 0, "regions": {"us-east-1": "x"},
    }

asic = "Machine Learning ASIC Instances"
with open("instances.json", "w", encoding="utf-8") as f:
    json.dump([row("dl1.24xlarge", asic), row("inf1.xlarge", asic), row("m5.large", "General")], f)
PY

# A bare file name has no parent directory to create.
python3 "$SCRIPT" --input instances.json --out catalog.json.gz >/dev/null
test -s catalog.json.gz

# A parent that does not exist yet is created.
python3 "$SCRIPT" --input instances.json --out new/dir/catalog.json.gz >/dev/null
test -s new/dir/catalog.json.gz

python3 - <<'PY'
import gzip
import json

with gzip.open("catalog.json.gz", "rt", encoding="utf-8") as f:
    types = {t["name"]: t for t in json.load(f)["instance_types"]}
expected = {"type": "gpu", "manufacturer": "habana", "name": "gaudi-hl-205"}
assert types["dl1.24xlarge"]["accelerator"] == expected, types["dl1.24xlarge"]
assert types["inf1.xlarge"]["accelerator"] == {"type": "inference"}, types["inf1.xlarge"]
assert "accelerator" not in types["m5.large"], types["m5.large"]
PY

echo "generate-ec2-instance-catalog tests passed"
