#!/usr/bin/env python3
"""Regenerate the EC2 instance type catalog served by DescribeInstanceTypes.

Source of truth: the public catalog at https://instances.vantage.sh/instances.json
(the ec2instances.info dataset), which merges `DescribeInstanceTypes`, the
pricing API and per-region offerings for every instance type. fakecloud vendors
a trimmed copy as `crates/fakecloud-ec2/data/instance_types.json.gz`, which
`fakecloud_ec2::instance_types` decodes once at first use.

Run:  python3 scripts/generate-ec2-instance-catalog.py [--input instances.json]
Without --input it downloads the dataset (network; no AWS credentials needed).
The output is deterministic: types sorted by name (byte order, which the server
binary-searches), compact JSON, gzip with no timestamp.

A type already in the vendored catalog that a newer dataset lacks is kept as is,
so a refresh never makes a type that worked before unknown. A dataset row with
neither prices nor Regions describes nothing EC2 sells anywhere (d3en.large and
p6e-gb200.36xlarge today), so it is dropped.

What is sourced, and how. A value the dataset does not hold is left out of the
catalog (the server then omits the element) rather than invented:

  Direct: name, vCPUs, memory (the dataset has GiB, so 8 -> 8192 MiB; a few old
  types have rounded GiB such as m1.medium 3.75, so their MiB can be slightly
  off), cores, architectures, current generation, bare metal, instance storage
  (disk count, size, type, NVMe), EBS-optimized figures, network performance,
  ENI + IPv4 limits, IPv6 support, Nitro Enclaves, regions.

  Derived from one dataset field:
    threads_per_core       vCPU / cores
    manufacturer           leading vendor word of physical_processor
    hypervisor             nitro_support: true -> nitro, false -> xen; bare metal
                           has none, as on EC2
    burstable              base_performance is set
    ebs_optimized_support  ebs_optimized false -> unsupported, Nitro -> default.
                           A Xen type's true does not say default or supported,
                           so it is omitted
    ebs_nvme_support       ebs_as_nvme: true -> required, false -> unsupported
    ena_support            Nitro -> required, enhanced_networking false ->
                           unsupported. A Xen type's true may mean Intel VF
                           rather than ENA, so it is omitted
    placement_strategies   placement_group_support is the cluster strategy
                           (t2 and Mac are false); partition and spread have no
                           instance type restriction in the EC2 User Guide
    usage_classes          a usable on-demand / spot price in any region. No spot
                           price does not mean no Spot, so the list only ever
                           says what is known: a type without spot here is not
                           claimed to lack it (54 types, mostly Mac and u-*)
    accelerator            family gives the type (gpu, fpga, inference, media);
                           GPU / FPGA the count, a fractional GPU counting as
                           the 1 device it shares; GPU_model the manufacturer
                           and name; GPU_memory the total. Inference chips have
                           no count, name or memory in the data
    virtualization_types   linux_virtualization_types; empty there means hvm
                           (only old types list a set)
    ipv6_per_interface     0 when IPv6 is unsupported; otherwise not in the data

  37 sparse records (cc2, f1, g2, g3, p2, p3, i3.metal...) have no vpc, cores or
  nitro_support. Their ebs_optimized, enhanced_networking, ipv6_support,
  placement_group_support and storage are placeholders (u-12tb1.112xlarge says no
  enhanced networking beside 100 Gigabit), so everything depending on them is
  omitted, including all placement strategies.

  Not in the dataset at all, so never served: EBS encryption, instance store
  encryption, EFA, network cards, IPv6 addresses per interface, root device
  types, boot modes, hibernation, auto recovery, dedicated hosts, free tier,
  Nitro TPM. The dataset's clock speed comes from the pricing API, not
  SustainedClockSpeedInGhz, so it is not served either.

Vendored schema (compact JSON, gzipped): {"source": URL, "instance_types": [...]},
snake_case members matching `InstanceTypeSpec` in instance_types.rs.
"""
import argparse
import gzip
import io
import json
import math
import os
import re
import urllib.request

SOURCE_URL = "https://instances.vantage.sh/instances.json"
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
DEFAULT_OUT = os.path.join(
    ROOT, "crates", "fakecloud-ec2", "data", "instance_types.json.gz"
)

MANUFACTURERS = ("Intel", "AMD", "AWS", "Apple", "NVIDIA")
# A plain Region, not a Local Zone or Wavelength entry (us-east-1-bos-1).
REGION_RE = re.compile(r"^[a-z]+(-[a-z]+)+-\d+$")
VIRTUALIZATION = {"HVM": "hvm", "PV": "paravirtual"}
ACCELERATOR_FAMILIES = {
    "GPU instance": "gpu",
    "FPGA Instances": "fpga",
    "Machine Learning ASIC Instances": "inference",
    "Media Accelerator Instances": "media",
}
# The dataset's model name -> the API's AcceleratorName, where they differ by
# more than case.
ACCELERATOR_NAMES = {
    "T4 Tensor Core": "t4",
    "T4G Tensor Core": "t4g",
    "Tesla M60": "m60",
    "Tesla V100": "v100",
    "Tesla K80": "k80",
    "GRID K520": "k520",
    "Radeon Pro V520": "radeon-pro-v520",
}


def manufacturer(processor):
    for vendor in MANUFACTURERS:
        if re.search(rf"\b{vendor}\b", processor or ""):
            return vendor
    return None


def usable_price(value):
    return value not in (None, "N/A", "", 0, "0")


def usage_classes(pricing):
    on_demand = spot = False
    for oses in pricing.values():
        for prices in oses.values():
            if not isinstance(prices, dict):
                continue
            if usable_price(prices.get("ondemand")):
                on_demand = True
            if any(k.startswith("spot") and usable_price(v) for k, v in prices.items()):
                spot = True
    return (["on-demand"] if on_demand else []) + (["spot"] if spot else [])


def accelerator_of(raw):
    gpus, fpgas = raw["GPU"], raw["FPGA"]
    kind = ACCELERATOR_FAMILIES.get(raw["family"])
    if kind is None:
        kind = "gpu" if gpus else "fpga" if fpgas else None
    if kind is None:
        return None
    entry = {"type": kind}
    if gpus or fpgas:
        entry["count"] = math.ceil(gpus) if gpus else fpgas
    if raw.get("GPU_model"):
        vendor, _, model = raw["GPU_model"].partition(" ")
        entry["manufacturer"] = vendor.lower()
        entry["name"] = ACCELERATOR_NAMES.get(model, model.lower())
    if raw.get("GPU_memory"):
        entry["total_memory_mib"] = math.floor(raw["GPU_memory"] * 1024)
    return entry


def storage_of(raw):
    if not raw:
        return None
    if raw["size_unit"] != "GB":
        raise ValueError(f"unexpected storage unit: {raw['size_unit']}")
    return {
        "total_gb": raw["devices"] * raw["size"],
        "disk_count": raw["devices"],
        "disk_size_gb": raw["size"],
        "disk_type": "ssd" if raw["ssd"] else "hdd",
        "nvme_support": "required" if raw["nvme_ssd"] else "unsupported",
    }


def ebs_optimized_of(raw):
    if not raw["ebs_optimized"] or not raw["ebs_max_bandwidth"]:
        return None
    return {
        "baseline_bandwidth_mbps": raw["ebs_baseline_bandwidth"],
        "baseline_throughput_mbps": raw["ebs_baseline_throughput"],
        "baseline_iops": raw["ebs_baseline_iops"],
        "max_bandwidth_mbps": raw["ebs_max_bandwidth"],
        "max_throughput_mbps": raw["ebs_throughput"],
        "max_iops": raw["ebs_iops"],
    }


def hypervisor_of(nitro, bare_metal):
    if nitro is None or bare_metal:
        return None
    return "nitro" if nitro else "xen"


def ebs_optimized_support_of(raw, nitro):
    if nitro is None:
        return None
    if not raw["ebs_optimized"]:
        return "unsupported"
    return "default" if nitro else None


def trim(raw):
    name = raw["instance_type"]
    # True for Nitro, False for Xen, None for the sparse records.
    nitro = raw.get("nitro_support")
    known = nitro is not None
    bare_metal = raw.get("is_bare_metal", ".metal" in name)
    cores = raw.get("cores") or None
    storage = storage_of(raw.get("storage"))
    vpc = raw.get("vpc") or {}
    ena = None
    if nitro:
        ena = "required"
    elif known and not raw["enhanced_networking"]:
        ena = "unsupported"
    entry = {
        "name": name,
        "vcpus": raw["vCPU"],
        # Floored: AWS's own MiB for a rounded GiB figure (1.7 -> 1740).
        "memory_mib": math.floor(raw["memory"] * 1024),
        "cores": cores,
        "threads_per_core": raw["vCPU"] // cores if cores else None,
        "architectures": raw["arch"],
        "manufacturer": manufacturer(raw.get("physical_processor")),
        "hypervisor": hypervisor_of(nitro, bare_metal),
        "current_generation": raw["generation"] == "current",
        "bare_metal": bare_metal,
        "burstable": raw.get("base_performance") is not None,
        # A sparse record's null storage does not mean no disks.
        "instance_storage_supported": True if storage else (False if known else None),
        "instance_storage": storage,
        "ebs_optimized_support": ebs_optimized_support_of(raw, nitro),
        "ebs_nvme_support": ("required" if raw["ebs_as_nvme"] else "unsupported")
        if known
        else None,
        "ebs_optimized_info": ebs_optimized_of(raw) if known else None,
        "network_performance": raw["network_performance"],
        "max_network_interfaces": vpc.get("max_enis"),
        "ipv4_per_interface": vpc.get("ips_per_eni"),
        "ipv6_supported": raw["ipv6_support"] if known else None,
        "ena_support": ena,
        "network_bandwidth_gbps": raw.get("baseline_bandwidth_gbps"),
        "nitro_enclaves_support": None,
        "placement_strategies": (
            (["cluster"] if raw["placement_group_support"] else [])
            + ["partition", "spread"]
        )
        if known
        else [],
        "accelerator": accelerator_of(raw),
        "usage_classes": usage_classes(raw.get("pricing") or {}),
        "virtualization_types": [
            VIRTUALIZATION[v] for v in raw["linux_virtualization_types"]
        ]
        or ["hvm"],
        "regions": sorted(
            r for r in (raw.get("regions") or {}) if REGION_RE.match(r)
        ),
    }
    enclaves = raw.get("nitro_enclave_support")
    if enclaves is not None:
        entry["nitro_enclaves_support"] = "supported" if enclaves else "unsupported"
    return {k: v for k, v in entry.items() if v is not None}


def load(path):
    if path:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    with urllib.request.urlopen(SOURCE_URL) as resp:
        return json.load(resp)


def vendored(path):
    if not os.path.exists(path):
        return []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)["instance_types"]


def sold_somewhere(raw):
    return bool(raw.get("pricing") or raw.get("regions"))


def build(raw, previous):
    entries = {r["instance_type"]: trim(r) for r in raw if sold_somewhere(r)}
    dropped = sorted(r["instance_type"] for r in raw if not sold_somewhere(r))
    print(f"dropped {len(dropped)} with no prices and no Regions: {dropped}")
    named = {r["instance_type"] for r in raw}
    for entry in previous:
        if entry["name"] not in named:
            print(f"kept {entry['name']}: not in this dataset")
            entries[entry["name"]] = entry
    return [entries[name] for name in sorted(entries)]


def write(entries, path):
    doc = {"source": SOURCE_URL, "instance_types": entries}
    data = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9) as gz:
        gz.write(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(buf.getvalue())
    print(f"{len(entries)} types, {len(data)} bytes raw, {buf.tell()} gzipped: {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", help="a local copy of instances.json")
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()
    write(build(load(args.input), vendored(args.out)), args.out)


if __name__ == "__main__":
    main()
