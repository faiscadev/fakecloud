#!/usr/bin/env python3
"""Regenerate the EC2 instance type catalog served by DescribeInstanceTypes.

Source of truth: EC2 itself. For every enabled Region of an account,
DescribeInstanceTypes plus DescribeInstanceTypeOfferings (LocationType region and
availability-zone), and DescribeInstanceTypes with IncludeUnsupportedInRegion, which
describes the types a Region does not sell. fakecloud vendors the result as
`crates/fakecloud-ec2/data/instance_types.json.gz`, which
`fakecloud_ec2::instance_types` decodes once at first use.

Run, either of:

  python3 scripts/generate-ec2-instance-catalog.py --profile PROFILE --dumps DIR
      Fetch with PROFILE's credentials (read-only DescribeRegions,
      DescribeInstanceTypes and DescribeInstanceTypeOfferings) into DIR, then build.

  python3 scripts/generate-ec2-instance-catalog.py --dumps DIR
      Build from DIR without touching AWS. No credentials needed.

A file already in DIR is not fetched again, so an interrupted fetch resumes. A real
refresh starts from an empty DIR.

DIR holds `<region>.types.json`, `<region>.region.json`, `<region>.az.json` and
optionally `<region>.unsupported.json`. The first and last are {"InstanceTypes": [...]}
and an offerings file {"InstanceTypeOfferings": [...]}, in either the API's own
element names (what --profile writes) or the `aws ec2 describe-*` output.

Fetching signs the Query API directly rather than running `aws ec2`, because the
CLI drops any element its bundled model does not know, without saying so: aws-cli
2.26.5 loses rebootMigrationSupport, every maximumEbsAttachments and most of the
network card details, and no botocore has IncludeUnsupportedInRegion yet. A dump
that has lost them is refused rather than vendored (see `load_types`). The `aws`
binary is only used for `aws configure export-credentials`.

The output is deterministic: types sorted by name (byte order, which the server
binary-searches), compact JSON with sorted keys, gzip with no timestamp.

Vendored schema:

  {"source": str, "dump_regions": [region...],
   "baseline_performance": {reference family: [included families]},
   "requirements_omitted": {region: [type...]},
   "not_described": {region: [type its unsupported listing leaves out]},
   "requirements_extra": {region: [type...]},
   "instance_types": [
    {"name": str, "info": InstanceTypeInfo, "offerings": {region: zone letters},
     "overrides": {region: {element: value}}}]}

`info` is the API's InstanceTypeInfo as a tree using its XML element names, with
every leaf as the verbatim text of the response (so 2375.0 stays 2375.0). The
server renders it back generically, so an element the API adds is served as soon
as the catalog is regenerated. Only supportedInRegion is left out, since the
server computes it per request.

Merging across Regions. The same type is described by every Region selling it.
Almost every field agrees. For one that does not, `info` holds the union (a list
takes every value, a boolean is true if any Region says true, a structure merges
field by field; a scalar or list of structures has no union, so the first Region in
sorted order wins), which is what a Region without its own data serves, and each
dumped Region whose value differs gets it in `overrides`, by top-level element.
Every disagreement is printed. A Region's unsupported listing gets the same
treatment: what it says about a type it does not sell differs too (usage classes and
dedicated hosts, and differently per Region), so it is kept in `overrides` as well,
and it is the only record of a type no enabled Region sells.

GetInstanceTypesFromInstanceRequirements is not simply DescribeInstanceTypes
filtered, so its answers are dumped too (`<region>.requirements.json`, the listing
with everything included, and `<region>.baseline.json`, the listing per
BaselinePerformanceFactors reference family, for the few Regions that between them
sell every family; the API has no other source for that measure):

  requirements_omitted   types it does not list in a Region that sells them. A Region
                         without its own data applies them all.
  requirements_extra     types it lists in a Region that does not describe them
  baseline_performance   the families each reference includes, unioned across those
                         Regions. They agree, apart from a Region that does not sell
                         the reference family, which lists nothing for it (the server
                         applies that too). A reference with no answer in the dumps
                         keeps the vendored one, and one listing nothing is left out.

What is carried over rather than fetched:

  Regions this account has not enabled keep the membership already vendored, with
  an empty zone string: EC2 sold the type there when last seen, per-zone data is
  unknown, and the server reports it for every zone of that Region. The original
  values came from the instances.vantage.sh dataset the catalog was built from
  before it moved to EC2's own data, and nothing can refresh them from here. For a
  Region in the dumps, its own offerings replace whatever was vendored.

  A type no enabled Region sells may have empty supportedUsageClasses (EC2 reports
  none for cc1, hs1, u-*.metal and a few others). If a carried-over Region sells it,
  it keeps the classes already vendored, so it does not read as unsellable where it
  is offered.

  A type the API no longer describes at all keeps its vendored record, so a refresh
  never makes a type that worked before unknown. It is printed as "kept". Today that
  is c6gn.metal, i2.large and i3p.16xlarge, with only their dataset values: no boot
  modes, cards or anything else that needed the API.

Zone letters are this account's: EC2 maps zone names to physical zones per
account, so `a` here and `a` elsewhere need not be the same zone. Zone IDs are not
vendored, the server derives them from the names exactly as
DescribeAvailabilityZones does.
"""
import argparse
import datetime
import glob
import gzip
import hashlib
import hmac
import io
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

SOURCE = (
    "EC2 DescribeInstanceTypes + DescribeInstanceTypeOfferings, "
    "scripts/generate-ec2-instance-catalog.py"
)
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
DEFAULT_OUT = os.path.join(
    ROOT, "crates", "fakecloud-ec2", "data", "instance_types.json.gz"
)

EC2_NS = "{http://ec2.amazonaws.com/doc/2016-11-15/}"
# The `aws` output's member names whose XML element is not simply the member with
# a lowercase first letter (ec2QueryName in aws-models/ec2.json).
WIRE_NAMES = {
    "EbsCards": "ebsCardSet",
    "InterfaceTypes": "interfaceTypeSet",
    "Workloads": "workloadSet",
}
# Computed per request, see the module docs.
NOT_VENDORED = {"supportedInRegion"}
# In every real response. Its absence means a CLI too old to know current elements.
CANARY = "rebootMigrationSupport"
LIST_ENTRIES = {EC2_NS + "item", EC2_NS + "member"}
ARCHITECTURES = ("i386", "x86_64", "arm64", "x86_64_mac", "arm64_mac")
# GetInstanceTypesFromInstanceRequirements once per instance family, per Region.
REQUIREMENTS_PACE_SECONDS = 0.1
# A plain AZ: the Region name plus one letter, not a Local or Wavelength Zone.
ZONE_RE = re.compile(r"^(?P<region>[a-z]+(-[a-z]+)+-\d+)(?P<letter>[a-z])$")


def family_of(info):
    """m5.large -> m5, of an InstanceTypeInfo in the vendored tree form."""
    return info["instanceType"].split(".")[0]


def wire_name(member):
    return WIRE_NAMES.get(member, member[0].lower() + member[1:])


def text_of(leaf):
    if isinstance(leaf, bool):
        return "true" if leaf else "false"
    return str(leaf)


def to_wire(value):
    """A record of either dump form -> the vendored tree. Idempotent."""
    if isinstance(value, dict):
        return {
            wire_name(k): to_wire(v)
            for k, v in value.items()
            if wire_name(k) not in NOT_VENDORED
        }
    if isinstance(value, list):
        return [to_wire(v) for v in value]
    return text_of(value)


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_types(path):
    types = [to_wire(t) for t in read_json(path)["InstanceTypes"]]
    stale = [t["instanceType"] for t in types if CANARY not in t]
    if stale:
        raise SystemExit(
            f"{path}: {len(stale)} types lack {CANARY} (such as {stale[0]}): this dump "
            "came from an `aws` CLI too old to keep every element. Use --profile, "
            "which reads the raw API, or a current CLI."
        )
    return types


def load_offerings(path):
    return [
        (o["InstanceType"], o["Location"])
        for o in read_json(path)["InstanceTypeOfferings"]
    ]


def dump_regions(directory):
    suffix = ".types.json"
    return sorted(
        os.path.basename(p)[: -len(suffix)]
        for p in glob.glob(os.path.join(directory, "*" + suffix))
    )


def write_dump(directory, name, key, items):
    """Atomically, so an interrupted fetch never leaves a truncated file to resume on."""
    path = os.path.join(directory, name)
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as f:
        json.dump({key: items}, f)
    os.replace(temporary, path)


# ---- merging ----


def merge(a, b, where, on_conflict):
    """Union `b` into `a`, reporting what differs (see the module docs)."""
    # An empty element: nothing to merge. Its type is unknown from the XML alone.
    if a == "":
        return b
    if b == "":
        return a
    if isinstance(a, dict) and isinstance(b, dict):
        out = dict(a)
        for key, value in b.items():
            if key in a:
                out[key] = merge(a[key], value, f"{where}.{key}", on_conflict)
            else:
                out[key] = value
                on_conflict(f"{where}.{key}", None, value)
        for key in set(a) - set(b):
            on_conflict(f"{where}.{key}", a[key], None)
        return out
    if isinstance(a, list) and isinstance(b, list):
        if all(isinstance(v, str) for v in a + b):
            # Regions order some lists differently (workloadSet), which says nothing.
            if set(a) != set(b):
                on_conflict(where, a, b)
            return a + sorted(set(b) - set(a))
        if len(a) == len(b):
            return [merge(x, y, f"{where}[]", on_conflict) for x, y in zip(a, b)]
        on_conflict(where, a, b)
        return a
    if a == b:
        return a
    on_conflict(where, a, b)
    # Only two strings can be a boolean: `a` or `b` may be a list or structure here.
    if isinstance(a, str) and isinstance(b, str) and {a, b} == {"true", "false"}:
        return "true"
    return a


def merge_regions(regions, by_region):
    """Merge each type's per-Region records. Returns (types, conflicts by field)."""
    merged = {}
    conflicts = {}
    for region in regions:
        for record in by_region[region]:
            name = record["instanceType"]
            if name not in merged:
                merged[name] = record
                continue

            def report(field, _a, _b, name=name, region=region):
                seen = conflicts.setdefault(field, {})
                seen.setdefault(name, []).append(region)

            merged[name] = merge(merged[name], record, "", report)
    return merged, conflicts


def canonical(value):
    """A value compared regardless of the order of a list of plain values."""
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return sorted(value)
    if isinstance(value, list):
        return [canonical(v) for v in value]
    if isinstance(value, dict):
        return {k: canonical(v) for k, v in value.items()}
    return value


def region_overrides(merged, regions, by_region):
    """{type: {region: {element: value}}} where a Region differs from the union."""
    overrides = {}
    for region in regions:
        for record in by_region[region]:
            name = record["instanceType"]
            different = {
                element: value
                for element, value in record.items()
                if canonical(value) != canonical(merged[name].get(element))
            }
            if different:
                overrides.setdefault(name, {})[region] = different
    return overrides


def zone_letters(offerings, regions):
    """{type: {region: letters}} from the zone offerings of `regions`."""
    letters = {}
    for region in regions:
        for name, location in offerings[region]:
            match = ZONE_RE.match(location)
            if match and match["region"] == region:
                letters.setdefault(name, {}).setdefault(region, set()).add(
                    match["letter"]
                )
    return letters


def vendored(path):
    if not os.path.exists(path):
        return {"instance_types": []}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def optional_json(path, key):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)[key]


def requirements_differences(directory, regions, sold):
    """({region: sold but not listed}, {region: listed but not described})."""
    omitted, extra = {}, {}
    for region in regions:
        listed = optional_json(
            os.path.join(directory, f"{region}.requirements.json"), "InstanceTypes"
        )
        if listed is None:
            continue
        if sold[region] - set(listed):
            omitted[region] = sorted(sold[region] - set(listed))
        if set(listed) - sold[region]:
            extra[region] = sorted(set(listed) - sold[region])
    return omitted, extra


def baseline_performance(directory, previous):
    """{reference: included families}, merged across the dumped Regions."""
    included = dict(previous)
    answers = {}
    for path in sorted(glob.glob(os.path.join(directory, "*.baseline.json"))):
        region = os.path.basename(path).split(".")[0]
        answers[region] = optional_json(path, "References")
    references = sorted({r for a in answers.values() for r in a})
    if not references:
        return included
    disagreements = {}
    for reference in references:
        families = set()
        for region, by_reference in answers.items():
            listed = by_reference.get(reference, [])
            families |= {t.split(".")[0] for t in listed}
        for region, by_reference in answers.items():
            here = {t.split(".")[0] for t in by_reference.get(reference, [])}
            # A Region not selling the reference family lists nothing for it.
            if not here:
                continue
            sold_here = {
                t.split(".")[0] for listed in by_reference.values() for t in listed
            }
            if (families & sold_here) - here:
                disagreements.setdefault(reference, []).append(region)
        if families:
            included[reference] = sorted(families)
        else:
            included.pop(reference, None)
    if disagreements:
        print(f"Regions disagree on baseline references: {disagreements}")
    return included


def build(directory, previous):
    regions = dump_regions(directory)
    if not regions:
        raise SystemExit(f"{directory}: no <region>.types.json files")
    types = {r: load_types(os.path.join(directory, f"{r}.types.json")) for r in regions}
    in_region = {
        r: load_offerings(os.path.join(directory, f"{r}.region.json")) for r in regions
    }
    in_zone = {
        r: load_offerings(os.path.join(directory, f"{r}.az.json")) for r in regions
    }
    merged, conflicts = merge_regions(regions, types)
    described_anywhere = set(merged)
    described_in = {r: {t["instanceType"] for t in types[r]} for r in regions}
    for region in regions:
        path = os.path.join(directory, f"{region}.unsupported.json")
        if not os.path.exists(path):
            continue
        for record in load_types(path):
            if record["instanceType"] not in described_in[region]:
                merged.setdefault(record["instanceType"], record)
                types[region].append(record)
    for region in regions:
        described = described_in[region]
        offered_here = {name for name, _ in in_region[region]}
        if described != offered_here:
            print(
                f"{region}: described but not offered {sorted(described - offered_here)}, "
                f"offered but not described {sorted(offered_here - described)}"
            )

    letters = zone_letters(in_zone, regions)
    offered = {r: {name for name, _ in in_region[r]} for r in regions}
    previous_by_name = {t["name"]: t for t in previous}
    entries = []
    sold = {r: {name for name, _ in in_region[r]} for r in regions}
    omitted, extra = requirements_differences(directory, regions, sold)
    for name in sorted(set(merged) | set(previous_by_name)):
        before = previous_by_name.get(name)
        offerings = {
            region: zones
            for region, zones in (before or {}).get("offerings", {}).items()
            if region not in regions
        }
        for region in regions:
            if name in offered[region]:
                zones = letters.get(name, {}).get(region, set())
                offerings[region] = "".join(sorted(zones))
        if name in merged:
            info = merged[name]
            if (
                name not in described_anywhere
                and offerings
                and not info.get("supportedUsageClasses")
                and before
                and before["info"].get("supportedUsageClasses")
            ):
                # A copy: `info` is also the record of the Region that said none.
                info = {**info, "supportedUsageClasses": before["info"]["supportedUsageClasses"]}
                merged[name] = info
        else:
            print(f"kept {name}: EC2 no longer describes it")
            info = before["info"]
        entries.append({"name": name, "info": info, "offerings": offerings})
    # After the loop above, which can still change `info`.
    overrides = region_overrides(merged, regions, types)
    for entry in entries:
        before = previous_by_name.get(entry["name"]) or {}
        own = {
            region: elements
            for region, elements in before.get("overrides", {}).items()
            if region not in regions
        }
        own.update(overrides.get(entry["name"], {}))
        if own:
            entry["overrides"] = own
    # A Region's unsupported listing leaves out some types it has never sold, which
    # are then unknown there even when asked for. A type EC2 no longer describes at
    # all is kept known, see the module docs.
    not_described = {}
    for region in regions:
        path = os.path.join(directory, f"{region}.unsupported.json")
        if os.path.exists(path):
            listed = {t["instanceType"] for t in types[region]}
            absent = sorted(set(merged) - listed)
            if absent:
                not_described[region] = absent
    report_conflicts(conflicts)
    print(f"{len(regions)} Regions dumped: {' '.join(regions)}")
    never = sum(1 for e in entries if not e["offerings"])
    print(f"{never} types not offered in any Region known to the catalog")
    print(f"requirements never list {omitted}; extra in {sorted(extra)}")
    return regions, entries, omitted, extra, not_described


def report_conflicts(conflicts):
    for field in sorted(conflicts):
        names = conflicts[field]
        sample = ", ".join(sorted(names)[:5])
        print(
            f"Regions disagree on {field} for {len(names)} types (such as {sample}): "
            "kept per Region"
        )


def write(regions, entries, omitted, extra, not_described, baseline, path):
    doc = {
        "not_described": not_described,
        "source": SOURCE,
        "dump_regions": regions,
        "baseline_performance": baseline,
        "requirements_omitted": omitted,
        "requirements_extra": extra,
        "instance_types": entries,
    }
    data = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9) as gz:
        gz.write(data)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(buf.getvalue())
    print(f"{len(entries)} types, {len(data)} bytes raw, {buf.tell()} gzipped: {path}")


# ---- fetching (--profile) ----


def credentials(profile):
    out = subprocess.run(
        ["aws", "configure", "export-credentials", "--profile", profile, "--format", "process"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout)


def _hmac(key, message):
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def signed_headers(creds, region, body):
    """SigV4 for a form POST to EC2."""
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    host = f"ec2.{region}.amazonaws.com"
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=utf-8",
        "host": host,
        "x-amz-date": stamp,
    }
    if creds.get("SessionToken"):
        headers["x-amz-security-token"] = creds["SessionToken"]
    names = sorted(headers)
    canonical = "\n".join(
        [
            "POST",
            "/",
            "",
            "".join(f"{n}:{headers[n]}\n" for n in names),
            ";".join(names),
            hashlib.sha256(body.encode()).hexdigest(),
        ]
    )
    scope = f"{day}/{region}/ec2/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = ("AWS4" + creds["SecretAccessKey"]).encode()
    for part in (day, region, "ec2", "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={creds['AccessKeyId']}/{scope}, "
        f"SignedHeaders={';'.join(names)}, Signature={signature}"
    )
    return headers


def ec2_call(creds, region, action, params):
    """One Query API call, retrying throttling and network errors."""
    body = urllib.parse.urlencode({"Action": action, "Version": "2016-11-15", **params})
    last = None
    for attempt in range(6):
        request = urllib.request.Request(
            f"https://ec2.{region}.amazonaws.com/",
            data=body.encode(),
            headers=signed_headers(creds, region, body),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return ET.fromstring(response.read())
        except urllib.error.HTTPError as err:
            detail = err.read()
            last = f"HTTP {err.code}: {detail[:300]!r}"
            if err.code < 500 and b"RequestLimitExceeded" not in detail:
                break
        except OSError as err:
            last = str(err)
        time.sleep(2**attempt)
    raise SystemExit(f"{region} {action}: {last}")


def element_tree(element):
    """A response element -> the same tree the vendored catalog stores."""
    kids = list(element)
    if not kids:
        return element.text or ""
    # inferenceAcceleratorInfo.accelerators is the one list under InstanceTypeInfo
    # whose entries are <member>, not <item>; the server renders it back that way.
    if all(k.tag in LIST_ENTRIES for k in kids):
        return [element_tree(k) for k in kids]
    return {k.tag.replace(EC2_NS, ""): element_tree(k) for k in kids}


def paged(creds, region, action, list_element, params):
    token = None
    while True:
        page = dict(params)
        if token:
            page["NextToken"] = token
        root = ec2_call(creds, region, action, page)
        listed = root.find(EC2_NS + list_element)
        if listed is not None:
            yield from (element_tree(item) for item in listed)
        next_token = root.find(EC2_NS + "nextToken")
        token = next_token.text if next_token is not None else None
        if not token:
            return


def requirements_listing(creds, region, reference=None):
    """Every type GetInstanceTypesFromInstanceRequirements lists, however it is built."""
    params = {
        "InstanceRequirements.VCpuCount.Min": "1",
        "InstanceRequirements.MemoryMiB.Min": "1",
        "InstanceRequirements.BareMetal": "included",
        "InstanceRequirements.BurstablePerformance": "included",
        "MaxResults": "1000",
    }
    for i, arch in enumerate(ARCHITECTURES, 1):
        params[f"ArchitectureType.{i}"] = arch
    for i, virtualization in enumerate(("hvm", "paravirtual"), 1):
        params[f"VirtualizationType.{i}"] = virtualization
    if reference:
        key = "InstanceRequirements.BaselinePerformanceFactors.Cpu.Reference.1.InstanceFamily"
        params[key] = reference
    time.sleep(REQUIREMENTS_PACE_SECONDS)
    listed = paged(
        creds, region, "GetInstanceTypesFromInstanceRequirements", "instanceTypeSet", params
    )
    return sorted(item["instanceType"] for item in listed)


def covering_regions(directory, regions):
    """A few Regions that together sell every family any dumped Region sells."""
    sold = {}
    for region in regions:
        sold[region] = {
            family_of(t) for t in load_types(os.path.join(directory, f"{region}.types.json"))
        }
    chosen, left = [], set().union(*sold.values())
    while left:
        best = max(sorted(sold), key=lambda r: len(sold[r] & left))
        chosen.append(best)
        left -= sold[best]
    return chosen


def fetch(profile, directory, only_regions):
    os.makedirs(directory, exist_ok=True)
    creds = credentials(profile)

    def have(name):
        return os.path.exists(os.path.join(directory, name))

    def save(name, key, items):
        write_dump(directory, name, key, items)

    root = ec2_call(creds, "us-east-1", "DescribeRegions", {})
    regions = sorted(
        element_tree(r)["regionName"] for r in root.find(EC2_NS + "regionInfo")
    )
    if only_regions:
        regions = [r for r in regions if r in only_regions]
    for region in regions:
        print(f"fetching {region}")
        if not have(f"{region}.types.json"):
            types = list(
                paged(
                    creds,
                    region,
                    "DescribeInstanceTypes",
                    "instanceTypeSet",
                    {"MaxResults": "100"},
                )
            )
            save(f"{region}.types.json", "InstanceTypes", types)
        if not have(f"{region}.unsupported.json"):
            types = list(
                paged(
                    creds,
                    region,
                    "DescribeInstanceTypes",
                    "instanceTypeSet",
                    {"MaxResults": "100", "IncludeUnsupportedInRegion": "true"},
                )
            )
            save(f"{region}.unsupported.json", "InstanceTypes", types)
        if not have(f"{region}.requirements.json"):
            listed = requirements_listing(creds, region)
            save(f"{region}.requirements.json", "InstanceTypes", listed)
        for suffix, location_type in (("region", "region"), ("az", "availability-zone")):
            if have(f"{region}.{suffix}.json"):
                continue
            offerings = [
                {"InstanceType": o["instanceType"], "Location": o["location"]}
                for o in paged(
                    creds,
                    region,
                    "DescribeInstanceTypeOfferings",
                    "instanceTypeOfferingSet",
                    {"LocationType": location_type, "MaxResults": "1000"},
                )
            ]
            save(f"{region}.{suffix}.json", "InstanceTypeOfferings", offerings)
    families = sorted(
        {
            family_of(t)
            for region in regions
            for t in load_types(os.path.join(directory, f"{region}.unsupported.json"))
        }
    )
    for region in covering_regions(directory, regions):
        name = f"{region}.baseline.json"
        answers = {}
        if have(name):
            with open(os.path.join(directory, name), encoding="utf-8") as f:
                answers = json.load(f)["References"]
        for family in families:
            if family not in answers:
                print(f"{region}: baseline reference {family}")
                answers[family] = requirements_listing(creds, region, family)
                save(name, "References", answers)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dumps", required=True, help="directory of per-Region dumps")
    parser.add_argument(
        "--profile", help="fetch into --dumps with this AWS profile first (read-only)"
    )
    parser.add_argument(
        "--region",
        action="append",
        help="with --profile, fetch only this Region (repeatable). The default is all "
        "enabled Regions, which is what a real refresh needs: a Region missing from the "
        "dumps keeps stale offerings.",
    )
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()
    if args.profile:
        fetch(args.profile, args.dumps, args.region)
    previous = vendored(args.out)
    regions, entries, omitted, extra, not_described = build(
        args.dumps, previous["instance_types"]
    )
    baseline = baseline_performance(args.dumps, previous.get("baseline_performance", {}))
    write(regions, entries, omitted, extra, not_described, baseline, args.out)


if __name__ == "__main__":
    main()
