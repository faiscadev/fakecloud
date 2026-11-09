#!/usr/bin/env python3
"""Generate the protocol table behind Step Functions `aws-sdk:*` Task integrations.

A Task names an AWS API action and its parameters in the casing of the API's
Smithy model; reaching the emulated service takes that service's real wire
protocol. This condenses `aws-models/*.json` into the operations, HTTP bindings
and shapes the interpreter needs, one compressed JSON document per service, in
`crates/fakecloud-stepfunctions/data/sdk_models.json`. Re-run it whenever
`aws-models/` changes; `--check` fails if the committed table is out of date.
"""
import base64
import gzip
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "crates/fakecloud-stepfunctions/data/sdk_models.json"
PROTOCOLS = {
    "aws.protocols#restJson1": "restJson",
    "aws.protocols#restXml": "restXml",
    "aws.protocols#awsJson1_0": "json",
    "aws.protocols#awsJson1_1": "json",
    "aws.protocols#awsQuery": "query",
    "aws.protocols#ec2Query": "ec2",
}
# Preferred when a model lists several: emulated services all take JSON then.
PROTOCOL_ORDER = [
    "aws.protocols#awsJson1_0",
    "aws.protocols#awsJson1_1",
    "aws.protocols#restJson1",
    "aws.protocols#restXml",
    "aws.protocols#awsQuery",
    "aws.protocols#ec2Query",
]
PRELUDE = {
    "String": "~s",
    "Boolean": "~B",
    "PrimitiveBoolean": "~B",
    "Byte": "~n",
    "PrimitiveByte": "~n",
    "Short": "~n",
    "PrimitiveShort": "~n",
    "Integer": "~n",
    "PrimitiveInteger": "~n",
    "Long": "~n",
    "PrimitiveLong": "~n",
    "Float": "~n",
    "PrimitiveFloat": "~n",
    "Double": "~n",
    "PrimitiveDouble": "~n",
    "BigInteger": "~n",
    "BigDecimal": "~n",
    "Timestamp": "~t",
    "Blob": "~b",
    "Document": "~d",
    "Unit": "",
}
SCALARS = {
    "string": "~s",
    "enum": "~s",
    "intEnum": "~n",
    "boolean": "~B",
    "byte": "~n",
    "short": "~n",
    "integer": "~n",
    "long": "~n",
    "float": "~n",
    "double": "~n",
    "bigInteger": "~n",
    "bigDecimal": "~n",
    "timestamp": "~t",
    "blob": "~b",
    "document": "~d",
}


def short(shape_id):
    return shape_id.split("#")[-1]


def target_of(shapes, shape_id):
    """A scalar's sentinel, or the short name of a structure, list or map."""
    if shape_id.startswith("smithy.api#"):
        return PRELUDE.get(short(shape_id), "~s")
    shape = shapes.get(shape_id)
    if shape is None:
        return "~s"
    kind = shape["type"]
    if kind == "blob" and "smithy.api#streaming" in shape.get("traits", {}):
        return "~sb"
    if kind in SCALARS:
        return SCALARS[kind]
    return short(shape_id)


def member_entry(shapes, member):
    traits = member.get("traits", {})
    out = {"t": target_of(shapes, member["target"])}
    for trait, key in [
        ("smithy.api#httpLabel", "l"),
        ("smithy.api#httpPayload", "p"),
        ("smithy.api#xmlFlattened", "f"),
        ("smithy.api#xmlAttribute", "a"),
        ("smithy.api#httpResponseCode", "rc"),
    ]:
        if trait in traits:
            out[key] = 1
    for trait, key in [
        ("smithy.api#httpQuery", "q"),
        ("smithy.api#httpHeader", "h"),
        ("smithy.api#xmlName", "x"),
        ("smithy.api#jsonName", "j"),
        ("aws.protocols#ec2QueryName", "e"),
    ]:
        if trait in traits:
            out[key] = traits[trait]
    if "smithy.api#httpPrefixHeaders" in traits:
        out["hp"] = traits["smithy.api#httpPrefixHeaders"]
    if "smithy.api#httpQueryParams" in traits:
        out["qp"] = 1
    return out


def shape_entry(shapes, shape_id):
    shape = shapes[shape_id]
    kind = shape["type"]
    traits = shape.get("traits", {})
    if kind in ("structure", "union"):
        out = {
            "k": "s",
            "m": {n: member_entry(shapes, m) for n, m in shape.get("members", {}).items()},
        }
        if "smithy.api#xmlName" in traits:
            out["x"] = traits["smithy.api#xmlName"]
        return out
    if kind == "list":
        member = shape["member"]
        out = {"k": "l", "t": target_of(shapes, member["target"])}
        name = member.get("traits", {}).get("smithy.api#xmlName")
        if name:
            out["x"] = name
        return out
    if kind == "map":
        out = {"k": "m", "t": target_of(shapes, shape["value"]["target"])}
        key_name = shape["key"].get("traits", {}).get("smithy.api#xmlName")
        value_name = shape["value"].get("traits", {}).get("smithy.api#xmlName")
        if key_name:
            out["kx"] = key_name
        if value_name:
            out["vx"] = value_name
        return out
    return None


def reachable(shapes, roots):
    seen = set()
    stack = list(roots)
    while stack:
        shape_id = stack.pop()
        if shape_id in seen or shape_id not in shapes:
            continue
        seen.add(shape_id)
        shape = shapes[shape_id]
        for member in shape.get("members", {}).values():
            stack.append(member["target"])
        for part in ("member", "key", "value"):
            if part in shape:
                stack.append(shape[part]["target"])
    return seen


def condense(model_key, model, registered):
    shapes = model["shapes"]
    service_id, service = next((k, v) for k, v in shapes.items() if v["type"] == "service")
    traits = service.get("traits", {})
    protocol = next((PROTOCOLS[p] for p in PROTOCOL_ORDER if p in traits), None)
    if protocol is None:
        return None, None
    sdk_id = traits.get("aws.api#service", {}).get("sdkId", model_key)
    operations = {}
    roots = []
    op_ids = [op["target"] for op in service.get("operations", [])]
    for resource in service.get("resources", []):
        stack = [resource["target"]]
        while stack:
            res = shapes[stack.pop()]
            op_ids += [o["target"] for o in res.get("operations", [])]
            op_ids += [o["target"] for o in res.get("collectionOperations", [])]
            op_ids += [res[k]["target"] for k in ("create", "put", "read", "update", "delete", "list") if k in res]
            stack += [r["target"] for r in res.get("resources", [])]
    for op_id in op_ids:
        op = shapes[op_id]
        http = op.get("traits", {}).get("smithy.api#http", {})
        input_id = op.get("input", {}).get("target", "smithy.api#Unit")
        output_id = op.get("output", {}).get("target", "smithy.api#Unit")
        roots += [input_id, output_id]
        operations[short(op_id)] = [
            http.get("method"),
            http.get("uri"),
            "" if input_id == "smithy.api#Unit" else short(input_id),
            "" if output_id == "smithy.api#Unit" else short(output_id),
        ]
    kept = {}
    for shape_id in sorted(reachable(shapes, roots)):
        entry = shape_entry(shapes, shape_id)
        if entry is not None:
            kept[short(shape_id)] = entry
    doc = {
        "n": sdk_id,
        "p": protocol,
        "r": registered,
        "sg": traits.get("aws.auth#sigv4", {}).get("name", registered),
        "v": service.get("version", ""),
        "o": operations,
        "h": kept,
    }
    namespace = traits.get("smithy.api#xmlNamespace", {}).get("uri")
    if namespace:
        doc["xn"] = namespace
    return sdk_id.lower().replace(" ", ""), doc


def decoded(table):
    """A table's documents, independent of how a given zlib compresses them."""
    return {k: json.loads(gzip.decompress(base64.b64decode(v))) for k, v in table.items()}


def main():
    service_map = json.loads((ROOT / "aws-models/service-map.json").read_text())
    table = {}
    for path in sorted((ROOT / "aws-models").glob("*.json")):
        if path.name == "service-map.json":
            continue
        model = json.loads(path.read_text())
        registered = service_map.get(path.stem, {}).get("service_name", path.stem)
        key, doc = condense(path.stem, model, registered)
        if doc is None:
            continue
        data = json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()
        table[key] = base64.b64encode(gzip.compress(data, 9, mtime=0)).decode()
    if "--check" in sys.argv[1:]:
        if decoded(json.loads(OUT.read_text())) != decoded(table):
            print(f"{OUT.relative_to(ROOT)} is stale: run scripts/gen-sfn-sdk-models.py", file=sys.stderr)
            sys.exit(1)
        print(f"{OUT.relative_to(ROOT)} is up to date")
        return
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(table, separators=(",", ":"), sort_keys=True) + "\n")
    print(f"{len(table)} services, {OUT.stat().st_size} bytes")
    print(sorted(table))


if __name__ == "__main__":
    main()
