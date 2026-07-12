#!/usr/bin/env python3
"""Regenerate crates/fakecloud-sagemaker/src/generated.rs from aws-models/sagemaker.json.

Amazon SageMaker is an awsJson1.1 service with ~403 operations; hand-maintaining
the operation table, per-operation input constraints, output member shapes, list
element shapes, resource families and identifier members would be error-prone, so
they are generated directly from the Smithy model. Unlike the restJson1 IoT crate,
SageMaker routes every request by its `X-Amz-Target: SageMaker.<Operation>` header
and carries *all* inputs in the JSON body (no HTTP path/label/query bindings), so
the generated metadata is simpler: an operation verb, its resource family, the
identifier member used as the storage key, and the validation / projection shapes.

Run after refreshing aws-models/sagemaker.json:

    python3 scripts/generate-sagemaker-tables.py
"""
import json, os, re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
M = os.path.join(REPO, "aws-models", "sagemaker.json")
d = json.load(open(M))
shapes = d["shapes"]
NS = "com.amazonaws.sagemaker#"


def sn(t):
    return t.split("#")[1]


def resolve(tid):
    return shapes.get(tid)


def kind_of(tid):
    s = resolve(tid)
    if not s:
        p = tid.split("#")[1] if "#" in tid else tid
        if p in ("String", "Document"):
            return "Str"
        if p in ("Integer", "Long", "Short", "Byte", "BigInteger",
                 "PrimitiveInteger", "PrimitiveLong"):
            return "Int"
        if p in ("Float", "Double", "BigDecimal", "PrimitiveFloat",
                 "PrimitiveDouble"):
            return "Num"
        if p in ("Boolean", "PrimitiveBoolean"):
            return "Bool"
        if p == "Blob":
            return "Blob"
        if p == "Timestamp":
            return "Ts"
        return "Str"
    t = s["type"]
    if t == "structure":
        return "Struct"
    if t == "list":
        return "List"
    if t in ("map",):
        return "Map"
    if t == "union":
        return "Struct"
    if t == "enum":
        return "Str"
    if t == "string":
        return "Str"
    if t in ("integer", "long", "short", "byte", "biginteger"):
        return "Int"
    if t in ("float", "double", "bigdecimal"):
        return "Num"
    if t == "boolean":
        return "Bool"
    if t == "blob":
        return "Blob"
    if t == "timestamp":
        return "Ts"
    if t == "document":
        return "Str"
    return "Str"


def enum_values(tid):
    s = resolve(tid)
    if not s:
        return None
    if s["type"] == "enum":
        vals = []
        for mn, mm in s.get("members", {}).items():
            ev = mm.get("traits", {}).get("smithy.api#enumValue", mn)
            vals.append(ev)
        return vals
    et = s.get("traits", {}).get("smithy.api#enum")
    if et:
        return [e["value"] for e in et]
    return None


def constraints(tid, mtr):
    s = resolve(tid)
    st = (s or {}).get("traits", {})

    def pick(k):
        if k in mtr:
            return mtr[k]
        return st.get(k)

    ln = pick("smithy.api#length") or {}
    rg = pick("smithy.api#range") or {}
    return ln.get("min"), ln.get("max"), rg.get("min"), rg.get("max")


def struct_members(tid):
    s = resolve(tid)
    if not s or s["type"] not in ("structure", "union"):
        return []
    return list(s.get("members", {}).items())


VERBS = ["Create", "Describe", "Delete", "List", "Update", "Get", "Stop",
         "Start", "Register", "Deregister", "Add", "Batch", "Search", "Send",
         "Retry", "Import", "Query", "Enable", "Disable", "Associate",
         "Disassociate", "Put", "Attach", "Detach", "Extend", "Render"]


def singular(b):
    if b.endswith("ies"):
        return b[:-3] + "y"
    if b.endswith("sses"):
        return b[:-2]
    if b.endswith("s") and not b.endswith("ss"):
        return b[:-1]
    return b


def family_of(name):
    for v in VERBS:
        if name.startswith(v):
            base = name[len(v):]
            if v == "List":
                base = singular(base)
            return v, base
    return "OTHER", name


def verb_of(name):
    if name.startswith("Create"):
        return "Create"
    if name.startswith("Describe"):
        return "Get"
    if name.startswith("List"):
        return "List"
    if name.startswith("Update"):
        return "Update"
    if name.startswith("Delete"):
        return "Delete"
    return "Action"


def kebab(fam):
    s = re.sub(r"(?<!^)(?=[A-Z])", "-", fam).lower()
    return s


def input_member_names(inp):
    return [mn for mn, _ in struct_members(inp)]


def key_member_of(name, fam, inp):
    mems = input_member_names(inp)
    for cand in (fam + "Name", fam + "Id", fam + "Arn", fam, fam + "JobName"):
        if cand in mems:
            return cand
    # first required string member
    for mn, m in struct_members(inp):
        if "smithy.api#required" in m.get("traits", {}) and kind_of(m["target"]) == "Str":
            return mn
    if mems:
        return mems[0]
    return ""


ops = sorted([sn(k) for k, v in shapes.items() if v.get("type") == "operation"])
meta = {}
for name in ops:
    o = shapes[NS + name]
    verb = verb_of(name)
    _v, base = family_of(name)
    fam = base if verb != "List" else base
    inp = o.get("input", {}).get("target")
    out = o.get("output", {}).get("target")
    if inp and sn(inp) == "Unit":
        inp = None
    if out and sn(out) == "Unit":
        out = None
    errs = [sn(e["target"]) for e in o.get("errors", [])]

    key_member = ""
    if verb in ("Create", "Get", "Update", "Delete") and inp:
        key_member = key_member_of(name, fam, inp)

    # validation rules: required / constrained / enum body members
    rules = []
    if inp:
        for mn, m in struct_members(inp):
            mtr = m.get("traits", {})
            req = "smithy.api#required" in mtr
            tid = m["target"]
            kn = kind_of(tid)
            mn_min, mn_max, rmin, rmax = constraints(tid, mtr)
            evs = enum_values(tid) if kn == "Str" else None
            if not (req or mn_min is not None or mn_max is not None
                    or rmin is not None or rmax is not None or evs):
                continue
            rules.append({"wire": mn, "req": req, "kind": kn,
                          "min_len": mn_min, "max_len": mn_max,
                          "min_val": rmin, "max_val": rmax, "enums": evs})

    # output members (all body members for awsJson)
    omembers = []
    if out:
        for mn, m in struct_members(out):
            omembers.append({"wire": mn, "kind": kind_of(m["target"])})

    # first list-typed output member -> list projection
    list_elem = None
    list_scalar = False
    if out:
        for mn, m in struct_members(out):
            if kind_of(m["target"]) == "List":
                ls = resolve(m["target"])
                elt = ls.get("member", {}).get("target") if ls else None
                ek = kind_of(elt) if elt else "Struct"
                if ek in ("Str", "Int", "Num", "Bool", "Blob", "Ts"):
                    list_scalar = True
                elems = []
                if elt:
                    for emn, em in struct_members(elt):
                        elems.append({"wire": emn, "kind": kind_of(em["target"])})
                list_elem = {"wire": mn, "elems": elems}
                break

    meta[name] = {
        "verb": verb, "family": fam, "key_member": key_member,
        "arn_path": kebab(fam) if fam else "",
        "errors": errs, "rules": rules, "omembers": omembers,
        "list_elem": list_elem, "list_scalar": list_scalar,
        "has_input": bool(inp),
    }

print("ops:", len(meta))
from collections import Counter
print(Counter(m["verb"] for m in meta.values()))


def rs(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def opt_i(x):
    return "None" if x is None else f"Some({int(x)})"


def kind(k):
    return f"K::{k}"


verbmap = {"Create": "Create", "Get": "Get", "List": "List",
           "Update": "Update", "Delete": "Delete", "Action": "Action"}

out = []
out.append("// @generated by scripts/generate-sagemaker-tables.py (do not edit by hand)")
out.append("// Amazon SageMaker (sagemaker) awsJson1.1 operation metadata derived from the Smithy model.")
out.append("#![allow(clippy::all)]")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum K { Str, Int, Num, Bool, Blob, Ts, List, Map, Struct }")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum Verb { Create, Get, List, Update, Delete, Action }")
out.append("")
out.append("pub struct Rule {")
out.append("    pub wire: &'static str,")
out.append("    pub req: bool,")
out.append("    pub kind: K,")
out.append("    pub min_len: Option<u64>,")
out.append("    pub max_len: Option<u64>,")
out.append("    pub min_val: Option<i64>,")
out.append("    pub max_val: Option<i64>,")
out.append("    pub enums: &'static [&'static str],")
out.append("}")
out.append("")
out.append("pub struct OpMeta {")
out.append("    pub op: &'static str,")
out.append("    pub verb: Verb,")
out.append("    pub family: &'static str,")
out.append("    pub key_member: &'static str,")
out.append("    pub arn_path: &'static str,")
out.append("    pub has_input: bool,")
out.append("    pub errors: &'static [&'static str],")
out.append("    pub rules: &'static [Rule],")
out.append("    pub omembers: &'static [(&'static str, K)],")
out.append("    pub list_field: Option<&'static str>,")
out.append("    pub list_elems: &'static [(&'static str, K)],")
out.append("    pub list_scalar: bool,")
out.append("}")
out.append("")
out.append("pub static OPS: &[OpMeta] = &[")
for name in sorted(meta):
    m = meta[name]
    errs = "&[" + ", ".join(rs(e) for e in m["errors"]) + "]"
    rules = []
    for r in m["rules"]:
        enums = "&[" + ", ".join(rs(e) for e in (r["enums"] or [])) + "]"
        rules.append(
            "Rule { wire: %s, req: %s, kind: %s, min_len: %s, max_len: %s, min_val: %s, max_val: %s, enums: %s }" % (
                rs(r["wire"]), "true" if r["req"] else "false", kind(r["kind"]),
                opt_i(r["min_len"]), opt_i(r["max_len"]), opt_i(r["min_val"]),
                opt_i(r["max_val"]), enums))
    rules_s = "&[" + ", ".join(rules) + "]"
    omem = "&[" + ", ".join(f"({rs(o['wire'])}, {kind(o['kind'])})" for o in m["omembers"]) + "]"
    if m["list_elem"]:
        lf = rs(m["list_elem"]["wire"])
        le = "&[" + ", ".join(f"({rs(e['wire'])}, {kind(e['kind'])})" for e in m["list_elem"]["elems"]) + "]"
        lf_s = f"Some({lf})"
    else:
        lf_s = "None"
        le = "&[]"
    out.append(
        "    OpMeta { op: %s, verb: Verb::%s, family: %s, key_member: %s, arn_path: %s, has_input: %s, errors: %s, rules: %s, omembers: %s, list_field: %s, list_elems: %s, list_scalar: %s }," % (
            rs(name), verbmap[m["verb"]], rs(m["family"]), rs(m["key_member"]),
            rs(m["arn_path"]), "true" if m["has_input"] else "false",
            errs, rules_s, omem, lf_s, le,
            "true" if m["list_scalar"] else "false"))
out.append("];")
out.append("")
out.append("pub static ACTIONS: &[&str] = &[")
for name in sorted(meta):
    out.append("    %s," % rs(name))
out.append("];")
out.append("")

dest = os.path.join(REPO, "crates", "fakecloud-sagemaker", "src", "generated.rs")
os.makedirs(os.path.dirname(dest), exist_ok=True)
open(dest, "w").write("\n".join(out) + "\n")
print("wrote crates/fakecloud-sagemaker/src/generated.rs")
