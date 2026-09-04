#!/usr/bin/env python3
"""Regenerate crates/fakecloud-glue/src/constraints.rs from aws-models/glue.json.

For every operation, walks the top-level members of its input shape and records
the `@length`, `@range`, and enum constraints that apply to each one, resolving
one level of shape reference (the member's target shape carries the trait).
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODEL = ROOT / "aws-models" / "glue.json"
OUT = ROOT / "crates" / "fakecloud-glue" / "src" / "constraints.rs"

HEADER = '''//! Generated constraint table for Glue input validation. Built from
//! aws-models/glue.json: per-operation, per-top-level-field `@length`,
//! `@range`, and enum constraints. Used to reject constraint-violating
//! inputs with InvalidInputException, matching AWS validation.
//!
//! DO NOT EDIT BY HAND -- regenerate with scripts/generate-glue-constraints.py.

/// A single field constraint: (field, len_min, len_max, range_min, range_max, enum_values).
pub(crate) struct FieldConstraint {
    pub field: &'static str,
    pub len_min: Option<u64>,
    pub len_max: Option<u64>,
    pub range_min: Option<i64>,
    pub range_max: Option<i64>,
    pub enum_values: &'static [&'static str],
}

/// Look up the constraint list for an operation, or an empty slice.
pub(crate) fn constraints_for(action: &str) -> &'static [FieldConstraint] {
    match action {
'''


def enum_values(shape, shapes):
    """Enum members, whether modeled as an `enum` shape or a `@enum`-trait string."""
    if shape.get("type") == "enum":
        return [
            m.get("traits", {}).get("smithy.api#enumValue", name)
            for name, m in shape.get("members", {}).items()
        ]
    trait = shape.get("traits", {}).get("smithy.api#enum")
    if trait:
        return [e["value"] for e in trait]
    return []


def constraint_for(target, shapes):
    shape = shapes.get(target)
    if shape is None:
        return None
    traits = shape.get("traits", {})
    length = traits.get("smithy.api#length", {})
    rng = traits.get("smithy.api#range", {})
    vals = enum_values(shape, shapes)
    if not length and not rng and not vals:
        return None
    return {
        "len_min": length.get("min"),
        "len_max": length.get("max"),
        "range_min": rng.get("min"),
        "range_max": rng.get("max"),
        "enum": vals,
    }


def opt(v):
    return f"Some({v})" if v is not None else "None"


def main():
    model = json.loads(MODEL.read_text())
    shapes = model["shapes"]
    out = [HEADER]
    count = 0
    for sid, shape in sorted(shapes.items()):
        if shape.get("type") != "operation":
            continue
        name = sid.split("#")[1]
        inp = shape.get("input", {}).get("target")
        if not inp or inp not in shapes:
            continue
        fields = []
        for member, m in shapes[inp].get("members", {}).items():
            c = constraint_for(m["target"], shapes)
            if c:
                fields.append((member, c))
        if not fields:
            continue
        count += 1
        out.append(f'        "{name}" => &[\n')
        for member, c in fields:
            out.append("            FieldConstraint {\n")
            out.append(f'                field: "{member}",\n')
            out.append(f'                len_min: {opt(c["len_min"])},\n')
            out.append(f'                len_max: {opt(c["len_max"])},\n')
            out.append(f'                range_min: {opt(c["range_min"])},\n')
            out.append(f'                range_max: {opt(c["range_max"])},\n')
            if c["enum"]:
                out.append("                enum_values: &[\n")
                for v in c["enum"]:
                    out.append(f'                    "{v}",\n')
                out.append("                ],\n")
            else:
                out.append("                enum_values: &[],\n")
            out.append("            },\n")
        out.append("        ],\n")
    out.append("        _ => &[],\n    }\n}\n")

    # Operations that declare InvalidInputException. Pagination uses this to
    # decide whether a malformed NextToken can be reported as an error at all.
    declaring = sorted(
        sid.split("#")[1]
        for sid, shape in shapes.items()
        if shape.get("type") == "operation"
        and any(
            e["target"].endswith("#InvalidInputException")
            for e in shape.get("errors", [])
        )
    )
    out.append(
        "\n/// Whether an operation declares `InvalidInputException` in the Smithy\n"
        "/// model. Operations that do not cannot report one, whatever the input.\n"
        "pub(crate) fn declares_invalid_input(action: &str) -> bool {\n"
        "    matches!(\n        action,\n"
    )
    out.append("        " + "\n            | ".join(f'"{n}"' for n in declaring) + "\n")
    out.append("    )\n}\n")
    OUT.write_text("".join(out))
    print(f"wrote {OUT.relative_to(ROOT)}: {count} operations", file=sys.stderr)


if __name__ == "__main__":
    main()
