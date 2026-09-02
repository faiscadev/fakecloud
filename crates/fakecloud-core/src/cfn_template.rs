//! Parsing for CloudFormation-shaped template bodies.
//!
//! A template body arrives as either JSON or YAML, and the YAML dialect
//! CloudFormation accepts is not plain YAML: intrinsic functions may be
//! written as short-form node tags (`!Ref`, `!GetAtt`, `!Sub`, ...). A plain
//! YAML-to-JSON deserialize rejects those outright — the whole document fails,
//! not just the tagged node — so a template using the short forms parsed to
//! nothing at all and stacks came up empty (#2480).
//!
//! Everything that reads a template body (CloudFormation, Serverless
//! Application Repository, Config conformance packs) goes through here, so a
//! short form behaves exactly like its `{"Fn::Sub": ...}` long-form spelling.

use serde_json::{Map, Value as Json};
use serde_yaml::value::TaggedValue;
use serde_yaml::Value as Yaml;

/// Short-form tags that expand to `{"Fn::<Tag>": <arg>}`. Covers the template
/// intrinsics, the condition functions, and the `Rules`-section functions —
/// CloudFormation defines no others, which is what lets an unrecognised tag be
/// treated as an error rather than quietly unwrapped.
const FN_TAGS: &[&str] = &[
    "And",
    "Base64",
    "Cidr",
    "Contains",
    "EachMemberEquals",
    "EachMemberIn",
    "Equals",
    "FindInMap",
    "GetAZs",
    "GetAtt",
    "If",
    "ImportValue",
    "Join",
    "Length",
    "Not",
    "Or",
    "RefAll",
    "Select",
    "Split",
    "Sub",
    "ToJsonString",
    "Transform",
    "ValueOf",
    "ValueOfAll",
];

/// Short-form tags that expand to a bare `{"<Tag>": <arg>}` key. `Ref` and
/// `Condition` are the two CloudFormation spells without the `Fn::` prefix.
const BARE_TAGS: &[&str] = &["Ref", "Condition"];

/// Parse a CloudFormation template body (JSON or YAML, short-form tags
/// included) into a JSON value with every intrinsic in its long form.
///
/// A body starting with `{` is tried as JSON first — YAML is a JSON superset,
/// but going through `serde_json` preserves exact numeric precision for the
/// programmatically-generated templates that dominate that case. It still
/// falls back to YAML on a JSON parse error, because YAML *flow* style also
/// opens with `{` (`{Resources: {A: {Type: X}}}` is valid YAML and invalid
/// JSON — unquoted keys), and committing to JSON on the first character would
/// silently drop such a template.
pub fn parse_template_body(body: &str) -> Result<Json, String> {
    let body = strip_bom(body);
    // Before either dialect: the `{`-leading branch falls back to serde_yaml
    // directly, so a guard living only in `parse_yaml` would not cover it.
    if exceeds_nesting_limit(body) {
        return Err(format!(
            "Invalid template: nesting deeper than {MAX_NESTING_DEPTH} levels"
        ));
    }
    if body.trim_start().starts_with('{') {
        return match serde_json::from_str(body) {
            Ok(value) => Ok(value),
            Err(json_err) => match serde_yaml::from_str::<Yaml>(body) {
                // It is valid YAML after all, so any remaining problem is a
                // YAML-stage one (an unknown intrinsic tag, a non-finite
                // number) and that message is the actionable diagnostic.
                // Reporting the JSON error here would point at syntax that is
                // legal in the dialect actually being used.
                Ok(yaml) => yaml_to_json(yaml).map_err(|e| format!("Invalid YAML template: {e}")),
                // Neither dialect parses. For a `{`-leading body the JSON
                // error is what the author needs.
                Err(_) => Err(format!("Invalid JSON template: {json_err}")),
            },
        };
    }
    parse_yaml(body)
}

/// Nesting depth past which a body is refused before it reaches the parser.
/// `serde_json` enforces its own recursion limit; `serde_yaml` does not, and
/// both it and the `yaml_to_json` walk below recurse, so a deeply-nested body
/// on an unauthenticated CreateStack could exhaust the stack. Real templates
/// nest a handful of levels; CloudFormation's own documented ceiling is far
/// below this.
///
/// This bounds *depth* only. It does not bound alias *expansion*: a YAML
/// "billion laughs" body (`a: &a [x,x,x]` / `b: &b [*a,*a,*a]` / ...) is only
/// two levels deep and passes this check, while `serde_yaml` re-deserializes
/// at each alias and blows up the allocation. That vector predates this guard
/// (every non-`{`-leading body already reached `serde_yaml`) and closing it
/// needs a real scanner — a textual anchor/alias scan would reject the very
/// common `Resource: "*"` — so it is deliberately out of scope here.
const MAX_NESTING_DEPTH: usize = 512;

fn parse_yaml(body: &str) -> Result<Json, String> {
    let yaml: Yaml =
        serde_yaml::from_str(body).map_err(|e| format!("Invalid YAML template: {e}"))?;
    yaml_to_json(yaml).map_err(|e| format!("Invalid YAML template: {e}"))
}

/// Cheap pre-scan for pathological nesting, covering both YAML styles.
///
/// *Flow* collections nest with `[` / `{`. *Block* collections nest with
/// indentation and with compact sequence markers -- `- - - - a` is two bytes
/// per level, so a few hundred KB reaches a depth that overflows the parser's
/// stack. Counting only the flow openers would leave the cheaper shape wide
/// open on an unauthenticated CreateStack.
///
/// Quoted scalars, block scalars (`|` / `>`) and `#` comments are skipped, so
/// brackets that are content rather than structure — an embedded IAM policy
/// document, a SAM `InlineCode` body, EC2 `UserData` — cannot push a shallow
/// template over the limit.
fn exceeds_nesting_limit(body: &str) -> bool {
    let mut flow_depth = 0usize;
    // A quote only *opens* where a scalar can start — at the beginning of a
    // line or straight after `:`/`,`/`[`/`{`/`-`/whitespace. YAML allows a bare
    // apostrophe inside a plain scalar (`baz: don't`), and treating that as an
    // opening quote would swallow everything after it.
    //
    // Quote state carries across lines, because a quoted scalar may legally
    // span them — but only for `MAX_QUOTE_LINES`. Openers seen while inside a
    // quote are counted separately: if the quote closes in time, the run
    // really was a scalar and they are discarded; if it outlives that budget
    // (or the body ends inside it), the "scalar" was never one and they are
    // added back.
    //
    // The budget is what keeps this fail-closed. Carrying quote state without
    // one reopens the bypass: alternating an apostrophe per line makes each
    // line close the previous quote and open a new one, so half the brackets
    // are suppressed and a bomb just has to be twice as long. Real multiline
    // scalars in a template close within a line or two.
    let mut quote: Option<u8> = None;
    let mut escaped = false;
    // True while the scanner sits where a *value* can begin: the start of a
    // line, or just past `:` / `,` / `[` / `{` / `-` and any spaces after it.
    // Any other byte clears it. A quote only opens at such a position, so the
    // apostrophe in a plain scalar (`baz: don't`, `x ', [`) is content rather
    // than a delimiter — which is what YAML says it is, and what stops an
    // apostrophe from suppressing the brackets that follow it.
    let mut at_value_start = true;
    let mut suppressed = 0usize;
    let mut quote_lines = 0usize;
    // Indentation of the key that opened a block scalar (`Body: |`), while its
    // content is being skipped. Block scalars are everywhere in real templates
    // — SAM `InlineCode`, EC2 `UserData`, inline policy documents — and their
    // content is a string, so its brackets are not nesting. YAML will not
    // recurse through them either, so skipping cannot hide a bomb.
    let mut block_scalar_indent: Option<usize> = None;
    for line in body.lines() {
        let line_indent = line.len() - line.trim_start().len();
        if let Some(opener_indent) = block_scalar_indent {
            // Blank lines and anything more-indented than the key belong to
            // the block scalar.
            if line.trim().is_empty() || line_indent > opener_indent {
                continue;
            }
            block_scalar_indent = None;
        }
        // Block depth: the line's indentation plus its compact `- ` markers.
        // Each level of block nesting costs at least one column, so the
        // indentation is itself an upper bound on the depth reached here.
        let mut block_depth = line_indent;
        let mut rest = line.trim_start();
        while let Some(tail) = rest.strip_prefix("- ") {
            block_depth += 1;
            rest = tail.trim_start();
        }
        if rest == "-" {
            block_depth += 1;
        }
        if block_depth > MAX_NESTING_DEPTH {
            return true;
        }

        // Flow depth carries across lines, since a flow collection may span
        // them. Brackets inside a quoted scalar are content, not structure --
        // an embedded IAM policy or inline nested template can carry hundreds
        // of braces in one string -- so quotes are tracked and their contents
        // skipped. A `#` outside quotes starts a comment.
        //
        // Where a `#` comment begins on this line, if any. The block-scalar
        // header check below must look at the content before it.
        let mut content_end = line.len();
        let mut prev_byte = b' ';
        for (idx, b) in line.bytes().enumerate() {
            match quote {
                Some(q) => {
                    // Only double-quoted YAML scalars honour backslash
                    // escapes; in single-quoted ones a backslash is literal.
                    if escaped {
                        escaped = false;
                    } else if q == b'"' && b == b'\\' {
                        escaped = true;
                    } else if b == q {
                        quote = None;
                        suppressed = 0;
                    } else if matches!(b, b'[' | b'{') {
                        suppressed += 1;
                    }
                }
                None => match b {
                    b'"' | b'\'' if at_value_start => {
                        quote = Some(b);
                        at_value_start = false;
                    }
                    // YAML starts a comment at `#` only at the beginning of
                    // a line or after whitespace; `a: b#c` is a plain scalar.
                    b'#' if matches!(prev_byte, b' ' | b'\t') || idx == 0 => {
                        content_end = idx;
                        break;
                    }
                    b'[' | b'{' => {
                        flow_depth += 1;
                        if flow_depth > MAX_NESTING_DEPTH {
                            return true;
                        }
                        at_value_start = true;
                    }
                    b']' | b'}' => {
                        flow_depth = flow_depth.saturating_sub(1);
                        at_value_start = false;
                    }
                    b':' | b',' | b'-' => at_value_start = true,
                    b' ' | b'\t' => {}
                    _ => at_value_start = false,
                },
            }
            prev_byte = b;
        }
        // A block scalar header (`Body: |`, `Body: >-`) means every
        // more-indented line after it is string content.
        if quote.is_none() && opens_block_scalar(&line[..content_end]) {
            block_scalar_indent = Some(line_indent);
        }

        // A new line is itself a value-start position. The escape state does
        // NOT survive it: a trailing backslash in a double-quoted scalar
        // escapes the line break itself, not the first byte of the next line,
        // and carrying it could swallow the real closing quote.
        at_value_start = true;
        escaped = false;

        if quote.is_some() {
            quote_lines += 1;
            if quote_lines > MAX_QUOTE_LINES {
                // Outlived the budget: treat it as never having been a scalar.
                flow_depth = flow_depth.saturating_add(suppressed);
                if flow_depth > MAX_NESTING_DEPTH {
                    return true;
                }
                quote = None;
                suppressed = 0;
                quote_lines = 0;
            }
        } else {
            quote_lines = 0;
        }
    }
    // The body ended inside a quote: it was never a scalar, so put back what
    // it suppressed.
    if quote.is_some() && flow_depth.saturating_add(suppressed) > MAX_NESTING_DEPTH {
        return true;
    }
    false
}

/// Whether a line opens a YAML block scalar: a `|` or `>` at the end of the
/// line's *content*, optionally followed by the chomping/indentation
/// indicators (`-`, `+`, a digit). Everything more-indented after it is string
/// content.
///
/// The caller passes the line with any trailing `#` comment removed, which
/// matters in both directions: `Body: | # note` really does open a block
/// scalar, and `Resources: # |` really does not — treating the latter as one
/// would skip every indented line after it and step around the depth guard.
fn opens_block_scalar(line: &str) -> bool {
    // Strip the key and any sequence markers, then require the VALUE to be
    // exactly the indicator. A suffix test is not enough: `Body: ||` and
    // `Command: echo a |` both end with `|` without opening anything, and
    // treating them as headers skips every indented line after them — which
    // steps around the depth guard.
    let mut value = line.trim();
    while let Some(rest) = value.strip_prefix("- ") {
        value = rest.trim_start();
    }
    if let Some((_key, rest)) = value.split_once(KEY_VALUE_SEP) {
        value = rest.trim();
    } else if value.ends_with(':') {
        value = "";
    }
    is_block_indicator(value)
}

/// The key/value separator. Split on `": "` rather than a bare colon so a
/// value that contains one (`Arn: arn:aws:...`) is not truncated.
const KEY_VALUE_SEP: &str = ": ";

/// A block-scalar indicator: `|` or `>`, optionally with the chomping (`-`,
/// `+`) and explicit-indentation (a digit) indicators in either order.
fn is_block_indicator(value: &str) -> bool {
    let mut chars = value.chars();
    if !matches!(chars.next(), Some('|') | Some('>')) {
        return false;
    }
    let rest: Vec<char> = chars.collect();
    match rest.len() {
        0 => true,
        1 => rest[0] == '-' || rest[0] == '+' || rest[0].is_ascii_digit(),
        2 => {
            (rest[0].is_ascii_digit() && (rest[1] == '-' || rest[1] == '+'))
                || ((rest[0] == '-' || rest[0] == '+') && rest[1].is_ascii_digit())
        }
        _ => false,
    }
}

/// How many physical lines a quoted scalar may span before the pre-scan stops
/// believing it is one. Real templates keep quoted scalars on a line or two;
/// long embedded documents use block scalars (`|`), which this budget does not
/// constrain because their content is not inside quotes.
const MAX_QUOTE_LINES: usize = 8;

/// Strip a UTF-8 byte-order mark. `str::trim_start` does not remove U+FEFF,
/// and PowerShell's `Out-File` / `Set-Content` write one by default — so a
/// BOM'd template's first line reads `\u{FEFF}Resources:`, which fails every
/// prefix check and sends the body down the lenient path (#2480 again).
fn strip_bom(body: &str) -> &str {
    body.strip_prefix('\u{FEFF}').unwrap_or(body)
}

/// Whether a body is meant to be a CloudFormation template *document*, as
/// opposed to a placeholder scalar (the conformance probe sends `"test"`-style
/// strings for `TemplateBody`).
///
/// A body that parses is judged on its shape: only a string scalar or an
/// absent/empty body is a placeholder.
/// One that does NOT parse still has to be classified — and it is exactly the
/// interesting case, since a syntax error (a stray tab, a bad indent, an
/// unbalanced bracket) is the most common way a real template breaks. Re-using
/// the parser here would answer "not a template" for every syntax error and
/// send it down the lenient degrade-to-empty path, which is the silent no-op
/// #2480 is about. So fall back to a shape check on the raw text.
pub fn is_template_document(body: &str) -> bool {
    let body = strip_bom(body);
    if let Ok(value) = parse_template_body(body) {
        // Presence, not shape: `Resources:` holding a sequence instead of a
        // mapping is a common YAML slip that the template parser rejects. It
        // is still unmistakably someone's template, so it must fail loudly
        // rather than degrade to an empty stack.
        //
        // The other top-level sections count too. A template that declares
        // `Parameters` or `AWSTemplateFormatVersion` but omits `Resources` is
        // rejected by the parser ("Template must contain a Resources
        // section"), and judging it on `Resources` alone would send that error
        // down the lenient path — accepting an empty stack, or on update
        // removing every existing resource. `Description` is deliberately not
        // in the list: on its own it is too generic to mark a body as a
        // template.
        // Only a string scalar and an absent/empty body stay lenient: the
        // first is the placeholder shape the probe sends
        // (`TemplateBody="test"`), the second is an omitted member. Anything
        // else — a mapping (whatever sections it has, including none), a
        // sequence, a number, a bool — is someone submitting a document, and
        // real CloudFormation rejects the ones that aren't valid templates
        // rather than building an empty stack from them.
        return !matches!(value, Json::String(_) | Json::Null);
    }
    looks_like_template_text(body)
}

/// Top-level sections that mark an *unparseable* body as someone's
/// CloudFormation template, per the template anatomy AWS documents. A body
/// that parses is judged by its shape instead (see `is_template_document`).
const TEMPLATE_SECTIONS: &[&str] = &[
    "AWSTemplateFormatVersion",
    "Conditions",
    "Mappings",
    "Metadata",
    "Outputs",
    "Parameters",
    "Resources",
    "Rules",
    "Transform",
];

/// Text-level shape check for a body that would not parse: does it *look* like
/// someone's template? Keyed on a top-level `Resources` section, the one
/// section CloudFormation requires.
fn looks_like_template_text(body: &str) -> bool {
    if body.trim_start().starts_with('{') {
        // A `{`-leading body that parses as neither JSON nor YAML is a broken
        // document, not a placeholder — the synthetic scalars are bare strings
        // (`test`), never braced. Substring-matching `Resources` here would be
        // wrong in both directions: a JSON template truncated before its
        // `"Resources"` key would be judged a placeholder and silently produce
        // an empty stack, while `"Description": "Resources: see docs"` would be
        // force-failed.
        return true;
    }
    // A top-level YAML key sits at column zero. It may be quoted, and may
    // carry whitespace before its colon — both legal, and both emitted by
    // real generators.
    body.lines().any(declares_resources)
}

fn declares_resources(line: &str) -> bool {
    let unquoted = line
        .strip_prefix('"')
        .or_else(|| line.strip_prefix('\''))
        .unwrap_or(line);
    // The same section list the parsed branch uses. Keying only on
    // `Resources:` would miss a template whose syntax error sits in (or
    // truncates at) the `Parameters` / `Mappings` block above it, sending it
    // back down the silent-empty-stack path.
    TEMPLATE_SECTIONS.iter().any(|section| {
        let Some(rest) = unquoted.strip_prefix(section) else {
            return false;
        };
        let rest = rest
            .strip_prefix('"')
            .or_else(|| rest.strip_prefix('\''))
            .unwrap_or(rest);
        // Require the colon. A real top-level key always has one, and without
        // it any unparseable body that merely contains one of these words (a
        // pasted log, a CSV header) would be classified as a template —
        // turning a lenient degrade into a hard error.
        rest.trim_start().starts_with(':')
    })
}

/// Convenience wrapper for callers that only want a template-shaped object and
/// treat anything else (a placeholder scalar, a parse failure) as absent.
pub fn parse_template_object(body: &str) -> Option<Json> {
    parse_template_body(body).ok().filter(Json::is_object)
}

fn yaml_to_json(value: Yaml) -> Result<Json, String> {
    Ok(match value {
        Yaml::Null => Json::Null,
        Yaml::Bool(b) => Json::Bool(b),
        Yaml::Number(n) => number_to_json(&n)?,
        Yaml::String(s) => Json::String(s),
        Yaml::Sequence(seq) => Json::Array(
            seq.into_iter()
                .map(yaml_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Yaml::Mapping(map) => {
            let mut obj = Map::new();
            for (k, v) in map {
                obj.insert(mapping_key(k)?, yaml_to_json(v)?);
            }
            Json::Object(obj)
        }
        Yaml::Tagged(tagged) => tagged_to_json(*tagged)?,
    })
}

/// JSON object keys are strings; a YAML mapping key that isn't one (`1: foo`)
/// takes its JSON rendering, matching how the key would have been written in
/// the equivalent JSON template.
fn mapping_key(key: Yaml) -> Result<String, String> {
    Ok(match yaml_to_json(key)? {
        Json::String(s) => s,
        other => other.to_string(),
    })
}

fn number_to_json(n: &serde_yaml::Number) -> Result<Json, String> {
    if let Some(i) = n.as_i64() {
        return Ok(Json::Number(i.into()));
    }
    if let Some(u) = n.as_u64() {
        return Ok(Json::Number(u.into()));
    }
    // YAML has `.nan` / `.inf`; JSON has no representation for either, and no
    // CloudFormation property legitimately holds one. Surfacing the bad value
    // beats silently rewriting it to `null`, which reads downstream as a
    // deliberately-absent property.
    n.as_f64()
        .and_then(serde_json::Number::from_f64)
        .map(Json::Number)
        .ok_or_else(|| format!("number {n} has no JSON representation"))
}

fn tagged_to_json(tagged: TaggedValue) -> Result<Json, String> {
    let rendered = tagged.tag.to_string();
    let name = rendered.strip_prefix('!').unwrap_or(&rendered);
    let arg = yaml_to_json(tagged.value)?;
    let key = if BARE_TAGS.contains(&name) {
        name.to_string()
    } else if FN_TAGS.contains(&name) {
        format!("Fn::{name}")
    } else {
        // An unrecognised single-`!` tag. CloudFormation defines a closed set
        // of short forms, so this is a typo (`!GettAtt`) or an unsupported
        // function. Unwrapping it to its argument would turn `!GettAtt
        // Topic.Arn` into the literal string "Topic.Arn" and provision a
        // wrong-but-plausible value under a CREATE_COMPLETE stack — the same
        // silent-wrong-value class this module exists to close.
        return Err(format!("unknown intrinsic tag !{name}"));
    };
    let mut obj = Map::new();
    obj.insert(key, arg);
    Ok(Json::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn short_form_ref_becomes_long_form() {
        let parsed = parse_template_body(
            r"
Resources:
  ReproBucket:
    Type: AWS::S3::Bucket
Outputs:
  BucketName:
    Value: !Ref ReproBucket
",
        )
        .expect("template parses");
        assert_eq!(
            parsed["Outputs"]["BucketName"]["Value"],
            json!({"Ref": "ReproBucket"})
        );
        assert_eq!(
            parsed["Resources"]["ReproBucket"]["Type"],
            "AWS::S3::Bucket"
        );
    }

    #[test]
    fn every_intrinsic_short_form_expands() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Sub: !Sub "${AWS::StackName}-bucket"
      GetAtt: !GetAtt Other.Arn
      GetAttList: !GetAtt [Other, Arn]
      Join: !Join ["-", [a, b]]
      Select: !Select [0, [a, b]]
      Split: !Split [",", "a,b"]
      Base64: !Base64 hello
      FindInMap: !FindInMap [Map, Key, Value]
      GetAZs: !GetAZs us-east-1
      ImportValue: !ImportValue OtherStackExport
      Cidr: !Cidr ["10.0.0.0/16", 6, 5]
      Length: !Length [a, b]
      ToJsonString: !ToJsonString {a: b}
      Transform: !Transform {Name: Macro}
"#,
        )
        .expect("template parses");
        let props = &parsed["Resources"]["Thing"]["Properties"];
        assert_eq!(props["Sub"], json!({"Fn::Sub": "${AWS::StackName}-bucket"}));
        assert_eq!(props["GetAtt"], json!({"Fn::GetAtt": "Other.Arn"}));
        assert_eq!(props["GetAttList"], json!({"Fn::GetAtt": ["Other", "Arn"]}));
        assert_eq!(props["Join"], json!({"Fn::Join": ["-", ["a", "b"]]}));
        assert_eq!(props["Select"], json!({"Fn::Select": [0, ["a", "b"]]}));
        assert_eq!(props["Split"], json!({"Fn::Split": [",", "a,b"]}));
        assert_eq!(props["Base64"], json!({"Fn::Base64": "hello"}));
        assert_eq!(
            props["FindInMap"],
            json!({"Fn::FindInMap": ["Map", "Key", "Value"]})
        );
        assert_eq!(props["GetAZs"], json!({"Fn::GetAZs": "us-east-1"}));
        assert_eq!(
            props["ImportValue"],
            json!({"Fn::ImportValue": "OtherStackExport"})
        );
        assert_eq!(props["Cidr"], json!({"Fn::Cidr": ["10.0.0.0/16", 6, 5]}));
        assert_eq!(props["Length"], json!({"Fn::Length": ["a", "b"]}));
        assert_eq!(
            props["ToJsonString"],
            json!({"Fn::ToJsonString": {"a": "b"}})
        );
        assert_eq!(
            props["Transform"],
            json!({"Fn::Transform": {"Name": "Macro"}})
        );
    }

    #[test]
    fn condition_short_forms_expand() {
        let parsed = parse_template_body(
            r#"
Conditions:
  IsProd: !Equals [!Ref Env, prod]
  NotProd: !Not [!Condition IsProd]
  Either: !Or [!Condition IsProd, !Condition NotProd]
  Both: !And [!Condition IsProd, !Condition NotProd]
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Name: !If [IsProd, prod-bucket, dev-bucket]
"#,
        )
        .expect("template parses");
        assert_eq!(
            parsed["Conditions"]["IsProd"],
            json!({"Fn::Equals": [{"Ref": "Env"}, "prod"]})
        );
        assert_eq!(
            parsed["Conditions"]["NotProd"],
            json!({"Fn::Not": [{"Condition": "IsProd"}]})
        );
        assert_eq!(
            parsed["Conditions"]["Either"],
            json!({"Fn::Or": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]})
        );
        assert_eq!(
            parsed["Conditions"]["Both"],
            json!({"Fn::And": [{"Condition": "IsProd"}, {"Condition": "NotProd"}]})
        );
        assert_eq!(
            parsed["Resources"]["Thing"]["Properties"]["Name"],
            json!({"Fn::If": ["IsProd", "prod-bucket", "dev-bucket"]})
        );
    }

    #[test]
    fn nested_short_forms_resolve_inside_out() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::SQS::Queue
    Properties:
      Name: !Join ["-", [!Ref Prefix, !GetAtt Other.Arn]]
"#,
        )
        .expect("template parses");
        assert_eq!(
            parsed["Resources"]["Thing"]["Properties"]["Name"],
            json!({"Fn::Join": ["-", [{"Ref": "Prefix"}, {"Fn::GetAtt": "Other.Arn"}]]})
        );
    }

    #[test]
    fn long_form_yaml_is_unchanged() {
        let parsed = parse_template_body(
            r#"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Name:
        Fn::Sub: "${AWS::StackName}-bucket"
      Owner:
        Ref: OwnerParam
"#,
        )
        .expect("template parses");
        let props = &parsed["Resources"]["Thing"]["Properties"];
        assert_eq!(
            props["Name"],
            json!({"Fn::Sub": "${AWS::StackName}-bucket"})
        );
        assert_eq!(props["Owner"], json!({"Ref": "OwnerParam"}));
    }

    #[test]
    fn json_templates_still_parse() {
        let parsed = parse_template_body(
            r#"{"Resources": {"Thing": {"Type": "AWS::S3::Bucket", "Properties": {"N": 1.5}}}}"#,
        )
        .expect("template parses");
        assert_eq!(parsed["Resources"]["Thing"]["Properties"]["N"], json!(1.5));
    }

    #[test]
    fn unknown_tags_are_an_error() {
        // This originally asserted the tag unwrapped to its value. That is
        // precisely the silent-wrong-value behaviour the module exists to
        // prevent: the property would provision as the string "hello" under a
        // CREATE_COMPLETE stack, with nothing reported.
        let err = parse_template_body(
            r"
Resources:
  Thing:
    Type: AWS::S3::Bucket
    Properties:
      Custom: !SomethingElse hello
",
        )
        .expect_err("an unrecognised intrinsic must be reported");
        assert!(
            err.contains("unknown intrinsic tag !SomethingElse"),
            "{err}"
        );
    }

    #[test]
    fn placeholder_bodies_are_not_objects() {
        assert!(parse_template_object("test").is_none());
        assert_eq!(parse_template_body("test").expect("scalar parses"), "test");
    }

    #[test]
    fn malformed_yaml_reports_an_error() {
        let err = parse_template_body("Resources:\n  - [unbalanced\n").expect_err("must fail");
        assert!(err.starts_with("Invalid YAML template:"), "{err}");
    }

    #[test]
    fn yaml_flow_style_body_still_parses() {
        // Valid YAML, invalid JSON (unquoted keys) despite the leading `{`.
        // Committing to JSON on the first character would drop it silently.
        let parsed =
            parse_template_body("{Resources: {A: {Type: AWS::SQS::Queue}}}").expect("parses");
        assert_eq!(parsed["Resources"]["A"]["Type"], "AWS::SQS::Queue");
        assert!(parse_template_object("{Resources: {A: {Type: AWS::SQS::Queue}}}").is_some());
    }

    #[test]
    fn json_body_that_is_neither_reports_the_json_error() {
        let err = parse_template_body("{\"Resources\": [oops").expect_err("must fail");
        assert!(err.starts_with("Invalid JSON template:"), "{err}");
    }

    #[test]
    fn pathological_nesting_is_refused_before_parsing() {
        // serde_yaml has no recursion limit of its own, and this path is
        // reachable unauthenticated via CreateStack.
        // Both entry shapes: a `{`-leading body reaches serde_yaml through the
        // JSON-failure fallback, so the guard cannot live in `parse_yaml`.
        for bomb in [
            format!("{}{}", "{", "[".repeat(100_000)),
            "[".repeat(100_000),
            // Compact block sequences nest with no brackets at all, two bytes
            // per level — the cheaper shape, and the one a flow-only guard
            // misses.
            "- ".repeat(100_000),
            // Pure indentation nesting.
            format!("{}a: 1", " ".repeat(100_000)),
        ] {
            let err = parse_template_body(&bomb).expect_err("must be refused");
            assert!(err.contains("nesting deeper than"), "{err}");
        }
        // A realistically-nested template is unaffected.
        assert!(parse_template_body(&format!(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      V: {}{}\n",
            "[".repeat(20),
            "]".repeat(20)
        ))
        .is_ok());
    }

    #[test]
    fn flow_style_body_reports_the_yaml_stage_error() {
        // Valid YAML, invalid JSON. The real problem is the misspelled
        // intrinsic, so reporting the JSON parse error would point the author
        // at syntax that is legal in the dialect they actually used.
        let err = parse_template_body(
            "{Resources: {Q: {Type: AWS::SQS::Queue, Properties: {V: !GettAtt A.B}}}}",
        )
        .expect_err("must fail");
        assert!(err.contains("unknown intrinsic tag !GettAtt"), "{err}");
        assert!(!err.contains("Invalid JSON template"), "{err}");
    }

    #[test]
    fn syntax_errors_are_still_recognised_as_template_documents() {
        // The #2480 case: a template that does not parse must still be
        // classified as a template, so the caller can fail loudly instead of
        // degrading to an empty stack.
        let tab_indent = "Resources:\n\tBad: indented with a tab\n";
        assert!(parse_template_body(tab_indent).is_err());
        assert!(is_template_document(tab_indent));

        let unbalanced = "Resources:\n  Queue:\n    Type: [AWS::SQS::Queue\n";
        assert!(parse_template_body(unbalanced).is_err());
        assert!(is_template_document(unbalanced));

        // Unbalanced in both dialects, so neither the JSON nor the YAML pass
        // can rescue it.
        let bad_json = "{\"Resources\": [oops";
        assert!(parse_template_body(bad_json).is_err());
        assert!(is_template_document(bad_json));

        // Truncated before the `Resources` key ever appears. A substring test
        // would call this a placeholder and silently build an empty stack.
        let truncated = "{\"AWSTemplateFormatVersion\": \"2010-09-09\", \"Desc";
        assert!(parse_template_body(truncated).is_err());
        assert!(is_template_document(truncated));
    }

    #[test]
    fn unknown_tags_are_rejected_not_unwrapped() {
        // A one-character typo used to unwrap to the literal string
        // "Topic.Arn" and provision a wrong-but-plausible value under a
        // CREATE_COMPLETE stack.
        let err = parse_template_body(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      V: !GettAtt Topic.Arn\n",
        )
        .expect_err("a misspelled intrinsic must not be silently unwrapped");
        assert!(err.contains("unknown intrinsic tag !GettAtt"), "{err}");
    }

    #[test]
    fn rules_section_short_forms_expand() {
        let parsed = parse_template_body(
            r#"
Rules:
  R:
    Assertions:
      - Assert: !Contains [[a, b], !Ref Thing]
      - Assert: !EachMemberEquals [[a], a]
      - Assert: !EachMemberIn [[a], [a, b]]
      - Assert: !ValueOf [Param, Tags]
      - Assert: !ValueOfAll ["AWS::EC2::Subnet::Id", VpcId]
      - Assert: !RefAll "AWS::EC2::VPC::Id"
Resources:
  Q:
    Type: AWS::SQS::Queue
"#,
        )
        .expect("Rules-section short forms are CloudFormation intrinsics");
        let asserts = &parsed["Rules"]["R"]["Assertions"];
        assert_eq!(
            asserts[0]["Assert"],
            json!({"Fn::Contains": [["a", "b"], {"Ref": "Thing"}]})
        );
        assert_eq!(
            asserts[1]["Assert"],
            json!({"Fn::EachMemberEquals": [["a"], "a"]})
        );
        assert_eq!(
            asserts[2]["Assert"],
            json!({"Fn::EachMemberIn": [["a"], ["a", "b"]]})
        );
        assert_eq!(
            asserts[3]["Assert"],
            json!({"Fn::ValueOf": ["Param", "Tags"]})
        );
        assert_eq!(
            asserts[5]["Assert"],
            json!({"Fn::RefAll": "AWS::EC2::VPC::Id"})
        );
    }

    #[test]
    fn double_bang_tags_pass_through() {
        // `!!`-prefixed tags never reach `tagged_to_json` at all: libyaml
        // expands the `!!` handle and serde_yaml resolves the result, so the
        // node arrives already plain. Verified for `!!binary`, `!!timestamp`,
        // `!!custom` and `!!str` — all come back as values, never
        // `Value::Tagged`. That is why the unknown-intrinsic check below can
        // reject every tag it sees without special-casing them.
        let parsed = parse_template_body(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      A: !!custom 5\n      B: !!binary aGk=\n      C: !!timestamp 2024-01-01\n",
        )
        .expect("`!!` tags resolve before reaching the intrinsic check");
        let props = &parsed["Resources"]["Q"]["Properties"];
        assert_eq!(props["A"], json!("5"));
        assert_eq!(props["B"], json!("aGk="));
        assert_eq!(props["C"], json!("2024-01-01"));
    }

    #[test]
    fn yaml_standard_tags_still_pass_through() {
        // `!!str` and friends are YAML's own tags, not CloudFormation's, and
        // must not trip the unknown-intrinsic check.
        let parsed = parse_template_body(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      A: !!str 5\n      B: !!int \"7\"\n",
        )
        .expect("standard YAML tags are not CloudFormation intrinsics");
        let props = &parsed["Resources"]["Q"]["Properties"];
        assert_eq!(props["A"], json!("5"));
        assert_eq!(props["B"], json!(7));
    }

    #[test]
    fn quoted_or_spaced_resources_key_is_recognised() {
        // Legal YAML spellings of the top-level key, each paired with a
        // syntax error elsewhere so classification runs on the raw text.
        for body in [
            "\"Resources\":\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
            "'Resources':\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
            "Resources :\n\tQ:\n\t\tType: AWS::SQS::Queue\n",
        ] {
            assert!(
                parse_template_body(body).is_err(),
                "{body:?} should not parse"
            );
            assert!(
                is_template_document(body),
                "{body:?} must be recognised as a template"
            );
        }
    }

    #[test]
    fn wrong_shaped_resources_section_is_still_a_template_document() {
        // `Resources` as a sequence instead of a mapping is a common YAML
        // slip. The template parser rejects it, so it has to be classified as
        // a template or it degrades to a silent empty stack.
        let seq = "Resources:\n  - Type: AWS::SQS::Queue\n";
        assert!(parse_template_body(seq).is_ok(), "parses as YAML");
        assert!(is_template_document(seq));

        // Unparseable flow-style YAML: the key is unquoted, so a check for
        // the JSON spelling alone would miss it.
        let broken_flow = "{Resources: {A: {Type: AWS::SQS::Queue}";
        assert!(parse_template_body(broken_flow).is_err());
        assert!(is_template_document(broken_flow));
    }

    #[test]
    fn non_finite_numbers_are_reported_not_nulled() {
        let err = parse_template_body("Resources:\n  Q:\n    Timeout: .nan\n")
            .expect_err("NaN has no JSON representation");
        assert!(err.contains("no JSON representation"), "{err}");
        // Still classified as a template, so the caller fails loudly.
        assert!(is_template_document(
            "Resources:\n  Q:\n    Timeout: .nan\n"
        ));
    }

    #[test]
    fn apostrophes_in_plain_scalars_do_not_leak_depth() {
        // YAML allows a bare apostrophe in a plain scalar. A per-line quote
        // scanner sees it open a quote that never closes, so the line's `{`
        // went uncounted-closed and leaked +1 upward; enough such lines
        // falsely rejected a valid template.
        let mut body = String::from("Resources:\n");
        for i in 0..(MAX_NESTING_DEPTH + 50) {
            body.push_str(&format!("  Q{i}: {{bar: \"x\", baz: don't}}\n"));
        }
        assert!(
            !exceeds_nesting_limit(&body),
            "a shallow template must not trip the depth guard"
        );
        // A real multi-line flow bomb is still caught.
        let bomb = "[\n".repeat(MAX_NESTING_DEPTH + 50);
        assert!(exceeds_nesting_limit(&bomb));
    }

    #[test]
    fn an_apostrophe_cannot_disable_the_depth_guard() {
        // Rolling back a line that ended mid-quote made the guard fail OPEN:
        // an apostrophe on every line suppressed that line's brackets, so a
        // genuine bomb sailed past the check it exists to enforce.
        let bomb = "[ don't\n".repeat(MAX_NESTING_DEPTH + 50);
        assert!(
            exceeds_nesting_limit(&bomb),
            "an apostrophe in a plain scalar must not suppress bracket counting"
        );
        // ... while a quoted scalar opened where a scalar legitimately starts
        // still has its contents skipped.
        let quoted = format!(
            "Resources:\n  Q:\n    Policy: '{}'\n",
            "{".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(!exceeds_nesting_limit(&quoted));

        // An UNTERMINATED quote must not suppress the brackets after it: the
        // run was never a scalar, so leaving one open on every line would
        // otherwise opt the body out of the guard entirely.
        let unterminated = "a: x ', [\n".repeat(MAX_NESTING_DEPTH + 50);
        assert!(
            exceeds_nesting_limit(&unterminated),
            "an unterminated quote must not disable bracket counting"
        );
    }

    #[test]
    fn multiline_quoted_scalars_are_not_counted() {
        // A quoted scalar may legally span lines. Its braces are content, so a
        // long embedded document must not trip the guard just because it wraps.
        let chunk = "{".repeat(80);
        let mut body = String::from("Resources:\n  Q:\n    Policy: \"");
        for _ in 0..8 {
            body.push_str(&chunk);
            body.push('\n');
        }
        body.push_str("\"\n");
        assert!(
            !exceeds_nesting_limit(&body),
            "a multiline quoted scalar is content, not nesting"
        );
    }

    #[test]
    fn a_trailing_backslash_does_not_swallow_the_closing_quote() {
        // In a double-quoted scalar a trailing backslash escapes the line
        // break, not the next line's first byte. Carrying the escape state
        // across the newline consumed the real closing quote, leaving the
        // scanner "inside" a quote for the rest of the template and
        // mis-accounting every flow collection after it.
        let body = "Resources:\n  Q:\n    A: \"wrapped \\\n\"\n    B: [1, 2]\n";
        assert!(
            !exceeds_nesting_limit(body),
            "a shallow template must not trip the guard"
        );
    }

    #[test]
    fn block_scalar_content_is_not_counted() {
        // SAM `InlineCode`, EC2 `UserData` and inline policy documents are all
        // block scalars. Their braces are string content, and YAML does not
        // recurse through them, so counting them only produces false
        // rejections of perfectly shallow templates.
        let code: String = std::iter::repeat_n("      if (x) { y({ z: 1 }); \n", 200).collect();
        let body = format!(
            "Resources:\n  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n      InlineCode: |\n{code}      Runtime: nodejs20.x\n"
        );
        assert!(
            !exceeds_nesting_limit(&body),
            "block scalar content is a string, not nesting"
        );

        // The chomping indicators are part of the header too.
        for header in ["|", "|-", "|+", ">", ">-", ">2"] {
            let b = format!(
                "Resources:\n  Q:\n    Body: {header}\n{}      done: true\n",
                "      {{{{\n".repeat(200)
            );
            assert!(
                !exceeds_nesting_limit(&b),
                "header {header} should open a block scalar"
            );
        }

        // Dedenting back out ends the block, so real nesting after it still
        // counts.
        let after = format!(
            "Resources:\n  Q:\n    Body: |\n      {{{{\n    Other: {}\n",
            "[".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(exceeds_nesting_limit(&after));
    }

    #[test]
    fn block_scalar_headers_are_read_past_comments() {
        // `Body: | # note` really does open a block scalar. Missing it would
        // count the string content and falsely reject a shallow template.
        let commented_header = format!(
            "Resources:\n  Q:\n    Body: | # inline note\n{}      done: true\n",
            "      {{{{\n".repeat(200)
        );
        assert!(!exceeds_nesting_limit(&commented_header));

        // `Resources: # |` really does NOT. Treating it as one would skip
        // every indented line after it and step around the guard entirely.
        let commented_key = format!(
            "Resources: # |\n  Q:\n    V: {}\n",
            "[".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(
            exceeds_nesting_limit(&commented_key),
            "a comment ending in `|` must not open a block scalar"
        );

        // A `#` that is not a comment (no preceding space) must not truncate
        // the scan either.
        let hash_in_scalar = format!(
            "Resources:\n  Q:\n    V: a#b{}\n",
            "[".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(exceeds_nesting_limit(&hash_in_scalar));
    }

    #[test]
    fn only_a_bare_indicator_opens_a_block_scalar() {
        // Real headers, including sequence items and every indicator spelling.
        for header in [
            "Body: |",
            "Body: >",
            "Body: |-",
            "Body: |+",
            "Body: >2",
            "Body: |2-",
            "Body: |-2",
            "- |",
            "Body:  |  ",
            "Body: | # note",
        ] {
            assert!(
                opens_block_scalar(header.split(" #").next().unwrap()),
                "{header:?} should open a block scalar"
            );
        }
        // Values that merely END with an indicator open nothing. Skipping the
        // indented lines after these would step around the depth guard.
        for not_header in [
            "Body: ||",
            "Body: >>",
            "Command: echo a |",
            "Body: a|",
            "Resources:",
            "Body: |x",
            "Arn: arn:aws:s3:::b |",
        ] {
            assert!(
                !opens_block_scalar(not_header),
                "{not_header:?} must not open a block scalar"
            );
        }

        // End to end: the bypass shape must still be caught.
        let bypass = format!(
            "Resources:\n  Q:\n    Body: ||\n      V: {}\n",
            "[".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(
            exceeds_nesting_limit(&bypass),
            "`||` must not suppress the lines after it"
        );
    }

    #[test]
    fn indented_section_names_are_not_top_level_keys() {
        // Only a column-zero key marks a template. An indented `Parameters:`
        // (a nested property, a pasted fragment) must keep the lenient path,
        // or a placeholder input would take the hard-failure route.
        let indented = "  Parameters:\n\tbroken\n";
        assert!(parse_template_body(indented).is_err());
        assert!(!is_template_document(indented));
    }

    #[test]
    fn unparseable_body_is_recognised_by_any_top_level_section() {
        // The syntax error sits in `Parameters`, before `Resources:` is ever
        // reached — a text scan keyed only on `Resources:` would call this a
        // placeholder and silently build an empty stack.
        let body =
            "Parameters:\n\tEnv:\n\t\tType: String\nResources:\n  Q:\n    Type: AWS::SQS::Queue\n";
        assert!(parse_template_body(body).is_err());
        assert!(is_template_document(body));

        // Truncated before `Resources:` appears at all.
        let truncated = "AWSTemplateFormatVersion: '2010-09-09'\nParameters:\n\tEnv:\n";
        assert!(parse_template_body(truncated).is_err());
        assert!(is_template_document(truncated));
    }

    #[test]
    fn other_top_level_sections_mark_a_template_document() {
        // Parses fine, but the template parser rejects it for having no
        // `Resources`. Judging on `Resources` alone would send that error down
        // the lenient path — an empty stack on create, and on update the
        // removal of every existing resource.
        for body in [
            "Parameters:\n  Env:\n    Type: String\n",
            "AWSTemplateFormatVersion: '2010-09-09'\n",
            "Outputs:\n  A:\n    Value: x\n",
            "Transform: AWS::Serverless-2016-10-31\n",
        ] {
            assert!(parse_template_body(body).is_ok(), "{body:?}");
            assert!(is_template_document(body), "{body:?}");
        }
    }

    #[test]
    fn embedded_json_in_a_quoted_string_does_not_trip_the_depth_guard() {
        // An inline IAM policy or nested template can carry hundreds of braces
        // inside one quoted scalar while nesting only a few levels.
        let blob = "{".repeat(MAX_NESTING_DEPTH + 50);
        let body = format!("Resources:\n  Q:\n    Type: AWS::SQS::Queue\n    Properties:\n      Policy: \"{blob}\"\n");
        assert!(
            parse_template_body(&body).is_ok(),
            "brackets inside a quoted scalar are content, not structure"
        );
        // A `#` comment is skipped too.
        let commented = format!(
            "Resources: # {}\n  Q:\n    Type: AWS::SQS::Queue\n",
            "[".repeat(MAX_NESTING_DEPTH + 50)
        );
        assert!(parse_template_body(&commented).is_ok());
        // Real structural nesting is still refused.
        assert!(parse_template_body(&"[".repeat(MAX_NESTING_DEPTH + 50)).is_err());
    }

    #[test]
    fn bom_prefixed_templates_are_recognised() {
        // PowerShell's Out-File / Set-Content write a UTF-8 BOM by default,
        // and `trim_start` does not strip U+FEFF.
        let good = "\u{FEFF}Resources:\n  Q:\n    Type: AWS::SQS::Queue\n";
        assert_eq!(
            parse_template_body(good).expect("BOM'd template parses")["Resources"]["Q"]["Type"],
            "AWS::SQS::Queue"
        );
        assert!(is_template_document(good));

        // The case that mattered: BOM + a syntax error must still be
        // classified as a template, or it degrades to an empty stack.
        let broken = "\u{FEFF}Resources:\n\tQ:\n\t\tType: AWS::SQS::Queue\n";
        assert!(parse_template_body(broken).is_err());
        assert!(is_template_document(broken));

        // BOM in front of a JSON body too.
        let json = "\u{FEFF}{\"Resources\": {\"Q\": {\"Type\": \"AWS::SQS::Queue\"}}}";
        assert!(parse_template_body(json).is_ok());
        assert!(is_template_document(json));
    }

    #[test]
    fn placeholders_are_not_template_documents() {
        // The conformance probe's synthetic TemplateBody values must keep the
        // lenient path — ValidationError is not in CreateStack's Smithy errors.
        for placeholder in ["test", "t", "aaaaaaaa", "", "dGVzdA=="] {
            assert!(
                !is_template_document(placeholder),
                "{placeholder:?} must not be treated as a template"
            );
        }
        // A mapping is always a submitted document, whatever it contains —
        // real CloudFormation rejects one with no Resources rather than
        // building an empty stack from it.
        assert!(is_template_document("Description: just a description\n"));
        assert!(is_template_document("{}"));
        assert!(is_template_document("foo: 1\n"));

        // An empty / absent body parses to null and must stay lenient — the
        // probe omits TemplateBody entirely on some variants.
        for empty in ["", "   ", "\n", "null", "~"] {
            assert!(
                !is_template_document(empty),
                "{empty:?} must keep the lenient path"
            );
        }

        // An unparseable body that merely mentions `Resources` without making
        // it a key (a pasted log, a CSV header) must stay lenient — the hard
        // failure paths would otherwise reject it outright.
        assert!(!is_template_document(
            "Name\tResources\tOwner\n\tbad\ttabs\there\n"
        ));
    }

    #[test]
    fn non_object_bodies_are_not_placeholders() {
        // These parse, but no CloudFormation template is a sequence, a number
        // or a bool. Real CFN rejects them as an unsupported structure;
        // treating them as placeholders would accept an empty stack (and on
        // update remove every existing resource).
        for body in ["[]", "[a, b]", "1", "true", "- a\n- b\n"] {
            assert!(parse_template_body(body).is_ok(), "{body:?} should parse");
            assert!(is_template_document(body), "{body:?} is not a placeholder");
        }
    }

    #[test]
    fn well_formed_templates_are_template_documents() {
        assert!(is_template_document(
            "Resources:\n  Q:\n    Type: AWS::SQS::Queue\n"
        ));
        assert!(is_template_document(
            r#"{"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}"#
        ));
    }
}
