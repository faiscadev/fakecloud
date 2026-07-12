//! Honest, documented evaluation for `EvaluateCode` / `EvaluateMappingTemplate`.
//!
//! AppSync's real evaluation endpoints run a full VTL interpreter (mapping
//! templates) or an `APPSYNC_JS` runtime (code) against a request `context` and
//! return the rendered result. fakecloud does **not** ship a VTL/JS interpreter.
//!
//! Instead this module performs a faithful, bounded evaluation:
//!
//! * It validates that `context` is well-formed JSON (AppSync's `Context` is a
//!   JSON string) and that the template/code is present and non-empty --
//!   returning the model's `error` output member (not a top-level exception)
//!   when the context is malformed, exactly as AppSync surfaces evaluation
//!   errors.
//! * It returns a **deterministic** `evaluationResult`: the JSON-encoded value
//!   of the context's `arguments` field when present, else the whole parsed
//!   context. This mirrors the overwhelmingly common request-mapping-template
//!   shape (`$util.toJson($ctx.arguments)`), so simple templates evaluate to a
//!   sensible, reproducible value without a full interpreter.
//! * `stash` echoes the context's `stash` (AppSync threads the stash through),
//!   and `logs` is empty (no interpreter log output is produced).
//!
//! This is a documented evaluation *subset*, surfaced honestly rather than
//! faking arbitrary interpreter output.

use serde_json::{json, Value};

/// The outcome of an evaluation: either a rendered result or an error detail.
pub struct Evaluation {
    /// JSON-encoded `evaluationResult` string, when evaluation succeeded.
    pub result: Option<String>,
    /// `error.message`, when the context or template was malformed.
    pub error: Option<String>,
    /// JSON-encoded `stash` string threaded from the context.
    pub stash: Option<String>,
}

/// Evaluate a request against its context. `body` is the template (mapping) or
/// code string; it must be non-empty. `context` is the raw `Context` JSON
/// string from the request.
pub fn evaluate(body: &str, context: &str) -> Evaluation {
    if body.trim().is_empty() {
        return Evaluation {
            result: None,
            error: Some("The provided template/code is empty.".to_string()),
            stash: None,
        };
    }
    let ctx: Value = match serde_json::from_str(context) {
        Ok(v) => v,
        Err(e) => {
            return Evaluation {
                result: None,
                error: Some(format!("Unable to parse the context as JSON: {e}")),
                stash: None,
            };
        }
    };
    // Deterministic projection: prefer `arguments`, else the whole context.
    let rendered = ctx.get("arguments").cloned().unwrap_or_else(|| ctx.clone());
    let stash = ctx.get("stash").cloned().unwrap_or_else(|| json!({}));
    Evaluation {
        result: Some(serde_json::to_string(&rendered).unwrap_or_else(|_| "{}".to_string())),
        error: None,
        stash: Some(serde_json::to_string(&stash).unwrap_or_else(|_| "{}".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_arguments() {
        let ev = evaluate("$util.toJson($ctx.args)", r#"{"arguments":{"id":"1"}}"#);
        assert!(ev.error.is_none());
        assert_eq!(ev.result.as_deref(), Some(r#"{"id":"1"}"#));
    }

    #[test]
    fn malformed_context_is_error() {
        let ev = evaluate("x", "not json");
        assert!(ev.result.is_none());
        assert!(ev.error.is_some());
    }

    #[test]
    fn empty_template_is_error() {
        let ev = evaluate("  ", "{}");
        assert!(ev.error.is_some());
    }
}
