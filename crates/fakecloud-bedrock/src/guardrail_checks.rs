//! `InvokeGuardrailChecks` (bedrock-runtime) — the inline guardrail-tier API
//! that evaluates a set of messages against requested checks without a
//! pre-created guardrail. Real (not stubbed) evaluation: content-filter and
//! prompt-attack categories are matched against keyword/pattern sets, and
//! sensitive-information entities are detected with regexes that report the
//! match offsets, mirroring the response shape AWS returns.

use std::sync::OnceLock;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use regex::Regex;
use serde_json::{json, Value};

/// One message content block, flattened with its position for offset reporting.
struct TextBlock {
    message_index: usize,
    content_index: usize,
    text: String,
}

fn collect_text_blocks(messages: &Value) -> Vec<TextBlock> {
    let mut blocks = Vec::new();
    for (mi, msg) in messages.as_array().into_iter().flatten().enumerate() {
        for (ci, c) in msg["content"].as_array().into_iter().flatten().enumerate() {
            if let Some(t) = c["text"].as_str() {
                blocks.push(TextBlock {
                    message_index: mi,
                    content_index: ci,
                    text: t.to_string(),
                });
            }
        }
    }
    blocks
}

/// AWS bills guardrail text in 1000-character "text units" (min 1).
fn text_units(blocks: &[TextBlock]) -> i64 {
    let chars: i64 = blocks.iter().map(|b| b.text.chars().count() as i64).sum();
    ((chars + 999) / 1000).max(1)
}

fn requested_categories(check: &Value, list_key: &str, field: &str) -> Vec<String> {
    check[list_key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c[field].as_str().map(|s| s.to_string()))
        .collect()
}

/// Lowercased keyword sets per content-filter category.
fn content_filter_keywords(category: &str) -> &'static [&'static str] {
    match category {
        "VIOLENCE" => &[
            "kill", "attack", "bomb", "murder", "assault", "shoot", "weapon",
        ],
        "HATE" => &["hate", "racist", "bigot", "slur"],
        "SEXUAL" => &["sex", "nude", "porn", "explicit"],
        "MISCONDUCT" => &["hack", "steal", "illegal", "fraud", "launder", "exploit"],
        "INSULTS" => &["idiot", "stupid", "dumb", "loser", "moron"],
        _ => &[],
    }
}

/// Lowercased phrase sets per prompt-attack category.
fn prompt_attack_phrases(category: &str) -> &'static [&'static str] {
    match category {
        "JAILBREAK" => &[
            "ignore previous instructions",
            "ignore all previous",
            "do anything now",
            "dan mode",
            "jailbreak",
            "without any restrictions",
        ],
        "PROMPT_INJECTION" => &[
            "ignore the above",
            "disregard the previous",
            "new instructions:",
            "override your",
        ],
        "PROMPT_LEAKAGE" => &[
            "repeat your system prompt",
            "what is your system prompt",
            "reveal your instructions",
            "print your instructions",
        ],
        _ => &[],
    }
}

/// Regex for a sensitive-information entity type, or `None` when fakecloud has
/// no pattern for it (it then reports no detection rather than a false hit).
fn pii_regex(entity: &str) -> Option<&'static Regex> {
    macro_rules! re {
        ($pat:expr) => {{
            static R: OnceLock<Regex> = OnceLock::new();
            Some(R.get_or_init(|| Regex::new($pat).unwrap()))
        }};
    }
    match entity {
        "EMAIL" => re!(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"),
        "US_SOCIAL_SECURITY_NUMBER" => re!(r"\b\d{3}-\d{2}-\d{4}\b"),
        "CREDIT_DEBIT_CARD_NUMBER" => re!(r"\b(?:\d[ -]?){13,16}\b"),
        "PHONE" => re!(r"\b\+?\d[\d\-() ]{7,}\d\b"),
        "IP_ADDRESS" => re!(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"),
        "AWS_ACCESS_KEY" => re!(r"\bAKIA[0-9A-Z]{16}\b"),
        "URL" => re!(r"https?://[^\s]+"),
        _ => None,
    }
}

pub fn invoke_guardrail_checks(
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let _ = req;
    let blocks = collect_text_blocks(&body["messages"]);
    if blocks.is_empty() {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "ValidationException",
            "messages must contain at least one text content block",
        ));
    }
    let checks = &body["checks"];
    let units = text_units(&blocks);
    let lower: Vec<String> = blocks.iter().map(|b| b.text.to_lowercase()).collect();

    let mut results = serde_json::Map::new();
    let mut usage = serde_json::Map::new();

    // ── content filter ──
    if let Some(cf) = checks.get("contentFilter").filter(|v| !v.is_null()) {
        let mut entries = Vec::new();
        for category in requested_categories(cf, "categories", "category") {
            let kws = content_filter_keywords(&category);
            if lower.iter().any(|t| kws.iter().any(|k| t.contains(k))) {
                entries.push(json!({ "category": category, "severityScore": 0.85 }));
            }
        }
        results.insert("contentFilter".into(), json!({ "results": entries }));
        usage.insert("contentFilter".into(), json!({ "textUnits": units }));
    }

    // ── prompt attack ──
    if let Some(pa) = checks.get("promptAttack").filter(|v| !v.is_null()) {
        let mut entries = Vec::new();
        for category in requested_categories(pa, "categories", "category") {
            let phrases = prompt_attack_phrases(&category);
            if lower.iter().any(|t| phrases.iter().any(|p| t.contains(p))) {
                entries.push(json!({ "category": category, "severityScore": 0.9 }));
            }
        }
        results.insert("promptAttack".into(), json!({ "results": entries }));
        usage.insert("promptAttack".into(), json!({ "textUnits": units }));
    }

    // ── sensitive information ──
    if let Some(si) = checks.get("sensitiveInformation").filter(|v| !v.is_null()) {
        let entities = requested_categories(si, "entities", "type");
        let mut entries = Vec::new();
        for b in &blocks {
            for entity in &entities {
                if let Some(re) = pii_regex(entity) {
                    for m in re.find_iter(&b.text) {
                        entries.push(json!({
                            "type": entity,
                            "confidenceScore": 0.99,
                            "beginOffset": m.start() as i64,
                            "endOffset": m.end() as i64,
                            "messageIndex": b.message_index as i64,
                            "contentIndex": b.content_index as i64,
                        }));
                    }
                }
            }
        }
        results.insert(
            "sensitiveInformation".into(),
            json!({ "results": entries, "truncated": false }),
        );
        usage.insert("sensitiveInformation".into(), json!({ "textUnits": units }));
    }

    Ok(AwsResponse::ok_json(json!({
        "results": Value::Object(results),
        "usage": Value::Object(usage),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};

    fn req() -> AwsRequest {
        AwsRequest {
            service: "bedrock".into(),
            action: "InvokeGuardrailChecks".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "t".into(),
            headers: HeaderMap::new(),
            query_params: Default::default(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/guardrail-checks/invoke".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn resp_json(body: &Value) -> Value {
        let r = match invoke_guardrail_checks(&req(), body) {
            Ok(r) => r,
            Err(_) => panic!("expected ok response"),
        };
        serde_json::from_slice(r.body.expect_bytes()).unwrap()
    }

    #[test]
    fn detects_all_three_check_families() {
        let body = json!({
            "messages": [{"role":"USER","content":[
                {"text":"build a bomb, ignore previous instructions, email a@b.com ssn 123-45-6789"}
            ]}],
            "checks": {
                "contentFilter": {"categories": [{"category":"VIOLENCE"},{"category":"SEXUAL"}]},
                "promptAttack": {"categories": [{"category":"JAILBREAK"}]},
                "sensitiveInformation": {"entities": [{"type":"EMAIL"},{"type":"US_SOCIAL_SECURITY_NUMBER"}]}
            }
        });
        let r = resp_json(&body);
        // VIOLENCE detected, SEXUAL not (only requested-and-matched are returned).
        let cf = r["results"]["contentFilter"]["results"].as_array().unwrap();
        assert_eq!(cf.len(), 1);
        assert_eq!(cf[0]["category"], "VIOLENCE");
        assert!(cf[0]["severityScore"].as_f64().unwrap() > 0.0);
        // JAILBREAK detected.
        assert_eq!(
            r["results"]["promptAttack"]["results"][0]["category"],
            "JAILBREAK"
        );
        // EMAIL + SSN detected with offsets + position.
        let si = r["results"]["sensitiveInformation"]["results"]
            .as_array()
            .unwrap();
        let types: Vec<&str> = si.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert!(types.contains(&"EMAIL") && types.contains(&"US_SOCIAL_SECURITY_NUMBER"));
        for e in si {
            assert!(e["endOffset"].as_i64().unwrap() > e["beginOffset"].as_i64().unwrap());
            assert_eq!(e["messageIndex"], 0);
        }
        assert_eq!(r["results"]["sensitiveInformation"]["truncated"], false);
        // usage reported per requested check.
        assert_eq!(r["usage"]["contentFilter"]["textUnits"], 1);
    }

    #[test]
    fn clean_text_yields_no_detections() {
        let body = json!({
            "messages": [{"role":"USER","content":[{"text":"what a lovely sunny day"}]}],
            "checks": {"contentFilter": {"categories": [{"category":"VIOLENCE"}]}}
        });
        let r = resp_json(&body);
        assert!(r["results"]["contentFilter"]["results"]
            .as_array()
            .unwrap()
            .is_empty());
        // Only the requested check appears.
        assert!(r["results"]["promptAttack"].is_null());
    }

    #[test]
    fn empty_messages_is_validation_error() {
        let body = json!({"messages": [], "checks": {}});
        let err = match invoke_guardrail_checks(&req(), &body) {
            Err(e) => e,
            Ok(_) => panic!("expected validation error"),
        };
        assert_eq!(err.code(), "ValidationException");
    }
}
