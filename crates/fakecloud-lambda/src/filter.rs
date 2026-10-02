//! Lambda event source mapping filter criteria.
//!
//! Implements the EventBridge-style JSON pattern subset documented for
//! Lambda ESM `FilterCriteria`. A record is delivered when *any*
//! supplied pattern matches; a record is dropped when *every* pattern
//! fails to match.
//!
//! Matching uses the shared AWS event-pattern evaluator
//! ([`fakecloud_aws::event_pattern`]), the same one EventBridge rules use, so
//! array flattening, presence rules and every operator behave identically.
//!
//! Before matching, the record is shaped the way AWS filters it:
//! - SQS: a `body` that is valid JSON is parsed, so patterns can address
//!   `{"body": {"field": [...]}}`; a non-JSON body stays a plain string.
//! - Kinesis: the pattern applies to the record's `kinesis` object, whose
//!   base64 `data` is decoded and parsed as JSON (so `{"data": {...}}`
//!   patterns work). Non-JSON data stays the base64 string and never matches
//!   a `data` object pattern, which drops the record as AWS does.
//! - DynamoDB Streams records are already JSON and are matched as-is.

use serde_json::Value;

/// Compiled filter set. `patterns` parses the raw `Filters: [{Pattern: "..."}]`
/// strings into JSON objects once at create time.
#[derive(Debug, Clone, Default)]
pub struct FilterSet {
    patterns: Vec<Value>,
}

impl FilterSet {
    /// Build from the raw filter pattern strings stored on
    /// [`crate::state::EventSourceMapping::filter_patterns`]. Patterns
    /// that fail to parse are logged and dropped; pre-validation at
    /// [`Self::validate`] (called from `CreateEventSourceMapping`)
    /// keeps the live data clean.
    pub fn from_strings<I, S>(raw: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let patterns = raw
            .into_iter()
            .filter_map(|s| match serde_json::from_str::<Value>(s.as_ref()) {
                // Patterns persisted before scalar leaves were rejected keep
                // their old meaning: `{"k": "v"}` is read as `{"k": ["v"]}`.
                Ok(v) => Some(fakecloud_aws::event_pattern::normalize_scalar_leaves(&v)),
                Err(err) => {
                    tracing::warn!(
                        pattern = s.as_ref(),
                        error = %err,
                        "lambda ESM filter pattern is invalid JSON; ignoring this pattern (other patterns still apply)"
                    );
                    None
                }
            })
            .collect();
        Self { patterns }
    }

    /// Validate raw filter patterns the same way real AWS rejects bad
    /// `FilterCriteria` at `CreateEventSourceMapping`. Returns the
    /// first invalid pattern's parse error so the service can surface
    /// it as `InvalidParameterValueException`.
    pub fn validate<I, S>(raw: I) -> Result<(), String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for s in raw {
            let pattern = serde_json::from_str::<Value>(s.as_ref())
                .map_err(|err| format!("FilterCriteria pattern is invalid JSON: {err}"))?;
            if !fakecloud_aws::event_pattern::is_valid_structure(&pattern) {
                return Err("Invalid filter pattern definition.".to_string());
            }
        }
        Ok(())
    }

    /// Returns `true` when the record matches at least one pattern, or
    /// when the filter set is empty (no filtering = pass-through).
    pub fn matches(&self, record: &Value) -> bool {
        if self.patterns.is_empty() {
            return true;
        }
        self.patterns.iter().any(|p| match_value(p, record))
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
}

fn match_value(pattern: &Value, record: &Value) -> bool {
    let target = filter_target(record);
    fakecloud_aws::event_pattern::matches(pattern, &target)
}

/// Build the value a filter pattern is evaluated against (see module docs).
fn filter_target(record: &Value) -> Value {
    if record.get("eventSource").and_then(Value::as_str) == Some("aws:kinesis") {
        if let Some(Value::Object(kinesis)) = record.get("kinesis") {
            let mut target = kinesis.clone();
            if let Some(Value::String(data)) = kinesis.get("data") {
                use base64::Engine;
                if let Some(parsed) = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                {
                    target.insert("data".to_string(), parsed);
                }
            }
            return Value::Object(target);
        }
    }
    if let Some(Value::String(body)) = record.get("body") {
        if let Ok(parsed) = serde_json::from_str::<Value>(body) {
            let mut target = record.clone();
            target["body"] = parsed;
            return target;
        }
    }
    record.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fs(patterns: &[&str]) -> FilterSet {
        FilterSet::from_strings(patterns.iter().map(|s| s.to_string()))
    }

    #[test]
    fn empty_pattern_passes_through() {
        let f = FilterSet::default();
        assert!(f.matches(&json!({"any": "thing"})));
    }

    #[test]
    fn exact_string_match() {
        let f = fs(&[r#"{"foo": ["bar"]}"#]);
        assert!(f.matches(&json!({"foo": "bar"})));
        assert!(!f.matches(&json!({"foo": "baz"})));
    }

    #[test]
    fn array_of_scalars_is_or() {
        let f = fs(&[r#"{"foo": ["a", "b"]}"#]);
        assert!(f.matches(&json!({"foo": "a"})));
        assert!(f.matches(&json!({"foo": "b"})));
        assert!(!f.matches(&json!({"foo": "c"})));
    }

    #[test]
    fn exists_operator_treats_null_as_present() {
        let exists_true = fs(&[r#"{"foo": [{"exists": true}]}"#]);
        // AWS treats `{"foo": null}` as foo-is-present.
        assert!(exists_true.matches(&json!({"foo": null})));
        let exists_false = fs(&[r#"{"foo": [{"exists": false}]}"#]);
        // ...and conversely a missing key is exists:false even though
        // the value lookup returns the same Null sentinel under the
        // hood.
        assert!(exists_false.matches(&json!({})));
        assert!(!exists_false.matches(&json!({"foo": null})));
    }

    #[test]
    fn numeric_odd_length_is_no_match() {
        let f = fs(&[r#"{"n": [{"numeric": [">", 0, "<"]}]}"#]);
        assert!(!f.matches(&json!({"n": 5})));
    }

    #[test]
    fn validate_rejects_invalid_json() {
        assert!(FilterSet::validate(["{not json"].iter()).is_err());
        assert!(FilterSet::validate([r#"{"ok": [true]}"#].iter()).is_ok());
        // Scalar leaves and non-object patterns are not valid patterns.
        assert!(FilterSet::validate([r#"{"ok": true}"#].iter()).is_err());
        assert!(FilterSet::validate([r#"{"a": {"b": "x"}}"#].iter()).is_err());
        assert!(FilterSet::validate([r#"["x"]"#].iter()).is_err());
        assert!(FilterSet::validate([r#"{"$or": [{"a": ["1"]}, {"b": ["2"]}]}"#].iter()).is_ok());
    }

    #[test]
    fn exists_operator() {
        let exists_true = fs(&[r#"{"foo": [{"exists": true}]}"#]);
        assert!(exists_true.matches(&json!({"foo": "x"})));
        assert!(!exists_true.matches(&json!({"bar": "x"})));

        let exists_false = fs(&[r#"{"foo": [{"exists": false}]}"#]);
        assert!(!exists_false.matches(&json!({"foo": "x"})));
        assert!(exists_false.matches(&json!({"bar": "x"})));
    }

    #[test]
    fn sqs_body_decode() {
        let f = fs(&[r#"{"body": {"action": ["process"]}}"#]);
        let record = json!({
            "body": "{\"action\": \"process\", \"id\": 42}",
        });
        assert!(f.matches(&record));
        let other = json!({
            "body": "{\"action\": \"skip\"}",
        });
        assert!(!f.matches(&other));
    }

    #[test]
    fn nested_object_match() {
        let f = fs(&[r#"{"order": {"status": ["paid"]}}"#]);
        assert!(f.matches(&json!({"order": {"status": "paid", "id": 1}})));
        assert!(!f.matches(&json!({"order": {"status": "pending"}})));
    }

    #[test]
    fn multiple_patterns_or() {
        let f = fs(&[r#"{"a": ["x"]}"#, r#"{"b": ["y"]}"#]);
        assert!(f.matches(&json!({"a": "x"})));
        assert!(f.matches(&json!({"b": "y"})));
        assert!(!f.matches(&json!({"c": "z"})));
    }

    #[test]
    fn scalar_pattern_matches_array_value() {
        let f = fs(&[r#"{"body": {"tags": ["b"]}}"#]);
        assert!(f.matches(&json!({"body": "{\"tags\": [\"a\", \"b\"]}"})));
        assert!(!f.matches(&json!({"body": "{\"tags\": [\"a\"]}"})));
    }

    #[test]
    fn object_pattern_matches_array_of_objects() {
        let f = fs(&[r#"{"body": {"items": {"sku": ["x"]}}}"#]);
        assert!(f.matches(&json!({"body": r#"{"items": [{"sku": "a"}, {"sku": "x"}]}"#})));
        assert!(!f.matches(&json!({"body": r#"{"items": [{"sku": "a"}]}"#})));
    }

    #[test]
    fn anything_but_on_array_and_numeric_repr() {
        let f = fs(&[r#"{"c": [{"anything-but": ["rugby"]}]}"#]);
        assert!(f.matches(&json!({"c": ["rugby", "golf"]})));
        assert!(!f.matches(&json!({"c": ["rugby"]})));
        // anything-but requires the field.
        assert!(!f.matches(&json!({})));
        let f = fs(&[r#"{"n": [{"anything-but": 100}]}"#]);
        assert!(!f.matches(&json!({"n": 100.0})));
        assert!(f.matches(&json!({"n": 7})));
    }

    #[test]
    fn non_json_sqs_body_matches_as_string() {
        let f = fs(&[r#"{"body": ["plain text"]}"#]);
        assert!(f.matches(&json!({"body": "plain text"})));
        assert!(!f.matches(&json!({"body": "other"})));
    }

    #[test]
    fn kinesis_data_is_base64_decoded_for_filtering() {
        use base64::Engine;
        let rec = |data: &[u8]| {
            json!({
                "eventSource": "aws:kinesis",
                "kinesis": {
                    "partitionKey": "pk-1",
                    "data": base64::engine::general_purpose::STANDARD.encode(data),
                }
            })
        };
        let f = fs(&[r#"{"data": {"order": {"type": ["buy"]}}}"#]);
        assert!(f.matches(&rec(br#"{"order": {"type": "buy"}}"#)));
        assert!(!f.matches(&rec(br#"{"order": {"type": "sell"}}"#)));
        // Non-JSON data never matches a data pattern.
        assert!(!f.matches(&rec(b"not json")));
        // Metadata properties are filterable at the top level.
        let f = fs(&[r#"{"partitionKey": ["pk-1"]}"#]);
        assert!(f.matches(&rec(b"not json")));
    }

    #[test]
    fn legacy_scalar_leaf_patterns_still_match_after_load() {
        // Stored before validation rejected scalar leaves.
        let f = fs(&[r#"{"body": {"action": "process"}}"#]);
        assert!(f.matches(&json!({"body": r#"{"action": "process"}"#})));
        assert!(!f.matches(&json!({"body": r#"{"action": "skip"}"#})));
    }
}
