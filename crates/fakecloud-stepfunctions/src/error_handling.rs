use serde_json::Value;

/// Check if an error matches a Catch block and return the target state name.
pub fn find_catcher(catchers: &[Value], error: &str) -> Option<(String, Option<String>)> {
    for catcher in catchers {
        let error_equals = match catcher["ErrorEquals"].as_array() {
            Some(arr) => arr,
            None => continue,
        };

        let matches = error_equals.iter().any(|e| {
            let pattern = e.as_str().unwrap_or("");
            pattern == "States.ALL" || pattern == error
        });

        if matches {
            let next = match catcher["Next"].as_str() {
                Some(s) => s.to_string(),
                None => continue, // skip malformed catcher, try next one
            };
            // Distinguish between absent ResultPath (None) and JSON null ResultPath
            let result_path = if catcher.get("ResultPath").is_some_and(|v| v.is_null()) {
                Some("null".to_string())
            } else {
                catcher["ResultPath"].as_str().map(|s| s.to_string())
            };
            return Some((next, result_path));
        }
    }
    None
}

/// Check if we should retry an error based on Retry configuration.
/// Returns the delay in milliseconds if we should retry, or None if retries are exhausted.
pub fn should_retry(retriers: &[Value], error: &str, attempt: u32) -> Option<u64> {
    for retrier in retriers {
        let error_equals = match retrier["ErrorEquals"].as_array() {
            Some(arr) => arr,
            None => continue,
        };

        let matches = error_equals.iter().any(|e| {
            let pattern = e.as_str().unwrap_or("");
            pattern == "States.ALL" || pattern == error
        });

        if matches {
            let max_attempts = retrier["MaxAttempts"].as_u64().unwrap_or(3) as u32;
            if attempt >= max_attempts {
                return None;
            }

            let interval_seconds = retrier["IntervalSeconds"].as_f64().unwrap_or(1.0);
            let backoff_rate = retrier["BackoffRate"].as_f64().unwrap_or(2.0);
            let max_delay = retrier["MaxDelaySeconds"].as_f64().unwrap_or(60.0);

            let delay = interval_seconds * backoff_rate.powi(attempt as i32);
            let delay = delay.min(max_delay);

            return Some((delay * 1000.0) as u64);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_find_catcher_exact_match() {
        let catchers = vec![json!({
            "ErrorEquals": ["CustomError"],
            "Next": "HandleError"
        })];
        let result = find_catcher(&catchers, "CustomError");
        assert_eq!(result, Some(("HandleError".to_string(), None)));
    }

    #[test]
    fn test_find_catcher_states_all() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "Next": "CatchAll"
        })];
        let result = find_catcher(&catchers, "AnyError");
        assert_eq!(result, Some(("CatchAll".to_string(), None)));
    }

    #[test]
    fn test_find_catcher_no_match() {
        let catchers = vec![json!({
            "ErrorEquals": ["SpecificError"],
            "Next": "Handle"
        })];
        let result = find_catcher(&catchers, "DifferentError");
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_catcher_with_result_path() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "Next": "Handle",
            "ResultPath": "$.error"
        })];
        let result = find_catcher(&catchers, "AnyError");
        assert_eq!(
            result,
            Some(("Handle".to_string(), Some("$.error".to_string())))
        );
    }

    #[test]
    fn test_find_catcher_skips_malformed_and_finds_next() {
        // First catcher matches but has no Next field — should skip to second catcher
        let catchers = vec![
            json!({
                "ErrorEquals": ["States.ALL"]
                // missing "Next"
            }),
            json!({
                "ErrorEquals": ["States.ALL"],
                "Next": "FallbackHandler"
            }),
        ];
        let result = find_catcher(&catchers, "AnyError");
        assert_eq!(result, Some(("FallbackHandler".to_string(), None)));
    }

    #[test]
    fn test_should_retry_first_attempt() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 1,
            "MaxAttempts": 3,
            "BackoffRate": 2.0
        })];
        let result = should_retry(&retriers, "AnyError", 0);
        assert_eq!(result, Some(1000)); // 1s * 2^0 = 1s
    }

    #[test]
    fn test_should_retry_second_attempt() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 1,
            "MaxAttempts": 3,
            "BackoffRate": 2.0
        })];
        let result = should_retry(&retriers, "AnyError", 1);
        assert_eq!(result, Some(2000)); // 1s * 2^1 = 2s
    }

    #[test]
    fn test_should_retry_exhausted() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "MaxAttempts": 2
        })];
        let result = should_retry(&retriers, "AnyError", 2);
        assert_eq!(result, None);
    }

    #[test]
    fn test_should_retry_no_match() {
        let retriers = vec![json!({
            "ErrorEquals": ["SpecificError"],
            "MaxAttempts": 3
        })];
        let result = should_retry(&retriers, "DifferentError", 0);
        assert_eq!(result, None);
    }
}
