use serde_json::Value;

/// Reserved error names that a wildcard `ErrorEquals` never matches. AWS
/// documents that `States.Runtime` and `States.DataLimitExceeded` always fail
/// the execution and cannot be caught or retried by a `States.ALL` (or
/// `States.TaskFailed`) wildcard.
const NON_WILDCARD_ERRORS: [&str; 2] = ["States.Runtime", "States.DataLimitExceeded"];

/// Decide whether a single `ErrorEquals` pattern matches an actual error name.
///
/// * `States.ALL` matches any error except the non-catchable reserved names.
/// * `States.TaskFailed` acts as a wildcard for task-originated errors: it
///   matches any of them except `States.Timeout` (and the non-catchable
///   reserved names). It applies only when the error came from a Task state, so
///   e.g. a Parallel/Map branch's `States.Runtime` is not swallowed by it.
/// * Any other pattern is an exact error-name match.
fn pattern_matches(pattern: &str, error: &str, is_task_error: bool) -> bool {
    match pattern {
        "States.ALL" => !NON_WILDCARD_ERRORS.contains(&error),
        "States.TaskFailed" => {
            is_task_error && error != "States.Timeout" && !NON_WILDCARD_ERRORS.contains(&error)
        }
        exact => exact == error,
    }
}

/// Check if an error matches a Catch block and return the target state name.
///
/// `is_task_error` indicates the error originated from a Task state, which
/// governs whether the `States.TaskFailed` wildcard applies.
pub fn find_catcher(
    catchers: &[Value],
    error: &str,
    is_task_error: bool,
) -> Option<(String, Option<String>)> {
    for catcher in catchers {
        let error_equals = match catcher["ErrorEquals"].as_array() {
            Some(arr) => arr,
            None => continue,
        };

        let matches = error_equals
            .iter()
            .any(|e| pattern_matches(e.as_str().unwrap_or(""), error, is_task_error));

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
///
/// `is_task_error` indicates the error originated from a Task state, which
/// governs whether the `States.TaskFailed` wildcard applies.
pub fn should_retry(
    retriers: &[Value],
    error: &str,
    attempt: u32,
    is_task_error: bool,
) -> Option<u64> {
    for retrier in retriers {
        let error_equals = match retrier["ErrorEquals"].as_array() {
            Some(arr) => arr,
            None => continue,
        };

        let matches = error_equals
            .iter()
            .any(|e| pattern_matches(e.as_str().unwrap_or(""), error, is_task_error));

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
        let result = find_catcher(&catchers, "CustomError", true);
        assert_eq!(result, Some(("HandleError".to_string(), None)));
    }

    #[test]
    fn test_find_catcher_states_all() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "Next": "CatchAll"
        })];
        let result = find_catcher(&catchers, "AnyError", true);
        assert_eq!(result, Some(("CatchAll".to_string(), None)));
    }

    #[test]
    fn test_find_catcher_no_match() {
        let catchers = vec![json!({
            "ErrorEquals": ["SpecificError"],
            "Next": "Handle"
        })];
        let result = find_catcher(&catchers, "DifferentError", true);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_catcher_with_result_path() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "Next": "Handle",
            "ResultPath": "$.error"
        })];
        let result = find_catcher(&catchers, "AnyError", true);
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
        let result = find_catcher(&catchers, "AnyError", true);
        assert_eq!(result, Some(("FallbackHandler".to_string(), None)));
    }

    // H1: States.TaskFailed is a wildcard for task-originated errors — a custom
    // Lambda errorType surfaces as the Error name and must be caught.
    #[test]
    fn test_find_catcher_task_failed_wildcard_catches_custom_error() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.TaskFailed"],
            "Next": "Handler"
        })];
        let result = find_catcher(&catchers, "CustomLambdaError", true);
        assert_eq!(result, Some(("Handler".to_string(), None)));
    }

    // H1: States.TaskFailed must NOT match States.Timeout (per AWS).
    #[test]
    fn test_find_catcher_task_failed_excludes_timeout() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.TaskFailed"],
            "Next": "Handler"
        })];
        assert_eq!(find_catcher(&catchers, "States.Timeout", true), None);
    }

    // H1: the TaskFailed wildcard only applies to task-originated errors.
    #[test]
    fn test_find_catcher_task_failed_only_for_task_errors() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.TaskFailed"],
            "Next": "Handler"
        })];
        assert_eq!(find_catcher(&catchers, "SomeBranchError", false), None);
    }

    // M2: States.ALL must NOT catch States.Runtime or States.DataLimitExceeded.
    #[test]
    fn test_find_catcher_states_all_excludes_runtime() {
        let catchers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "Next": "Handler"
        })];
        assert_eq!(find_catcher(&catchers, "States.Runtime", true), None);
        assert_eq!(
            find_catcher(&catchers, "States.DataLimitExceeded", true),
            None
        );
        // But States.ALL still catches States.Timeout and ordinary errors.
        assert!(find_catcher(&catchers, "States.Timeout", true).is_some());
        assert!(find_catcher(&catchers, "Whatever", true).is_some());
    }

    #[test]
    fn test_should_retry_first_attempt() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 1,
            "MaxAttempts": 3,
            "BackoffRate": 2.0
        })];
        let result = should_retry(&retriers, "AnyError", 0, true);
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
        let result = should_retry(&retriers, "AnyError", 1, true);
        assert_eq!(result, Some(2000)); // 1s * 2^1 = 2s
    }

    #[test]
    fn test_should_retry_exhausted() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "MaxAttempts": 2
        })];
        let result = should_retry(&retriers, "AnyError", 2, true);
        assert_eq!(result, None);
    }

    #[test]
    fn test_should_retry_no_match() {
        let retriers = vec![json!({
            "ErrorEquals": ["SpecificError"],
            "MaxAttempts": 3
        })];
        let result = should_retry(&retriers, "DifferentError", 0, true);
        assert_eq!(result, None);
    }

    // H1: Retry on States.TaskFailed retries a custom task error.
    #[test]
    fn test_should_retry_task_failed_wildcard() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.TaskFailed"],
            "IntervalSeconds": 1,
            "MaxAttempts": 3
        })];
        assert_eq!(should_retry(&retriers, "CustomError", 0, true), Some(1000));
        // Not a task error → no retry.
        assert_eq!(should_retry(&retriers, "CustomError", 0, false), None);
    }

    // M3: the computed backoff honours MaxDelaySeconds and is not clamped to 5s.
    #[test]
    fn test_should_retry_honours_max_delay_seconds() {
        let retriers = vec![json!({
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 30,
            "MaxAttempts": 5,
            "BackoffRate": 2.0,
            "MaxDelaySeconds": 40
        })];
        // 30 * 2^1 = 60s, clamped to MaxDelaySeconds (40s), NOT to 5s.
        assert_eq!(should_retry(&retriers, "AnyError", 1, true), Some(40_000));
        // First attempt: 30s, well above the old 5s cap.
        assert_eq!(should_retry(&retriers, "AnyError", 0, true), Some(30_000));
    }
}
