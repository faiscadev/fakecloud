//! Custom S3 prefixes: `!{timestamp:<pattern>}`, `!{firehose:random-string}` and
//! `!{firehose:error-output-type}`, evaluated when a batch is cut.

use chrono::{DateTime, Utc};
use fakecloud_core::service::AwsServiceError;

use crate::config::invalid_argument;

const CONSTRAINT: &str = "The supplied prefix(es) do not satisfy the following constraint:";
const RANDOM_STRING_LENGTH: usize = 11;

pub(crate) fn has_expressions(prefix: &str) -> bool {
    prefix.contains("!{")
}

/// Expressions only delivery of dynamic partitions can evaluate.
pub(crate) fn has_partition_keys(prefix: &str) -> bool {
    prefix.contains("!{partitionKeyFrom")
}

fn invalid(reason: impl std::fmt::Display) -> AwsServiceError {
    invalid_argument(format!("{CONSTRAINT} {reason}"))
}

/// Each `!{namespace:key}` of `prefix`, with the text before it.
fn expressions(prefix: &str) -> impl Iterator<Item = (&str, Option<&str>)> {
    let mut rest = prefix;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let Some(start) = rest.find("!{") else {
            let literal = std::mem::take(&mut rest);
            return Some((literal, None));
        };
        let literal = &rest[..start];
        let Some(end) = rest[start..].find('}') else {
            let whole = std::mem::take(&mut rest);
            return Some((whole, None));
        };
        let expression = &rest[start + 2..start + end];
        rest = &rest[start + end + 1..];
        Some((literal, Some(expression)))
    })
}

fn validate_expressions(prefix: &str, error_prefix: bool) -> Result<(), AwsServiceError> {
    if let Some(start) = prefix.rfind("!{") {
        if !prefix[start..].contains('}') {
            return Err(invalid("Expression is missing its closing brace!"));
        }
    }
    for (_, expression) in expressions(prefix) {
        let Some(expression) = expression else {
            continue;
        };
        let (namespace, key) = expression
            .split_once(':')
            .ok_or_else(|| invalid(format!("Expression ({expression}) has no namespace!")))?;
        match (namespace, key) {
            ("timestamp", pattern) => {
                render_timestamp(pattern, Utc::now())
                    .map_err(|()| invalid(format!("Invalid timestamp pattern ({pattern})!")))?;
            }
            ("firehose", "random-string") => {}
            ("firehose", "error-output-type") if error_prefix => {}
            ("firehose", "error-output-type") => {
                return Err(invalid(
                    "firehose:error-output-type can only be used in ErrorOutputPrefix!",
                ));
            }
            ("firehose", other) => {
                return Err(invalid(format!("Invalid firehose keyword ({other})!")));
            }
            ("partitionKeyFromQuery" | "partitionKeyFromLambda", _) => {}
            (other, _) => {
                return Err(invalid(format!(
                    "Namespace to the left of colon ({other}) is an invalid keyword!"
                )));
            }
        }
    }
    Ok(())
}

/// What `CreateDeliveryStream` and `UpdateDestination` require of a destination's
/// two prefixes. Expressions in `Prefix` need an `ErrorOutputPrefix`, which need
/// not hold any itself.
pub(crate) fn validate_prefixes(
    prefix: Option<&str>,
    error_output_prefix: Option<&str>,
) -> Result<(), AwsServiceError> {
    let prefix = prefix.unwrap_or_default();
    let error_output_prefix = error_output_prefix.unwrap_or_default();
    if has_expressions(prefix) && error_output_prefix.is_empty() {
        return Err(invalid(
            "ErrorOutputPrefix cannot be null or empty when Prefix contains expressions",
        ));
    }
    validate_expressions(prefix, false)?;
    validate_expressions(error_output_prefix, true)
}

fn random_string() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    uuid::Uuid::new_v4()
        .as_bytes()
        .iter()
        .take(RANDOM_STRING_LENGTH)
        .map(|byte| char::from(ALPHABET[usize::from(*byte) % ALPHABET.len()]))
        .collect()
}

/// `prefix` with its expressions evaluated at `at`, the arrival of the batch's
/// oldest record. Validated prefixes hold nothing else; anything else stays as
/// written.
pub(crate) fn render(prefix: &str, at: DateTime<Utc>, error_output_type: Option<&str>) -> String {
    let mut rendered = String::with_capacity(prefix.len());
    for (literal, expression) in expressions(prefix) {
        rendered.push_str(literal);
        let Some(expression) = expression else {
            continue;
        };
        let value = match expression.split_once(':') {
            Some(("timestamp", pattern)) => render_timestamp(pattern, at).ok(),
            Some(("firehose", "random-string")) => Some(random_string()),
            Some(("firehose", "error-output-type")) => error_output_type.map(str::to_owned),
            _ => None,
        };
        rendered.push_str(&value.unwrap_or_else(|| format!("!{{{expression}}}")));
    }
    rendered
}

/// A Java `DateTimeFormatter` pattern, the kind AWS documents for this namespace,
/// over the fields a date in UTC has. Other letters are refused.
fn render_timestamp(pattern: &str, at: DateTime<Utc>) -> Result<String, ()> {
    let mut output = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                output.push('\'');
                continue;
            }
            loop {
                match chars.next() {
                    Some('\'') => break,
                    Some(quoted) => output.push(quoted),
                    None => return Err(()),
                }
            }
        } else if c.is_ascii_alphabetic() {
            let mut run = 1;
            while chars.peek() == Some(&c) {
                chars.next();
                run += 1;
            }
            let format = match (c, run) {
                ('y' | 'u', 2) => "%y",
                ('y' | 'u', _) => "%Y",
                ('M', 1) => "%-m",
                ('M', 2) => "%m",
                ('M', 3) => "%b",
                ('M', 4) => "%B",
                ('d', 1) => "%-d",
                ('d', 2) => "%d",
                ('H', 1) => "%-H",
                ('H', 2) => "%H",
                ('m', 1) => "%-M",
                ('m', 2) => "%M",
                ('s', 1) => "%-S",
                ('s', 2) => "%S",
                ('S', 3) => "%3f",
                ('D', 1..=3) => "%j",
                ('E', 1..=3) => "%a",
                ('E', 4) => "%A",
                ('a', 1) => "%p",
                _ => return Err(()),
            };
            output.push_str(&at.format(format).to_string());
        } else {
            output.push(c);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-09T14:19:56.123Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn timestamps_and_error_types_are_evaluated() {
        assert_eq!(render("s/!{timestamp:yyyy}/", at(), None), "s/2026/");
        assert_eq!(
            render("!{timestamp:yyyy/MM/dd/HH}/", at(), None),
            "2026/11/09/14/"
        );
        assert_eq!(
            render("!{timestamp:yyyy-MM-dd'T'HH:mm:ss.SSS}", at(), None),
            "2026-11-09T14:19:56.123"
        );
        assert_eq!(
            render(
                "e/!{firehose:error-output-type}/",
                at(),
                Some("elasticsearch-failed")
            ),
            "e/elasticsearch-failed/"
        );
        assert_eq!(render("static/", at(), None), "static/");
    }

    #[test]
    fn random_strings_have_the_recorded_shape() {
        let one = render("!{firehose:random-string}", at(), None);
        assert_eq!(one.len(), 11, "{one}");
        assert!(one.bytes().all(|b| b.is_ascii_alphanumeric()), "{one}");
        assert_ne!(one, render("!{firehose:random-string}", at(), None));
    }

    #[test]
    fn recorded_prefix_rules() {
        // Accepted as recorded, including a static ErrorOutputPrefix.
        validate_prefixes(
            Some("s/!{timestamp:yyyy}/!{firehose:random-string}/"),
            Some("e/"),
        )
        .unwrap();
        validate_prefixes(Some("s/"), Some("e/!{firehose:error-output-type}/")).unwrap();
        let message = |prefix, error| validate_prefixes(prefix, error).unwrap_err().message();
        assert_eq!(
            message(Some("!{bogus:x}/"), Some("e/")),
            "The supplied prefix(es) do not satisfy the following constraint: Namespace to the \
             left of colon (bogus) is an invalid keyword!"
        );
        assert_eq!(
            message(Some("!{firehose:error-output-type}/"), None),
            "The supplied prefix(es) do not satisfy the following constraint: ErrorOutputPrefix \
             cannot be null or empty when Prefix contains expressions"
        );
    }

    #[test]
    fn malformed_expressions_are_refused() {
        for prefix in [
            "!{timestamp:yyyy",
            "!{timestamp}",
            "!{timestamp:QQ}",
            "!{firehose:other}",
            "!{firehose:error-output-type}",
        ] {
            assert!(
                validate_prefixes(Some(prefix), Some("e/")).is_err(),
                "{prefix}"
            );
        }
    }
}
