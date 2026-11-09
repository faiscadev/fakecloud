//! Custom S3 prefixes: `!{timestamp:<pattern>}`, `!{firehose:random-string}` and
//! `!{firehose:error-output-type}`, evaluated when a batch is cut.

use chrono::{DateTime, Datelike, Timelike, Utc};
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

/// `member` is the prefix's name in the recorded "Invalid conversion character"
/// message, which was only recorded for `Prefix`.
fn validate_expressions(prefix: &str, member: &str) -> Result<(), AwsServiceError> {
    let usage = || {
        invalid(format!(
            "Invalid expression usage: {prefix}.Must be of the form !{{<namespace>:<value>}}"
        ))
    };
    let conversion = || {
        invalid(format!(
            "Invalid conversion character used in the value of the expression in {member}"
        ))
    };
    if let Some(start) = prefix.rfind("!{") {
        if !prefix[start..].contains('}') {
            return Err(usage());
        }
    }
    for (_, expression) in expressions(prefix) {
        let Some(expression) = expression else {
            continue;
        };
        let (namespace, key) = expression.split_once(':').ok_or_else(usage)?;
        match (namespace, key) {
            ("timestamp", "") => return Err(usage()),
            ("timestamp", pattern) => match render_timestamp(pattern, widest_instant()) {
                Ok(_) => {}
                Err(PatternError::Invalid) => return Err(conversion()),
                Err(PatternError::PadOverflow) => {
                    return Err(invalid(format!(
                        "Invalid input to timestamp evaluation: {pattern}"
                    )))
                }
            },
            ("firehose", "random-string") => {}
            ("firehose", "error-output-type") if member == "ErrorOutputPrefix" => {}
            ("firehose", _) => return Err(conversion()),
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

/// AWS refuses `pH`, a padded field that can outgrow its pad, when the stream is
/// created. That was recorded at an hour with two digits; refusing it at every hour
/// is inferred, and makes creation independent of the clock. Evaluated at the
/// instant where every field is at its widest.
fn widest_instant() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-12-26T23:59:59.999999999Z")
        .map(|at| at.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

/// Whether a stored prefix can be evaluated. Admission asks only this: the rule on
/// `ErrorOutputPrefix` and anything about a prefix delivery does not read were
/// never required of streams already stored, which must keep delivering.
pub(crate) fn validate_evaluable(prefix: &str) -> Result<(), AwsServiceError> {
    validate_expressions(prefix, "Prefix")
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
    validate_expressions(prefix, "Prefix")?;
    validate_expressions(error_output_prefix, "ErrorOutputPrefix")
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

/// Why a pattern does not format: Java refuses it, or a padded field outgrew its pad.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PatternError {
    Invalid,
    PadOverflow,
}

/// A Java `DateTimeFormatter` pattern, the kind AWS documents for this namespace,
/// formatted as Java does in `Locale.US` and UTC. A pattern Java would refuse is
/// refused.
fn render_timestamp(pattern: &str, at: DateTime<Utc>) -> Result<String, PatternError> {
    let mut output = String::new();
    let mut chars = pattern.chars().peekable();
    let mut optional_depth = 0usize;
    let mut pad_next = None;
    while let Some(c) = chars.next() {
        let item = match c {
            '\'' => quoted_text(&mut chars).map_err(|()| PatternError::Invalid)?,
            '[' => {
                optional_depth += 1;
                continue;
            }
            ']' => {
                optional_depth = optional_depth.checked_sub(1).ok_or(PatternError::Invalid)?;
                continue;
            }
            '{' | '}' | '#' => return Err(PatternError::Invalid),
            c if c.is_ascii_alphabetic() => {
                let mut count = 1;
                while chars.peek() == Some(&c) {
                    chars.next();
                    count += 1;
                }
                if c == 'p' {
                    pad_next = Some(count);
                    continue;
                }
                date_field(c, count, at).map_err(|()| PatternError::Invalid)?
            }
            c => c.to_string(),
        };
        match pad_next.take() {
            Some(width) if item.chars().count() > width => {
                return Err(PatternError::PadOverflow);
            }
            Some(width) => output.push_str(&format!("{item:>width$}")),
            None => output.push_str(&item),
        }
    }
    if pad_next.is_some() {
        return Err(PatternError::Invalid);
    }
    Ok(output)
}

/// The text of a quote whose opening `'` was read. `''` alone is one quote mark,
/// and so is `''` inside a quote.
fn quoted_text(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<String, ()> {
    if chars.peek() == Some(&'\'') {
        chars.next();
        return Ok("'".to_owned());
    }
    let mut text = String::new();
    loop {
        match chars.next().ok_or(())? {
            '\'' if chars.peek() == Some(&'\'') => {
                chars.next();
                text.push('\'');
            }
            '\'' => return Ok(text),
            other => text.push(other),
        }
    }
}

/// The period of the day in `Locale.US`, with its narrow forms for the two exact
/// times that have their own names.
fn day_period(at: DateTime<Utc>, narrow: bool) -> &'static str {
    match (at.hour(), at.minute()) {
        (0, 0) if narrow => "mi",
        (0, 0) => "midnight",
        (12, 0) if narrow => "n",
        (12, 0) => "noon",
        (6..=11, _) => "in the morning",
        (12..=17, _) => "in the afternoon",
        (18..=20, _) => "in the evening",
        _ => "at night",
    }
}

/// One pattern letter repeated `count` times. Numbers are zero padded to `count`,
/// and each letter has its own limit, past which Java refuses the pattern.
fn date_field(letter: char, count: usize, at: DateTime<Utc>) -> Result<String, ()> {
    let number = |value: i64, max_count: usize| {
        (count <= max_count)
            .then(|| format!("{value:0count$}"))
            .ok_or(())
    };
    let year = |value: i32| {
        if count == 2 {
            Ok(format!("{:02}", value.rem_euclid(100)))
        } else {
            number(i64::from(value), 19)
        }
    };
    let text = |short: &str, long: &str, narrow: &str| match count {
        1..=3 => Ok(short.to_owned()),
        4 => Ok(long.to_owned()),
        5 => Ok(narrow.to_owned()),
        _ => Err(()),
    };
    let short_month = at.format("%b").to_string();
    let long_month = at.format("%B").to_string();
    let short_day = at.format("%a").to_string();
    let long_day = at.format("%A").to_string();
    let month_text = || text(&short_month, &long_month, &short_month[..1]);
    let day_text = || text(&short_day, &long_day, &short_day[..1]);
    let day_of_week = i64::from(at.weekday().num_days_from_sunday());
    // Week fields follow Locale.US: weeks start on Sunday, and the one holding
    // January 1 is week 1, of the year its Saturday falls in.
    let saturday = at.date_naive() + chrono::Duration::days(6 - day_of_week);
    let first_of_month_weekday = (i64::from(at.day0()) - day_of_week).rem_euclid(7);
    let first_of_month_weekday = (7 - first_of_month_weekday) % 7;
    let hour = i64::from(at.hour());
    let quarter = i64::from(at.month0() / 3 + 1);
    let nanos_of_second = i64::from(at.nanosecond() % 1_000_000_000);
    let nanos_of_day = i64::from(at.num_seconds_from_midnight()) * 1_000_000_000 + nanos_of_second;
    match letter {
        'G' => text("AD", "Anno Domini", "A"),
        'y' | 'u' => year(at.year()),
        'Y' => year(saturday.year()),
        'D' => number(i64::from(at.ordinal()), 3),
        'M' | 'L' if count <= 2 => number(i64::from(at.month()), 2),
        'M' | 'L' => month_text(),
        'd' => number(i64::from(at.day()), 2),
        'Q' | 'q' => match count {
            1 | 5 => Ok(quarter.to_string()),
            2 => Ok(format!("{quarter:02}")),
            3 => Ok(format!("Q{quarter}")),
            4 => Ok(format!(
                "{quarter}{} quarter",
                ["st", "nd", "rd", "th"][quarter as usize - 1]
            )),
            _ => Err(()),
        },
        'w' => number(i64::from(saturday.ordinal0() / 7 + 1), 2),
        'W' => number((i64::from(at.day0()) + first_of_month_weekday) / 7 + 1, 1),
        'E' => day_text(),
        'e' if count <= 2 => number(day_of_week + 1, 2),
        'e' => day_text(),
        'c' if count == 1 => Ok((day_of_week + 1).to_string()),
        'c' if count == 2 => Err(()),
        'c' => day_text(),
        'F' => number(i64::from(at.day0() % 7) + 1, 1),
        'a' if count == 1 => Ok(at.format("%p").to_string()),
        'a' => Err(()),
        'B' if count == 1 || count == 4 || count == 5 => Ok(day_period(at, count == 5).to_owned()),
        'H' => number(hour, 2),
        'k' => number(if hour == 0 { 24 } else { hour }, 2),
        'K' => number(hour % 12, 2),
        'h' => number(if hour % 12 == 0 { 12 } else { hour % 12 }, 2),
        'm' => number(i64::from(at.minute()), 2),
        's' => number(i64::from(at.second()), 2),
        'S' if count <= 9 => Ok(format!("{nanos_of_second:09}")[..count].to_owned()),
        'S' => Err(()),
        'A' => number(nanos_of_day / 1_000_000, 19),
        'n' => number(nanos_of_second, 19),
        'N' => number(nanos_of_day, 19),
        // Days since the Modified Julian Day epoch, 1858-11-17.
        'g' => number(i64::from(at.date_naive().num_days_from_ce()) - 678_576, 19),
        'V' if count == 2 => Ok("UTC".to_owned()),
        'v' if count == 1 => Ok("UTC".to_owned()),
        'v' if count == 4 => Ok("Coordinated Universal Time".to_owned()),
        'z' if count <= 3 => Ok("UTC".to_owned()),
        'z' if count == 4 => Ok("Coordinated Universal Time".to_owned()),
        'O' if count == 1 || count == 4 => Ok("GMT".to_owned()),
        'X' if count <= 5 => Ok("Z".to_owned()),
        'x' => match count {
            1 => Ok("+00".to_owned()),
            2 | 4 => Ok("+0000".to_owned()),
            3 | 5 => Ok("+00:00".to_owned()),
            _ => Err(()),
        },
        'Z' => match count {
            1..=3 => Ok("+0000".to_owned()),
            4 => Ok("GMT".to_owned()),
            5 => Ok("Z".to_owned()),
            _ => Err(()),
        },
        _ => Err(()),
    }
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

    /// Outputs of `DateTimeFormatter.ofPattern(pattern, Locale.US)` on JDK 17, the
    /// JDK bundled with OpenSearch 2.11, in zone UTC, for four instants. `ERR` is a
    /// pattern it refuses.
    #[test]
    fn timestamps_match_java_date_time_formatter() {
        let instants = [
            "2026-11-09T14:19:56.123456789Z",
            "2026-12-27T00:05:06.007Z",
            "2027-01-01T12:00:00Z",
            "2024-02-29T23:59:59Z",
        ]
        .map(|at| {
            DateTime::parse_from_rfc3339(at)
                .unwrap()
                .with_timezone(&Utc)
        });
        let golden = include_str!("../tests/fixtures/java_date_patterns.tsv");
        for row in golden.lines() {
            let mut columns = row.split('\t');
            let pattern = columns.next().unwrap();
            let first = columns.next().unwrap();
            if first == "ERR" {
                assert_eq!(
                    render_timestamp(pattern, instants[0]),
                    Err(PatternError::Invalid),
                    "{pattern}"
                );
                continue;
            }
            let expected = std::iter::once(first).chain(columns.take(3));
            for (at, expected) in instants.iter().zip(expected) {
                assert_eq!(
                    render_timestamp(pattern, *at).as_deref(),
                    Ok(expected),
                    "{pattern} at {at}"
                );
            }
        }
    }

    /// More of the same JDK, across a day: day periods, zone names and the am/pm
    /// marker, whose wider forms Java refuses. `FMTERR` is a padded field wider than
    /// its pad, which Java accepts as a pattern and fails only when formatting.
    #[test]
    fn day_periods_and_pad_overflow_match_java() {
        let times = [
            (0, 0),
            (0, 5),
            (5, 59),
            (6, 0),
            (11, 59),
            (12, 0),
            (12, 1),
            (17, 59),
            (18, 0),
            (20, 59),
            (21, 0),
            (23, 59),
        ]
        .map(|(hour, minute)| {
            DateTime::parse_from_rfc3339(&format!("2026-11-09T{hour:02}:{minute:02}:00Z"))
                .unwrap()
                .with_timezone(&Utc)
        });
        let golden = include_str!("../tests/fixtures/java_date_periods.tsv");
        for row in golden.lines() {
            let mut columns = row.split('\t');
            let pattern = columns.next().unwrap();
            let expected: Vec<&str> = columns.collect();
            if expected == ["ERR"] {
                assert_eq!(
                    render_timestamp(pattern, times[0]),
                    Err(PatternError::Invalid),
                    "{pattern}"
                );
                continue;
            }
            for (at, expected) in times.iter().zip(expected) {
                let rendered = render_timestamp(pattern, *at);
                match expected {
                    "FMTERR" => {
                        assert_eq!(
                            rendered,
                            Err(PatternError::PadOverflow),
                            "{pattern} at {at}"
                        )
                    }
                    expected => {
                        assert_eq!(rendered.as_deref(), Ok(expected), "{pattern} at {at}")
                    }
                }
            }
        }
    }

    /// As recorded for each kind of refused expression, and for two that AWS takes.
    #[test]
    fn recorded_expression_refusals() {
        const CONSTRAINT: &str = "The supplied prefix(es) do not satisfy the following constraint:";
        let message = |prefix: &str| {
            validate_prefixes(Some(prefix), Some("e/"))
                .unwrap_err()
                .message()
        };
        for hour in ["!{timestamp:pH}/", "x/!{timestamp:pH}"] {
            assert_eq!(
                message(hour),
                format!("{CONSTRAINT} Invalid input to timestamp evaluation: pH")
            );
        }
        for conversion in [
            "!{timestamp:aa}/",
            "!{timestamp:ZZZZZZ}/",
            "!{firehose:nope}/",
        ] {
            assert_eq!(
                message(conversion),
                format!(
                    "{CONSTRAINT} Invalid conversion character used in the value of the \
                     expression in Prefix"
                )
            );
        }
        assert_eq!(
            message("p/!{timestamp:}/"),
            format!(
                "{CONSTRAINT} Invalid expression usage: p/!{{timestamp:}}/.Must be of the form \
                 !{{<namespace>:<value>}}"
            )
        );
        validate_prefixes(Some("!{timestamp:B}/!{timestamp:vvvv}/"), Some("e/")).unwrap();
        assert_eq!(render("!{timestamp:B}", at(), None), "in the afternoon");
        assert_eq!(
            render("!{timestamp:vvvv}", at(), None),
            "Coordinated Universal Time"
        );
    }

    #[test]
    fn malformed_expressions_are_refused() {
        for prefix in [
            "!{timestamp:yyyy",
            "!{timestamp}",
            "!{timestamp:ddd}",
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
