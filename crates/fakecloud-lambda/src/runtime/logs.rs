//! Per-invocation log capture from container runtime logs.
//!
//! Docker, Podman and Kubernetes keep a timestamp with every line a
//! container writes. An invocation's output is what its instance wrote after
//! the position its log was at when the event was dispatched (the cursor):
//! the latest timestamp, and how many lines of each stream carry it, since a
//! runtime may print several lines within the timestamp's precision (Podman
//! reports whole seconds). Stdout and stderr are merged back by timestamp,
//! each stream keeping its own order.

use chrono::{DateTime, FixedOffset};

/// How many trailing lines a cursor is computed from.
pub(crate) const CURSOR_TAIL_LINES: usize = 100;

/// Split a `--timestamps` log line into its timestamp and text.
fn split(line: &str) -> Option<(DateTime<FixedOffset>, &str)> {
    let (ts, text) = line.split_once(' ').unwrap_or((line, ""));
    DateTime::parse_from_rfc3339(ts).ok().map(|t| (t, text))
}

/// The cursor for a log whose tail is `streams` (each a `--timestamps` read
/// of one stream, in stream order): `<latest timestamp>|<lines at it in
/// stream 0>|<... stream 1>...`. `None` for an empty log.
pub(crate) fn cursor(streams: &[&str]) -> Option<String> {
    let latest = streams
        .iter()
        .flat_map(|s| s.lines())
        .filter_map(split)
        .map(|(t, _)| t)
        .max()?;
    let mut out = latest.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    for stream in streams {
        let at = stream
            .lines()
            .filter_map(split)
            .filter(|(t, _)| *t == latest)
            .count();
        out.push_str(&format!("|{at}"));
    }
    Some(out)
}

/// Whether a cursor computed from a tail read can be trusted: some line in
/// the tail is older than the latest timestamp, so every line sharing it was
/// read. When the whole tail shares it, older lines may share it too; the
/// cursor must then be computed from a `--since` read of that timestamp.
pub(crate) fn tail_covers_cursor(streams: &[&str]) -> bool {
    let stamps: Vec<_> = streams
        .iter()
        .flat_map(|s| s.lines())
        .filter_map(split)
        .map(|(t, _)| t)
        .collect();
    match stamps.iter().max() {
        Some(latest) => stamps.iter().any(|t| t < latest),
        None => true,
    }
}

/// The timestamp a cursor starts at, for a `--since` read.
pub(crate) fn cursor_since(cursor: &str) -> &str {
    cursor.split('|').next().unwrap_or(cursor)
}

/// The lines of `streams` written after `cursor` (all of them for `None`),
/// interleaved by timestamp, without timestamps.
pub(crate) fn lines_since(streams: &[&str], cursor: Option<&str>) -> String {
    let mut parts = cursor.map(|c| c.split('|'));
    let since = parts
        .as_mut()
        .and_then(Iterator::next)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
    let skips: Vec<usize> = parts
        .map(|p| p.filter_map(|n| n.parse().ok()).collect())
        .unwrap_or_default();
    let mut lines: Vec<(DateTime<FixedOffset>, usize, usize, &str)> = Vec::new();
    for (stream, text) in streams.iter().enumerate() {
        let mut skip = skips.get(stream).copied().unwrap_or(0);
        for (n, line) in text.lines().enumerate() {
            let Some((t, text)) = split(line) else {
                continue;
            };
            match since {
                Some(s) if t < s => continue,
                Some(s) if t == s && skip > 0 => {
                    skip -= 1;
                    continue;
                }
                _ => lines.push((t, stream, n, text)),
            }
        }
    }
    lines.sort_by_key(|(t, stream, n, _)| (*t, *stream, *n));
    let mut out = String::new();
    for (_, _, _, text) in lines {
        out.push_str(text);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_lines_after_the_cursor_in_write_order() {
        let before_out = "2026-11-11T12:00:00.100000000Z old out\n";
        let before_err = "2026-11-11T12:00:00.200Z old err\n";
        let c = cursor(&[before_out, before_err]).unwrap();
        assert_eq!(c, "2026-11-11T12:00:00.200000000Z|0|1");
        assert_eq!(cursor_since(&c), "2026-11-11T12:00:00.200000000Z");
        let stdout = "2026-11-11T12:00:00.200Z same instant, new\n\
                      2026-11-11T12:00:01.100Z first\n\
                      2026-11-11T12:00:01.300Z third\n";
        let stderr = "2026-11-11T12:00:00.200Z old err\n\
                      2026-11-11T12:00:01.2Z second\n";
        assert_eq!(
            lines_since(&[stdout, stderr], Some(&c)),
            "same instant, new\nfirst\nsecond\nthird\n"
        );
        assert_eq!(cursor(&["", ""]), None);
        assert_eq!(lines_since(&[before_out], None), "old out\n");
    }

    /// Podman reports whole seconds: lines printed in the cursor's second
    /// before the event are skipped by count, later ones kept.
    #[test]
    fn a_tail_that_all_shares_the_latest_timestamp_is_not_trusted() {
        let same = "2026-11-11T10:15:38Z a\n2026-11-11T10:15:38Z b\n";
        assert!(!tail_covers_cursor(&[same, ""]));
        let mixed = "2026-11-11T10:15:37Z a\n2026-11-11T10:15:38Z b\n";
        assert!(tail_covers_cursor(&[mixed, ""]));
        assert!(tail_covers_cursor(&["", ""]));
    }

    #[test]
    fn second_precision_logs_skip_by_count() {
        let tail = "2026-11-11T10:15:38-03:00 a\n2026-11-11T10:15:38-03:00 b\n";
        let c = cursor(&[tail, ""]).unwrap();
        let later = "2026-11-11T10:15:38-03:00 a\n\
                     2026-11-11T10:15:38-03:00 b\n\
                     2026-11-11T10:15:38-03:00 c\n\
                     2026-11-11T10:15:39-03:00 d\n";
        assert_eq!(lines_since(&[later, ""], Some(&c)), "c\nd\n");
    }
}
