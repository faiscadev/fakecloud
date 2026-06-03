//! DNS-1123-safe naming helpers shared by every FakeCloud k8s backend.
//!
//! Kubernetes object names must be DNS-1123 labels: lowercase
//! alphanumerics and dashes, 63 chars max, ending in an alphanumeric.
//! Resource identifiers FakeCloud works with (function names, cluster
//! IDs, task ARNs) routinely violate that, so every backend funnels its
//! identifiers through [`pod_name`] / [`label_safe`].

/// Maximum length of a DNS-1123 label (and therefore a Pod name).
pub const DNS1123_MAX: usize = 63;

/// Lowercase + replace anything outside `[a-z0-9-]` with `-`, then trim
/// trailing dashes so the result ends in an alphanumeric (a DNS-1123
/// label requirement). Does not enforce the length cap — callers that
/// need a bounded label should truncate the result.
pub fn label_safe(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    while out.ends_with('-') {
        out.pop();
    }
    out
}

/// 12-char hex projection of an arbitrary string. Used to suffix Pod
/// names so that a changed identifier (a new deploy, a recreated
/// resource) produces a fresh, non-colliding Pod name while staying
/// deterministic — the launcher and any later lookup agree on the name.
/// Not cryptographic; the 2^48 space is ample for per-resource
/// uniqueness within a namespace.
pub fn simple_hex12(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let bytes = h.finalize();
    bytes.iter().take(6).map(|b| format!("{b:02x}")).collect()
}

/// Build a deterministic, DNS-1123-safe Pod name of the form
/// `<prefix>-<id-part>-<hex12>`.
///
/// `prefix` is the service marker (e.g. `fakecloud-lambda`,
/// `fakecloud-ec`); `id` is the resource identifier whose hash forms the
/// stable suffix; `id_part` is a human-readable slug derived from
/// `id` (or a separate display name) and truncated so the whole name
/// fits in [`DNS1123_MAX`].
pub fn pod_name(prefix: &str, id_part: &str, id: &str) -> String {
    let digest = simple_hex12(id);
    // Reserve room for: prefix + '-' + slug + '-' + 12-char digest.
    let fixed = prefix.len() + 1 + 1 + digest.len();
    let slug_budget = DNS1123_MAX.saturating_sub(fixed);
    let slug: String = label_safe(id_part).chars().take(slug_budget).collect();
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        format!("{prefix}-{digest}")
    } else {
        format!("{prefix}-{slug}-{digest}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_safe_lowercases_and_dashes() {
        assert_eq!(label_safe("My_Awesome.Fn"), "my-awesome-fn");
    }

    #[test]
    fn label_safe_trims_trailing_dashes() {
        assert_eq!(label_safe("foo___"), "foo");
    }

    #[test]
    fn pod_name_is_dns1123_safe() {
        let name = pod_name("fakecloud-lambda", "My_Awesome_Function", "abc/123+def==");
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        assert!(!name.ends_with('-'));
        assert!(name.len() <= DNS1123_MAX, "{} too long", name);
    }

    #[test]
    fn pod_name_stable_for_same_inputs() {
        assert_eq!(
            pod_name("fakecloud-ec", "redis-1", "deploy"),
            pod_name("fakecloud-ec", "redis-1", "deploy")
        );
    }

    #[test]
    fn pod_name_differs_when_id_changes() {
        assert_ne!(
            pod_name("fakecloud-ec", "c", "id-1"),
            pod_name("fakecloud-ec", "c", "id-2")
        );
    }

    #[test]
    fn pod_name_handles_very_long_slug() {
        let long = "a".repeat(200);
        let name = pod_name("fakecloud-elasticache", &long, "x");
        assert!(name.len() <= DNS1123_MAX);
        assert!(name.starts_with("fakecloud-elasticache-"));
    }

    #[test]
    fn pod_name_handles_empty_slug() {
        // An id_part of only special chars collapses to empty -> name is
        // prefix + digest, still valid.
        let name = pod_name("fakecloud-rds", "___", "id");
        assert!(name.starts_with("fakecloud-rds-"));
        assert!(!name.ends_with('-'));
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    }
}
