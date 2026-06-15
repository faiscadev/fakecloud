//! Operator-configurable Pod scheduling + metadata shared by every
//! FakeCloud k8s backend.
//!
//! Lets operators attach a `nodeSelector`, taint `tolerations`, and
//! arbitrary `annotations` to the Pods FakeCloud creates, without each
//! service re-implementing the parsing and application. Each of the three
//! is resolvable at three levels, lowest to highest precedence:
//!
//! 1. Global env  - `FAKECLOUD_K8S_<KEY>`
//! 2. Service env - `FAKECLOUD_<SERVICE>_K8S_<KEY>`
//!    (e.g. `FAKECLOUD_LAMBDA_K8S_NODE_SELECTOR`)
//! 3. Per-instance tag - a reserved tag on the resource itself
//!    (e.g. a Lambda function tag `fakecloud-k8s/node-selector`)
//!
//! A backend reads the global+service base once at startup with
//! [`K8sPodConfig::resolved_base`], then per-Pod merges the resource's
//! tags over it with [`K8sPodConfig::merge`] + [`K8sPodConfig::from_tags`]
//! and writes the result onto the built Pod with [`K8sPodConfig::apply`].
//!
//! Value formats:
//! - `NODE_SELECTOR` / `ANNOTATIONS`: a flat `key=value,key=value` map.
//! - `TOLERATIONS`: a JSON array of k8s `Toleration` objects.
//!
//! Merge semantics across the three levels: the maps (node selector,
//! annotations) union per key, with the higher level winning on a key
//! clash; tolerations combine additively (a Pod tolerates the union of
//! taints), dropping exact duplicates. This mirrors the repo's SAM
//! Globals convention (maps deep-merge, list-typed keys combine).
//!
//! Error handling is asymmetric on purpose: a malformed *env* value fails
//! fast at startup (an operator typo should be loud), but a malformed
//! per-instance *tag* is warned about and ignored for that field -- a bad
//! tag must never make a single resource un-runnable.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Pod, Toleration};

/// Reserved per-instance tag key carrying a `key=value,key=value`
/// node-selector for this resource's Pod.
pub const TAG_NODE_SELECTOR: &str = "fakecloud-k8s/node-selector";
/// Reserved per-instance tag key carrying a JSON array of `Toleration`s.
pub const TAG_TOLERATIONS: &str = "fakecloud-k8s/tolerations";
/// Reserved per-instance tag key carrying `key=value,key=value`
/// annotations merged onto this resource's Pod.
pub const TAG_ANNOTATIONS: &str = "fakecloud-k8s/annotations";

/// The global env prefix every service's base config inherits from.
const GLOBAL_PREFIX: &str = "FAKECLOUD_K8S";

/// Resolved node selector, tolerations, and annotations for a Pod.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct K8sPodConfig {
    /// `spec.nodeSelector` entries.
    pub node_selector: BTreeMap<String, String>,
    /// `spec.tolerations` entries.
    pub tolerations: Vec<Toleration>,
    /// `metadata.annotations` entries.
    pub annotations: BTreeMap<String, String>,
}

/// Errors parsing the `FAKECLOUD_*_K8S_*` Pod configuration. Only env
/// parsing fails fast; per-instance tags warn and fall back instead.
#[derive(Debug, thiserror::Error)]
pub enum K8sPodConfigError {
    #[error("{0} is not a valid JSON array of Tolerations: {1}")]
    InvalidTolerations(String, String),
}

impl K8sPodConfig {
    /// Read a single env level. `prefix` is the part before the key, e.g.
    /// `FAKECLOUD_K8S` (global) or `FAKECLOUD_LAMBDA_K8S` (service). Reads
    /// `{prefix}_NODE_SELECTOR`, `{prefix}_TOLERATIONS`, and
    /// `{prefix}_ANNOTATIONS`. Empty/whitespace values are treated as
    /// unset; malformed tolerations JSON fails fast.
    pub fn from_env_prefix(prefix: &str) -> Result<Self, K8sPodConfigError> {
        let node_selector = env_value(prefix, "NODE_SELECTOR")
            .map(|raw| parse_kv_map(&raw))
            .unwrap_or_default();
        let annotations = env_value(prefix, "ANNOTATIONS")
            .map(|raw| parse_kv_map(&raw))
            .unwrap_or_default();
        let tolerations = match env_value(prefix, "TOLERATIONS") {
            Some(raw) => parse_tolerations(&raw).map_err(|e| {
                K8sPodConfigError::InvalidTolerations(format!("{prefix}_TOLERATIONS"), e)
            })?,
            None => Vec::new(),
        };
        Ok(Self {
            node_selector,
            tolerations,
            annotations,
        })
    }

    /// The global+service base for a backend: the global
    /// `FAKECLOUD_K8S_*` config with the per-service
    /// `{service_prefix}_*` config merged over it. Call once at backend
    /// startup and keep the result; merge per-instance tags over it later.
    pub fn resolved_base(service_prefix: &str) -> Result<Self, K8sPodConfigError> {
        let global = Self::from_env_prefix(GLOBAL_PREFIX)?;
        let service = Self::from_env_prefix(service_prefix)?;
        Ok(global.merge(service))
    }

    /// Build a config from a resource's reserved tags. Malformed
    /// tolerations are warned about and dropped (a bad tag must not make
    /// the resource un-runnable); the other fields still parse.
    pub fn from_tags(tags: &BTreeMap<String, String>) -> Self {
        let node_selector = tags
            .get(TAG_NODE_SELECTOR)
            .map(|raw| parse_kv_map(raw))
            .unwrap_or_default();
        let annotations = tags
            .get(TAG_ANNOTATIONS)
            .map(|raw| parse_kv_map(raw))
            .unwrap_or_default();
        let tolerations = match tags.get(TAG_TOLERATIONS) {
            Some(raw) => parse_tolerations(raw).unwrap_or_else(|e| {
                tracing::warn!(
                    tag = TAG_TOLERATIONS,
                    error = %e,
                    "ignoring malformed per-instance tolerations tag"
                );
                Vec::new()
            }),
            None => Vec::new(),
        };
        Self {
            node_selector,
            tolerations,
            annotations,
        }
    }

    /// Merge `higher` (higher precedence) over `self`. Map keys from
    /// `higher` win on a clash; tolerations combine additively, dropping
    /// exact duplicates.
    pub fn merge(mut self, higher: Self) -> Self {
        self.node_selector.extend(higher.node_selector);
        self.annotations.extend(higher.annotations);
        for tol in higher.tolerations {
            if !self.tolerations.contains(&tol) {
                self.tolerations.push(tol);
            }
        }
        self
    }

    /// True when nothing is configured, so [`apply`](Self::apply) is a
    /// no-op.
    pub fn is_empty(&self) -> bool {
        self.node_selector.is_empty() && self.tolerations.is_empty() && self.annotations.is_empty()
    }

    /// Write this config onto an already-built Pod: merge the node
    /// selector and tolerations into `spec` and the annotations into
    /// `metadata`. Existing entries are preserved (annotation keys this
    /// config sets win; tolerations are added without duplicating). Safe
    /// when `spec` is `None` -- node selector/tolerations are simply
    /// skipped, annotations still land on the metadata.
    pub fn apply(&self, pod: &mut Pod) {
        if let Some(spec) = pod.spec.as_mut() {
            if !self.node_selector.is_empty() {
                spec.node_selector
                    .get_or_insert_with(BTreeMap::new)
                    .extend(self.node_selector.clone());
            }
            if !self.tolerations.is_empty() {
                let tolerations = spec.tolerations.get_or_insert_with(Vec::new);
                for tol in &self.tolerations {
                    if !tolerations.contains(tol) {
                        tolerations.push(tol.clone());
                    }
                }
            }
        }
        if !self.annotations.is_empty() {
            pod.metadata
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .extend(self.annotations.clone());
        }
    }
}

/// Read `{prefix}_{suffix}` from the environment, treating
/// empty/whitespace as unset (matches the `FAKECLOUD_K8S_*` convention in
/// [`super::env`]).
fn env_value(prefix: &str, suffix: &str) -> Option<String> {
    std::env::var(format!("{prefix}_{suffix}"))
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Parse a flat `key=value,key=value` map. Entries are trimmed; empty
/// entries, entries without `=`, and entries with an empty key are
/// skipped. A repeated key keeps the last value.
fn parse_kv_map(raw: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if let Some((key, value)) = entry.split_once('=') {
            let key = key.trim();
            if key.is_empty() {
                continue;
            }
            map.insert(key.to_string(), value.trim().to_string());
        }
    }
    map
}

/// Parse a JSON array of k8s `Toleration` objects.
fn parse_tolerations(raw: &str) -> Result<Vec<Toleration>, String> {
    serde_json::from_str::<Vec<Toleration>>(raw).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn toleration(key: &str, effect: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some("Exists".to_string()),
            effect: Some(effect.to_string()),
            ..Toleration::default()
        }
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // --- parse_kv_map -------------------------------------------------

    #[test]
    fn parse_kv_map_empty_is_empty() {
        assert!(parse_kv_map("").is_empty());
        assert!(parse_kv_map("   ").is_empty());
    }

    #[test]
    fn parse_kv_map_single_and_multiple() {
        assert_eq!(parse_kv_map("a=b"), map(&[("a", "b")]));
        assert_eq!(parse_kv_map("a=b,c=d"), map(&[("a", "b"), ("c", "d")]));
    }

    #[test]
    fn parse_kv_map_trims_whitespace() {
        assert_eq!(
            parse_kv_map(" a = b , c=d "),
            map(&[("a", "b"), ("c", "d")])
        );
    }

    #[test]
    fn parse_kv_map_skips_trailing_comma_and_malformed_entries() {
        // trailing comma, an entry with no '=', and an empty-key entry.
        assert_eq!(
            parse_kv_map("a=b,,bad,=v,c=d,"),
            map(&[("a", "b"), ("c", "d")])
        );
    }

    #[test]
    fn parse_kv_map_duplicate_key_keeps_last() {
        assert_eq!(parse_kv_map("a=b,a=c"), map(&[("a", "c")]));
    }

    #[test]
    fn parse_kv_map_value_may_contain_equals() {
        assert_eq!(parse_kv_map("a=b=c"), map(&[("a", "b=c")]));
    }

    // --- parse_tolerations --------------------------------------------

    #[test]
    fn parse_tolerations_valid() {
        let raw = r#"[{"key":"dedicated","operator":"Equal","value":"gpu","effect":"NoSchedule"}]"#;
        let tols = parse_tolerations(raw).unwrap();
        assert_eq!(tols.len(), 1);
        assert_eq!(tols[0].key.as_deref(), Some("dedicated"));
        assert_eq!(tols[0].value.as_deref(), Some("gpu"));
        assert_eq!(tols[0].effect.as_deref(), Some("NoSchedule"));
    }

    #[test]
    fn parse_tolerations_invalid_errors() {
        assert!(parse_tolerations("not json").is_err());
        // A JSON object (not an array) is also rejected.
        assert!(parse_tolerations(r#"{"key":"x"}"#).is_err());
    }

    // --- merge --------------------------------------------------------

    #[test]
    fn merge_maps_higher_wins_per_key() {
        let base = K8sPodConfig {
            node_selector: map(&[("disktype", "ssd"), ("zone", "a")]),
            annotations: map(&[("team", "core")]),
            ..Default::default()
        };
        let higher = K8sPodConfig {
            node_selector: map(&[("disktype", "nvme")]),
            annotations: map(&[("team", "infra"), ("env", "dev")]),
            ..Default::default()
        };
        let merged = base.merge(higher);
        assert_eq!(
            merged.node_selector,
            map(&[("disktype", "nvme"), ("zone", "a")])
        );
        assert_eq!(
            merged.annotations,
            map(&[("team", "infra"), ("env", "dev")])
        );
    }

    #[test]
    fn merge_tolerations_additive_and_deduped() {
        let base = K8sPodConfig {
            tolerations: vec![toleration("a", "NoSchedule")],
            ..Default::default()
        };
        let higher = K8sPodConfig {
            tolerations: vec![toleration("a", "NoSchedule"), toleration("b", "NoExecute")],
            ..Default::default()
        };
        let merged = base.merge(higher);
        assert_eq!(
            merged.tolerations,
            vec![toleration("a", "NoSchedule"), toleration("b", "NoExecute")]
        );
    }

    // --- from_tags ----------------------------------------------------

    #[test]
    fn from_tags_parses_all_three() {
        let tags = map(&[
            (TAG_NODE_SELECTOR, "disktype=ssd"),
            (TAG_ANNOTATIONS, "team=core"),
            (
                TAG_TOLERATIONS,
                r#"[{"key":"a","operator":"Exists","effect":"NoSchedule"}]"#,
            ),
        ]);
        let cfg = K8sPodConfig::from_tags(&tags);
        assert_eq!(cfg.node_selector, map(&[("disktype", "ssd")]));
        assert_eq!(cfg.annotations, map(&[("team", "core")]));
        assert_eq!(cfg.tolerations, vec![toleration("a", "NoSchedule")]);
    }

    #[test]
    fn from_tags_absent_is_empty() {
        assert!(K8sPodConfig::from_tags(&BTreeMap::new()).is_empty());
    }

    #[test]
    fn from_tags_malformed_tolerations_warns_and_skips_but_keeps_other_fields() {
        let tags = map(&[
            (TAG_NODE_SELECTOR, "disktype=ssd"),
            (TAG_TOLERATIONS, "not json"),
        ]);
        let cfg = K8sPodConfig::from_tags(&tags);
        assert_eq!(cfg.node_selector, map(&[("disktype", "ssd")]));
        assert!(cfg.tolerations.is_empty());
    }

    // --- apply --------------------------------------------------------

    fn pod_with_spec() -> Pod {
        Pod {
            spec: Some(k8s_openapi::api::core::v1::PodSpec::default()),
            ..Pod::default()
        }
    }

    #[test]
    fn apply_sets_all_three_fields() {
        let cfg = K8sPodConfig {
            node_selector: map(&[("disktype", "ssd")]),
            tolerations: vec![toleration("a", "NoSchedule")],
            annotations: map(&[("team", "core")]),
        };
        let mut pod = pod_with_spec();
        cfg.apply(&mut pod);
        let spec = pod.spec.unwrap();
        assert_eq!(spec.node_selector.unwrap(), map(&[("disktype", "ssd")]));
        assert_eq!(
            spec.tolerations.unwrap(),
            vec![toleration("a", "NoSchedule")]
        );
        assert_eq!(pod.metadata.annotations.unwrap(), map(&[("team", "core")]));
    }

    #[test]
    fn apply_merges_into_existing_annotations() {
        let cfg = K8sPodConfig {
            annotations: map(&[("team", "core")]),
            ..Default::default()
        };
        let mut pod = pod_with_spec();
        pod.metadata.annotations = Some(map(&[("existing", "1")]));
        cfg.apply(&mut pod);
        assert_eq!(
            pod.metadata.annotations.unwrap(),
            map(&[("existing", "1"), ("team", "core")])
        );
    }

    #[test]
    fn apply_empty_config_is_noop() {
        let mut pod = pod_with_spec();
        K8sPodConfig::default().apply(&mut pod);
        let spec = pod.spec.unwrap();
        assert!(spec.node_selector.is_none());
        assert!(spec.tolerations.is_none());
        assert!(pod.metadata.annotations.is_none());
    }

    #[test]
    fn apply_without_spec_still_sets_annotations() {
        let cfg = K8sPodConfig {
            node_selector: map(&[("disktype", "ssd")]),
            annotations: map(&[("team", "core")]),
            ..Default::default()
        };
        let mut pod = Pod::default();
        cfg.apply(&mut pod);
        assert!(pod.spec.is_none());
        assert_eq!(pod.metadata.annotations.unwrap(), map(&[("team", "core")]));
    }

    // --- env parsing (process-global; serialized) ---------------------

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ENV_KEYS: &[&str] = &[
        "FAKECLOUD_K8S_NODE_SELECTOR",
        "FAKECLOUD_K8S_TOLERATIONS",
        "FAKECLOUD_K8S_ANNOTATIONS",
        "FAKECLOUD_LAMBDA_K8S_NODE_SELECTOR",
        "FAKECLOUD_LAMBDA_K8S_TOLERATIONS",
        "FAKECLOUD_LAMBDA_K8S_ANNOTATIONS",
    ];

    struct EnvGuard(Vec<(String, Option<String>)>);
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    fn with_env(set: &[(&str, &str)], f: impl FnOnce()) {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let _restore = EnvGuard(
            ENV_KEYS
                .iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect(),
        );
        for k in ENV_KEYS {
            std::env::remove_var(k);
        }
        for (k, v) in set {
            std::env::set_var(k, v);
        }
        f();
    }

    #[test]
    fn from_env_prefix_reads_all_three() {
        with_env(
            &[
                ("FAKECLOUD_K8S_NODE_SELECTOR", "disktype=ssd"),
                ("FAKECLOUD_K8S_ANNOTATIONS", "team=core"),
                (
                    "FAKECLOUD_K8S_TOLERATIONS",
                    r#"[{"key":"a","operator":"Exists","effect":"NoSchedule"}]"#,
                ),
            ],
            || {
                let cfg = K8sPodConfig::from_env_prefix("FAKECLOUD_K8S").unwrap();
                assert_eq!(cfg.node_selector, map(&[("disktype", "ssd")]));
                assert_eq!(cfg.annotations, map(&[("team", "core")]));
                assert_eq!(cfg.tolerations, vec![toleration("a", "NoSchedule")]);
            },
        );
    }

    #[test]
    fn from_env_prefix_unset_is_empty() {
        with_env(&[], || {
            assert!(K8sPodConfig::from_env_prefix("FAKECLOUD_K8S")
                .unwrap()
                .is_empty());
        });
    }

    #[test]
    fn from_env_prefix_blank_value_treated_as_unset() {
        with_env(&[("FAKECLOUD_K8S_NODE_SELECTOR", "   ")], || {
            assert!(K8sPodConfig::from_env_prefix("FAKECLOUD_K8S")
                .unwrap()
                .node_selector
                .is_empty());
        });
    }

    #[test]
    fn from_env_prefix_invalid_tolerations_fails_fast() {
        with_env(&[("FAKECLOUD_K8S_TOLERATIONS", "not json")], || {
            let err = K8sPodConfig::from_env_prefix("FAKECLOUD_K8S").unwrap_err();
            assert!(
                matches!(err, K8sPodConfigError::InvalidTolerations(var, _) if var == "FAKECLOUD_K8S_TOLERATIONS")
            );
        });
    }

    #[test]
    fn resolved_base_merges_service_over_global() {
        with_env(
            &[
                ("FAKECLOUD_K8S_NODE_SELECTOR", "disktype=ssd,zone=a"),
                ("FAKECLOUD_LAMBDA_K8S_NODE_SELECTOR", "disktype=nvme"),
            ],
            || {
                let cfg = K8sPodConfig::resolved_base("FAKECLOUD_LAMBDA_K8S").unwrap();
                // service overrides disktype, global zone survives.
                assert_eq!(
                    cfg.node_selector,
                    map(&[("disktype", "nvme"), ("zone", "a")])
                );
            },
        );
    }
}
