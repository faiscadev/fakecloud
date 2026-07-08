//! The device-shadow state machine shared by the IoT Data Plane handlers:
//! deep-merge with null-deletion semantics, `state.delta` computation,
//! per-leaf `metadata` timestamp stamping, and epoch-time helpers.
//!
//! Kept in one place so update / get / delete cannot diverge on the shadow
//! document's wire shape.

use serde_json::{json, Map, Value};

/// Current time as epoch seconds. AWS shadow `timestamp` / `metadata` leaves
/// are epoch **seconds**.
pub fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Current time as epoch milliseconds. Retained-message `lastModifiedTime` is
/// epoch **millis**.
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Deep-merge `patch` into `target`. A `null` leaf in `patch` deletes that key
/// from `target` (AWS shadow semantics); nested objects merge recursively;
/// every other value overwrites.
pub fn merge_into(target: &mut Value, patch: &Value) {
    let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) else {
        // Non-object patch overwrites wholesale.
        *target = patch.clone();
        return;
    };
    for (k, v) in patch_obj {
        if v.is_null() {
            target_obj.remove(k);
        } else if v.is_object() && target_obj.get(k).map(Value::is_object).unwrap_or(false) {
            merge_into(target_obj.get_mut(k).expect("present"), v);
        } else {
            target_obj.insert(k.clone(), v.clone());
        }
    }
}

/// Compute `state.delta`: the subset of `desired` whose value differs from
/// `reported` (a leaf missing from `reported`, or nested objects that differ).
/// Returns `None` when there is no difference.
pub fn compute_delta(desired: &Value, reported: &Value) -> Option<Value> {
    let desired_obj = desired.as_object()?;
    let empty = Map::new();
    let reported_obj = reported.as_object().unwrap_or(&empty);
    let mut out = Map::new();
    for (k, dv) in desired_obj {
        match reported_obj.get(k) {
            None => {
                out.insert(k.clone(), dv.clone());
            }
            Some(rv) => {
                if dv.is_object() && rv.is_object() {
                    if let Some(sub) = compute_delta(dv, rv) {
                        out.insert(k.clone(), sub);
                    }
                } else if dv != rv {
                    out.insert(k.clone(), dv.clone());
                }
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

/// Stamp per-leaf `metadata` timestamps for every leaf touched by `patch`.
/// A `null` leaf in `patch` removes the corresponding metadata leaf; objects
/// recurse; arrays become an array of `{ "timestamp": ts }` (one per element);
/// scalars become `{ "timestamp": ts }`.
pub fn stamp_metadata(meta: &mut Value, patch: &Value, ts: i64) {
    let Some(patch_obj) = patch.as_object() else {
        return;
    };
    if !meta.is_object() {
        *meta = json!({});
    }
    let meta_obj = meta.as_object_mut().expect("meta is object");
    for (k, v) in patch_obj {
        if v.is_null() {
            meta_obj.remove(k);
        } else if v.is_object() {
            let entry = meta_obj.entry(k.clone()).or_insert_with(|| json!({}));
            stamp_metadata(entry, v, ts);
        } else {
            meta_obj.insert(k.clone(), metadata_leaf(v, ts));
        }
    }
}

/// Build a fresh metadata leaf for a value: an object of leaves recurses, an
/// array becomes per-element metadata, a scalar becomes `{ "timestamp": ts }`.
fn metadata_leaf(v: &Value, ts: i64) -> Value {
    match v {
        Value::Object(_) => {
            let mut m = json!({});
            stamp_metadata(&mut m, v, ts);
            m
        }
        Value::Array(items) => Value::Array(items.iter().map(|it| metadata_leaf(it, ts)).collect()),
        _ => json!({ "timestamp": ts }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_overwrites_and_recurses() {
        let mut t = json!({"a": 1, "nested": {"x": 1, "y": 2}});
        merge_into(&mut t, &json!({"a": 5, "nested": {"y": 9, "z": 3}}));
        assert_eq!(t, json!({"a": 5, "nested": {"x": 1, "y": 9, "z": 3}}));
    }

    #[test]
    fn merge_null_deletes_leaf() {
        let mut t = json!({"a": 1, "b": 2});
        merge_into(&mut t, &json!({"b": null}));
        assert_eq!(t, json!({"a": 1}));
    }

    #[test]
    fn delta_is_desired_minus_reported() {
        let d = json!({"color": "red", "power": "on", "nested": {"a": 1}});
        let r = json!({"color": "blue", "power": "on", "nested": {"a": 1}});
        let delta = compute_delta(&d, &r).unwrap();
        assert_eq!(delta, json!({"color": "red"}));
    }

    #[test]
    fn delta_none_when_equal() {
        let d = json!({"color": "red"});
        assert!(compute_delta(&d, &d).is_none());
    }

    #[test]
    fn delta_includes_missing_reported_key() {
        let delta = compute_delta(&json!({"a": 1, "b": 2}), &json!({"a": 1})).unwrap();
        assert_eq!(delta, json!({"b": 2}));
    }

    #[test]
    fn metadata_stamped_at_leaves() {
        let mut meta = json!({});
        stamp_metadata(
            &mut meta,
            &json!({"color": "red", "nested": {"a": 1}}),
            1000,
        );
        assert_eq!(meta["color"], json!({"timestamp": 1000}));
        assert_eq!(meta["nested"]["a"], json!({"timestamp": 1000}));
    }

    #[test]
    fn metadata_null_removes_leaf() {
        let mut meta = json!({"color": {"timestamp": 1}, "power": {"timestamp": 1}});
        stamp_metadata(&mut meta, &json!({"power": null}), 2000);
        assert!(meta.get("power").is_none());
        assert_eq!(meta["color"], json!({"timestamp": 1}));
    }
}
