//! Generic create/get/update/delete/list helpers over a [`JsonStore`].
//!
//! Many Glue control-plane families are CRUD over a name-keyed resource. These
//! helpers persist the create input verbatim and serve reads from it, so the
//! data round-trips. Each caller supplies the field allow-list for the
//! resource's output shape so responses never carry undeclared fields.

use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

use crate::common::{already_exists, entity_not_found};
use crate::state::JsonStore;

/// Create a uniquely-named resource. `key` is the resource's identifier value;
/// `stored` is the value persisted (typically the built entity JSON). Errors
/// with `AlreadyExistsException` when the key already exists.
pub(crate) fn create_unique(
    store: &mut JsonStore,
    key: &str,
    stored: Value,
    kind: &str,
) -> Result<(), AwsServiceError> {
    if store.contains_key(key) {
        return Err(already_exists(format!("{kind} {key} already exists")));
    }
    store.insert(key.to_string(), stored);
    Ok(())
}

/// Delete a resource by key, erroring when absent.
pub(crate) fn delete(store: &mut JsonStore, key: &str, kind: &str) -> Result<(), AwsServiceError> {
    store
        .remove(key)
        .map(|_| ())
        .ok_or_else(|| entity_not_found(format!("{kind} {key} not found")))
}

/// Merge update fields into an existing resource, erroring when absent.
pub(crate) fn update_merge(
    store: &mut JsonStore,
    key: &str,
    kind: &str,
    updates: Vec<(&str, Value)>,
) -> Result<(), AwsServiceError> {
    let existing = store
        .get_mut(key)
        .ok_or_else(|| entity_not_found(format!("{kind} {key} not found")))?;
    if let Some(obj) = existing.as_object_mut() {
        for (k, v) in updates {
            obj.insert(k.to_string(), v);
        }
    }
    Ok(())
}
