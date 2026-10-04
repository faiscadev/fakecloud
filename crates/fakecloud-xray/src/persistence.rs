//! Snapshot save/load for AWS X-Ray state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{
    LegacyXraySnapshot, SharedXrayState, XraySnapshot, XRAY_SNAPSHOT_SCHEMA_VERSION,
};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read xray persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse xray persistence snapshot: {0}")]
    Parse(String),
    #[error("xray persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedXrayState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let snapshot = parse_snapshot(&bytes)?;
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

#[derive(serde::Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted snapshot, migrating a v1 one (one state per account) by
/// moving every resource into the region its ARN names.
fn parse_snapshot(bytes: &[u8]) -> Result<XraySnapshot, LoadError> {
    let parse_err = |e: serde_json::Error| LoadError::Parse(e.to_string());
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes).map_err(parse_err)?;
    if schema_version > XRAY_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: schema_version,
            supported: XRAY_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    if schema_version == XRAY_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes).map_err(parse_err);
    }
    let legacy: LegacyXraySnapshot = serde_json::from_slice(bytes).map_err(parse_err)?;
    Ok(XraySnapshot {
        schema_version: XRAY_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy.accounts.into_regional(),
    })
}

pub async fn save_snapshot(
    state: &SharedXrayState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = XraySnapshot {
        schema_version: XRAY_SNAPSHOT_SCHEMA_VERSION,
        accounts: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write xray snapshot"),
        Err(err) => tracing::error!(%err, "xray snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::XrayData;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::sync::Mutex;

    struct MemStore(Mutex<Option<Vec<u8>>>);
    impl SnapshotStore for MemStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.0.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    fn state() -> SharedXrayState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        )))
    }

    #[test]
    fn empty_store_is_empty() {
        assert_eq!(
            load_into(&MemStore(Mutex::new(None)), &state()).unwrap(),
            LoadOutcome::Empty
        );
    }

    #[test]
    fn round_trip_restores_groups() {
        let mut accounts: fakecloud_core::multi_account::MultiRegionState<XrayData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let data = accounts.regional_mut("111122223333", "eu-west-1");
        data.groups.insert(
            "g1".to_string(),
            json!({ "GroupName": "g1", "GroupARN": "arn:aws:xray:eu-west-1:111122223333:group/g1/abc" }),
        );
        let snap = XraySnapshot {
            schema_version: XRAY_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        let restored = state();
        assert_eq!(
            load_into(&store, &restored).unwrap(),
            LoadOutcome::Loaded(2)
        );
        let guard = restored.read();
        assert!(guard
            .regional("111122223333", "eu-west-1")
            .unwrap()
            .groups
            .contains_key("g1"));
    }

    #[test]
    fn v1_snapshot_splits_resources_by_arn_region() {
        use fakecloud_core::multi_account::AccountState;
        let mut legacy: MultiAccountState<XrayData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let data = legacy.default_mut();
        let west_group = "arn:aws:xray:eu-west-1:000000000000:group/w/abc";
        let east_group = "arn:aws:xray:us-east-1:000000000000:group/e/def";
        data.groups.insert(
            "w".to_string(),
            json!({ "GroupName": "w", "GroupARN": west_group }),
        );
        data.groups.insert(
            "e".to_string(),
            json!({ "GroupName": "e", "GroupARN": east_group }),
        );
        data.sampling_rules.insert(
            "west-rule".to_string(),
            json!({ "SamplingRule": {
                "RuleName": "west-rule",
                "RuleARN": "arn:aws:xray:eu-west-1:000000000000:sampling-rule/west-rule",
            }}),
        );
        data.tags.insert(
            west_group.to_string(),
            [("k".to_string(), "v".to_string())].into(),
        );
        data.encryption_config = Some(json!({ "Type": "KMS" }));
        let bytes =
            serde_json::to_vec(&json!({ "schema_version": 1, "accounts": legacy })).unwrap();
        let store = MemStore(Mutex::new(Some(bytes)));
        let restored = state();
        load_into(&store, &restored).unwrap();
        let guard = restored.read();
        let east = guard.regional("000000000000", "us-east-1").unwrap();
        let west = guard.regional("000000000000", "eu-west-1").unwrap();
        assert!(east.groups.contains_key("e") && !east.groups.contains_key("w"));
        assert!(west.groups.contains_key("w") && !west.groups.contains_key("e"));
        assert!(west.sampling_rules.contains_key("west-rule"));
        assert!(!east.sampling_rules.contains_key("west-rule"));
        assert!(west.tags.contains_key(west_group));
        // Region-less config stays in the default region.
        assert!(east.encryption_config.is_some());
        assert!(west.encryption_config.is_none());
        // Each region carries its own built-in Default rule with its ARN.
        let default_arn = |d: &XrayData| {
            d.sampling_rules[crate::state::DEFAULT_SAMPLING_RULE]["SamplingRule"]["RuleARN"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            default_arn(west),
            "arn:aws:xray:eu-west-1:000000000000:sampling-rule/Default"
        );
        assert_eq!(
            default_arn(&XrayData::new_for_account("000000000000", "us-east-1", "")),
            default_arn(east)
        );
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: fakecloud_core::multi_account::MultiRegionState<XrayData> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": XRAY_SNAPSHOT_SCHEMA_VERSION + 1,
            "accounts": accounts,
        }))
        .unwrap();
        let store = MemStore(Mutex::new(Some(bytes)));
        assert!(matches!(
            load_into(&store, &state()),
            Err(LoadError::SchemaTooNew { .. })
        ));
    }
}
