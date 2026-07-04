//! Snapshot save/load for AWS AppConfig state.

use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::state::{AppConfigSnapshot, SharedAppConfigState, APPCONFIG_SNAPSHOT_SCHEMA_VERSION};

#[derive(Debug, PartialEq, Eq)]
pub enum LoadOutcome {
    Empty,
    Loaded(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read appconfig persistence snapshot: {0}")]
    Io(String),
    #[error("failed to parse appconfig persistence snapshot: {0}")]
    Parse(String),
    #[error("appconfig persistence schema too new: on-disk={on_disk}, max supported={supported}")]
    SchemaTooNew { on_disk: u32, supported: u32 },
}

pub fn load_into(
    store: &dyn SnapshotStore,
    state: &SharedAppConfigState,
) -> Result<LoadOutcome, LoadError> {
    let Some(bytes) = store.load().map_err(|e| LoadError::Io(e.to_string()))? else {
        return Ok(LoadOutcome::Empty);
    };
    let snapshot: AppConfigSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Parse(e.to_string()))?;
    if snapshot.schema_version > APPCONFIG_SNAPSHOT_SCHEMA_VERSION {
        return Err(LoadError::SchemaTooNew {
            on_disk: snapshot.schema_version,
            supported: APPCONFIG_SNAPSHOT_SCHEMA_VERSION,
        });
    }
    let accounts = snapshot.accounts.account_count();
    *state.write() = snapshot.accounts;
    Ok(LoadOutcome::Loaded(accounts))
}

pub async fn save_snapshot(
    state: &SharedAppConfigState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = AppConfigSnapshot {
        schema_version: APPCONFIG_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write appconfig snapshot"),
        Err(err) => tracing::error!(%err, "appconfig snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AppConfigState, ApplicationRecord, HostedVersionRecord, ProfileRecord};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;
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

    fn state() -> SharedAppConfigState {
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
    fn round_trip_restores_hosted_version_bytes() {
        let mut accounts: MultiAccountState<AppConfigState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        {
            let acct = accounts.get_or_create("000000000000");
            let mut profile = ProfileRecord {
                id: "prof123".to_string(),
                application_id: "app1234".to_string(),
                name: "p".to_string(),
                description: None,
                location_uri: "hosted".to_string(),
                retrieval_role_arn: None,
                validators: serde_json::json!([]),
                profile_type: "AWS.Freeform".to_string(),
                kms_key_arn: None,
                kms_key_identifier: None,
                hosted_versions: BTreeMap::new(),
                next_version_number: 2,
            };
            profile.hosted_versions.insert(
                1,
                HostedVersionRecord {
                    version_number: 1,
                    description: None,
                    content: b"\x00\x01\x02hello".to_vec(),
                    content_type: "application/json".to_string(),
                    version_label: None,
                },
            );
            let mut app = ApplicationRecord {
                id: "app1234".to_string(),
                name: "app".to_string(),
                description: None,
                environments: BTreeMap::new(),
                profiles: BTreeMap::new(),
                experiments: BTreeMap::new(),
            };
            app.profiles.insert("prof123".to_string(), profile);
            acct.applications.insert("app1234".to_string(), app);
        }
        let snap = AppConfigSnapshot {
            schema_version: APPCONFIG_SNAPSHOT_SCHEMA_VERSION,
            accounts,
        };
        let store = MemStore(Mutex::new(Some(serde_json::to_vec(&snap).unwrap())));
        let restored = state();
        assert_eq!(
            load_into(&store, &restored).unwrap(),
            LoadOutcome::Loaded(1)
        );
        let guard = restored.read();
        let acct = guard.get("000000000000").unwrap();
        let bytes = &acct.applications["app1234"].profiles["prof123"].hosted_versions[&1].content;
        assert_eq!(bytes, b"\x00\x01\x02hello");
    }

    #[test]
    fn rejects_future_schema() {
        let accounts: MultiAccountState<AppConfigState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": APPCONFIG_SNAPSHOT_SCHEMA_VERSION + 1,
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
