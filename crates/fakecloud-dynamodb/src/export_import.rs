//! Startup bulk-importer for AWS-format DynamoDB S3 exports (DYNAMODB_JSON).
//!
//! Reads a local copy of an AWS DynamoDB S3 export (manifests + gzipped data
//! files) plus the table's `describe-table` JSON, and materialises it as a
//! fresh table in state. The AWS attribute wire format IS our storage format
//! (`AttributeValue` is a `serde_json::Value`), so items copy through verbatim
//! with no type decoding.

use std::collections::HashMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::state::{DynamoTable, ProvisionedThroughput, SharedDynamoDbState};

/// A single DynamoDB item: attribute name -> typed AWS wire value.
type Item = HashMap<String, Value>;

/// Outcome of a startup export import.
///
/// The import is idempotent: in persistent-storage mode the snapshot is loaded
/// before this import runs, so on a restart with the same import flags the
/// target table is already present. Rather than erroring (which would refuse to
/// boot), the import is skipped and the already-loaded table is left untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportOutcome {
    /// A fresh table was materialised from the export with `items` items.
    Imported { table: String, items: usize },
    /// A table of the same name already existed in state; import was skipped and
    /// the existing data was left untouched.
    SkippedExisting { table: String },
}

/// Import an AWS-format DynamoDB export as a new table.
///
/// Idempotent: if the target table already exists in state (for example loaded
/// from a persisted snapshot on restart), the import is skipped and the existing
/// data is preserved instead of erroring out.
pub fn import_aws_export(
    state: &SharedDynamoDbState,
    account_id: &str,
    region: &str,
    export_dir: &Path,      // folder holding manifest-summary.json etc.
    describe_table: &Value, // parsed `aws dynamodb describe-table` JSON
) -> Result<ImportOutcome, String> {
    // Accept either the full `{"Table": {...}}` dump or a bare table object.
    let shape = describe_table.get("Table").unwrap_or(describe_table);
    let table_name = shape["TableName"]
        .as_str()
        .ok_or("describe-table: TableName missing or not a string")?
        .to_string();

    // Idempotent restart guard: if the table is already present (e.g. restored
    // from a persisted snapshot before this import runs), leave it untouched and
    // skip rather than error and refuse to boot. Checked before reading the
    // export so a no-op restart does no work.
    if table_exists(state, account_id, &table_name) {
        tracing::warn!(
            table = %table_name,
            "skipping DynamoDB export import: table already exists in state; existing data left untouched"
        );
        return Ok(ImportOutcome::SkippedExisting { table: table_name });
    }

    let (items, declared_total) = read_export_items(export_dir)?;
    let item_count = items.len();
    // Manifest integrity: the summary's declared grand total must match what was
    // actually read. Absent field -> skip the check (older/partial manifests).
    if let Some(declared) = declared_total {
        if declared != item_count {
            return Err(format!(
                "table {table_name}: manifest-summary declares itemCount {declared} but {item_count} items were read (truncated or corrupt export)"
            ));
        }
    }
    let table = build_table(&table_name, region, account_id, shape, items)?;

    let mut guard = state.write();
    let account = guard.get_or_create(account_id);
    // Re-check under the write lock to close any check-then-insert gap and to
    // handle a genuinely conflicting table gracefully (warn + skip, never panic).
    if account.tables.contains_key(&table_name) {
        tracing::warn!(
            table = %table_name,
            "skipping DynamoDB export import: table already exists in state; existing data left untouched"
        );
        return Ok(ImportOutcome::SkippedExisting { table: table_name });
    }
    account.tables.insert(table_name.clone(), table);

    Ok(ImportOutcome::Imported {
        table: table_name,
        items: item_count,
    })
}

/// Multi-table counterpart to `import_aws_export`: imports every immediate
/// subdirectory of `root_dir` as its own table (see docs/services/dynamodb.md
/// for the on-disk layout). Stops at the first table that fails; tables
/// already imported before that point are not rolled back.
pub fn import_aws_exports_dir(
    state: &SharedDynamoDbState,
    account_id: &str,
    region: &str,
    root_dir: &Path,
) -> Result<Vec<ImportOutcome>, String> {
    let mut subdirs: Vec<PathBuf> = std::fs::read_dir(root_dir)
        .map_err(|e| format!("read {}: {e}", root_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    // Deterministic order: stable logging/import order across runs regardless
    // of the OS's directory-listing order.
    subdirs.sort();

    if subdirs.is_empty() {
        return Err(format!(
            "no per-table subdirectories found under {}",
            root_dir.display()
        ));
    }

    let mut outcomes = Vec::with_capacity(subdirs.len());
    for dir in subdirs {
        let describe = read_json(&dir.join("describe-table.json"))
            .map_err(|e| format!("{}: {e}", dir.display()))?;
        let outcome = import_aws_export(state, account_id, region, &dir, &describe)
            .map_err(|e| format!("{}: {e}", dir.display()))?;
        outcomes.push(outcome);
    }

    Ok(outcomes)
}

/// True if `account_id` already holds a table named `table_name`.
fn table_exists(state: &SharedDynamoDbState, account_id: &str, table_name: &str) -> bool {
    state
        .read()
        .get(account_id)
        .is_some_and(|account| account.tables.contains_key(table_name))
}

/// Walk the manifests (not a prefix listing) and read every item. Returns the
/// items plus the summary manifest's declared `itemCount` (if present) so the
/// caller can verify the grand total.
fn read_export_items(export_dir: &Path) -> Result<(Vec<Item>, Option<usize>), String> {
    let summary = read_json(&export_dir.join("manifest-summary.json"))?;
    let format = summary["exportFormat"].as_str().unwrap_or_default();
    if format != "DYNAMODB_JSON" {
        return Err(format!(
            "unsupported exportFormat {format:?} (only DYNAMODB_JSON is supported)"
        ));
    }

    // `manifest-files.json` is JSON Lines (one object per line), each naming a
    // data file via `dataFileS3Key`.
    let manifest = read_text(&export_dir.join("manifest-files.json"))?;
    let mut items = Vec::new();
    for line in nonempty_lines(&manifest) {
        let entry: Value =
            serde_json::from_str(line).map_err(|e| format!("parse manifest-files line: {e}"))?;
        let s3_key = entry["dataFileS3Key"]
            .as_str()
            .ok_or("manifest-files: dataFileS3Key missing")?;
        // The manifest key uses the standard `AWSDynamoDB/{export-id}/data/<file>`
        // layout; resolve it against the local export as `<export_dir>/data/<basename>`.
        let basename = s3_key.rsplit('/').next().unwrap_or(s3_key);
        let file_items = read_data_file(&export_dir.join("data").join(basename))?;
        // Per-file integrity: when the manifest entry declares an itemCount it
        // must match the records actually read from that data file.
        if let Some(declared) = entry["itemCount"].as_u64() {
            if declared as usize != file_items.len() {
                return Err(format!(
                    "data file {basename}: manifest declares itemCount {declared} but {} records were read (truncated or corrupt export)",
                    file_items.len()
                ));
            }
        }
        items.extend(file_items);
    }
    let declared_total = summary["itemCount"].as_u64().map(|n| n as usize);
    Ok((items, declared_total))
}

/// Read one gzipped data file; each line is a `{"Item": {...}}` record.
fn read_data_file(path: &Path) -> Result<Vec<Item>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut contents = String::new();
    flate2::read::GzDecoder::new(file)
        .read_to_string(&mut contents)
        .map_err(|e| format!("gunzip {}: {e}", path.display()))?;

    // No dedup: AWS exports carry unique primary keys, so a single data file
    // never contains a duplicate. Colliding keys would both be retained.
    nonempty_lines(&contents)
        .map(|line| {
            let record: Value =
                serde_json::from_str(line).map_err(|e| format!("parse data line: {e}"))?;
            record["Item"]
                .as_object()
                .map(|obj| obj.clone().into_iter().collect())
                .ok_or_else(|| "data line missing Item object".to_string())
        })
        .collect()
}

/// Build a fresh `DynamoTable` from a `describe-table` shape and its items.
fn build_table(
    name: &str,
    region: &str,
    account_id: &str,
    shape: &Value,
    items: Vec<Item>,
) -> Result<DynamoTable, String> {
    let key_schema =
        crate::parse_key_schema(&shape["KeySchema"]).map_err(|e| format!("KeySchema: {e}"))?;
    let attribute_definitions = crate::parse_attribute_definitions(&shape["AttributeDefinitions"])
        .map_err(|e| format!("AttributeDefinitions: {e}"))?;

    // Guard against a wrong/hand-edited describe-table producing an
    // ACTIVE-but-key-corrupt table. For each declared key attribute (HASH and,
    // if present, RANGE) every item must (1) carry the attribute and (2) carry
    // it with the scalar type declared in AttributeDefinitions -- the same
    // presence + type parity the normal write path enforces
    // (validate_key_in_item / check_key_type). A wrong-typed key would let a
    // correctly-typed read fail to find the row, so the data would appear to
    // vanish. Fail the whole import so no partial table lands.
    for elem in &key_schema {
        if elem.key_type != "HASH" && elem.key_type != "RANGE" {
            continue;
        }
        let role = if elem.key_type == "HASH" {
            "partition"
        } else {
            "sort"
        };
        let expected_type = attribute_definitions
            .iter()
            .find(|d| d.attribute_name == elem.attribute_name)
            .map(|d| d.attribute_type.as_str());
        for item in &items {
            let Some(value) = item.get(&elem.attribute_name) else {
                return Err(format!(
                    "table {name}: item missing {role} key attribute {:?} declared in KeySchema: {item:?}",
                    elem.attribute_name
                ));
            };
            if let Some(expected) = expected_type {
                let actual = crate::state::attribute_type_and_value(value).map(|(ty, _)| ty);
                if actual != Some(expected) {
                    return Err(format!(
                        "table {name}: {role} key attribute {:?} has type {} but AttributeDefinitions declares {expected}: {item:?}",
                        elem.attribute_name,
                        actual.unwrap_or("<none>"),
                    ));
                }
            }
        }
    }

    // BillingModeSummary wins over a bare BillingMode; default to PROVISIONED.
    let billing_mode = shape["BillingModeSummary"]["BillingMode"]
        .as_str()
        .or_else(|| shape["BillingMode"].as_str())
        .unwrap_or("PROVISIONED")
        .to_string();
    let provisioned_throughput = if billing_mode == "PAY_PER_REQUEST" {
        ProvisionedThroughput {
            read_capacity_units: 0,
            write_capacity_units: 0,
        }
    } else {
        crate::parse_provisioned_throughput(&shape["ProvisionedThroughput"])
            .map_err(|e| format!("ProvisionedThroughput: {e}"))?
    };

    let mut table = DynamoTable {
        name: name.to_string(),
        arn: format!("arn:aws:dynamodb:{region}:{account_id}:table/{name}"),
        table_id: uuid::Uuid::new_v4().to_string().replace('-', ""),
        key_schema,
        attribute_definitions,
        provisioned_throughput,
        items,
        // Left unbuilt here; the `recalculate_stats()` below builds it.
        key_index: Default::default(),
        gsi: crate::parse_gsi(&shape["GlobalSecondaryIndexes"], &billing_mode),
        lsi: crate::parse_lsi(&shape["LocalSecondaryIndexes"]),
        tags: crate::parse_tags(&shape["Tags"]),
        created_at: chrono::Utc::now(),
        status: "ACTIVE".to_string(),
        item_count: 0,
        size_bytes: 0,
        billing_mode,
        ttl_attribute: None,
        ttl_enabled: false,
        resource_policy: None,
        pitr_enabled: false,
        kinesis_destinations: Vec::new(),
        contributor_insights_status: "DISABLED".to_string(),
        contributor_insights_counters: std::collections::BTreeMap::new(),
        stream_enabled: false,
        stream_view_type: None,
        stream_arn: None,
        stream_records: std::sync::Arc::new(parking_lot::RwLock::new(Vec::new())),
        sse_type: None,
        sse_kms_key_arn: None,
        deletion_protection_enabled: false,
        on_demand_throughput: None,
        table_class: "STANDARD".to_string(),
        vector_indexes: Vec::new(),
    };
    table.recalculate_stats(); // fills item_count / size_bytes
    Ok(table)
}

fn read_text(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_str(&read_text(path)?).map_err(|e| format!("parse {}: {e}", path.display()))
}

fn nonempty_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines().filter(|l| !l.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn imports_aws_export_into_state() {
        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let describe = read_json(&fixtures.join("describe-table.json")).unwrap();
        let export_dir = fixtures.join("export");

        let outcome =
            import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
                .expect("import should succeed");
        assert_eq!(
            outcome,
            ImportOutcome::Imported {
                table: "Music".to_string(),
                items: 3,
            }
        );

        // Table lives in state with the right item count.
        let mut guard = state.write();
        let table = guard
            .get_or_create("123456789012")
            .tables
            .get("Music")
            .expect("table in state");
        assert_eq!(table.items.len(), 3);
        assert_eq!(table.item_count, 3);

        // Type preservation: the "Foo" item carries every DynamoDB attribute
        // type and each one survives verbatim. Complex types are asserted for
        // FULL equality (not spot-checked) so a serializer regression that
        // dropped a later list element / set member would be caught.
        let foo = table
            .items
            .iter()
            .find(|i| i.get("Artist") == Some(&json!({"S": "Foo"})))
            .expect("item with Artist=Foo");

        // Scalars: whole typed value.
        assert_eq!(foo.get("str"), Some(&json!({"S": "hello"})));
        assert_eq!(foo.get("Plays"), Some(&json!({"N": "42"})));
        assert_eq!(foo.get("bin"), Some(&json!({"B": "aGVsbG8="})));
        assert_eq!(foo.get("flag"), Some(&json!({"BOOL": true})));
        assert_eq!(foo.get("nothing"), Some(&json!({"NULL": true})));

        // List (3 mixed elements) and map (2 fields): exact whole-value equality.
        assert_eq!(
            foo.get("list"),
            Some(&json!({"L": [{"S": "nested"}, {"N": "7"}, {"BOOL": false}]}))
        );
        assert_eq!(
            foo.get("map"),
            Some(&json!({"M": {"genre": {"S": "rock"}, "year": {"N": "1994"}}}))
        );

        // Sets: assert length AND every member (order is preserved verbatim,
        // but check membership so a dropped-element regression can't hide).
        let ss = foo.get("strset").unwrap()["SS"].as_array().unwrap();
        assert_eq!(ss.len(), 2);
        assert!(ss.contains(&json!("x")));
        assert!(ss.contains(&json!("y")));

        let ns = foo.get("numset").unwrap()["NS"].as_array().unwrap();
        assert_eq!(ns.len(), 2);
        assert!(ns.contains(&json!("1")));
        assert!(ns.contains(&json!("2")));

        let bs = foo.get("binset").unwrap()["BS"].as_array().unwrap();
        assert_eq!(bs.len(), 2);
        assert!(bs.contains(&json!("YQ==")));
        assert!(bs.contains(&json!("Yg==")));
    }

    #[tokio::test]
    async fn imported_table_persists_through_snapshot_save() {
        // Regression: a startup bulk-import mutates state directly, so unless it
        // is written through the DynamoDB snapshot store it is durable only if a
        // later mutating API call happens to trigger a save. A read-only workload
        // would lose the imported table on restart. Exercise import -> save ->
        // reload (the exact path the server now wires) and assert the table
        // survives.
        use crate::save_dynamodb_snapshot;
        use crate::state::DynamoDbSnapshot;
        use fakecloud_persistence::{DiskSnapshotStore, SnapshotStore};
        use std::sync::Arc;

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let describe = read_json(&fixtures.join("describe-table.json")).unwrap();
        let export_dir = fixtures.join("export");
        let outcome =
            import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
                .expect("import should succeed");
        assert!(matches!(outcome, ImportOutcome::Imported { .. }));

        let tmp = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn SnapshotStore> = Arc::new(DiskSnapshotStore::new(
            tmp.path().join("dynamodb").join("snapshot.json"),
        ));
        let lock = tokio::sync::Mutex::new(());
        let wrote = save_dynamodb_snapshot(&state, Some(store.clone()), &lock)
            .await
            .expect("snapshot save should succeed");
        assert!(
            wrote,
            "a snapshot store is configured, so a save must occur"
        );

        // Reload exactly as boot does: read the bytes back and deserialize.
        let bytes = store
            .load()
            .expect("load ok")
            .expect("snapshot bytes on disk");
        let snapshot: DynamoDbSnapshot =
            serde_json::from_slice(&bytes).expect("snapshot deserializes");
        let accounts = snapshot
            .accounts
            .expect("v2 multi-account snapshot written");
        let account = accounts
            .get("123456789012")
            .expect("account present after reload");
        let table = account
            .tables
            .get("Music")
            .expect("imported table survives snapshot round-trip");
        assert_eq!(table.items.len(), 3, "all imported items are durable");
    }

    #[test]
    fn rejects_items_missing_declared_key_attribute() {
        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let export_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/export");

        // KeySchema names a partition key that the exported items don't carry;
        // the import must fail rather than materialise a key-corrupt table.
        let describe = json!({
            "Table": {
                "TableName": "Music",
                "KeySchema": [{ "AttributeName": "DoesNotExist", "KeyType": "HASH" }],
                "AttributeDefinitions": [
                    { "AttributeName": "DoesNotExist", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }
        });

        let err = import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
            .expect_err("import should fail when items lack the declared key attribute");
        assert!(
            err.contains("DoesNotExist"),
            "error should name the missing key attribute: {err}"
        );
    }

    #[test]
    fn rejects_key_attribute_type_mismatch() {
        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let export_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/export");

        // The exported items carry Artist as a string (`{"S": ...}`), but this
        // describe-table declares the partition key as numeric (`N`). The import
        // must fail with a type mismatch, matching the normal write path's
        // check_key_type, rather than materialise a table a correctly-typed read
        // could never query.
        let describe = json!({
            "Table": {
                "TableName": "MusicTypeMismatch",
                "KeySchema": [
                    { "AttributeName": "Artist", "KeyType": "HASH" },
                    { "AttributeName": "SongTitle", "KeyType": "RANGE" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "Artist", "AttributeType": "N" },
                    { "AttributeName": "SongTitle", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }
        });

        let err = import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
            .expect_err("import should fail when a key attribute has the wrong type");
        assert!(
            err.contains("Artist") && err.contains("type S") && err.contains("declares N"),
            "error should describe the key type mismatch: {err}"
        );
        // Nothing partial landed in state.
        let mut guard = state.write();
        assert!(!guard
            .get_or_create("123456789012")
            .tables
            .contains_key("MusicTypeMismatch"));
    }

    #[test]
    fn import_is_idempotent_when_table_already_exists() {
        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let describe = read_json(&fixtures.join("describe-table.json")).unwrap();
        let export_dir = fixtures.join("export");

        // First import materialises the table (3 items).
        let first = import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
            .expect("first import should succeed");
        assert_eq!(
            first,
            ImportOutcome::Imported {
                table: "Music".to_string(),
                items: 3,
            }
        );

        // Replace the loaded data with a single sentinel row, standing in for a
        // table restored from a persisted snapshot that has since diverged.
        {
            let mut guard = state.write();
            let table = guard
                .get_or_create("123456789012")
                .tables
                .get_mut("Music")
                .expect("table in state");
            let mut sentinel: Item = HashMap::new();
            sentinel.insert("Artist".to_string(), json!({ "S": "SENTINEL" }));
            sentinel.insert("SongTitle".to_string(), json!({ "S": "only" }));
            table.items = vec![sentinel];
            table.recalculate_stats();
        }

        // Re-running with the same flags must skip (not error, not overwrite).
        let second = import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
            .expect("re-import should be a no-op, not an error");
        assert_eq!(
            second,
            ImportOutcome::SkippedExisting {
                table: "Music".to_string(),
            }
        );

        // Existing data was left untouched: still the single sentinel row, not
        // re-populated with the 3 export items.
        let mut guard = state.write();
        let table = guard
            .get_or_create("123456789012")
            .tables
            .get("Music")
            .expect("table still in state");
        assert_eq!(table.items.len(), 1);
        assert_eq!(
            table.items[0].get("Artist"),
            Some(&json!({ "S": "SENTINEL" }))
        );
    }

    #[test]
    fn rejects_manifest_item_count_mismatch() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write as _;

        // Build a throwaway export whose manifest-summary over-declares the item
        // count (99) relative to the single row actually present, simulating a
        // truncated/corrupt export.
        let dir = std::env::temp_dir().join(format!("fc-ddb-import-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("data")).unwrap();
        std::fs::write(
            dir.join("manifest-summary.json"),
            r#"{"version":"2020-06-30","exportFormat":"DYNAMODB_JSON","itemCount":99}"#,
        )
        .unwrap();
        // Per-file entry carries no itemCount, so only the summary-level check
        // can fire here.
        std::fs::write(
            dir.join("manifest-files.json"),
            r#"{"dataFileS3Key":"AWSDynamoDB/01700000000000-abcd/data/0001.json.gz"}"#,
        )
        .unwrap();
        let data_file = std::fs::File::create(dir.join("data/0001.json.gz")).unwrap();
        let mut enc = GzEncoder::new(data_file, Compression::default());
        enc.write_all(b"{\"Item\":{\"Artist\":{\"S\":\"A\"},\"SongTitle\":{\"S\":\"B\"}}}\n")
            .unwrap();
        enc.finish().unwrap();

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let describe = json!({
            "Table": {
                "TableName": "MusicTruncated",
                "KeySchema": [
                    { "AttributeName": "Artist", "KeyType": "HASH" },
                    { "AttributeName": "SongTitle", "KeyType": "RANGE" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "Artist", "AttributeType": "S" },
                    { "AttributeName": "SongTitle", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }
        });

        let err = import_aws_export(&state, "123456789012", "us-east-1", &dir, &describe)
            .expect_err("import should fail when manifest itemCount disagrees with the data");
        assert!(
            err.contains("99") && err.contains("1 items"),
            "error should report the declared vs actual counts: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_per_file_item_count_mismatch() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write as _;

        // manifest-summary agrees with the data (1 item), but the per-file
        // manifest entry over-declares itemCount (5). The per-file integrity
        // check must catch it.
        let dir = std::env::temp_dir().join(format!("fc-ddb-import-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("data")).unwrap();
        std::fs::write(
            dir.join("manifest-summary.json"),
            r#"{"version":"2020-06-30","exportFormat":"DYNAMODB_JSON","itemCount":1}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("manifest-files.json"),
            r#"{"itemCount":5,"dataFileS3Key":"AWSDynamoDB/01700000000000-abcd/data/0001.json.gz"}"#,
        )
        .unwrap();
        let data_file = std::fs::File::create(dir.join("data/0001.json.gz")).unwrap();
        let mut enc = GzEncoder::new(data_file, Compression::default());
        enc.write_all(b"{\"Item\":{\"Artist\":{\"S\":\"A\"},\"SongTitle\":{\"S\":\"B\"}}}\n")
            .unwrap();
        enc.finish().unwrap();

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let describe = json!({
            "Table": {
                "TableName": "MusicPerFile",
                "KeySchema": [
                    { "AttributeName": "Artist", "KeyType": "HASH" },
                    { "AttributeName": "SongTitle", "KeyType": "RANGE" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "Artist", "AttributeType": "S" },
                    { "AttributeName": "SongTitle", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }
        });

        let err = import_aws_export(&state, "123456789012", "us-east-1", &dir, &describe)
            .expect_err("import should fail when a data file's itemCount is wrong");
        assert!(
            err.contains("0001.json.gz") && err.contains('5'),
            "error should name the data file and declared count: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes a minimal self-contained single-item table export under
    /// `root/<subdir_name>`, as `import_aws_exports_dir` expects.
    fn write_table_subdir(root: &Path, subdir_name: &str, table_name: &str) {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write as _;

        let dir = root.join(subdir_name);
        std::fs::create_dir_all(dir.join("data")).unwrap();
        std::fs::write(
            dir.join("describe-table.json"),
            json!({
                "Table": {
                    "TableName": table_name,
                    "KeySchema": [{ "AttributeName": "Id", "KeyType": "HASH" }],
                    "AttributeDefinitions": [{ "AttributeName": "Id", "AttributeType": "S" }],
                    "BillingMode": "PAY_PER_REQUEST"
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.join("manifest-summary.json"),
            r#"{"version":"2020-06-30","exportFormat":"DYNAMODB_JSON","itemCount":1}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("manifest-files.json"),
            r#"{"itemCount":1,"dataFileS3Key":"AWSDynamoDB/01700000000000-abcd/data/0001.json.gz"}"#,
        )
        .unwrap();
        let data_file = std::fs::File::create(dir.join("data/0001.json.gz")).unwrap();
        let mut enc = GzEncoder::new(data_file, Compression::default());
        enc.write_all(format!(r#"{{"Item":{{"Id":{{"S":"{table_name}-row"}}}}}}"#).as_bytes())
            .unwrap();
        enc.write_all(b"\n").unwrap();
        enc.finish().unwrap();
    }

    #[test]
    fn imports_multiple_tables_from_root_dir() {
        let root = std::env::temp_dir().join(format!("fc-ddb-multi-{}", uuid::Uuid::new_v4()));
        write_table_subdir(&root, "a-table", "TableA");
        write_table_subdir(&root, "b-table", "TableB");

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));

        let outcomes = import_aws_exports_dir(&state, "123456789012", "us-east-1", &root)
            .expect("multi-table import should succeed");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes
            .iter()
            .all(|o| matches!(o, ImportOutcome::Imported { items: 1, .. })));

        let mut guard = state.write();
        let account = guard.get_or_create("123456789012");
        assert!(account.tables.contains_key("TableA"));
        assert!(account.tables.contains_key("TableB"));
        drop(guard);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_table_import_fails_when_root_has_no_subdirectories() {
        let root =
            std::env::temp_dir().join(format!("fc-ddb-multi-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let err = import_aws_exports_dir(&state, "123456789012", "us-east-1", &root)
            .expect_err("empty root should fail");
        assert!(err.contains("no per-table subdirectories"));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_table_import_fails_on_subdir_missing_describe_table() {
        let root = std::env::temp_dir().join(format!("fc-ddb-multi-bad-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("bad-table")).unwrap();

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let err = import_aws_exports_dir(&state, "123456789012", "us-east-1", &root)
            .expect_err("subdir without describe-table.json should fail");
        assert!(
            err.contains("bad-table"),
            "error should name the offending subdirectory: {err}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_table_import_stops_at_first_failure_leaving_earlier_tables_committed() {
        let root =
            std::env::temp_dir().join(format!("fc-ddb-multi-partial-{}", uuid::Uuid::new_v4()));
        write_table_subdir(&root, "a-table", "TableA");
        std::fs::create_dir_all(root.join("b-table")).unwrap(); // no describe-table.json

        let state: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let err = import_aws_exports_dir(&state, "123456789012", "us-east-1", &root)
            .expect_err("second table should fail");
        assert!(err.contains("b-table"));

        // "a-table" sorts before "b-table", so it was already imported and
        // committed to state before the second subdirectory failed.
        let mut guard = state.write();
        assert!(guard
            .get_or_create("123456789012")
            .tables
            .contains_key("TableA"));
        drop(guard);

        let _ = std::fs::remove_dir_all(&root);
    }
}
