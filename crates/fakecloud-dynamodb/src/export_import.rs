//! Startup bulk-importer for AWS-format DynamoDB S3 exports (DYNAMODB_JSON).
//!
//! Reads a local copy of an AWS DynamoDB S3 export (manifests + gzipped data
//! files) plus the table's `describe-table` JSON, and materialises it as a
//! fresh table in state. The AWS attribute wire format IS our storage format
//! (`AttributeValue` is a `serde_json::Value`), so items copy through verbatim
//! with no type decoding.

use std::collections::HashMap;
use std::io::Read as _;
use std::path::Path;

use serde_json::Value;

use crate::state::{DynamoTable, ProvisionedThroughput, SharedDynamoDbState};

/// A single DynamoDB item: attribute name -> typed AWS wire value.
type Item = HashMap<String, Value>;

/// Import an AWS-format DynamoDB export as a new table. Returns
/// `(table_name, item_count)` on success.
pub fn import_aws_export(
    state: &SharedDynamoDbState,
    account_id: &str,
    region: &str,
    export_dir: &Path,      // folder holding manifest-summary.json etc.
    describe_table: &Value, // parsed `aws dynamodb describe-table` JSON
) -> Result<(String, usize), String> {
    // Accept either the full `{"Table": {...}}` dump or a bare table object.
    let shape = describe_table.get("Table").unwrap_or(describe_table);
    let table_name = shape["TableName"]
        .as_str()
        .ok_or("describe-table: TableName missing or not a string")?
        .to_string();

    let items = read_export_items(export_dir)?;
    let item_count = items.len();
    let table = build_table(&table_name, region, account_id, shape, items)?;

    let mut guard = state.write();
    let account = guard.get_or_create(account_id);
    if account.tables.contains_key(&table_name) {
        return Err(format!("table already exists: {table_name}"));
    }
    account.tables.insert(table_name.clone(), table);

    Ok((table_name, item_count))
}

/// Walk the manifests (not a prefix listing) and read every item.
fn read_export_items(export_dir: &Path) -> Result<Vec<Item>, String> {
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
        // ponytail: assume the standard `AWSDynamoDB/{export-id}/data/<file>`
        // layout and resolve the key as `<export_dir>/data/<basename>`.
        let basename = s3_key.rsplit('/').next().unwrap_or(s3_key);
        items.extend(read_data_file(&export_dir.join("data").join(basename))?);
    }
    Ok(items)
}

/// Read one gzipped data file; each line is a `{"Item": {...}}` record.
fn read_data_file(path: &Path) -> Result<Vec<Item>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut contents = String::new();
    flate2::read::GzDecoder::new(file)
        .read_to_string(&mut contents)
        .map_err(|e| format!("gunzip {}: {e}", path.display()))?;

    // ponytail: no dedup — AWS exports carry unique primary keys, so this never
    // sees a duplicate. Colliding keys would both land.
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
    // ACTIVE-but-key-corrupt table: every item must carry each declared key
    // attribute (HASH and, if present, RANGE). Presence only — value type is
    // not checked here. Fail the whole import so no partial table lands.
    for elem in &key_schema {
        if elem.key_type != "HASH" && elem.key_type != "RANGE" {
            continue;
        }
        if let Some(item) = items.iter().find(|item| !item.contains_key(&elem.attribute_name)) {
            let role = if elem.key_type == "HASH" {
                "partition"
            } else {
                "sort"
            };
            return Err(format!(
                "table {name}: item missing {role} key attribute {:?} declared in KeySchema: {item:?}",
                elem.attribute_name
            ));
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

        let (name, count) =
            import_aws_export(&state, "123456789012", "us-east-1", &export_dir, &describe)
                .expect("import should succeed");
        assert_eq!(name, "Music");
        assert_eq!(count, 3);

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
}
