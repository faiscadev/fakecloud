use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use fakecloud_aws::arn::Arn;

/// A table's ARN (`arn:<partition>:dynamodb:<region>:<account>:table/<name>`),
/// in the region's partition. Stream, backup, export and import ARNs extend it
/// with `/<kind>/<id>`.
pub fn table_arn(region: &str, account_id: &str, name: &str) -> String {
    Arn::regional("dynamodb", region, account_id, &format!("table/{name}")).to_string()
}

/// A global table's ARN: no region field, in the partition of `region`.
pub fn global_table_arn(region: &str, account_id: &str, name: &str) -> String {
    Arn::global_in(
        region,
        "dynamodb",
        account_id,
        &format!("global-table/{name}"),
    )
    .to_string()
}

fn empty_stream_records() -> Arc<RwLock<Vec<StreamRecord>>> {
    Arc::new(RwLock::new(Vec::new()))
}

/// Serde for `Arc<RwLock<Vec<StreamRecord>>>`: persist the inner change records
/// so a stream consumer's un-read records survive a snapshot restart
/// (bug-audit 2026-05-28, 4.5). The field was `#[serde(skip)]`, so table data
/// was preserved across restart but pending stream records silently vanished.
mod stream_records_serde {
    use super::{Arc, RwLock, StreamRecord};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        v: &Arc<RwLock<Vec<StreamRecord>>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.read().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Arc<RwLock<Vec<StreamRecord>>>, D::Error> {
        let records = Vec::<StreamRecord>::deserialize(d)?;
        // Raise the in-memory sequence-number floor above every persisted
        // record so newly-minted numbers cannot collide with them after a
        // restart, even if the wall-clock seed went backwards (4.4 / Cubic).
        for r in &records {
            crate::streams::observe_stream_sequence(&r.dynamodb.sequence_number);
        }
        Ok(Arc::new(RwLock::new(records)))
    }
}

/// A single DynamoDB attribute value (tagged union matching the AWS wire format).
/// AWS sends attribute values as `{"S": "hello"}`, `{"N": "42"}`, etc.
pub type AttributeValue = Value;

/// Extract the "typed" inner value for comparison purposes.
/// Returns (type_tag, inner_value) e.g. ("S", "hello") or ("N", "42").
pub fn attribute_type_and_value(av: &Value) -> Option<(&str, &Value)> {
    let obj = av.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (k, v) = obj.iter().next()?;
    Some((k.as_str(), v))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySchemaElement {
    pub attribute_name: String,
    pub key_type: String, // HASH or RANGE
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String, // S, N, B
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionedThroughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}

/// On-demand capacity caps for PAY_PER_REQUEST tables and GSIs. Real AWS
/// accepts both fields independently; `-1` (the AWS sentinel for "no cap")
/// is the default and is what `DescribeTable` returns when the caller never
/// set a value — the Terraform provider asserts on that exact value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnDemandThroughput {
    pub max_read_request_units: i64,
    pub max_write_request_units: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSecondaryIndex {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub on_demand_throughput: Option<OnDemandThroughput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSecondaryIndex {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
}

/// A vector index: a similarity-search index over one list-valued attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndex {
    pub index_name: String,
    pub index_arn: String,
    /// The attribute holding each item's vector.
    pub vector_attribute: String,
    pub dimensions: i64,
    /// `VectorDistanceFunction` (COSINE | DOT_PRODUCT | EUCLIDEAN).
    pub distance_function: String,
    /// `SearchSchema` entries as `(AttributeName, SearchSchemaElementType)`.
    pub search_schema: Vec<(String, String)>,
    pub projection: Projection,
    /// When the index was added to a live table by UpdateTable. Such an index
    /// builds online, on the GSI machinery: it allocates resources, then
    /// backfills, and only then serves searches (see
    /// [`VectorIndex::phase`]). `None` for an index created with its table,
    /// which is ACTIVE as soon as the table is.
    #[serde(default)]
    pub online_created_at: Option<DateTime<Utc>>,
}

/// Where an online-built vector index is in its creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexPhase {
    /// Resources are being allocated: IndexStatus CREATING, Backfilling
    /// false, and the table itself UPDATING.
    Allocating,
    /// Existing items are being indexed: IndexStatus CREATING, Backfilling
    /// true, the table back to ACTIVE.
    Backfilling,
    /// Serving searches.
    Active,
}

/// How long an index added by UpdateTable spends allocating resources.
pub const VECTOR_INDEX_ALLOCATION_MS: i64 = 3_000;
/// How long it then spends backfilling before it serves searches.
pub const VECTOR_INDEX_BACKFILL_MS: i64 = 7_000;

impl VectorIndex {
    /// The creation phase at `now`.
    pub fn phase(&self, now: DateTime<Utc>) -> VectorIndexPhase {
        let Some(started) = self.online_created_at else {
            return VectorIndexPhase::Active;
        };
        let elapsed = (now - started).num_milliseconds();
        if elapsed < VECTOR_INDEX_ALLOCATION_MS {
            VectorIndexPhase::Allocating
        } else if elapsed < VECTOR_INDEX_ALLOCATION_MS + VECTOR_INDEX_BACKFILL_MS {
            VectorIndexPhase::Backfilling
        } else {
            VectorIndexPhase::Active
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub projection_type: String, // ALL, KEYS_ONLY, INCLUDE
    pub non_key_attributes: Vec<String>,
}

/// Write `v` to `out` in a form that depends only on the value, never on the
/// order its object keys happen to be stored in.
///
/// `serde_json::to_string` is not that form here: the server binary pulls in
/// `serde_json/preserve_order` through feature unification, which makes
/// `Value::Object` an insertion-ordered map, while `Value`'s own `PartialEq`
/// (what the key comparison uses) is order-independent. Two values that
/// compare equal must encode identically, so object keys are sorted.
fn write_canonical_json(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                if let Some(inner) = map.get(k) {
                    write_canonical_json(inner, out);
                }
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// A row's primary key as the key index stores it, which is also the table's
/// Scan order.
///
/// Two keys are equal iff DynamoDB considers them the same key. Rows order
/// first by `partition_hash`, a stable hash of the partition-key encoding,
/// then by the partition encoding itself (only to break hash ties), then by
/// sort-key value. So a partition's rows stay together, partitions come in
/// hash order and rows within one come in sort-key order -- the way DynamoDB
/// scans. The order depends only on key values, never on which rows exist, so
/// a Scan page resumes after `ExclusiveStartKey` correctly even when that row
/// has since been deleted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowKey {
    partition_hash: u64,
    partition: String,
    /// `None` when the table has no sort key.
    sort: Option<SortKeyPart>,
}

/// A sort-key value, ordered the way DynamoDB orders sort keys and equal
/// exactly when `values_equal` says the values are.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SortKeyPart {
    /// The row has no sort-key attribute (only an import could produce one).
    Missing,
    /// A valid Number, by its canonical decimal: numerically-equal spellings
    /// share one form, and numbers order by value.
    Number(String),
    /// A String, ordered by its UTF-8 bytes.
    Str(String),
    /// A Binary, ordered by its decoded bytes. The base64 text breaks ties,
    /// so equality stays exact on the text, as `values_equal` has it.
    Binary(Vec<u8>, String),
    /// Anything else (a malformed number, a non-key type), by its canonical
    /// encoding.
    Other(String),
}

impl SortKeyPart {
    fn of(v: Option<&Value>) -> Self {
        use base64::Engine;
        let Some(v) = v else {
            return SortKeyPart::Missing;
        };
        match attribute_type_and_value(v) {
            Some(("N", n)) => {
                if let Some(canon) = n
                    .as_str()
                    .and_then(crate::service::helpers::partiql::canonical_number)
                {
                    return SortKeyPart::Number(canon);
                }
            }
            Some(("S", Value::String(s))) => return SortKeyPart::Str(s.clone()),
            Some(("B", Value::String(b))) => {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b) {
                    return SortKeyPart::Binary(bytes, b.clone());
                }
            }
            _ => {}
        }
        SortKeyPart::Other(DynamoTable::encode_key_value(v))
    }

    fn rank(&self) -> u8 {
        match self {
            SortKeyPart::Missing => 0,
            SortKeyPart::Number(_) => 1,
            SortKeyPart::Str(_) => 2,
            SortKeyPart::Binary(..) => 3,
            SortKeyPart::Other(_) => 4,
        }
    }
}

impl Ord for SortKeyPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use SortKeyPart::*;
        match (self, other) {
            (Number(a), Number(b)) => {
                crate::service::helpers::partiql::compare_number_strings(a, b)
            }
            (Str(a), Str(b)) | (Other(a), Other(b)) => a.cmp(b),
            (Binary(a, at), Binary(b, bt)) => a.cmp(b).then_with(|| at.cmp(bt)),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl PartialOrd for SortKeyPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The top-level attribute a document path starts at: `#a.b[2]` -> `#a`, and
/// a double-quoted PartiQL name up to its closing quote.
fn top_level_attribute(path: &str) -> &str {
    let path = path.trim();
    if let Some(rest) = path.strip_prefix('"') {
        return match rest.find('"') {
            Some(end) => &path[..end + 2],
            None => path,
        };
    }
    let end = path.find(['.', '[']).unwrap_or(path.len());
    path[..end].trim()
}

/// FNV-1a, 64-bit. The Scan order is derived from it, so it has to be fixed:
/// std's `DefaultHasher` is explicitly allowed to change between releases.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Scan order over rows that may lack a key: keyed rows by [`RowKey`], and
/// any row missing its partition key after all of them.
fn cmp_scan_order(a: Option<&RowKey>, b: Option<&RowKey>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// What an item contributed to a table's cached stats and key index before it
/// was mutated in place. Produced by `DynamoTable::snapshot_item_at` and
/// consumed by `DynamoTable::sync_item_at`.
#[derive(Debug, Clone)]
struct ItemSlot {
    size: i64,
    key: Option<RowKey>,
}

type Item = HashMap<String, AttributeValue>;

/// Identity of a row within its table.
///
/// Assigned when the row is inserted and never changed or reused while the
/// row lives, so removing one row leaves every other row's id valid. Ids grow
/// with insertion, which makes their order the table's storage order. Storage
/// order is not Scan order; see [`RowKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ItemId(u64);

/// A table's rows, in storage order.
///
/// Rows are keyed by [`ItemId`] rather than stored by position. A position
/// is invalidated by every removal before it, so deleting from a `Vec` both
/// shifted the tail and forced the key index to repair every shifted
/// position: O(table size) per delete, and quadratic over a bulk delete
/// (#2504). With stable ids a delete touches only its own row.
///
/// Readable from anywhere, but only mutable from this module: the key index
/// records ids in here, so an insert or remove that bypassed
/// [`DynamoTable`]'s helpers would leave it pointing at the wrong rows.
/// Serialized as the bare list of rows, so snapshots are unchanged; ids are
/// reassigned in order on load.
#[derive(Debug, Clone, Default)]
pub struct TableItems {
    rows: BTreeMap<ItemId, Item>,
    next_id: u64,
}

impl TableItems {
    /// Wrap rows that are about to become a table's contents. Crate-internal:
    /// whoever does this owns re-deriving the key index and the stats, which
    /// is why [`DynamoTable::replace_items`] is the way to do it.
    pub(crate) fn new(items: Vec<Item>) -> Self {
        let mut out = Self::default();
        for item in items {
            out.push(item);
        }
        out
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The rows in storage order.
    pub fn iter(&self) -> std::collections::btree_map::Values<'_, ItemId, Item> {
        self.rows.values()
    }

    /// The rows in storage order, each with its id.
    pub fn iter_with_ids(&self) -> impl DoubleEndedIterator<Item = (ItemId, &Item)> {
        self.rows.iter().map(|(id, item)| (*id, item))
    }

    pub fn get(&self, id: ItemId) -> Option<&Item> {
        self.rows.get(&id)
    }

    /// A copy of the rows in storage order.
    pub fn to_vec(&self) -> Vec<Item> {
        self.rows.values().cloned().collect()
    }

    fn get_mut(&mut self, id: ItemId) -> Option<&mut Item> {
        self.rows.get_mut(&id)
    }

    /// Append a row after every existing one.
    fn push(&mut self, item: Item) -> ItemId {
        let id = ItemId(self.next_id);
        self.next_id += 1;
        self.rows.insert(id, item);
        id
    }

    fn remove(&mut self, id: ItemId) -> Option<Item> {
        self.rows.remove(&id)
    }
}

impl std::ops::Index<ItemId> for TableItems {
    type Output = Item;

    /// Panics if no row has this id.
    fn index(&self, id: ItemId) -> &Item {
        &self.rows[&id]
    }
}

impl<'a> IntoIterator for &'a TableItems {
    type Item = &'a HashMap<String, AttributeValue>;
    type IntoIter = std::collections::btree_map::Values<'a, ItemId, Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.values()
    }
}

impl Serialize for TableItems {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.rows.values())
    }
}

impl<'de> Deserialize<'de> for TableItems {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Vec::<Item>::deserialize(d).map(Self::new)
    }
}

/// State of a table's primary-key index.
///
/// The index is derived, not persisted, so "empty" and "not built yet" have to
/// be distinguishable, and a table holding two rows under one key cannot be
/// answered by a key -> row map at all. Naming the three states makes both
/// cases explicit -- comparing `key_index.len()` with `items.len()` told them
/// apart badly, because a table with duplicate keys is permanently shorter
/// than `items` and so would rebuild on every single write, the very cost
/// #2502 removes.
///
/// Each built state records the number of rows it was maintained against.
/// [`TableItems`] stops anything outside this module from pushing to or
/// removing from the vector, but the whole field can still be reassigned
/// (`table.items = rows.into()`), which no borrow check can catch; a row
/// count that no longer matches means exactly that happened, and the index is
/// rebuilt instead of trusted.
#[derive(Debug, Clone, Default)]
pub enum KeyIndex {
    /// Not built: restored from a snapshot, or freshly constructed with
    /// `items` assigned in bulk. Lookups scan; the next `ensure_key_index`
    /// builds it.
    #[default]
    Unbuilt,
    /// Primary key -> row id in `items`, covering every addressable item,
    /// plus the row count it was maintained against. Ordered, because its
    /// order is the table's Scan order.
    Built {
        ids: BTreeMap<RowKey, ItemId>,
        rows: usize,
    },
    /// `items` holds more than one row under the same primary key, so no
    /// key -> row map can answer lookups the way the linear scan does once the
    /// first of them is removed. Such a table is only reachable by importing
    /// an export that repeats a key (or by loading a snapshot written by a
    /// build that allowed it), and permanently falls back to the scan, which
    /// is exactly the pre-index behaviour. Recorded rather than re-derived so
    /// a degenerate table does not pay a full rebuild on every write.
    Ambiguous { rows: usize },
}

impl KeyIndex {
    /// The row count this index was built or maintained against, if any.
    fn rows(&self) -> Option<usize> {
        match self {
            KeyIndex::Unbuilt => None,
            KeyIndex::Built { rows, .. } | KeyIndex::Ambiguous { rows } => Some(*rows),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoTable {
    pub name: String,
    pub arn: String,
    pub table_id: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub provisioned_throughput: ProvisionedThroughput,
    pub(crate) items: TableItems,
    /// Primary-key -> row id in `items`, so a write does not have to scan
    /// the whole table to decide insert-vs-overwrite. Without it every write
    /// was O(table size) and a bulk load was quadratic (#2502).
    ///
    /// Not persisted: it is derived state, and rebuilding it on load keeps
    /// existing snapshots readable. `items` stays the source of truth --
    /// scans, pagination and the stream paths all still index into it -- and
    /// is only mutable through the helpers here (`put_item_at_key`,
    /// `remove_item_by_key`, `update_item_at`, `replace_items`, ...), which
    /// keep the two in step. `TableItems` enforces that: the vector is
    /// private, and out-of-crate callers read the rows through
    /// [`DynamoTable::items`] and build tables through [`DynamoTable::new`].
    #[serde(skip)]
    pub(crate) key_index: KeyIndex,
    pub gsi: Vec<GlobalSecondaryIndex>,
    pub lsi: Vec<LocalSecondaryIndex>,
    pub tags: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub status: String,
    pub item_count: i64,
    pub size_bytes: i64,
    pub billing_mode: String, // PROVISIONED or PAY_PER_REQUEST
    pub ttl_attribute: Option<String>,
    pub ttl_enabled: bool,
    pub resource_policy: Option<String>,
    /// PITR enabled
    pub pitr_enabled: bool,
    /// Kinesis streaming destinations: stream_arn -> status
    pub kinesis_destinations: Vec<KinesisDestination>,
    /// Contributor insights status
    pub contributor_insights_status: String,
    /// Contributor insights: partition key access counters (key_value_string -> count)
    pub contributor_insights_counters: BTreeMap<String, u64>,
    /// DynamoDB Streams configuration
    pub stream_enabled: bool,
    pub stream_view_type: Option<String>, // KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES
    pub stream_arn: Option<String>,
    /// Stream records (retained for 24 hours). Not persisted: stream
    /// records are ephemeral and would be garbage anyway across restarts.
    #[serde(with = "stream_records_serde", default = "empty_stream_records")]
    pub stream_records: Arc<RwLock<Vec<StreamRecord>>>,
    /// Server-side encryption type: AES256 (owned) or KMS
    pub sse_type: Option<String>,
    /// KMS key ARN for SSE (only when sse_type is KMS)
    pub sse_kms_key_arn: Option<String>,
    /// Deletion protection: when true, DeleteTable is rejected with
    /// `ResourceInUseException`. Defaults to false. Returned on every
    /// `DescribeTable` and toggleable via `UpdateTable`.
    pub deletion_protection_enabled: bool,
    /// Table-level on-demand throughput caps. Only meaningful for
    /// PAY_PER_REQUEST tables, but real AWS echoes the field on every
    /// DescribeTable once set.
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Storage class: STANDARD (default) or STANDARD_INFREQUENT_ACCESS.
    /// Returned inside `TableClassSummary` on DescribeTable; set at
    /// CreateTable and changed via UpdateTable.
    #[serde(default = "default_table_class")]
    pub table_class: String,
    /// Vector indexes for similarity search, keyed by index name.
    #[serde(default)]
    pub vector_indexes: Vec<VectorIndex>,
}

pub(crate) fn default_table_class() -> String {
    "STANDARD".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub event_id: String,
    pub event_name: String, // INSERT, MODIFY, REMOVE
    pub event_version: String,
    pub event_source: String,
    pub aws_region: String,
    pub dynamodb: DynamoDbStreamRecord,
    pub event_source_arn: String,
    pub timestamp: DateTime<Utc>,
    /// Set only for system-generated changes. TTL deletions carry
    /// `{principalId: "dynamodb.amazonaws.com", type: "Service"}` so consumers
    /// can distinguish an expiry REMOVE from a user-driven DeleteItem. Absent
    /// (and omitted from the wire) for ordinary writes.
    #[serde(default)]
    pub user_identity: Option<StreamUserIdentity>,
}

/// `userIdentity` block on a stream record. Present only for system-generated
/// events such as TTL expirations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamUserIdentity {
    pub principal_id: String,
    pub identity_type: String,
}

impl StreamUserIdentity {
    /// The marker AWS attaches to a TTL-expiry REMOVE record.
    pub fn ttl() -> Self {
        Self {
            principal_id: "dynamodb.amazonaws.com".to_string(),
            identity_type: "Service".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbStreamRecord {
    pub keys: HashMap<String, AttributeValue>,
    pub new_image: Option<HashMap<String, AttributeValue>>,
    pub old_image: Option<HashMap<String, AttributeValue>>,
    pub sequence_number: String,
    pub size_bytes: i64,
    pub stream_view_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisDestination {
    pub stream_arn: String,
    pub destination_status: String,
    pub approximate_creation_date_time_precision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupDescription {
    pub backup_arn: String,
    pub backup_name: String,
    pub table_name: String,
    pub table_arn: String,
    pub backup_status: String,
    pub backup_type: String,
    pub backup_creation_date: DateTime<Utc>,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub provisioned_throughput: ProvisionedThroughput,
    pub billing_mode: String,
    pub item_count: i64,
    pub size_bytes: i64,
    /// Snapshot of the table items at backup creation time.
    pub items: Vec<HashMap<String, AttributeValue>>,
    /// Real DDB persists GSI/LSI/tags/TTL/SSE/Stream into the backup
    /// payload so RestoreTableFromBackup brings the full table back
    /// up. Older snapshots may not have these fields, so all default
    /// to empty/false via serde.
    #[serde(default)]
    pub gsi: Vec<GlobalSecondaryIndex>,
    #[serde(default)]
    pub lsi: Vec<LocalSecondaryIndex>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub ttl_attribute: Option<String>,
    #[serde(default)]
    pub ttl_enabled: bool,
    #[serde(default)]
    pub sse_type: Option<String>,
    #[serde(default)]
    pub sse_kms_key_arn: Option<String>,
    #[serde(default)]
    pub stream_enabled: bool,
    #[serde(default)]
    pub stream_view_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTableDescription {
    pub global_table_name: String,
    pub global_table_arn: String,
    pub global_table_status: String,
    pub creation_date: DateTime<Utc>,
    pub replication_group: Vec<ReplicaDescription>,
    /// Billing mode applied across all replicas via
    /// `UpdateGlobalTableSettings` (`PROVISIONED` / `PAY_PER_REQUEST`).
    /// Defaults to PROVISIONED to match real DynamoDB global-table v1.
    #[serde(default = "default_global_billing_mode")]
    pub billing_mode: String,
    /// Global provisioned write capacity applied across all replicas via
    /// `UpdateGlobalTableSettings`. `None` under PAY_PER_REQUEST.
    #[serde(default)]
    pub provisioned_write_capacity_units: Option<i64>,
}

fn default_global_billing_mode() -> String {
    "PROVISIONED".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDescription {
    pub region_name: String,
    pub replica_status: String,
    /// Per-replica provisioned-read-capacity autoscaling settings, as supplied
    /// via `UpdateTableReplicaAutoScaling`. Round-tripped through
    /// `DescribeTableReplicaAutoScaling` as `AutoScalingSettingsDescription`.
    #[serde(default)]
    pub read_capacity_auto_scaling: Option<serde_json::Value>,
    /// Per-replica provisioned-write-capacity autoscaling settings.
    #[serde(default)]
    pub write_capacity_auto_scaling: Option<serde_json::Value>,
    /// Per-replica provisioned read capacity supplied via
    /// `UpdateGlobalTableSettings` ReplicaSettingsUpdate.
    #[serde(default)]
    pub read_capacity_units: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportDescription {
    pub export_arn: String,
    /// `IN_PROGRESS` while the background export runs, then `COMPLETED` or
    /// `FAILED`.
    pub export_status: String,
    pub table_arn: String,
    pub s3_bucket: String,
    pub s3_prefix: Option<String>,
    pub export_format: String,
    pub start_time: DateTime<Utc>,
    /// Set once the export has finished (successfully or not).
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    pub export_time: DateTime<Utc>,
    pub item_count: i64,
    pub billed_size_bytes: i64,
    /// Set when the export failed, reported by DescribeExport.
    #[serde(default)]
    pub failure_code: Option<String>,
    #[serde(default)]
    pub failure_message: Option<String>,
    /// S3 key of the export's `manifest-summary.json`, once written.
    #[serde(default)]
    pub export_manifest: Option<String>,
    #[serde(default)]
    pub table_id: Option<String>,
    #[serde(default)]
    pub s3_bucket_owner: Option<String>,
    #[serde(default)]
    pub s3_sse_algorithm: Option<String>,
    #[serde(default)]
    pub s3_sse_kms_key_id: Option<String>,
    #[serde(default)]
    pub client_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportDescription {
    pub import_arn: String,
    /// `IN_PROGRESS` while the background import runs, then `COMPLETED` or
    /// `FAILED`.
    pub import_status: String,
    pub table_arn: String,
    pub table_name: String,
    pub s3_bucket_source: String,
    pub input_format: String,
    pub start_time: DateTime<Utc>,
    /// Set once the import has finished (successfully or not).
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    pub processed_item_count: i64,
    pub processed_size_bytes: i64,
    /// Rows written to the table: processed rows less the invalid ones, with
    /// rows sharing a primary key counted once.
    #[serde(default)]
    pub imported_item_count: i64,
    /// Rows skipped because they could not form a valid item.
    #[serde(default)]
    pub error_count: i64,
    #[serde(default)]
    pub table_id: Option<String>,
    #[serde(default)]
    pub s3_key_prefix: Option<String>,
    #[serde(default)]
    pub s3_bucket_owner: Option<String>,
    #[serde(default)]
    pub input_compression_type: Option<String>,
    /// `InputFormatOptions` exactly as requested (CSV delimiter / header).
    #[serde(default)]
    pub input_format_options: Option<serde_json::Value>,
    /// `TableCreationParameters` exactly as requested.
    #[serde(default)]
    pub table_creation_parameters: Option<serde_json::Value>,
    #[serde(default)]
    pub client_token: Option<String>,
    #[serde(default)]
    pub failure_code: Option<String>,
    #[serde(default)]
    pub failure_message: Option<String>,
}

impl DynamoTable {
    /// A new empty table. The identity and the schema have to be given; every
    /// other field starts at its documented default and is a public field the
    /// caller can set afterwards. Out-of-crate callers (the CloudFormation
    /// provisioner) build tables this way, because `items` and the key index
    /// they have to stay in step with are not theirs to assign.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        arn: String,
        table_id: String,
        key_schema: Vec<KeySchemaElement>,
        attribute_definitions: Vec<AttributeDefinition>,
        provisioned_throughput: ProvisionedThroughput,
        billing_mode: String,
        created_at: DateTime<Utc>,
    ) -> Self {
        DynamoTable {
            name,
            arn,
            table_id,
            key_schema,
            attribute_definitions,
            provisioned_throughput,
            items: TableItems::default(),
            key_index: KeyIndex::default(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: BTreeMap::new(),
            created_at,
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
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: empty_stream_records(),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: "STANDARD".to_string(),
            vector_indexes: Vec::new(),
        }
    }

    /// The table's rows, in storage order. Read-only: the key index records
    /// row ids in here, so mutating it is the business of the helpers below.
    pub fn items(&self) -> &TableItems {
        &self.items
    }

    /// The first primary-key attribute an UpdateExpression writes, if any.
    ///
    /// DynamoDB refuses any SET, REMOVE, ADD or DELETE that targets a key
    /// attribute, whatever the value: a row's key is its identity, not data.
    /// Accepting one would move the row to another key -- onto a row already
    /// there, or off any key at all -- leaving rows no key lookup can address
    /// and no Scan cursor can resume after.
    pub fn key_attribute_in_update_expression(
        &self,
        expr: &str,
        expr_attr_names: &HashMap<String, String>,
    ) -> Option<String> {
        use crate::service::helpers::{parse_update_clauses, resolve_attr_name, UpdateAction};
        let key_attrs: Vec<&str> = std::iter::once(self.hash_key_name())
            .chain(self.range_key_name())
            .collect();
        for (action, assignments) in parse_update_clauses(expr) {
            for assignment in &assignments {
                let target = match action {
                    UpdateAction::Set => match assignment.split_once('=') {
                        Some((left, _)) => left,
                        None => continue,
                    },
                    UpdateAction::Remove => assignment.as_str(),
                    UpdateAction::Add | UpdateAction::Delete => {
                        assignment.split_whitespace().next().unwrap_or_default()
                    }
                };
                let attr = resolve_attr_name(top_level_attribute(target), expr_attr_names);
                if key_attrs.contains(&attr.as_str()) {
                    return Some(attr);
                }
            }
        }
        None
    }

    /// DynamoDB's message for an update that writes key attribute `attr`.
    pub fn key_attribute_update_message(attr: &str) -> String {
        format!(
            "One or more parameter values were invalid: Cannot update attribute {attr}. \
             This attribute is part of the key"
        )
    }

    /// Get the hash key attribute name from the key schema.
    pub fn hash_key_name(&self) -> &str {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "HASH")
            .map(|k| k.attribute_name.as_str())
            .unwrap_or("")
    }

    /// Get the range key attribute name from the key schema (if any).
    pub fn range_key_name(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "RANGE")
            .map(|k| k.attribute_name.as_str())
    }

    /// Canonical string form of one key attribute, used to build the
    /// `key_index` lookup string.
    ///
    /// This must induce exactly the same equivalence classes as
    /// `values_equal`, which the linear scan used before: two attribute
    /// values are equal iff their encodings are equal. Numbers are the only
    /// type with a non-byte-exact equality — `{"N":"1"}` and `{"N":"1.0"}`
    /// are the same DynamoDB number — so a *valid* number is encoded by its
    /// canonical decimal form. A malformed number (`{"N":"abc"}`) falls back
    /// to its exact byte form, matching `values_equal`'s deliberate strictness
    /// there: a bad operand must never collide with a valid stored key
    /// (Cubic P1, 2026-07-01).
    fn encode_key_value(v: &Value) -> String {
        use crate::service::helpers::partiql::canonical_number;
        if let Some(("N", n)) = attribute_type_and_value(v) {
            if let Some(canon) = n.as_str().and_then(canonical_number) {
                return format!("N:{canon}");
            }
        }
        // The type tag is part of the encoding, so `{"S":"1"}` cannot collide
        // with `{"N":"1"}`.
        let mut out = String::from("X:");
        write_canonical_json(v, &mut out);
        out
    }

    /// Canonical form of an item's full primary key, or `None` if the item is
    /// missing the hash key — such an item is not addressable by key and is
    /// left out of the index, mirroring the old scan, which required
    /// `item.get(hash_key).is_some()`.
    fn encode_key(&self, item: &HashMap<String, AttributeValue>) -> Option<RowKey> {
        self.encode_key_with(|name| item.get(name))
    }

    /// [`Self::encode_key`] over any attribute lookup, so a row already
    /// rendered as a JSON object can be placed in Scan order too.
    pub(crate) fn encode_key_with<'a>(
        &self,
        get: impl Fn(&str) -> Option<&'a AttributeValue>,
    ) -> Option<RowKey> {
        let partition = Self::encode_key_value(get(self.hash_key_name())?);
        let partition_hash = fnv1a_64(partition.as_bytes());
        // The sort key is part of the identity when the schema declares one;
        // `values_equal(None, None)` was true in the scan, so an item with no
        // sort-key attribute is only equal to another item that also lacks it.
        let sort = self.range_key_name().map(|rk| SortKeyPart::of(get(rk)));
        Some(RowKey {
            partition_hash,
            partition,
            sort,
        })
    }

    /// The rows in Scan order, starting just after the primary key `start`
    /// (all of them when `start` is `None`). `start` need not name a row that
    /// still exists: the order is a function of key values alone, so a page
    /// resumes in the right place after its `ExclusiveStartKey` row was
    /// deleted. A `start` without the partition key selects nothing.
    ///
    /// Walks the key index from `start` when it covers every row -- the page
    /// then costs O(log n) plus the rows it visits, rather than a pass over
    /// the whole table. Otherwise (index not built yet, duplicate keys, or a
    /// row with no key) it sorts the rows itself, into the same order.
    pub(crate) fn scan_rows_after<'a>(
        &'a self,
        start: Option<&HashMap<String, AttributeValue>>,
    ) -> Box<dyn Iterator<Item = &'a HashMap<String, AttributeValue>> + 'a> {
        use std::ops::Bound;
        let start = match start {
            Some(key) => match self.encode_key(key) {
                Some(k) => Some(k),
                None => return Box::new(std::iter::empty()),
            },
            None => None,
        };
        if let KeyIndex::Built { ids, rows } = &self.key_index {
            if *rows == self.items.len() && ids.len() == self.items.len() {
                let lower = match &start {
                    Some(k) => Bound::Excluded(k),
                    None => Bound::Unbounded,
                };
                return Box::new(
                    ids.range::<RowKey, _>((lower, Bound::Unbounded))
                        .filter_map(|(_, id)| self.items.get(*id)),
                );
            }
        }
        let mut rows: Vec<(Option<RowKey>, &HashMap<String, AttributeValue>)> = self
            .items
            .iter()
            .map(|item| (self.encode_key(item), item))
            .collect();
        // Stable, so rows under one key (only a duplicate-key import has them)
        // keep their storage order.
        rows.sort_by(|a, b| cmp_scan_order(a.0.as_ref(), b.0.as_ref()));
        Box::new(
            rows.into_iter()
                .filter(move |(key, _)| match (&start, key) {
                    (Some(start), Some(key)) => key > start,
                    _ => true,
                })
                .map(|(_, item)| item),
        )
    }

    /// Rebuild `key_index` from `items`. Called after loading a snapshot (the
    /// index is not persisted) and after any bulk rewrite of `items`.
    pub fn rebuild_key_index(&mut self) {
        let mut index = BTreeMap::new();
        let mut duplicate_key = false;
        // Well-formed tables have no duplicate keys; an imported export or an
        // older snapshot might, and those tables give up the index entirely
        // rather than answer a lookup differently from the scan (which returns
        // the *first* matching row, a key -> id map cannot keep doing that once
        // the first is removed).
        for (id, item) in self.items.iter_with_ids() {
            if let Some(k) = self.encode_key(item) {
                match index.entry(k) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(id);
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {
                        duplicate_key = true;
                        break;
                    }
                }
            }
        }
        let rows = self.items.len();
        self.key_index = if duplicate_key {
            KeyIndex::Ambiguous { rows }
        } else {
            KeyIndex::Built { ids: index, rows }
        };
    }

    /// Ensure `key_index` is usable, building it if this table came from a
    /// snapshot (where the index is not persisted) or from a bulk assignment to
    /// `items`. Repairing lazily here means no load path can forget to do it
    /// and silently turn every lookup into a miss — which would make writes
    /// duplicate rows instead of overwriting them.
    pub fn ensure_key_index(&mut self) {
        if self.key_index.rows() != Some(self.items.len()) {
            self.rebuild_key_index();
        }
    }

    /// Forget that `item` lives at row `id`. An entry for the same key that
    /// records a different row is left alone: it is not this row's to drop.
    fn index_remove(&mut self, item: &HashMap<String, AttributeValue>, id: ItemId) {
        let Some(k) = self.encode_key(item) else {
            return;
        };
        if let KeyIndex::Built { ids, .. } = &mut self.key_index {
            if ids.get(&k) == Some(&id) {
                ids.remove(&k);
            }
        }
    }

    /// Find an item's row id by its primary key. O(log n) once the index is
    /// built: a hash lookup for the id, then the row itself to confirm it.
    ///
    /// Takes `&self`, so it cannot build the index; it falls back to the
    /// original linear scan in that case rather than returning a wrong answer.
    /// Every mutating path goes through the `&mut self` helpers below, which
    /// call `ensure_key_index` first, so the fallback is a correctness
    /// backstop rather than the normal path.
    pub fn find_item_index(&self, key: &HashMap<String, AttributeValue>) -> Option<ItemId> {
        let KeyIndex::Built { ids, rows } = &self.key_index else {
            return self.find_item_index_scan(key);
        };
        if *rows != self.items.len() {
            // `items` was reassigned wholesale behind the index's back; the
            // recorded ids describe rows that are no longer there.
            return self.find_item_index_scan(key);
        }
        // A key that cannot be encoded is missing the hash key, so it matches
        // nothing -- the same answer the scan gave.
        let encoded = self.encode_key(key)?;
        let id = ids.get(&encoded).copied()?;
        // Costs one key encoding, so it stays cheap and keeps an id that
        // drifted from ever addressing the wrong row: answer from the rows
        // themselves when the recorded one does not carry this key.
        match self.items.get(id) {
            Some(item) if self.encode_key(item).as_ref() == Some(&encoded) => Some(id),
            _ => self.find_item_index_scan(key),
        }
    }

    /// The pre-index linear scan. Retained as the fallback for a stale index
    /// and as the oracle the index is tested against.
    fn find_item_index_scan(&self, key: &HashMap<String, AttributeValue>) -> Option<ItemId> {
        let hash_key = self.hash_key_name();
        let range_key = self.range_key_name();

        // Compare keys with numeric-aware equality: a Number key stored as
        // `{"N":"1"}` must match a lookup of `{"N":"1.0"}` (they are the same
        // DynamoDB number). Raw JSON `==` treated them as distinct, so the
        // write/lookup path could miss an existing item or store a duplicate
        // even though the compare/filter path was already canonical
        // (bug-hunt 2026-07-01, DynamoDB write-path number-canon).
        use crate::service::helpers::partiql::values_equal;
        self.items
            .iter_with_ids()
            .find(|(_, item)| {
                let hash_match = values_equal(item.get(hash_key), key.get(hash_key))
                    && item.get(hash_key).is_some();
                if !hash_match {
                    return false;
                }
                match range_key {
                    Some(rk) => values_equal(item.get(rk), key.get(rk)),
                    None => true,
                }
            })
            .map(|(id, _)| id)
    }

    /// Insert or overwrite the item with this key, keeping `key_index` and the
    /// cached stats in step. Returns the row id it now occupies, and whether
    /// this replaced an existing item. An overwrite keeps the row's place in
    /// storage order; an insert goes after every existing row.
    pub fn put_item_at_key(&mut self, item: HashMap<String, AttributeValue>) -> (ItemId, bool) {
        self.ensure_key_index();
        match self.find_item_index(&item) {
            Some(id) => {
                // Adjust the cached size by the delta rather than resumming the
                // whole table (#2502).
                self.size_bytes -= Self::estimate_item_size(&self.items[id]);
                self.size_bytes += Self::estimate_item_size(&item);
                *self
                    .items
                    .get_mut(id)
                    .expect("row located by find_item_index") = item;
                (id, true)
            }
            None => {
                self.size_bytes += Self::estimate_item_size(&item);
                self.item_count += 1;
                let key = self.encode_key(&item);
                let id = self.items.push(item);
                if let (Some(k), KeyIndex::Built { ids, .. }) = (key, &mut self.key_index) {
                    ids.insert(k, id);
                }
                match &mut self.key_index {
                    KeyIndex::Built { rows, .. } | KeyIndex::Ambiguous { rows } => *rows += 1,
                    KeyIndex::Unbuilt => {}
                }
                (id, false)
            }
        }
    }

    /// Remove the item with this key, keeping `key_index` and the cached stats
    /// in step. Returns the removed item, if there was one.
    pub fn remove_item_by_key(
        &mut self,
        key: &HashMap<String, AttributeValue>,
    ) -> Option<HashMap<String, AttributeValue>> {
        self.ensure_key_index();
        let id = self.find_item_index(key)?;
        Some(self.remove_item_at(id))
    }

    /// Remove the row `id`, keeping `key_index` and the cached stats in step.
    /// For callers that already located the row by id (a PartiQL `WHERE`
    /// sweep, say) rather than by key.
    ///
    /// Touches only this row: every other row keeps its id, so ids collected
    /// before a sweep of removals all stay valid (#2504).
    ///
    /// Panics if no row has this id.
    pub fn remove_item_at(&mut self, id: ItemId) -> HashMap<String, AttributeValue> {
        let removed = self.items.remove(id).expect("remove_item_at: no such row");
        self.index_remove(&removed, id);
        match &mut self.key_index {
            KeyIndex::Built { rows, .. } | KeyIndex::Ambiguous { rows } => *rows -= 1,
            KeyIndex::Unbuilt => {}
        }
        self.size_bytes -= Self::estimate_item_size(&removed);
        self.item_count -= 1;
        removed
    }

    /// Mutate the row `id` in place, keeping the cached size and the key
    /// index in step.
    ///
    /// All or nothing: an UpdateExpression is applied clause by clause, so
    /// `f` can fail with some clauses already written. The item is then put
    /// back exactly as it was, so a rejected update leaves no trace -- as a
    /// rejected UpdateItem does on AWS. That costs a copy of the row, so a
    /// mutation that cannot fail should use [`Self::mutate_item_at`].
    ///
    /// Panics if no row has this id.
    pub fn update_item_at<E>(
        &mut self,
        id: ItemId,
        f: impl FnOnce(&mut HashMap<String, AttributeValue>) -> Result<(), E>,
    ) -> Result<(), E> {
        let before = self.snapshot_item_at(id);
        let row = self.items.get_mut(id).expect("update_item_at: no such row");
        let original = row.clone();
        match f(row) {
            Ok(()) => {
                self.sync_item_at(id, before);
                Ok(())
            }
            Err(err) => {
                // Restoring the original leaves the size and the indexed key
                // exactly as `before` recorded them, so there is nothing to
                // settle.
                *row = original;
                Err(err)
            }
        }
    }

    /// Mutate the row `id` in place with a mutation that cannot fail,
    /// keeping the cached size and the key index in step. Same as
    /// [`Self::update_item_at`] without the copy taken for the rollback.
    ///
    /// Panics if no row has this id.
    pub fn mutate_item_at(
        &mut self,
        id: ItemId,
        f: impl FnOnce(&mut HashMap<String, AttributeValue>),
    ) {
        let before = self.snapshot_item_at(id);
        f(self.items.get_mut(id).expect("mutate_item_at: no such row"));
        self.sync_item_at(id, before);
    }

    /// Replace every row at once, then re-derive the stats and the key index
    /// from the new rows. For the bulk paths (an import, a transaction
    /// revert), where a full pass is proportional to work already done.
    pub fn replace_items(&mut self, items: Vec<HashMap<String, AttributeValue>>) {
        self.items = TableItems::new(items);
        self.recalculate_stats();
    }

    /// Remove every row `remove` selects, preserving the order of the rest,
    /// and return the removed rows in their storage order. One pass to select,
    /// then each removal touches only its own row, so the survivors keep their
    /// ids and the index and stats are settled incrementally.
    pub fn remove_items_where(
        &mut self,
        mut remove: impl FnMut(&HashMap<String, AttributeValue>) -> bool,
    ) -> Vec<HashMap<String, AttributeValue>> {
        let doomed: Vec<ItemId> = self
            .items
            .iter_with_ids()
            .filter(|(_, item)| remove(item))
            .map(|(id, _)| id)
            .collect();
        let removed: Vec<_> = doomed
            .into_iter()
            .map(|id| self.remove_item_at(id))
            .collect();
        // A table that fell back to the scan over duplicate keys may have just
        // lost the duplicates. The sweep already paid a full pass; one more,
        // to see whether the index can be trusted again, keeps it O(n).
        if !removed.is_empty() && matches!(self.key_index, KeyIndex::Ambiguous { .. }) {
            self.rebuild_key_index();
        }
        removed
    }

    /// What the row `id` contributed before an in-place mutation: its size,
    /// and the key it was indexed under. Pair with [`Self::sync_item_at`]
    /// around the mutation.
    fn snapshot_item_at(&self, id: ItemId) -> ItemSlot {
        match self.items.get(id) {
            Some(item) => ItemSlot {
                size: Self::estimate_item_size(item),
                key: self.encode_key(item),
            },
            None => ItemSlot { size: 0, key: None },
        }
    }

    /// Settle the cached size and the key index after the row `id` was
    /// mutated in place.
    ///
    /// Every update path rejects writing a primary-key attribute, as DynamoDB
    /// does (see [`Self::key_attribute_in_update_expression`]), but a mutation
    /// handed to [`Self::update_item_at`] / [`Self::mutate_item_at`] can still
    /// move a row to a different key. Re-pointing the index here keeps it in step with `items`, matching
    /// what the linear scan would have answered; without it a later write
    /// would overwrite or delete the wrong row.
    fn sync_item_at(&mut self, id: ItemId, before: ItemSlot) {
        let Some((size_after, key_after)) = self
            .items
            .get(id)
            .map(|item| (Self::estimate_item_size(item), self.encode_key(item)))
        else {
            return;
        };
        self.size_bytes += size_after - before.size;
        if key_after == before.key {
            return;
        }
        if let KeyIndex::Built { ids, rows } = &mut self.key_index {
            let rows = *rows;
            if let Some(old) = &before.key {
                ids.remove(old);
            }
            // A rewritten key that lands on another row leaves two rows under
            // one key, which no key -> id map can resolve the way the scan
            // does; fall back to the scan for this table.
            if let Some(new_key) = key_after {
                if ids.insert(new_key, id).is_some() {
                    self.key_index = KeyIndex::Ambiguous { rows };
                }
            }
        }
    }

    /// An item's size in bytes, as DynamoDB measures it for TableSizeBytes,
    /// the 400KB limit and consumed capacity.
    pub(crate) fn estimate_item_size(item: &HashMap<String, AttributeValue>) -> i64 {
        crate::service::helpers::item_size(item) as i64
    }

    #[cfg(test)]
    fn estimate_value_size(v: &Value) -> i64 {
        crate::service::helpers::attribute_value_size(v) as i64
    }

    /// Record a partition key access for contributor insights.
    /// Only records if contributor insights is enabled.
    pub fn record_key_access(&mut self, key: &HashMap<String, AttributeValue>) {
        if self.contributor_insights_status != "ENABLED" {
            return;
        }
        let hash_key = self.hash_key_name().to_string();
        if let Some(pk_value) = key.get(&hash_key) {
            let key_str = pk_value.to_string();
            *self
                .contributor_insights_counters
                .entry(key_str)
                .or_insert(0) += 1;
        }
    }

    /// Record a partition key access from a full item (extracts the key first).
    pub fn record_item_access(&mut self, item: &HashMap<String, AttributeValue>) {
        if self.contributor_insights_status != "ENABLED" {
            return;
        }
        let hash_key = self.hash_key_name().to_string();
        if let Some(pk_value) = item.get(&hash_key) {
            let key_str = pk_value.to_string();
            *self
                .contributor_insights_counters
                .entry(key_str)
                .or_insert(0) += 1;
        }
    }

    /// Get top N contributors sorted by access count (descending).
    pub fn top_contributors(&self, n: usize) -> Vec<(&str, u64)> {
        let mut entries: Vec<(&str, u64)> = self
            .contributor_insights_counters
            .iter()
            .map(|(k, &v)| (k.as_str(), v))
            .collect();
        entries.sort_by_key(|e| std::cmp::Reverse(e.1));
        entries.truncate(n);
        entries
    }

    /// Recalculate item_count and size_bytes from the items vec, and rebuild
    /// the key index.
    ///
    /// This is O(table size). The single-item write paths keep both the stats
    /// and the index up to date incrementally instead (#2502) — reserve this
    /// for bulk paths that rewrite `items` wholesale (import, restore, TTL
    /// sweep), where the full pass is proportional to the work already done.
    pub fn recalculate_stats(&mut self) {
        self.item_count = self.items.len() as i64;
        self.size_bytes = self.items.iter().map(Self::estimate_item_size).sum::<i64>();
        self.rebuild_key_index();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbState {
    pub account_id: String,
    pub region: String,
    pub tables: BTreeMap<String, DynamoTable>,
    pub backups: BTreeMap<String, BackupDescription>,
    pub global_tables: BTreeMap<String, GlobalTableDescription>,
    pub exports: BTreeMap<String, ExportDescription>,
    pub imports: BTreeMap<String, ImportDescription>,
    /// DynamoDB Streams -> Lambda event-source-mapping checkpoints: the
    /// last stream sequence number delivered for each mapping
    /// (keyed by ESM uuid). Persisted so the streams poller resumes from
    /// where it left off after a restart instead of re-seeding TRIM_HORIZON
    /// and re-invoking the target Lambda with the whole retained backlog
    /// (duplicate side effects). Mirrors `KinesisState.lambda_checkpoints`.
    /// `#[serde(default)]` keeps older snapshots loadable.
    #[serde(default)]
    pub lambda_stream_checkpoints: BTreeMap<String, String>,
    /// Resource-based policies attached to DynamoDB streams, keyed by stream
    /// ARN. A stream's policy is its own -- separate from its table's -- and
    /// belongs to that stream: re-enabling a table's stream mints a new ARN
    /// with no policy.
    #[serde(default)]
    pub stream_policies: BTreeMap<String, String>,
}

/// On-disk snapshot envelope. The payload is the full [`DynamoDbState`];
/// `schema_version` lets us evolve the format without accidentally loading
/// an incompatible dump on upgrade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbSnapshot {
    pub schema_version: u32,
    /// v2+: multi-account state.
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<DynamoDbState>>,
    /// v1 compat: single-account state.
    #[serde(default)]
    pub state: Option<DynamoDbState>,
}

pub const DYNAMODB_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

impl DynamoDbState {
    /// Rebuild what a snapshot does not carry, or carries from an older
    /// build. Call once after loading a snapshot.
    ///
    /// The key index is not persisted, so a restored table has none, and until
    /// something writes to it each lookup is a linear scan and each Scan page
    /// sorts the whole table. `item_count` and `size_bytes` are persisted but
    /// maintained incrementally, so a snapshot written by a build that sized
    /// items differently would otherwise leave `TableSizeBytes` drifting (or
    /// going negative as rows sized under the new rules are removed). Both are
    /// recomputed from the rows here.
    pub fn rebuild_derived_state(&mut self) {
        for table in self.tables.values_mut() {
            table.recalculate_stats();
        }
    }

    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            tables: BTreeMap::new(),
            backups: BTreeMap::new(),
            global_tables: BTreeMap::new(),
            exports: BTreeMap::new(),
            imports: BTreeMap::new(),
            lambda_stream_checkpoints: BTreeMap::new(),
            stream_policies: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.tables.clear();
        self.backups.clear();
        self.global_tables.clear();
        self.exports.clear();
        self.imports.clear();
        self.lambda_stream_checkpoints.clear();
        self.stream_policies.clear();
    }

    /// Last stream sequence number delivered for the given DynamoDB
    /// Streams -> Lambda event source mapping, or `None` if the mapping
    /// has never delivered (so the poller seeds from StartingPosition).
    pub fn lambda_stream_checkpoint(&self, mapping_uuid: &str) -> Option<String> {
        self.lambda_stream_checkpoints.get(mapping_uuid).cloned()
    }

    /// Record the last stream sequence number delivered for a mapping. The
    /// value rides along with the next DynamoDB snapshot save, exactly the
    /// way Kinesis lambda checkpoints persist through their snapshot.
    pub fn set_lambda_stream_checkpoint(&mut self, mapping_uuid: &str, sequence_number: String) {
        self.lambda_stream_checkpoints
            .insert(mapping_uuid.to_string(), sequence_number);
    }
}

impl fakecloud_core::multi_account::AccountState for DynamoDbState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub type SharedDynamoDbState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<DynamoDbState>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn attribute_type_and_value_valid() {
        let v = json!({"S": "hi"});
        let (ty, val) = attribute_type_and_value(&v).unwrap();
        assert_eq!(ty, "S");
        assert_eq!(val, &json!("hi"));
    }

    #[test]
    fn attribute_type_and_value_empty_returns_none() {
        let v = json!({});
        assert!(attribute_type_and_value(&v).is_none());
    }

    #[test]
    fn attribute_type_and_value_multiple_entries_returns_none() {
        let v = json!({"S": "hi", "N": "1"});
        assert!(attribute_type_and_value(&v).is_none());
    }

    #[test]
    fn attribute_type_and_value_non_object_returns_none() {
        let v = json!("not-object");
        assert!(attribute_type_and_value(&v).is_none());
    }

    #[test]
    fn account_state_trait_impl() {
        use fakecloud_core::multi_account::AccountState;
        let state = DynamoDbState::new_for_account("123", "us-east-1", "");
        assert_eq!(state.account_id, "123");
        assert_eq!(state.region, "us-east-1");
    }

    #[test]
    fn new_and_reset() {
        let state = DynamoDbState::new("123", "us-east-1");
        assert!(state.tables.is_empty());
    }

    fn table_with_hash_key(hash: &str) -> DynamoTable {
        DynamoTable {
            name: "t".to_string(),
            arn: "arn:aws:dynamodb:us-east-1:123:table/t".to_string(),
            table_id: "id".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: hash.to_string(),
                key_type: "HASH".to_string(),
            }],
            attribute_definitions: vec![],
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 1,
                write_capacity_units: 1,
            },
            items: Default::default(),
            key_index: Default::default(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PROVISIONED".to_string(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: empty_stream_records(),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: default_table_class(),
            vector_indexes: Vec::new(),
        }
    }

    #[test]
    fn hash_key_name_extracts_from_schema() {
        let t = table_with_hash_key("pk");
        assert_eq!(t.hash_key_name(), "pk");
    }

    #[test]
    fn hash_key_name_empty_when_no_hash_schema() {
        let mut t = table_with_hash_key("pk");
        t.key_schema.clear();
        assert_eq!(t.hash_key_name(), "");
    }

    #[test]
    fn record_key_access_noop_when_disabled() {
        let mut t = table_with_hash_key("pk");
        let mut key = HashMap::new();
        key.insert("pk".to_string(), json!({"S": "a"}));
        t.record_key_access(&key);
        assert!(t.contributor_insights_counters.is_empty());
    }

    #[test]
    fn record_key_access_increments_when_enabled() {
        let mut t = table_with_hash_key("pk");
        t.contributor_insights_status = "ENABLED".to_string();
        let mut key = HashMap::new();
        key.insert("pk".to_string(), json!({"S": "a"}));
        t.record_key_access(&key);
        t.record_key_access(&key);
        assert_eq!(t.contributor_insights_counters.values().sum::<u64>(), 2);
    }

    #[test]
    fn record_item_access_uses_hash_key_from_item() {
        let mut t = table_with_hash_key("pk");
        t.contributor_insights_status = "ENABLED".to_string();
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"S": "user-1"}));
        item.insert("other".to_string(), json!({"N": "42"}));
        t.record_item_access(&item);
        assert_eq!(t.contributor_insights_counters.values().sum::<u64>(), 1);
    }

    #[test]
    fn find_item_index_canonicalizes_number_keys() {
        // Write path stored `{"N":"1.0"}`; a lookup of `{"N":"1"}` must find it
        // (same DynamoDB number) rather than miss/duplicate (bug-hunt 2026-07-01).
        let mut t = table_with_hash_key("pk");
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"N": "1.0"}));
        t.put_item_at_key(item);

        let mut lookup = HashMap::new();
        lookup.insert("pk".to_string(), json!({"N": "1"}));
        assert_eq!(t.find_item_index(&lookup), Some(ItemId(0)));

        // A different number still misses.
        let mut other = HashMap::new();
        other.insert("pk".to_string(), json!({"N": "2"}));
        assert_eq!(t.find_item_index(&other), None);
    }

    #[test]
    fn find_item_index_malformed_number_key_does_not_match_valid() {
        // A malformed Number operand must not compare equal to a valid stored
        // numeric key -- otherwise DeleteItem{"N":"abc"} could delete the wrong
        // row (Cubic P1, 2026-07-01).
        let mut t = table_with_hash_key("pk");
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"N": "5"}));
        t.put_item_at_key(item);

        let mut bad = HashMap::new();
        bad.insert("pk".to_string(), json!({"N": "abc"}));
        assert_eq!(t.find_item_index(&bad), None);
    }

    #[test]
    fn top_contributors_returns_sorted() {
        let mut t = table_with_hash_key("pk");
        t.contributor_insights_counters.insert("a".to_string(), 3);
        t.contributor_insights_counters.insert("b".to_string(), 10);
        t.contributor_insights_counters.insert("c".to_string(), 1);
        let top = t.top_contributors(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0], ("b", 10));
        assert_eq!(top[1], ("a", 3));
    }

    #[test]
    fn recalculate_stats_matches_items() {
        let mut t = table_with_hash_key("pk");
        let mut item1 = HashMap::new();
        item1.insert("pk".to_string(), json!({"S": "hello"}));
        let mut item2 = HashMap::new();
        item2.insert("pk".to_string(), json!({"N": "42"}));
        item2.insert("flag".to_string(), json!({"BOOL": true}));
        t.put_item_at_key(item1);
        t.put_item_at_key(item2);
        t.recalculate_stats();
        assert_eq!(t.item_count, 2);
        assert!(t.size_bytes > 0);
    }

    #[test]
    fn estimate_value_size_covers_all_types() {
        let s = DynamoTable::estimate_value_size(&json!({"S": "abc"}));
        assert_eq!(s, 3);
        let n = DynamoTable::estimate_value_size(&json!({"N": "42"}));
        assert_eq!(n, 2);
        let b = DynamoTable::estimate_value_size(&json!({"BOOL": true}));
        assert_eq!(b, 1);
        let null = DynamoTable::estimate_value_size(&json!({"NULL": true}));
        assert_eq!(null, 1);
        // A list or map costs 3 bytes plus one per element.
        let l = DynamoTable::estimate_value_size(&json!({"L": [{"S": "x"}, {"S": "yy"}]}));
        assert_eq!(l, 8);
        let m = DynamoTable::estimate_value_size(&json!({"M": {"key": {"S": "v"}}}));
        assert_eq!(m, 8);
        let ss = DynamoTable::estimate_value_size(&json!({"SS": ["ab", "cde"]}));
        assert_eq!(ss, 5);
        let ns = DynamoTable::estimate_value_size(&json!({"NS": ["12", "345"]}));
        assert_eq!(ns, 5);
        let bin = DynamoTable::estimate_value_size(&json!({"B": "AAAAAAAA"}));
        assert_eq!(bin, 6);
    }

    /// #2502: the key index must induce exactly the same equivalence classes
    /// as the linear scan it replaced. The scan is kept as
    /// `find_item_index_scan`, so it doubles as the oracle here.
    #[test]
    fn key_index_agrees_with_linear_scan() {
        let cases: Vec<Value> = vec![
            json!({"S": "a"}),
            json!({"S": "b"}),
            json!({"S": "1"}), // must not collide with {"N":"1"}
            json!({"N": "1"}),
            json!({"N": "1.0"}), // same number as {"N":"1"}
            json!({"N": "1e0"}), // ditto
            json!({"N": "-0"}),  // negative zero == zero
            json!({"N": "0"}),
            json!({"N": "abc"}), // malformed: byte-equality only
            json!({"N": "abc "}),
            json!({"B": "AAAA"}),
            json!({"BOOL": true}),
        ];
        let mut t = table_with_hash_key("pk");
        // Seed through the scan alone, so the fixture does not depend on the
        // index it is the oracle for: one row per equivalence class.
        let mut rows: Vec<HashMap<String, AttributeValue>> = Vec::new();
        for v in &cases {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), v.clone());
            t.replace_items(rows.clone());
            if t.find_item_index_scan(&item).is_none() {
                rows.push(item);
            }
        }
        t.replace_items(rows);

        for v in &cases {
            let mut probe = HashMap::new();
            probe.insert("pk".to_string(), v.clone());
            assert_eq!(
                t.find_item_index(&probe),
                t.find_item_index_scan(&probe),
                "index and scan disagree for {v}"
            );
        }
    }

    /// A malformed Number probe must not be answered with a valid stored row
    /// via the index either (the scan already guaranteed this).
    #[test]
    fn key_index_malformed_number_does_not_match_valid() {
        let mut t = table_with_hash_key("pk");
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"N": "5"}));
        t.put_item_at_key(item);
        t.rebuild_key_index();

        let mut bad = HashMap::new();
        bad.insert("pk".to_string(), json!({"N": "abc"}));
        assert_eq!(t.find_item_index(&bad), None);
    }

    /// A composite key must distinguish rows that share a hash key, and must
    /// not let the hash/range boundary be forged by a crafted string.
    #[test]
    fn key_index_composite_keys_are_unambiguous() {
        let mut t = table_with_hash_key("pk");
        t.key_schema.push(KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: "RANGE".to_string(),
        });
        let mk = |pk: &str, sk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m.insert("sk".to_string(), json!({ "S": sk }));
            m
        };
        for (pk, sk) in [("a", "b"), ("a", "c"), ("ab", "")] {
            t.put_item_at_key(mk(pk, sk));
        }
        assert_eq!(t.items.len(), 3);
        assert_eq!(t.find_item_index(&mk("a", "b")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk("a", "c")), Some(ItemId(1)));
        assert_eq!(t.find_item_index(&mk("ab", "")), Some(ItemId(2)));
        assert_eq!(t.find_item_index(&mk("a", "z")), None);
    }

    /// #2502: incremental stats must match a full recompute, and the index
    /// must stay consistent across interleaved puts, overwrites and deletes.
    #[test]
    fn incremental_stats_and_index_match_full_recompute() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str, payload: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m.insert("v".to_string(), json!({ "S": payload }));
            m
        };
        for i in 0..50 {
            t.put_item_at_key(mk(&format!("k{i}"), "xxxxx"));
        }
        // Overwrite with a different size, and delete from the middle.
        for i in (0..50).step_by(3) {
            t.put_item_at_key(mk(&format!("k{i}"), "yy"));
        }
        for i in (0..50).step_by(7) {
            t.remove_item_by_key(&mk(&format!("k{i}"), ""));
        }

        let (inc_count, inc_size) = (t.item_count, t.size_bytes);
        t.recalculate_stats();
        assert_eq!(inc_count, t.item_count, "item_count drifted");
        assert_eq!(inc_size, t.size_bytes, "size_bytes drifted");

        // Every surviving row is still addressable under its own id.
        for (id, item) in t.items.clone().iter_with_ids() {
            assert_eq!(t.find_item_index(item), Some(id));
            assert_eq!(t.find_item_index_scan(item), Some(id));
        }
    }

    /// The index is `#[serde(skip)]`, so a table restored from a snapshot
    /// arrives with items and no index. Lookups must still be correct, and a
    /// write must overwrite rather than duplicate.
    #[test]
    fn key_index_recovers_after_snapshot_round_trip() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m
        };
        for i in 0..5 {
            t.put_item_at_key(mk(&format!("k{i}")));
        }
        let json = serde_json::to_string(&t).unwrap();
        let mut restored: DynamoTable = serde_json::from_str(&json).unwrap();
        assert!(
            matches!(restored.key_index, KeyIndex::Unbuilt),
            "index should not persist"
        );

        // Read path works via the scan fallback...
        assert_eq!(restored.find_item_index(&mk("k3")), Some(ItemId(3)));
        // ...and the write path repairs the index instead of duplicating.
        restored.put_item_at_key(mk("k3"));
        assert_eq!(
            restored.items.len(),
            5,
            "write after restore duplicated a row"
        );
        assert!(
            matches!(restored.key_index, KeyIndex::Built { .. }),
            "index was not rebuilt"
        );
        assert_eq!(restored.find_item_index(&mk("k3")), Some(ItemId(3)));
    }

    /// A table holding two rows under one key cannot be answered by a
    /// key -> row map: once the first is removed the second has to surface, which is what
    /// the linear scan did. Such a table must fall back to the scan *and* stay
    /// fallen back, rather than paying a full rebuild on every write — the
    /// cost #2502 is about.
    #[test]
    fn key_index_duplicate_keys_fall_back_to_scan() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m
        };
        // Only reachable by a bulk assignment (an import of an export that
        // repeats a key); the write helpers never create a duplicate.
        t.replace_items(vec![mk("dup"), mk("dup"), mk("other")]);
        assert!(matches!(t.key_index, KeyIndex::Ambiguous { .. }));

        assert_eq!(t.find_item_index(&mk("dup")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk("other")), Some(ItemId(2)));

        // Removing the first duplicate must surface the second, exactly as the
        // scan does.
        t.remove_item_by_key(&mk("dup"));
        assert_eq!(t.find_item_index(&mk("dup")), Some(ItemId(1)));
        assert_eq!(
            t.find_item_index(&mk("dup")),
            t.find_item_index_scan(&mk("dup"))
        );

        // A write does not silently trigger a full rebuild on every call.
        t.put_item_at_key(mk("third"));
        assert!(matches!(t.key_index, KeyIndex::Ambiguous { .. }));
        assert_eq!(t.item_count, 3);
    }

    /// A mutation handed to `update_item_at` can rewrite a primary-key
    /// attribute (the request paths reject that first, as DynamoDB does, but
    /// the helper does not rely on it). `update_item_at` must re-point the
    /// index at the new key, or a later write finds the row under a key it no
    /// longer has.
    #[test]
    fn in_place_key_rewrite_repoints_the_index() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m
        };
        t.put_item_at_key(mk("before"));
        t.put_item_at_key(mk("bystander"));

        let rewrite = t.update_item_at(ItemId(0), |item| {
            item.insert("pk".to_string(), json!({"S": "after"}));
            Ok::<(), ()>(())
        });
        assert!(rewrite.is_ok());

        assert_eq!(t.find_item_index(&mk("after")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk("before")), None);
        assert_eq!(
            t.find_item_index(&mk("before")),
            t.find_item_index_scan(&mk("before"))
        );

        // The freed key is genuinely free: writing it appends a new row rather
        // than clobbering the renamed one.
        t.put_item_at_key(mk("before"));
        assert_eq!(t.items.len(), 3);
        assert_eq!(t.find_item_index(&mk("after")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk("before")), Some(ItemId(2)));
    }

    /// A key rewrite that lands on another row leaves two rows under one key,
    /// which only the scan can answer correctly.
    #[test]
    fn in_place_key_rewrite_onto_another_row_falls_back_to_scan() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m
        };
        t.put_item_at_key(mk("a"));
        t.put_item_at_key(mk("b"));

        let rewrite = t.update_item_at(ItemId(1), |item| {
            item.insert("pk".to_string(), json!({"S": "a"}));
            Ok::<(), ()>(())
        });
        assert!(rewrite.is_ok());

        assert!(matches!(t.key_index, KeyIndex::Ambiguous { .. }));
        assert_eq!(t.find_item_index(&mk("a")), Some(ItemId(0)));
        assert_eq!(
            t.find_item_index(&mk("a")),
            t.find_item_index_scan(&mk("a"))
        );
        assert_eq!(t.find_item_index(&mk("b")), None);
    }

    /// `values_equal` compares `Value`s structurally, so it does not care what
    /// order an object's keys are stored in. The `serde_json/preserve_order`
    /// feature (which the server binary pulls in through feature unification)
    /// makes that order observable in `to_string`, so the encoding sorts keys:
    /// two values that compare equal must never encode differently.
    #[test]
    fn key_encoding_ignores_object_key_order() {
        let a = json!({"S": "a", "N": "1"});
        let b = json!({"N": "1", "S": "a"});
        assert_eq!(a, b, "the two values must compare equal to begin with");
        assert_eq!(
            DynamoTable::encode_key_value(&a),
            DynamoTable::encode_key_value(&b)
        );
    }

    /// `TableItems` stops anything outside this module from pushing to or
    /// removing from the row vector, but the field itself can still be
    /// reassigned wholesale, which no borrow check catches. The recorded row
    /// count makes that detectable: the index must be rebuilt rather than
    /// trusted, or a lookup answers with a row id that no longer exists.
    #[test]
    fn wholesale_reassignment_is_detected_not_trusted() {
        let mut t = table_with_hash_key("pk");
        let mk = |pk: &str| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({ "S": pk }));
            m
        };
        for k in ["a", "b", "c"] {
            t.put_item_at_key(mk(k));
        }
        assert_eq!(t.find_item_index(&mk("c")), Some(ItemId(2)));

        // Behind the index's back, as a future caller might.
        t.items = TableItems::new(vec![mk("c")]);

        // The read path answers from the rows, not from an id that no row
        // carries any more...
        assert_eq!(t.find_item_index(&mk("c")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk("a")), None);
        // ...and the write path repairs the index instead of panicking or
        // overwriting a row that is no longer there.
        t.put_item_at_key(mk("c"));
        assert_eq!(t.items.len(), 1, "the overwrite duplicated a row");
        t.put_item_at_key(mk("d"));
        assert_eq!(t.find_item_index(&mk("d")), Some(ItemId(1)));
        assert_eq!(
            t.find_item_index(&mk("d")),
            t.find_item_index_scan(&mk("d"))
        );
    }

    /// Incremental `size_bytes` must track an in-place update whose item grew
    /// or shrank, matching a full recompute.
    #[test]
    fn in_place_update_keeps_size_bytes_exact() {
        let mut t = table_with_hash_key("pk");
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"S": "k"}));
        item.insert("v".to_string(), json!({"S": "short"}));
        t.put_item_at_key(item);

        let grow = t.update_item_at(ItemId(0), |item| {
            item.insert("v".to_string(), json!({"S": "a much longer value"}));
            Ok::<(), ()>(())
        });
        assert!(grow.is_ok());

        let incremental = t.size_bytes;
        t.recalculate_stats();
        assert_eq!(incremental, t.size_bytes);
    }

    fn mk_pk(pk: &str) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".to_string(), json!({ "S": pk }));
        m
    }

    fn pks(t: &DynamoTable) -> Vec<String> {
        t.items
            .iter()
            .map(|item| item["pk"]["S"].as_str().unwrap().to_string())
            .collect()
    }

    /// #2504: a delete must touch only its own row. Every other row keeps its
    /// id, and the index entries of the survivors are left exactly as they
    /// were -- no repair pass over the rest of the table, which is what made
    /// a bulk delete quadratic.
    #[test]
    fn delete_leaves_every_other_row_id_and_index_entry_alone() {
        let mut t = table_with_hash_key("pk");
        for i in 0..10 {
            t.put_item_at_key(mk_pk(&format!("k{i}")));
        }
        let ids_before: Vec<(ItemId, HashMap<String, AttributeValue>)> = t
            .items
            .iter_with_ids()
            .map(|(id, item)| (id, item.clone()))
            .collect();
        let KeyIndex::Built { ids, .. } = t.key_index.clone() else {
            panic!("index should be built after writes");
        };

        let removed = t.remove_item_by_key(&mk_pk("k3")).unwrap();
        assert_eq!(removed, mk_pk("k3"));

        let KeyIndex::Built { ids: after, rows } = &t.key_index else {
            panic!("a delete must not drop the index");
        };
        assert_eq!(*rows, 9);
        assert_eq!(after.len(), ids.len() - 1);
        for (id, item) in ids_before {
            if item == mk_pk("k3") {
                assert!(t.items.get(id).is_none());
                continue;
            }
            assert_eq!(t.items.get(id), Some(&item), "row {id:?} moved");
            let key = t.encode_key(&item).unwrap();
            assert_eq!(after.get(&key), ids.get(&key));
            assert_eq!(t.find_item_index(&item), Some(id));
        }
    }

    /// Scan pagination resumes after `ExclusiveStartKey` in storage order, so
    /// deletes must never reorder the rows that remain, and a new row always
    /// lands after every existing one -- including after the table's first
    /// rows were deleted. An overwrite keeps its row's place.
    #[test]
    fn storage_order_survives_deletes_inserts_and_overwrites() {
        let mut t = table_with_hash_key("pk");
        for k in ["a", "b", "c", "d", "e"] {
            t.put_item_at_key(mk_pk(k));
        }
        t.remove_item_by_key(&mk_pk("a"));
        t.remove_item_by_key(&mk_pk("c"));
        assert_eq!(pks(&t), ["b", "d", "e"]);

        t.put_item_at_key(mk_pk("a"));
        t.put_item_at_key(mk_pk("f"));
        assert_eq!(pks(&t), ["b", "d", "e", "a", "f"]);

        let mut overwrite = mk_pk("d");
        overwrite.insert("v".to_string(), json!({"S": "new"}));
        let (_, replaced) = t.put_item_at_key(overwrite);
        assert!(replaced);
        assert_eq!(pks(&t), ["b", "d", "e", "a", "f"]);

        // A snapshot writes the rows in storage order and a restore keeps it.
        let json = serde_json::to_string(&t).unwrap();
        let mut restored: DynamoTable = serde_json::from_str(&json).unwrap();
        assert_eq!(pks(&restored), ["b", "d", "e", "a", "f"]);
        restored.put_item_at_key(mk_pk("g"));
        restored.remove_item_by_key(&mk_pk("b"));
        assert_eq!(pks(&restored), ["d", "e", "a", "f", "g"]);
    }

    /// Ids collected up front (a PartiQL `DELETE ... WHERE` sweep) must all
    /// stay valid while the rows they name are removed one by one, in any
    /// order: with ids, only a back-to-front sweep was safe.
    #[test]
    fn ids_collected_before_a_removal_sweep_stay_valid() {
        let mut t = table_with_hash_key("pk");
        for i in 0..8 {
            t.put_item_at_key(mk_pk(&format!("k{i}")));
        }
        let doomed: Vec<ItemId> = t
            .items
            .iter_with_ids()
            .filter(|(_, item)| {
                let n: usize = item["pk"]["S"].as_str().unwrap()[1..].parse().unwrap();
                n.is_multiple_of(2)
            })
            .map(|(id, _)| id)
            .collect();
        for id in doomed {
            let removed = t.remove_item_at(id);
            let n: usize = removed["pk"]["S"].as_str().unwrap()[1..].parse().unwrap();
            assert!(
                n.is_multiple_of(2),
                "front-to-back sweep removed the wrong row"
            );
        }
        assert_eq!(pks(&t), ["k1", "k3", "k5", "k7"]);
        let (inc_count, inc_size) = (t.item_count, t.size_bytes);
        t.recalculate_stats();
        assert_eq!((inc_count, inc_size), (t.item_count, t.size_bytes));
    }

    /// The TTL sweep removes rows through `remove_items_where`. It must hand
    /// back the removed rows in storage order and keep the index built and in
    /// step, instead of rebuilding it from scratch.
    #[test]
    fn remove_items_where_settles_index_and_stats_incrementally() {
        let mut t = table_with_hash_key("pk");
        for k in ["a", "b", "c", "d"] {
            t.put_item_at_key(mk_pk(k));
        }
        let survivor_ids: Vec<ItemId> = ["a", "c"]
            .iter()
            .map(|k| t.find_item_index(&mk_pk(k)).unwrap())
            .collect();

        let removed =
            t.remove_items_where(|item| matches!(item["pk"]["S"].as_str(), Some("b") | Some("d")));
        assert_eq!(removed, vec![mk_pk("b"), mk_pk("d")]);
        assert_eq!(pks(&t), ["a", "c"]);
        assert!(matches!(t.key_index, KeyIndex::Built { rows: 2, .. }));
        assert_eq!(t.find_item_index(&mk_pk("a")), Some(survivor_ids[0]));
        assert_eq!(t.find_item_index(&mk_pk("c")), Some(survivor_ids[1]));
        assert_eq!(t.find_item_index(&mk_pk("b")), None);
        let (inc_count, inc_size) = (t.item_count, t.size_bytes);
        t.recalculate_stats();
        assert_eq!((inc_count, inc_size), (t.item_count, t.size_bytes));
    }

    /// Delete every row in a scrambled order, checking after each one that the
    /// index still agrees with the scan for every key ever written.
    #[test]
    fn index_agrees_with_scan_through_a_full_scrambled_delete() {
        let mut t = table_with_hash_key("pk");
        let n = 64;
        for i in 0..n {
            t.put_item_at_key(mk_pk(&format!("k{i}")));
        }
        for step in 0..n {
            // 37 is coprime with 64, so this visits every key exactly once.
            let victim = (step * 37) % n;
            assert!(t
                .remove_item_by_key(&mk_pk(&format!("k{victim}")))
                .is_some());
            for i in 0..n {
                let probe = mk_pk(&format!("k{i}"));
                assert_eq!(
                    t.find_item_index(&probe),
                    t.find_item_index_scan(&probe),
                    "disagree on k{i} after {} deletes",
                    step + 1
                );
            }
        }
        assert!(t.items.is_empty());
        assert_eq!((t.item_count, t.size_bytes), (0, 0));
    }

    /// A table on the scan fallback over duplicate keys recovers its index
    /// once a sweep removes the duplicates.
    #[test]
    fn remove_items_where_restores_index_once_duplicates_are_gone() {
        let mut t = table_with_hash_key("pk");
        let mut expiring = mk_pk("dup");
        expiring.insert("ttl".to_string(), json!({"N": "1"}));
        t.replace_items(vec![mk_pk("dup"), expiring, mk_pk("other")]);
        assert!(matches!(t.key_index, KeyIndex::Ambiguous { .. }));

        let removed = t.remove_items_where(|item| item.contains_key("ttl"));
        assert_eq!(removed.len(), 1);
        assert!(matches!(t.key_index, KeyIndex::Built { rows: 2, .. }));
        assert_eq!(t.find_item_index(&mk_pk("dup")), Some(ItemId(0)));
        assert_eq!(t.find_item_index(&mk_pk("other")), Some(ItemId(2)));
    }

    fn composite_table() -> DynamoTable {
        let mut t = table_with_hash_key("pk");
        t.key_schema.push(KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: "RANGE".to_string(),
        });
        t
    }

    fn mk_pk_sk(pk: &str, sk: i64) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".to_string(), json!({ "S": pk }));
        m.insert("sk".to_string(), json!({ "N": sk.to_string() }));
        m
    }

    /// The same rows, with the index forced off so `scan_rows_after` takes
    /// its sorting fallback.
    fn without_index(t: &DynamoTable) -> DynamoTable {
        let mut copy = t.clone();
        copy.key_index = KeyIndex::Unbuilt;
        copy
    }

    fn scan_after(
        t: &DynamoTable,
        start: Option<&HashMap<String, AttributeValue>>,
    ) -> Vec<HashMap<String, AttributeValue>> {
        t.scan_rows_after(start).cloned().collect()
    }

    /// Scan order is a function of the keys alone: the index walk and the
    /// sorting fallback agree for every start key, including keys of rows
    /// that were deleted, keys that never existed, and a numerically-equal
    /// spelling of a stored number.
    #[test]
    fn scan_order_index_walk_matches_sorting_fallback() {
        let mut t = composite_table();
        for pk in ["a", "b", "c", "d", "e", "f"] {
            for sk in [3, 1, 2] {
                t.put_item_at_key(mk_pk_sk(pk, sk));
            }
        }
        t.remove_item_by_key(&mk_pk_sk("c", 2));
        t.remove_item_by_key(&mk_pk_sk("e", 1));
        t.remove_item_by_key(&mk_pk_sk("e", 2));
        t.remove_item_by_key(&mk_pk_sk("e", 3));
        assert!(matches!(t.key_index, KeyIndex::Built { .. }));
        let fallback = without_index(&t);

        let full = scan_after(&t, None);
        assert_eq!(full, scan_after(&fallback, None));
        assert_eq!(full.len(), t.items.len());

        let mut starts: Vec<HashMap<String, AttributeValue>> = full.clone();
        starts.push(mk_pk_sk("c", 2)); // deleted
        starts.push(mk_pk_sk("e", 1)); // whole partition deleted
        starts.push(mk_pk_sk("zz", 9)); // never existed
        let mut spelled = mk_pk_sk("a", 1);
        spelled.insert("sk".to_string(), json!({"N": "1.0"}));
        starts.push(spelled);
        for start in &starts {
            let via_index = scan_after(&t, Some(start));
            assert_eq!(via_index, scan_after(&fallback, Some(start)), "{start:?}");
            // Exactly the rows ordered after the start key.
            let start_key = t.encode_key(start).unwrap();
            let expected: Vec<_> = full
                .iter()
                .filter(|item| t.encode_key(item).unwrap() > start_key)
                .cloned()
                .collect();
            assert_eq!(via_index, expected, "{start:?}");
        }
    }

    /// The Scan order must not depend on insertion order, or two tables
    /// holding the same rows would page differently.
    #[test]
    fn scan_order_ignores_insertion_order() {
        let mut forward = composite_table();
        let mut backward = composite_table();
        let keys: Vec<(String, i64)> = (0..20).map(|i| (format!("p{}", i % 7), i)).collect();
        for (pk, sk) in &keys {
            forward.put_item_at_key(mk_pk_sk(pk, *sk));
        }
        for (pk, sk) in keys.iter().rev() {
            backward.put_item_at_key(mk_pk_sk(pk, *sk));
        }
        assert_eq!(scan_after(&forward, None), scan_after(&backward, None));
        // A partition's rows stay together.
        let order: Vec<String> = scan_after(&forward, None)
            .iter()
            .map(|item| item["pk"]["S"].as_str().unwrap().to_string())
            .collect();
        let mut seen: Vec<&String> = Vec::new();
        for pk in &order {
            if seen.last() != Some(&pk) {
                assert!(!seen.contains(&pk), "partition {pk} split: {order:?}");
                seen.push(pk);
            }
        }
    }

    /// A client draining a table deletes every row of a page -- including the
    /// one its `LastEvaluatedKey` names -- before asking for the next page.
    /// Every row must still be visited exactly once.
    #[test]
    fn paging_drain_that_deletes_the_start_row_visits_every_row() {
        let mut t = composite_table();
        for i in 0..50 {
            t.put_item_at_key(mk_pk_sk(&format!("p{}", i % 9), i));
        }
        let mut seen = Vec::new();
        let mut start: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let page: Vec<_> = t.scan_rows_after(start.as_ref()).take(4).cloned().collect();
            if page.is_empty() {
                break;
            }
            for item in &page {
                t.remove_item_by_key(item).unwrap();
            }
            start = page.last().cloned();
            seen.extend(page);
        }
        assert_eq!(seen.len(), 50);
        assert!(t.items.is_empty());
    }

    /// Rows with no partition key (only an import can produce one) are left
    /// out of the index, so the scan sorts instead, and they come last. A
    /// start key without the partition key selects nothing.
    #[test]
    fn scan_order_places_keyless_rows_last() {
        let mut t = table_with_hash_key("pk");
        let mut keyless = HashMap::new();
        keyless.insert("other".to_string(), json!({"S": "x"}));
        t.replace_items(vec![keyless.clone(), mk_pk("b"), mk_pk("a")]);
        let rows = scan_after(&t, None);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.last(), Some(&keyless));
        assert_eq!(scan_after(&t, Some(&rows[0])).last(), Some(&keyless));
        assert!(scan_after(&t, Some(&keyless)).is_empty());
    }

    /// Scan order is persisted nowhere, so it has to come out the same after a
    /// restart: the hash is fixed, not std's per-release `DefaultHasher`.
    #[test]
    fn scan_order_hash_is_fixed() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn snapshot_load_builds_key_indexes() {
        let mut state = DynamoDbState::new("123456789012", "us-east-1");
        let mut t = table_with_hash_key("pk");
        t.put_item_at_key(mk_pk("a"));
        state.tables.insert("t".to_string(), t);
        let json = serde_json::to_string(&state).unwrap();
        let mut restored: DynamoDbState = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored.tables["t"].key_index, KeyIndex::Unbuilt));
        restored.rebuild_derived_state();
        assert!(matches!(
            restored.tables["t"].key_index,
            KeyIndex::Built { rows: 1, .. }
        ));
    }

    /// A snapshot from a build that sized items differently carries a stale
    /// `size_bytes`; loading it recomputes the figure from the rows.
    #[test]
    fn snapshot_load_recomputes_table_size() {
        let mut state = DynamoDbState::new("123456789012", "us-east-1");
        let mut t = table_with_hash_key("pk");
        t.put_item_at_key(mk_pk("a"));
        let expected = t.size_bytes;
        t.size_bytes = 999_999;
        t.item_count = 42;
        state.tables.insert("t".to_string(), t);
        let json = serde_json::to_string(&state).unwrap();
        let mut restored: DynamoDbState = serde_json::from_str(&json).unwrap();
        restored.rebuild_derived_state();
        assert_eq!(restored.tables["t"].size_bytes, expected);
        assert_eq!(restored.tables["t"].item_count, 1);
    }

    /// Within a partition, a Scan returns rows in sort-key order, as DynamoDB
    /// does: numbers by value (not as text, where 10 sorts before 2), strings
    /// by UTF-8 bytes, binaries by decoded bytes. Numerically-equal spellings
    /// are still one key.
    #[test]
    fn scan_order_within_a_partition_follows_sort_key_values() {
        let mk = |sk: Value| {
            let mut m = HashMap::new();
            m.insert("pk".to_string(), json!({"S": "p"}));
            m.insert("sk".to_string(), sk);
            m
        };
        let sorted = |t: &DynamoTable| -> Vec<Value> {
            scan_after(t, None)
                .iter()
                .map(|i| i["sk"].clone())
                .collect()
        };

        let mut numbers = composite_table();
        for n in ["10", "2", "-1", "-2", "1.5", "1e1", "0", "-0.5"] {
            numbers.put_item_at_key(mk(json!({ "N": n })));
        }
        // "1e1" is the same number as "10", so it overwrote that row.
        assert_eq!(numbers.items.len(), 7);
        assert_eq!(
            sorted(&numbers),
            ["-2", "-1", "-0.5", "0", "1.5", "2", "1e1"]
                .iter()
                .map(|n| json!({ "N": n }))
                .collect::<Vec<_>>()
        );
        assert_eq!(sorted(&numbers), sorted(&without_index(&numbers)));

        let mut strings = composite_table();
        for s in ["b", "B", "aa", "a", "\u{e9}", "z"] {
            strings.put_item_at_key(mk(json!({ "S": s })));
        }
        assert_eq!(
            sorted(&strings),
            ["B", "a", "aa", "b", "z", "\u{e9}"]
                .iter()
                .map(|s| json!({ "S": s }))
                .collect::<Vec<_>>()
        );

        let mut binaries = composite_table();
        // 0xff, 0x00 0x01, 0x7f -- as base64 their text order differs.
        for b in ["/w==", "AAE=", "fw=="] {
            binaries.put_item_at_key(mk(json!({ "B": b })));
        }
        assert_eq!(
            sorted(&binaries),
            ["AAE=", "fw==", "/w=="]
                .iter()
                .map(|b| json!({ "B": b }))
                .collect::<Vec<_>>()
        );

        // Paging resumes by value too: after sk=2, a start key spelled 2.0.
        let after: Vec<Value> = scan_after(&numbers, Some(&mk(json!({"N": "2.0"}))))
            .iter()
            .map(|i| i["sk"].clone())
            .collect();
        assert_eq!(after, vec![json!({"N": "1e1"})]);
    }
}
