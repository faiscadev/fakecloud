use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    pub projection_type: String, // ALL, KEYS_ONLY, INCLUDE
    pub non_key_attributes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoTable {
    pub name: String,
    pub arn: String,
    pub table_id: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub provisioned_throughput: ProvisionedThroughput,
    pub items: Vec<HashMap<String, AttributeValue>>,
    /// Primary-key -> position in `items`, so a write does not have to scan
    /// the whole table to decide insert-vs-overwrite. Without it every write
    /// was O(table size) and a bulk load was quadratic (#2502).
    ///
    /// Not persisted: it is derived state, and rebuilding it on load keeps
    /// existing snapshots readable. `items` stays the source of truth —
    /// scans, pagination and the stream paths all still index into it — so
    /// the two must be mutated together via `put_item_at_key` /
    /// `remove_item_by_key`, never by touching `items` directly.
    /// Public so out-of-crate constructors (the CloudFormation provisioner)
    /// can build a table with `key_index: Default::default()`.
    #[serde(skip)]
    pub key_index: HashMap<String, usize>,
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
    pub export_status: String,
    pub table_arn: String,
    pub s3_bucket: String,
    pub s3_prefix: Option<String>,
    pub export_format: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub export_time: DateTime<Utc>,
    pub item_count: i64,
    pub billed_size_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportDescription {
    pub import_arn: String,
    pub import_status: String,
    pub table_arn: String,
    pub table_name: String,
    pub s3_bucket_source: String,
    pub input_format: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub processed_item_count: i64,
    pub processed_size_bytes: i64,
}

impl DynamoTable {
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
        // `to_string` on a serde_json Value is stable for a given value, and
        // the type tag is part of it, so `{"S":"1"}` cannot collide with
        // `{"N":"1"}`.
        format!("X:{v}")
    }

    /// Canonical string form of an item's full primary key, or `None` if the
    /// item is missing the hash key (or a declared range key) — such an item
    /// is not addressable by key and is left out of the index, mirroring the
    /// old scan, which required `item.get(hash_key).is_some()`.
    fn encode_key(&self, item: &HashMap<String, AttributeValue>) -> Option<String> {
        let hash_key = self.hash_key_name();
        let mut out = Self::encode_key_value(item.get(hash_key)?);
        if let Some(rk) = self.range_key_name() {
            // The range key is part of the identity when the schema declares
            // one; `values_equal(None, None)` was true in the scan, so an item
            // with no range-key attribute is only equal to another item that
            // also lacks it. A distinct sentinel keeps that class separate
            // from any real value.
            out.push('\u{1f}');
            match item.get(rk) {
                Some(v) => out.push_str(&Self::encode_key_value(v)),
                None => out.push_str("\u{0}none"),
            }
        }
        Some(out)
    }

    /// Rebuild `key_index` from `items`. Called after loading a snapshot (the
    /// index is not persisted) and after any bulk rewrite of `items`.
    pub fn rebuild_key_index(&mut self) {
        let mut index = HashMap::with_capacity(self.items.len());
        // Iterate forward and keep the *first* position for a duplicate key so
        // lookups agree with the old `position()` scan. Well-formed tables have
        // no duplicates; a snapshot written by an older build might.
        for (i, item) in self.items.iter().enumerate() {
            if let Some(k) = self.encode_key(item) {
                index.entry(k).or_insert(i);
            }
        }
        self.key_index = index;
    }

    /// Whether `key_index` still needs building. It is `#[serde(skip)]`, so a
    /// table restored from a snapshot arrives with items but an empty index;
    /// an empty table legitimately has an empty index, hence the `items` check.
    fn key_index_is_stale(&self) -> bool {
        self.key_index.len() != self.items.len()
    }

    /// Ensure `key_index` is populated, rebuilding it if this table came from a
    /// snapshot (where the index is not persisted) or from a bulk assignment to
    /// `items`. Repairing lazily here means no load path can forget to do it
    /// and silently turn every lookup into a miss — which would make writes
    /// duplicate rows instead of overwriting them.
    pub fn ensure_key_index(&mut self) {
        if self.key_index_is_stale() {
            self.rebuild_key_index();
        }
    }

    /// Find an item index by its primary key. O(1) via `key_index`.
    ///
    /// Takes `&self`, so it cannot repair a stale index; it falls back to the
    /// original linear scan in that case rather than returning a wrong answer.
    /// Every mutating path goes through the `&mut self` helpers below, which
    /// call `ensure_key_index` first, so the fallback is a correctness
    /// backstop rather than the normal path.
    pub fn find_item_index(&self, key: &HashMap<String, AttributeValue>) -> Option<usize> {
        if self.key_index_is_stale() {
            return self.find_item_index_scan(key);
        }
        match self.encode_key(key) {
            Some(k) => self.key_index.get(&k).copied(),
            // A key that cannot be encoded is missing the hash key, so it
            // matches nothing — the same answer the scan gave.
            None => None,
        }
    }

    /// The pre-index linear scan. Retained as the fallback for a stale index
    /// and as the oracle the index is tested against.
    fn find_item_index_scan(&self, key: &HashMap<String, AttributeValue>) -> Option<usize> {
        let hash_key = self.hash_key_name();
        let range_key = self.range_key_name();

        // Compare keys with numeric-aware equality: a Number key stored as
        // `{"N":"1"}` must match a lookup of `{"N":"1.0"}` (they are the same
        // DynamoDB number). Raw JSON `==` treated them as distinct, so the
        // write/lookup path could miss an existing item or store a duplicate
        // even though the compare/filter path was already canonical
        // (bug-hunt 2026-07-01, DynamoDB write-path number-canon).
        use crate::service::helpers::partiql::values_equal;
        self.items.iter().position(|item| {
            let hash_match =
                values_equal(item.get(hash_key), key.get(hash_key)) && item.get(hash_key).is_some();
            if !hash_match {
                return false;
            }
            match range_key {
                Some(rk) => values_equal(item.get(rk), key.get(rk)),
                None => true,
            }
        })
    }

    /// Insert or overwrite the item with this key, keeping `key_index` and the
    /// cached stats in step. Returns the index it now occupies, and whether
    /// this replaced an existing item.
    pub fn put_item_at_key(&mut self, item: HashMap<String, AttributeValue>) -> (usize, bool) {
        self.ensure_key_index();
        match self.find_item_index(&item) {
            Some(idx) => {
                // Adjust the cached size by the delta rather than resumming the
                // whole table (#2502).
                self.size_bytes -= Self::estimate_item_size(&self.items[idx]);
                self.size_bytes += Self::estimate_item_size(&item);
                self.items[idx] = item;
                (idx, true)
            }
            None => {
                let idx = self.items.len();
                if let Some(k) = self.encode_key(&item) {
                    self.key_index.insert(k, idx);
                }
                self.size_bytes += Self::estimate_item_size(&item);
                self.item_count += 1;
                self.items.push(item);
                (idx, false)
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
        let idx = self.find_item_index(key)?;
        let removed = self.items.remove(idx);
        if let Some(k) = self.encode_key(&removed) {
            self.key_index.remove(&k);
        }
        // `Vec::remove` shifts every later element down one, so their recorded
        // positions are now stale. Repair just those rather than rebuilding the
        // whole index.
        for pos in self.key_index.values_mut() {
            if *pos > idx {
                *pos -= 1;
            }
        }
        self.size_bytes -= Self::estimate_item_size(&removed);
        self.item_count -= 1;
        Some(removed)
    }

    /// Replace the item already stored at `idx`, keeping the cached size in
    /// step. The caller must not change the item's primary key.
    pub fn replace_item_at(&mut self, idx: usize, item: HashMap<String, AttributeValue>) {
        self.size_bytes -= Self::estimate_item_size(&self.items[idx]);
        self.size_bytes += Self::estimate_item_size(&item);
        self.items[idx] = item;
    }

    /// The size the item at `idx` currently contributes to `size_bytes`.
    /// Pair with [`Self::sync_item_size_at`] around an in-place mutation.
    pub fn item_size_at(&self, idx: usize) -> i64 {
        self.items
            .get(idx)
            .map(Self::estimate_item_size)
            .unwrap_or(0)
    }

    /// Settle `size_bytes` after the item at `idx` was mutated in place,
    /// given the size it contributed beforehand.
    pub fn sync_item_size_at(&mut self, idx: usize, size_before: i64) {
        self.size_bytes += self.item_size_at(idx) - size_before;
    }

    /// Estimate item size in bytes (rough approximation).
    fn estimate_item_size(item: &HashMap<String, AttributeValue>) -> i64 {
        let mut size: i64 = 0;
        for (k, v) in item {
            size += k.len() as i64;
            size += Self::estimate_value_size(v);
        }
        size
    }

    fn estimate_value_size(v: &Value) -> i64 {
        match v {
            Value::Object(obj) => {
                if let Some(s) = obj.get("S").and_then(|v| v.as_str()) {
                    s.len() as i64
                } else if let Some(n) = obj.get("N").and_then(|v| v.as_str()) {
                    n.len() as i64
                } else if obj.contains_key("BOOL") || obj.contains_key("NULL") {
                    1
                } else if let Some(l) = obj.get("L").and_then(|v| v.as_array()) {
                    3 + l.iter().map(Self::estimate_value_size).sum::<i64>()
                } else if let Some(m) = obj.get("M").and_then(|v| v.as_object()) {
                    3 + m
                        .iter()
                        .map(|(k, v)| k.len() as i64 + Self::estimate_value_size(v))
                        .sum::<i64>()
                } else if let Some(ss) = obj.get("SS").and_then(|v| v.as_array()) {
                    ss.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.len() as i64)
                        .sum()
                } else if let Some(ns) = obj.get("NS").and_then(|v| v.as_array()) {
                    ns.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.len() as i64)
                        .sum()
                } else if let Some(b) = obj.get("B").and_then(|v| v.as_str()) {
                    // Base64-encoded binary
                    (b.len() as i64 * 3) / 4
                } else {
                    v.to_string().len() as i64
                }
            }
            _ => v.to_string().len() as i64,
        }
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
        }
    }

    pub fn reset(&mut self) {
        self.tables.clear();
        self.backups.clear();
        self.global_tables.clear();
        self.exports.clear();
        self.imports.clear();
        self.lambda_stream_checkpoints.clear();
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
            items: Vec::new(),
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
        t.items.push(item);

        let mut lookup = HashMap::new();
        lookup.insert("pk".to_string(), json!({"N": "1"}));
        assert_eq!(t.find_item_index(&lookup), Some(0));

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
        t.items.push(item);

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
        t.items.push(item1);
        t.items.push(item2);
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
        let l = DynamoTable::estimate_value_size(&json!({"L": [{"S": "x"}, {"S": "yy"}]}));
        assert_eq!(l, 6);
        let m = DynamoTable::estimate_value_size(&json!({"M": {"key": {"S": "v"}}}));
        assert_eq!(m, 7);
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
        for v in &cases {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), v.clone());
            // Only insert if the scan says it is not already present, so the
            // table holds one row per equivalence class.
            if t.find_item_index_scan(&item).is_none() {
                t.items.push(item);
            }
        }
        t.rebuild_key_index();

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
        t.items.push(item);
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
        assert_eq!(t.find_item_index(&mk("a", "b")), Some(0));
        assert_eq!(t.find_item_index(&mk("a", "c")), Some(1));
        assert_eq!(t.find_item_index(&mk("ab", "")), Some(2));
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
        // Overwrite with a different size, and delete from the middle so the
        // index has to repair the shifted positions.
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

        // Every surviving row is still addressable at its true position.
        for (i, item) in t.items.clone().iter().enumerate() {
            assert_eq!(t.find_item_index(item), Some(i));
            assert_eq!(t.find_item_index_scan(item), Some(i));
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
        assert!(restored.key_index.is_empty(), "index should not persist");

        // Read path works via the scan fallback...
        assert_eq!(restored.find_item_index(&mk("k3")), Some(3));
        // ...and the write path repairs the index instead of duplicating.
        restored.put_item_at_key(mk("k3"));
        assert_eq!(
            restored.items.len(),
            5,
            "write after restore duplicated a row"
        );
        assert!(!restored.key_index.is_empty(), "index was not rebuilt");
        assert_eq!(restored.find_item_index(&mk("k3")), Some(3));
    }
}
