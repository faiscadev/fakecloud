//! Multi-region tables: version 2019.11.21 replicas (added and removed with
//! `UpdateTable` `ReplicaUpdates`) and legacy 2017.11.29 global tables
//! (`CreateGlobalTable` over same-named tables in several regions).
//!
//! Every replica is an ordinary table in its own region's state: describable,
//! listable and writable from a client in that region. A write to any replica
//! is replicated to the others, each replicated change recorded on the
//! receiving replica's stream the way DynamoDB records replicated writes.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::Utc;
use fakecloud_core::multi_account::MultiRegionState;
use fakecloud_core::service::AwsServiceError;
use http::StatusCode;
use serde_json::Value;

use crate::state::{AttributeValue, DynamoDbState, DynamoTable};

type Item = HashMap<String, AttributeValue>;

/// The region of `account` whose state holds the legacy global table `name`
/// as seen from `region`: `region` itself when the global table was created
/// there, otherwise the region of a global table of that name whose
/// replication group includes `region`. A legacy global table is one
/// resource across its replica regions, describable from any of them.
pub(crate) fn global_table_region(
    accounts: &MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
) -> Option<String> {
    if accounts
        .regional(account, region)
        .is_some_and(|s| s.global_tables.contains_key(name))
    {
        return Some(region.to_string());
    }
    accounts.get(account)?.regions().find_map(|(r, s)| {
        s.global_tables
            .get(name)
            .filter(|gt| gt.replication_group.iter().any(|g| g.region_name == region))
            .map(|_| r.to_string())
    })
}

/// The other regions holding a replica of the table `name` that lives in
/// (`account`, `region`): its 2019.11.21 replicas plus the other regions of
/// any legacy global table of that name whose replication group includes
/// `region`. Empty for a single-region table, or one that does not exist.
pub(crate) fn replica_peers(
    accounts: &MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
) -> Vec<String> {
    let Some(table) = accounts
        .regional(account, region)
        .and_then(|s| s.tables.get(name))
    else {
        return Vec::new();
    };
    let mut peers: BTreeSet<String> = table.replica_regions.iter().cloned().collect();
    if let Some(regional) = accounts.get(account) {
        for (_, state) in regional.regions() {
            if let Some(gt) = state.global_tables.get(name) {
                if gt.replication_group.iter().any(|r| r.region_name == region) {
                    peers.extend(gt.replication_group.iter().map(|r| r.region_name.clone()));
                }
            }
        }
    }
    peers.remove(region);
    peers.into_iter().collect()
}

/// Copy the rows at `keys` of the table `name` in (`account`, `region`) to
/// each of its other replicas after a write there: a row the source holds is
/// written to the replica (unless it already holds the same row), a row the
/// source no longer holds is deleted from it, and each change lands on the
/// replica's stream. Only the written rows are touched -- a concurrent write
/// to another row of another replica is never undone -- and, as on AWS, the
/// last write to a row wins. A peer region without a same-named table of the
/// same key schema is skipped. A no-op for a table without replicas.
pub(crate) fn replicate_keys(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
    keys: &[Item],
) {
    if keys.is_empty() {
        return;
    }
    let peers = replica_peers(accounts, account, region, name);
    if peers.is_empty() {
        return;
    }
    let Some(source) = accounts
        .regional_get_mut(account, region)
        .and_then(|s| s.tables.get_mut(name))
    else {
        return;
    };
    source.ensure_key_index();
    let key_schema = key_schema_of(source);
    // Each written key once, with the source's current row (None: deleted).
    let mut seen = BTreeSet::new();
    let changes: Vec<(Item, Option<Item>)> = keys
        .iter()
        .filter(|key| seen.insert(serde_json::to_string(&sorted(key)).unwrap_or_default()))
        .map(|key| {
            let row = source
                .find_item_index(key)
                .map(|id| source.items()[id].clone());
            (key.clone(), row)
        })
        .collect();
    for peer in peers {
        let Some(replica) = accounts
            .regional_get_mut(account, &peer)
            .and_then(|s| s.tables.get_mut(name))
        else {
            continue;
        };
        if key_schema_of(replica) != key_schema {
            continue;
        }
        apply_changes(replica, &changes, &peer);
    }
}

fn sorted(key: &Item) -> BTreeMap<&String, &AttributeValue> {
    key.iter().collect()
}

fn key_schema_of(table: &DynamoTable) -> Vec<(String, String)> {
    table
        .key_schema
        .iter()
        .map(|k| (k.attribute_name.clone(), k.key_type.clone()))
        .collect()
}

fn apply_changes(replica: &mut DynamoTable, changes: &[(Item, Option<Item>)], region: &str) {
    replica.ensure_key_index();
    for (key, row) in changes {
        let old = replica
            .find_item_index(key)
            .map(|id| replica.items()[id].clone());
        match row {
            Some(row) => {
                if old.as_ref() == Some(row) {
                    continue;
                }
                replica.put_item_at_key(row.clone());
                let event = if old.is_some() { "MODIFY" } else { "INSERT" };
                record(replica, event, key.clone(), old, Some(row.clone()), region);
            }
            None => {
                if let Some(old) = replica.remove_item_by_key(key) {
                    record(replica, "REMOVE", key.clone(), Some(old), None, region);
                }
            }
        }
    }
}

fn record(
    table: &mut DynamoTable,
    event: &str,
    keys: Item,
    old: Option<Item>,
    new: Option<Item>,
    region: &str,
) {
    if let Some(record) =
        crate::streams::generate_stream_record(table, event, keys, old, new, region)
    {
        crate::streams::add_stream_record(table, record);
    }
}

/// One entry of `UpdateTable` `ReplicaUpdates`.
#[derive(Debug, Clone)]
pub(crate) enum ReplicaUpdate {
    Create {
        region: String,
        kms_master_key_id: Option<String>,
        read_capacity_override: Option<i64>,
        table_class: Option<String>,
    },
    Update {
        region: String,
        read_capacity_override: Option<i64>,
        table_class: Option<String>,
    },
    Delete {
        region: String,
    },
    /// Leave the replication group, keeping the local replica as a
    /// standalone regional table.
    Disassociate {
        region: String,
        mode: Option<String>,
    },
}

impl ReplicaUpdate {
    pub(crate) fn region(&self) -> &str {
        match self {
            Self::Create { region, .. }
            | Self::Update { region, .. }
            | Self::Delete { region }
            | Self::Disassociate { region, .. } => region,
        }
    }
}

/// Parse `ReplicaUpdates`; entries without a `RegionName` are skipped.
pub(crate) fn parse_replica_updates(body: &Value) -> Vec<ReplicaUpdate> {
    let rcu = |v: &Value| v["ProvisionedThroughputOverride"]["ReadCapacityUnits"].as_i64();
    let class = |v: &Value| v["TableClassOverride"].as_str().map(str::to_string);
    body["ReplicaUpdates"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|u| {
            if let Some(c) = u.get("Create").filter(|c| c.is_object()) {
                return Some(ReplicaUpdate::Create {
                    region: c["RegionName"].as_str()?.to_string(),
                    kms_master_key_id: c["KMSMasterKeyId"].as_str().map(str::to_string),
                    read_capacity_override: rcu(c),
                    table_class: class(c),
                });
            }
            if let Some(c) = u.get("Update").filter(|c| c.is_object()) {
                return Some(ReplicaUpdate::Update {
                    region: c["RegionName"].as_str()?.to_string(),
                    read_capacity_override: rcu(c),
                    table_class: class(c),
                });
            }
            if let Some(d) = u.get("Disassociate").filter(|d| d.is_object()) {
                // Both members are required; a missing region is rejected by
                // `validate_replica_updates`.
                return Some(ReplicaUpdate::Disassociate {
                    region: d["RegionName"].as_str().unwrap_or_default().to_string(),
                    mode: d["DisassociationMode"].as_str().map(str::to_string),
                });
            }
            let d = u.get("Delete").filter(|d| d.is_object())?;
            Some(ReplicaUpdate::Delete {
                region: d["RegionName"].as_str()?.to_string(),
            })
        })
        .collect()
}

fn validation(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", message)
}

/// Check `updates` against the replica set of the table `name` in
/// (`account`, `region`) before anything changes, so a rejected UpdateTable
/// leaves every region as it was.
pub(crate) fn validate_replica_updates(
    accounts: &MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
    updates: &[ReplicaUpdate],
) -> Result<(), AwsServiceError> {
    let Some(table) = accounts
        .regional(account, region)
        .and_then(|s| s.tables.get(name))
    else {
        // The caller reports the missing table.
        return Ok(());
    };
    let mut replicas: BTreeSet<String> = table.replica_regions.iter().cloned().collect();
    for update in updates {
        let target = update.region();
        if target.is_empty() {
            return Err(validation("Replica RegionName must not be empty"));
        }
        if let ReplicaUpdate::Disassociate { mode, .. } = update {
            return validate_disassociate(table, region, target, mode.as_deref(), updates.len());
        }
        if target == region {
            return Err(validation(format!(
                "Replica update for table: {name} names the region of the table itself: {region}"
            )));
        }
        match update {
            ReplicaUpdate::Create { .. } => {
                let exists = accounts
                    .regional(account, target)
                    .is_some_and(|s| s.tables.contains_key(name));
                if replicas.contains(target) || exists {
                    return Err(validation(format!(
                        "Failed to create a the new replica of table with name: '{name}' \
                         because one or more replicas already existed as tables."
                    )));
                }
                replicas.insert(target.to_string());
            }
            ReplicaUpdate::Update { .. } | ReplicaUpdate::Delete { .. } => {
                if !replicas.contains(target) {
                    return Err(validation(
                        "Replica specified in the Replica Update or Replica Delete action of \
                         the request was not found.",
                    ));
                }
                if matches!(update, ReplicaUpdate::Delete { .. }) {
                    replicas.remove(target);
                }
            }
            ReplicaUpdate::Disassociate { .. } => unreachable!("handled above"),
        }
    }
    Ok(())
}

/// A `Disassociate` replica update: it names the region the request is sent
/// to (the local replica), stands alone in `ReplicaUpdates`, uses the only
/// supported mode, and needs a replica of a version 2019.11.21 global table
/// whose disassociation protection is off.
fn validate_disassociate(
    table: &DynamoTable,
    region: &str,
    target: &str,
    mode: Option<&str>,
    update_count: usize,
) -> Result<(), AwsServiceError> {
    match mode {
        Some("WAIT_FOR_REPLICATION") => {}
        Some(other) => {
            return Err(validation(format!(
                "1 validation error detected: Value '{other}' at \
                 'replicaUpdates.1.member.disassociate.disassociationMode' failed to satisfy \
                 constraint: Member must satisfy enum value set: [WAIT_FOR_REPLICATION]"
            )));
        }
        None => {
            return Err(validation(
                "1 validation error detected: Value null at \
                 'replicaUpdates.1.member.disassociate.disassociationMode' failed to satisfy \
                 constraint: Member must not be null",
            ));
        }
    }
    if update_count > 1 {
        return Err(validation(
            "A Disassociate replica update cannot be combined with other replica updates",
        ));
    }
    if target != region {
        return Err(validation(format!(
            "Disassociate must name the Region of the request: {region}. To disassociate the \
             replica in {target}, send the request to {target}."
        )));
    }
    if table.replica_regions.is_empty() {
        return Err(validation(format!(
            "Table {} is not a replica of a global table",
            table.name
        )));
    }
    if table.disassociation_protection_enabled {
        return Err(validation(format!(
            "Replica {} in {region} has disassociation protection enabled. Disable \
             disassociation protection before disassociating the replica.",
            table.name
        )));
    }
    Ok(())
}

/// Apply validated `updates` to the table `name` in (`account`, `region`):
/// a Create builds the replica table in the target region from the table's
/// current schema, settings and rows; a Delete drops the replica table; and
/// every remaining member's replica list is refreshed. `kms_keys` holds the
/// KMS key resolved in each Create's region for a KMS-encrypted table.
pub(crate) fn apply_replica_updates(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
    updates: &[ReplicaUpdate],
    kms_keys: &HashMap<String, String>,
) {
    if updates.is_empty() {
        return;
    }
    let Some(source) = accounts
        .regional(account, region)
        .and_then(|s| s.tables.get(name))
        .cloned()
    else {
        return;
    };
    let mut members: BTreeSet<String> = source.replica_regions.iter().cloned().collect();
    members.insert(region.to_string());
    for update in updates {
        match update {
            ReplicaUpdate::Create {
                region: target,
                read_capacity_override,
                table_class,
                ..
            } => {
                let replica = new_replica(
                    &source,
                    account,
                    target,
                    kms_keys.get(target).cloned(),
                    *read_capacity_override,
                    table_class.clone(),
                );
                accounts
                    .regional_mut(account, target)
                    .tables
                    .insert(name.to_string(), replica);
                members.insert(target.clone());
            }
            ReplicaUpdate::Update {
                region: target,
                read_capacity_override,
                table_class,
            } => {
                if let Some(replica) = accounts
                    .regional_get_mut(account, target)
                    .and_then(|s| s.tables.get_mut(name))
                {
                    if let Some(rcu) = read_capacity_override {
                        replica.provisioned_throughput.read_capacity_units = *rcu;
                    }
                    if let Some(class) = table_class {
                        replica.table_class = class.clone();
                    }
                }
            }
            ReplicaUpdate::Delete { region: target } => {
                if let Some(state) = accounts.regional_get_mut(account, target) {
                    if let Some(table) = state.tables.remove(name) {
                        let prefix = format!("{}/stream/", table.arn);
                        state
                            .stream_policies
                            .retain(|arn, _| !arn.starts_with(&prefix));
                    }
                }
                members.remove(target);
            }
            ReplicaUpdate::Disassociate { region: target, .. } => {
                // Replication is synchronous here, so nothing is pending: the
                // replica leaves the group at once, keeping its rows, indexes
                // and settings, and the rest of the group carries on without
                // it. Protection is a replica setting: the standalone table
                // reports none and returns to the default.
                if let Some(table) = accounts
                    .regional_get_mut(account, target)
                    .and_then(|s| s.tables.get_mut(name))
                {
                    table.replica_regions.clear();
                    table.disassociation_protection_enabled = true;
                }
                members.remove(target);
            }
        }
    }
    set_replica_set(accounts, account, name, &members);
}

/// One replica a declarative caller (CloudFormation's
/// `AWS::DynamoDB::GlobalTable`) wants: its region and its own overrides.
#[derive(Debug, Clone, Default)]
pub struct ReplicaSpec {
    pub region: String,
    pub kms_master_key_id: Option<String>,
    pub read_capacity_units: Option<i64>,
    pub table_class: Option<String>,
}

/// Make the replica set of the table `name` in (`account`, `region`) exactly
/// `wanted` (the other regions; an entry for `region` itself is ignored):
/// missing replicas are created as real tables in their regions, replicas no
/// longer wanted are deleted, the overrides of the ones kept are applied, and
/// every replica takes the table's shared settings. Errors (unknown table, a same-named non-replica table in a target
/// region) leave every region untouched.
pub fn set_table_replicas(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
    wanted: &[ReplicaSpec],
) -> Result<(), String> {
    let current: BTreeSet<String> = accounts
        .regional(account, region)
        .and_then(|s| s.tables.get(name))
        .ok_or_else(|| format!("DynamoDB table {name} not found in {region}"))?
        .replica_regions
        .iter()
        .cloned()
        .collect();
    let wanted: Vec<&ReplicaSpec> = wanted.iter().filter(|r| r.region != region).collect();
    let mut updates = Vec::new();
    let mut kms_keys = HashMap::new();
    for spec in &wanted {
        if current.contains(&spec.region) {
            updates.push(ReplicaUpdate::Update {
                region: spec.region.clone(),
                read_capacity_override: spec.read_capacity_units,
                table_class: spec.table_class.clone(),
            });
        } else {
            if let Some(key) = &spec.kms_master_key_id {
                kms_keys.insert(spec.region.clone(), key.clone());
            }
            updates.push(ReplicaUpdate::Create {
                region: spec.region.clone(),
                kms_master_key_id: spec.kms_master_key_id.clone(),
                read_capacity_override: spec.read_capacity_units,
                table_class: spec.table_class.clone(),
            });
        }
    }
    for gone in current
        .iter()
        .filter(|r| !wanted.iter().any(|w| &w.region == *r))
    {
        updates.push(ReplicaUpdate::Delete {
            region: gone.clone(),
        });
    }
    validate_replica_updates(accounts, account, region, name, &updates).map_err(|e| e.message())?;
    apply_replica_updates(accounts, account, region, name, &updates, &kms_keys);
    // The kept replicas pick up the table's current shared settings.
    sync_replica_settings(accounts, account, region, name);
    Ok(())
}

/// Copy the settings a version 2019.11.21 global table keeps in step across
/// its replicas from the table `name` in (`account`, `region`) to each of its
/// replicas: billing mode and capacity, on-demand throughput, attribute
/// definitions, global secondary indexes, the stream specification (a
/// replica whose stream turns on gets a stream ARN of its own region) and
/// the TTL setting. Per-replica settings -- table class, encryption key,
/// deletion protection, tags, resource policy, PITR, contributor insights,
/// Kinesis destinations -- stay each replica's own.
pub(crate) fn sync_replica_settings(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
) {
    let Some(source) = accounts
        .regional(account, region)
        .and_then(|s| s.tables.get(name))
        .filter(|t| !t.replica_regions.is_empty())
    else {
        return;
    };
    // The settings only -- never the rows.
    let source = SyncedSettings {
        replica_regions: source.replica_regions.clone(),
        billing_mode: source.billing_mode.clone(),
        provisioned_throughput: source.provisioned_throughput.clone(),
        on_demand_throughput: source.on_demand_throughput.clone(),
        attribute_definitions: source.attribute_definitions.clone(),
        gsi: source.gsi.clone(),
        stream_enabled: source.stream_enabled,
        stream_view_type: source.stream_view_type.clone(),
        ttl_attribute: source.ttl_attribute.clone(),
        ttl_enabled: source.ttl_enabled,
    };
    for peer in &source.replica_regions {
        let Some(replica) = accounts
            .regional_get_mut(account, peer)
            .and_then(|s| s.tables.get_mut(name))
        else {
            continue;
        };
        replica.billing_mode = source.billing_mode.clone();
        replica.provisioned_throughput = source.provisioned_throughput.clone();
        replica.on_demand_throughput = source.on_demand_throughput.clone();
        replica.attribute_definitions = source.attribute_definitions.clone();
        replica.gsi = source.gsi.clone();
        replica.stream_enabled = source.stream_enabled;
        replica.stream_view_type = source.stream_view_type.clone();
        if source.stream_enabled && replica.stream_arn.is_none() {
            replica.stream_arn = Some(format!(
                "{}/stream/{}",
                replica.arn,
                Utc::now().format("%Y-%m-%dT%H:%M:%S.%3f")
            ));
        }
        replica.ttl_attribute = source.ttl_attribute.clone();
        replica.ttl_enabled = source.ttl_enabled;
    }
}

struct SyncedSettings {
    replica_regions: Vec<String>,
    billing_mode: String,
    provisioned_throughput: crate::state::ProvisionedThroughput,
    on_demand_throughput: Option<crate::state::OnDemandThroughput>,
    attribute_definitions: Vec<crate::state::AttributeDefinition>,
    gsi: Vec<crate::state::GlobalSecondaryIndex>,
    stream_enabled: bool,
    stream_view_type: Option<String>,
    ttl_attribute: Option<String>,
    ttl_enabled: bool,
}

/// Record `members` as the replica set of the table `name`: each member's
/// table lists every other member, and a table left alone is single-region.
pub(crate) fn set_replica_set(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    name: &str,
    members: &BTreeSet<String>,
) {
    for member in members {
        if let Some(table) = accounts
            .regional_get_mut(account, member)
            .and_then(|s| s.tables.get_mut(name))
        {
            table.replica_regions = members.iter().filter(|r| *r != member).cloned().collect();
        }
    }
}

/// Drop the table `name` of `region` from the replica set it belongs to,
/// once that table has been deleted.
pub(crate) fn leave_replica_set(
    accounts: &mut MultiRegionState<DynamoDbState>,
    account: &str,
    region: &str,
    name: &str,
    former_replicas: &[String],
) {
    let members: BTreeSet<String> = former_replicas
        .iter()
        .filter(|r| *r != region)
        .cloned()
        .collect();
    set_replica_set(accounts, account, name, &members);
}

/// A new replica of `source` in `region`: the same schema, indexes, billing,
/// stream, TTL and encryption settings and the same rows, with an identity of
/// its own (ARN, table id, stream ARN) and none of the per-replica settings
/// (tags, resource policy, PITR, Kinesis destinations, contributor insights,
/// deletion protection) carried over.
fn new_replica(
    source: &DynamoTable,
    account: &str,
    region: &str,
    kms_key: Option<String>,
    read_capacity_override: Option<i64>,
    table_class: Option<String>,
) -> DynamoTable {
    let now = Utc::now();
    let mut table = source.clone();
    table.arn = crate::state::table_arn(region, account, &source.name);
    table.table_id = uuid::Uuid::new_v4().to_string().replace('-', "");
    table.created_at = now;
    table.status = "ACTIVE".to_string();
    table.tags.clear();
    table.resource_policy = None;
    table.pitr_enabled = false;
    table.pitr_history = Default::default();
    table.kinesis_destinations.clear();
    table.contributor_insights_status = "DISABLED".to_string();
    table.contributor_insights_counters.clear();
    table.deletion_protection_enabled = false;
    table.disassociation_protection_enabled = true;
    table.stream_records = std::sync::Arc::new(parking_lot::RwLock::new(Vec::new()));
    table.stream_arn = source.stream_enabled.then(|| {
        format!(
            "{}/stream/{}",
            table.arn,
            now.format("%Y-%m-%dT%H:%M:%S.%3f")
        )
    });
    if table.sse_type.as_deref() == Some("KMS") {
        table.sse_kms_key_arn = kms_key;
    }
    if let Some(rcu) = read_capacity_override {
        table.provisioned_throughput.read_capacity_units = rcu;
    }
    if let Some(class) = table_class {
        table.table_class = class;
    }
    for index in &mut table.vector_indexes {
        index.index_arn = index.index_arn.replacen(&source.arn, &table.arn, 1);
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{KeySchemaElement, ProvisionedThroughput};
    use serde_json::json;

    const ACCOUNT: &str = "123456789012";

    fn table(region: &str) -> DynamoTable {
        let mut t = DynamoTable::new(
            "T".to_string(),
            crate::state::table_arn(region, ACCOUNT, "T"),
            "id".to_string(),
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            vec![],
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            },
            "PAY_PER_REQUEST".to_string(),
            Utc::now(),
        );
        t.stream_enabled = true;
        t.stream_view_type = Some("NEW_AND_OLD_IMAGES".to_string());
        t.stream_arn = Some(format!("{}/stream/x", t.arn));
        t
    }

    fn row(pk: &str, v: &str) -> Item {
        HashMap::from([
            ("pk".to_string(), json!({"S": pk})),
            ("v".to_string(), json!({"S": v})),
        ])
    }

    fn accounts() -> MultiRegionState<DynamoDbState> {
        let mut accounts = MultiRegionState::<DynamoDbState>::new(ACCOUNT, "us-east-1", "");
        let mut t = table("us-east-1");
        t.put_item_at_key(row("a", "1"));
        t.tags.insert("team".into(), "x".into());
        accounts
            .regional_mut(ACCOUNT, "us-east-1")
            .tables
            .insert("T".into(), t);
        accounts
    }

    /// Apply `f` to the table of `region` with its writes tracked, then
    /// replicate exactly those writes, as the service does for a request.
    fn tracked_write(
        accounts: &mut MultiRegionState<DynamoDbState>,
        region: &str,
        f: impl FnOnce(&mut DynamoTable),
    ) {
        let t = accounts
            .regional_get_mut(ACCOUNT, region)
            .unwrap()
            .tables
            .get_mut("T")
            .unwrap();
        t.track_changes();
        f(t);
        let keys = t.finish_tracking();
        replicate_keys(accounts, ACCOUNT, region, "T", &keys);
    }

    fn create(region: &str) -> ReplicaUpdate {
        ReplicaUpdate::Create {
            region: region.to_string(),
            kms_master_key_id: None,
            read_capacity_override: None,
            table_class: None,
        }
    }

    #[test]
    fn a_created_replica_is_a_table_of_its_own_region() {
        let mut accounts = accounts();
        let updates = [create("eu-west-1")];
        validate_replica_updates(&accounts, ACCOUNT, "us-east-1", "T", &updates).unwrap();
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &updates,
            &HashMap::new(),
        );
        let replica = &accounts.regional(ACCOUNT, "eu-west-1").unwrap().tables["T"];
        assert_eq!(
            replica.arn,
            "arn:aws:dynamodb:eu-west-1:123456789012:table/T"
        );
        assert!(replica
            .stream_arn
            .as_deref()
            .unwrap()
            .starts_with("arn:aws:dynamodb:eu-west-1:123456789012:table/T/stream/"));
        assert_eq!(replica.item_count, 1);
        assert!(replica.tags.is_empty());
        assert!(replica.stream_records.read().is_empty());
        assert_eq!(replica.replica_regions, vec!["us-east-1".to_string()]);
        let source = &accounts.regional(ACCOUNT, "us-east-1").unwrap().tables["T"];
        assert_eq!(source.replica_regions, vec!["eu-west-1".to_string()]);
        // A second Create of the same region is refused.
        let err = validate_replica_updates(&accounts, ACCOUNT, "us-east-1", "T", &updates)
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn writes_replicate_to_every_replica_with_stream_records() {
        let mut accounts = accounts();
        let updates = [create("eu-west-1"), create("ap-south-1")];
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &updates,
            &HashMap::new(),
        );
        // A write in eu-west-1 reaches us-east-1 and ap-south-1.
        tracked_write(&mut accounts, "eu-west-1", |t| {
            t.put_item_at_key(row("a", "2"));
            t.put_item_at_key(row("b", "1"));
        });
        for region in ["us-east-1", "ap-south-1"] {
            let t = &accounts.regional(ACCOUNT, region).unwrap().tables["T"];
            assert_eq!(t.item_count, 2, "{region}");
            let id = t.find_item_index(&row("a", "")).unwrap();
            assert_eq!(t.items()[id]["v"], json!({"S": "2"}));
            let events: Vec<String> = t
                .stream_records
                .read()
                .iter()
                .map(|r| format!("{}:{}", r.event_name, r.aws_region))
                .collect();
            assert_eq!(
                events,
                vec![format!("MODIFY:{region}"), format!("INSERT:{region}")],
                "{region}"
            );
        }
        // A delete replicates as a REMOVE.
        tracked_write(&mut accounts, "us-east-1", |t| {
            t.remove_item_by_key(&HashMap::from([("pk".to_string(), json!({"S": "b"}))]));
        });
        let t = &accounts.regional(ACCOUNT, "eu-west-1").unwrap().tables["T"];
        assert_eq!(t.item_count, 1);
        assert_eq!(t.stream_records.read().last().unwrap().event_name, "REMOVE");
    }

    #[test]
    fn deleting_a_replica_drops_its_table_and_updates_the_set() {
        let mut accounts = accounts();
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &[create("eu-west-1"), create("ap-south-1")],
            &HashMap::new(),
        );
        let delete = [ReplicaUpdate::Delete {
            region: "eu-west-1".to_string(),
        }];
        validate_replica_updates(&accounts, ACCOUNT, "us-east-1", "T", &delete).unwrap();
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &delete,
            &HashMap::new(),
        );
        assert!(!accounts
            .regional(ACCOUNT, "eu-west-1")
            .unwrap()
            .tables
            .contains_key("T"));
        assert_eq!(
            accounts.regional(ACCOUNT, "us-east-1").unwrap().tables["T"].replica_regions,
            vec!["ap-south-1".to_string()]
        );
        assert_eq!(
            accounts.regional(ACCOUNT, "ap-south-1").unwrap().tables["T"].replica_regions,
            vec!["us-east-1".to_string()]
        );
        // Deleting a region that holds no replica is refused.
        let err = validate_replica_updates(&accounts, ACCOUNT, "us-east-1", "T", &delete)
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn legacy_global_tables_are_seen_from_every_replica_region_and_replicate() {
        let mut accounts = accounts();
        accounts
            .regional_mut(ACCOUNT, "eu-west-1")
            .tables
            .insert("T".into(), table("eu-west-1"));
        accounts
            .regional_mut(ACCOUNT, "us-east-1")
            .global_tables
            .insert(
                "T".into(),
                crate::state::GlobalTableDescription {
                    global_table_name: "T".into(),
                    global_table_arn: crate::state::global_table_arn("us-east-1", ACCOUNT, "T"),
                    global_table_status: "ACTIVE".into(),
                    creation_date: Utc::now(),
                    replication_group: ["us-east-1", "eu-west-1"]
                        .iter()
                        .map(|r| crate::state::ReplicaDescription {
                            region_name: r.to_string(),
                            replica_status: "ACTIVE".into(),
                            read_capacity_auto_scaling: None,
                            write_capacity_auto_scaling: None,
                            read_capacity_units: None,
                        })
                        .collect(),
                    billing_mode: "PROVISIONED".into(),
                    provisioned_write_capacity_units: None,
                },
            );
        assert_eq!(
            global_table_region(&accounts, ACCOUNT, "eu-west-1", "T").as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            global_table_region(&accounts, ACCOUNT, "ap-south-1", "T"),
            None
        );
        assert_eq!(
            replica_peers(&accounts, ACCOUNT, "eu-west-1", "T"),
            vec!["us-east-1".to_string()]
        );
        tracked_write(&mut accounts, "us-east-1", |t| {
            t.put_item_at_key(row("a", "legacy"));
        });
        assert_eq!(
            accounts.regional(ACCOUNT, "eu-west-1").unwrap().tables["T"].item_count,
            1
        );
    }

    /// Writes to different rows in different replicas that land before
    /// either replicates must both survive: replication copies the rows a
    /// write touched, never the whole table.
    #[test]
    fn concurrent_writes_in_two_replicas_both_survive() {
        let mut accounts = accounts();
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &[create("eu-west-1")],
            &HashMap::new(),
        );
        // Both writes land locally first...
        let mut pending = Vec::new();
        for (region, pk) in [("us-east-1", "east"), ("eu-west-1", "west")] {
            let t = accounts
                .regional_get_mut(ACCOUNT, region)
                .unwrap()
                .tables
                .get_mut("T")
                .unwrap();
            t.track_changes();
            t.put_item_at_key(row(pk, pk));
            pending.push((region, t.finish_tracking()));
        }
        // ...then each replicates.
        for (region, keys) in pending {
            replicate_keys(&mut accounts, ACCOUNT, region, "T", &keys);
        }
        for region in ["us-east-1", "eu-west-1"] {
            let t = &accounts.regional(ACCOUNT, region).unwrap().tables["T"];
            for pk in ["a", "east", "west"] {
                assert!(
                    t.find_item_index(&row(pk, "")).is_some(),
                    "{region} lost {pk}"
                );
            }
        }
    }

    /// Each replica keeps its own point-in-time history: a new replica
    /// starts without the source's, and a replicated write is recorded in
    /// the receiving replica's history when its recovery is on.
    #[test]
    fn replicated_writes_land_in_each_replicas_own_pitr_history() {
        let mut accounts = accounts();
        accounts
            .regional_get_mut(ACCOUNT, "us-east-1")
            .unwrap()
            .tables
            .get_mut("T")
            .unwrap()
            .set_pitr(true);
        tracked_write(&mut accounts, "us-east-1", |t| {
            t.put_item_at_key(row("before", "1"));
        });
        apply_replica_updates(
            &mut accounts,
            ACCOUNT,
            "us-east-1",
            "T",
            &[create("eu-west-1")],
            &HashMap::new(),
        );
        {
            let replica = accounts
                .regional_get_mut(ACCOUNT, "eu-west-1")
                .unwrap()
                .tables
                .get_mut("T")
                .unwrap();
            assert!(!replica.pitr_enabled);
            assert!(replica.pitr_history.changes.is_empty());
            replica.set_pitr(true);
        }
        tracked_write(&mut accounts, "us-east-1", |t| {
            t.put_item_at_key(row("after", "1"));
        });
        let replica = &accounts.regional(ACCOUNT, "eu-west-1").unwrap().tables["T"];
        let keys: Vec<_> = replica
            .pitr_history
            .changes
            .iter()
            .map(|c| c.keys["pk"].clone())
            .collect();
        assert_eq!(keys, vec![json!({"S": "after"})]);
        let source = &accounts.regional(ACCOUNT, "us-east-1").unwrap().tables["T"];
        assert_eq!(source.pitr_history.changes.len(), 2);
    }

    /// Untracked writes are not recorded, so nothing is replicated for them.
    #[test]
    fn only_tracked_writes_are_recorded() {
        let mut t = table("us-east-1");
        t.put_item_at_key(row("x", "1"));
        t.track_changes();
        t.put_item_at_key(row("y", "1"));
        let keys = t.finish_tracking();
        assert_eq!(
            keys,
            vec![HashMap::from([("pk".to_string(), json!({"S": "y"}))])]
        );
        t.put_item_at_key(row("z", "1"));
        t.track_changes();
        assert!(t.finish_tracking().is_empty());
    }
}
