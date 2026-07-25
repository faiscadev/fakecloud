//! `AWS::Timestream::Database` and `AWS::Timestream::Table` CloudFormation
//! provisioning. Each is written through to the `timestream` service state as the
//! same typed `Database` / `Table` record the direct `CreateDatabase` /
//! `CreateTable` handlers store, so a CFN-created resource reads back on
//! `DescribeDatabase` / `DescribeTable` and persists through the `timestream`
//! snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the database name (Database) / the `<db>|<table>` composite
//! (Table). A table is keyed by `table_key(db, table)` and its physical id
//! encodes `<db>|<table>` so delete / `Fn::GetAtt` reach the nested record.

use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};
use fakecloud_timestream::shared::{database_arn, now_epoch, table_arn, table_key};
use fakecloud_timestream::state::{Database, Table};

impl ResourceProvisioner {
    // ------------------------------------------------------------- Database

    pub(super) fn create_timestream_database(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DatabaseName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = database_arn(region, account, &name);
        let now = now_epoch();
        let kms = props
            .get("KmsKeyId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                Some(format!(
                    "arn:aws:kms:{region}:{account}:key/timestream-default"
                ))
            });

        let db = Database {
            name: name.clone(),
            arn: arn.clone(),
            kms_key_id: kms,
            table_count: 0,
            creation_time: now,
            last_updated_time: now,
        };

        let mut guard = self.timestream_state.write();
        let data = guard.get_or_create(account);
        if data.databases.contains_key(&name) {
            return Err(format!("Database {name} already exists"));
        }
        data.databases.insert(name.clone(), db);
        let tags = string_tag_map(props.get("Tags"));
        if !tags.is_empty() {
            data.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("DatabaseName", name))
    }

    pub(super) fn delete_timestream_database(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.timestream_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(db) = data.databases.remove(physical_id) {
            data.tags.remove(&db.arn);
        }
        Ok(())
    }

    pub(super) fn get_att_timestream_database(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.timestream_state.read();
        let data = guard.get(&self.account_id)?;
        let db = data.databases.get(physical_id)?;
        match attribute {
            "Arn" => Some(db.arn.clone()),
            "DatabaseName" => Some(db.name.clone()),
            _ => None,
        }
    }

    // ---------------------------------------------------------------- Table

    pub(super) fn create_timestream_table(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let database = props
            .get("DatabaseName")
            .and_then(Value::as_str)
            .ok_or("AWS::Timestream::Table requires DatabaseName")?
            .to_string();
        let table = props
            .get("TableName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = table_arn(region, account, &database, &table);
        let key = table_key(&database, &table);
        let now = now_epoch();

        let record = Table {
            name: table.clone(),
            database_name: database.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            retention_properties: props
                .get("RetentionProperties")
                .cloned()
                .unwrap_or_else(default_retention),
            magnetic_store_write_properties: props
                .get("MagneticStoreWriteProperties")
                .filter(|v| !v.is_null())
                .cloned(),
            schema: props.get("Schema").cloned().unwrap_or(Value::Null),
            creation_time: now,
            last_updated_time: now,
        };

        let mut guard = self.timestream_state.write();
        let data = guard.get_or_create(account);
        if !data.databases.contains_key(&database) {
            return Err(format!("Database {database} not yet provisioned"));
        }
        if data.tables.contains_key(&key) {
            return Err(format!("Table {table} already exists"));
        }
        data.tables.insert(key.clone(), record);
        data.records.entry(key).or_default();
        if let Some(db) = data.databases.get_mut(&database) {
            db.table_count += 1;
            db.last_updated_time = now;
        }
        let tags = string_tag_map(props.get("Tags"));
        if !tags.is_empty() {
            data.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(format!("{database}|{table}"))
            .with("Arn", arn)
            .with("Name", table))
    }

    pub(super) fn delete_timestream_table(&self, physical_id: &str) -> Result<(), String> {
        let Some((database, table)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let key = table_key(database, table);
        let mut guard = self.timestream_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(t) = data.tables.remove(&key) {
            data.records.remove(&key);
            data.tags.remove(&t.arn);
            if let Some(db) = data.databases.get_mut(database) {
                db.table_count = (db.table_count - 1).max(0);
            }
        }
        Ok(())
    }

    /// In-place `UpdateTable`: mutate the table's editable properties while
    /// preserving every ingested record. Real AWS `UpdateTable`
    /// (RetentionProperties / MagneticStoreWriteProperties / Schema / Tags) is
    /// applied in place; the previous reprovision fallback deleted the table's
    /// entire `records` set on a benign retention/tag change.
    pub(super) fn update_timestream_table(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // A changed DatabaseName/TableName is a replacement (reprovision path);
        // an in-place update keeps the existing physical id and its records.
        let physical_id = existing.physical_id.clone();
        let Some((database, table)) = physical_id.split_once('|') else {
            return Err(format!(
                "Invalid Timestream table physical id: {physical_id}"
            ));
        };
        let key = table_key(database, table);

        let mut guard = self.timestream_state.write();
        let data = guard.get_or_create(&self.account_id);
        let arn = {
            let record = data
                .tables
                .get_mut(&key)
                .ok_or_else(|| format!("Table {table} not found"))?;
            if let Some(rp) = props.get("RetentionProperties").filter(|v| !v.is_null()) {
                record.retention_properties = rp.clone();
            }
            record.magnetic_store_write_properties = props
                .get("MagneticStoreWriteProperties")
                .filter(|v| !v.is_null())
                .cloned();
            if let Some(schema) = props.get("Schema").filter(|v| !v.is_null()) {
                record.schema = schema.clone();
            }
            record.last_updated_time = now_epoch();
            record.arn.clone()
        };
        // Tags are replaced wholesale on update, matching CFN tag semantics.
        let tags = string_tag_map(props.get("Tags"));
        if tags.is_empty() {
            data.tags.remove(&arn);
        } else {
            data.tags.insert(arn.clone(), tags);
        }
        // data.records[key] intentionally left untouched.
        Ok(ProvisionResult::new(physical_id.clone())
            .with("Arn", arn)
            .with("Name", table.to_string()))
    }

    pub(super) fn get_att_timestream_table(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (database, table) = physical_id.split_once('|')?;
        let key = table_key(database, table);
        let guard = self.timestream_state.read();
        let data = guard.get(&self.account_id)?;
        let t = data.tables.get(&key)?;
        match attribute {
            "Arn" => Some(t.arn.clone()),
            "Name" => Some(t.name.clone()),
            _ => None,
        }
    }
}

/// Default retention when the template omits `RetentionProperties`.
fn default_retention() -> Value {
    json!({
        "MemoryStoreRetentionPeriodInHours": "24",
        "MagneticStoreRetentionPeriodInDays": "73000",
    })
}

fn string_tag_map(value: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = value.and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}
