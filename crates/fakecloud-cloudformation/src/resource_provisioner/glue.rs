//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `glue`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn create_glue_database(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let input = props
            .get("DatabaseInput")
            .ok_or("DatabaseInput is required")?;
        let name = input
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = input
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let location_uri = input
            .get("LocationUri")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let parameters = input
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        if dbs.contains_key(&name) {
            return Err(format!("Database {name} already exists"));
        }
        dbs.insert(
            name.clone(),
            fakecloud_glue::Database {
                name: name.clone(),
                description,
                location_uri,
                parameters,
                created_at: Utc::now(),
                catalog_id: self.account_id.clone(),
                tables: BTreeMap::new(),
            },
        );
        Ok(ProvisionResult::new(name.clone()))
    }

    pub(super) fn delete_glue_database(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        state.dbs_in_mut(&self.region).remove(physical_id);
        Ok(())
    }

    pub(super) fn create_glue_table(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let db_name = props
            .get("DatabaseName")
            .and_then(|v| v.as_str())
            .ok_or("DatabaseName is required")?
            .to_string();
        let input = props.get("TableInput").ok_or("TableInput is required")?;
        let name = input
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("TableInput.Name is required")?
            .to_string();
        let now = Utc::now();

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| format!("Database {db_name} not found"))?;
        if db.tables.contains_key(&name) {
            return Err(format!("Table {name} already exists"));
        }
        db.tables.insert(
            name.clone(),
            fakecloud_glue::Table {
                name: name.clone(),
                database_name: db_name.clone(),
                catalog_id: self.account_id.clone(),
                description: input
                    .get("Description")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                owner: input
                    .get("Owner")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                create_time: now,
                update_time: now,
                last_access_time: None,
                retention: input.get("Retention").and_then(|v| v.as_i64()).unwrap_or(0),
                storage_descriptor: None,
                partition_keys: Vec::new(),
                view_original_text: input
                    .get("ViewOriginalText")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                view_expanded_text: input
                    .get("ViewExpandedText")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                table_type: input
                    .get("TableType")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                parameters: BTreeMap::new(),
                partitions: BTreeMap::new(),
            },
        );
        let physical_id = format!("{db_name}|{name}");
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_glue_table(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid Glue table physical id: {physical_id}"));
        }
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(parts[0])
            .ok_or_else(|| format!("Database {} not found", parts[0]))?;
        db.tables.remove(parts[1]);
        Ok(())
    }

    pub(super) fn create_glue_partition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let db_name = props
            .get("DatabaseName")
            .and_then(|v| v.as_str())
            .ok_or("DatabaseName is required")?
            .to_string();
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .ok_or("TableName is required")?
            .to_string();
        let input = props
            .get("PartitionInput")
            .ok_or("PartitionInput is required")?;
        let values: Vec<String> = input
            .get("Values")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if values.is_empty() {
            return Err("PartitionInput.Values is required".to_string());
        }
        let key = values
            .iter()
            .map(|v| {
                v.replace('%', "%25")
                    .replace('|', "%7C")
                    .replace('/', "%2F")
            })
            .collect::<Vec<_>>()
            .join("/");

        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(&db_name)
            .ok_or_else(|| format!("Database {db_name} not found"))?;
        let table = db
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| format!("Table {table_name} not found"))?;
        if table.partitions.contains_key(&key) {
            return Err(format!("Partition {key} already exists"));
        }
        table.partitions.insert(
            key.clone(),
            fakecloud_glue::Partition {
                values: values.clone(),
                catalog_id: self.account_id.clone(),
                database_name: db_name.clone(),
                table_name: table_name.clone(),
                create_time: Utc::now(),
                last_access_time: None,
                storage_descriptor: None,
                parameters: BTreeMap::new(),
            },
        );
        let physical_id = format!("{db_name}|{table_name}|{key}");
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_glue_partition(
        &self,
        physical_id: &str,
        _attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut accounts = self.glue_state.write();
        let state = accounts.get_or_create(&self.account_id, &self.region);
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid Glue partition physical id: {physical_id}"));
        }
        let dbs = state.dbs_in_mut(&self.region);
        let db = dbs
            .get_mut(parts[0])
            .ok_or_else(|| format!("Database {} not found", parts[0]))?;
        let table = db
            .tables
            .get_mut(parts[1])
            .ok_or_else(|| format!("Table {} not found", parts[1]))?;
        table.partitions.remove(parts[2]);
        Ok(())
    }
}
