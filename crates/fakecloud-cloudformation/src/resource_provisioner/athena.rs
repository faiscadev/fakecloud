//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `athena`.

use super::*;

impl ResourceProvisioner {
    // --- Athena ---

    pub(super) fn create_athena_work_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let configuration = props.get("Configuration").cloned();
        let state_str = props
            .get("State")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLED");
        let tags = Self::parse_athena_tags(props.get("Tags"));

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if account.work_groups.contains_key(&name) {
            return Err(format!("WorkGroup {name} already exists"));
        }
        let wg = WorkGroup {
            name: name.clone(),
            state: state_str.to_string(),
            description,
            configuration,
            creation_time: Utc::now(),
            engine_version: Some("AUTO".to_string()),
        };
        let arn = format!(
            "arn:aws:athena:{}:{}:workgroup/{}",
            self.region, self.account_id, name
        );
        account.work_groups.insert(name.clone(), wg);
        if !tags.is_empty() {
            account.tags.insert(arn.clone(), tags);
        }
        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(super) fn delete_athena_work_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.work_groups.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_athena_work_group(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let wg = account.work_groups.get(physical_id)?;
        match attribute {
            "Arn" => Some(format!(
                "arn:aws:athena:{}:{}:workgroup/{}",
                self.region, self.account_id, wg.name
            )),
            "Name" => Some(wg.name.clone()),
            _ => None,
        }
    }

    pub(super) fn create_athena_data_catalog(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let cat_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or("Type is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let parameters = props
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let connection_type = props
            .get("ConnectionType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = Self::parse_athena_tags(props.get("Tags"));

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if account.data_catalogs.contains_key(&name) {
            return Err(format!("DataCatalog {name} already exists"));
        }
        let cat = DataCatalog {
            name: name.clone(),
            description,
            cat_type,
            parameters,
            status: "CREATE_COMPLETE".to_string(),
            connection_type,
            error: None,
        };
        let arn = format!(
            "arn:aws:athena:{}:{}:datacatalog/{}",
            self.region, self.account_id, name
        );
        account.data_catalogs.insert(name.clone(), cat);
        if !tags.is_empty() {
            account.tags.insert(arn.clone(), tags);
        }
        Ok(ProvisionResult::new(name.clone())
            .with("Arn", arn)
            .with("Name", name))
    }

    pub(super) fn delete_athena_data_catalog(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.data_catalogs.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_athena_data_catalog(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let cat = account.data_catalogs.get(physical_id)?;
        match attribute {
            "Arn" => Some(format!(
                "arn:aws:athena:{}:{}:datacatalog/{}",
                self.region, self.account_id, cat.name
            )),
            "Name" => Some(cat.name.clone()),
            _ => None,
        }
    }

    pub(super) fn create_athena_named_query(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let database = props
            .get("Database")
            .and_then(|v| v.as_str())
            .ok_or("Database is required")?
            .to_string();
        let query_string = props
            .get("QueryString")
            .and_then(|v| v.as_str())
            .ok_or("QueryString is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let work_group = props
            .get("WorkGroup")
            .and_then(|v| v.as_str())
            .unwrap_or("primary")
            .to_string();

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if !account.work_groups.contains_key(&work_group) {
            return Err(format!("Workgroup {work_group} not found"));
        }
        let id = Uuid::new_v4().to_string();
        let nq = NamedQuery {
            named_query_id: id.clone(),
            name,
            description,
            database,
            query_string,
            work_group,
            last_used_at: None,
        };
        account.named_queries.insert(id.clone(), nq);
        Ok(ProvisionResult::new(id.clone()).with("NamedQueryId", id))
    }

    pub(super) fn delete_athena_named_query(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.named_queries.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_athena_named_query(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let nq = account.named_queries.get(physical_id)?;
        match attribute {
            "NamedQueryId" => Some(nq.named_query_id.clone()),
            _ => None,
        }
    }

    pub(super) fn create_athena_prepared_statement(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let statement_name = props
            .get("StatementName")
            .and_then(|v| v.as_str())
            .ok_or("StatementName is required")?
            .to_string();
        let work_group_name = props
            .get("WorkGroupName")
            .and_then(|v| v.as_str())
            .ok_or("WorkGroupName is required")?
            .to_string();
        let query_statement = props
            .get("QueryStatement")
            .and_then(|v| v.as_str())
            .ok_or("QueryStatement is required")?
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        account.ensure_initialized();
        if !account.work_groups.contains_key(&work_group_name) {
            return Err(format!("Workgroup {work_group_name} not found"));
        }
        let key = (work_group_name.clone(), statement_name.clone());
        if account.prepared_statements.contains_key(&key) {
            return Err(format!(
                "PreparedStatement {statement_name} already exists in {work_group_name}"
            ));
        }
        let ps = PreparedStatement {
            statement_name: statement_name.clone(),
            work_group_name: work_group_name.clone(),
            query_statement,
            description,
            last_modified_time: Utc::now(),
        };
        let physical_id = format!("{work_group_name}|{statement_name}");
        account.prepared_statements.insert(key, ps);
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_athena_prepared_statement(
        &self,
        physical_id: &str,
        _attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid PreparedStatement physical id: {physical_id}"
            ));
        }
        let key = (parts[0].to_string(), parts[1].to_string());
        account.prepared_statements.remove(&key);
        Ok(())
    }

    pub(super) fn get_att_athena_prepared_statement(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.athena_state.write();
        let account = accounts
            .accounts
            .entry(self.account_id.clone())
            .or_default();
        let parts: Vec<&str> = physical_id.split('|').collect();
        if parts.len() != 2 {
            return None;
        }
        let ps = account
            .prepared_statements
            .get(&(parts[0].to_string(), parts[1].to_string()))?;
        match attribute {
            "StatementName" => Some(ps.statement_name.clone()),
            "WorkGroupName" => Some(ps.work_group_name.clone()),
            _ => None,
        }
    }
}
