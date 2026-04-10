use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;
use uuid::Uuid;

pub type SharedRdsState = Arc<RwLock<RdsState>>;

#[derive(Clone)]
pub struct DbInstance {
    pub db_instance_identifier: String,
    pub db_instance_arn: String,
    pub db_instance_class: String,
    pub engine: String,
    pub engine_version: String,
    pub db_instance_status: String,
    pub master_username: String,
    pub db_name: Option<String>,
    pub endpoint_address: String,
    pub port: i32,
    pub allocated_storage: i32,
    pub publicly_accessible: bool,
    pub deletion_protection: bool,
    pub created_at: DateTime<Utc>,
    pub dbi_resource_id: String,
    pub master_user_password: String,
    pub container_id: String,
    pub host_port: u16,
    pub tags: Vec<RdsTag>,
    pub read_replica_source_db_instance_identifier: Option<String>,
    pub read_replica_db_instance_identifiers: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
}

impl fmt::Debug for DbInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbInstance")
            .field("db_instance_identifier", &self.db_instance_identifier)
            .field("db_instance_arn", &self.db_instance_arn)
            .field("db_instance_class", &self.db_instance_class)
            .field("engine", &self.engine)
            .field("engine_version", &self.engine_version)
            .field("db_instance_status", &self.db_instance_status)
            .field("master_username", &self.master_username)
            .field("db_name", &self.db_name)
            .field("endpoint_address", &self.endpoint_address)
            .field("port", &self.port)
            .field("allocated_storage", &self.allocated_storage)
            .field("publicly_accessible", &self.publicly_accessible)
            .field("deletion_protection", &self.deletion_protection)
            .field("created_at", &self.created_at)
            .field("dbi_resource_id", &self.dbi_resource_id)
            .field("master_user_password", &"<redacted>")
            .field("container_id", &self.container_id)
            .field("host_port", &self.host_port)
            .field("tags", &self.tags)
            .field(
                "read_replica_source_db_instance_identifier",
                &self.read_replica_source_db_instance_identifier,
            )
            .field(
                "read_replica_db_instance_identifiers",
                &self.read_replica_db_instance_identifiers,
            )
            .field("vpc_security_group_ids", &self.vpc_security_group_ids)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdsTag {
    pub key: String,
    pub value: String,
}

#[derive(Clone)]
pub struct DbSnapshot {
    pub db_snapshot_identifier: String,
    pub db_snapshot_arn: String,
    pub db_instance_identifier: String,
    pub snapshot_create_time: DateTime<Utc>,
    pub engine: String,
    pub engine_version: String,
    pub allocated_storage: i32,
    pub status: String,
    pub port: i32,
    pub master_username: String,
    pub db_name: Option<String>,
    pub dbi_resource_id: String,
    pub snapshot_type: String,
    pub master_user_password: String,
    pub tags: Vec<RdsTag>,
    pub dump_data: Vec<u8>,
}

impl fmt::Debug for DbSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbSnapshot")
            .field("db_snapshot_identifier", &self.db_snapshot_identifier)
            .field("db_snapshot_arn", &self.db_snapshot_arn)
            .field("db_instance_identifier", &self.db_instance_identifier)
            .field("snapshot_create_time", &self.snapshot_create_time)
            .field("engine", &self.engine)
            .field("engine_version", &self.engine_version)
            .field("allocated_storage", &self.allocated_storage)
            .field("status", &self.status)
            .field("port", &self.port)
            .field("master_username", &self.master_username)
            .field("db_name", &self.db_name)
            .field("dbi_resource_id", &self.dbi_resource_id)
            .field("snapshot_type", &self.snapshot_type)
            .field("master_user_password", &"<redacted>")
            .field("tags", &self.tags)
            .field("dump_data", &format!("<{} bytes>", self.dump_data.len()))
            .finish()
    }
}

#[derive(Debug)]
pub struct RdsState {
    pub account_id: String,
    pub region: String,
    pub instances: HashMap<String, DbInstance>,
    pub in_progress_instance_ids: HashSet<String>,
    pub snapshots: HashMap<String, DbSnapshot>,
    pub subnet_groups: HashMap<String, DbSubnetGroup>,
    pub parameter_groups: HashMap<String, DbParameterGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineVersionInfo {
    pub engine: String,
    pub engine_version: String,
    pub db_parameter_group_family: String,
    pub db_engine_description: String,
    pub db_engine_version_description: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderableDbInstanceOption {
    pub engine: String,
    pub engine_version: String,
    pub db_instance_class: String,
    pub license_model: String,
    pub storage_type: String,
    pub min_storage_size: i32,
    pub max_storage_size: i32,
}

#[derive(Debug, Clone)]
pub struct DbSubnetGroup {
    pub db_subnet_group_name: String,
    pub db_subnet_group_arn: String,
    pub db_subnet_group_description: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub subnet_availability_zones: Vec<String>,
    pub tags: Vec<RdsTag>,
}

#[derive(Debug, Clone)]
pub struct DbParameterGroup {
    pub db_parameter_group_name: String,
    pub db_parameter_group_arn: String,
    pub db_parameter_group_family: String,
    pub description: String,
    pub parameters: HashMap<String, String>,
    pub tags: Vec<RdsTag>,
}

impl RdsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            instances: HashMap::new(),
            in_progress_instance_ids: HashSet::new(),
            snapshots: HashMap::new(),
            subnet_groups: HashMap::new(),
            parameter_groups: default_parameter_groups(account_id, region),
        }
    }

    pub fn reset(&mut self) {
        self.instances.clear();
        self.in_progress_instance_ids.clear();
        self.snapshots.clear();
        self.subnet_groups.clear();
        self.parameter_groups = default_parameter_groups(&self.account_id, &self.region);
    }

    pub fn db_instance_arn(&self, db_instance_identifier: &str) -> String {
        Arn::new(
            "rds",
            &self.region,
            &self.account_id,
            &format!("db:{db_instance_identifier}"),
        )
        .to_string()
    }

    pub fn db_snapshot_arn(&self, db_snapshot_identifier: &str) -> String {
        Arn::new(
            "rds",
            &self.region,
            &self.account_id,
            &format!("snapshot:{db_snapshot_identifier}"),
        )
        .to_string()
    }

    pub fn db_subnet_group_arn(&self, db_subnet_group_name: &str) -> String {
        Arn::new(
            "rds",
            &self.region,
            &self.account_id,
            &format!("subgrp:{db_subnet_group_name}"),
        )
        .to_string()
    }

    pub fn db_parameter_group_arn(&self, db_parameter_group_name: &str) -> String {
        Arn::new(
            "rds",
            &self.region,
            &self.account_id,
            &format!("pg:{db_parameter_group_name}"),
        )
        .to_string()
    }

    pub fn next_dbi_resource_id(&self) -> String {
        format!("db-{}", Uuid::new_v4().simple())
    }

    pub fn begin_instance_creation(&mut self, db_instance_identifier: &str) -> bool {
        if self.instances.contains_key(db_instance_identifier)
            || self
                .in_progress_instance_ids
                .contains(db_instance_identifier)
        {
            return false;
        }

        self.in_progress_instance_ids
            .insert(db_instance_identifier.to_string());
        true
    }

    pub fn finish_instance_creation(&mut self, instance: DbInstance) {
        self.in_progress_instance_ids
            .remove(&instance.db_instance_identifier);
        self.instances
            .insert(instance.db_instance_identifier.clone(), instance);
    }

    pub fn cancel_instance_creation(&mut self, db_instance_identifier: &str) {
        self.in_progress_instance_ids.remove(db_instance_identifier);
    }
}

pub fn default_engine_versions() -> Vec<EngineVersionInfo> {
    vec![EngineVersionInfo {
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        db_parameter_group_family: "postgres16".to_string(),
        db_engine_description: "PostgreSQL".to_string(),
        db_engine_version_description: "PostgreSQL 16.3".to_string(),
        status: "available".to_string(),
    }]
}

pub fn default_orderable_options() -> Vec<OrderableDbInstanceOption> {
    vec![OrderableDbInstanceOption {
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        db_instance_class: "db.t3.micro".to_string(),
        license_model: "postgresql-license".to_string(),
        storage_type: "gp2".to_string(),
        min_storage_size: 20,
        max_storage_size: 16384,
    }]
}

pub fn default_parameter_groups(
    account_id: &str,
    region: &str,
) -> HashMap<String, DbParameterGroup> {
    let mut groups = HashMap::new();

    let default_group = DbParameterGroup {
        db_parameter_group_name: "default.postgres16".to_string(),
        db_parameter_group_arn: format!("arn:aws:rds:{region}:{account_id}:pg:default.postgres16"),
        db_parameter_group_family: "postgres16".to_string(),
        description: "Default parameter group for postgres16".to_string(),
        parameters: HashMap::new(),
        tags: Vec::new(),
    };

    groups.insert("default.postgres16".to_string(), default_group);
    groups
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{default_engine_versions, default_orderable_options, DbInstance, RdsState};

    #[test]
    fn new_initializes_account_and_region() {
        let state = RdsState::new("123456789012", "us-east-1");

        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.instances.is_empty());
        assert!(state.in_progress_instance_ids.is_empty());
    }

    #[test]
    fn reset_clears_instances() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        state.instances.insert(
            "db-1".to_string(),
            DbInstance {
                db_instance_identifier: "db-1".to_string(),
                db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: Some("postgres".to_string()),
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: true,
                deletion_protection: false,
                created_at: Utc::now(),
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
            },
        );

        state.reset();

        assert!(state.instances.is_empty());
        assert!(state.in_progress_instance_ids.is_empty());
    }

    #[test]
    fn default_engine_versions_are_postgres_metadata() {
        let versions = default_engine_versions();

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].engine, "postgres");
        assert_eq!(versions[0].engine_version, "16.3");
        assert_eq!(versions[0].db_parameter_group_family, "postgres16");
    }

    #[test]
    fn default_orderable_options_match_engine_versions() {
        let versions = default_engine_versions();
        let options = default_orderable_options();

        assert_eq!(options.len(), 1);
        assert_eq!(options[0].engine, versions[0].engine);
        assert_eq!(options[0].engine_version, versions[0].engine_version);
        assert_eq!(options[0].db_instance_class, "db.t3.micro");
    }

    #[test]
    fn begin_instance_creation_rejects_duplicate_identifiers() {
        let mut state = RdsState::new("123456789012", "us-east-1");

        assert!(state.begin_instance_creation("db-1"));
        assert!(!state.begin_instance_creation("db-1"));

        state.cancel_instance_creation("db-1");
        assert!(state.begin_instance_creation("db-1"));
    }
}
