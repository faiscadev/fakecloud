use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use uuid::Uuid;

use super::{
    apply_snapshot_dump_result, build_restored_instance, db_instance_xml, default_db_name,
    default_parameter_group, default_port_for_engine, filter_engine_versions,
    filter_orderable_options, license_model_for_engine, merge_tags, optional_i32_param,
    parse_tag_keys, parse_tags, save_snapshot_static, validate_create_request, RdsService,
    RdsSourceType,
};
use crate::state::{
    default_engine_versions, default_orderable_options, DbInstance, RdsSnapshot, RdsTag,
    SharedRdsState, RDS_SNAPSHOT_SCHEMA_VERSION,
};
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsService, AwsServiceError};
use fakecloud_persistence::{DiskSnapshotStore, SnapshotStore};
use tokio::sync::Mutex as AsyncMutex;

#[test]
fn default_port_matches_aws_for_each_engine() {
    assert_eq!(default_port_for_engine("postgres"), 5432);
    assert_eq!(default_port_for_engine("mysql"), 3306);
    assert_eq!(default_port_for_engine("mariadb"), 3306);
    assert_eq!(default_port_for_engine("oracle-ee"), 1521);
    assert_eq!(default_port_for_engine("oracle-se2"), 1521);
    assert_eq!(default_port_for_engine("sqlserver-ee"), 1433);
    assert_eq!(default_port_for_engine("sqlserver-ex"), 1433);
    assert_eq!(default_port_for_engine("db2-se"), 50000);
    assert_eq!(default_port_for_engine("db2-ae"), 50000);
}

#[test]
fn default_parameter_group_uses_engine_major_version() {
    assert_eq!(
        default_parameter_group("postgres", "16.3"),
        "default.postgres16"
    );
    assert_eq!(
        default_parameter_group("mysql", "8.0.35"),
        "default.mysql8.0"
    );
    assert_eq!(
        default_parameter_group("oracle-ee", "23.0.0"),
        "default.oracle-ee-23"
    );
    assert_eq!(
        default_parameter_group("sqlserver-ex", "16.00.4085.2.v1"),
        "default.sqlserver-ex-16"
    );
    assert_eq!(
        default_parameter_group("db2-se", "11.5.9.0.sb00000000.r1"),
        "default.db2-se-11.5"
    );
}

#[test]
fn license_model_reflects_engine_class() {
    assert_eq!(license_model_for_engine("postgres"), "postgresql-license");
    assert_eq!(license_model_for_engine("mysql"), "general-public-license");
    assert_eq!(license_model_for_engine("oracle-ee"), "license-included");
    assert_eq!(license_model_for_engine("sqlserver-se"), "license-included");
    assert_eq!(license_model_for_engine("db2-ae"), "bring-your-own-license");
}

#[test]
fn default_db_name_picks_per_engine_default() {
    assert_eq!(default_db_name("postgres"), "postgres");
    assert_eq!(default_db_name("mysql"), "mysql");
    assert_eq!(default_db_name("oracle-ee"), "ORCL");
    assert_eq!(default_db_name("sqlserver-ex"), "master");
    assert_eq!(default_db_name("db2-se"), "BLUDB");
}

#[test]
fn validate_create_request_accepts_new_engines() {
    for (engine, version, port) in [
        ("oracle-ee", "23.0.0", 1521),
        ("sqlserver-ex", "16.00.4085.2.v1", 1433),
        ("db2-se", "11.5.9.0.sb00000000.r1", 50000),
    ] {
        validate_create_request("test-db", 20, "db.t3.micro", engine, version, port)
            .expect("engine should be accepted");
    }
}

#[test]
fn validate_create_request_accepts_postgres_17() {
    // Regression for #2352: real AWS RDS offers postgres 17.x. Both the
    // major ("17") and full-triplet ("17.4") forms must validate.
    for version in ["17", "17.4"] {
        validate_create_request("test-db", 20, "db.t3.micro", "postgres", version, 5432)
            .unwrap_or_else(|e| panic!("postgres {version} should be accepted: {e:?}"));
    }
}

#[test]
fn validate_create_request_rejects_unsupported_engine_version() {
    let err = validate_create_request("test-db", 20, "db.t3.micro", "oracle-ee", "12.0.0", 1521)
        .expect_err("12.x is not in the supported list");
    let msg = format!("{err:?}");
    assert!(msg.contains("EngineVersion"), "unexpected: {msg}");
}

#[test]
fn filter_engine_versions_matches_requested_engine() {
    let versions = default_engine_versions();

    let filtered = filter_engine_versions(&versions, &Some("postgres".to_string()), &None, &None);

    assert_eq!(filtered.len(), 6); // All postgres versions
    assert!(filtered.iter().all(|v| v.engine == "postgres"));
}

#[test]
fn filter_orderable_options_respects_instance_class() {
    let options = default_orderable_options();

    let filtered = filter_orderable_options(
        &options,
        &Some("postgres".to_string()),
        &Some("16.3".to_string()),
        &Some("db.t3.micro".to_string()),
        &None,
        Some(true),
    );

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].db_instance_class, "db.t3.micro");
}

#[test]
fn validate_create_request_rejects_unsupported_engine() {
    let error = validate_create_request("test-db", 20, "db.t3.micro", "mysql", "16.3", 5432)
        .expect_err("unsupported engine");

    assert_eq!(error.code(), "InsufficientDBInstanceCapacity");
}

#[test]
fn optional_i32_param_rejects_invalid_integer() {
    let request = request("CreateDBInstance", &[("Port", "not-a-number")]);

    let error = optional_i32_param(&request, "Port").expect_err("invalid port");

    assert_eq!(error.code(), "InvalidParameterValue");
}

#[test]
fn db_instance_xml_renders_endpoint_and_status() {
    let created_at = Utc::now();
    let instance = DbInstance {
        db_instance_identifier: "test-db".to_string(),
        db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:test-db".to_string(),
        db_instance_class: "db.t3.micro".to_string(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        db_instance_status: "available".to_string(),
        master_username: "admin".to_string(),
        db_name: Some("appdb".to_string()),
        endpoint_address: "127.0.0.1".to_string(),
        port: 15432,
        allocated_storage: 20,
        publicly_accessible: true,
        deletion_protection: false,
        created_at,
        dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
        master_user_password: "secret123".to_string(),
        container_id: "container".to_string(),
        host_port: 15432,
        tags: Vec::new(),
        read_replica_source_db_instance_identifier: None,
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids: vec!["sg-12345678".to_string()],
        db_parameter_group_name: Some("default.postgres16".to_string()),
        backup_retention_period: 1,
        preferred_backup_window: "03:00-04:00".to_string(),
        preferred_maintenance_window: None,
        latest_restorable_time: Some(created_at),
        option_group_name: None,
        multi_az: false,
        pending_modified_values: None,
        db_subnet_group_name: None,
        availability_zone: None,
        storage_type: None,
        storage_encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        iops: None,
        monitoring_interval: None,
        monitoring_role_arn: None,
        performance_insights_enabled: false,
        performance_insights_kms_key_id: None,
        performance_insights_retention_period: None,
        enabled_cloudwatch_logs_exports: Vec::new(),
        ca_certificate_identifier: None,
        network_type: None,
        character_set_name: None,
        auto_minor_version_upgrade: None,
        copy_tags_to_snapshot: None,
        master_user_secret_arn: None,
        master_user_secret_kms_key_id: None,
        license_model: None,
        max_allocated_storage: None,
        multi_tenant: None,
        storage_throughput: None,
        tde_credential_arn: None,
        delete_automated_backups: None,
        db_security_groups: Vec::new(),
        domain: None,
        domain_fqdn: None,
        domain_ou: None,
        domain_iam_role_name: None,
        domain_auth_secret_arn: None,
        domain_dns_ips: Vec::new(),
        db_cluster_identifier: None,
        activity_stream: None,
    };

    let xml = db_instance_xml(&instance, Some("creating"), None);

    assert!(xml.contains("<DBInstanceIdentifier>test-db</DBInstanceIdentifier>"));
    assert!(xml.contains("<DBInstanceStatus>creating</DBInstanceStatus>"));
    assert!(xml.contains("<Address>127.0.0.1</Address><Port>15432</Port>"));
    // Fields AWS always returns and SDKs deserialize unconditionally.
    assert!(
        xml.contains("<IAMDatabaseAuthenticationEnabled>false</IAMDatabaseAuthenticationEnabled>")
    );
    assert!(xml.contains("<PerformanceInsightsEnabled>false</PerformanceInsightsEnabled>"));
    assert!(xml.contains("<EnabledCloudwatchLogsExports/>"));
    assert!(xml.contains("<ProcessorFeatures/>"));
    assert!(xml.contains("<ActivityStreamStatus>stopped</ActivityStreamStatus>"));
    assert!(xml.contains("<StorageEncrypted>false</StorageEncrypted>"));
}

#[test]
fn db_instance_xml_renders_dynamic_storage_and_kms() {
    let mut instance = make_instance_with_defaults("dyn");
    instance.availability_zone = Some("eu-west-1c".to_string());
    instance.storage_type = Some("gp3".to_string());
    instance.storage_encrypted = true;
    instance.kms_key_id = Some("arn:aws:kms:us-east-1:123456789012:key/abc".to_string());
    instance.iam_database_authentication_enabled = true;
    instance.iops = Some(3000);
    instance.monitoring_interval = Some(60);
    instance.monitoring_role_arn = Some("arn:aws:iam::123456789012:role/rds-monitor".to_string());
    instance.performance_insights_enabled = true;
    instance.performance_insights_retention_period = Some(7);
    instance.enabled_cloudwatch_logs_exports = vec!["error".to_string(), "general".to_string()];
    instance.ca_certificate_identifier = Some("rds-ca-rsa2048-g1".to_string());
    instance.network_type = Some("DUAL".to_string());
    instance.master_user_secret_arn =
        Some("arn:aws:secretsmanager:us-east-1:123:secret:rds!sec-abc".to_string());
    instance.master_user_secret_kms_key_id =
        Some("arn:aws:kms:us-east-1:123:key/aws/secretsmanager".to_string());

    let xml = db_instance_xml(&instance, None, None);

    assert!(xml.contains("<AvailabilityZone>eu-west-1c</AvailabilityZone>"));
    assert!(xml.contains("<StorageType>gp3</StorageType>"));
    assert!(xml.contains("<StorageEncrypted>true</StorageEncrypted>"));
    assert!(xml.contains("<KmsKeyId>arn:aws:kms:us-east-1:123456789012:key/abc</KmsKeyId>"));
    assert!(
        xml.contains("<IAMDatabaseAuthenticationEnabled>true</IAMDatabaseAuthenticationEnabled>")
    );
    assert!(xml.contains("<Iops>3000</Iops>"));
    assert!(xml.contains("<MonitoringInterval>60</MonitoringInterval>"));
    assert!(xml.contains("<EnhancedMonitoringResourceArn>arn:aws:iam::123456789012:role/rds-monitor</EnhancedMonitoringResourceArn>"));
    assert!(xml.contains("<PerformanceInsightsEnabled>true</PerformanceInsightsEnabled>"));
    assert!(
        xml.contains("<PerformanceInsightsRetentionPeriod>7</PerformanceInsightsRetentionPeriod>")
    );
    assert!(xml.contains("<EnabledCloudwatchLogsExports><member>error</member><member>general</member></EnabledCloudwatchLogsExports>"));
    assert!(xml.contains("<CACertificateIdentifier>rds-ca-rsa2048-g1</CACertificateIdentifier>"));
    assert!(xml.contains("<NetworkType>DUAL</NetworkType>"));
    assert!(xml.contains("<MasterUserSecret>"));
    assert!(xml.contains("<SecretStatus>active</SecretStatus>"));
}

#[test]
fn db_snapshot_xml_emits_extended_fields() {
    use super::db_snapshot_xml;
    let snapshot = crate::state::DbSnapshot {
        db_snapshot_identifier: "snap-1".to_string(),
        db_snapshot_arn: "arn:aws:rds:us-east-1:123:snapshot:snap-1".to_string(),
        db_instance_identifier: "src-db".to_string(),
        snapshot_create_time: Utc::now(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        allocated_storage: 20,
        status: "available".to_string(),
        port: 5432,
        master_username: "admin".to_string(),
        db_name: Some("appdb".to_string()),
        dbi_resource_id: "db-rid".to_string(),
        snapshot_type: "manual".to_string(),
        master_user_password: "secret".to_string(),
        tags: Vec::new(),
        dump_data: Vec::new(),
        availability_zone: Some("us-east-1a".to_string()),
        vpc_id: Some("vpc-abc".to_string()),
        instance_create_time: Some(Utc::now()),
        license_model: Some("postgresql-license".to_string()),
        iops: Some(3000),
        option_group_name: Some("default:postgres-16".to_string()),
        percent_progress: Some(100),
        storage_type: Some("gp3".to_string()),
        encrypted: true,
        kms_key_id: Some("arn:aws:kms:us-east-1:123:key/abc".to_string()),
        iam_database_authentication_enabled: true,
        timezone: None,
        storage_throughput: Some(125),
        snapshot_attributes: std::collections::BTreeMap::new(),
    };

    let xml = db_snapshot_xml(&snapshot);

    assert!(xml.contains("<AvailabilityZone>us-east-1a</AvailabilityZone>"));
    assert!(xml.contains("<VpcId>vpc-abc</VpcId>"));
    assert!(xml.contains("<InstanceCreateTime>"));
    assert!(xml.contains("<LicenseModel>postgresql-license</LicenseModel>"));
    assert!(xml.contains("<Iops>3000</Iops>"));
    assert!(xml.contains("<OptionGroupName>default:postgres-16</OptionGroupName>"));
    assert!(xml.contains("<PercentProgress>100</PercentProgress>"));
    assert!(xml.contains("<StorageType>gp3</StorageType>"));
    assert!(xml.contains("<Encrypted>true</Encrypted>"));
    assert!(xml.contains("<KmsKeyId>arn:aws:kms:us-east-1:123:key/abc</KmsKeyId>"));
    assert!(
        xml.contains("<IAMDatabaseAuthenticationEnabled>true</IAMDatabaseAuthenticationEnabled>")
    );
    assert!(xml.contains("<StorageThroughput>125</StorageThroughput>"));
    assert!(xml.contains("<ProcessorFeatures/>"));
}

fn make_instance_with_defaults(id: &str) -> DbInstance {
    let created_at = Utc::now();
    DbInstance {
        db_instance_identifier: id.to_string(),
        db_instance_arn: format!("arn:aws:rds:us-east-1:123:db:{id}"),
        db_instance_class: "db.t3.micro".to_string(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        db_instance_status: "available".to_string(),
        master_username: "admin".to_string(),
        db_name: None,
        endpoint_address: "127.0.0.1".to_string(),
        port: 5432,
        allocated_storage: 20,
        publicly_accessible: true,
        deletion_protection: false,
        created_at,
        dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
        master_user_password: "p".to_string(),
        container_id: "c".to_string(),
        host_port: 0,
        tags: Vec::new(),
        read_replica_source_db_instance_identifier: None,
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids: Vec::new(),
        db_parameter_group_name: None,
        backup_retention_period: 0,
        preferred_backup_window: String::new(),
        preferred_maintenance_window: None,
        latest_restorable_time: None,
        option_group_name: None,
        multi_az: false,
        pending_modified_values: None,
        db_subnet_group_name: None,
        availability_zone: None,
        storage_type: None,
        storage_encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        iops: None,
        monitoring_interval: None,
        monitoring_role_arn: None,
        performance_insights_enabled: false,
        performance_insights_kms_key_id: None,
        performance_insights_retention_period: None,
        enabled_cloudwatch_logs_exports: Vec::new(),
        ca_certificate_identifier: None,
        network_type: None,
        character_set_name: None,
        auto_minor_version_upgrade: None,
        copy_tags_to_snapshot: None,
        master_user_secret_arn: None,
        master_user_secret_kms_key_id: None,
        license_model: None,
        max_allocated_storage: None,
        multi_tenant: None,
        storage_throughput: None,
        tde_credential_arn: None,
        delete_automated_backups: None,
        db_security_groups: Vec::new(),
        domain: None,
        domain_fqdn: None,
        domain_ou: None,
        domain_iam_role_name: None,
        domain_auth_secret_arn: None,
        domain_dns_ips: Vec::new(),
        db_cluster_identifier: None,
        activity_stream: None,
    }
}

#[test]
fn parse_tags_reads_rds_query_shape() {
    let request = request(
        "AddTagsToResource",
        &[
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "dev"),
            ("Tags.Tag.2.Key", "team"),
            ("Tags.Tag.2.Value", "core"),
        ],
    );

    let tags = parse_tags(&request).expect("tags");

    assert_eq!(
        tags,
        vec![
            RdsTag {
                key: "env".to_string(),
                value: "dev".to_string(),
            },
            RdsTag {
                key: "team".to_string(),
                value: "core".to_string(),
            }
        ]
    );
}

#[test]
fn parse_tag_keys_reads_member_shape() {
    let request = request(
        "RemoveTagsFromResource",
        &[("TagKeys.member.1", "env"), ("TagKeys.member.2", "team")],
    );

    let tag_keys = parse_tag_keys(&request).expect("tag keys");

    assert_eq!(tag_keys, vec!["env".to_string(), "team".to_string()]);
}

#[test]
fn merge_tags_updates_existing_values() {
    let mut tags = vec![RdsTag {
        key: "env".to_string(),
        value: "dev".to_string(),
    }];

    merge_tags(
        &mut tags,
        &[
            RdsTag {
                key: "env".to_string(),
                value: "prod".to_string(),
            },
            RdsTag {
                key: "team".to_string(),
                value: "core".to_string(),
            },
        ],
    );

    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].value, "prod");
    assert_eq!(tags[1].key, "team");
}

#[tokio::test]
async fn describe_engine_versions_returns_xml_body() {
    let service = RdsService::new(Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    )));
    let request = request("DescribeDBEngineVersions", &[("Engine", "postgres")]);

    let response = service.handle(request).await.expect("response");
    let body = String::from_utf8(response.body.expect_bytes().to_vec()).expect("utf8");

    assert!(body.contains("<DescribeDBEngineVersionsResponse"));
    assert!(body.contains("<Engine>postgres</Engine>"));
    assert!(body.contains("<DBParameterGroupFamily>postgres16</DBParameterGroupFamily>"));
}

fn request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::from([("Action".to_string(), action.to_string())]);
    for (key, value) in params {
        query_params.insert((*key).to_string(), (*value).to_string());
    }

    AwsRequest {
        service: "rds".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

// ── Helpers for handler tests ────────────────────────────────────

fn make_service() -> RdsService {
    RdsService::new(Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    )))
}

#[derive(Default)]
struct CapturedEvent {
    source: String,
    detail_type: String,
    detail: String,
}

#[derive(Default)]
struct RecordingEb {
    events: std::sync::Mutex<Vec<CapturedEvent>>,
}

impl fakecloud_core::delivery::EventBridgeDelivery for RecordingEb {
    fn put_event(&self, source: &str, detail_type: &str, detail: &str, _bus: &str) {
        self.events.lock().unwrap().push(CapturedEvent {
            source: source.to_string(),
            detail_type: detail_type.to_string(),
            detail: detail.to_string(),
        });
    }
}

fn make_service_with_recorder() -> (RdsService, Arc<RecordingEb>) {
    let recorder = Arc::new(RecordingEb::default());
    let bus = Arc::new(DeliveryBus::new().with_eventbridge(recorder.clone()));
    let svc = RdsService::new(Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    )))
    .with_delivery_bus(bus);
    (svc, recorder)
}

#[test]
fn emit_event_emits_aws_rds_event_via_bus() {
    let (svc, rec) = make_service_with_recorder();
    svc.emit_event(
        RdsSourceType::DbInstance,
        "my-db",
        "arn:aws:rds:us-east-1:123456789012:db:my-db",
        "RDS-EVENT-0005",
        &["creation"],
        "DB instance created",
    );
    let events = rec.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    let e = &events[0];
    assert_eq!(e.source, "aws.rds");
    assert_eq!(e.detail_type, "RDS DB Instance Event");
    let detail: serde_json::Value = serde_json::from_str(&e.detail).unwrap();
    assert_eq!(detail["EventID"], "RDS-EVENT-0005");
    assert_eq!(detail["SourceType"], "DB_INSTANCE");
    assert_eq!(detail["SourceIdentifier"], "my-db");
    assert_eq!(detail["Message"], "DB instance created");
    assert_eq!(detail["EventCategories"][0], "creation");
}

#[test]
fn emit_event_no_op_without_bus() {
    let svc = make_service();
    svc.emit_event(
        RdsSourceType::DbSnapshot,
        "snap",
        "arn:aws:rds:us-east-1:123456789012:snapshot:snap",
        "RDS-EVENT-0042",
        &["creation"],
        "Manual snapshot created",
    );
}

#[test]
fn rds_source_type_detail_type_mapping() {
    assert_eq!(
        RdsSourceType::DbInstance.detail_type(),
        "RDS DB Instance Event"
    );
    assert_eq!(
        RdsSourceType::DbSnapshot.detail_type(),
        "RDS DB Snapshot Event"
    );
    assert_eq!(
        RdsSourceType::DbParameterGroup.detail_type(),
        "RDS DB Parameter Group Event"
    );
}

fn body_of(resp: fakecloud_core::service::AwsResponse) -> String {
    String::from_utf8(resp.body.expect_bytes().to_vec()).expect("utf8")
}

fn seed_instance(svc: &RdsService, identifier: &str) -> String {
    let arn = format!("arn:aws:rds:us-east-1:123456789012:db:{identifier}");
    let mut accounts = svc.state.write();
    let state = accounts.default_mut();
    state.instances.insert(
        identifier.to_string(),
        DbInstance {
            db_instance_identifier: identifier.to_string(),
            db_instance_arn: arn.clone(),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            endpoint_address: "127.0.0.1".to_string(),
            port: 15432,
            allocated_storage: 20,
            publicly_accessible: true,
            deletion_protection: false,
            created_at: Utc::now(),
            dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
            master_user_password: "secret".to_string(),
            container_id: "container".to_string(),
            host_port: 15432,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids: vec!["sg-12345678".to_string()],
            db_parameter_group_name: Some("default.postgres16".to_string()),
            backup_retention_period: 1,
            preferred_backup_window: "03:00-04:00".to_string(),
            preferred_maintenance_window: None,
            latest_restorable_time: None,
            option_group_name: None,
            multi_az: false,
            pending_modified_values: None,
            db_subnet_group_name: None,
            availability_zone: None,
            storage_type: None,
            storage_encrypted: false,
            kms_key_id: None,
            iam_database_authentication_enabled: false,
            iops: None,
            monitoring_interval: None,
            monitoring_role_arn: None,
            performance_insights_enabled: false,
            performance_insights_kms_key_id: None,
            performance_insights_retention_period: None,
            enabled_cloudwatch_logs_exports: Vec::new(),
            ca_certificate_identifier: None,
            network_type: None,
            character_set_name: None,
            auto_minor_version_upgrade: None,
            copy_tags_to_snapshot: None,
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: None,
            license_model: None,
            max_allocated_storage: None,
            multi_tenant: None,
            storage_throughput: None,
            tde_credential_arn: None,
            delete_automated_backups: None,
            db_security_groups: Vec::new(),
            domain: None,
            domain_fqdn: None,
            domain_ou: None,
            domain_iam_role_name: None,
            domain_auth_secret_arn: None,
            domain_dns_ips: Vec::new(),
            db_cluster_identifier: None,
            activity_stream: None,
        },
    );
    arn
}

fn assert_code<T>(result: Result<T, AwsServiceError>, expected_code: &str) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected error {expected_code}, got Ok"),
        Err(e) => {
            assert_eq!(e.code(), expected_code, "wrong error code");
            e
        }
    }
}

// ── Tag operations ───────────────────────────────────────────────

#[test]
fn add_tags_requires_resource_name() {
    let svc = make_service();
    let req = request("AddTagsToResource", &[]);
    assert_code(svc.add_tags_to_resource(&req), "MissingParameter");
}

#[test]
fn add_tags_with_no_tag_keys_is_a_noop() {
    // AWS RDS' Smithy model declares no `MissingParameter` analogue on
    // AddTagsToResource, so we accept an empty Tags list as a no-op
    // rather than emit an undeclared error code the strict conformance
    // probe would reject.
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    let req = request("AddTagsToResource", &[("ResourceName", arn.as_str())]);
    svc.add_tags_to_resource(&req).expect("noop ok");
}

#[test]
fn add_tags_appends_then_list_tags_returns_them() {
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    let add_req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "dev"),
        ],
    );
    svc.add_tags_to_resource(&add_req).unwrap();

    let list_req = request("ListTagsForResource", &[("ResourceName", arn.as_str())]);
    let body = body_of(svc.list_tags_for_resource(&list_req).unwrap());
    assert!(body.contains("<Key>env</Key>"));
    assert!(body.contains("<Value>dev</Value>"));
}

#[test]
fn list_tags_ignores_unsupported_filters_param() {
    // Smithy doesn't declare an "unsupported filter" error on
    // ListTagsForResource. Real AWS silently ignores unknown filters
    // and so do we, returning the tag list as if no filter was set.
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    let req = request(
        "ListTagsForResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Filters.Filter.1.Name", "x"),
        ],
    );
    svc.list_tags_for_resource(&req).expect("filters ignored");
}

#[test]
fn list_tags_missing_db_instance_returns_typed_not_found() {
    let svc = make_service();
    let req = request(
        "ListTagsForResource",
        &[("ResourceName", "arn:aws:rds:us-east-1:123456789012:db:nope")],
    );
    assert_code(svc.list_tags_for_resource(&req), "DBInstanceNotFound");
}

#[test]
fn list_tags_unknown_arn_resource_type_errors() {
    let svc = make_service();
    let req = request(
        "ListTagsForResource",
        &[(
            "ResourceName",
            "arn:aws:rds:us-east-1:123456789012:bogus:nope",
        )],
    );
    assert_code(svc.list_tags_for_resource(&req), "DBInstanceNotFound");
}

#[test]
fn list_tags_malformed_arn_errors() {
    let svc = make_service();
    let req = request(
        "ListTagsForResource",
        &[("ResourceName", "not-even-an-arn")],
    );
    assert_code(svc.list_tags_for_resource(&req), "DBInstanceNotFound");
}

#[test]
fn add_tags_to_snapshot_arn_persists() {
    let svc = make_service();
    seed_snapshot(&svc, "snap-1", "db1");
    let arn = {
        let __a = svc.state.read();
        __a.default_ref()
            .snapshots
            .get("snap-1")
            .unwrap()
            .db_snapshot_arn
            .clone()
    };
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "platform"),
        ],
    );
    svc.add_tags_to_resource(&req).unwrap();
    let __a = svc.state.read();
    let snap = __a.default_ref().snapshots.get("snap-1").unwrap();
    assert_eq!(snap.tags.len(), 1);
    assert_eq!(snap.tags[0].key, "team");
    assert_eq!(snap.tags[0].value, "platform");
}

#[test]
fn add_tags_to_parameter_group_arn_persists_and_lists() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let arn = {
        let __a = svc.state.read();
        __a.default_ref()
            .parameter_groups
            .get("pg1")
            .unwrap()
            .db_parameter_group_arn
            .clone()
    };
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
        ],
    );
    svc.add_tags_to_resource(&req).unwrap();

    let req = request("ListTagsForResource", &[("ResourceName", arn.as_str())]);
    let resp = svc.list_tags_for_resource(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Key>env</Key>"));
    assert!(body.contains("<Value>prod</Value>"));
}

#[test]
fn add_tags_to_subnet_group_arn_persists() {
    let svc = make_service();
    let arn = {
        let mut __a = svc.state.write();
        let state = __a.default_mut();
        let arn = state.db_subnet_group_arn(&state.region, "sg1");
        state.subnet_groups.insert(
            "sg1".to_string(),
            crate::state::DbSubnetGroup {
                db_subnet_group_name: "sg1".to_string(),
                db_subnet_group_arn: arn.clone(),
                db_subnet_group_description: "desc".to_string(),
                vpc_id: "vpc-1".to_string(),
                subnet_ids: Vec::new(),
                subnet_availability_zones: Vec::new(),
                tags: Vec::new(),
            },
        );
        arn
    };
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Tags.Tag.1.Key", "owner"),
            ("Tags.Tag.1.Value", "team-a"),
        ],
    );
    svc.add_tags_to_resource(&req).unwrap();
    let __a = svc.state.read();
    let g = __a.default_ref().subnet_groups.get("sg1").unwrap();
    assert_eq!(g.tags.len(), 1);
    assert_eq!(g.tags[0].key, "owner");
}

#[test]
fn remove_tags_from_parameter_group_only_listed_keys() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let arn = {
        let __a = svc.state.read();
        __a.default_ref()
            .parameter_groups
            .get("pg1")
            .unwrap()
            .db_parameter_group_arn
            .clone()
    };
    let add = request(
        "AddTagsToResource",
        &[
            ("ResourceName", arn.as_str()),
            ("Tags.Tag.1.Key", "k1"),
            ("Tags.Tag.1.Value", "v1"),
            ("Tags.Tag.2.Key", "k2"),
            ("Tags.Tag.2.Value", "v2"),
        ],
    );
    svc.add_tags_to_resource(&add).unwrap();
    let remove = request(
        "RemoveTagsFromResource",
        &[("ResourceName", arn.as_str()), ("TagKeys.member.1", "k1")],
    );
    svc.remove_tags_from_resource(&remove).unwrap();
    let __a = svc.state.read();
    let pg = __a.default_ref().parameter_groups.get("pg1").unwrap();
    assert_eq!(pg.tags.len(), 1);
    assert_eq!(pg.tags[0].key, "k2");
}

#[test]
fn add_tags_to_extras_resource_arn_stores_on_json() {
    // Cluster ARNs are extras-stored; tags land in a `Tags` array on
    // the JSON entry so they survive serde round-trips.
    let svc = make_service();
    let cluster_arn = {
        let mut __a = svc.state.write();
        let state = __a.default_mut();
        let arn = format!(
            "arn:aws:rds:us-east-1:{}:cluster:my-cluster",
            state.account_id
        );
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(
                "my-cluster".to_string(),
                serde_json::json!({"DBClusterIdentifier": "my-cluster"}),
            );
        arn
    };
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", cluster_arn.as_str()),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    );
    svc.add_tags_to_resource(&req).unwrap();
    let __a = svc.state.read();
    let entry = __a
        .default_ref()
        .extras
        .get("clusters")
        .unwrap()
        .get("my-cluster")
        .unwrap();
    let tags = entry.get("Tags").and_then(|t| t.as_array()).unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].get("Key").and_then(|k| k.as_str()), Some("team"));
}

/// Seed an extras-backed RDS resource with a minimal JSON entry so the
/// tagging dispatcher can locate it. The kind/bucket pairs mirror the
/// ones used by the create-time handlers in `extras.rs`; keeping this
/// helper local to the test module avoids leaking test-only surface
/// into the prod crate API.
fn seed_extras_entry(svc: &RdsService, bucket: &str, name: &str) {
    let mut accounts = svc.state.write();
    let state = accounts.default_mut();
    state
        .extras
        .entry(bucket.to_string())
        .or_default()
        .insert(name.to_string(), serde_json::json!({"Name": name}));
}

#[test]
fn tags_dispatch_covers_every_supported_resource_type() {
    // One tag round-trip (add -> list -> remove) per ARN segment, so the
    // dispatcher and tag_resource_not_found mapping stay in lockstep
    // with the resource buckets the rest of the crate writes to.
    let svc = make_service();
    let region = "us-east-1";
    let acct = "123456789012";

    // State-backed: db / snapshot / pg / subgrp.
    let _db_arn = seed_instance(&svc, "db1");
    seed_snapshot(&svc, "snap-1", "db1");
    create_param_group(&svc, "pg1");
    create_subnet_group(&svc, "sub1");

    // Extras-backed: cluster / cluster-snapshot / cluster-pg / og /
    // secgrp / es / db-proxy.
    seed_extras_entry(&svc, "clusters", "cluster-1");
    seed_extras_entry(&svc, "cluster_snapshots", "csnap-1");
    seed_extras_entry(&svc, "cluster_param_groups", "cpg-1");
    seed_extras_entry(&svc, "option_groups", "og-1");
    seed_extras_entry(&svc, "security_groups", "secgrp-1");
    seed_extras_entry(&svc, "event_subscriptions", "es-1");
    seed_extras_entry(&svc, "proxies", "proxy-1");

    let cases: &[(&str, &str)] = &[
        ("db", "db1"),
        ("snapshot", "snap-1"),
        ("pg", "pg1"),
        ("subgrp", "sub1"),
        ("cluster", "cluster-1"),
        ("cluster-snapshot", "csnap-1"),
        ("cluster-pg", "cpg-1"),
        ("og", "og-1"),
        ("secgrp", "secgrp-1"),
        ("es", "es-1"),
        ("db-proxy", "proxy-1"),
    ];

    for (kind, name) in cases {
        let arn = format!("arn:aws:rds:{region}:{acct}:{kind}:{name}");

        let add = request(
            "AddTagsToResource",
            &[
                ("ResourceName", arn.as_str()),
                ("Tags.Tag.1.Key", "env"),
                ("Tags.Tag.1.Value", "prod"),
            ],
        );
        svc.add_tags_to_resource(&add)
            .unwrap_or_else(|e| panic!("AddTags failed for kind={kind}: {e:?}"));

        let list = request("ListTagsForResource", &[("ResourceName", arn.as_str())]);
        let body = body_of(
            svc.list_tags_for_resource(&list)
                .unwrap_or_else(|e| panic!("ListTags failed for kind={kind}: {e:?}")),
        );
        assert!(
            body.contains("<Key>env</Key>") && body.contains("<Value>prod</Value>"),
            "ListTags for kind={kind} should echo the tag, body was: {body}"
        );

        let rm = request(
            "RemoveTagsFromResource",
            &[("ResourceName", arn.as_str()), ("TagKeys.member.1", "env")],
        );
        svc.remove_tags_from_resource(&rm)
            .unwrap_or_else(|e| panic!("RemoveTags failed for kind={kind}: {e:?}"));

        let body = body_of(svc.list_tags_for_resource(&list).unwrap());
        assert!(
            !body.contains("<Key>env</Key>"),
            "RemoveTags for kind={kind} should strip the tag, body was: {body}"
        );
    }
}

#[test]
fn tags_dispatch_typed_not_found_per_resource_type() {
    // Each known resource-type must surface its own NotFound code
    // rather than the generic `DBInstanceNotFound` fallback we use for
    // malformed ARNs.
    let svc = make_service();
    let region = "us-east-1";
    let acct = "123456789012";

    let cases: &[(&str, &str)] = &[
        ("db", "DBInstanceNotFound"),
        ("snapshot", "DBSnapshotNotFound"),
        ("cluster", "DBClusterNotFoundFault"),
        ("cluster-snapshot", "DBClusterSnapshotNotFoundFault"),
        ("pg", "DBParameterGroupNotFound"),
        ("cluster-pg", "DBParameterGroupNotFound"),
        ("og", "OptionGroupNotFoundFault"),
        ("subgrp", "DBSubnetGroupNotFoundFault"),
        ("secgrp", "DBSecurityGroupNotFound"),
        ("db-proxy", "DBProxyNotFoundFault"),
        ("es", "SubscriptionNotFound"),
    ];

    for (kind, expected_code) in cases {
        let arn = format!("arn:aws:rds:{region}:{acct}:{kind}:ghost");
        let req = request("ListTagsForResource", &[("ResourceName", arn.as_str())]);
        assert_code(svc.list_tags_for_resource(&req), expected_code);
    }
}

#[test]
fn remove_tags_strips_only_listed_keys() {
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    {
        let mut __a = svc.state.write();
        let state = __a.default_mut();
        let inst = state.instances.get_mut("db1").unwrap();
        inst.tags = vec![
            RdsTag {
                key: "env".to_string(),
                value: "dev".to_string(),
            },
            RdsTag {
                key: "team".to_string(),
                value: "core".to_string(),
            },
        ];
    }
    let req = request(
        "RemoveTagsFromResource",
        &[("ResourceName", arn.as_str()), ("TagKeys.member.1", "env")],
    );
    svc.remove_tags_from_resource(&req).unwrap();

    let __a = svc.state.read();
    let state = __a.default_ref();
    let tags = &state.instances.get("db1").unwrap().tags;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key, "team");
}

#[test]
fn remove_tags_with_no_keys_is_a_noop() {
    // RemoveTagsFromResource declares no `MissingParameter`-equivalent
    // wire shape in Smithy; treat empty TagKeys as a no-op rather than
    // emit an undeclared error code.
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    let req = request("RemoveTagsFromResource", &[("ResourceName", arn.as_str())]);
    svc.remove_tags_from_resource(&req).expect("noop ok");
}

// ── DB Subnet Groups ─────────────────────────────────────────────

fn create_subnet_group(svc: &RdsService, name: &str) {
    let req = request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", name),
            ("DBSubnetGroupDescription", "test"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-aaa"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-bbb"),
        ],
    );
    svc.create_db_subnet_group(&req).unwrap();
}

#[test]
fn create_db_subnet_group_requires_two_subnets() {
    let svc = make_service();
    let req = request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sg1"),
            ("DBSubnetGroupDescription", "t"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-aaa"),
        ],
    );
    assert_code(
        svc.create_db_subnet_group(&req),
        "DBSubnetGroupDoesNotCoverEnoughAZs",
    );
}

#[test]
fn create_db_subnet_group_rejects_empty_subnets() {
    // Folded into the `subnet_ids.len() < 2` check that emits the
    // Smithy-declared `DBSubnetGroupDoesNotCoverEnoughAZs` shape.
    let svc = make_service();
    let req = request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sg1"),
            ("DBSubnetGroupDescription", "t"),
        ],
    );
    assert_code(
        svc.create_db_subnet_group(&req),
        "DBSubnetGroupDoesNotCoverEnoughAZs",
    );
}

#[test]
fn create_db_subnet_group_rejects_duplicates() {
    let svc = make_service();
    create_subnet_group(&svc, "sg1");
    let req = request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sg1"),
            ("DBSubnetGroupDescription", "t"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-x"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-y"),
        ],
    );
    assert_code(
        svc.create_db_subnet_group(&req),
        "DBSubnetGroupAlreadyExists",
    );
}

#[test]
fn describe_db_instances_echoes_the_subnet_group() {
    // AWS returns the whole `DBSubnetGroup` on every DBInstance placed in
    // one; graders and Terraform read `DBSubnetGroup.DBSubnetGroupName`
    // off DescribeDBInstances to check where a DB landed.
    let svc = make_service();
    create_subnet_group(&svc, "private-subnets");
    seed_instance(&svc, "db1");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state.instances.get_mut("db1").unwrap().db_subnet_group_name =
            Some("private-subnets".to_string());
    }

    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", "db1")]);
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(body.contains("<DBSubnetGroup><DBSubnetGroupName>private-subnets"));
    assert!(body.contains("<SubnetIdentifier>subnet-aaa</SubnetIdentifier>"));
    assert!(body.contains("<SubnetIdentifier>subnet-bbb</SubnetIdentifier>"));
}

#[test]
fn db_instance_xml_omits_subnet_group_when_absent() {
    // Instances outside a subnet group (EC2-Classic-style seeds, Aurora
    // members created without one) must not grow an empty element.
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", "db1")]);
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(!body.contains("<DBSubnetGroup>"));
}

#[test]
fn describe_db_subnet_groups_by_name_or_list() {
    let svc = make_service();
    create_subnet_group(&svc, "sg-alpha");
    create_subnet_group(&svc, "sg-beta");

    let by_name = request(
        "DescribeDBSubnetGroups",
        &[("DBSubnetGroupName", "sg-alpha")],
    );
    let body = body_of(svc.describe_db_subnet_groups(&by_name).unwrap());
    assert!(body.contains("sg-alpha"));
    assert!(!body.contains("sg-beta"));

    let list_all = request("DescribeDBSubnetGroups", &[]);
    let body = body_of(svc.describe_db_subnet_groups(&list_all).unwrap());
    assert!(body.contains("sg-alpha"));
    assert!(body.contains("sg-beta"));
}

#[test]
fn describe_db_subnet_groups_unknown_name_errors() {
    let svc = make_service();
    let req = request("DescribeDBSubnetGroups", &[("DBSubnetGroupName", "ghost")]);
    assert_code(
        svc.describe_db_subnet_groups(&req),
        "DBSubnetGroupNotFoundFault",
    );
}

#[test]
fn delete_db_subnet_group_unknown_errors() {
    let svc = make_service();
    let req = request("DeleteDBSubnetGroup", &[("DBSubnetGroupName", "ghost")]);
    assert_code(
        svc.delete_db_subnet_group(&req),
        "DBSubnetGroupNotFoundFault",
    );
}

#[test]
fn delete_db_subnet_group_removes_entry() {
    let svc = make_service();
    create_subnet_group(&svc, "sg1");
    let req = request("DeleteDBSubnetGroup", &[("DBSubnetGroupName", "sg1")]);
    svc.delete_db_subnet_group(&req).unwrap();
    assert!(svc.state.read().default_ref().subnet_groups.is_empty());
}

#[test]
fn modify_db_subnet_group_updates_subnet_ids() {
    let svc = make_service();
    create_subnet_group(&svc, "sg1");
    let req = request(
        "ModifyDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sg1"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-new1"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-new2"),
        ],
    );
    svc.modify_db_subnet_group(&req).unwrap();

    let __a = svc.state.read();
    let state = __a.default_ref();
    let sg = state.subnet_groups.get("sg1").unwrap();
    assert_eq!(sg.subnet_ids, vec!["subnet-new1", "subnet-new2"]);
}

// ── DB Parameter Groups ──────────────────────────────────────────

fn create_param_group(svc: &RdsService, name: &str) {
    let req = request(
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupName", name),
            ("DBParameterGroupFamily", "postgres16"),
            ("Description", "test"),
        ],
    );
    svc.create_db_parameter_group(&req).unwrap();
}

#[test]
fn create_db_parameter_group_accepts_unknown_family() {
    // Smithy declares no `InvalidParameterValue` shape on
    // CreateDBParameterGroup, so we accept any family verbatim
    // rather than emit an undeclared wire code.
    let svc = make_service();
    let req = request(
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("DBParameterGroupFamily", "oracle19"),
            ("Description", "t"),
        ],
    );
    svc.create_db_parameter_group(&req)
        .expect("unknown family accepted");
}

#[test]
fn create_db_parameter_group_rejects_duplicates() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request(
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("DBParameterGroupFamily", "postgres16"),
            ("Description", "t"),
        ],
    );
    assert_code(
        svc.create_db_parameter_group(&req),
        "DBParameterGroupAlreadyExists",
    );
}

#[test]
fn describe_db_parameter_groups_by_name_or_list() {
    let svc = make_service();
    create_param_group(&svc, "pg-alpha");
    create_param_group(&svc, "pg-beta");
    let by_name = request(
        "DescribeDBParameterGroups",
        &[("DBParameterGroupName", "pg-alpha")],
    );
    let body = body_of(svc.describe_db_parameter_groups(&by_name).unwrap());
    assert!(body.contains("pg-alpha"));
    assert!(!body.contains("pg-beta"));
    let list = request("DescribeDBParameterGroups", &[]);
    let body = body_of(svc.describe_db_parameter_groups(&list).unwrap());
    assert!(body.contains("pg-alpha"));
    assert!(body.contains("pg-beta"));
}

#[test]
fn describe_db_parameter_groups_unknown_name_errors() {
    let svc = make_service();
    let req = request(
        "DescribeDBParameterGroups",
        &[("DBParameterGroupName", "ghost")],
    );
    assert_code(
        svc.describe_db_parameter_groups(&req),
        "DBParameterGroupNotFound",
    );
}

#[test]
fn delete_db_parameter_group_rejects_default_groups() {
    let svc = make_service();
    let req = request(
        "DeleteDBParameterGroup",
        &[("DBParameterGroupName", "default.postgres16")],
    );
    assert_code(
        svc.delete_db_parameter_group(&req),
        "InvalidDBParameterGroupState",
    );
}

#[test]
fn delete_db_parameter_group_unknown_errors() {
    let svc = make_service();
    let req = request(
        "DeleteDBParameterGroup",
        &[("DBParameterGroupName", "ghost")],
    );
    assert_code(
        svc.delete_db_parameter_group(&req),
        "DBParameterGroupNotFound",
    );
}

#[test]
fn delete_db_parameter_group_removes_entry() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request("DeleteDBParameterGroup", &[("DBParameterGroupName", "pg1")]);
    svc.delete_db_parameter_group(&req).unwrap();
    assert!(!svc
        .state
        .read()
        .default_ref()
        .parameter_groups
        .contains_key("pg1"));
}

#[test]
fn modify_db_parameter_group_updates_description() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Description", "shiny new"),
        ],
    );
    svc.modify_db_parameter_group(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    assert_eq!(
        state.parameter_groups.get("pg1").unwrap().description,
        "shiny new"
    );
}

#[test]
fn modify_db_parameter_group_unknown_errors() {
    let svc = make_service();
    let req = request(
        "ModifyDBParameterGroup",
        &[("DBParameterGroupName", "ghost"), ("Description", "x")],
    );
    assert_code(
        svc.modify_db_parameter_group(&req),
        "DBParameterGroupNotFound",
    );
}

#[test]
fn modify_db_parameter_group_persists_parameters() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Parameters.member.1.ParameterName", "max_connections"),
            ("Parameters.member.1.ParameterValue", "200"),
            ("Parameters.member.1.ApplyMethod", "immediate"),
            ("Parameters.member.2.ParameterName", "shared_buffers"),
            ("Parameters.member.2.ParameterValue", "256MB"),
            ("Parameters.member.2.ApplyMethod", "pending-reboot"),
        ],
    );
    svc.modify_db_parameter_group(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let pg = state.parameter_groups.get("pg1").unwrap();
    assert_eq!(
        pg.parameters.get("max_connections").map(String::as_str),
        Some("200")
    );
    assert_eq!(
        pg.parameters.get("shared_buffers").map(String::as_str),
        Some("256MB")
    );
}

#[test]
fn describe_db_parameters_returns_user_set_values() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Parameters.member.1.ParameterName", "max_connections"),
            ("Parameters.member.1.ParameterValue", "200"),
        ],
    );
    svc.modify_db_parameter_group(&req).unwrap();

    let req = request("DescribeDBParameters", &[("DBParameterGroupName", "pg1")]);
    let resp = svc.describe_db_parameters_real(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ParameterName>max_connections</ParameterName>"));
    assert!(body.contains("<ParameterValue>200</ParameterValue>"));
    assert!(body.contains("<Source>user</Source>"));
}

#[test]
fn describe_db_parameters_with_engine_default_source_omits_user_params() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    // Modify a parameter that is NOT seeded as an engine default so the
    // `engine-default` source filter has a clean way to demonstrate it
    // skips user-only parameters.
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Parameters.member.1.ParameterName", "user_only_knob"),
            ("Parameters.member.1.ParameterValue", "42"),
        ],
    );
    svc.modify_db_parameter_group(&req).unwrap();

    let req = request(
        "DescribeDBParameters",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Source", "engine-default"),
        ],
    );
    let resp = svc.describe_db_parameters_real(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // User-only parameter is hidden when filtering on engine defaults.
    assert!(!body.contains("user_only_knob"));
    // Engine defaults still surface (postgres16 seeds `max_connections`).
    assert!(body.contains("max_connections"));
    assert!(body.contains("<Source>engine-default</Source>"));
    assert!(!body.contains("<Source>user</Source>"));
}

#[test]
fn describe_db_parameters_with_no_source_returns_user_and_engine_defaults() {
    let svc = make_service();
    create_param_group(&svc, "pg1");
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "pg1"),
            ("Parameters.member.1.ParameterName", "max_connections"),
            ("Parameters.member.1.ParameterValue", "200"),
        ],
    );
    svc.modify_db_parameter_group(&req).unwrap();

    let req = request("DescribeDBParameters", &[("DBParameterGroupName", "pg1")]);
    let resp = svc.describe_db_parameters_real(&req).unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // User override of `max_connections` shadows the engine default so
    // the parameter appears exactly once with `Source=user`.
    assert_eq!(
        body.matches("<ParameterName>max_connections</ParameterName>")
            .count(),
        1
    );
    assert!(body.contains("<ParameterValue>200</ParameterValue>"));
    // Other engine defaults (e.g. work_mem) still come through.
    assert!(body.contains("<ParameterName>work_mem</ParameterName>"));
    assert!(body.contains("<Source>engine-default</Source>"));
}

#[test]
fn describe_db_parameters_unknown_group_returns_not_found() {
    let svc = make_service();
    let req = request("DescribeDBParameters", &[("DBParameterGroupName", "ghost")]);
    assert_code(
        svc.describe_db_parameters_real(&req),
        "DBParameterGroupNotFound",
    );
}

// ── DescribeDBInstances ──────────────────────────────────────────

#[test]
fn describe_db_instances_by_id_returns_only_one() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");
    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", "db1")]);
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_unknown_id_errors() {
    let svc = make_service();
    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", "ghost")]);
    assert_code(svc.describe_db_instances(&req), "DBInstanceNotFound");
}

#[test]
fn describe_db_instances_lists_all_when_unbounded() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");
    seed_instance(&svc, "db3");
    let req = request("DescribeDBInstances", &[]);
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    for id in ["db1", "db2", "db3"] {
        assert!(body.contains(&format!(
            "<DBInstanceIdentifier>{id}</DBInstanceIdentifier>"
        )));
    }
}

// ── DescribeDBInstances Filters ──────────────────────────────────

/// Read back the generated resource id of a seeded instance.
fn resource_id_of(svc: &RdsService, identifier: &str) -> String {
    svc.state
        .read()
        .default_ref()
        .instances
        .get(identifier)
        .expect("seeded instance")
        .dbi_resource_id
        .clone()
}

#[test]
fn describe_db_instances_filters_by_dbi_resource_id() {
    // Regression for #2481: the Terraform/OpenTofu AWS provider reads a
    // DB instance back by `dbi-resource-id`. Ignoring the filter returns
    // every instance, so the provider can't resolve the one it created.
    let svc = make_service();
    seed_instance(&svc, "mydb-01-default");
    seed_instance(&svc, "mydb-02-default");
    let wanted = resource_id_of(&svc, "mydb-02-default");

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "dbi-resource-id"),
            ("Filters.Filter.1.Values.Value.1", &wanted),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());

    assert!(body.contains("<DBInstanceIdentifier>mydb-02-default</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>mydb-01-default</DBInstanceIdentifier>"));
}

#[test]
fn a_wrong_type_arn_never_widens_a_targeted_read() {
    // A wrong-type ARN normalizes to "no identifier", and an absent
    // identifier means "no filter" -- so without an explicit guard a
    // misconfigured ARN turns a single-resource read into a full listing
    // and a client expecting one row matches an arbitrary resource.
    let svc = make_service();
    seed_instance(&svc, "mydb");
    seed_instance(&svc, "otherdb");
    seed_snapshot(&svc, "snap-1", "mydb");

    let cluster_arn = "arn:aws:rds:us-east-1:123456789012:cluster:mycl";
    let req = request(
        "DescribeDBInstances",
        &[("DBInstanceIdentifier", cluster_arn)],
    );
    assert_code(svc.describe_db_instances(&req), "DBInstanceNotFound");

    let cluster_snapshot_arn = "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:snap-1";
    let req = request(
        "DescribeDBSnapshots",
        &[("DBSnapshotIdentifier", cluster_snapshot_arn)],
    );
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");

    // ...and the same for the instance filter on DescribeDBSnapshots.
    let req = request(
        "DescribeDBSnapshots",
        &[("DBInstanceIdentifier", cluster_arn)],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(!body.contains("<DBSnapshotIdentifier>"), "body: {body}");

    // Every narrowing parameter is AND-ed with the identifier, the same
    // rule SnapshotType and Filters follow: a matching instance id keeps
    // the named snapshot, a non-matching one excludes it.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", "snap-1"),
            ("DBInstanceIdentifier", "mydb"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>snap-1</DBSnapshotIdentifier>"));

    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", "snap-1"),
            ("DBInstanceIdentifier", "otherdb"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        !body.contains("<DBSnapshotIdentifier>"),
        "the instance id was dropped on the named-snapshot path: {body}"
    );

    // Right type, EMPTY resource id: still an identifier that names
    // nothing, not an absent parameter.
    let req = request(
        "DescribeDBInstances",
        &[(
            "DBInstanceIdentifier",
            "arn:aws:rds:us-east-1:123456789012:db:",
        )],
    );
    assert_code(svc.describe_db_instances(&req), "DBInstanceNotFound");

    let req = request(
        "DescribeDBSnapshots",
        &[(
            "DBSnapshotIdentifier",
            "arn:aws:rds:us-east-1:123456789012:snapshot:",
        )],
    );
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");
}

#[tokio::test]
async fn restore_db_instance_reports_the_identifier_it_was_given() {
    // The cluster-snapshot parameter is an alias, so an ARN of that type
    // resolves through it -- and an unknown one echoes the caller's own
    // identifier instead of a bare "(none)".
    let svc = make_service();

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            (
                "DBClusterSnapshotIdentifier",
                "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:ghost",
            ),
        ],
    );
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Err(err) => {
            // A Multi-AZ DB cluster snapshot has its own declared fault.
            assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
            let message = format!("{err:?}");
            assert!(
                message.contains("ghost"),
                "the caller's identifier was dropped from the error: {message}"
            );
        }
        Ok(_) => panic!("unknown snapshot should fault"),
    }
}

#[test]
fn describe_db_instances_rejects_another_accounts_arn() {
    // A DB instance is never shared across accounts, so a foreign ARN
    // must not report this account's same-named instance.
    let svc = make_service();
    seed_instance(&svc, "mydb");

    let req = request(
        "DescribeDBInstances",
        &[(
            "DBInstanceIdentifier",
            "arn:aws:rds:us-east-1:999999999999:db:mydb",
        )],
    );
    assert_code(svc.describe_db_instances(&req), "DBInstanceNotFound");
}

#[test]
fn describe_db_instances_identifier_accepts_an_arn() {
    // The Smithy doc: "The user-supplied instance identifier or the
    // Amazon Resource Name (ARN) of the DB instance".
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");

    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", &arn)]);
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_filter_accepts_member_element_spelling() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");
    let wanted = resource_id_of(&svc, "db1");

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.member.1.Name", "dbi-resource-id"),
            ("Filters.member.1.Values.member.1", &wanted),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());

    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_filter_values_are_ored() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");
    seed_instance(&svc, "db3");

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "db-instance-id"),
            ("Filters.Filter.1.Values.Value.1", "db1"),
            ("Filters.Filter.1.Values.Value.2", "db3"),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());

    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(body.contains("<DBInstanceIdentifier>db3</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_db_instance_id_filter_accepts_an_arn() {
    let svc = make_service();
    let arn = seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "db-instance-id"),
            ("Filters.Filter.1.Values.Value.1", &arn),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());

    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_separate_filters_are_anded() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_instance(&svc, "db2");
    let wanted = resource_id_of(&svc, "db1");

    // engine matches both instances, the resource id only db1.
    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "postgres"),
            ("Filters.Filter.2.Name", "dbi-resource-id"),
            ("Filters.Filter.2.Values.Value.1", &wanted),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());

    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>db2</DBInstanceIdentifier>"));

    // A filter no instance satisfies yields an empty list, not an error.
    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "mysql"),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(!body.contains("<DBInstanceIdentifier>"), "body: {body}");
}

#[test]
fn describe_db_instances_filters_by_db_cluster_id() {
    let svc = make_service();
    seed_instance(&svc, "writer");
    seed_instance(&svc, "standalone");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .instances
            .get_mut("writer")
            .expect("seeded instance")
            .db_cluster_identifier = Some("aurora-1".to_string());
    }

    for value in [
        "aurora-1",
        "arn:aws:rds:us-east-1:123456789012:cluster:aurora-1",
    ] {
        let req = request(
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", value),
            ],
        );
        let body = body_of(svc.describe_db_instances(&req).unwrap());
        assert!(
            body.contains("<DBInstanceIdentifier>writer</DBInstanceIdentifier>"),
            "value {value} body: {body}"
        );
        assert!(!body.contains("<DBInstanceIdentifier>standalone</DBInstanceIdentifier>"));
    }
}

#[test]
fn describe_db_instances_filter_is_anded_with_the_identifier() {
    // The instance exists, so a filter it doesn't satisfy yields an
    // empty list rather than DBInstanceNotFound.
    let svc = make_service();
    seed_instance(&svc, "db1");

    let req = request(
        "DescribeDBInstances",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "mysql"),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(!body.contains("<DBInstanceIdentifier>"), "body: {body}");

    let req = request(
        "DescribeDBInstances",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "postgres"),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(body.contains("<DBInstanceIdentifier>db1</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_instances_unrecognized_filter_matches_nothing() {
    // `InvalidParameterValue` isn't declared on DescribeDBInstances, so
    // an unknown filter name can't be rejected the way AWS does; an
    // empty result is the closest in-shape behaviour, and is safer than
    // returning every instance to a caller that asked to narrow.
    let svc = make_service();
    seed_instance(&svc, "db1");

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "not-a-real-filter"),
            ("Filters.Filter.1.Values.Value.1", "whatever"),
        ],
    );
    let resp = svc.describe_db_instances(&req).unwrap();
    assert!(resp.status.is_success());
    assert!(!body_of(resp).contains("<DBInstanceIdentifier>"));
}

// ── ModifyDBInstance ─────────────────────────────────────────────

#[test]
fn modify_db_instance_with_no_changes_is_a_noop() {
    // Smithy declares no `InvalidParameterCombination` shape on
    // ModifyDBInstance. A modify call that touches nothing returns
    // the unchanged DB instance description rather than an error.
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request("ModifyDBInstance", &[("DBInstanceIdentifier", "db1")]);
    svc.modify_db_instance(&req).expect("noop modify ok");
}

#[test]
fn modify_db_instance_unknown_errors() {
    let svc = make_service();
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "ghost"),
            ("DBInstanceClass", "db.t3.small"),
        ],
    );
    assert_code(svc.modify_db_instance(&req), "DBInstanceNotFound");
}

#[test]
fn modify_db_instance_apply_immediately_updates_class() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("DBInstanceClass", "db.t3.small"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    assert_eq!(
        state.instances.get("db1").unwrap().db_instance_class,
        "db.t3.small"
    );
}

#[test]
fn modify_db_instance_pending_when_not_apply_immediately() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("DBInstanceClass", "db.t3.small"),
            ("ApplyImmediately", "false"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    assert_eq!(inst.db_instance_class, "db.t3.micro");
    assert_eq!(
        inst.pending_modified_values
            .as_ref()
            .unwrap()
            .db_instance_class
            .as_deref(),
        Some("db.t3.small"),
    );
}

#[test]
fn modify_db_instance_apply_immediately_updates_engine_and_storage() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("EngineVersion", "16.4"),
            ("AllocatedStorage", "100"),
            ("Iops", "3000"),
            ("StorageType", "io2"),
            ("PreferredMaintenanceWindow", "Mon:00:00-Mon:01:00"),
            ("MultiAZ", "true"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    assert_eq!(inst.engine_version, "16.4");
    assert_eq!(inst.allocated_storage, 100);
    assert_eq!(inst.iops, Some(3000));
    assert_eq!(inst.storage_type.as_deref(), Some("io2"));
    assert_eq!(
        inst.preferred_maintenance_window.as_deref(),
        Some("Mon:00:00-Mon:01:00")
    );
    assert!(inst.multi_az);
    assert!(inst.pending_modified_values.is_none());
}

#[test]
fn modify_db_instance_pending_stages_extended_fields() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("EngineVersion", "16.4"),
            ("AllocatedStorage", "100"),
            ("PreferredBackupWindow", "04:00-05:00"),
            ("DBParameterGroupName", "custom-pg"),
            ("MultiAZ", "true"),
            ("ApplyImmediately", "false"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    let pending = inst.pending_modified_values.as_ref().unwrap();
    assert_eq!(pending.engine_version.as_deref(), Some("16.4"));
    assert_eq!(pending.allocated_storage, Some(100));
    assert_eq!(
        pending.preferred_backup_window.as_deref(),
        Some("04:00-05:00")
    );
    assert_eq!(
        pending.db_parameter_group_name.as_deref(),
        Some("custom-pg")
    );
    assert_eq!(pending.multi_az, Some(true));
    // Live values unchanged.
    assert_eq!(inst.engine_version, "16.3");
    assert_eq!(inst.allocated_storage, 20);
}

#[test]
fn modify_db_instance_immediate_only_fields_apply_with_apply_immediately_false() {
    // CACertificateIdentifier, MasterUserSecretKmsKeyId, and the
    // CloudwatchLogsExportConfiguration are AWS-immediate fields:
    // ApplyImmediately=false must not stage them.
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("CACertificateIdentifier", "rds-ca-2024"),
            ("MasterUserSecretKmsKeyId", "alias/aws/rds"),
            (
                "CloudwatchLogsExportConfiguration.EnableLogTypes.member.1",
                "postgresql",
            ),
            ("ApplyImmediately", "false"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    assert_eq!(
        inst.ca_certificate_identifier.as_deref(),
        Some("rds-ca-2024")
    );
    assert_eq!(
        inst.master_user_secret_kms_key_id.as_deref(),
        Some("alias/aws/rds")
    );
    assert!(inst
        .enabled_cloudwatch_logs_exports
        .iter()
        .any(|t| t == "postgresql"));
    // No pending values for these — they were applied directly.
    assert!(inst.pending_modified_values.is_none());
}

#[test]
fn modify_db_instance_cloudwatch_disable_log_types_removes_existing() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    {
        let mut __a = svc.state.write();
        let state = __a.default_mut();
        let inst = state.instances.get_mut("db1").unwrap();
        inst.enabled_cloudwatch_logs_exports =
            vec!["postgresql".to_string(), "upgrade".to_string()];
    }
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            (
                "CloudwatchLogsExportConfiguration.DisableLogTypes.member.1",
                "upgrade",
            ),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    assert_eq!(inst.enabled_cloudwatch_logs_exports, vec!["postgresql"]);
}

// ── Snapshots (sync ops only) ────────────────────────────────────

/// A snapshot record owned by an account other than the default one,
/// for the shared / public listing paths.
fn other_account_snapshot(snapshot_id: &str) -> crate::state::DbSnapshot {
    crate::state::DbSnapshot {
        db_snapshot_identifier: snapshot_id.to_string(),
        db_snapshot_arn: format!("arn:aws:rds:us-east-1:999999999999:snapshot:{snapshot_id}"),
        db_instance_identifier: "other-db".to_string(),
        snapshot_create_time: Utc::now(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        allocated_storage: 20,
        status: "available".to_string(),
        port: 5432,
        master_username: "admin".to_string(),
        db_name: Some("appdb".to_string()),
        dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
        snapshot_type: "manual".to_string(),
        master_user_password: "secret".to_string(),
        tags: Vec::new(),
        dump_data: Vec::new(),
        availability_zone: None,
        vpc_id: None,
        instance_create_time: None,
        license_model: None,
        iops: None,
        option_group_name: None,
        percent_progress: None,
        storage_type: None,
        encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        timezone: None,
        storage_throughput: None,
        snapshot_attributes: std::collections::BTreeMap::new(),
    }
}

fn seed_snapshot(svc: &RdsService, snapshot_id: &str, instance_id: &str) {
    let mut __a = svc.state.write();
    let state = __a.default_mut();
    let arn = state.db_snapshot_arn(&state.region, snapshot_id);
    state.snapshots.insert(
        snapshot_id.to_string(),
        crate::state::DbSnapshot {
            db_snapshot_identifier: snapshot_id.to_string(),
            db_snapshot_arn: arn,
            db_instance_identifier: instance_id.to_string(),
            snapshot_create_time: Utc::now(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            allocated_storage: 20,
            status: "available".to_string(),
            port: 5432,
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            dbi_resource_id: format!("db-{}", Uuid::new_v4().simple()),
            snapshot_type: "manual".to_string(),
            master_user_password: "secret".to_string(),
            tags: Vec::new(),
            dump_data: Vec::new(),
            availability_zone: None,
            vpc_id: None,
            instance_create_time: None,
            license_model: None,
            iops: None,
            option_group_name: None,
            percent_progress: None,
            storage_type: None,
            encrypted: false,
            kms_key_id: None,
            iam_database_authentication_enabled: false,
            timezone: None,
            storage_throughput: None,
            snapshot_attributes: std::collections::BTreeMap::new(),
        },
    );
}

#[test]
fn migrate_loaded_retypes_persisted_final_snapshots() {
    // Final snapshots were once persisted as `automated`; AWS types them
    // `manual`. SnapshotType now actually narrows the result, so without
    // a migration a pre-existing row would silently vanish from
    // `--snapshot-type manual` after an upgrade.
    let svc = make_service();
    seed_snapshot(&svc, "final-snap", "db1");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .snapshots
            .get_mut("final-snap")
            .expect("seeded snapshot")
            .snapshot_type = "automated".to_string();
        state.migrate_loaded(crate::state::RDS_FINAL_SNAPSHOT_AUTOMATED_SCHEMA);
    }

    let req = request("DescribeDBSnapshots", &[("SnapshotType", "manual")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>final-snap</DBSnapshotIdentifier>"));
}

#[test]
fn migrate_loaded_leaves_newer_state_alone() {
    // The rewrite is only sound while nothing produces genuine automated
    // snapshots; a file written at a newer schema must pass through.
    let svc = make_service();
    seed_snapshot(&svc, "auto-snap", "db1");
    let mut accounts = svc.state.write();
    let state = accounts.default_mut();
    state
        .snapshots
        .get_mut("auto-snap")
        .expect("seeded snapshot")
        .snapshot_type = "automated".to_string();

    state.migrate_loaded(crate::state::RDS_SNAPSHOT_SCHEMA_VERSION);

    assert_eq!(state.snapshots["auto-snap"].snapshot_type, "automated");
}

#[tokio::test]
async fn restore_db_instance_from_a_cluster_snapshot() {
    // AWS models DBClusterSnapshotIdentifier on this operation for
    // Multi-AZ DB cluster snapshots; their metadata lives in the cluster
    // snapshot store, so resolving only `snapshots` always 404'd.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "clu-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "clu-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                    "EngineVersion": "8.0.mysql_aurora.3.04.0",
                    "MasterUsername": "admin",
                }),
            );
    }

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            ("DBClusterSnapshotIdentifier", "clu-snap"),
        ],
    );
    // Without a container runtime the restore fails after the lookup, so
    // any error other than a not-found proves the snapshot resolved.
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Ok(_) => {}
        Err(err) => assert!(
            !err.code().contains("NotFound"),
            "cluster snapshot did not resolve: {err:?}"
        ),
    }
}

#[tokio::test]
async fn create_db_instance_persists_the_domain_membership() {
    // The `domain` filter reads instance.domain, so an instance created
    // WITH a domain (rather than modified into one) has to carry it --
    // otherwise the filter is dead for the common case.
    // The stub runtime lets the create reach the point where the record
    // is staged; without one it fails before that and the assertions
    // below would never run.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    let req = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "domain-db"),
            ("DBInstanceClass", "db.t3.micro"),
            ("Engine", "postgres"),
            ("AllocatedStorage", "20"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secret123"),
            ("Domain", "d-1234567890"),
            ("DomainIAMRoleName", "rds-directory"),
        ],
    );
    svc.create_db_instance(&req)
        .await
        .expect("create with the stub runtime");

    let (domain, role) = svc
        .state
        .read()
        .default_ref()
        .instances
        .get("domain-db")
        .map(|instance| {
            (
                instance.domain.clone(),
                instance.domain_iam_role_name.clone(),
            )
        })
        // Requiring the row is the point: an `if let` here would let a
        // regression in create-time domain persistence pass silently.
        .expect("the created instance is recorded");
    assert_eq!(domain.as_deref(), Some("d-1234567890"));
    assert_eq!(role.as_deref(), Some("rds-directory"));
}

#[test]
fn describe_db_instances_filters_by_domain() {
    let svc = make_service();
    seed_instance(&svc, "joined");
    seed_instance(&svc, "standalone");
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .instances
            .get_mut("joined")
            .expect("seeded instance")
            .domain = Some("d-1234567890".to_string());
    }

    let req = request(
        "DescribeDBInstances",
        &[
            ("Filters.Filter.1.Name", "domain"),
            ("Filters.Filter.1.Values.Value.1", "d-1234567890"),
        ],
    );
    let body = body_of(svc.describe_db_instances(&req).unwrap());
    assert!(body.contains("<DBInstanceIdentifier>joined</DBInstanceIdentifier>"));
    assert!(!body.contains("<DBInstanceIdentifier>standalone</DBInstanceIdentifier>"));
}

#[test]
fn restored_instance_inherits_the_snapshots_encryption() {
    // The snapshot reports StorageEncrypted / KmsKeyId, so an instance
    // restored from it must too -- otherwise the pair is internally
    // inconsistent and Terraform diffs storage_encrypted forever. Both
    // the synthesized source AND the instance builder have to carry it;
    // fixing only one leaves the other dropping the value.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "enc-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "enc-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-postgresql",
                    "StorageEncrypted": true,
                    "KmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/abc",
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "enc-snap");
    assert!(source.encrypted);
    assert_eq!(
        source.kms_key_id.as_deref(),
        Some("arn:aws:kms:us-east-1:123456789012:key/abc")
    );

    let instance = crate::service::service_helpers::build_restored_instance(
        "restored-db",
        "arn:aws:rds:us-east-1:123456789012:db:restored-db".to_string(),
        "db-1".to_string(),
        Utc::now(),
        Vec::new(),
        &source,
        &crate::service::service_helpers::creating_placeholder_container(),
        Vec::new(),
    );
    assert!(
        instance.storage_encrypted,
        "the instance builder dropped the snapshot's encryption"
    );
    assert_eq!(
        instance.kms_key_id.as_deref(),
        Some("arn:aws:kms:us-east-1:123456789012:key/abc")
    );
}

#[test]
fn a_shared_snapshot_resolves_with_its_owners_instance_arn() {
    // The named form of a query the list form answers must not return
    // nothing: the instance ARN's account is checked against the row's
    // owner, not simply against the caller.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared.db_instance_identifier = "db-1".to_string();
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);
    }

    let req = request(
        "DescribeDBSnapshots",
        &[
            (
                "DBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:shared-snap",
            ),
            (
                "DBInstanceIdentifier",
                "arn:aws:rds:us-east-1:999999999999:db:db-1",
            ),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"),
        "the owner's instance ARN excluded their own shared snapshot: {body}"
    );
}

#[tokio::test]
async fn cluster_snapshot_restore_reports_the_writers_engine_version() {
    // Engine and EngineVersion must come from the same place: the
    // writer's engine against the cluster's Aurora version is a pair AWS
    // never reports, and a Terraform engine_version comparison would
    // diff forever.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "clu-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "clu-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                    "EngineVersion": "8.0.mysql_aurora.3.04.0",
                    "SourceEngine": "mysql",
                    "SourceEngineVersion": "8.0.36",
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "clu-snap");
    assert_eq!(source.engine, "mysql");
    assert_eq!(source.engine_version, "8.0.36");
}

#[tokio::test]
async fn cluster_snapshot_restore_uses_the_writers_engine_and_credentials() {
    // The dump was taken from the writer with ITS engine, credentials and
    // database. Rebuilding the source from the cluster row instead hands
    // the runtime `aurora-postgresql` (which no engine match accepts, so
    // the instance fails) and replays the dump into the wrong database
    // under the wrong password.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "clu-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "clu-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    // The cluster's own family, which is not a container engine.
                    "Engine": "aurora-postgresql",
                    "MasterUsername": "cluster-admin",
                    "MasterUserPassword": "cluster-pw",
                    // What the writer actually ran when the dump was taken.
                    "SourceEngine": "postgres",
                    "SourceMasterUsername": "writer-admin",
                    "SourceMasterUserPassword": "writer-pw",
                    "SourceDBName": "appdb",
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "clu-snap");
    assert_eq!(source.engine, "postgres");
    assert_eq!(source.master_username, "writer-admin");
    assert_eq!(source.master_user_password, "writer-pw");
    assert_eq!(source.db_name.as_deref(), Some("appdb"));
}

#[tokio::test]
async fn restored_cluster_does_not_carry_the_sources_writer_settings() {
    // The writer settings describe the SOURCE cluster's writer. Left on
    // the restored row they survive into a later snapshot of it (taken
    // before an instance attaches) and an instance restore then starts
    // with the old credentials and database.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "SourceEngine": "postgres",
                    "SourceMasterUsername": "old-admin",
                    "SourceMasterUserPassword": "old-pw",
                    "SourceDBName": "olddb",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    let restored = cluster_entry(&svc, "restored");
    for key in [
        "SourceEngine",
        "SourceMasterUsername",
        "SourceMasterUserPassword",
        "SourceDBName",
    ] {
        assert!(
            restored.get(key).is_none(),
            "restored cluster kept {key} from the source"
        );
    }
}

#[tokio::test]
async fn cluster_snapshot_restore_replays_a_staged_dump() {
    // A cluster restored from a snapshot stages its data under
    // PendingRestoreDumpB64 until an instance attaches; a snapshot taken
    // in that window carries the key verbatim. The cluster restore
    // replays it, so the instance restore must too.
    let svc = make_service();
    {
        use base64::Engine;
        let dump = base64::engine::general_purpose::STANDARD.encode(b"-- staged --");
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "staged-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "staged-snap",
                    "DBClusterIdentifier": "restored-cluster",
                    "Status": "available",
                    "Engine": "aurora-postgresql",
                    "PendingRestoreDumpB64": dump,
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "staged-snap");
    assert_eq!(source.dump_data, b"-- staged --".to_vec());
}

#[tokio::test]
async fn a_remapped_engine_never_keeps_the_aurora_version() {
    // A snapshot taken before any writer attached records no
    // SourceEngine, so the family is remapped to a container engine --
    // and the cluster's Aurora version must not ride along with it.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "no-writer".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "no-writer",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                    "EngineVersion": "8.0.mysql_aurora.3.04.0",
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "no-writer");
    assert_eq!(source.engine, "mysql");
    assert_ne!(
        source.engine_version, "8.0.mysql_aurora.3.04.0",
        "a remapped engine kept the cluster's Aurora version"
    );

    // A non-Aurora cluster keeps its own version, since nothing was
    // remapped.
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "plain".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "plain",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "postgres",
                    "EngineVersion": "16.3",
                }),
            );
    }
    let source = cluster_snapshot_source_for_test(&svc, "plain");
    assert_eq!(source.engine, "postgres");
    assert_eq!(source.engine_version, "16.3");
}

#[tokio::test]
async fn a_restored_cluster_is_not_reported_as_a_copy() {
    // SourceDBClusterSnapshotArn describes the snapshot a COPY came
    // from. Left on a restored cluster row it propagates into the next
    // snapshot of that cluster, which the renderer now reports.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "SourceDBClusterSnapshotArn":
                        "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:unrelated",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    assert!(
        cluster_entry(&svc, "restored")
            .get("SourceDBClusterSnapshotArn")
            .is_none(),
        "the restored cluster claims to be a copy of an unrelated snapshot"
    );
}

#[tokio::test]
async fn cluster_snapshot_restore_maps_an_aurora_family_to_its_engine() {
    // A metadata-only snapshot (no writer recorded) still must not hand
    // the runtime an `aurora-*` engine it cannot start.
    let svc = make_service();
    for (snapshot_id, family, expected) in [
        ("aurora-pg", "aurora-postgresql", "postgres"),
        ("aurora-my", "aurora-mysql", "mysql"),
    ] {
        {
            let mut accounts = svc.state.write();
            accounts
                .default_mut()
                .extras
                .entry("cluster_snapshots".to_string())
                .or_default()
                .insert(
                    snapshot_id.to_string(),
                    serde_json::json!({
                        "DBClusterSnapshotIdentifier": snapshot_id,
                        "DBClusterIdentifier": "src-cluster",
                        "Status": "available",
                        "Engine": family,
                    }),
                );
        }

        let source = cluster_snapshot_source_for_test(&svc, snapshot_id);
        assert_eq!(source.engine, expected, "family {family}");
    }
}

#[tokio::test]
async fn cluster_snapshot_restore_carries_the_dump_and_credentials() {
    // A restore that reports available with an empty database, or that
    // hands the container an empty password (which the engines reject),
    // is a silent data-loss / failed-instance bug.
    let svc = make_service();
    {
        use base64::Engine;
        let dump = base64::engine::general_purpose::STANDARD.encode(b"-- dump --");
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "clu-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "clu-snap",
                    "DBClusterSnapshotArn":
                        "arn:aws:rds:us-east-1:999999999999:cluster-snapshot:clu-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                    "MasterUsername": "admin",
                    "MasterUserPassword": "s3cret",
                    "DumpDataB64": dump,
                }),
            );
    }

    let source = cluster_snapshot_source_for_test(&svc, "clu-snap");
    assert_eq!(source.dump_data, b"-- dump --".to_vec());
    assert_eq!(source.master_user_password, "s3cret");
    // The ARN names the snapshot's owner, not the caller.
    assert!(source.db_snapshot_arn.contains("999999999999"));
}

#[tokio::test]
async fn cluster_snapshot_restore_validates_the_subnet_group() {
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "clu-snap".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "clu-snap",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                }),
            );
    }

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            ("DBClusterSnapshotIdentifier", "clu-snap"),
            ("DBSubnetGroupName", "ghost-group"),
        ],
    );
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Err(err) => assert_eq!(err.code(), "DBSubnetGroupNotFoundFault"),
        Ok(_) => panic!("restore accepted a nonexistent subnet group"),
    }
}

#[test]
fn describe_db_snapshots_reports_a_foreign_instance_arns_shared_snapshots() {
    // A DB instance ARN naming another account matches none of this
    // account's snapshots -- but the owner may have shared snapshots OF
    // that instance, and those must still be listed.
    let svc = make_service();
    seed_snapshot(&svc, "mine", "src");
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared.db_instance_identifier = "src".to_string();
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);
    }

    let req = request(
        "DescribeDBSnapshots",
        &[
            (
                "DBInstanceIdentifier",
                "arn:aws:rds:us-east-1:999999999999:db:src",
            ),
            ("IncludeShared", "true"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"),
        "the owner's shared snapshots were suppressed: {body}"
    );
    // This account's own snapshot of a same-named instance is not the
    // one the ARN named.
    assert!(!body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));
}

#[test]
fn describe_db_snapshots_reports_shared_and_public_snapshots() {
    // `shared` / `public` select snapshots another account shared via
    // ModifyDBSnapshotAttribute's `restore` attribute; they are not the
    // caller's own snapshots, so an owned-type match would return
    // nothing.
    let svc = make_service();
    seed_snapshot(&svc, "mine", "db1");
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);

        let mut public = other_account_snapshot("public-snap");
        public
            .snapshot_attributes
            .insert("restore".to_string(), vec!["all".to_string()]);
        other.snapshots.insert("public-snap".to_string(), public);

        let unshared = other_account_snapshot("private-snap");
        other.snapshots.insert("private-snap".to_string(), unshared);
    }

    let req = request("DescribeDBSnapshots", &[("SnapshotType", "shared")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>private-snap</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));

    let req = request("DescribeDBSnapshots", &[("SnapshotType", "public")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>public-snap</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));

    // IncludeShared / IncludePublic widen an otherwise-unqualified list.
    let req = request("DescribeDBSnapshots", &[]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));

    let req = request("DescribeDBSnapshots", &[("IncludeShared", "true")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));
    assert!(body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));

    let req = request("DescribeDBSnapshots", &[("IncludePublic", "true")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>public-snap</DBSnapshotIdentifier>"));
}

#[test]
fn include_shared_does_not_widen_an_owned_snapshot_type() {
    // AWS: IncludeShared / IncludePublic don't apply when SnapshotType
    // selects an owned type, and a foreign row must never slip past the
    // type selector.
    let svc = make_service();
    seed_snapshot(&svc, "mine", "db1");
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared.snapshot_type = "automated".to_string();
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);
    }

    let req = request(
        "DescribeDBSnapshots",
        &[("SnapshotType", "manual"), ("IncludeShared", "true")],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"),
        "IncludeShared widened an owned SnapshotType: {body}"
    );
}

#[test]
fn describe_db_snapshots_resolves_a_shared_snapshot_by_identifier() {
    // The list path reports it, so re-reading it by id (the Terraform
    // read-back pattern) must not 404.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);
    }

    // AWS requires the ARN to address a snapshot another account shared
    // with you; a bare id would also be ambiguous once two accounts have
    // shared one under the same name.
    let shared_arn = "arn:aws:rds:us-east-1:999999999999:snapshot:shared-snap";
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", shared_arn),
            ("SnapshotType", "shared"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));

    // The bare id resolves nothing: this account owns no such snapshot.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", "shared-snap"),
            ("SnapshotType", "shared"),
        ],
    );
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");

    // A snapshot nobody shared stays invisible.
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        other.snapshots.insert(
            "private-snap".to_string(),
            other_account_snapshot("private-snap"),
        );
    }
    let req = request(
        "DescribeDBSnapshots",
        &[("DBSnapshotIdentifier", "private-snap")],
    );
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");

    // IncludeShared on an unqualified read resolves it too -- the list
    // path reports it under the same flag.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", shared_arn),
            ("IncludeShared", "true"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"),
        "IncludeShared read-back returned nothing: {body}"
    );

    // Naming the snapshot explicitly resolves it without IncludeShared:
    // AWS resolves a shared snapshot addressed by id or ARN. (The flag
    // still gates whether it appears in an unqualified *list*.)
    let req = request(
        "DescribeDBSnapshots",
        &[("DBSnapshotIdentifier", shared_arn)],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));

    let req = request("DescribeDBSnapshots", &[]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        !body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"),
        "unqualified list leaked a shared snapshot: {body}"
    );
}

#[test]
fn include_public_does_not_apply_to_snapshot_type_shared() {
    // AWS: "The IncludePublic parameter doesn't apply when SnapshotType
    // is set to shared" (and IncludeShared doesn't apply to `public`).
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);

        let mut public = other_account_snapshot("public-snap");
        public
            .snapshot_attributes
            .insert("restore".to_string(), vec!["all".to_string()]);
        other.snapshots.insert("public-snap".to_string(), public);
    }

    let req = request(
        "DescribeDBSnapshots",
        &[("SnapshotType", "shared"), ("IncludePublic", "true")],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>shared-snap</DBSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBSnapshotIdentifier>public-snap</DBSnapshotIdentifier>"),
        "IncludePublic applied to SnapshotType=shared: {body}"
    );
}

#[test]
fn describe_db_snapshots_tolerates_a_junk_include_flag() {
    // InvalidParameterValue isn't declared on this op, so an unparsable
    // boolean is treated as absent rather than rejected.
    let svc = make_service();
    seed_snapshot(&svc, "mine", "db1");

    let req = request("DescribeDBSnapshots", &[("IncludeShared", "yes-please")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine</DBSnapshotIdentifier>"));
}

#[test]
fn a_foreign_arn_is_never_typed_as_owned() {
    // Ownership must come from where the row resolved, not from a bare
    // id probe: with a local namesake, a foreign row was typed "owned",
    // so `--snapshot-type shared` dropped it while `--snapshot-type
    // manual` wrongly returned another account's snapshot.
    let svc = make_service();
    seed_snapshot(&svc, "snap-1", "my-db");
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("snap-1");
        shared.db_instance_identifier = "their-db".to_string();
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("snap-1".to_string(), shared);
    }

    let foreign_arn = "arn:aws:rds:us-east-1:999999999999:snapshot:snap-1";

    // The shared row answers to `shared`...
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", foreign_arn),
            ("SnapshotType", "shared"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        body.contains("<DBInstanceIdentifier>their-db</DBInstanceIdentifier>"),
        "shared lookup dropped the foreign row: {body}"
    );

    // ...and not to an owned type.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", foreign_arn),
            ("SnapshotType", "manual"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        !body.contains("<DBSnapshotIdentifier>"),
        "a foreign row answered to an owned SnapshotType: {body}"
    );

    // The snapshot-type filter agrees with the parameter.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", foreign_arn),
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "shared"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBInstanceIdentifier>their-db</DBInstanceIdentifier>"));
}

#[test]
fn describe_db_snapshots_arn_account_wins_over_a_local_namesake() {
    // Resolving the local row first would return B's own `prod-snap` for
    // an ARN that named account A -- the same aliasing the delete path
    // guards against.
    let svc = make_service();
    seed_snapshot(&svc, "prod-snap", "my-db");
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("prod-snap");
        shared.db_instance_identifier = "their-db".to_string();
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("prod-snap".to_string(), shared);
    }

    let req = request(
        "DescribeDBSnapshots",
        &[(
            "DBSnapshotIdentifier",
            "arn:aws:rds:us-east-1:999999999999:snapshot:prod-snap",
        )],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(
        body.contains("<DBInstanceIdentifier>their-db</DBInstanceIdentifier>"),
        "a foreign ARN resolved to the local namesake: {body}"
    );
    assert!(!body.contains("<DBInstanceIdentifier>my-db</DBInstanceIdentifier>"));
}

#[tokio::test]
async fn restore_db_instance_refuses_a_local_namesake_for_a_foreign_arn() {
    // Hydrating from the local snapshot would silently restore the wrong
    // data (engine, credentials, dump) and report success.
    let svc = make_service();
    seed_snapshot(&svc, "prod-snap", "my-db");

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            (
                "DBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:prod-snap",
            ),
        ],
    );
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Ok(_) => panic!("restored from the local namesake of a foreign ARN"),
        Err(err) => assert!(
            format!("{err:?}").contains("DBSnapshotNotFound"),
            "unexpected error: {err:?}"
        ),
    }
}

#[test]
fn snapshot_type_filter_speaks_the_same_vocabulary_as_the_parameter() {
    // A snapshot reported by `--snapshot-type public` must also match
    // `--filters Name=snapshot-type,Values=public`.
    let svc = make_service();
    seed_snapshot(&svc, "mine-public", "db1");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .snapshots
            .get_mut("mine-public")
            .expect("seeded snapshot")
            .snapshot_attributes
            .insert("restore".to_string(), vec!["all".to_string()]);
    }

    for params in [
        vec![("SnapshotType", "public")],
        vec![
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "public"),
        ],
    ] {
        let req = request("DescribeDBSnapshots", &params);
        let body = body_of(svc.describe_db_snapshots(&req).unwrap());
        assert!(
            body.contains("<DBSnapshotIdentifier>mine-public</DBSnapshotIdentifier>"),
            "public snapshot missed by {params:?}: {body}"
        );
    }

    // The stored type still matches too.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "manual"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine-public</DBSnapshotIdentifier>"));
}

#[test]
fn delete_db_snapshot_refuses_another_accounts_arn() {
    // Reducing a foreign ARN to its bare id would delete THIS account's
    // same-named snapshot while the client believes it addressed the
    // other account's.
    let svc = make_service();
    seed_snapshot(&svc, "prod-snap", "db1");

    let req = request(
        "DeleteDBSnapshot",
        &[(
            "DBSnapshotIdentifier",
            "arn:aws:rds:us-east-1:999999999999:snapshot:prod-snap",
        )],
    );
    assert_code(svc.delete_db_snapshot(&req), "DBSnapshotNotFound");
    assert!(
        svc.state
            .read()
            .default_ref()
            .snapshots
            .contains_key("prod-snap"),
        "a foreign ARN deleted the local snapshot"
    );
}

#[test]
fn describe_db_snapshots_reports_an_owned_public_snapshot() {
    // AWS: "public - Return all DB snapshots that have been marked as
    // public" -- not scoped to other accounts.
    let svc = make_service();
    seed_snapshot(&svc, "mine-public", "db1");
    seed_snapshot(&svc, "mine-private", "db1");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .snapshots
            .get_mut("mine-public")
            .expect("seeded snapshot")
            .snapshot_attributes
            .insert("restore".to_string(), vec!["all".to_string()]);
    }

    let req = request("DescribeDBSnapshots", &[("SnapshotType", "public")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>mine-public</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>mine-private</DBSnapshotIdentifier>"));

    // `shared` is "shared TO me", which an owned snapshot never is.
    let req = request("DescribeDBSnapshots", &[("SnapshotType", "shared")]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(!body.contains("<DBSnapshotIdentifier>mine-public</DBSnapshotIdentifier>"));
}

#[tokio::test]
async fn restore_db_instance_from_a_shared_snapshot() {
    // DescribeDBSnapshots reports snapshots other accounts shared with
    // the caller, so restoring from one must work -- otherwise the
    // sharing surface is listable but unusable. Without a container
    // runtime the restore fails after the lookup, so anything other than
    // DBSnapshotNotFound proves the snapshot resolved.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = other_account_snapshot("shared-snap");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["123456789012".to_string()]);
        other.snapshots.insert("shared-snap".to_string(), shared);
    }

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            (
                "DBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:shared-snap",
            ),
        ],
    );
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Ok(_) => {}
        Err(err) => assert!(
            !format!("{err:?}").contains("DBSnapshotNotFound"),
            "shared snapshot did not resolve for restore: {err:?}"
        ),
    }
}

#[test]
fn delete_db_snapshot_accepts_an_arn_identifier() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    let arn = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap1")
        .expect("seeded snapshot")
        .db_snapshot_arn
        .clone();

    let req = request("DeleteDBSnapshot", &[("DBSnapshotIdentifier", &arn)]);
    svc.delete_db_snapshot(&req)
        .expect("ARN-form identifier should resolve");
    assert!(svc.state.read().default_ref().snapshots.is_empty());
}

#[test]
fn describe_db_snapshots_accepts_an_arn_identifier() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    seed_snapshot(&svc, "snap2", "db2");
    let arn = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap1")
        .expect("seeded snapshot")
        .db_snapshot_arn
        .clone();

    let req = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", &arn)]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>snap1</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>snap2</DBSnapshotIdentifier>"));
}

#[tokio::test]
async fn restore_db_instance_from_db_snapshot_accepts_an_arn_identifier() {
    // `aws_db_instance.snapshot_identifier` in the Terraform provider
    // holds a full snapshot ARN. Without a container runtime the restore
    // fails after the lookup, so assert on the error we get: anything
    // other than DBSnapshotNotFound proves the ARN resolved.
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    let arn = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap1")
        .expect("seeded snapshot")
        .db_snapshot_arn
        .clone();

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            ("DBSnapshotIdentifier", &arn),
        ],
    );
    match svc.restore_db_instance_from_db_snapshot(&req).await {
        Ok(_) => {}
        Err(err) => assert!(
            !format!("{err:?}").contains("DBSnapshotNotFound"),
            "ARN-form snapshot identifier did not resolve: {err:?}"
        ),
    }
}

#[test]
fn describe_db_snapshots_honors_the_dbi_resource_id_parameter() {
    // The modeled parameter, not the filter: ignoring it returns every
    // snapshot to a client that asked for one instance's -- the same
    // failure #2481 reported for the filter form.
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    seed_snapshot(&svc, "snap2", "db2");
    let wanted = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap2")
        .expect("seeded snapshot")
        .dbi_resource_id
        .clone();

    let req = request("DescribeDBSnapshots", &[("DbiResourceId", &wanted)]);
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(body.contains("<DBSnapshotIdentifier>snap2</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>snap1</DBSnapshotIdentifier>"));

    // AND-ed with a named snapshot, like every other narrowing parameter.
    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", "snap1"),
            ("DbiResourceId", &wanted),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(!body.contains("<DBSnapshotIdentifier>"), "body: {body}");
}

#[tokio::test]
async fn an_explicit_domain_replaces_every_source_field() {
    // Keeping the source's DNS IPs beside a different domain name is an
    // incoherent DomainMembership; all six fields move together.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    seed_instance(&svc, "source-db");
    {
        let mut accounts = svc.state.write();
        let source = accounts
            .default_mut()
            .instances
            .get_mut("source-db")
            .expect("seeded instance");
        source.domain = Some("d-old".to_string());
        source.domain_dns_ips = vec!["10.0.0.1".to_string()];
    }

    let req = request(
        "RestoreDBInstanceToPointInTime",
        &[
            ("SourceDBInstanceIdentifier", "source-db"),
            ("TargetDBInstanceIdentifier", "pit-db"),
            ("UseLatestRestorableTime", "true"),
            ("Domain", "d-new"),
        ],
    );
    svc.restore_db_instance_to_point_in_time(&req)
        .await
        .expect("PITR with the stub runtime");

    let restored = svc
        .state
        .read()
        .default_ref()
        .instances
        .get("pit-db")
        .map(|i| (i.domain.clone(), i.domain_dns_ips.clone()))
        .expect("the restored instance is recorded");
    assert_eq!(restored.0.as_deref(), Some("d-new"));
    assert!(
        restored.1.is_empty(),
        "the new domain kept the source's DNS IPs: {:?}",
        restored.1
    );
}

#[tokio::test]
async fn point_in_time_restore_carries_the_requested_domain() {
    // Modeled on the request, and an explicit Domain overrides whatever
    // the source carried -- otherwise the new instance is invisible to
    // the `domain` filter.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    seed_instance(&svc, "source-db");

    let req = request(
        "RestoreDBInstanceToPointInTime",
        &[
            ("SourceDBInstanceIdentifier", "source-db"),
            ("TargetDBInstanceIdentifier", "pit-db"),
            ("UseLatestRestorableTime", "true"),
            ("Domain", "d-1234567890"),
            ("DomainIAMRoleName", "rds-directory"),
        ],
    );
    svc.restore_db_instance_to_point_in_time(&req)
        .await
        .expect("PITR with the stub runtime");

    let (domain, role) = svc
        .state
        .read()
        .default_ref()
        .instances
        .get("pit-db")
        .map(|instance| {
            (
                instance.domain.clone(),
                instance.domain_iam_role_name.clone(),
            )
        })
        .expect("the restored instance is recorded");
    assert_eq!(domain.as_deref(), Some("d-1234567890"));
    assert_eq!(role.as_deref(), Some("rds-directory"));
}

#[tokio::test]
async fn restore_db_instance_carries_the_requested_domain() {
    // Settable on the restore request as it is on create; dropping it
    // leaves the instance invisible to the `domain` filter.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    seed_snapshot(&svc, "snap1", "db1");

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored-db"),
            ("DBSnapshotIdentifier", "snap1"),
            ("Domain", "d-1234567890"),
            ("DomainIAMRoleName", "rds-directory"),
        ],
    );
    svc.restore_db_instance_from_db_snapshot(&req)
        .await
        .expect("restore with the stub runtime");

    let (domain, role) = svc
        .state
        .read()
        .default_ref()
        .instances
        .get("restored-db")
        .map(|instance| {
            (
                instance.domain.clone(),
                instance.domain_iam_role_name.clone(),
            )
        })
        .expect("the restored instance is recorded");
    assert_eq!(domain.as_deref(), Some("d-1234567890"));
    assert_eq!(role.as_deref(), Some("rds-directory"));
}

#[test]
fn describe_db_snapshots_filters_by_dbi_resource_id() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    seed_snapshot(&svc, "snap2", "db2");
    let wanted = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap2")
        .expect("seeded snapshot")
        .dbi_resource_id
        .clone();

    let req = request(
        "DescribeDBSnapshots",
        &[
            ("Filters.Filter.1.Name", "dbi-resource-id"),
            ("Filters.Filter.1.Values.Value.1", &wanted),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());

    assert!(body.contains("<DBSnapshotIdentifier>snap2</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>snap1</DBSnapshotIdentifier>"));
}

#[test]
fn describe_db_snapshots_ignores_another_accounts_instance_arn() {
    // A DB instance is never cross-account, so an ARN naming another
    // account must not list this account's same-named instance's
    // snapshots.
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "mydb");

    let req = request(
        "DescribeDBSnapshots",
        &[(
            "DBInstanceIdentifier",
            "arn:aws:rds:us-east-1:999999999999:db:mydb",
        )],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(!body.contains("<DBSnapshotIdentifier>"), "body: {body}");
}

#[test]
fn describe_db_snapshots_filters_by_db_instance_id() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    seed_snapshot(&svc, "snap2", "db2");

    let req = request(
        "DescribeDBSnapshots",
        &[
            ("Filters.Filter.1.Name", "db-instance-id"),
            ("Filters.Filter.1.Values.Value.1", "db1"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());

    assert!(body.contains("<DBSnapshotIdentifier>snap1</DBSnapshotIdentifier>"));
    assert!(!body.contains("<DBSnapshotIdentifier>snap2</DBSnapshotIdentifier>"));
}

#[test]
fn describe_db_snapshots_honors_snapshot_type() {
    let svc = make_service();
    seed_snapshot(&svc, "manual-snap", "db1");
    seed_snapshot(&svc, "auto-snap", "db1");
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .snapshots
            .get_mut("auto-snap")
            .expect("seeded snapshot")
            .snapshot_type = "automated".to_string();
    }

    // Both the SnapshotType parameter and the snapshot-type filter
    // narrow the result the same way.
    for params in [
        vec![("SnapshotType", "automated")],
        vec![
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "automated"),
        ],
    ] {
        let req = request("DescribeDBSnapshots", &params);
        let body = body_of(svc.describe_db_snapshots(&req).unwrap());
        assert!(
            body.contains("<DBSnapshotIdentifier>auto-snap</DBSnapshotIdentifier>"),
            "body: {body}"
        );
        assert!(!body.contains("<DBSnapshotIdentifier>manual-snap</DBSnapshotIdentifier>"));
    }
}

#[test]
fn describe_db_snapshots_snapshot_type_is_anded_with_the_identifier() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");

    let req = request(
        "DescribeDBSnapshots",
        &[
            ("DBSnapshotIdentifier", "snap1"),
            ("SnapshotType", "automated"),
        ],
    );
    let body = body_of(svc.describe_db_snapshots(&req).unwrap());
    assert!(!body.contains("<DBSnapshotIdentifier>"), "body: {body}");
}

#[test]
fn delete_db_snapshot_removes_entry() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    let req = request("DeleteDBSnapshot", &[("DBSnapshotIdentifier", "snap1")]);
    svc.delete_db_snapshot(&req).unwrap();
    assert!(svc.state.read().default_ref().snapshots.is_empty());
}

#[test]
fn delete_db_snapshot_unknown_errors() {
    let svc = make_service();
    let req = request("DeleteDBSnapshot", &[("DBSnapshotIdentifier", "ghost")]);
    assert_code(svc.delete_db_snapshot(&req), "DBSnapshotNotFound");
}

/// Force a seeded snapshot's status to `creating` (persisted mid-dump).
fn set_snapshot_creating(svc: &RdsService, snapshot_id: &str) {
    svc.state
        .write()
        .default_mut()
        .snapshots
        .get_mut(snapshot_id)
        .unwrap()
        .status = "creating".to_string();
}

fn snapshot_status(svc: &RdsService, snapshot_id: &str) -> String {
    svc.state
        .read()
        .default_ref()
        .snapshots
        .get(snapshot_id)
        .unwrap()
        .status
        .clone()
}

#[test]
fn reconcile_rearms_creating_snapshot_when_source_present() {
    // 0.1: a `creating` snapshot present at load whose source instance still
    // exists is re-armed (not lost, not marked terminal).
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_snapshot(&svc, "snap1", "db1");
    set_snapshot_creating(&svc, "snap1");
    // has_runtime=true exercises the re-arm branch without Docker.
    let (rearm, reap) = svc.plan_snapshot_recovery(true);
    assert_eq!(rearm.len(), 1);
    assert_eq!(rearm[0].snapshot_id, "snap1");
    assert_eq!(rearm[0].source_id, "db1");
    assert_eq!(rearm[0].db_name, "appdb");
    assert!(reap.is_empty());
    // Left `creating`; the re-armed dump flips it to `available`.
    assert_eq!(snapshot_status(&svc, "snap1"), "creating");
}

#[test]
fn reconcile_fails_and_reaps_creating_snapshot_when_source_gone() {
    // 0.1: a final-snapshot orphan (source instance row removed synchronously
    // by DeleteDBInstance) is marked `failed` so the id is reusable, and its
    // leaked source container/volume is queued for reaping.
    let svc = make_service();
    seed_snapshot(&svc, "final-snap", "db-gone"); // no instance seeded
    set_snapshot_creating(&svc, "final-snap");
    let (rearm, reap) = svc.plan_snapshot_recovery(true);
    assert!(rearm.is_empty());
    assert_eq!(reap.len(), 1);
    assert_eq!(reap[0].source_id, "db-gone");
    assert_eq!(snapshot_status(&svc, "final-snap"), "failed");
}

#[test]
fn reconcile_fails_creating_snapshot_without_runtime() {
    // 0.1: no runtime on restart means the dump can never complete, so a
    // source-present `creating` snapshot is marked terminal `failed` rather
    // than left stuck `creating`. No reap (the source instance is present).
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_snapshot(&svc, "snap1", "db1");
    set_snapshot_creating(&svc, "snap1");
    let (rearm, reap) = svc.plan_snapshot_recovery(false);
    assert!(rearm.is_empty());
    assert!(reap.is_empty());
    assert_eq!(snapshot_status(&svc, "snap1"), "failed");
}

#[test]
fn reconcile_ignores_available_snapshots() {
    // 0.1: only `creating` rows are reconciled; a completed snapshot is left
    // untouched.
    let svc = make_service();
    seed_instance(&svc, "db1");
    seed_snapshot(&svc, "snap1", "db1"); // status `available` by default
    let (rearm, reap) = svc.plan_snapshot_recovery(true);
    assert!(rearm.is_empty());
    assert!(reap.is_empty());
    assert_eq!(snapshot_status(&svc, "snap1"), "available");
}

/// The source-snapshot record the cluster-snapshot restore synthesizes,
/// for asserting on the dump / credentials / ARN it carries.
fn cluster_snapshot_source_for_test(
    svc: &RdsService,
    snapshot_id: &str,
) -> crate::state::DbSnapshot {
    let accounts = svc.state.read();
    let entry = accounts
        .default_ref()
        .extras
        .get("cluster_snapshots")
        .and_then(|m| m.get(snapshot_id))
        .cloned()
        .expect("seeded cluster snapshot");
    super::snapshots::cluster_snapshot_as_source(&entry, snapshot_id, "123456789012", "us-east-1")
}

/// Seed a cluster entry with the identity fields a restore must not
/// inherit verbatim.
fn seed_cluster_entry(svc: &RdsService, id: &str, extra: serde_json::Value) {
    let mut accounts = svc.state.write();
    let state = accounts.default_mut();
    let mut entry = serde_json::json!({
        "DBClusterIdentifier": id,
        "DBClusterArn": format!("arn:aws:rds:us-east-1:123456789012:cluster:{id}"),
        "Engine": "postgres",
        "Status": "available",
        "DbClusterResourceId": format!("cluster-{id}"),
    });
    if let (Some(obj), Some(extra)) = (entry.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            obj.insert(k.clone(), v.clone());
        }
    }
    state
        .extras
        .entry("clusters".to_string())
        .or_default()
        .insert(id.to_string(), entry);
}

fn cluster_entry(svc: &RdsService, id: &str) -> serde_json::Value {
    svc.state
        .read()
        .default_ref()
        .extras
        .get("clusters")
        .and_then(|m| m.get(id))
        .cloned()
        .unwrap_or_else(|| panic!("cluster {id} not recorded"))
}

#[tokio::test]
async fn restore_db_cluster_to_point_in_time_assigns_its_own_resource_id() {
    // The restored cluster is a new resource: inheriting the source's
    // immutable resource id makes `db-cluster-resource-id` — a unique
    // match on AWS — select two clusters.
    let svc = make_service();
    seed_cluster_entry(&svc, "src-cluster", serde_json::json!({}));

    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "restored-cluster"),
            ("SourceDBClusterIdentifier", "src-cluster"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    svc.restore_db_cluster_to_point_in_time(&req).await.unwrap();

    let restored = cluster_entry(&svc, "restored-cluster");
    let source = cluster_entry(&svc, "src-cluster");
    assert_ne!(
        restored["DbClusterResourceId"], source["DbClusterResourceId"],
        "restored cluster reused the source resource id"
    );
    assert!(restored["DbClusterResourceId"]
        .as_str()
        .unwrap_or_default()
        .starts_with("cluster-"));
}

#[tokio::test]
async fn restore_db_cluster_to_point_in_time_clone_group_follows_restore_type() {
    let svc = make_service();
    seed_cluster_entry(&svc, "src-cluster", serde_json::json!({}));

    // copy-on-write: clone and source share a clone group.
    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "clone-cluster"),
            ("SourceDBClusterIdentifier", "src-cluster"),
            ("RestoreType", "copy-on-write"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    svc.restore_db_cluster_to_point_in_time(&req).await.unwrap();

    let clone_group = cluster_entry(&svc, "clone-cluster")["CloneGroupId"]
        .as_str()
        .expect("clone carries a clone group")
        .to_string();
    assert_eq!(
        cluster_entry(&svc, "src-cluster")["CloneGroupId"].as_str(),
        Some(clone_group.as_str()),
        "source was not stamped with the shared clone group"
    );

    // full-copy (the default) is an independent cluster and must not
    // inherit the source's group.
    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "full-copy-cluster"),
            ("SourceDBClusterIdentifier", "src-cluster"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    svc.restore_db_cluster_to_point_in_time(&req).await.unwrap();
    assert!(
        cluster_entry(&svc, "full-copy-cluster")
            .get("CloneGroupId")
            .is_none(),
        "full-copy restore inherited the source clone group"
    );
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_accepts_an_arn_snapshot_identifier() {
    // The Terraform provider stores a full ARN in `snapshot_identifier`.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Engine": "postgres",
                    "Status": "available",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored-cluster"),
            (
                "SnapshotIdentifier",
                "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:snap-1",
            ),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req)
        .await
        .expect("ARN-form snapshot identifier should resolve");
    assert_eq!(
        cluster_entry(&svc, "restored-cluster")["DBClusterIdentifier"].as_str(),
        Some("restored-cluster")
    );
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_drops_inherited_identity() {
    // CreateDBClusterSnapshot copies the whole cluster JSON, so the
    // snapshot carries the source's resource id and clone group. A
    // restore is an independent full copy of both.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Engine": "postgres",
                    "Status": "available",
                    "DbClusterResourceId": "cluster-source",
                    "CloneGroupId": "clone-group-source",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored-cluster"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    let restored = cluster_entry(&svc, "restored-cluster");
    assert_ne!(
        restored["DbClusterResourceId"].as_str(),
        Some("cluster-source"),
        "restored cluster reused the snapshot's resource id"
    );
    assert!(
        restored.get("CloneGroupId").is_none(),
        "restored cluster inherited the source clone group"
    );
}

#[tokio::test]
async fn create_db_cluster_snapshot_reports_an_unknown_cluster_first() {
    // A duplicate id against a cluster that doesn't exist is a
    // DBClusterNotFoundFault, not AlreadyExists.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({"DBClusterSnapshotIdentifier": "snap-1"}),
            );
    }

    let req = request(
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "snap-2"),
            ("DBClusterIdentifier", "ghost-cluster"),
        ],
    );
    match svc.create_db_cluster_snapshot(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("snapshot of a nonexistent cluster accepted"),
    }
}

#[tokio::test]
async fn create_db_cluster_snapshot_rejects_a_duplicate_identifier() {
    let svc = make_service();
    seed_cluster_entry(&svc, "clu-1", serde_json::json!({}));
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({"DBClusterSnapshotIdentifier": "snap-1"}),
            );
    }

    let req = request(
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("DBClusterIdentifier", "clu-1"),
        ],
    );
    match svc.create_db_cluster_snapshot(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotAlreadyExistsFault"),
        Ok(_) => panic!("duplicate snapshot identifier accepted"),
    }
}

#[test]
fn describe_db_snapshots_not_found_echoes_the_caller_identifier() {
    let svc = make_service();
    let arn = "arn:aws:rds:us-east-1:999999999999:snapshot:snap-1";

    let req = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", arn)]);
    match svc.describe_db_snapshots(&req) {
        Err(err) => {
            let message = format!("{err:?}");
            assert!(
                message.contains(arn),
                "the error reported the reduced id, not the caller's ARN: {message}"
            );
        }
        Ok(_) => panic!("unknown snapshot should fault"),
    }
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_carries_the_snapshot_fields() {
    // CreateDBClusterSnapshot copies the whole cluster row in, so the
    // restore reflects the snapshot -- including changes made to the
    // source cluster only BEFORE the snapshot was taken.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "Engine": "aurora-mysql",
                    "EngineVersion": "8.0.mysql_aurora.3.04.0",
                    "BackupRetentionPeriod": 21,
                }),
            );
        // A same-named local cluster with different settings must not be
        // what the restore reads.
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(
                "src-cluster".to_string(),
                serde_json::json!({
                    "DBClusterIdentifier": "src-cluster",
                    "Engine": "aurora-postgresql",
                    "EngineVersion": "16.9",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    let restored = cluster_entry(&svc, "restored");
    assert_eq!(restored["Engine"].as_str(), Some("aurora-mysql"));
    assert_eq!(restored["BackupRetentionPeriod"].as_i64(), Some(21));
    assert_eq!(restored["Status"].as_str(), Some("available"));
    assert!(restored["DBClusterArn"]
        .as_str()
        .unwrap_or_default()
        .ends_with(":cluster:restored"));
}

#[tokio::test]
async fn cluster_restores_refuse_an_existing_target() {
    // Overwriting would replace a live cluster row whose members were
    // just stripped, orphaning its writer instance -- and a
    // self-targeted PITR would destroy the very cluster it reads.
    let svc = make_service();
    seed_cluster_entry(
        &svc,
        "prod",
        serde_json::json!({"WriterDBInstanceIdentifier": "writer-1"}),
    );
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "prod",
                    "Status": "available",
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "prod"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    match svc.restore_db_cluster_from_snapshot(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterAlreadyExistsFault"),
        Ok(_) => panic!("restore overwrote an existing cluster"),
    }

    // A self-targeted PITR is the destructive case.
    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "prod"),
            ("SourceDBClusterIdentifier", "prod"),
            ("RestoreType", "copy-on-write"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    match svc.restore_db_cluster_to_point_in_time(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterAlreadyExistsFault"),
        Ok(_) => panic!("PITR overwrote its own source"),
    }

    // The live cluster still has its writer registration.
    assert_eq!(
        cluster_entry(&svc, "prod")["WriterDBInstanceIdentifier"].as_str(),
        Some("writer-1")
    );
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_reports_a_wrong_type_arn_as_not_found() {
    // A DB-snapshot ARN names no cluster snapshot, so it reduces to
    // None -- but the parameter WAS supplied, and "is required" would
    // send the caller looking for the wrong problem.
    let svc = make_service();
    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            (
                "SnapshotIdentifier",
                "arn:aws:rds:us-east-1:123456789012:snapshot:s1",
            ),
        ],
    );
    match svc.restore_db_cluster_from_snapshot(&req).await {
        Err(err) => {
            assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
            let message = format!("{err:?}");
            assert!(message.contains("s1"), "identifier dropped: {message}");
            assert!(
                !message.contains("is required"),
                "a supplied identifier was reported as missing: {message}"
            );
        }
        Ok(_) => panic!("a DB-snapshot ARN resolved as a cluster snapshot"),
    }
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_unknown_snapshot_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "ghost"),
        ],
    );
    match svc.restore_db_cluster_from_snapshot(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("unknown snapshot should fault"),
    }
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_carries_a_staged_dump_forward() {
    // A snapshot of a cluster that was itself restored, taken before an
    // instance attached, has no DumpDataB64 -- there was no writer to
    // dump -- and holds the data under PendingRestoreDumpB64. That IS
    // the snapshot's data: dropping it loses the database on a
    // cluster -> snapshot -> cluster chain, while the instance restore
    // reads it from the very same snapshot.
    let svc = make_service();
    {
        use base64::Engine;
        let staged = base64::engine::general_purpose::STANDARD.encode(b"-- staged --");
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "PendingRestoreDumpB64": staged,
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    use base64::Engine;
    let staged = base64::engine::general_purpose::STANDARD.encode(b"-- staged --");
    assert_eq!(
        cluster_entry(&svc, "restored")["PendingRestoreDumpB64"].as_str(),
        Some(staged.as_str()),
        "the snapshot's staged data was lost on restore"
    );
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_prefers_a_fresh_dump() {
    // When the snapshot carries both, the writer's own dump wins over
    // anything staged by an earlier restore.
    let svc = make_service();
    {
        use base64::Engine;
        let fresh = base64::engine::general_purpose::STANDARD.encode(b"-- fresh --");
        let stale = base64::engine::general_purpose::STANDARD.encode(b"-- stale --");
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "DumpDataB64": fresh,
                    "PendingRestoreDumpB64": stale,
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    use base64::Engine;
    let fresh = base64::engine::general_purpose::STANDARD.encode(b"-- fresh --");
    assert_eq!(
        cluster_entry(&svc, "restored")["PendingRestoreDumpB64"].as_str(),
        Some(fresh.as_str()),
        "the restore replayed the stale staged dump over the writer's own"
    );
}

#[tokio::test]
async fn restore_db_cluster_from_snapshot_does_not_inherit_sharing() {
    // Sharing must not propagate: CreateDBClusterSnapshot copies the
    // restored cluster's whole row into the next snapshot.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                serde_json::json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "src-cluster",
                    "Status": "available",
                    "SnapshotAttributes": {"restore": ["all"]},
                }),
            );
    }

    let req = request(
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap-1"),
        ],
    );
    svc.restore_db_cluster_from_snapshot(&req).await.unwrap();

    assert!(
        cluster_entry(&svc, "restored")
            .get("SnapshotAttributes")
            .is_none(),
        "restored cluster inherited the snapshot's share list"
    );
}

#[tokio::test]
async fn restore_db_cluster_to_point_in_time_unknown_source_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SourceDBClusterIdentifier", "ghost"),
        ],
    );
    match svc.restore_db_cluster_to_point_in_time(&req).await {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("unknown source should fault"),
    }
}

#[tokio::test]
async fn restore_db_cluster_to_point_in_time_carries_source_fields() {
    let svc = make_service();
    seed_cluster_entry(
        &svc,
        "src-cluster",
        serde_json::json!({"EngineVersion": "16.2"}),
    );

    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "pit"),
            ("SourceDBClusterIdentifier", "src-cluster"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    svc.restore_db_cluster_to_point_in_time(&req).await.unwrap();

    let restored = cluster_entry(&svc, "pit");
    assert_eq!(restored["EngineVersion"].as_str(), Some("16.2"));
    assert_eq!(restored["Status"].as_str(), Some("available"));
    assert_eq!(restored["UseLatestRestorableTime"].as_str(), Some("true"));
}

#[tokio::test]
async fn restore_db_cluster_to_point_in_time_returns_promptly() {
    // 1.1: the source-writer dump is backgrounded, so PITR returns without
    // blocking on the (unbounded) mysqldump/pg_dump. Docker-free: with no
    // runtime wired the handler records the restored-cluster placeholder
    // synchronously and returns Ok immediately.
    let svc = make_service();
    {
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(
                "src-cluster".to_string(),
                serde_json::json!({
                    "DBClusterIdentifier": "src-cluster",
                    "Engine": "postgres",
                    "WriterDBInstanceIdentifier": "writer-1",
                }),
            );
    }
    seed_instance(&svc, "writer-1");

    let req = request(
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "restored-cluster"),
            ("SourceDBClusterIdentifier", "src-cluster"),
            ("UseLatestRestorableTime", "true"),
        ],
    );
    let resp = svc
        .restore_db_cluster_to_point_in_time(&req)
        .await
        .expect("PITR should return promptly");
    let body = body_of(resp);
    assert!(body.contains("restored-cluster"), "body: {body}");

    // The target cluster placeholder was recorded synchronously, and with no
    // runtime no dump is staged (metadata-only) — proving the handler did not
    // block on a dump.
    let accounts = svc.state.read();
    let clusters = accounts.default_ref().extras.get("clusters").unwrap();
    let target = clusters
        .get("restored-cluster")
        .expect("restored cluster recorded synchronously");
    assert!(
        target.get("PendingRestoreDumpB64").is_none(),
        "no dump should be staged inline without a runtime"
    );
}

#[test]
fn describe_db_snapshots_accepts_both_filters() {
    // Both snapshot id + instance id is tolerated: the snapshot id
    // takes precedence below. Smithy doesn't declare an
    // `InvalidParameterCombination` error shape on this op.
    let svc = make_service();
    let req = request(
        "DescribeDBSnapshots",
        &[("DBSnapshotIdentifier", "s"), ("DBInstanceIdentifier", "i")],
    );
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");
}

#[test]
fn describe_db_snapshots_by_id_or_instance() {
    let svc = make_service();
    seed_snapshot(&svc, "snap1", "db1");
    seed_snapshot(&svc, "snap2", "db2");

    let by_id = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", "snap1")]);
    let body = body_of(svc.describe_db_snapshots(&by_id).unwrap());
    assert!(body.contains("snap1"));
    assert!(!body.contains("snap2"));

    let by_instance = request("DescribeDBSnapshots", &[("DBInstanceIdentifier", "db2")]);
    let body = body_of(svc.describe_db_snapshots(&by_instance).unwrap());
    assert!(body.contains("snap2"));
    assert!(!body.contains("snap1"));

    let list_all = request("DescribeDBSnapshots", &[]);
    let body = body_of(svc.describe_db_snapshots(&list_all).unwrap());
    assert!(body.contains("snap1"));
    assert!(body.contains("snap2"));
}

#[test]
fn describe_db_snapshots_unknown_id_errors() {
    let svc = make_service();
    let req = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", "ghost")]);
    assert_code(svc.describe_db_snapshots(&req), "DBSnapshotNotFound");
}

// ── Error branch tests ──

#[test]
fn describe_db_instances_not_found() {
    let svc = make_service();
    let req = request("DescribeDBInstances", &[("DBInstanceIdentifier", "ghost")]);
    assert_code(svc.describe_db_instances(&req), "DBInstanceNotFound");
}

#[tokio::test]
async fn delete_db_instance_not_found() {
    let svc = make_service();
    let req = request(
        "DeleteDBInstance",
        &[
            ("DBInstanceIdentifier", "ghost"),
            ("SkipFinalSnapshot", "true"),
        ],
    );
    assert_code(svc.delete_db_instance(&req).await, "DBInstanceNotFound");
}

#[test]
fn modify_db_instance_not_found() {
    let svc = make_service();
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "ghost"),
            ("AllocatedStorage", "20"),
        ],
    );
    // AllocatedStorage is a valid mutable field — validation passes
    // and the existence check fires next.
    assert_code(svc.modify_db_instance(&req), "DBInstanceNotFound");
}

#[test]
fn modify_db_instance_no_fields_against_missing_instance_returns_not_found() {
    // After dropping the synthetic `InvalidParameterCombination` check
    // (no Smithy analogue), the resource-not-found path is what
    // surfaces for a probe-style empty Modify against a non-existent
    // instance.
    let svc = make_service();
    let req = request("ModifyDBInstance", &[("DBInstanceIdentifier", "anyone")]);
    assert_code(svc.modify_db_instance(&req), "DBInstanceNotFound");
}

// ── M1: NewDBInstanceIdentifier rename ───────────────────────────

#[test]
fn modify_db_instance_renames_instance_and_arn() {
    let svc = make_service();
    seed_instance(&svc, "db-old");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db-old"),
            ("NewDBInstanceIdentifier", "db-new"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();

    let __a = svc.state.read();
    let state = __a.default_ref();
    assert!(
        !state.instances.contains_key("db-old"),
        "old identifier must no longer resolve after rename"
    );
    let renamed = state
        .instances
        .get("db-new")
        .expect("instance now lives under the new identifier");
    assert_eq!(renamed.db_instance_identifier, "db-new");
    assert_eq!(
        renamed.db_instance_arn, "arn:aws:rds:us-east-1:123456789012:db:db-new",
        "ARN must track the renamed identifier"
    );
}

#[test]
fn modify_db_instance_rename_rejects_existing_target() {
    let svc = make_service();
    seed_instance(&svc, "db-a");
    seed_instance(&svc, "db-b");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db-a"),
            ("NewDBInstanceIdentifier", "db-b"),
            ("ApplyImmediately", "true"),
        ],
    );
    assert_code(svc.modify_db_instance(&req), "DBInstanceAlreadyExists");
    // Both instances must still be intact — no silent clobber.
    let __a = svc.state.read();
    let state = __a.default_ref();
    assert!(state.instances.contains_key("db-a"));
    assert!(state.instances.contains_key("db-b"));
}

#[test]
fn modify_db_instance_rename_updates_endpoint_host() {
    let svc = make_service();
    seed_instance(&svc, "db-old");
    {
        // Simulate an AWS-style endpoint host embedding the identifier.
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        state.instances.get_mut("db-old").unwrap().endpoint_address =
            "db-old.abc123.us-east-1.rds.amazonaws.com".to_string();
    }
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db-old"),
            ("NewDBInstanceIdentifier", "db-new"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    assert_eq!(
        state.instances.get("db-new").unwrap().endpoint_address,
        "db-new.abc123.us-east-1.rds.amazonaws.com",
        "endpoint host must follow the rename"
    );
}

// ── M2: DBPortNumber must not break the reachable endpoint ────────

#[test]
fn modify_db_instance_port_number_keeps_endpoint_reachable() {
    let svc = make_service();
    // seed_instance sets host_port == port == 15432 (consistent).
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("DBPortNumber", "5433"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    let inst = state.instances.get("db1").unwrap();
    assert_eq!(
        inst.port,
        i32::from(inst.host_port),
        "advertised endpoint port must stay equal to the reachable host_port"
    );
    assert_eq!(inst.port, 15432);
}

#[test]
fn modify_db_instance_port_number_honored_when_no_container() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    {
        // Offline / no-backend instance: no live container to re-publish.
        let mut accounts = svc.state.write();
        let state = accounts.default_mut();
        let inst = state.instances.get_mut("db1").unwrap();
        inst.host_port = 0;
        inst.port = 0;
        inst.endpoint_address = String::new();
    }
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("DBPortNumber", "5433"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    let state = __a.default_ref();
    assert_eq!(state.instances.get("db1").unwrap().port, 5433);
}

// ── L2: reject storage shrink / engine-version downgrade ──────────

#[test]
fn modify_db_instance_rejects_storage_shrink() {
    let svc = make_service();
    seed_instance(&svc, "db1"); // allocated_storage == 20
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("AllocatedStorage", "10"),
            ("ApplyImmediately", "true"),
        ],
    );
    assert_code(svc.modify_db_instance(&req), "InvalidParameterCombination");
    // Unchanged.
    let __a = svc.state.read();
    assert_eq!(
        __a.default_ref()
            .instances
            .get("db1")
            .unwrap()
            .allocated_storage,
        20
    );
}

#[test]
fn modify_db_instance_allows_storage_grow() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("AllocatedStorage", "100"),
            ("ApplyImmediately", "true"),
        ],
    );
    svc.modify_db_instance(&req).unwrap();
    let __a = svc.state.read();
    assert_eq!(
        __a.default_ref()
            .instances
            .get("db1")
            .unwrap()
            .allocated_storage,
        100
    );
}

#[test]
fn modify_db_instance_rejects_version_downgrade() {
    let svc = make_service();
    seed_instance(&svc, "db1"); // engine_version == 16.3
    let req = request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("EngineVersion", "15.4"),
            ("ApplyImmediately", "true"),
        ],
    );
    assert_code(svc.modify_db_instance(&req), "InvalidParameterCombination");
    let __a = svc.state.read();
    assert_eq!(
        __a.default_ref()
            .instances
            .get("db1")
            .unwrap()
            .engine_version,
        "16.3"
    );
}

// ── L3: no synthetic sg-default fabricated on create ──────────────

#[test]
fn create_db_instance_does_not_fabricate_sg_default() {
    // parse_vpc_security_group_ids backs Create/Restore/RestoreFromSnapshot.
    // With no VpcSecurityGroupIds supplied it must yield an empty list, not
    // a synthetic `sg-default` that doesn't exist in the account.
    let req = request("CreateDBInstance", &[("DBInstanceIdentifier", "db1")]);
    assert!(
        super::parse_vpc_security_group_ids(&req).is_empty(),
        "absent VpcSecurityGroupIds must not synthesize sg-default"
    );

    let req_with = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-abc"),
        ],
    );
    assert_eq!(
        super::parse_vpc_security_group_ids(&req_with),
        vec!["sg-abc".to_string()]
    );
}

#[test]
fn is_version_downgrade_detects_lower_versions() {
    use super::is_version_downgrade;
    assert!(is_version_downgrade("16.3", "15.4"));
    assert!(is_version_downgrade("16.3", "16.2"));
    assert!(!is_version_downgrade("16.3", "16.4"));
    assert!(!is_version_downgrade("16.3", "17.1"));
    assert!(!is_version_downgrade("16.3", "16.3"));
    // Shorter-but-equal prefix isn't a downgrade.
    assert!(!is_version_downgrade("16.3", "16"));
    // Non-numeric engine strings are treated conservatively (not a downgrade).
    assert!(!is_version_downgrade("aurora", "aurora"));
}

#[tokio::test]
async fn reboot_db_instance_not_found() {
    let svc = make_service();
    let req = request("RebootDBInstance", &[("DBInstanceIdentifier", "ghost")]);
    assert_code(svc.reboot_db_instance(&req).await, "DBInstanceNotFound");
}

#[tokio::test]
async fn create_db_snapshot_instance_not_found() {
    let svc = make_service();
    let req = request(
        "CreateDBSnapshot",
        &[
            ("DBInstanceIdentifier", "ghost"),
            ("DBSnapshotIdentifier", "snap1"),
        ],
    );
    // Instance lookup happens before the runtime probe so a missing
    // source surfaces the declared `DBInstanceNotFoundFault` shape.
    assert_code(svc.create_db_snapshot(&req).await, "DBInstanceNotFound");
}

#[tokio::test]
async fn restore_db_instance_snapshot_not_found() {
    let svc = make_service();
    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("DBSnapshotIdentifier", "ghost-snap"),
        ],
    );
    assert_code(
        svc.restore_db_instance_from_db_snapshot(&req).await,
        "DBSnapshotNotFound",
    );
}

#[tokio::test]
async fn create_db_instance_read_replica_source_not_found() {
    let svc = make_service();
    let req = request(
        "CreateDBInstanceReadReplica",
        &[
            ("DBInstanceIdentifier", "replica"),
            ("SourceDBInstanceIdentifier", "ghost"),
        ],
    );
    assert_code(
        svc.create_db_instance_read_replica(&req).await,
        "DBInstanceNotFound",
    );
}

#[test]
fn describe_db_engine_versions_basic() {
    let svc = make_service();
    let req = request("DescribeDBEngineVersions", &[]);
    let resp = svc.describe_db_engine_versions(&req).unwrap();
    let body = body_of(resp);
    assert!(body.contains("<DBEngineVersions>"));
}

#[test]
fn describe_orderable_db_instance_options_basic() {
    let svc = make_service();
    let req = request("DescribeOrderableDBInstanceOptions", &[("Engine", "mysql")]);
    let resp = svc.describe_orderable_db_instance_options(&req).unwrap();
    let body = body_of(resp);
    assert!(body.contains("<OrderableDBInstanceOptions>"));
}

#[test]
fn describe_db_parameter_group_not_found() {
    let svc = make_service();
    let req = request(
        "DescribeDBParameterGroups",
        &[("DBParameterGroupName", "ghost")],
    );
    assert_code(
        svc.describe_db_parameter_groups(&req),
        "DBParameterGroupNotFound",
    );
}

#[test]
fn delete_db_parameter_group_not_found() {
    let svc = make_service();
    let req = request(
        "DeleteDBParameterGroup",
        &[("DBParameterGroupName", "ghost")],
    );
    assert_code(
        svc.delete_db_parameter_group(&req),
        "DBParameterGroupNotFound",
    );
}

#[test]
fn describe_db_subnet_group_not_found() {
    let svc = make_service();
    let req = request("DescribeDBSubnetGroups", &[("DBSubnetGroupName", "ghost")]);
    assert_code(
        svc.describe_db_subnet_groups(&req),
        "DBSubnetGroupNotFoundFault",
    );
}

#[test]
fn delete_db_subnet_group_not_found() {
    let svc = make_service();
    let req = request("DeleteDBSubnetGroup", &[("DBSubnetGroupName", "ghost")]);
    assert_code(
        svc.delete_db_subnet_group(&req),
        "DBSubnetGroupNotFoundFault",
    );
}

#[test]
fn add_tags_resource_not_found() {
    let svc = make_service();
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", "arn:aws:rds:us-east-1:123:db:ghost"),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    );
    // `Tags.member.N` is the generic awsQuery wire form that the
    // conformance probe (and modern SDKs) emit — accepted alongside the
    // canonical `Tags.Tag.N.Key`/`Tags.Tag.N.Value` form. With non-empty
    // tags the call advances to the resource-lookup step and returns
    // the declared `DBInstanceNotFound`.
    assert_code(svc.add_tags_to_resource(&req), "DBInstanceNotFound");
}

#[test]
fn list_tags_resource_not_found() {
    let svc = make_service();
    let req = request(
        "ListTagsForResource",
        &[("ResourceName", "arn:aws:rds:us-east-1:123:db:ghost")],
    );
    assert_code(svc.list_tags_for_resource(&req), "DBInstanceNotFound");
}

// ── snapshot operations ──

#[tokio::test]
async fn create_db_snapshot_missing_id_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBSnapshot",
        &[("DBInstanceIdentifier", "nonexistent")],
    );
    assert_code(svc.create_db_snapshot(&req).await, "MissingParameter");
}

#[tokio::test]
async fn create_db_snapshot_unknown_instance_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBSnapshot",
        &[
            ("DBSnapshotIdentifier", "snap1"),
            ("DBInstanceIdentifier", "ghost"),
        ],
    );
    assert!(svc.create_db_snapshot(&req).await.is_err());
}

// ── delete_db_instance ──

#[tokio::test]
async fn delete_db_instance_missing_id_errors() {
    let svc = make_service();
    let req = request("DeleteDBInstance", &[]);
    assert_code(svc.delete_db_instance(&req).await, "MissingParameter");
}

// ── reboot_db_instance ──

#[tokio::test]
async fn reboot_db_instance_missing_id_errors() {
    let svc = make_service();
    let req = request("RebootDBInstance", &[]);
    assert_code(svc.reboot_db_instance(&req).await, "MissingParameter");
}

#[tokio::test]
async fn reboot_db_instance_restart_failure_resets_status() {
    // Regression (bug-hunt 2026-07-19): a failed container restart must not
    // leave the instance stuck reporting "rebooting" forever. The stub runtime
    // has no registered container, so `restart_container` returns Err
    // immediately; the background task must flip the status back to "available".
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    seed_instance(&svc, "db1");

    let req = request("RebootDBInstance", &[("DBInstanceIdentifier", "db1")]);
    // The synchronous response reports "rebooting".
    svc.reboot_db_instance(&req).await.expect("reboot accepted");

    // The background restart fails, then resets the status. Poll until it clears
    // (the reset is a fast Err path, but the task is spawned asynchronously).
    let mut status = String::new();
    for _ in 0..200 {
        {
            let accounts = svc.state.read();
            status = accounts
                .default_ref()
                .instances
                .get("db1")
                .expect("instance present")
                .db_instance_status
                .clone();
        }
        if status != "rebooting" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        status, "available",
        "failed reboot must reset status to available, not stay stuck at rebooting"
    );
}

// ── create_db_instance validation ──

#[tokio::test]
async fn create_db_instance_missing_id_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstance",
        &[
            ("Engine", "postgres"),
            ("DBInstanceClass", "db.t3.micro"),
            ("AllocatedStorage", "20"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
        ],
    );
    assert!(svc.create_db_instance(&req).await.is_err());
}

#[tokio::test]
async fn create_db_instance_unsupported_engine_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("Engine", "mongodb"),
            ("DBInstanceClass", "db.t3.micro"),
            ("AllocatedStorage", "20"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
        ],
    );
    assert!(svc.create_db_instance(&req).await.is_err());
}

#[tokio::test]
async fn create_db_instance_persists_request_tags() {
    // Real RDS applies create-time `Tags` immediately. The stub runtime
    // lets the handler reach the synchronously-stored "creating"
    // placeholder without a container daemon; the background start fails
    // harmlessly and never touches `tags`.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    let req = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "tagged-db"),
            ("Engine", "postgres"),
            ("DBInstanceClass", "db.t3.micro"),
            ("AllocatedStorage", "20"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
            ("Tags.Tag.2.Key", "owner"),
            ("Tags.Tag.2.Value", "platform"),
        ],
    );
    svc.create_db_instance(&req).await.expect("create ok");

    let accounts = svc.state.read();
    let state = accounts.default_ref();
    let inst = state
        .instances
        .get("tagged-db")
        .expect("placeholder stored");
    assert_eq!(
        inst.tags,
        vec![
            RdsTag {
                key: "env".to_string(),
                value: "prod".to_string()
            },
            RdsTag {
                key: "owner".to_string(),
                value: "platform".to_string()
            },
        ]
    );
}

// ── restore_db_instance_from_db_snapshot ──

#[tokio::test]
async fn restore_db_instance_missing_ids_errors() {
    let svc = make_service();
    let req = request("RestoreDBInstanceFromDBSnapshot", &[]);
    assert!(svc
        .restore_db_instance_from_db_snapshot(&req)
        .await
        .is_err());
}

#[tokio::test]
async fn restore_db_instance_unknown_snapshot_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("DBSnapshotIdentifier", "missing"),
        ],
    );
    assert!(svc
        .restore_db_instance_from_db_snapshot(&req)
        .await
        .is_err());
}

#[tokio::test]
async fn restore_db_instance_from_db_snapshot_persists_tags() {
    // Real round-trip: the handler parses `Tags.Tag.N.{Key,Value}` via
    // `parse_tags` then forwards them to `build_restored_instance`,
    // which writes them onto the new `DbInstance.tags`. The runtime
    // call between those two steps doesn't touch tags, so we can stub
    // out the running container struct and assert end-state.
    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("DBSnapshotIdentifier", "snap"),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
            ("Tags.Tag.2.Key", "owner"),
            ("Tags.Tag.2.Value", "platform"),
        ],
    );
    let tags = parse_tags(&req).expect("tags parse");

    let snapshot = crate::state::DbSnapshot {
        db_snapshot_identifier: "snap".to_string(),
        db_snapshot_arn: "arn:aws:rds:us-east-1:123456789012:snapshot:snap".to_string(),
        db_instance_identifier: "src".to_string(),
        snapshot_create_time: Utc::now(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        allocated_storage: 20,
        status: "available".to_string(),
        port: 5432,
        master_username: "admin".to_string(),
        db_name: Some("appdb".to_string()),
        dbi_resource_id: "db-rid".to_string(),
        snapshot_type: "manual".to_string(),
        master_user_password: "secret".to_string(),
        tags: Vec::new(),
        dump_data: Vec::new(),
        availability_zone: None,
        vpc_id: None,
        instance_create_time: None,
        license_model: None,
        iops: None,
        option_group_name: None,
        percent_progress: None,
        storage_type: None,
        encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        timezone: None,
        storage_throughput: None,
        snapshot_attributes: std::collections::BTreeMap::new(),
    };
    let running = crate::runtime::RunningDbContainer {
        container_id: "c-restored".to_string(),
        host_port: 15432,
        endpoint_address: "127.0.0.1".to_string(),
        endpoint_port: 15432,
    };
    let instance = build_restored_instance(
        "restored",
        "arn:aws:rds:us-east-1:123456789012:db:restored".to_string(),
        "db-restored".to_string(),
        Utc::now(),
        Vec::new(),
        &snapshot,
        &running,
        tags,
    );
    assert_eq!(
        instance.tags,
        vec![
            RdsTag {
                key: "env".to_string(),
                value: "prod".to_string()
            },
            RdsTag {
                key: "owner".to_string(),
                value: "platform".to_string()
            },
        ]
    );
}

#[tokio::test]
async fn restore_db_instance_to_point_in_time_missing_ids_errors() {
    let svc = make_service();
    let req = request("RestoreDBInstanceToPointInTime", &[]);
    assert!(svc
        .restore_db_instance_to_point_in_time(&req)
        .await
        .is_err());
}

#[tokio::test]
async fn restore_db_instance_to_point_in_time_missing_target_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBInstanceToPointInTime",
        &[("SourceDBInstanceIdentifier", "src")],
    );
    assert!(svc
        .restore_db_instance_to_point_in_time(&req)
        .await
        .is_err());
}

#[tokio::test]
async fn restore_db_instance_to_point_in_time_unknown_source_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBInstanceToPointInTime",
        &[
            ("SourceDBInstanceIdentifier", "ghost"),
            ("TargetDBInstanceIdentifier", "restored"),
        ],
    );
    let err = svc
        .restore_db_instance_to_point_in_time(&req)
        .await
        .err()
        .expect("unknown source should error");
    assert_eq!(err.code(), "DBInstanceNotFound");
}

#[tokio::test]
async fn restore_db_instance_from_s3_missing_ids_errors() {
    let svc = make_service();
    let req = request("RestoreDBInstanceFromS3", &[]);
    assert!(svc.restore_db_instance_from_s3(&req).await.is_err());
}

#[tokio::test]
async fn restore_db_instance_from_s3_without_bus_errors() {
    let svc = make_service();
    let req = request(
        "RestoreDBInstanceFromS3",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("S3BucketName", "backups"),
            ("S3Prefix", "dump.sql"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "password"),
            ("Engine", "postgres"),
        ],
    );
    let err = svc
        .restore_db_instance_from_s3(&req)
        .await
        .err()
        .expect("missing bus should error");
    assert_eq!(err.code(), "InvalidS3BucketFault");
}

// ── create_db_instance_read_replica ──

#[tokio::test]
async fn create_read_replica_missing_source_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstanceReadReplica",
        &[("DBInstanceIdentifier", "replica1")],
    );
    assert!(svc.create_db_instance_read_replica(&req).await.is_err());
}

#[tokio::test]
async fn create_read_replica_unknown_source_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstanceReadReplica",
        &[
            ("DBInstanceIdentifier", "replica1"),
            ("SourceDBInstanceIdentifier", "ghost"),
        ],
    );
    assert!(svc.create_db_instance_read_replica(&req).await.is_err());
}

// ── subnet-group placement on replica / restore paths ──

#[tokio::test]
async fn read_replica_with_explicit_subnet_group_uses_it_not_the_source() {
    // Real AWS `CreateDBInstanceReadReplica` accepts its OWN DBSubnetGroupName
    // and lands the replica there instead of inheriting the source's group.
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    create_subnet_group(&svc, "source-subnets");
    create_subnet_group(&svc, "replica-subnets");
    seed_instance(&svc, "src");
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .instances
            .get_mut("src")
            .unwrap()
            .db_subnet_group_name = Some("source-subnets".to_string());
    }

    let req = request(
        "CreateDBInstanceReadReplica",
        &[
            ("DBInstanceIdentifier", "replica1"),
            ("SourceDBInstanceIdentifier", "src"),
            ("DBSubnetGroupName", "replica-subnets"),
        ],
    );
    // The synchronous response echoes the replica's OWN group, not the source's.
    let body = body_of(svc.create_db_instance_read_replica(&req).await.expect("ok"));
    assert!(
        body.contains("<DBSubnetGroup><DBSubnetGroupName>replica-subnets"),
        "replica must report its own subnet group, body was: {body}"
    );
    assert!(!body.contains("source-subnets"));

    let accounts = svc.state.read();
    assert_eq!(
        accounts
            .default_ref()
            .instances
            .get("replica1")
            .expect("replica stored")
            .db_subnet_group_name
            .as_deref(),
        Some("replica-subnets"),
    );
}

#[tokio::test]
async fn read_replica_without_subnet_group_inherits_the_source() {
    // Omitting DBSubnetGroupName keeps the source's group (unchanged behavior).
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    create_subnet_group(&svc, "source-subnets");
    seed_instance(&svc, "src");
    {
        let mut accounts = svc.state.write();
        accounts
            .default_mut()
            .instances
            .get_mut("src")
            .unwrap()
            .db_subnet_group_name = Some("source-subnets".to_string());
    }

    let req = request(
        "CreateDBInstanceReadReplica",
        &[
            ("DBInstanceIdentifier", "replica1"),
            ("SourceDBInstanceIdentifier", "src"),
        ],
    );
    let body = body_of(svc.create_db_instance_read_replica(&req).await.expect("ok"));
    assert!(body.contains("<DBSubnetGroup><DBSubnetGroupName>source-subnets"));
}

#[tokio::test]
async fn read_replica_unknown_subnet_group_rejected_leaves_no_instance() {
    // An explicit-but-unknown group is rejected before any provisioning, and
    // the reservation is rolled back so a retry isn't blocked.
    let svc = make_service();
    seed_instance(&svc, "src");
    let req = request(
        "CreateDBInstanceReadReplica",
        &[
            ("DBInstanceIdentifier", "replica1"),
            ("SourceDBInstanceIdentifier", "src"),
            ("DBSubnetGroupName", "ghost"),
        ],
    );
    assert_code(
        svc.create_db_instance_read_replica(&req).await,
        "DBSubnetGroupNotFoundFault",
    );
    let accounts = svc.state.read();
    let state = accounts.default_ref();
    assert!(!state.instances.contains_key("replica1"));
    assert!(!state.in_progress_instance_ids.contains("replica1"));
}

#[tokio::test]
async fn restore_from_snapshot_with_subnet_group_reports_it() {
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    create_subnet_group(&svc, "restore-subnets");
    seed_snapshot(&svc, "snap", "src");

    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("DBSnapshotIdentifier", "snap"),
            ("DBSubnetGroupName", "restore-subnets"),
        ],
    );
    let body = body_of(
        svc.restore_db_instance_from_db_snapshot(&req)
            .await
            .expect("restore ok"),
    );
    assert!(
        body.contains("<DBSubnetGroup><DBSubnetGroupName>restore-subnets"),
        "restored instance must report its subnet group, body was: {body}"
    );
    let accounts = svc.state.read();
    assert_eq!(
        accounts
            .default_ref()
            .instances
            .get("restored")
            .expect("restored stored")
            .db_subnet_group_name
            .as_deref(),
        Some("restore-subnets"),
    );
}

#[tokio::test]
async fn restore_from_snapshot_unknown_subnet_group_rejected_leaves_no_instance() {
    // Validation runs before the runtime is resolved, so no container is
    // needed to exercise the rejection + rollback.
    let svc = make_service();
    seed_snapshot(&svc, "snap", "src");
    let req = request(
        "RestoreDBInstanceFromDBSnapshot",
        &[
            ("DBInstanceIdentifier", "restored"),
            ("DBSnapshotIdentifier", "snap"),
            ("DBSubnetGroupName", "ghost"),
        ],
    );
    assert_code(
        svc.restore_db_instance_from_db_snapshot(&req).await,
        "DBSubnetGroupNotFoundFault",
    );
    let accounts = svc.state.read();
    let state = accounts.default_ref();
    assert!(!state.instances.contains_key("restored"));
    assert!(!state.in_progress_instance_ids.contains("restored"));
}

// ── describe_db_snapshots with filters ──

#[test]
fn describe_db_snapshots_by_snapshot_id_only() {
    let svc = make_service();
    seed_snapshot(&svc, "s1", "inst1");
    let req = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", "s1")]);
    let resp = svc.describe_db_snapshots(&req).unwrap();
    let b = body_of(resp);
    assert!(b.contains("<DBSnapshotIdentifier>s1</DBSnapshotIdentifier>"));
}

#[test]
fn describe_db_snapshots_by_instance_id_returns_matching() {
    let svc = make_service();
    seed_snapshot(&svc, "s1", "inst1");
    seed_snapshot(&svc, "s2", "inst2");
    let req = request("DescribeDBSnapshots", &[("DBInstanceIdentifier", "inst1")]);
    let resp = svc.describe_db_snapshots(&req).unwrap();
    let b = body_of(resp);
    assert!(b.contains("s1"));
    assert!(!b.contains("<DBSnapshotIdentifier>s2</DBSnapshotIdentifier>"));
}

// ── modify_db_parameter_group ──

#[test]
fn modify_db_parameter_group_missing_name() {
    let svc = make_service();
    let req = request("ModifyDBParameterGroup", &[]);
    assert!(svc.modify_db_parameter_group(&req).is_err());
}

// ── modify_db_subnet_group ──

#[test]
fn modify_db_subnet_group_unknown_errors() {
    let svc = make_service();
    let req = request(
        "ModifyDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "ghost"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-a"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-b"),
        ],
    );
    assert!(svc.modify_db_subnet_group(&req).is_err());
}

// ── describe_db_instances ──

#[test]
fn describe_db_instances_empty_returns_xml() {
    let svc = make_service();
    let req = request("DescribeDBInstances", &[]);
    let resp = svc.describe_db_instances(&req).unwrap();
    let b = body_of(resp);
    assert!(b.contains("DescribeDBInstancesResult"));
}

#[test]
fn describe_db_snapshots_empty_returns_empty_list() {
    let svc = make_service();
    let req = request("DescribeDBSnapshots", &[]);
    let resp = svc.describe_db_snapshots(&req).unwrap();
    let b = body_of(resp);
    assert!(b.contains("DescribeDBSnapshotsResult"));
}

#[test]
fn add_tags_unknown_resource_errors() {
    let svc = make_service();
    let req = request(
        "AddTagsToResource",
        &[
            ("ResourceName", "arn:aws:rds:us-east-1:123:db:ghost"),
            ("Tags.member.1.Key", "k"),
            ("Tags.member.1.Value", "v"),
        ],
    );
    assert!(svc.add_tags_to_resource(&req).is_err());
}

#[test]
fn remove_tags_unknown_resource_errors() {
    let svc = make_service();
    let req = request(
        "RemoveTagsFromResource",
        &[
            ("ResourceName", "arn:aws:rds:us-east-1:123:db:ghost"),
            ("TagKeys.member.1", "k"),
        ],
    );
    assert!(svc.remove_tags_from_resource(&req).is_err());
}

#[test]
fn create_db_parameter_group_missing_name_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupFamily", "postgres16"),
            ("Description", "d"),
        ],
    );
    assert!(svc.create_db_parameter_group(&req).is_err());
}

#[test]
fn create_db_subnet_group_missing_desc_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sg1"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-a"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-b"),
        ],
    );
    assert!(svc.create_db_subnet_group(&req).is_err());
}

#[tokio::test]
async fn create_db_instance_missing_class_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "miss-class"),
            ("Engine", "postgres"),
            ("AllocatedStorage", "20"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
        ],
    );
    assert!(svc.create_db_instance(&req).await.is_err());
}

#[tokio::test]
async fn create_db_instance_missing_master_username_errors() {
    let svc = make_service();
    let req = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "miss-mu"),
            ("Engine", "postgres"),
            ("DBInstanceClass", "db.t3.micro"),
            ("AllocatedStorage", "20"),
            ("MasterUserPassword", "secretpass"),
        ],
    );
    assert!(svc.create_db_instance(&req).await.is_err());
}

#[test]
fn modify_db_instance_missing_id_errors() {
    let svc = make_service();
    let req = request("ModifyDBInstance", &[]);
    assert!(svc.modify_db_instance(&req).is_err());
}

#[test]
fn modify_db_parameter_group_unknown_pg_errors() {
    let svc = make_service();
    let req = request(
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "ghost"),
            ("Parameters.member.1.ParameterName", "p"),
            ("Parameters.member.1.ParameterValue", "v"),
            ("Parameters.member.1.ApplyMethod", "immediate"),
        ],
    );
    assert!(svc.modify_db_parameter_group(&req).is_err());
}

#[test]
fn describe_db_parameter_groups_unknown_errors() {
    let svc = make_service();
    let req = request(
        "DescribeDBParameterGroups",
        &[("DBParameterGroupName", "ghost")],
    );
    assert!(svc.describe_db_parameter_groups(&req).is_err());
}

#[test]
fn describe_db_subnet_groups_unknown_errors() {
    let svc = make_service();
    let req = request("DescribeDBSubnetGroups", &[("DBSubnetGroupName", "ghost")]);
    assert!(svc.describe_db_subnet_groups(&req).is_err());
}

/// Issue #914: the bg container-start task flips status from `creating`
/// to `available`. Without persisting after the flip, a restart loaded a
/// `creating` placeholder which the load path then dropped, making the
/// DB instance disappear. `save_snapshot_static` is the free fn the bg
/// task calls — exercise it directly to lock the contract: the latest
/// state lands on disk for every caller, not just service handlers.
#[tokio::test]
async fn save_snapshot_static_persists_status_flip_from_bg_task() {
    fn make_instance(id: &str, status: &str) -> DbInstance {
        let now = Utc::now();
        DbInstance {
            db_instance_identifier: id.to_string(),
            db_instance_arn: format!("arn:aws:rds:us-east-1:123456789012:db:{id}"),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: status.to_string(),
            master_username: "admin".to_string(),
            db_name: Some("appdb".to_string()),
            endpoint_address: String::new(),
            port: 0,
            allocated_storage: 20,
            publicly_accessible: true,
            deletion_protection: false,
            created_at: now,
            dbi_resource_id: format!("db-{id}"),
            master_user_password: "secret123".to_string(),
            container_id: String::new(),
            host_port: 0,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids: Vec::new(),
            db_parameter_group_name: None,
            backup_retention_period: 1,
            preferred_backup_window: "03:00-04:00".to_string(),
            preferred_maintenance_window: None,
            latest_restorable_time: Some(now),
            option_group_name: None,
            multi_az: false,
            pending_modified_values: None,
            db_subnet_group_name: None,
            availability_zone: None,
            storage_type: None,
            storage_encrypted: false,
            kms_key_id: None,
            iam_database_authentication_enabled: false,
            iops: None,
            monitoring_interval: None,
            monitoring_role_arn: None,
            performance_insights_enabled: false,
            performance_insights_kms_key_id: None,
            performance_insights_retention_period: None,
            enabled_cloudwatch_logs_exports: Vec::new(),
            ca_certificate_identifier: None,
            network_type: None,
            character_set_name: None,
            auto_minor_version_upgrade: None,
            copy_tags_to_snapshot: None,
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: None,
            license_model: None,
            max_allocated_storage: None,
            multi_tenant: None,
            storage_throughput: None,
            tde_credential_arn: None,
            delete_automated_backups: None,
            db_security_groups: Vec::new(),
            domain: None,
            domain_fqdn: None,
            domain_ou: None,
            domain_iam_role_name: None,
            domain_auth_secret_arn: None,
            domain_dns_ips: Vec::new(),
            db_cluster_identifier: None,
            activity_stream: None,
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("rds.snapshot.json");
    let store: Arc<dyn SnapshotStore> = Arc::new(DiskSnapshotStore::new(path.clone()));
    let lock = Arc::new(AsyncMutex::new(()));

    let state: SharedRdsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        s.instances
            .insert("db-1".to_string(), make_instance("db-1", "creating"));
    }

    // First save: simulates the synchronous CreateDBInstance handler save.
    save_snapshot_static(state.clone(), Some(store.clone()), lock.clone()).await;
    let bytes = std::fs::read(&path).expect("snapshot file should exist");
    let snap: RdsSnapshot = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(snap.schema_version, RDS_SNAPSHOT_SCHEMA_VERSION);
    let acc = snap.accounts.expect("multi-account");
    let s = acc.get("123456789012").expect("account state");
    assert_eq!(s.instances["db-1"].db_instance_status, "creating");

    // Bg task flips the status and saves again — the regression path.
    {
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        let inst = s.instances.get_mut("db-1").expect("placeholder still here");
        inst.db_instance_status = "available".to_string();
        inst.host_port = 15432;
        inst.port = 15432;
        inst.endpoint_address = "127.0.0.1".to_string();
        inst.container_id = "container-id".to_string();
    }
    save_snapshot_static(state.clone(), Some(store.clone()), lock.clone()).await;

    let bytes = std::fs::read(&path).unwrap();
    let snap: RdsSnapshot = serde_json::from_slice(&bytes).unwrap();
    let acc = snap.accounts.expect("multi-account");
    let s = acc.get("123456789012").expect("account state");
    assert_eq!(
        s.instances["db-1"].db_instance_status, "available",
        "post-bg-task save must overwrite the `creating` placeholder",
    );
    assert_eq!(s.instances["db-1"].host_port, 15432);
}

/// CreateDBSnapshot must NOT block on the (unbounded, possibly-minutes)
/// mysqldump/pg_dump: it records the snapshot as `creating` and returns
/// immediately, so the response and an instant DescribeDBSnapshots both show
/// `creating`. The dump runs in a detached task (bug-hunt: sync long op in the
/// async request path). Uses a stub runtime so `require_runtime` returns `Some`
/// without needing Docker; the background dump fails harmlessly.
#[tokio::test]
async fn create_db_snapshot_returns_creating_without_blocking_on_dump() {
    let svc = make_service().with_runtime(Arc::new(crate::runtime::RdsRuntime::new_stub()));
    seed_instance(&svc, "db1");

    let req = request(
        "CreateDBSnapshot",
        &[
            ("DBSnapshotIdentifier", "snap-1"),
            ("DBInstanceIdentifier", "db1"),
        ],
    );
    let body = body_of(svc.create_db_snapshot(&req).await.unwrap());
    // The synchronous response reflects the in-progress call.
    assert!(
        body.contains("<Status>creating</Status>"),
        "CreateDBSnapshot must return `creating`, got: {body}"
    );

    // DescribeDBSnapshots immediately after create sees the row as `creating`
    // (the record insert is synchronous; only the dump is backgrounded).
    let desc = request("DescribeDBSnapshots", &[("DBSnapshotIdentifier", "snap-1")]);
    let desc_body = body_of(svc.describe_db_snapshots(&desc).unwrap());
    assert!(
        desc_body.contains("<DBSnapshotIdentifier>snap-1</DBSnapshotIdentifier>"),
        "snapshot must be visible immediately: {desc_body}"
    );
    assert!(
        desc_body.contains("<Status>creating</Status>"),
        "snapshot must be `creating` right after create: {desc_body}"
    );
}

/// CreateDBSnapshot with no runtime wired keeps the historical fast-fail: the
/// snapshot never enters `creating` because there is no container to dump.
#[tokio::test]
async fn create_db_snapshot_requires_runtime() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "CreateDBSnapshot",
        &[
            ("DBSnapshotIdentifier", "snap-x"),
            ("DBInstanceIdentifier", "db1"),
        ],
    );
    assert!(svc.create_db_snapshot(&req).await.is_err());
    // Nothing was recorded.
    let desc = request("DescribeDBSnapshots", &[]);
    let desc_body = body_of(svc.describe_db_snapshots(&desc).unwrap());
    assert!(!desc_body.contains("snap-x"));
}

/// The backgrounded finalizer's state transition, exercised directly (no
/// container runtime): a successful dump flips `creating` -> `available` with
/// the captured bytes and full progress; a failed dump flips to `failed`.
#[test]
fn apply_snapshot_dump_result_transitions_status() {
    fn seed_creating(state: &SharedRdsState, id: &str) {
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        s.snapshots.insert(
            id.to_string(),
            crate::state::DbSnapshot {
                db_snapshot_identifier: id.to_string(),
                db_snapshot_arn: format!("arn:aws:rds:us-east-1:123456789012:snapshot:{id}"),
                db_instance_identifier: "src".to_string(),
                snapshot_create_time: Utc::now(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                allocated_storage: 20,
                status: "creating".to_string(),
                port: 5432,
                master_username: "admin".to_string(),
                db_name: Some("appdb".to_string()),
                dbi_resource_id: "db-rid".to_string(),
                snapshot_type: "manual".to_string(),
                master_user_password: "secret".to_string(),
                tags: Vec::new(),
                dump_data: Vec::new(),
                availability_zone: None,
                vpc_id: None,
                instance_create_time: Some(Utc::now()),
                license_model: None,
                iops: None,
                option_group_name: None,
                percent_progress: Some(0),
                storage_type: None,
                encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                timezone: None,
                storage_throughput: None,
                snapshot_attributes: std::collections::BTreeMap::new(),
            },
        );
    }

    let state: SharedRdsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));

    // Success path.
    seed_creating(&state, "ok");
    apply_snapshot_dump_result(&state, "123456789012", "ok", Ok(b"DUMP-BYTES".to_vec()));
    {
        let accounts = state.read();
        let snap = &accounts.get("123456789012").unwrap().snapshots["ok"];
        assert_eq!(snap.status, "available");
        assert_eq!(snap.dump_data, b"DUMP-BYTES");
        assert_eq!(snap.percent_progress, Some(100));
    }

    // Failure path.
    seed_creating(&state, "bad");
    apply_snapshot_dump_result(
        &state,
        "123456789012",
        "bad",
        Err(crate::runtime::RuntimeError::Unavailable),
    );
    {
        let accounts = state.read();
        let snap = &accounts.get("123456789012").unwrap().snapshots["bad"];
        assert_eq!(snap.status, "failed");
        assert!(snap.dump_data.is_empty());
    }

    // A snapshot deleted mid-dump is a no-op, not a panic.
    apply_snapshot_dump_result(&state, "123456789012", "ghost", Ok(vec![1, 2, 3]));
}

/// Memory mode: no store wired, save is a no-op. Guards against
/// accidentally requiring a store for the bg-task path.
#[tokio::test]
async fn save_snapshot_static_is_noop_without_store() {
    let lock = Arc::new(AsyncMutex::new(()));
    let state: SharedRdsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    save_snapshot_static(state, None, lock).await;
}

// ── DescribeDBLogFiles / DownloadDBLogFilePortion (M10) ─────────

#[tokio::test]
async fn describe_db_log_files_returns_synthetic_files_when_runtime_absent() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request("DescribeDBLogFiles", &[("DBInstanceIdentifier", "db1")]);
    let body = body_of(svc.describe_db_log_files(&req).await.unwrap());
    assert!(
        body.contains("<LogFileName>error/postgres.log</LogFileName>"),
        "expected error/postgres.log entry in {body}"
    );
    assert!(body.contains("<LastWritten>"));
    assert!(body.contains("<Size>"));
}

#[tokio::test]
async fn describe_db_log_files_unknown_instance_returns_not_found() {
    let svc = make_service();
    let req = request("DescribeDBLogFiles", &[("DBInstanceIdentifier", "ghost")]);
    assert_code(svc.describe_db_log_files(&req).await, "DBInstanceNotFound");
}

#[tokio::test]
async fn describe_db_log_files_filename_contains_filter_applied() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "DescribeDBLogFiles",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("FilenameContains", "trace"),
        ],
    );
    let body = body_of(svc.describe_db_log_files(&req).await.unwrap());
    assert!(
        body.contains("<LogFileName>trace/postgres-trace.log</LogFileName>"),
        "trace file should pass filter: {body}"
    );
    assert!(
        !body.contains("<LogFileName>error/postgres.log</LogFileName>"),
        "error file should be filtered out: {body}"
    );
}

#[tokio::test]
async fn download_db_log_file_portion_unknown_instance_errors() {
    let svc = make_service();
    let req = request(
        "DownloadDBLogFilePortion",
        &[
            ("DBInstanceIdentifier", "ghost"),
            ("LogFileName", "error/postgres.log"),
        ],
    );
    assert_code(
        svc.download_db_log_file_portion(&req).await,
        "DBInstanceNotFound",
    );
}

#[tokio::test]
async fn download_db_log_file_portion_returns_empty_when_runtime_absent() {
    let svc = make_service();
    seed_instance(&svc, "db1");
    let req = request(
        "DownloadDBLogFilePortion",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("LogFileName", "error/postgres.log"),
        ],
    );
    let body = body_of(svc.download_db_log_file_portion(&req).await.unwrap());
    assert!(
        body.contains("<LogFileData></LogFileData>"),
        "expected empty LogFileData in {body}"
    );
    assert!(body.contains("<AdditionalDataPending>false</AdditionalDataPending>"));
    assert!(body.contains("<Marker>0</Marker>"));
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating RDS state
/// directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn SnapshotStore> = Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

#[tokio::test]
async fn repro_issue_2107_corrupt_state_when_runtime_absent() {
    // No runtime -> require_runtime() fails. Reproduce issue #2107:
    // a failed create must NOT leak the in-progress reservation.
    let svc = make_service();
    let create = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "corrupt-db"),
            ("Engine", "mysql"),
            ("EngineVersion", "8.0"),
            ("DBInstanceClass", "db.t3.micro"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
        ],
    );

    // First create fails because there is no container runtime.
    let e1 = match svc.create_db_instance(&create).await {
        Ok(_) => panic!("expected failure with no runtime"),
        Err(e) => e,
    };
    assert_eq!(e1.code(), "InsufficientDBInstanceCapacity");
    // The human-readable message must stay actionable: tell the operator how
    // to enable the runtime rather than just that it is missing.
    assert!(
        e1.message().contains("Install and start Docker or Podman")
            && e1.message().contains("FAKECLOUD_CONTAINER_CLI"),
        "runtime-missing error must explain how to fix it: {}",
        e1.message()
    );

    // Second create must ALSO be InsufficientDBInstanceCapacity, NOT
    // DBInstanceAlreadyExists. A leaked reservation shows up here.
    let e2 = match svc.create_db_instance(&create).await {
        Ok(_) => panic!("expected failure with no runtime"),
        Err(e) => e,
    };
    assert_ne!(
        e2.code(),
        "DBInstanceAlreadyExists",
        "leaked in-progress reservation corrupts state"
    );

    let state = svc.state.read();
    assert!(
        state.default_ref().in_progress_instance_ids.is_empty(),
        "failed create left a leaked reservation"
    );
}

#[tokio::test]
async fn create_without_engine_version_uses_engine_default() {
    // Issue #2107: a version-less create must default EngineVersion to a
    // version in the requested engine's supported list -- not a fixed
    // postgres value like "16.3", which would make every version-less
    // mysql/mariadb/oracle/... create fail validation with
    // "EngineVersion '16.3' is not available" before it ever reaches the
    // runtime. With no runtime the create still fails, but it must fail
    // at require_runtime (InsufficientDBInstanceCapacity), proving
    // validation passed with the engine-appropriate default.
    let svc = make_service();
    let create = request(
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "mysql-nover"),
            ("Engine", "mysql"),
            ("DBInstanceClass", "db.t3.micro"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "secretpass"),
        ],
    );
    let err = match svc.create_db_instance(&create).await {
        Ok(_) => panic!("expected failure with no runtime"),
        Err(e) => e,
    };
    assert_eq!(
        err.code(),
        "InsufficientDBInstanceCapacity",
        "version-less mysql create must pass validation via the engine default, \
         not fail on a hardcoded postgres version"
    );
}

// ── bug-hunt: Copy/Modify/Restore extras + activity-stream + PI fields ──
// Each op below previously returned canned XML without persisting; these
// tests assert the write now round-trips through the matching Describe.

fn create_cluster(svc: &RdsService, id: &str) {
    svc.handle_extra_action(&request(
        "CreateDBCluster",
        &[("DBClusterIdentifier", id), ("Engine", "aurora-postgresql")],
    ))
    .expect("CreateDBCluster");
}

#[test]
fn copy_db_snapshot_persists_and_describe_finds_target() {
    let svc = make_service();
    seed_snapshot(&svc, "src-snap", "db1");
    let body = body_of(
        svc.handle_extra_action(&request(
            "CopyDBSnapshot",
            &[
                ("SourceDBSnapshotIdentifier", "src-snap"),
                ("TargetDBSnapshotIdentifier", "copy-snap"),
            ],
        ))
        .expect("CopyDBSnapshot"),
    );
    assert!(
        body.contains("<DBSnapshotIdentifier>copy-snap</DBSnapshotIdentifier>"),
        "{body}"
    );
    assert!(body.contains("<Status>available</Status>"));
    // The copy is now describable instead of DBSnapshotNotFoundFault.
    let d = body_of(
        svc.describe_db_snapshots(&request(
            "DescribeDBSnapshots",
            &[("DBSnapshotIdentifier", "copy-snap")],
        ))
        .expect("DescribeDBSnapshots"),
    );
    assert!(d.contains("copy-snap"));
    assert!(d.contains("<Engine>postgres</Engine>"));
}

#[test]
fn copy_db_snapshot_unknown_source_errors() {
    let svc = make_service();
    assert_code(
        svc.handle_extra_action(&request(
            "CopyDBSnapshot",
            &[
                ("SourceDBSnapshotIdentifier", "ghost"),
                ("TargetDBSnapshotIdentifier", "t"),
            ],
        )),
        "DBSnapshotNotFound",
    );
}

#[test]
fn copy_db_parameter_group_persists_and_describe_finds_target() {
    let svc = make_service();
    svc.create_db_parameter_group(&request(
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupName", "src-pg"),
            ("DBParameterGroupFamily", "postgres16"),
            ("Description", "source"),
        ],
    ))
    .expect("CreateDBParameterGroup");
    let body = body_of(
        svc.handle_extra_action(&request(
            "CopyDBParameterGroup",
            &[
                ("SourceDBParameterGroupIdentifier", "src-pg"),
                ("TargetDBParameterGroupIdentifier", "copy-pg"),
                ("TargetDBParameterGroupDescription", "the copy"),
            ],
        ))
        .expect("CopyDBParameterGroup"),
    );
    assert!(body.contains("copy-pg"), "{body}");
    let d = body_of(
        svc.describe_db_parameter_groups(&request(
            "DescribeDBParameterGroups",
            &[("DBParameterGroupName", "copy-pg")],
        ))
        .expect("DescribeDBParameterGroups"),
    );
    assert!(d.contains("copy-pg"));
    assert!(d.contains("the copy"));
    assert!(d.contains("postgres16"));
}

#[test]
fn modify_db_snapshot_attribute_round_trips_describe() {
    let svc = make_service();
    seed_snapshot(&svc, "snap-attr", "db1");
    svc.handle_extra_action(&request(
        "ModifyDBSnapshotAttribute",
        &[
            ("DBSnapshotIdentifier", "snap-attr"),
            ("AttributeName", "restore"),
            ("ValuesToAdd.AttributeValue.1", "111111111111"),
            ("ValuesToAdd.AttributeValue.2", "222222222222"),
        ],
    ))
    .expect("ModifyDBSnapshotAttribute add");
    let body = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBSnapshotAttributes",
            &[("DBSnapshotIdentifier", "snap-attr")],
        ))
        .expect("DescribeDBSnapshotAttributes"),
    );
    assert!(
        body.contains("<AttributeName>restore</AttributeName>"),
        "{body}"
    );
    assert!(body.contains("<AttributeValue>111111111111</AttributeValue>"));
    assert!(body.contains("<AttributeValue>222222222222</AttributeValue>"));
    // Removing a value drops it; removing the last empties the attribute.
    svc.handle_extra_action(&request(
        "ModifyDBSnapshotAttribute",
        &[
            ("DBSnapshotIdentifier", "snap-attr"),
            ("AttributeName", "restore"),
            ("ValuesToRemove.AttributeValue.1", "111111111111"),
        ],
    ))
    .expect("ModifyDBSnapshotAttribute remove");
    let body2 = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBSnapshotAttributes",
            &[("DBSnapshotIdentifier", "snap-attr")],
        ))
        .expect("DescribeDBSnapshotAttributes 2"),
    );
    assert!(!body2.contains("111111111111"), "{body2}");
    assert!(body2.contains("222222222222"));
}

#[test]
fn describe_db_snapshot_attributes_unknown_errors() {
    let svc = make_service();
    assert_code(
        svc.handle_extra_action(&request(
            "DescribeDBSnapshotAttributes",
            &[("DBSnapshotIdentifier", "ghost")],
        )),
        "DBSnapshotNotFound",
    );
}

#[test]
fn modify_db_snapshot_applies_engine_version_and_option_group() {
    let svc = make_service();
    seed_snapshot(&svc, "snap-mod", "db1");
    svc.handle_extra_action(&request(
        "ModifyDBSnapshot",
        &[
            ("DBSnapshotIdentifier", "snap-mod"),
            ("EngineVersion", "16.4"),
            ("OptionGroupName", "custom-og"),
        ],
    ))
    .expect("ModifyDBSnapshot");
    let d = body_of(
        svc.describe_db_snapshots(&request(
            "DescribeDBSnapshots",
            &[("DBSnapshotIdentifier", "snap-mod")],
        ))
        .expect("DescribeDBSnapshots"),
    );
    assert!(d.contains("<EngineVersion>16.4</EngineVersion>"), "{d}");
    assert!(d.contains("<OptionGroupName>custom-og</OptionGroupName>"));
}

#[test]
fn enable_disable_http_endpoint_round_trips_describe() {
    let svc = make_service();
    create_cluster(&svc, "aur1");
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster:aur1";
    svc.handle_extra_action(&request("EnableHttpEndpoint", &[("ResourceArn", arn)]))
        .expect("EnableHttpEndpoint");
    let d = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "aur1")],
        ))
        .expect("DescribeDBClusters"),
    );
    assert!(
        d.contains("<HttpEndpointEnabled>true</HttpEndpointEnabled>"),
        "{d}"
    );
    svc.handle_extra_action(&request("DisableHttpEndpoint", &[("ResourceArn", arn)]))
        .expect("DisableHttpEndpoint");
    let d2 = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "aur1")],
        ))
        .expect("DescribeDBClusters 2"),
    );
    assert!(
        d2.contains("<HttpEndpointEnabled>false</HttpEndpointEnabled>"),
        "{d2}"
    );
}

#[test]
fn enable_http_endpoint_unknown_cluster_errors() {
    let svc = make_service();
    assert_code(
        svc.handle_extra_action(&request(
            "EnableHttpEndpoint",
            &[(
                "ResourceArn",
                "arn:aws:rds:us-east-1:123456789012:cluster:ghost",
            )],
        )),
        // EnableHttpEndpoint declares ResourceNotFoundFault (not the typed
        // DBClusterNotFoundFault) in its Smithy error set.
        "ResourceNotFoundFault",
    );
}

#[test]
fn modify_current_db_cluster_capacity_echoes_requested_capacity() {
    let svc = make_service();
    create_cluster(&svc, "aur-cap");
    let body = body_of(
        svc.handle_extra_action(&request(
            "ModifyCurrentDBClusterCapacity",
            &[("DBClusterIdentifier", "aur-cap"), ("Capacity", "8")],
        ))
        .expect("ModifyCurrentDBClusterCapacity"),
    );
    assert!(
        body.contains("<DBClusterIdentifier>aur-cap</DBClusterIdentifier>"),
        "{body}"
    );
    assert!(
        body.contains("<CurrentCapacity>8</CurrentCapacity>"),
        "{body}"
    );
    // No longer the hardcoded id/capacity.
    assert!(!body.contains("<DBClusterIdentifier>x</DBClusterIdentifier>"));
    assert!(!body.contains("<CurrentCapacity>4</CurrentCapacity>"));
    // The applied capacity round-trips through DescribeDBClusters.
    let described = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "aur-cap")],
        ))
        .expect("DescribeDBClusters"),
    );
    assert!(described.contains("<Capacity>8</Capacity>"), "{described}");
}

#[test]
fn modify_db_snapshot_attribute_resolves_a_value_in_add_and_remove() {
    // AWS rejects this with InvalidParameterCombination, which is not
    // even a shape in the RDS model -- emitting it would be an undeclared
    // error. Resolve deterministically instead, and fail CLOSED: this is
    // a permission surface, so a contradictory request must leave the
    // snapshot unshared rather than shared.
    let svc = make_service();
    seed_snapshot(&svc, "snap-dup", "db1");

    let resp = svc
        .handle_extra_action(&request(
            "ModifyDBSnapshotAttribute",
            &[
                ("DBSnapshotIdentifier", "snap-dup"),
                ("AttributeName", "restore"),
                ("ValuesToAdd.AttributeValue.1", "111111111111"),
                ("ValuesToRemove.AttributeValue.1", "111111111111"),
            ],
        ))
        .expect("overlapping values should resolve, not fault");
    let body = body_of(resp);
    assert!(
        !body.contains("<AttributeValue>111111111111</AttributeValue>"),
        "an ambiguous sharing request left the snapshot shared: {body}"
    );

    // The snapshot really is unshared, so the shared listing can't see it.
    let shared_with = svc
        .state
        .read()
        .default_ref()
        .snapshots
        .get("snap-dup")
        .expect("seeded snapshot")
        .snapshot_attributes
        .contains_key("restore");
    assert!(
        !shared_with,
        "an empty attribute should be dropped entirely"
    );
}

#[test]
fn restore_db_cluster_from_s3_creates_persisted_cluster() {
    let svc = make_service();
    svc.handle_extra_action(&request(
        "RestoreDBClusterFromS3",
        &[
            ("DBClusterIdentifier", "s3clus"),
            ("Engine", "aurora-mysql"),
        ],
    ))
    .expect("RestoreDBClusterFromS3");
    let d = body_of(
        svc.handle_extra_action(&request(
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "s3clus")],
        ))
        .expect("DescribeDBClusters"),
    );
    assert!(
        d.contains("<DBClusterIdentifier>s3clus</DBClusterIdentifier>"),
        "{d}"
    );
    assert!(d.contains("<Status>available</Status>"));
}

#[test]
fn modify_db_shard_group_persists_capacity() {
    let svc = make_service();
    svc.handle_extra_action(&request(
        "CreateDBShardGroup",
        &[
            ("DBShardGroupIdentifier", "sg1"),
            ("DBClusterIdentifier", "c1"),
            ("MaxACU", "16"),
            ("MinACU", "2"),
        ],
    ))
    .expect("CreateDBShardGroup");
    let d = body_of(
        svc.handle_extra_action(&request("DescribeDBShardGroups", &[]))
            .expect("DescribeDBShardGroups"),
    );
    assert!(d.contains("<MaxACU>16</MaxACU>"), "{d}");
    assert!(d.contains("<MinACU>2</MinACU>"));
    svc.handle_extra_action(&request(
        "ModifyDBShardGroup",
        &[
            ("DBShardGroupIdentifier", "sg1"),
            ("MaxACU", "32"),
            ("ComputeRedundancy", "1"),
        ],
    ))
    .expect("ModifyDBShardGroup");
    let d2 = body_of(
        svc.handle_extra_action(&request("DescribeDBShardGroups", &[]))
            .expect("DescribeDBShardGroups 2"),
    );
    assert!(d2.contains("<MaxACU>32</MaxACU>"), "{d2}");
    assert!(d2.contains("<ComputeRedundancy>1</ComputeRedundancy>"));
    // MinACU from create is retained through the modify.
    assert!(d2.contains("<MinACU>2</MinACU>"));
}

#[test]
fn activity_stream_start_stop_round_trips_instance_xml() {
    let svc = make_service();
    seed_instance(&svc, "das-db");
    let arn = "arn:aws:rds:us-east-1:123456789012:db:das-db";
    svc.handle_extra_action(&request(
        "StartActivityStream",
        &[
            ("ResourceArn", arn),
            ("Mode", "async"),
            ("KmsKeyId", "key-123"),
        ],
    ))
    .expect("StartActivityStream");
    {
        let accounts = svc.state.read();
        let inst = accounts
            .default_ref()
            .instances
            .get("das-db")
            .expect("instance");
        let xml = db_instance_xml(inst, None, None);
        assert!(
            xml.contains("<ActivityStreamStatus>started</ActivityStreamStatus>"),
            "{xml}"
        );
        assert!(xml.contains(
            "<ActivityStreamKinesisStreamName>aws-rds-das-das-db</ActivityStreamKinesisStreamName>"
        ));
        assert!(xml.contains("<ActivityStreamMode>async</ActivityStreamMode>"));
    }
    svc.handle_extra_action(&request("StopActivityStream", &[("ResourceArn", arn)]))
        .expect("StopActivityStream");
    {
        let accounts = svc.state.read();
        let inst = accounts
            .default_ref()
            .instances
            .get("das-db")
            .expect("instance");
        assert!(inst.activity_stream.is_none());
        let xml = db_instance_xml(inst, None, None);
        assert!(
            xml.contains("<ActivityStreamStatus>stopped</ActivityStreamStatus>"),
            "{xml}"
        );
    }
}

#[test]
fn start_activity_stream_unknown_instance_errors() {
    let svc = make_service();
    assert_code(
        svc.handle_extra_action(&request(
            "StartActivityStream",
            &[("ResourceArn", "arn:aws:rds:us-east-1:123456789012:db:ghost")],
        )),
        "DBInstanceNotFound",
    );
}

#[test]
fn modify_db_instance_applies_monitoring_role_and_pi_fields() {
    let svc = make_service();
    seed_instance(&svc, "pi-db");
    svc.modify_db_instance(&request(
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "pi-db"),
            (
                "MonitoringRoleArn",
                "arn:aws:iam::123456789012:role/rds-monitor",
            ),
            (
                "PerformanceInsightsKMSKeyId",
                "arn:aws:kms:us-east-1:123456789012:key/pi-key",
            ),
            ("PerformanceInsightsRetentionPeriod", "731"),
        ],
    ))
    .expect("ModifyDBInstance");
    let accounts = svc.state.read();
    let inst = accounts
        .default_ref()
        .instances
        .get("pi-db")
        .expect("instance");
    assert_eq!(
        inst.monitoring_role_arn.as_deref(),
        Some("arn:aws:iam::123456789012:role/rds-monitor")
    );
    assert_eq!(
        inst.performance_insights_kms_key_id.as_deref(),
        Some("arn:aws:kms:us-east-1:123456789012:key/pi-key")
    );
    assert_eq!(inst.performance_insights_retention_period, Some(731));
}

#[test]
fn modify_db_subnet_group_applies_description() {
    let svc = make_service();
    svc.create_db_subnet_group(&request(
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sng1"),
            ("DBSubnetGroupDescription", "original"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-aaaa1111"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-bbbb2222"),
        ],
    ))
    .expect("CreateDBSubnetGroup");
    let body = body_of(
        svc.modify_db_subnet_group(&request(
            "ModifyDBSubnetGroup",
            &[
                ("DBSubnetGroupName", "sng1"),
                ("DBSubnetGroupDescription", "updated desc"),
                ("SubnetIds.SubnetIdentifier.1", "subnet-aaaa1111"),
                ("SubnetIds.SubnetIdentifier.2", "subnet-bbbb2222"),
            ],
        ))
        .expect("ModifyDBSubnetGroup"),
    );
    assert!(
        body.contains("<DBSubnetGroupDescription>updated desc</DBSubnetGroupDescription>"),
        "{body}"
    );
}
