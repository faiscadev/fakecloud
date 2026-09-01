use crate::service::RdsService;
use crate::state::{RdsState, SharedRdsState};
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::AwsRequest;
use http::Method;
use parking_lot::RwLock;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn svc() -> RdsService {
    let state: SharedRdsState = Arc::new(RwLock::new(MultiAccountState::<RdsState>::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    RdsService::new(state)
}

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut q = HashMap::new();
    q.insert("Action".to_string(), action.to_string());
    for (k, v) in params {
        q.insert(k.to_string(), v.to_string());
    }
    AwsRequest {
        service: "rds".to_string(),
        method: Method::POST,
        raw_path: "/".to_string(),
        raw_query: String::new(),
        path_segments: vec![],
        query_params: q,
        headers: http::HeaderMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        account_id: "000000000000".to_string(),
        region: "us-east-1".to_string(),
        request_id: "rid".to_string(),
        action: action.to_string(),
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn ok(action: &str, params: &[(&str, &str)]) {
    ok_on(&svc(), action, params);
}

fn ok_on(svc: &RdsService, action: &str, params: &[(&str, &str)]) {
    let r = svc.handle_extra_action(&req(action, params));
    let resp = match r {
        Ok(r) => r,
        Err(e) => panic!("{action} failed: {e:?}"),
    };
    assert!(resp.status.is_success(), "{action} status: {}", resp.status);
}

#[test]
fn describe_events_returns_emitted_events() {
    let svc = svc();
    // Push two events directly into state.
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        s.push_event(crate::state::RdsEventRecord {
            source_identifier: "instance-a".to_string(),
            source_type: "db-instance".to_string(),
            source_arn: "arn:aws:rds:us-east-1:000000000000:db:instance-a".to_string(),
            event_id: "RDS-EVENT-0001".to_string(),
            event_categories: vec!["creation".to_string()],
            message: "DB instance created".to_string(),
            date: chrono::Utc::now(),
        });
        s.push_event(crate::state::RdsEventRecord {
            source_identifier: "instance-b".to_string(),
            source_type: "db-instance".to_string(),
            source_arn: "arn:aws:rds:us-east-1:000000000000:db:instance-b".to_string(),
            event_id: "RDS-EVENT-0002".to_string(),
            event_categories: vec!["failure".to_string()],
            message: "DB instance failed".to_string(),
            date: chrono::Utc::now(),
        });
    }
    let resp = svc
        .handle_extra_action(&req("DescribeEvents", &[]))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("instance-a"), "missing instance-a in {body}");
    assert!(body.contains("instance-b"), "missing instance-b in {body}");
    assert!(body.contains("DB instance created"));
}

#[test]
fn describe_events_filters_by_source_identifier() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        for id in ["i-a", "i-b", "i-c"] {
            s.push_event(crate::state::RdsEventRecord {
                source_identifier: id.to_string(),
                source_type: "db-instance".to_string(),
                source_arn: format!("arn:aws:rds:us-east-1:000000000000:db:{id}"),
                event_id: "RDS-EVENT-0001".to_string(),
                event_categories: vec!["creation".to_string()],
                message: format!("created {id}"),
                date: chrono::Utc::now(),
            });
        }
    }
    let resp = svc
        .handle_extra_action(&req("DescribeEvents", &[("SourceIdentifier", "i-b")]))
        .unwrap();
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("created i-b"));
    assert!(!body.contains("created i-a"));
    assert!(!body.contains("created i-c"));
}

#[test]
fn create_db_cluster_response_renders_computed_fields() {
    // CreateDBCluster previously returned only id/arn/status via the canned
    // db_cluster_xml; Endpoint/Port/Engine and DbClusterResourceId were
    // dropped (bug-audit 2026-06-20, 1.4).
    let svc = svc();
    let resp = svc
        .handle_extra_action(&req(
            "CreateDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("Engine", "aurora-mysql"),
                ("EngineVersion", "8.0"),
            ],
        ))
        .expect("CreateDBCluster");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<Engine>aurora-mysql</Engine>"), "{body}");
    assert!(
        body.contains("<EngineVersion>8.0</EngineVersion>"),
        "{body}"
    );
    assert!(
        body.contains("<Endpoint>c1.cluster-xxx.us-east-1.rds.amazonaws.com</Endpoint>"),
        "{body}"
    );
    assert!(body.contains("<ReaderEndpoint>"), "{body}");
    // aurora-mysql defaults to 3306, not the redis-ish 5432.
    assert!(body.contains("<Port>3306</Port>"), "{body}");
    assert!(body.contains("<DbClusterResourceId>cluster-"), "{body}");

    // The same fields must survive into DescribeDBClusters.
    let dr = svc
        .handle_extra_action(&req("DescribeDBClusters", &[]))
        .unwrap();
    let dbody = String::from_utf8(dr.body.expect_bytes().to_vec()).unwrap();
    assert!(dbody.contains("<DbClusterResourceId>cluster-"), "{dbody}");
}

#[test]
fn cluster_lifecycle() {
    // The lifecycle ops require the cluster to actually exist and be
    // in the right state; share a single service so each call sees
    // the previous mutation.
    let svc = svc();
    ok_on(&svc, "CreateDBCluster", &[("DBClusterIdentifier", "c1")]);
    ok_on(
        &svc,
        "ModifyDBCluster",
        &[("DBClusterIdentifier", "c1"), ("EngineVersion", "16.4")],
    );
    ok_on(&svc, "RebootDBCluster", &[("DBClusterIdentifier", "c1")]);
    // Backtrack requires aurora-mysql; switch the engine first.
    ok_on(
        &svc,
        "ModifyDBCluster",
        &[("DBClusterIdentifier", "c1"), ("EngineVersion", "8.0")],
    );
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("Engine".to_string(), json!("aurora-mysql"));
                }
            }
        }
    }
    ok_on(
        &svc,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("BacktrackTo", "2026-05-01T00:00:00Z"),
        ],
    );
    ok_on(&svc, "FailoverDBCluster", &[("DBClusterIdentifier", "c1")]);
    ok_on(&svc, "StopDBCluster", &[("DBClusterIdentifier", "c1")]);
    ok_on(&svc, "StartDBCluster", &[("DBClusterIdentifier", "c1")]);
    ok_on(
        &svc,
        "PromoteReadReplicaDBCluster",
        &[("DBClusterIdentifier", "c1")],
    );
    ok_on(&svc, "DescribeDBClusters", &[]);
    ok_on(&svc, "DeleteDBCluster", &[("DBClusterIdentifier", "c1")]);
}

#[test]
fn cluster_snapshot_lifecycle() {
    let svc = svc();
    snapshot_cluster(&svc, "cs1", "c1");
    ok_on(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("TargetDBClusterSnapshotIdentifier", "cs2"),
            ("SourceDBClusterSnapshotIdentifier", "cs1"),
        ],
    );
    ok_on(&svc, "DescribeDBClusterSnapshots", &[]);
    ok_on(
        &svc,
        "DescribeDBClusterSnapshotAttributes",
        &[("DBClusterSnapshotIdentifier", "cs1")],
    );
    // AttributeName is @required in the model; the handler now stores
    // the share list rather than echoing an empty attribute set.
    ok_on(
        &svc,
        "ModifyDBClusterSnapshotAttribute",
        &[
            ("DBClusterSnapshotIdentifier", "cs1"),
            ("AttributeName", "restore"),
            ("ValuesToAdd.AttributeValue.1", "999999999999"),
        ],
    );
    ok_on(&svc, "DescribeDBClusterAutomatedBackups", &[]);
    ok_on(&svc, "DeleteDBClusterAutomatedBackup", &[]);
    ok_on(&svc, "DescribeDBClusterBacktracks", &[]);
    ok_on(
        &svc,
        "DeleteDBClusterSnapshot",
        &[("DBClusterSnapshotIdentifier", "cs1")],
    );
}

#[test]
fn cluster_param_groups_lifecycle() {
    ok(
        "CreateDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg")],
    );
    ok(
        "CopyDBClusterParameterGroup",
        &[("TargetDBClusterParameterGroupIdentifier", "cpg2")],
    );
    ok(
        "ModifyDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg")],
    );
    ok(
        "ResetDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg")],
    );
    ok("DescribeDBClusterParameterGroups", &[]);
    ok(
        "DescribeDBClusterParameters",
        &[("DBClusterParameterGroupName", "cpg")],
    );
    ok("DescribeEngineDefaultClusterParameters", &[]);
    ok(
        "DeleteDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg")],
    );
}

#[test]
fn endpoints_proxies_secgroups() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "ce1")],
    );
    ok_on(
        &svc,
        "ModifyDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "ce1")],
    );
    ok_on(&svc, "DescribeDBClusterEndpoints", &[]);
    ok_on(
        &svc,
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "ce1")],
    );
    ok_on(&svc, "CreateDBProxy", &[("DBProxyName", "p1")]);
    ok_on(&svc, "DescribeDBProxies", &[]);
    ok_on(
        &svc,
        "CreateDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")],
    );
    ok_on(
        &svc,
        "ModifyDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")],
    );
    ok_on(&svc, "DescribeDBProxyEndpoints", &[]);
    ok_on(&svc, "DescribeDBProxyTargetGroups", &[]);
    ok_on(&svc, "DescribeDBProxyTargets", &[("DBProxyName", "p1")]);
    ok_on(&svc, "ModifyDBProxyTargetGroup", &[("DBProxyName", "p1")]);
    ok_on(
        &svc,
        "RegisterDBProxyTargets",
        &[
            ("DBProxyName", "p1"),
            ("DBInstanceIdentifiers.member.1", "db1"),
        ],
    );
    ok_on(
        &svc,
        "DeregisterDBProxyTargets",
        &[
            ("DBProxyName", "p1"),
            ("DBInstanceIdentifiers.member.1", "db1"),
        ],
    );
    ok_on(
        &svc,
        "DeleteDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")],
    );
    ok_on(&svc, "ModifyDBProxy", &[("DBProxyName", "p1")]);
    ok_on(&svc, "DeleteDBProxy", &[("DBProxyName", "p1")]);
    ok_on(
        &svc,
        "CreateDBSecurityGroup",
        &[("DBSecurityGroupName", "sg1")],
    );
    ok_on(
        &svc,
        "AuthorizeDBSecurityGroupIngress",
        &[("DBSecurityGroupName", "sg1")],
    );
    ok_on(
        &svc,
        "RevokeDBSecurityGroupIngress",
        &[("DBSecurityGroupName", "sg1")],
    );
    ok_on(&svc, "DescribeDBSecurityGroups", &[]);
    ok_on(
        &svc,
        "DeleteDBSecurityGroup",
        &[("DBSecurityGroupName", "sg1")],
    );
}

#[test]
fn option_groups_event_subs_global_clusters() {
    let svc = svc();
    ok_on(&svc, "CreateOptionGroup", &[("OptionGroupName", "og1")]);
    ok_on(&svc, "ModifyOptionGroup", &[("OptionGroupName", "og1")]);
    ok_on(
        &svc,
        "CopyOptionGroup",
        &[("TargetOptionGroupIdentifier", "og2")],
    );
    ok_on(&svc, "DescribeOptionGroups", &[]);
    ok_on(&svc, "DescribeOptionGroupOptions", &[]);
    ok_on(&svc, "DeleteOptionGroup", &[("OptionGroupName", "og1")]);
    ok_on(
        &svc,
        "CreateEventSubscription",
        &[("SubscriptionName", "es1")],
    );
    ok_on(
        &svc,
        "ModifyEventSubscription",
        &[("SubscriptionName", "es1")],
    );
    ok_on(
        &svc,
        "AddSourceIdentifierToSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "db1")],
    );
    ok_on(
        &svc,
        "RemoveSourceIdentifierFromSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "db1")],
    );
    ok_on(&svc, "DescribeEventSubscriptions", &[]);
    ok_on(
        &svc,
        "DeleteEventSubscription",
        &[("SubscriptionName", "es1")],
    );
    ok_on(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
    ok_on(
        &svc,
        "ModifyGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
    ok_on(
        &svc,
        "FailoverGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
    ok_on(
        &svc,
        "SwitchoverGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
    ok_on(
        &svc,
        "RemoveFromGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
    ok_on(&svc, "DescribeGlobalClusters", &[]);
    ok_on(
        &svc,
        "DeleteGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")],
    );
}

#[test]
fn integrations_blue_green_shard_groups_tenant_dbs() {
    let svc = svc();
    ok_on(&svc, "CreateIntegration", &[("IntegrationName", "i1")]);
    ok_on(
        &svc,
        "ModifyIntegration",
        &[("IntegrationIdentifier", "i1")],
    );
    ok_on(&svc, "DescribeIntegrations", &[]);
    ok_on(
        &svc,
        "DeleteIntegration",
        &[("IntegrationIdentifier", "i1")],
    );
    ok_on(&svc, "DescribeBlueGreenDeployments", &[]);
    ok_on(
        &svc,
        "CreateDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")],
    );
    ok_on(
        &svc,
        "ModifyDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1"), ("MaxACU", "16")],
    );
    ok_on(
        &svc,
        "RebootDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")],
    );
    ok_on(&svc, "DescribeDBShardGroups", &[]);
    ok_on(
        &svc,
        "DeleteDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")],
    );
    ok_on(&svc, "CreateCustomDBEngineVersion", &[]);
    ok_on(&svc, "ModifyCustomDBEngineVersion", &[]);
    ok_on(&svc, "DeleteCustomDBEngineVersion", &[]);
    ok_on(&svc, "CreateTenantDatabase", &[("TenantDBName", "t1")]);
    ok_on(
        &svc,
        "ModifyTenantDatabase",
        &[("DBInstanceIdentifier", "db1"), ("TenantDBName", "t1")],
    );
    ok_on(&svc, "DescribeTenantDatabases", &[]);
    ok_on(&svc, "DescribeDBSnapshotTenantDatabases", &[]);
    ok_on(&svc, "DeleteTenantDatabase", &[("TenantDBName", "t1")]);
}

#[test]
fn export_activity_replicas_recommendations_certs_pending() {
    ok("StartExportTask", &[("ExportTaskIdentifier", "ex1")]);
    ok("CancelExportTask", &[]);
    ok("DescribeExportTasks", &[]);
    // StartActivityStream / ModifyActivityStream / StopActivityStream now
    // persist onto a real instance; see activity_stream_persists_on_instance.
    ok("AddRoleToDBCluster", &[]);
    ok("RemoveRoleFromDBCluster", &[]);
    ok("AddRoleToDBInstance", &[]);
    ok("RemoveRoleFromDBInstance", &[]);
    ok(
        "ApplyPendingMaintenanceAction",
        &[
            (
                "ResourceIdentifier",
                "arn:aws:rds:us-east-1:000000000000:db:any",
            ),
            ("ApplyAction", "system-update"),
            ("OptInType", "immediate"),
        ],
    );
    ok("DescribePendingMaintenanceActions", &[]);
    ok("PurchaseReservedDBInstancesOffering", &[]);
    ok("DescribeReservedDBInstances", &[]);
    ok("DescribeReservedDBInstancesOfferings", &[]);
    // PromoteReadReplica + SwitchoverReadReplica need a real
    // replica instance; covered by the dedicated tests below.
    // StartDBInstance / StopDBInstance moved to the service-level
    // dispatch (they need the container runtime); see the
    // dedicated E2E coverage in fakecloud-e2e/tests/rds_persistence.rs.
    ok("StartDBInstanceAutomatedBackupsReplication", &[]);
    ok("StopDBInstanceAutomatedBackupsReplication", &[]);
    ok("DeleteDBInstanceAutomatedBackup", &[]);
    ok("DescribeDBInstanceAutomatedBackups", &[]);
    ok("DescribeDBRecommendations", &[]);
    ok("ModifyDBRecommendation", &[]);
    ok("DescribeCertificates", &[]);
    ok("ModifyCertificates", &[]);
}

#[test]
fn snapshots_restores_account_events() {
    // Copy/Modify snapshot + parameter-group + cluster-capacity + http-endpoint
    // ops now persist real state and validate required params; they have
    // dedicated round-trip coverage in service_tests.rs. This smoke test keeps
    // the stateless describe-style ops.
    ok("DescribeDBParameters", &[]);
    ok("ResetDBParameterGroup", &[("DBParameterGroupName", "p1")]);
    ok("DescribeEngineDefaultParameters", &[]);
    ok(
        "RestoreDBClusterFromS3",
        &[("DBClusterIdentifier", "s3clus")],
    );
    ok("DescribeAccountAttributes", &[]);
    ok("DescribeEventCategories", &[]);
    ok("DescribeEvents", &[]);
    ok("DescribeSourceRegions", &[]);
    ok("DescribeDBMajorEngineVersions", &[]);
    ok("DescribeValidDBInstanceModifications", &[]);
}

#[test]
fn activity_stream_persists_on_instance() {
    let svc = svc();
    // seed_replica inserts both instances; exercise the stream on the source.
    seed_replica(&svc, "rep-a", "src-a");
    let arn = "arn:aws:rds:us-east-1:000000000000:db:src-a";
    ok_on(
        &svc,
        "StartActivityStream",
        &[("ResourceArn", arn), ("Mode", "sync"), ("KmsKeyId", "k1")],
    );
    {
        let accounts = svc.state_handle().read();
        let stream = accounts
            .get("000000000000")
            .and_then(|s| s.instances.get("src-a"))
            .and_then(|i| i.activity_stream.clone())
            .expect("activity stream persisted");
        assert_eq!(stream.status, "started");
        assert_eq!(stream.mode.as_deref(), Some("sync"));
        assert_eq!(
            stream.kinesis_stream_name.as_deref(),
            Some("aws-rds-das-src-a")
        );
    }
    ok_on(&svc, "StopActivityStream", &[("ResourceArn", arn)]);
    assert!(svc
        .state_handle()
        .read()
        .get("000000000000")
        .and_then(|s| s.instances.get("src-a"))
        .and_then(|i| i.activity_stream.clone())
        .is_none());
}

#[test]
fn start_activity_stream_requires_existing_instance() {
    let svc = svc();
    let r = svc.handle_extra_action(&req(
        "StartActivityStream",
        &[("ResourceArn", "arn:aws:rds:us-east-1:000000000000:db:ghost")],
    ));
    match r {
        Err(e) => assert_eq!(e.code(), "DBInstanceNotFound"),
        Ok(_) => panic!("expected DBInstanceNotFound"),
    }
}

fn seed_replica(svc: &RdsService, replica_id: &str, source_id: &str) {
    use crate::state::DbInstance;
    use chrono::Utc;
    let now = Utc::now();
    let mut accounts = svc.state_handle().write();
    let state = accounts.get_or_create("000000000000");
    let arn = state.db_instance_arn(&state.region, replica_id);
    let source_arn = state.db_instance_arn(&state.region, source_id);
    // Source first.
    state.instances.insert(
        source_id.to_string(),
        DbInstance {
            db_instance_identifier: source_id.to_string(),
            db_instance_arn: source_arn,
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: None,
            endpoint_address: "127.0.0.1".to_string(),
            port: 5432,
            allocated_storage: 20,
            publicly_accessible: false,
            deletion_protection: false,
            created_at: now,
            dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
            master_user_password: "".to_string(),
            container_id: String::new(),
            host_port: 0,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: vec![replica_id.to_string()],
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
        },
    );
    // Replica points at source.
    state.instances.insert(
        replica_id.to_string(),
        DbInstance {
            db_instance_identifier: replica_id.to_string(),
            db_instance_arn: arn,
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: None,
            endpoint_address: "127.0.0.1".to_string(),
            port: 5432,
            allocated_storage: 20,
            publicly_accessible: false,
            deletion_protection: false,
            created_at: now,
            dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
            master_user_password: "".to_string(),
            container_id: String::new(),
            host_port: 0,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: Some(source_id.to_string()),
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
        },
    );
}

#[test]
fn promote_read_replica_clears_source_pointer_and_trims_source_list() {
    let svc = svc();
    seed_replica(&svc, "replica-1", "source-1");
    let resp = svc
        .handle_extra_action(&req(
            "PromoteReadReplica",
            &[
                ("DBInstanceIdentifier", "replica-1"),
                ("BackupRetentionPeriod", "7"),
                ("PreferredBackupWindow", "04:00-05:00"),
            ],
        ))
        .expect("PromoteReadReplica");
    assert!(resp.status.is_success());
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<DBInstanceIdentifier>replica-1</DBInstanceIdentifier>"));

    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    let replica = state.instances.get("replica-1").unwrap();
    assert!(replica.read_replica_source_db_instance_identifier.is_none());
    assert_eq!(replica.backup_retention_period, 7);
    assert_eq!(replica.preferred_backup_window, "04:00-05:00");
    let source = state.instances.get("source-1").unwrap();
    assert!(source.read_replica_db_instance_identifiers.is_empty());
}

#[test]
fn promote_read_replica_rejects_non_replica() {
    let svc = svc();
    seed_replica(&svc, "replica-1", "source-1");
    let err = svc
        .handle_extra_action(&req(
            "PromoteReadReplica",
            &[("DBInstanceIdentifier", "source-1")],
        ))
        .err()
        .expect("non-replica should be rejected");
    assert_eq!(err.code(), "InvalidDBInstanceState");
}

#[test]
fn switchover_read_replica_swaps_primary_and_replica_roles() {
    let svc = svc();
    seed_replica(&svc, "replica-1", "source-1");
    let resp = svc
        .handle_extra_action(&req(
            "SwitchoverReadReplica",
            &[("DBInstanceIdentifier", "replica-1")],
        ))
        .expect("SwitchoverReadReplica");
    assert!(resp.status.is_success());
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.starts_with("<SwitchoverReadReplicaResponse"));
    assert!(body.contains("<DBInstanceIdentifier>replica-1</DBInstanceIdentifier>"));

    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    // The former replica is the new primary: no upstream, owns the
    // former primary as a replica.
    let new_primary = state.instances.get("replica-1").unwrap();
    assert!(new_primary
        .read_replica_source_db_instance_identifier
        .is_none());
    assert_eq!(
        new_primary.read_replica_db_instance_identifiers,
        vec!["source-1".to_string()]
    );
    // The former primary is now a replica of the new primary.
    let former_primary = state.instances.get("source-1").unwrap();
    assert_eq!(
        former_primary.read_replica_source_db_instance_identifier,
        Some("replica-1".to_string())
    );
    assert!(former_primary
        .read_replica_db_instance_identifiers
        .is_empty());
}

#[test]
fn switchover_read_replica_repoints_sibling_replicas() {
    let svc = svc();
    seed_replica(&svc, "replica-a", "source-1");
    // Add a second replica off the same source.
    seed_replica(&svc, "replica-b", "source-1");
    // `seed_replica` overwrites the source's replica list each call,
    // so re-set it to include both replicas.
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        let src = state.instances.get_mut("source-1").unwrap();
        src.read_replica_db_instance_identifiers =
            vec!["replica-a".to_string(), "replica-b".to_string()];
    }

    svc.handle_extra_action(&req(
        "SwitchoverReadReplica",
        &[("DBInstanceIdentifier", "replica-a")],
    ))
    .expect("SwitchoverReadReplica");

    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    let new_primary = state.instances.get("replica-a").unwrap();
    // New primary owns both the former primary and the sibling
    // replica.
    let mut owned = new_primary.read_replica_db_instance_identifiers.clone();
    owned.sort();
    assert_eq!(owned, vec!["replica-b".to_string(), "source-1".to_string()]);
    // Sibling now points at the new primary.
    let sibling = state.instances.get("replica-b").unwrap();
    assert_eq!(
        sibling.read_replica_source_db_instance_identifier,
        Some("replica-a".to_string())
    );
}

#[test]
fn switchover_read_replica_rejects_non_replica() {
    let svc = svc();
    seed_replica(&svc, "replica-1", "source-1");
    let err = svc
        .handle_extra_action(&req(
            "SwitchoverReadReplica",
            &[("DBInstanceIdentifier", "source-1")],
        ))
        .err()
        .expect("non-replica should be rejected");
    assert_eq!(err.code(), "InvalidDBInstanceState");
}

#[test]
fn switchover_read_replica_unknown_instance_returns_not_found() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "SwitchoverReadReplica",
            &[("DBInstanceIdentifier", "ghost")],
        ))
        .err()
        .expect("unknown instance should be rejected");
    assert_eq!(err.code(), "DBInstanceNotFound");
}

#[test]
fn promote_read_replica_unknown_instance_returns_not_found() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "PromoteReadReplica",
            &[("DBInstanceIdentifier", "ghost")],
        ))
        .err()
        .expect("unknown instance should be rejected");
    assert_eq!(err.code(), "DBInstanceNotFound");
}

fn cluster_value(svc: &RdsService, id: &str) -> serde_json::Value {
    let accounts = svc.state_handle().read();
    accounts
        .get("000000000000")
        .and_then(|s| s.extras.get("clusters"))
        .and_then(|m| m.get(id))
        .cloned()
        .expect("cluster present")
}

fn create_cluster(svc: &RdsService, id: &str) {
    svc.handle_extra_action(&req("CreateDBCluster", &[("DBClusterIdentifier", id)]))
        .expect("CreateDBCluster");
}

#[test]
fn modify_db_cluster_persists_fields() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req(
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("EngineVersion", "16.4"),
            ("BackupRetentionPeriod", "14"),
            ("PreferredBackupWindow", "01:00-02:00"),
            ("PreferredMaintenanceWindow", "sun:03:00-sun:04:00"),
            ("Port", "5433"),
            ("DeletionProtection", "true"),
            ("EnableIAMDatabaseAuthentication", "true"),
            ("CopyTagsToSnapshot", "true"),
            ("DBClusterParameterGroupName", "custom-pg"),
        ],
    ))
    .expect("ModifyDBCluster");
    let v = cluster_value(&svc, "c1");
    assert_eq!(v["EngineVersion"].as_str(), Some("16.4"));
    // Numeric/bool fields are coerced at persist time so describes
    // serialize them in the right XML shape.
    assert_eq!(v["BackupRetentionPeriod"].as_i64(), Some(14));
    assert_eq!(v["PreferredBackupWindow"].as_str(), Some("01:00-02:00"));
    assert_eq!(
        v["PreferredMaintenanceWindow"].as_str(),
        Some("sun:03:00-sun:04:00")
    );
    assert_eq!(v["Port"].as_i64(), Some(5433));
    assert_eq!(v["DeletionProtection"].as_bool(), Some(true));
    assert_eq!(v["IAMDatabaseAuthenticationEnabled"].as_bool(), Some(true));
    assert_eq!(v["CopyTagsToSnapshot"].as_bool(), Some(true));
    assert_eq!(v["DBClusterParameterGroupName"].as_str(), Some("custom-pg"));
}

#[test]
fn start_db_cluster_sets_status_available() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
        .expect("StopDBCluster");
    assert_eq!(
        cluster_value(&svc, "c1")["Status"].as_str(),
        Some("stopped")
    );
    svc.handle_extra_action(&req("StartDBCluster", &[("DBClusterIdentifier", "c1")]))
        .expect("StartDBCluster");
    assert_eq!(
        cluster_value(&svc, "c1")["Status"].as_str(),
        Some("available")
    );
}

#[test]
fn reboot_db_cluster_sets_status_available() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req("RebootDBCluster", &[("DBClusterIdentifier", "c1")]))
        .expect("RebootDBCluster");
    assert_eq!(
        cluster_value(&svc, "c1")["Status"].as_str(),
        Some("available")
    );
}

#[test]
fn failover_db_cluster_records_target_writer() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req(
        "FailoverDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("TargetDBInstanceIdentifier", "writer-2"),
        ],
    ))
    .expect("FailoverDBCluster");
    assert_eq!(
        cluster_value(&svc, "c1")["WriterDBInstanceIdentifier"].as_str(),
        Some("writer-2")
    );
}

#[test]
fn backtrack_db_cluster_records_target() {
    let svc = svc();
    create_cluster(&svc, "c1");
    // Backtrack is Aurora MySQL only; flip the engine to satisfy the
    // engine-compatibility check.
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("Engine".to_string(), json!("aurora-mysql"));
                }
            }
        }
    }
    svc.handle_extra_action(&req(
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("BacktrackTo", "2026-05-01T00:00:00Z"),
        ],
    ))
    .expect("BacktrackDBCluster");
    assert_eq!(
        cluster_value(&svc, "c1")["BacktrackTo"].as_str(),
        Some("2026-05-01T00:00:00Z")
    );
}

#[test]
fn backtrack_db_cluster_rejects_non_aurora_mysql() {
    let svc = svc();
    // Default engine is aurora-postgresql which doesn't support backtrack.
    create_cluster(&svc, "c1");
    let err = svc
        .handle_extra_action(&req(
            "BacktrackDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("BacktrackTo", "2026-05-01T00:00:00Z"),
            ],
        ))
        .err()
        .expect("aurora-postgresql backtrack should be rejected");
    assert_eq!(err.code(), "InvalidParameterCombination");
}

#[test]
fn backtrack_db_cluster_records_history() {
    let svc = svc();
    create_cluster(&svc, "c1");
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("Engine".to_string(), json!("aurora-mysql"));
                }
            }
        }
    }
    svc.handle_extra_action(&req(
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("BacktrackTo", "2026-05-01T00:00:00Z"),
        ],
    ))
    .expect("BacktrackDBCluster");
    let accounts = svc.state_handle().read();
    let backtracks = accounts
        .get("000000000000")
        .and_then(|s| s.extras.get("cluster_backtracks"))
        .expect("cluster_backtracks recorded");
    assert_eq!(backtracks.len(), 1);
}

#[test]
fn start_db_cluster_rejects_when_already_available() {
    let svc = svc();
    create_cluster(&svc, "c1");
    let err = svc
        .handle_extra_action(&req("StartDBCluster", &[("DBClusterIdentifier", "c1")]))
        .err()
        .expect("starting an already-available cluster should error");
    assert_eq!(err.code(), "InvalidDBClusterStateFault");
}

#[test]
fn stop_db_cluster_rejects_when_already_stopped() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
        .expect("StopDBCluster");
    let err = svc
        .handle_extra_action(&req("StopDBCluster", &[("DBClusterIdentifier", "c1")]))
        .err()
        .expect("stopping an already-stopped cluster should error");
    assert_eq!(err.code(), "InvalidDBClusterStateFault");
}

#[test]
fn modify_db_cluster_unknown_cluster_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "ModifyDBCluster",
            &[("DBClusterIdentifier", "ghost"), ("EngineVersion", "16.4")],
        ))
        .err()
        .expect("unknown cluster should error");
    assert_eq!(err.code(), "DBClusterNotFoundFault");
}

#[test]
fn modify_db_cluster_renames_via_new_identifier() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req(
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("NewDBClusterIdentifier", "c1-renamed"),
        ],
    ))
    .expect("ModifyDBCluster");
    let renamed = cluster_value(&svc, "c1-renamed");
    assert_eq!(renamed["DBClusterIdentifier"].as_str(), Some("c1-renamed"));
    assert!(renamed["DBClusterArn"]
        .as_str()
        .unwrap_or_default()
        .ends_with(":cluster:c1-renamed"));
    let accounts = svc.state_handle().read();
    assert!(accounts
        .get("000000000000")
        .and_then(|s| s.extras.get("clusters"))
        .map(|m| !m.contains_key("c1"))
        .unwrap_or(false));
}

#[test]
fn modify_db_cluster_persists_extended_fields() {
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req(
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("AllocatedStorage", "100"),
            ("DBClusterInstanceClass", "db.r6g.large"),
            ("Iops", "3000"),
            ("StorageEncrypted", "true"),
            ("BacktrackWindow", "86400"),
            ("EnableHttpEndpoint", "true"),
            ("AutoMinorVersionUpgrade", "false"),
            ("ManageMasterUserPassword", "true"),
            ("CACertificateIdentifier", "rds-ca-2019"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", "8.0"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-aaa"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.2", "sg-bbb"),
            (
                "CloudwatchLogsExportConfiguration.EnableLogTypes.member.1",
                "audit",
            ),
            (
                "CloudwatchLogsExportConfiguration.EnableLogTypes.member.2",
                "general",
            ),
        ],
    ))
    .expect("ModifyDBCluster");
    let v = cluster_value(&svc, "c1");
    assert_eq!(v["AllocatedStorage"].as_i64(), Some(100));
    assert_eq!(v["DBClusterInstanceClass"].as_str(), Some("db.r6g.large"));
    assert_eq!(v["Iops"].as_i64(), Some(3000));
    assert_eq!(v["StorageEncrypted"].as_bool(), Some(true));
    assert_eq!(v["BacktrackWindow"].as_i64(), Some(86400));
    assert_eq!(v["HttpEndpointEnabled"].as_bool(), Some(true));
    assert_eq!(v["AutoMinorVersionUpgrade"].as_bool(), Some(false));
    assert_eq!(v["ManageMasterUserPassword"].as_bool(), Some(true));
    assert_eq!(v["CACertificateIdentifier"].as_str(), Some("rds-ca-2019"));
    assert_eq!(
        v["ServerlessV2ScalingConfiguration.MinCapacity"].as_str(),
        Some("0.5")
    );
    assert_eq!(
        v["ServerlessV2ScalingConfiguration.MaxCapacity"].as_str(),
        Some("8.0")
    );
    let sgs: Vec<String> = v["VpcSecurityGroupIds"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect();
    assert_eq!(sgs, vec!["sg-aaa", "sg-bbb"]);
    let logs: Vec<String> = v["EnabledCloudwatchLogsExports"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect();
    assert_eq!(logs, vec!["audit", "general"]);
}

#[test]
fn describe_db_clusters_renders_modified_fields() {
    // ModifyDBCluster persisted these, but the DescribeDBClusters renderer
    // omitted them, so a describe echoed the create-time defaults
    // (bug-audit 2026-06-20, 1.17).
    let svc = svc();
    create_cluster(&svc, "c1");
    svc.handle_extra_action(&req(
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("StorageType", "io1"),
            ("Iops", "3000"),
            ("BacktrackWindow", "86400"),
            ("EnableIAMDatabaseAuthentication", "true"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", "8.0"),
        ],
    ))
    .expect("ModifyDBCluster");

    let resp = svc
        .handle_extra_action(&req("DescribeDBClusters", &[]))
        .expect("DescribeDBClusters");
    let xml = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(xml.contains("<StorageType>io1</StorageType>"), "{xml}");
    assert!(xml.contains("<Iops>3000</Iops>"), "{xml}");
    assert!(
        xml.contains("<BacktrackWindow>86400</BacktrackWindow>"),
        "{xml}"
    );
    assert!(
        xml.contains("<IAMDatabaseAuthenticationEnabled>true</IAMDatabaseAuthenticationEnabled>"),
        "{xml}"
    );
    assert!(xml.contains("<ServerlessV2ScalingConfiguration>"), "{xml}");
    assert!(xml.contains("<MinCapacity>0.5</MinCapacity>"), "{xml}");
    assert!(xml.contains("<MaxCapacity>8.0</MaxCapacity>"), "{xml}");
}

#[test]
fn failover_db_cluster_picks_replica_when_no_target() {
    let svc = svc();
    create_cluster(&svc, "c1");
    // Seed a writer + a reader.
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert(
                        "DBClusterMembers".to_string(),
                        json!([
                            {
                                "DBInstanceIdentifier": "writer-1",
                                "IsClusterWriter": true,
                                "PromotionTier": 1,
                            },
                            {
                                "DBInstanceIdentifier": "reader-1",
                                "IsClusterWriter": false,
                                "PromotionTier": 2,
                            },
                        ]),
                    );
                    obj.insert("WriterDBInstanceIdentifier".to_string(), json!("writer-1"));
                }
            }
        }
    }
    svc.handle_extra_action(&req("FailoverDBCluster", &[("DBClusterIdentifier", "c1")]))
        .expect("FailoverDBCluster");
    let v = cluster_value(&svc, "c1");
    assert_eq!(v["WriterDBInstanceIdentifier"].as_str(), Some("reader-1"));
    let members = v["DBClusterMembers"].as_array().expect("members");
    let writer_count = members
        .iter()
        .filter(|m| m["IsClusterWriter"].as_bool() == Some(true))
        .count();
    assert_eq!(writer_count, 1);
    let writer_id = members
        .iter()
        .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
        .and_then(|m| m["DBInstanceIdentifier"].as_str())
        .expect("writer member");
    assert_eq!(writer_id, "reader-1");
}

#[test]
fn failover_db_cluster_rejects_non_member_target() {
    let svc = svc();
    create_cluster(&svc, "c1");
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert(
                        "DBClusterMembers".to_string(),
                        json!([
                            {
                                "DBInstanceIdentifier": "writer-1",
                                "IsClusterWriter": true,
                            },
                        ]),
                    );
                }
            }
        }
    }
    let err = svc
        .handle_extra_action(&req(
            "FailoverDBCluster",
            &[
                ("DBClusterIdentifier", "c1"),
                ("TargetDBInstanceIdentifier", "stranger"),
            ],
        ))
        .err()
        .expect("non-member target should be rejected");
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[test]
fn promote_read_replica_db_cluster_clears_source() {
    let svc = svc();
    create_cluster(&svc, "c1");
    // Seed cluster as a replica.
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut("c1") {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert(
                        "ReplicationSourceIdentifier".to_string(),
                        json!("arn:aws:rds:us-east-1:000000000000:cluster:source"),
                    );
                }
            }
        }
    }
    svc.handle_extra_action(&req(
        "PromoteReadReplicaDBCluster",
        &[("DBClusterIdentifier", "c1")],
    ))
    .expect("PromoteReadReplicaDBCluster");
    assert!(cluster_value(&svc, "c1")
        .get("ReplicationSourceIdentifier")
        .is_none());
}

#[test]
fn cluster_lifecycle_op_missing_identifier_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req("ModifyDBCluster", &[]))
        .err()
        .expect("missing identifier should error");
    assert_eq!(err.code(), "InvalidParameterValue");
}

fn seed_blue_instance(svc: &RdsService, id: &str, addr: &str, port: i32) {
    use crate::state::DbInstance;
    use chrono::Utc;
    let now = Utc::now();
    let mut accounts = svc.state_handle().write();
    let state = accounts.get_or_create("000000000000");
    let arn = state.db_instance_arn(&state.region, id);
    state.instances.insert(
        id.to_string(),
        DbInstance {
            db_instance_identifier: id.to_string(),
            db_instance_arn: arn,
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: None,
            endpoint_address: addr.to_string(),
            port,
            allocated_storage: 20,
            publicly_accessible: false,
            deletion_protection: false,
            created_at: now,
            dbi_resource_id: format!("db-{}", uuid::Uuid::new_v4().simple()),
            master_user_password: "secret".to_string(),
            container_id: format!("c-{id}"),
            host_port: port as u16,
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
        },
    );
}

fn create_bg_deployment(svc: &RdsService, source_id: &str, target_id: &str) -> String {
    let resp = svc
        .handle_extra_action(&req(
            "CreateBlueGreenDeployment",
            &[
                (
                    "Source",
                    &format!("arn:aws:rds:us-east-1:000000000000:db:{source_id}"),
                ),
                ("TargetDBInstanceName", target_id),
            ],
        ))
        .expect("CreateBlueGreenDeployment");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    // Extract bgd id from body.
    let needle = "<BlueGreenDeploymentIdentifier>";
    let start = body.find(needle).expect("bgd id present") + needle.len();
    let end = body[start..]
        .find("</BlueGreenDeploymentIdentifier>")
        .expect("close tag");
    body[start..start + end].to_string()
}

#[test]
fn create_blue_green_deployment_clones_source_into_green() {
    let svc = svc();
    seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
    let bgd_id = create_bg_deployment(&svc, "blue", "green");
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    assert!(state.instances.contains_key("green"));
    let green = state.instances.get("green").unwrap();
    assert_eq!(green.engine, "postgres");
    assert_eq!(
        green.read_replica_source_db_instance_identifier.as_deref(),
        Some("blue")
    );
    let entry = state
        .extras
        .get("blue_green")
        .unwrap()
        .get(&bgd_id)
        .unwrap();
    assert_eq!(entry["Status"].as_str(), Some("AVAILABLE"));
    assert_eq!(entry["SourceDBInstanceIdentifier"].as_str(), Some("blue"));
    assert_eq!(entry["TargetDBInstanceIdentifier"].as_str(), Some("green"));
}

#[test]
fn create_blue_green_deployment_with_cluster_source_provisions_green_cluster() {
    let svc = svc();
    // Create a source DBCluster (not a DBInstance).
    ok_on(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "blue-cluster"),
            ("Engine", "aurora-postgresql"),
        ],
    );
    let resp = svc
        .handle_extra_action(&req(
            "CreateBlueGreenDeployment",
            &[
                (
                    "Source",
                    "arn:aws:rds:us-east-1:000000000000:cluster:blue-cluster",
                ),
                ("TargetDBInstanceName", "green-cluster"),
            ],
        ))
        .expect("CreateBlueGreenDeployment");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    let needle = "<BlueGreenDeploymentIdentifier>";
    let start = body.find(needle).expect("bgd id present") + needle.len();
    let end = body[start..]
        .find("</BlueGreenDeploymentIdentifier>")
        .expect("close tag");
    let bgd_id = body[start..start + end].to_string();
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    // Cluster sources must provision a green cluster (not a stray
    // green instance).
    let clusters = state.extras.get("clusters").expect("clusters");
    assert!(
        clusters.contains_key("green-cluster"),
        "green cluster missing from extras['clusters']"
    );
    assert!(
        !state.instances.contains_key("green-cluster"),
        "green cluster source must not provision a stray DBInstance"
    );
    let entry = state
        .extras
        .get("blue_green")
        .unwrap()
        .get(&bgd_id)
        .unwrap();
    assert_eq!(entry["Status"].as_str(), Some("AVAILABLE"));
    assert_eq!(entry["SourceIsCluster"].as_bool(), Some(true));
}

#[test]
fn create_blue_green_deployment_unknown_source_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "CreateBlueGreenDeployment",
            &[("Source", "arn:aws:rds:us-east-1:000000000000:db:ghost")],
        ))
        .err()
        .expect("missing source should error");
    assert_eq!(err.code(), "DBInstanceNotFound");
}

#[test]
fn switchover_blue_green_swaps_endpoints() {
    let svc = svc();
    seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
    let bgd_id = create_bg_deployment(&svc, "blue", "green");
    // Before swap: blue is the cloned source endpoint, green inherited the same.
    // Mutate green endpoint to make swap observable.
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        let green = state.instances.get_mut("green").unwrap();
        green.endpoint_address = "10.0.0.2".to_string();
        green.port = 5433;
    }
    svc.handle_extra_action(&req(
        "SwitchoverBlueGreenDeployment",
        &[("BlueGreenDeploymentIdentifier", &bgd_id)],
    ))
    .expect("SwitchoverBlueGreenDeployment");
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    let blue = state.instances.get("blue").unwrap();
    let green = state.instances.get("green").unwrap();
    assert_eq!(blue.endpoint_address, "10.0.0.2");
    assert_eq!(blue.port, 5433);
    assert_eq!(green.endpoint_address, "10.0.0.1");
    assert_eq!(green.port, 5432);
    // Green is now writer.
    assert!(green.read_replica_source_db_instance_identifier.is_none());
    let entry = state
        .extras
        .get("blue_green")
        .unwrap()
        .get(&bgd_id)
        .unwrap();
    assert_eq!(entry["Status"].as_str(), Some("SWITCHOVER_COMPLETED"));
}

#[test]
fn switchover_blue_green_unknown_id_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "SwitchoverBlueGreenDeployment",
            &[("BlueGreenDeploymentIdentifier", "bgd-ghost")],
        ))
        .err()
        .expect("unknown bgd should error");
    assert_eq!(err.code(), "BlueGreenDeploymentNotFoundFault");
}

#[test]
fn delete_blue_green_with_target_drops_green_instance() {
    let svc = svc();
    seed_blue_instance(&svc, "blue", "10.0.0.1", 5432);
    let bgd_id = create_bg_deployment(&svc, "blue", "green");
    svc.handle_extra_action(&req(
        "DeleteBlueGreenDeployment",
        &[
            ("BlueGreenDeploymentIdentifier", &bgd_id),
            ("DeleteTarget", "true"),
        ],
    ))
    .expect("DeleteBlueGreenDeployment");
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    assert!(!state.instances.contains_key("green"));
    let map = state.extras.get("blue_green").cloned().unwrap_or_default();
    assert!(!map.contains_key(&bgd_id));
}

fn extras_value(svc: &RdsService, category: &str, key: &str) -> serde_json::Value {
    let accounts = svc.state_handle().read();
    accounts
        .get("000000000000")
        .and_then(|s| s.extras.get(category))
        .and_then(|m| m.get(key))
        .cloned()
        .unwrap_or_else(|| panic!("{category}/{key} present"))
}

#[test]
fn modify_event_subscription_persists_topic_and_enabled_flag() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateEventSubscription",
        &[
            ("SubscriptionName", "es1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:000:original"),
        ],
    );
    ok_on(
        &svc,
        "ModifyEventSubscription",
        &[
            ("SubscriptionName", "es1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:000:updated"),
            ("SourceType", "db-instance"),
            ("Enabled", "false"),
        ],
    );
    let v = extras_value(&svc, "event_subscriptions", "es1");
    assert_eq!(
        v["SnsTopicArn"].as_str(),
        Some("arn:aws:sns:us-east-1:000:updated")
    );
    assert_eq!(v["SourceType"].as_str(), Some("db-instance"));
    assert_eq!(v["Enabled"].as_bool(), Some(false));
}

#[test]
fn modify_event_subscription_unknown_subscription_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "ModifyEventSubscription",
            &[("SubscriptionName", "ghost")],
        ))
        .err()
        .expect("missing subscription should error");
    assert_eq!(err.code(), "SubscriptionNotFound");
}

#[test]
fn modify_db_cluster_endpoint_persists_endpoint_type() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ce1"),
            ("DBClusterIdentifier", "c1"),
            ("EndpointType", "READER"),
        ],
    );
    ok_on(
        &svc,
        "ModifyDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ce1"),
            ("EndpointType", "ANY"),
            ("StaticMembers.member.1", "writer-1"),
            ("ExcludedMembers.member.1", "replica-1"),
        ],
    );
    let v = extras_value(&svc, "cluster_endpoints", "ce1");
    assert_eq!(v["EndpointType"].as_str(), Some("ANY"));
    assert_eq!(
        v["StaticMembers"].as_array().unwrap()[0].as_str(),
        Some("writer-1")
    );
    assert_eq!(
        v["ExcludedMembers"].as_array().unwrap()[0].as_str(),
        Some("replica-1")
    );
}

#[test]
fn modify_db_proxy_persists_auth_and_tls() {
    let svc = svc();
    ok_on(&svc, "CreateDBProxy", &[("DBProxyName", "p1")]);
    ok_on(
        &svc,
        "ModifyDBProxy",
        &[
            ("DBProxyName", "p1"),
            ("RequireTLS", "true"),
            ("IdleClientTimeout", "120"),
            ("DebugLogging", "true"),
            ("Auth.member.1.AuthScheme", "SECRETS"),
            (
                "Auth.member.1.SecretArn",
                "arn:aws:secretsmanager:us-east-1:000:secret:rds!sec",
            ),
            ("Auth.member.1.IAMAuth", "DISABLED"),
        ],
    );
    let v = extras_value(&svc, "proxies", "p1");
    assert_eq!(v["RequireTLS"].as_bool(), Some(true));
    assert_eq!(v["IdleClientTimeout"].as_i64(), Some(120));
    assert_eq!(v["DebugLogging"].as_bool(), Some(true));
    let auth = v["Auth"].as_array().expect("auth array");
    assert_eq!(auth.len(), 1);
    assert_eq!(auth[0]["AuthScheme"].as_str(), Some("SECRETS"));
}

#[test]
fn modify_db_proxy_endpoint_persists_security_groups() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")],
    );
    ok_on(
        &svc,
        "ModifyDBProxyEndpoint",
        &[
            ("DBProxyEndpointName", "pe1"),
            ("VpcSecurityGroupIds.member.1", "sg-1"),
            ("VpcSecurityGroupIds.member.2", "sg-2"),
        ],
    );
    let v = extras_value(&svc, "proxy_endpoints", "pe1");
    let sgs: Vec<&str> = v["VpcSecurityGroupIds"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert_eq!(sgs, vec!["sg-1", "sg-2"]);
}

#[test]
fn modify_db_proxy_target_group_persists_pool_config() {
    let svc = svc();
    ok_on(
        &svc,
        "ModifyDBProxyTargetGroup",
        &[
            ("DBProxyName", "p1"),
            ("TargetGroupName", "default"),
            ("ConnectionPoolConfig.MaxConnectionsPercent", "75"),
            ("ConnectionPoolConfig.MaxIdleConnectionsPercent", "30"),
            ("ConnectionPoolConfig.ConnectionBorrowTimeout", "10"),
        ],
    );
    let v = extras_value(&svc, "proxy_target_groups", "p1/default");
    assert_eq!(
        v["ConnectionPoolConfig"]["MaxConnectionsPercent"].as_i64(),
        Some(75)
    );
    assert_eq!(
        v["ConnectionPoolConfig"]["MaxIdleConnectionsPercent"].as_i64(),
        Some(30)
    );
}

#[test]
fn modify_tenant_database_renames() {
    let svc = svc();
    ok_on(&svc, "CreateTenantDatabase", &[("TenantDBName", "tdb1")]);
    ok_on(
        &svc,
        "ModifyTenantDatabase",
        &[
            ("DBInstanceIdentifier", "db1"),
            ("TenantDBName", "tdb1"),
            ("NewTenantDBName", "tdb2"),
            ("MasterUserPassword", "newpw"),
        ],
    );
    let accounts = svc.state_handle().read();
    let map = accounts
        .get("000000000000")
        .unwrap()
        .extras
        .get("tenant_dbs")
        .cloned()
        .unwrap_or_default();
    assert!(!map.contains_key("tdb1"));
    let v = map.get("tdb2").expect("renamed entry");
    assert_eq!(v["TenantDBName"].as_str(), Some("tdb2"));
    assert_eq!(v["MasterUserPassword"].as_str(), Some("newpw"));
}

#[test]
fn modify_option_group_persists_options_to_include_and_remove() {
    let svc = svc();
    ok_on(&svc, "CreateOptionGroup", &[("OptionGroupName", "og1")]);
    ok_on(
        &svc,
        "ModifyOptionGroup",
        &[
            ("OptionGroupName", "og1"),
            ("OptionsToInclude.member.1.OptionName", "OEM"),
            ("OptionsToInclude.member.1.Port", "1158"),
            ("OptionsToRemove.member.1", "Native Network Encryption"),
        ],
    );
    let v = extras_value(&svc, "option_groups", "og1");
    // The effective Options list reflects the included option; the removed
    // name was never present, so it stays absent. DescribeOptionGroups now
    // renders this set instead of an empty <Options/>.
    assert_eq!(v["Options"][0]["OptionName"].as_str(), Some("OEM"));
    assert_eq!(v["Options"][0]["Port"].as_str(), Some("1158"));
    assert!(!v["Options"]
        .as_array()
        .unwrap()
        .iter()
        .any(|o| o["OptionName"].as_str() == Some("Native Network Encryption")));
}

#[test]
fn event_subscription_round_trips_source_ids_and_categories() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateEventSubscription",
        &[
            ("SubscriptionName", "es1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:000000000000:t"),
            ("SourceIds.member.1", "db1"),
            ("SourceIds.member.2", "db2"),
            ("EventCategories.member.1", "creation"),
        ],
    );
    let v = extras_value(&svc, "event_subscriptions", "es1");
    assert_eq!(v["SourceIdsList"][0].as_str(), Some("db1"));
    assert_eq!(v["SourceIdsList"][1].as_str(), Some("db2"));
    assert_eq!(v["EventCategoriesList"][0].as_str(), Some("creation"));
    // Add/remove a source identifier mutates the persisted list.
    ok_on(
        &svc,
        "AddSourceIdentifierToSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "db3")],
    );
    let v = extras_value(&svc, "event_subscriptions", "es1");
    assert!(v["SourceIdsList"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x.as_str() == Some("db3")));
    ok_on(
        &svc,
        "RemoveSourceIdentifierFromSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "db1")],
    );
    let v = extras_value(&svc, "event_subscriptions", "es1");
    assert!(!v["SourceIdsList"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x.as_str() == Some("db1")));
}

#[test]
fn register_db_proxy_targets_round_trips() {
    let svc = svc();
    ok_on(&svc, "CreateDBProxy", &[("DBProxyName", "p1")]);
    ok_on(
        &svc,
        "RegisterDBProxyTargets",
        &[
            ("DBProxyName", "p1"),
            ("DBInstanceIdentifiers.member.1", "db1"),
        ],
    );
    let v = extras_value(&svc, "proxy_targets", "p1/default");
    assert_eq!(v[0]["RdsResourceId"].as_str(), Some("db1"));
    assert_eq!(v[0]["Type"].as_str(), Some("RDS_INSTANCE"));
    ok_on(
        &svc,
        "DeregisterDBProxyTargets",
        &[
            ("DBProxyName", "p1"),
            ("DBInstanceIdentifiers.member.1", "db1"),
        ],
    );
    let v = extras_value(&svc, "proxy_targets", "p1/default");
    assert_eq!(v.as_array().map(|a| a.len()), Some(0));
}

#[test]
fn modify_global_cluster_persists_deletion_protection() {
    let svc = svc();
    ok_on(
        &svc,
        "CreateGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "gc1"),
            ("Engine", "aurora-postgresql"),
        ],
    );
    ok_on(
        &svc,
        "ModifyGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "gc1"),
            ("DeletionProtection", "true"),
        ],
    );
    let v = extras_value(&svc, "global_clusters", "gc1");
    assert_eq!(v["DeletionProtection"].as_bool(), Some(true));
}

#[test]
fn modify_certificates_records_default() {
    let svc = svc();
    ok_on(
        &svc,
        "ModifyCertificates",
        &[("CertificateIdentifier", "rds-ca-rsa2048-g1")],
    );
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    assert_eq!(
        state.default_certificate_identifier.as_deref(),
        Some("rds-ca-rsa2048-g1"),
    );
    drop(accounts);
    ok_on(
        &svc,
        "ModifyCertificates",
        &[("RemoveCustomerOverride", "true")],
    );
    let accounts = svc.state_handle().read();
    let state = accounts.get("000000000000").unwrap();
    assert!(state.default_certificate_identifier.is_none());
}

#[test]
fn apply_pending_maintenance_action_drains_into_live_instance() {
    let svc = svc();
    seed_replica(&svc, "replica-1", "source-1");
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create("000000000000");
        let inst = state.instances.get_mut("source-1").unwrap();
        inst.pending_modified_values = Some(crate::state::PendingModifiedValues {
            engine_version: Some("16.4".to_string()),
            storage_type: Some("gp3".to_string()),
            ..Default::default()
        });
    }
    let arn = "arn:aws:rds:us-east-1:000000000000:db:source-1";
    let resp = svc
        .handle_extra_action(&req(
            "ApplyPendingMaintenanceAction",
            &[
                ("ResourceIdentifier", arn),
                ("ApplyAction", "system-update"),
                ("OptInType", "immediate"),
            ],
        ))
        .expect("ApplyPendingMaintenanceAction");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<ResourceIdentifier>"));
    assert!(body.contains("<PendingMaintenanceActionDetails/>"));
    let accounts = svc.state_handle().read();
    let inst = accounts
        .get("000000000000")
        .unwrap()
        .instances
        .get("source-1")
        .unwrap();
    assert!(inst.pending_modified_values.is_none());
    assert_eq!(inst.engine_version, "16.4");
    assert_eq!(inst.storage_type.as_deref(), Some("gp3"));
}

#[test]
fn apply_pending_maintenance_action_missing_action_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "ApplyPendingMaintenanceAction",
            &[(
                "ResourceIdentifier",
                "arn:aws:rds:us-east-1:000000000000:db:any",
            )],
        ))
        .err()
        .expect("missing ApplyAction should error");
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[test]
fn copy_db_cluster_snapshot_carries_source_engine() {
    let svc = svc();
    // Seed source cluster with an engine and snapshot it.
    ok_on(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "src"),
            ("Engine", "aurora-mysql"),
            ("EngineVersion", "8.0.32"),
        ],
    );
    snapshot_cluster(&svc, "snap-src", "src");
    ok_on(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "snap-src"),
            ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
        ],
    );
    let v = extras_value(&svc, "cluster_snapshots", "snap-copy");
    assert_eq!(v["Engine"].as_str(), Some("aurora-mysql"));
    assert_eq!(v["EngineVersion"].as_str(), Some("8.0.32"));
    assert_eq!(v["DBClusterIdentifier"].as_str(), Some("src"));
    assert_eq!(v["SnapshotType"].as_str(), Some("manual"));
}

#[test]
fn copy_db_cluster_snapshot_unknown_source_errors() {
    let svc = svc();
    let err = svc
        .handle_extra_action(&req(
            "CopyDBClusterSnapshot",
            &[
                ("SourceDBClusterSnapshotIdentifier", "ghost"),
                ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
            ],
        ))
        .err()
        .expect("missing source should error");
    assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
}

#[test]
fn start_activity_stream_returns_full_kms_arn() {
    let svc = svc();
    ok_on(&svc, "CreateDBCluster", &[("DBClusterIdentifier", "c1")]);
    let resp = svc
        .handle_extra_action(&req(
            "StartActivityStream",
            &[
                (
                    "ResourceArn",
                    "arn:aws:rds:us-east-1:000000000000:cluster:c1",
                ),
                ("KmsKeyId", "1234abcd-12ab-34cd-56ef-1234567890ab"),
                ("Mode", "sync"),
            ],
        ))
        .expect("StartActivityStream");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        body.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab-34cd-56ef-1234567890ab</KmsKeyId>"),
        "missing kms arn in {body}"
    );
    assert!(body.contains("<KinesisStreamName>aws-rds-das-c1</KinesisStreamName>"));
    assert!(body.contains("<Mode>sync</Mode>"));
    // Persisted on the cluster: DescribeDBClusters round-trips the stream.
    let describe = String::from_utf8(
        svc.handle_extra_action(&req("DescribeDBClusters", &[("DBClusterIdentifier", "c1")]))
            .expect("DescribeDBClusters")
            .body
            .expect_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        describe.contains("<ActivityStreamStatus>started</ActivityStreamStatus>"),
        "{describe}"
    );
    assert!(describe.contains("<ActivityStreamMode>sync</ActivityStreamMode>"));
}

#[test]
fn start_activity_stream_passes_through_existing_arn() {
    let svc = svc();
    ok_on(&svc, "CreateDBCluster", &[("DBClusterIdentifier", "c1")]);
    let resp = svc
        .handle_extra_action(&req(
            "StartActivityStream",
            &[
                (
                    "ResourceArn",
                    "arn:aws:rds:us-east-1:000000000000:cluster:c1",
                ),
                ("KmsKeyId", "arn:aws:kms:eu-west-1:222:key/abcd"),
            ],
        ))
        .expect("StartActivityStream");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<KmsKeyId>arn:aws:kms:eu-west-1:222:key/abcd</KmsKeyId>"));
}

#[test]
fn start_activity_stream_accepts_alias() {
    let svc = svc();
    ok_on(&svc, "CreateDBCluster", &[("DBClusterIdentifier", "c1")]);
    let resp = svc
        .handle_extra_action(&req(
            "StartActivityStream",
            &[
                (
                    "ResourceArn",
                    "arn:aws:rds:us-east-1:000000000000:cluster:c1",
                ),
                ("KmsKeyId", "alias/aws/rds"),
            ],
        ))
        .expect("StartActivityStream");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(body.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:alias/aws/rds</KmsKeyId>"));
}

#[test]
fn create_db_cluster_persists_safety_fields() {
    // Regression: CreateDBCluster dropped DeletionProtection / StorageEncrypted
    // / KmsKeyId / BackupRetentionPeriod / DatabaseName until a follow-up
    // ModifyDBCluster. They must be persisted at create time and echoed by both
    // CreateDBCluster and DescribeDBClusters.
    let svc = svc();
    let resp = svc
        .handle_extra_action(&req(
            "CreateDBCluster",
            &[
                ("DBClusterIdentifier", "secure"),
                ("Engine", "aurora-postgresql"),
                ("DeletionProtection", "true"),
                ("StorageEncrypted", "true"),
                (
                    "KmsKeyId",
                    "arn:aws:kms:us-east-1:000000000000:key/abcd-1234",
                ),
                ("BackupRetentionPeriod", "14"),
                ("DatabaseName", "appdb"),
            ],
        ))
        .expect("CreateDBCluster");
    let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
    assert!(
        body.contains("<DeletionProtection>true</DeletionProtection>"),
        "create body missing DeletionProtection: {body}"
    );
    assert!(
        body.contains("<StorageEncrypted>true</StorageEncrypted>"),
        "create body missing StorageEncrypted: {body}"
    );
    assert!(
        body.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:key/abcd-1234</KmsKeyId>"),
        "create body missing KmsKeyId: {body}"
    );
    assert!(
        body.contains("<BackupRetentionPeriod>14</BackupRetentionPeriod>"),
        "create body missing BackupRetentionPeriod: {body}"
    );
    assert!(
        body.contains("<DatabaseName>appdb</DatabaseName>"),
        "create body missing DatabaseName: {body}"
    );

    // The same values must survive into DescribeDBClusters.
    let dr = svc
        .handle_extra_action(&req("DescribeDBClusters", &[]))
        .unwrap();
    let dbody = String::from_utf8(dr.body.expect_bytes().to_vec()).unwrap();
    assert!(
        dbody.contains("<DeletionProtection>true</DeletionProtection>"),
        "describe missing DeletionProtection: {dbody}"
    );
    assert!(
        dbody.contains("<StorageEncrypted>true</StorageEncrypted>"),
        "describe missing StorageEncrypted: {dbody}"
    );
    assert!(
        dbody.contains("<KmsKeyId>arn:aws:kms:us-east-1:000000000000:key/abcd-1234</KmsKeyId>"),
        "describe missing KmsKeyId: {dbody}"
    );
    assert!(
        dbody.contains("<BackupRetentionPeriod>14</BackupRetentionPeriod>"),
        "describe missing BackupRetentionPeriod: {dbody}"
    );
    assert!(
        dbody.contains("<DatabaseName>appdb</DatabaseName>"),
        "describe missing DatabaseName: {dbody}"
    );
}

// ── Describe* Filters ────────────────────────────────────────────

/// Body of a successful extras action.
fn body_of_action(svc: &RdsService, action: &str, params: &[(&str, &str)]) -> String {
    let resp = svc
        .handle_extra_action(&req(action, params))
        .unwrap_or_else(|e| panic!("{action} failed: {e:?}"));
    assert!(resp.status.is_success(), "{action} status: {}", resp.status);
    String::from_utf8(resp.body.expect_bytes().to_vec()).expect("utf8")
}

fn local_snapshot(snapshot_id: &str, instance_id: &str, account: &str) -> crate::state::DbSnapshot {
    crate::state::DbSnapshot {
        db_snapshot_identifier: snapshot_id.to_string(),
        db_snapshot_arn: format!("arn:aws:rds:us-east-1:{account}:snapshot:{snapshot_id}"),
        db_instance_identifier: instance_id.to_string(),
        snapshot_create_time: chrono::Utc::now(),
        engine: "postgres".to_string(),
        engine_version: "16.3".to_string(),
        allocated_storage: 20,
        status: "available".to_string(),
        port: 5432,
        master_username: "admin".to_string(),
        db_name: Some("appdb".to_string()),
        dbi_resource_id: format!("db-{snapshot_id}"),
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

fn seed_cluster(svc: &RdsService, id: &str, resource_id: &str, engine: &str) {
    let state = svc.state_handle();
    let mut accounts = state.write();
    let s = accounts.get_or_create("000000000000");
    s.extras.entry("clusters".to_string()).or_default().insert(
        id.to_string(),
        json!({
            "DBClusterIdentifier": id,
            "DBClusterArn": format!("arn:aws:rds:us-east-1:000000000000:cluster:{id}"),
            "DbClusterResourceId": resource_id,
            "Status": "available",
            "Engine": engine,
        }),
    );
}

/// Snapshot a seeded cluster the way `CreateDBClusterSnapshot` does --
/// clone the cluster row, stamp the snapshot fields -- without needing a
/// container runtime to dump a writer.
fn snapshot_cluster(svc: &RdsService, snapshot_id: &str, cluster_id: &str) {
    let state = svc.state_handle();
    let mut accounts = state.write();
    let s = accounts.get_or_create("000000000000");
    let mut entry = s
        .extras
        .get("clusters")
        .and_then(|m| m.get(cluster_id))
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some(obj) = entry.as_object_mut() {
        obj.insert(
            "DBClusterSnapshotIdentifier".to_string(),
            json!(snapshot_id),
        );
        obj.insert(
            "DBClusterSnapshotArn".to_string(),
            json!(format!(
                "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:{snapshot_id}"
            )),
        );
        obj.insert("DBClusterIdentifier".to_string(), json!(cluster_id));
        obj.insert("Status".to_string(), json!("available"));
        obj.insert("SnapshotType".to_string(), json!("manual"));
    }
    s.extras
        .entry("cluster_snapshots".to_string())
        .or_default()
        .insert(snapshot_id.to_string(), entry);
}

fn seed_cluster_snapshot(svc: &RdsService, id: &str, cluster: &str, snapshot_type: &str) {
    let state = svc.state_handle();
    let mut accounts = state.write();
    let s = accounts.get_or_create("000000000000");
    s.extras
        .entry("cluster_snapshots".to_string())
        .or_default()
        .insert(
            id.to_string(),
            json!({
                "DBClusterSnapshotIdentifier": id,
                "DBClusterSnapshotArn":
                    format!("arn:aws:rds:us-east-1:000000000000:cluster-snapshot:{id}"),
                "DBClusterIdentifier": cluster,
                "Status": "available",
                "SnapshotType": snapshot_type,
                "Engine": "aurora-postgresql",
            }),
        );
}

#[test]
fn describe_db_clusters_filters_by_db_cluster_resource_id() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");
    seed_cluster(&svc, "clu-2", "cluster-BBBB", "aurora-mysql");

    let body = body_of_action(
        &svc,
        "DescribeDBClusters",
        &[
            ("Filters.Filter.1.Name", "db-cluster-resource-id"),
            ("Filters.Filter.1.Values.Value.1", "cluster-BBBB"),
        ],
    );

    assert!(body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"));
    assert!(!body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"));
}

#[test]
fn describe_db_clusters_filters_by_db_cluster_id_and_arn() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");
    seed_cluster(&svc, "clu-2", "cluster-BBBB", "aurora-mysql");

    for value in ["clu-1", "arn:aws:rds:us-east-1:000000000000:cluster:clu-1"] {
        let body = body_of_action(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", value),
            ],
        );
        assert!(
            body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"),
            "value {value} body: {body}"
        );
        assert!(!body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"));
    }
}

#[test]
fn describe_db_clusters_unrecognized_filter_matches_nothing() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");

    let body = body_of_action(
        &svc,
        "DescribeDBClusters",
        &[
            ("Filters.Filter.1.Name", "not-a-real-filter"),
            ("Filters.Filter.1.Values.Value.1", "whatever"),
        ],
    );

    assert!(!body.contains("<DBClusterIdentifier>"), "body: {body}");
}

#[test]
fn describe_db_cluster_snapshots_honors_the_identifier_params() {
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");
    seed_cluster_snapshot(&svc, "snap-2", "clu-2", "manual");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "snap-2")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>snap-2</DBClusterSnapshotIdentifier>"));
    assert!(!body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"));

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterIdentifier", "clu-1")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"));
    assert!(!body.contains("<DBClusterSnapshotIdentifier>snap-2</DBClusterSnapshotIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_unknown_identifier_is_not_found() {
    // `DBClusterSnapshotNotFoundFault` is declared on this operation, so
    // an unknown named snapshot errors rather than returning an empty
    // list -- matching DescribeDBInstances / DescribeDBSnapshots.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "ghost")],
    ));
    match result {
        Err(err) => assert!(
            format!("{err:?}").contains("DBClusterSnapshotNotFoundFault"),
            "unexpected error: {err:?}"
        ),
        Ok(_) => panic!("unknown snapshot should be a fault"),
    }
}

#[test]
fn describe_lists_use_the_smithy_member_tag() {
    // The AWS SDKs unmarshal an empty list from the generic `<member>`
    // element, so every list whose Smithy member declares an `xmlName`
    // has to emit that name -- otherwise the rows are on the wire but
    // invisible to real clients. Names verified against aws-models.
    let cases = [
        (
            "cluster_endpoints",
            "DescribeDBClusterEndpoints",
            "DBClusterEndpointList",
        ),
        (
            "security_groups",
            "DescribeDBSecurityGroups",
            "DBSecurityGroup",
        ),
        ("integrations", "DescribeIntegrations", "Integration"),
        ("shard_groups", "DescribeDBShardGroups", "DBShardGroup"),
        ("tenant_dbs", "DescribeTenantDatabases", "TenantDatabase"),
        ("export_tasks", "DescribeExportTasks", "ExportTask"),
    ];

    for (category, action, member_tag) in cases {
        let svc = svc();
        {
            let state = svc.state_handle();
            let mut accounts = state.write();
            let s = accounts.get_or_create("000000000000");
            s.extras
                .entry(category.to_string())
                .or_default()
                .insert("entry-1".to_string(), json!({"Status": "available"}));
        }

        let body = body_of_action(&svc, action, &[]);
        assert!(
            body.contains(&format!("<{member_tag}>")),
            "{action} did not use <{member_tag}>: {body}"
        );
        assert!(
            !body.contains("<member>"),
            "{action} still emits the generic <member>: {body}"
        );
    }
}

#[test]
fn cluster_snapshot_attributes_round_trip_and_drive_shared() {
    // ModifyDBClusterSnapshotAttribute used to be a no-op that always
    // rendered an empty attribute set, so no cluster snapshot could ever
    // appear under SnapshotType=shared.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let body = body_of_action(
        &svc,
        "ModifyDBClusterSnapshotAttribute",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("AttributeName", "restore"),
            ("ValuesToAdd.AttributeValue.1", "111111111111"),
        ],
    );
    assert!(body.contains("<AttributeName>restore</AttributeName>"));
    assert!(body.contains("<AttributeValue>111111111111</AttributeValue>"));

    // Describe reads the stored value back.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshotAttributes",
        &[("DBClusterSnapshotIdentifier", "snap-1")],
    );
    assert!(body.contains("<AttributeValue>111111111111</AttributeValue>"));

    // Removing the last value reads back as unshared, matching AWS.
    let body = body_of_action(
        &svc,
        "ModifyDBClusterSnapshotAttribute",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("AttributeName", "restore"),
            ("ValuesToRemove.AttributeValue.1", "111111111111"),
        ],
    );
    assert!(body.contains("<DBClusterSnapshotAttributes/>"));
}

#[test]
fn describe_db_cluster_snapshots_reports_shared_and_public() {
    // A snapshot another account shared with this caller is selected by
    // SnapshotType=shared; one shared with `all` by SnapshotType=public.
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        let bucket = other
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default();
        bucket.insert(
            "shared-snap".to_string(),
            json!({
                "DBClusterSnapshotIdentifier": "shared-snap",
                "DBClusterIdentifier": "other-clu",
                "Status": "available",
                "SnapshotType": "manual",
                "SnapshotAttributes": {"restore": ["000000000000"]},
            }),
        );
        bucket.insert(
            "public-snap".to_string(),
            json!({
                "DBClusterSnapshotIdentifier": "public-snap",
                "DBClusterIdentifier": "other-clu",
                "Status": "available",
                "SnapshotType": "manual",
                "SnapshotAttributes": {"restore": ["all"]},
            }),
        );
        bucket.insert(
            "private-snap".to_string(),
            json!({
                "DBClusterSnapshotIdentifier": "private-snap",
                "DBClusterIdentifier": "other-clu",
                "Status": "available",
                "SnapshotType": "manual",
            }),
        );
    }
    seed_cluster_snapshot(&svc, "mine", "clu-1", "manual");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("SnapshotType", "shared")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>private-snap</DBClusterSnapshotIdentifier>")
    );
    assert!(!body.contains("<DBClusterSnapshotIdentifier>mine</DBClusterSnapshotIdentifier>"));

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("SnapshotType", "public")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>public-snap</DBClusterSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
    );

    // An owned type still lists only the caller's own snapshots.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("SnapshotType", "manual")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>mine</DBClusterSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
    );
}

#[test]
fn copy_db_cluster_snapshot_does_not_inherit_the_share_list() {
    // A copy is a fresh sharing surface: inheriting the source's
    // `restore` list would publish a snapshot nobody shared.
    let svc = svc();
    create_cluster(&svc, "src");
    snapshot_cluster(&svc, "s1", "src");
    ok_on(
        &svc,
        "ModifyDBClusterSnapshotAttribute",
        &[
            ("DBClusterSnapshotIdentifier", "s1"),
            ("AttributeName", "restore"),
            ("ValuesToAdd.AttributeValue.1", "all"),
        ],
    );
    ok_on(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "s1"),
            ("TargetDBClusterSnapshotIdentifier", "s2"),
        ],
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshotAttributes",
        &[("DBClusterSnapshotIdentifier", "s2")],
    );
    assert!(
        body.contains("<DBClusterSnapshotAttributes/>"),
        "copied snapshot inherited the share list: {body}"
    );
}

#[test]
fn modify_db_cluster_snapshot_attribute_refuses_another_accounts_arn() {
    let svc = svc();
    seed_cluster_snapshot(&svc, "prod-snap", "clu-1", "manual");

    let result = svc.handle_extra_action(&req(
        "ModifyDBClusterSnapshotAttribute",
        &[
            (
                "DBClusterSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:cluster-snapshot:prod-snap",
            ),
            ("AttributeName", "restore"),
            ("ValuesToAdd.AttributeValue.1", "all"),
        ],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("a foreign ARN modified the local snapshot"),
    }

    // The local snapshot is untouched.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshotAttributes",
        &[("DBClusterSnapshotIdentifier", "prod-snap")],
    );
    assert!(body.contains("<DBClusterSnapshotAttributes/>"));
}

#[test]
fn describe_db_cluster_snapshots_resolves_a_named_shared_snapshot() {
    // Addressing a shared snapshot by identifier resolves it without
    // IncludeShared; previously the existence check accepted it but the
    // listing dropped it, yielding 200 with an empty list.
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        other
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "shared-snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "shared-snap",
                    "DBClusterIdentifier": "other-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    // AWS requires the ARN to reach another account's shared snapshot;
    // a bare id could match several accounts and return duplicate rows.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:999999999999:cluster-snapshot:shared-snap",
        )],
    );
    assert!(
        body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>"),
        "named shared snapshot returned an empty list: {body}"
    );

    // The bare id names nothing this account owns.
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "shared-snap")],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("a bare id reached another account's snapshot"),
    }
}

#[test]
fn copy_db_snapshot_prefers_the_account_the_arn_names() {
    // A foreign ARN must not silently copy this account's same-named
    // snapshot -- the caller would end up with the wrong data.
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let mine = accounts.get_or_create("000000000000");
        mine.snapshots.insert(
            "snap-1".to_string(),
            local_snapshot("snap-1", "my-db", "000000000000"),
        );
        let other = accounts.get_or_create("999999999999");
        let mut shared = local_snapshot("snap-1", "their-db", "999999999999");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["000000000000".to_string()]);
        other.snapshots.insert("snap-1".to_string(), shared);
    }

    // AWS supports copying a snapshot shared with you: the ARN picks the
    // other account's row, not the local one.
    ok_on(
        &svc,
        "CopyDBSnapshot",
        &[
            (
                "SourceDBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:snap-1",
            ),
            ("TargetDBSnapshotIdentifier", "copy-1"),
        ],
    );
    let copied = svc
        .state_handle()
        .read()
        .get("000000000000")
        .and_then(|s| s.snapshots.get("copy-1").cloned())
        .expect("copy recorded");
    assert_eq!(
        copied.db_instance_identifier, "their-db",
        "copied the local snapshot instead of the shared one"
    );
}

#[test]
fn copy_db_snapshot_rejects_an_unshared_foreign_arn() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        other.snapshots.insert(
            "snap-1".to_string(),
            local_snapshot("snap-1", "their-db", "999999999999"),
        );
    }

    let result = svc.handle_extra_action(&req(
        "CopyDBSnapshot",
        &[
            (
                "SourceDBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:snap-1",
            ),
            ("TargetDBSnapshotIdentifier", "copy-1"),
        ],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBSnapshotNotFound"),
        Ok(_) => panic!("copied a snapshot nobody shared"),
    }
}

#[test]
fn describe_db_cluster_snapshots_named_lookup_prefers_the_owned_row() {
    // Two rows for one named id is the "couldn't resolve a single
    // result" failure this whole change set exists to avoid.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap", "clu-1", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        other
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "snap",
                    "DBClusterIdentifier": "other-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "snap")],
    );
    assert_eq!(
        body.matches("<DBClusterSnapshotIdentifier>snap</DBClusterSnapshotIdentifier>")
            .count(),
        1,
        "named lookup returned more than one row: {body}"
    );
    assert!(body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"));
}

#[test]
fn wrong_type_arns_do_not_widen_the_cluster_describes() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");
    seed_cluster(&svc, "clu-2", "cluster-BBBB", "aurora-mysql");
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    // A DB-instance ARN is not a cluster.
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusters",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:000000000000:db:clu-1",
        )],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("a db ARN resolved as a cluster"),
    }

    // A cluster ARN is not a cluster snapshot.
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster:snap-1",
        )],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("a cluster ARN resolved as a cluster snapshot"),
    }

    // A wrong-type cluster filter matches nothing rather than listing all.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:000000000000:db:clu-1",
        )],
    );
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>"),
        "wrong-type cluster filter widened the listing: {body}"
    );
}

#[test]
fn wrong_type_arns_raise_the_declared_fault_not_invalid_parameter() {
    // `InvalidParameterValue` isn't declared on any of these ops, so an
    // unmodeled error would hard-fail a Terraform destroy that should
    // simply treat the snapshot as gone.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");
    let wrong_type = "arn:aws:rds:us-east-1:000000000000:snapshot:snap-1";

    for action in [
        "DeleteDBClusterSnapshot",
        "DescribeDBClusterSnapshotAttributes",
    ] {
        let result =
            svc.handle_extra_action(&req(action, &[("DBClusterSnapshotIdentifier", wrong_type)]));
        match result {
            Err(err) => assert_eq!(
                err.code(),
                "DBClusterSnapshotNotFoundFault",
                "{action} raised {}",
                err.code()
            ),
            Ok(_) => panic!("{action} accepted a wrong-type ARN"),
        }
    }

    let result = svc.handle_extra_action(&req(
        "ModifyDBSnapshot",
        &[(
            "DBSnapshotIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:snap-1",
        )],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBSnapshotNotFound"),
        Ok(_) => panic!("ModifyDBSnapshot accepted a wrong-type ARN"),
    }
}

#[test]
fn describe_db_clusters_rejects_another_accounts_arn() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");

    let result = svc.handle_extra_action(&req(
        "DescribeDBClusters",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:999999999999:cluster:clu-1",
        )],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("a foreign ARN resolved to the local cluster"),
    }
}

#[test]
fn describe_db_cluster_snapshots_honors_the_arn_account() {
    // A foreign ARN must resolve against the account it names -- not
    // this account's same-named snapshot, and not a third account that
    // happens to have shared one under the same id.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "my-clu", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let sharer = accounts.get_or_create("333333333333");
        sharer
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "sharer-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    // An ARN naming account 111 matches neither the local row nor 333's.
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:111111111111:cluster-snapshot:snap-1",
        )],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(resp) => {
            let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
            panic!("a foreign ARN resolved: {body}");
        }
    }

    // The sharer's own ARN resolves to the sharer's row.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:333333333333:cluster-snapshot:snap-1",
        )],
    );
    assert!(body.contains("<DBClusterIdentifier>sharer-clu</DBClusterIdentifier>"));
    assert!(!body.contains("<DBClusterIdentifier>my-clu</DBClusterIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_filtered_owned_row_is_not_replaced() {
    // An owned row excluded by a filter must yield an empty result, not
    // fall through to another account's identically-named snapshot.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "my-clu", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        // The owned row is aurora-postgresql...
        if let Some(entry) = accounts
            .get_or_create("000000000000")
            .extras
            .get_mut("cluster_snapshots")
            .and_then(|m| m.get_mut("snap-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-postgresql"));
        }
        // ...while a shared one with the same id is aurora-mysql.
        accounts
            .get_or_create("999999999999")
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "other-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "Engine": "aurora-mysql",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "aurora-mysql"),
        ],
    );
    assert!(
        !body.contains("<DBClusterIdentifier>other-clu</DBClusterIdentifier>"),
        "a filtered-out owned row was replaced by a foreign one: {body}"
    );
}

#[test]
fn describe_db_cluster_snapshots_honors_the_cluster_resource_id_parameter() {
    // Modeled narrowing parameter; ignoring it returns the whole list.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");
    seed_cluster_snapshot(&svc, "snap-2", "clu-2", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        for (id, resource) in [("snap-1", "cluster-AAAA"), ("snap-2", "cluster-BBBB")] {
            if let Some(entry) = s
                .extras
                .get_mut("cluster_snapshots")
                .and_then(|m| m.get_mut(id))
                .and_then(|v| v.as_object_mut())
            {
                entry.insert("DbClusterResourceId".to_string(), json!(resource));
            }
        }
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DbClusterResourceId", "cluster-BBBB")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>snap-2</DBClusterSnapshotIdentifier>"));
    assert!(!body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_paginates_and_orders_stably() {
    // Sharing makes an unqualified listing unbounded, and the
    // cross-account scan walks a HashMap -- so the rows need an explicit
    // order and the modeled MaxRecords / Marker have to work.
    let svc = svc();
    for i in 1..=5 {
        seed_cluster_snapshot(&svc, &format!("snap-{i}"), "clu-1", "manual");
    }

    let first = body_of_action(&svc, "DescribeDBClusterSnapshots", &[("MaxRecords", "2")]);
    assert_eq!(
        first.matches("<DBClusterSnapshotIdentifier>").count(),
        2,
        "MaxRecords ignored: {first}"
    );
    let marker = first
        .split("<Marker>")
        .nth(1)
        .and_then(|rest| rest.split("</Marker>").next())
        .expect("a next-page marker")
        .to_string();

    let second = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("MaxRecords", "2"), ("Marker", &marker)],
    );
    assert_eq!(second.matches("<DBClusterSnapshotIdentifier>").count(), 2);
    // The second page does not repeat the first.
    for id in ["snap-1", "snap-2", "snap-3", "snap-4", "snap-5"] {
        let tag = format!("<DBClusterSnapshotIdentifier>{id}</DBClusterSnapshotIdentifier>");
        assert!(
            !(first.contains(&tag) && second.contains(&tag)),
            "{id} appeared on both pages"
        );
    }

    // Identical requests return identical order.
    let again = body_of_action(&svc, "DescribeDBClusterSnapshots", &[("MaxRecords", "2")]);
    assert_eq!(first, again, "listing order is not stable");
}

#[test]
fn describe_db_cluster_snapshots_honors_include_shared() {
    // The modeled IncludeShared / IncludePublic members widen an
    // unqualified listing, as on DescribeDBSnapshots.
    let svc = svc();
    seed_cluster_snapshot(&svc, "mine", "clu-1", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        other
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "shared-snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "shared-snap",
                    "DBClusterIdentifier": "other-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &[]);
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("IncludeShared", "true")],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>mine</DBClusterSnapshotIdentifier>"));
    assert!(body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_accepts_an_arn_identifier() {
    // Clients pass the snapshot ARN here as readily as the plain id
    // (CopyDBClusterSnapshot normalizes the same way), so an ARN must
    // resolve rather than 404.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");
    seed_cluster_snapshot(&svc, "snap-2", "clu-2", "manual");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:snap-1",
        )],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"));
    assert!(!body.contains("<DBClusterSnapshotIdentifier>snap-2</DBClusterSnapshotIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_treats_empty_identifiers_as_absent() {
    // `DBClusterSnapshotIdentifier=` on the wire reaches the handler as
    // Some(""); AWS ignores an empty parameter rather than matching the
    // empty string, so this must list, not raise NotFound.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("DBClusterSnapshotIdentifier", ""),
            ("DBClusterIdentifier", ""),
            ("SnapshotType", ""),
        ],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"));
}

#[test]
fn describe_db_cluster_snapshots_defaults_missing_snapshot_type_to_manual() {
    // A stored entry written before SnapshotType was persisted renders
    // as `manual`, so it must also be selected by `manual` -- renderer
    // and matcher have to share the default.
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        s.extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "legacy-snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "legacy-snap",
                    "DBClusterIdentifier": "clu-1",
                    "Status": "available",
                }),
            );
    }

    let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &[]);
    assert!(
        body.contains("<SnapshotType>manual</SnapshotType>"),
        "renderer default changed: {body}"
    );

    for params in [
        vec![("SnapshotType", "manual")],
        vec![
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "manual"),
        ],
    ] {
        let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &params);
        assert!(
            body.contains("<DBClusterSnapshotIdentifier>legacy-snap</DBClusterSnapshotIdentifier>"),
            "entry rendered as manual but excluded by {params:?}: {body}"
        );
    }
}

#[test]
fn describe_db_cluster_snapshots_keeps_colon_bearing_identifiers() {
    // `rds:mydb-...` is a real AWS identifier, not an ARN: it must be
    // looked up verbatim rather than trimmed at the last colon.
    let svc = svc();
    seed_cluster_snapshot(&svc, "rds:clu-1-2026-08-30-06-00", "clu-1", "automated");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "rds:clu-1-2026-08-30-06-00")],
    );
    assert!(body.contains(
        "<DBClusterSnapshotIdentifier>rds:clu-1-2026-08-30-06-00</DBClusterSnapshotIdentifier>"
    ));
}

#[test]
fn delete_db_cluster_snapshot_unknown_identifier_is_not_found() {
    // A 200 here would report success -- and emit a "snapshot deleted"
    // event -- for a snapshot that never existed, while the sibling
    // Describe raises the fault for the same id.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let result = svc.handle_extra_action(&req(
        "DeleteDBClusterSnapshot",
        &[("DBClusterSnapshotIdentifier", "ghost")],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("deleting a nonexistent snapshot reported success"),
    }
}

#[test]
fn a_named_owned_cluster_snapshot_is_never_shadowed_by_a_shared_one() {
    // `data.aws_db_cluster_snapshot` sets include_shared, so the flag
    // must not append another account's row for an id the caller owns --
    // two rows is the "couldn't resolve a single result" failure.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "my-clu", "manual");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .get_or_create("999999999999")
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "snap-1".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "snap-1",
                    "DBClusterIdentifier": "other-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("IncludeShared", "true"),
        ],
    );
    assert_eq!(
        body.matches("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>")
            .count(),
        1,
        "IncludeShared shadowed the owned row: {body}"
    );
    assert!(body.contains("<DBClusterIdentifier>my-clu</DBClusterIdentifier>"));
}

#[test]
fn copy_db_cluster_snapshot_response_reports_the_snapshot_type() {
    // The Describe path reports SnapshotType/Engine, so the copy
    // response must too -- otherwise the client reads a blank off the
    // copy it just made.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let body = body_of_action(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "snap-1"),
            ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
        ],
    );
    assert!(
        body.contains("<SnapshotType>manual</SnapshotType>"),
        "{body}"
    );
    assert!(
        body.contains("<Engine>aurora-postgresql</Engine>"),
        "{body}"
    );
}

#[test]
fn cluster_snapshot_responses_report_the_stored_fields() {
    // A copied snapshot's source ARN and the cluster resource id are
    // stored and modeled; `aws_db_cluster_snapshot` reads both, and the
    // delete response reports the same detail as create / copy.
    let svc = svc();
    create_cluster(&svc, "src");
    snapshot_cluster(&svc, "snap-1", "src");

    let body = body_of_action(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "snap-1"),
            ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
        ],
    );
    assert!(
        body.contains("<SnapshotType>manual</SnapshotType>"),
        "{body}"
    );

    let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &[]);
    assert!(
        body.contains("<SourceDBClusterSnapshotArn>"),
        "copy source ARN not reported: {body}"
    );
    assert!(
        body.contains("<DbClusterResourceId>"),
        "cluster resource id not reported: {body}"
    );

    let body = body_of_action(
        &svc,
        "DeleteDBClusterSnapshot",
        &[("DBClusterSnapshotIdentifier", "snap-copy")],
    );
    assert!(
        body.contains("<SnapshotType>manual</SnapshotType>"),
        "delete response dropped the detail fields: {body}"
    );
}

#[test]
fn copy_db_cluster_snapshot_stamps_its_own_creation_time() {
    // The copy is created now, not when its source was -- CopyDBSnapshot
    // already behaves this way, and a stale time sorts the copy wrongly
    // in a time-ordered listing.
    let svc = svc();
    create_cluster(&svc, "src");
    snapshot_cluster(&svc, "snap-1", "src");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .get_or_create("000000000000")
            .extras
            .get_mut("cluster_snapshots")
            .and_then(|m| m.get_mut("snap-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert(
                "SnapshotCreateTime".to_string(),
                json!("2020-01-01T00:00:00+00:00"),
            );
        }
    }

    ok_on(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "snap-1"),
            ("TargetDBClusterSnapshotIdentifier", "snap-copy"),
        ],
    );

    let copied = extras_value(&svc, "cluster_snapshots", "snap-copy");
    // Require the field: `as_str()` on a missing key is None, which
    // would satisfy an inequality against the source's time and let a
    // copy that reports NO creation time pass.
    let copied_time = copied["SnapshotCreateTime"]
        .as_str()
        .expect("the copy records its own creation time")
        .to_string();
    assert_ne!(
        copied_time, "2020-01-01T00:00:00+00:00",
        "the copy kept its source's creation time"
    );
    // And it records where it was copied from, as an ARN.
    assert!(copied["SourceDBClusterSnapshotArn"]
        .as_str()
        .unwrap_or_default()
        .starts_with("arn:aws:rds:"));
}

#[test]
fn copy_db_cluster_snapshot_rejects_an_existing_target() {
    // Overwriting would silently replace the target's dump and revoke
    // its sharing on a retried copy.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");
    seed_cluster_snapshot(&svc, "snap-2", "clu-2", "manual");

    let result = svc.handle_extra_action(&req(
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "snap-1"),
            ("TargetDBClusterSnapshotIdentifier", "snap-2"),
        ],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotAlreadyExistsFault"),
        Ok(_) => panic!("copy overwrote an existing snapshot"),
    }

    // The existing target is untouched.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "snap-2")],
    );
    assert!(body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"));
}

#[test]
fn delete_db_cluster_snapshot_accepts_an_arn_identifier() {
    // A delete that doesn't resolve the ARN reports success while
    // leaving the entry behind, so the following Describe keeps
    // reporting the snapshot and a destroy never converges.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    ok_on(
        &svc,
        "DeleteDBClusterSnapshot",
        &[(
            "DBClusterSnapshotIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:snap-1",
        )],
    );

    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "snap-1")],
    ));
    match result {
        Err(err) => assert!(
            format!("{err:?}").contains("DBClusterSnapshotNotFoundFault"),
            "unexpected error: {err:?}"
        ),
        Ok(_) => panic!("snapshot still present after ARN-form delete"),
    }
}

#[test]
fn describe_db_clusters_identifier_accepts_arn_and_ignores_empty() {
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");
    seed_cluster(&svc, "clu-2", "cluster-BBBB", "aurora-mysql");

    // Empty parameter means "not supplied", so everything lists.
    let body = body_of_action(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "")]);
    assert!(body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"));
    assert!(body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"));

    // AWS documents this parameter as accepting a cluster ARN.
    let body = body_of_action(
        &svc,
        "DescribeDBClusters",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster:clu-1",
        )],
    );
    assert!(body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"));
    assert!(!body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"));
}

#[test]
fn describe_db_clusters_unknown_identifier_is_not_found() {
    // `DBClusterNotFoundFault` is declared on the operation, so a named
    // cluster that doesn't exist errors rather than returning an empty
    // list -- a client polling a deleted cluster needs to tell "gone"
    // from "no match".
    let svc = svc();
    seed_cluster(&svc, "clu-1", "cluster-AAAA", "aurora-postgresql");

    let result = svc.handle_extra_action(&req(
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "ghost")],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("unknown cluster should be a fault"),
    }
}

#[test]
fn describe_db_clusters_reports_clone_group_id() {
    // A copy-on-write restore puts source and clone in one clone group,
    // which is what the `clone-group-id` filter selects on.
    let svc = svc();
    seed_cluster(&svc, "source-clu", "cluster-AAAA", "aurora-postgresql");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        for (id, resource_id) in [
            ("source-clu", "cluster-AAAA"),
            ("clone-clu", "cluster-BBBB"),
        ] {
            let entry = s
                .extras
                .entry("clusters".to_string())
                .or_default()
                .entry(id.to_string())
                .or_insert_with(|| {
                    json!({
                        "DBClusterIdentifier": id,
                        "DBClusterArn":
                            format!("arn:aws:rds:us-east-1:000000000000:cluster:{id}"),
                        "DbClusterResourceId": resource_id,
                        "Status": "available",
                        "Engine": "aurora-postgresql",
                    })
                });
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("CloneGroupId".to_string(), json!("clone-group-1"));
            }
        }
    }

    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert!(
        body.contains("<CloneGroupId>clone-group-1</CloneGroupId>"),
        "clone group id not rendered: {body}"
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusters",
        &[
            ("Filters.Filter.1.Name", "clone-group-id"),
            ("Filters.Filter.1.Values.Value.1", "clone-group-1"),
        ],
    );
    assert!(body.contains("<DBClusterIdentifier>source-clu</DBClusterIdentifier>"));
    assert!(body.contains("<DBClusterIdentifier>clone-clu</DBClusterIdentifier>"));

    let body = body_of_action(
        &svc,
        "DescribeDBClusters",
        &[
            ("Filters.Filter.1.Name", "clone-group-id"),
            ("Filters.Filter.1.Values.Value.1", "other-group"),
        ],
    );
    assert!(!body.contains("<DBClusterIdentifier>"), "body: {body}");
}

#[test]
fn describe_db_cluster_snapshots_filters_by_snapshot_type_and_cluster() {
    let svc = svc();
    seed_cluster_snapshot(&svc, "manual-snap", "clu-1", "manual");
    seed_cluster_snapshot(&svc, "auto-snap", "clu-1", "automated");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("Filters.Filter.1.Name", "snapshot-type"),
            ("Filters.Filter.1.Values.Value.1", "automated"),
        ],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>auto-snap</DBClusterSnapshotIdentifier>"));
    assert!(
        !body.contains("<DBClusterSnapshotIdentifier>manual-snap</DBClusterSnapshotIdentifier>")
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("Filters.Filter.1.Name", "db-cluster-id"),
            (
                "Filters.Filter.1.Values.Value.1",
                "arn:aws:rds:us-east-1:000000000000:cluster:clu-1",
            ),
        ],
    );
    assert!(body.contains("<DBClusterSnapshotIdentifier>auto-snap</DBClusterSnapshotIdentifier>"));
    assert!(body.contains("<DBClusterSnapshotIdentifier>manual-snap</DBClusterSnapshotIdentifier>"));
}
