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
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ce1"),
            ("DBClusterIdentifier", "clu-1"),
        ],
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
    // Role association is no longer a no-op: it needs a real cluster.
    let svc = svc();
    create_cluster(&svc, "role-clu");
    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "role-clu"),
            ("RoleArn", "arn:aws:iam::000000000000:role/rds-role"),
        ],
    );
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "role-clu"),
            ("RoleArn", "arn:aws:iam::000000000000:role/rds-role"),
        ],
    );
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
            associated_roles: Vec::new(),
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
            associated_roles: Vec::new(),
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
            associated_roles: Vec::new(),
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
    create_cluster(&svc, "c1");
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
    // AWS maps the request's EndpointType onto CustomEndpointType and
    // reports the endpoint itself as CUSTOM -- this operation only ever
    // creates custom endpoints.
    assert_eq!(v["CustomEndpointType"].as_str(), Some("ANY"));
    assert_eq!(v["EndpointType"].as_str(), Some("CUSTOM"));
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
        source_db_snapshot_arn: None,
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
        body.matches("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
            .count()
            == 1,
        "named shared snapshot did not return exactly one row: {body}"
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

    // IncludeShared widens to foreign rows, which is exactly what
    // `data.aws_db_cluster_snapshot` sends. With one sharer the bare id
    // still resolves to that single row.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("DBClusterSnapshotIdentifier", "shared-snap"),
            ("IncludeShared", "true"),
        ],
    );
    // Exactly one row, not merely "present": two accounts sharing the
    // same name is the duplicate this branch exists to prevent, and a
    // `contains` check would pass on it.
    assert_eq!(
        body.matches("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
            .count(),
        1,
        "IncludeShared did not return exactly one shared row: {body}"
    );

    // Once a SECOND account shares a snapshot of the same name, the bare
    // id names two rows. Returning both would hand the data source two
    // results for one lookup, so an ambiguous bare id resolves to
    // nothing and the caller has to pin the owner with an ARN.
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let third = accounts.get_or_create("888888888888");
        third
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "shared-snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "shared-snap",
                    "DBClusterSnapshotArn":
                        "arn:aws:rds:us-east-1:888888888888:cluster-snapshot:shared-snap",
                    "DBClusterIdentifier": "third-clu",
                    "Status": "available",
                    "SnapshotType": "manual",
                    "SnapshotAttributes": {"restore": ["000000000000"]},
                }),
            );
    }
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[
            ("DBClusterSnapshotIdentifier", "shared-snap"),
            ("IncludeShared", "true"),
        ],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(response) => panic!(
            "an ambiguous bare id resolved: {}",
            String::from_utf8_lossy(response.body.expect_bytes())
        ),
    }

    // Each ARN still resolves -- ambiguity is a property of the bare id,
    // not of the snapshots.
    for owner in ["999999999999", "888888888888"] {
        let arn = format!("arn:aws:rds:us-east-1:{owner}:cluster-snapshot:shared-snap");
        let body = body_of_action(
            &svc,
            "DescribeDBClusterSnapshots",
            &[("DBClusterSnapshotIdentifier", &arn)],
        );
        assert_eq!(
            body.matches("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>")
                .count(),
            1,
            "ARN for {owner} did not return exactly one row: {body}"
        );
        // And it is the row that account owns, not the other one.
        assert!(
            body.contains(&format!(
                "<DBClusterSnapshotArn>arn:aws:rds:us-east-1:{owner}:cluster-snapshot:shared-snap</DBClusterSnapshotArn>"
            )),
            "ARN for {owner} resolved another account's row: {body}"
        );
    }
}

/// Same-named rows from different accounts must page apart.
///
/// The cursor is the last row's key. Keyed on the identifier, two
/// accounts sharing `dup-snap` would produce the same cursor for both
/// rows, and the lookup for page two would find the first one again --
/// so a paginating client would re-read the same row forever and never
/// see the second. The account-qualified ARN is what makes the key
/// unique.
#[test]
fn shared_cluster_snapshots_with_one_name_paginate_apart() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        for owner in ["999999999999", "888888888888", "777777777777"] {
            accounts
                .get_or_create(owner)
                .extras
                .entry("cluster_snapshots".to_string())
                .or_default()
                .insert(
                    "dup-snap".to_string(),
                    json!({
                        "DBClusterSnapshotIdentifier": "dup-snap",
                        "DBClusterIdentifier": format!("clu-{owner}"),
                        "Status": "available",
                        "SnapshotType": "manual",
                        "SnapshotCreateTime": "2026-01-01T00:00:00Z",
                        "SnapshotAttributes": {"restore": ["000000000000"]},
                    }),
                );
        }
    }

    // Unqualified listing, one row per page: walk every page and collect
    // the ARNs in order.
    let mut seen: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;
    for _ in 0..6 {
        let mut params: Vec<(&str, &str)> = vec![("IncludeShared", "true"), ("MaxRecords", "1")];
        let held;
        if let Some(value) = marker.as_deref() {
            held = value.to_string();
            params.push(("Marker", &held));
        }
        let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &params);
        for arn in body.split("<DBClusterSnapshotArn>").skip(1) {
            let arn = arn
                .split("</DBClusterSnapshotArn>")
                .next()
                .unwrap_or_default();
            seen.push(arn.to_string());
        }
        marker = body
            .split("<Marker>")
            .nth(1)
            .and_then(|rest| rest.split("</Marker>").next())
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if marker.is_none() {
            break;
        }
    }

    // The paged walk must reproduce the unpaginated listing EXACTLY --
    // same rows, same order. Sorting first would only prove membership,
    // and a paginator that reordered or repeated rows would still pass.
    let unpaged = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("IncludeShared", "true")],
    );
    let expected: Vec<String> = unpaged
        .split("<DBClusterSnapshotArn>")
        .skip(1)
        .filter_map(|rest| rest.split("</DBClusterSnapshotArn>").next())
        .map(str::to_string)
        .collect();
    assert_eq!(
        seen, expected,
        "paginating a shared listing did not reproduce the unpaginated order"
    );

    // And that listing is the three distinct rows -- so the comparison
    // above can't be satisfied by two identical sequences of one row.
    let mut unique = expected.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique,
        vec![
            "arn:aws:rds:us-east-1:777777777777:cluster-snapshot:dup-snap".to_string(),
            "arn:aws:rds:us-east-1:888888888888:cluster-snapshot:dup-snap".to_string(),
            "arn:aws:rds:us-east-1:999999999999:cluster-snapshot:dup-snap".to_string(),
        ],
        "the shared listing itself lost a row"
    );
}

/// A row persisted or seeded without an ARN still matches an ARN-valued
/// filter, and the ARN it gets carries the region's own partition.
#[test]
fn cluster_snapshot_without_an_arn_is_stamped_before_filtering() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .get_or_create("000000000000")
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "bare-snap".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "bare-snap",
                    "DBClusterIdentifier": "clu-1",
                    "Status": "available",
                    "SnapshotType": "manual",
                }),
            );
    }

    // Filtering by the snapshot's ARN: the row carries none in state, so
    // stamping has to happen BEFORE the filter runs or it is filtered
    // out before it can be stamped.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("Filters.Filter.1.Name", "db-cluster-snapshot-id"),
            (
                "Filters.Filter.1.Values.Value.1",
                "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:bare-snap",
            ),
        ],
    );
    assert!(
        body.contains("<DBClusterSnapshotIdentifier>bare-snap</DBClusterSnapshotIdentifier>"),
        "an ARN filter missed a row that had no stored ARN: {body}"
    );
}

/// `Arn::new` hardcodes the `aws` partition, so a synthesized ARN in a
/// China or GovCloud region would be wrong on the wire -- and, since
/// pagination keys on it, inconsistent with a stored one.
#[test]
fn synthesized_cluster_snapshot_arn_uses_the_regions_partition() {
    for (region, partition) in [
        ("us-east-1", "aws"),
        ("cn-north-1", "aws-cn"),
        ("us-gov-west-1", "aws-us-gov"),
    ] {
        let arn = super::cluster_snapshot_arn(region, "000000000000", "snap-1");
        assert_eq!(
            arn,
            format!("arn:{partition}:rds:{region}:000000000000:cluster-snapshot:snap-1")
        );
    }
}

/// A copy's own ARN names the copier, so the cluster it records is only
/// reachable by ARN through the SOURCE snapshot's ARN.
#[test]
fn db_cluster_id_filter_matches_a_cross_account_copys_source_cluster() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .get_or_create("000000000000")
            .extras
            .entry("cluster_snapshots".to_string())
            .or_default()
            .insert(
                "mycopy".to_string(),
                json!({
                    "DBClusterSnapshotIdentifier": "mycopy",
                    "DBClusterSnapshotArn":
                        "arn:aws:rds:us-east-1:000000000000:cluster-snapshot:mycopy",
                    // The cluster belongs to the account that shared the
                    // source, not to the copier.
                    "DBClusterIdentifier": "cluB",
                    "SourceDBClusterSnapshotArn":
                        "arn:aws:rds:us-east-1:222222222222:cluster-snapshot:snapB",
                    "Status": "available",
                    "SnapshotType": "manual",
                }),
            );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("Filters.Filter.1.Name", "db-cluster-id"),
            (
                "Filters.Filter.1.Values.Value.1",
                "arn:aws:rds:us-east-1:222222222222:cluster:cluB",
            ),
        ],
    );
    assert!(
        body.contains("<DBClusterSnapshotIdentifier>mycopy</DBClusterSnapshotIdentifier>"),
        "the source cluster's ARN did not match a copy: {body}"
    );

    // The copier's own account does NOT own that cluster, so the ARN
    // rebuilt from the copy's own ARN must not match.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[
            ("Filters.Filter.1.Name", "db-cluster-id"),
            (
                "Filters.Filter.1.Values.Value.1",
                "arn:aws:rds:us-east-1:000000000000:cluster:cluB",
            ),
        ],
    );
    assert!(
        body.contains("<DBClusterSnapshotIdentifier>mycopy</DBClusterSnapshotIdentifier>"),
        "the copy's own account ARN stopped matching: {body}"
    );
}

/// BacktrackDBCluster has always recorded its backtracks; the Describe
/// answered with a hardcoded empty list, so every one was invisible.
#[test]
fn describe_db_cluster_backtracks_returns_recorded_backtracks() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }

    ok_on(
        &svc,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackTo", "2026-01-01T00:00:00Z"),
        ],
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "clu-1")],
    );
    // The NAMED member tag: the list carries xmlName
    // `DBClusterBacktrack`, and an SDK unmarshals an empty list from the
    // generic `<member>`.
    assert!(
        body.contains("<DBClusterBacktrack>"),
        "backtrack not rendered under its named member tag: {body}"
    );
    assert!(
        body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"),
        "the recorded backtrack was not returned: {body}"
    );
    // Lowercase, as AWS reports it and as the model's own Status
    // documentation spells it (applying / completed / failed / pending).
    // The filter itself matches case-insensitively, so a record
    // persisted by an older build is still selectable.
    assert!(
        body.contains("<Status>completed</Status>"),
        "status case does not match the documented filter values: {body}"
    );

    // The documented filters select it, and a non-matching value doesn't.
    let filtered = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("Filters.Filter.1.Name", "db-cluster-backtrack-status"),
            ("Filters.Filter.1.Values.Value.1", "completed"),
        ],
    );
    assert!(filtered.contains("<DBClusterBacktrack>"), "{filtered}");

    let filtered = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("Filters.Filter.1.Name", "db-cluster-backtrack-status"),
            ("Filters.Filter.1.Values.Value.1", "failed"),
        ],
    );
    assert!(
        !filtered.contains("<DBClusterBacktrack>"),
        "a non-matching status still returned the backtrack: {filtered}"
    );

    // Another cluster's backtracks are not this cluster's.
    create_cluster(&svc, "clu-2");
    let other = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "clu-2")],
    );
    assert!(
        !other.contains("<DBClusterBacktrack>"),
        "a backtrack leaked across clusters: {other}"
    );

    // A cluster that doesn't exist gets the declared fault, not an empty
    // list a caller would read as "no backtracks".
    match svc.handle_extra_action(&req(
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "ghost")],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("an unknown cluster returned a list"),
    }
}

/// The endpoint filters name fields that have to reach state and the
/// wire before they can select anything.
#[test]
fn describe_db_cluster_endpoints_honors_filters() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-custom"),
            ("DBClusterIdentifier", "clu-1"),
            // The REQUEST's type becomes the endpoint's
            // CustomEndpointType; the endpoint itself reads back CUSTOM.
            ("EndpointType", "READER"),
            ("StaticMembers.member.1", "inst-1"),
        ],
    );
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-reader"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "ANY"),
        ],
    );

    // Stored AND rendered: a caller has to be able to read back what it
    // set, and the filter has to have something to match.
    let all = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        all.contains("<CustomEndpointType>READER</CustomEndpointType>"),
        "the custom type never reached the wire: {all}"
    );
    assert!(
        all.contains("<member>inst-1</member>"),
        "static members never reached the wire: {all}"
    );

    for (name, value, expected, unexpected) in [
        (
            "db-cluster-endpoint-custom-type",
            "READER",
            "ep-custom",
            "ep-reader",
        ),
        (
            "db-cluster-endpoint-id",
            "ep-reader",
            "ep-reader",
            "ep-custom",
        ),
    ] {
        let body = body_of_action(
            &svc,
            "DescribeDBClusterEndpoints",
            &[
                ("Filters.Filter.1.Name", name),
                ("Filters.Filter.1.Values.Value.1", value),
            ],
        );
        assert!(
            body.contains(&format!(
                "<DBClusterEndpointIdentifier>{expected}</DBClusterEndpointIdentifier>"
            )),
            "{name}={value} dropped {expected}: {body}"
        );
        assert!(
            !body.contains(&format!(
                "<DBClusterEndpointIdentifier>{unexpected}</DBClusterEndpointIdentifier>"
            )),
            "{name}={value} kept {unexpected}: {body}"
        );
    }

    // Both endpoints are CUSTOM: that is what this operation creates.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-type"),
            ("Filters.Filter.1.Values.Value.1", "custom"),
        ],
    );
    assert!(body.contains("<DBClusterEndpointIdentifier>ep-custom</DBClusterEndpointIdentifier>"));
    assert!(body.contains("<DBClusterEndpointIdentifier>ep-reader</DBClusterEndpointIdentifier>"));

    // Status defaults to `available` on both the stored row and the
    // renderer, so the filter has to see that same default -- and a
    // status the endpoints are NOT in selects nothing.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-status"),
            ("Filters.Filter.1.Values.Value.1", "available"),
        ],
    );
    assert!(body.contains("<DBClusterEndpointIdentifier>ep-custom</DBClusterEndpointIdentifier>"));
    assert!(body.contains("<DBClusterEndpointIdentifier>ep-reader</DBClusterEndpointIdentifier>"));
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-status"),
            ("Filters.Filter.1.Values.Value.1", "creating"),
        ],
    );
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>"),
        "a status no endpoint is in still returned rows: {body}"
    );

    // An unrecognized name matches nothing rather than returning the
    // full list, as on the sibling Describes.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "not-a-filter"),
            ("Filters.Filter.1.Values.Value.1", "ep-custom"),
        ],
    );
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>"),
        "an unknown filter name returned rows: {body}"
    );
}

/// The documented filter values are spelled in a different case than
/// the API stores and returns, so an exact comparison selects nothing
/// for a caller copying the docs verbatim.
#[test]
fn cluster_endpoint_filters_accept_the_documented_lowercase_values() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-custom"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    // `aws rds describe-db-cluster-endpoints --filters
    //  Name=db-cluster-endpoint-type,Values=custom`, as documented.
    for (name, value) in [
        ("db-cluster-endpoint-type", "custom"),
        ("db-cluster-endpoint-custom-type", "reader"),
        ("db-cluster-endpoint-status", "AVAILABLE"),
    ] {
        let body = body_of_action(
            &svc,
            "DescribeDBClusterEndpoints",
            &[
                ("Filters.Filter.1.Name", name),
                ("Filters.Filter.1.Values.Value.1", value),
            ],
        );
        assert!(
            body.contains("<DBClusterEndpointIdentifier>ep-custom</DBClusterEndpointIdentifier>"),
            "{name}={value} selected nothing: {body}"
        );
    }

    // Case-insensitive is not match-anything.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-custom-type"),
            ("Filters.Filter.1.Values.Value.1", "writer"),
        ],
    );
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>"),
        "a non-matching type still returned rows: {body}"
    );
}

/// Identifier parameters arrive as ARNs -- the Terraform provider sends
/// them -- so comparing the raw value against a stored bare identifier
/// finds nothing, or reports an existing cluster as not found.
#[test]
fn describes_accept_arn_identifiers() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }
    ok_on(
        &svc,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackTo", "2026-01-01T00:00:00Z"),
        ],
    );
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    let cluster_arn = "arn:aws:rds:us-east-1:000000000000:cluster:clu-1";
    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", cluster_arn)],
    );
    assert!(
        body.contains("<DBClusterBacktrack>"),
        "a cluster ARN found no backtracks: {body}"
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[("DBClusterIdentifier", cluster_arn)],
    );
    assert!(
        body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "a cluster ARN narrowed the endpoints to nothing: {body}"
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[(
            "DBClusterEndpointIdentifier",
            "arn:aws:rds:us-east-1:000000000000:cluster-endpoint:ep-1",
        )],
    );
    assert!(
        body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "an endpoint ARN narrowed to nothing: {body}"
    );
}

/// A named resource that doesn't exist gets the fault the model declares
/// for it, not an empty list a poller reads as "still there".
#[test]
fn named_lookups_raise_the_declared_not_found_faults() {
    let svc = svc();
    create_cluster(&svc, "clu-1");

    match svc.handle_extra_action(&req(
        "DescribeDBShardGroups",
        &[("DBShardGroupIdentifier", "sg-gone")],
    )) {
        Err(err) => assert_eq!(err.code(), "DBShardGroupNotFound"),
        Ok(_) => panic!("an unknown shard group returned a list"),
    }

    match svc.handle_extra_action(&req(
        "DescribeDBClusterBacktracks",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackIdentifier", "bt-gone"),
        ],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterBacktrackNotFoundFault"),
        Ok(_) => panic!("an unknown backtrack returned a list"),
    }
}

/// Modify has to leave the row coherent: a stale custom type is now both
/// rendered and selectable, and a list sent empty is a request to clear
/// it rather than one to leave it alone.
#[test]
fn modify_cluster_endpoint_clears_stale_fields() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
            ("StaticMembers.member.1", "inst-1"),
        ],
    );

    // Retargeting the custom endpoint replaces the custom type rather
    // than leaving the old one selectable.
    ok_on(
        &svc,
        "ModifyDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("EndpointType", "ANY"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<CustomEndpointType>ANY</CustomEndpointType>"),
        "{body}"
    );
    let filtered = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-custom-type"),
            ("Filters.Filter.1.Values.Value.1", "reader"),
        ],
    );
    assert!(
        !filtered.contains("<DBClusterEndpointIdentifier>"),
        "the replaced custom type still matched: {filtered}"
    );

    // Static members survive a modify that doesn't mention them...
    assert!(body.contains("<member>inst-1</member>"), "{body}");

    // ...and an explicitly empty list clears them.
    ok_on(
        &svc,
        "ModifyDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("StaticMembers", ""),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        !body.contains("<member>inst-1</member>"),
        "clearing the static members was ignored: {body}"
    );
}

/// Two calls in the same clock tick must not collide: the backtrack id
/// is a map key, so a duplicate silently drops one of the records.
#[test]
fn backtrack_ids_are_unique_within_a_clock_tick() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }

    for _ in 0..25 {
        ok_on(
            &svc,
            "BacktrackDBCluster",
            &[
                ("DBClusterIdentifier", "clu-1"),
                ("BacktrackTo", "2026-01-01T00:00:00Z"),
            ],
        );
    }

    let stored = svc
        .state_handle()
        .read()
        .default_ref()
        .extras
        .get("cluster_backtracks")
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(stored, 25, "backtrack ids collided and dropped records");
}

/// MaxRecords / Marker are modeled on these operations; without paging
/// a client that asked for a page got the whole list and no Marker.
#[test]
fn describe_db_cluster_endpoints_pages() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    for id in ["ep-1", "ep-2", "ep-3"] {
        ok_on(
            &svc,
            "CreateDBClusterEndpoint",
            &[
                ("DBClusterEndpointIdentifier", id),
                ("DBClusterIdentifier", "clu-1"),
                ("EndpointType", "READER"),
            ],
        );
    }

    // Every row, built-ins included, identified by its Endpoint address:
    // a built-in carries no identifier, so comparing only custom ids
    // would leave two of the five rows unchecked.
    let addresses = |body: &str| -> Vec<String> {
        body.split("<Endpoint>")
            .skip(1)
            .filter_map(|rest| rest.split("</Endpoint>").next())
            .map(str::to_string)
            .collect()
    };
    let unpaged = addresses(&body_of_action(&svc, "DescribeDBClusterEndpoints", &[]));
    assert_eq!(
        unpaged.len(),
        5,
        "expected three custom endpoints and two built-ins"
    );

    let mut seen: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;
    // Three custom endpoints plus the cluster's two built-ins.
    for _ in 0..7 {
        let mut params: Vec<(&str, &str)> = vec![("MaxRecords", "1")];
        let held;
        if let Some(value) = marker.as_deref() {
            held = value.to_string();
            params.push(("Marker", &held));
        }
        let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &params);
        let rows = addresses(&body);
        let page = rows.len();
        seen.extend(rows);
        // MaxRecords=1 means ONE row per page. Without this the test
        // passes on a handler that ignores paging entirely and returns
        // the whole list on the first request.
        assert_eq!(page, 1, "MaxRecords=1 returned {page} rows: {body}");
        marker = body
            .split("<Marker>")
            .nth(1)
            .and_then(|rest| rest.split("</Marker>").next())
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if marker.is_none() {
            break;
        }
    }

    // Every row, once, in the unpaginated order -- built-ins included.
    assert_eq!(
        seen, unpaged,
        "the paged walk did not reproduce the unpaginated listing"
    );
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "a row was returned twice: {seen:?}"
    );
    assert!(marker.is_none(), "the last page still carried a Marker");
}

/// The modeled output is DBClusterBacktrack, not DBCluster -- and
/// without it the caller never learns the id it needs to address the
/// backtrack.
#[test]
fn backtrack_db_cluster_returns_the_backtrack_record() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }

    let body = body_of_action(
        &svc,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackTo", "2026-01-01T00:00:00Z"),
        ],
    );
    assert!(
        body.contains("<BacktrackIdentifier>bt-"),
        "the response carried no backtrack id: {body}"
    );
    assert!(body.contains("<Status>completed</Status>"), "{body}");
    assert!(
        !body.contains("<Endpoint>"),
        "the response is still a DBCluster body: {body}"
    );

    // The id it reported addresses the backtrack.
    let id = body
        .split("<BacktrackIdentifier>")
        .nth(1)
        .and_then(|rest| rest.split("</BacktrackIdentifier>").next())
        .expect("id")
        .to_string();
    let listed = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackIdentifier", &id),
        ],
    );
    assert!(listed.contains(&format!("<BacktrackIdentifier>{id}</BacktrackIdentifier>")));
}

/// An explicitly empty identifier is not a request for a resource named
/// "" -- the new hard not-found must not fire on it, and an ARN has to
/// reduce first.
#[test]
fn empty_and_arn_identifiers_do_not_trip_the_not_found_checks() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBShardGroup",
        &[
            ("DBShardGroupIdentifier", "sg-1"),
            ("DBClusterIdentifier", "clu-1"),
        ],
    );

    let body = body_of_action(
        &svc,
        "DescribeDBShardGroups",
        &[("DBShardGroupIdentifier", "")],
    );
    assert!(
        body.contains("<DBShardGroupIdentifier>sg-1</DBShardGroupIdentifier>"),
        "an empty identifier 404'd instead of listing: {body}"
    );

    let body = body_of_action(
        &svc,
        "DescribeDBShardGroups",
        &[(
            "DBShardGroupIdentifier",
            "arn:aws:rds:us-east-1:000000000000:shard-group:sg-1",
        )],
    );
    assert!(
        body.contains("<DBShardGroupIdentifier>sg-1</DBShardGroupIdentifier>"),
        "a shard group ARN 404'd: {body}"
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackIdentifier", ""),
        ],
    );
    assert!(
        !body.contains("<DBClusterBacktrack>"),
        "no backtracks exist yet: {body}"
    );
}

/// The declared fault, not an empty list -- the same rule the sibling
/// Describes apply.
#[test]
fn describe_db_cluster_endpoints_reports_an_unknown_cluster() {
    let svc = svc();
    match svc.handle_extra_action(&req(
        "DescribeDBClusterEndpoints",
        &[("DBClusterIdentifier", "ghost")],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("an unknown cluster returned a list"),
    }
}

/// `parse_member_list` reads through the form-body fallback, so the
/// presence check has to as well -- otherwise the members are parsed and
/// then discarded as "never sent".
#[test]
fn member_lists_are_read_from_an_unmerged_form_body() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    let mut request = req("ModifyDBClusterEndpoint", &[]);
    request.query_params.clear();
    request.body = bytes::Bytes::from_static(
        b"Action=ModifyDBClusterEndpoint&DBClusterEndpointIdentifier=ep-1&StaticMembers.member.1=inst-9",
    );
    svc.handle_extra_action(&request)
        .expect("ModifyDBClusterEndpoint failed");

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<member>inst-9</member>"),
        "members sent in the form body were parsed and then discarded: {body}"
    );
}

/// An identifier the operation can't resolve must NARROW to nothing, not
/// widen to everything.
///
/// `normalized_identifier` reports `None` both for "absent" and for "an
/// ARN of the wrong type", so reading it as "no narrowing" answered a
/// targeted request with the whole list -- and skipped the not-found
/// check on the way.
#[test]
fn an_unresolvable_identifier_matches_nothing() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );
    ok_on(
        &svc,
        "CreateDBShardGroup",
        &[
            ("DBShardGroupIdentifier", "sg-1"),
            ("DBClusterIdentifier", "clu-1"),
        ],
    );

    // An ARN of the WRONG resource type.
    match svc.handle_extra_action(&req(
        "DescribeDBClusterEndpoints",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:000000000000:db:mydb",
        )],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(response) => panic!(
            "a wrong-type ARN returned rows: {}",
            String::from_utf8_lossy(response.body.expect_bytes())
        ),
    }

    // ANOTHER account's ARN must not alias onto this account's resource.
    match svc.handle_extra_action(&req(
        "DescribeDBClusterEndpoints",
        &[(
            "DBClusterIdentifier",
            "arn:aws:rds:us-east-1:999999999999:cluster:clu-1",
        )],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(response) => panic!(
            "a foreign ARN resolved to this account's cluster: {}",
            String::from_utf8_lossy(response.body.expect_bytes())
        ),
    }

    // Same on the shard-group listing, which reports its own fault.
    match svc.handle_extra_action(&req(
        "DescribeDBShardGroups",
        &[(
            "DBShardGroupIdentifier",
            "arn:aws:rds:us-east-1:999999999999:shard-group:sg-1",
        )],
    )) {
        Err(err) => assert_eq!(err.code(), "DBShardGroupNotFound"),
        Ok(response) => panic!(
            "a foreign shard-group ARN returned the list: {}",
            String::from_utf8_lossy(response.body.expect_bytes())
        ),
    }

    // And the endpoint's own identifier: no rows, rather than all of
    // them.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[(
            "DBClusterEndpointIdentifier",
            "arn:aws:rds:us-east-1:999999999999:cluster-endpoint:ep-1",
        )],
    );
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "a foreign endpoint ARN resolved to this account's endpoint: {body}"
    );
}

/// A backtrack id with no cluster names nothing rather than reporting
/// every backtrack as not found.
#[test]
fn a_backtrack_id_without_a_cluster_selects_nothing() {
    let svc = svc();
    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("BacktrackIdentifier", "bt-anything")],
    );
    assert!(
        !body.contains("<DBClusterBacktrack>"),
        "a backtrack id with no cluster returned rows: {body}"
    );
}

/// The endpoint a REAL client gets back.
///
/// `CustomEndpointType` is not a member of either input shape, so no SDK,
/// CLI or Terraform caller can send it -- AWS derives it from the
/// request's `EndpointType` and reports the endpoint itself as `CUSTOM`.
/// A handler that read `CustomEndpointType` off the request stored
/// nothing, and `aws_rds_cluster_endpoint` (which writes
/// `custom_endpoint_type` as `EndpointType` and reads it back from
/// `CustomEndpointType`) would fail its post-apply consistency check.
#[test]
fn create_cluster_endpoint_maps_endpoint_type_to_the_custom_type() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let body = body_of_action(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );
    assert!(
        body.contains("<CustomEndpointType>READER</CustomEndpointType>"),
        "the create response dropped the custom type: {body}"
    );
    assert!(
        body.contains("<EndpointType>CUSTOM</EndpointType>"),
        "a custom endpoint did not read back as CUSTOM: {body}"
    );

    // The listing agrees, and the documented filter selects it.
    let listed = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-custom-type"),
            ("Filters.Filter.1.Values.Value.1", "reader"),
        ],
    );
    assert!(
        listed.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "the documented filter selected nothing: {listed}"
    );
}

/// The Describe side reduces an ARN, so the Create side has to store the
/// reduced form or the endpoint is unreachable by the bare id.
#[test]
fn create_cluster_endpoint_stores_a_reduced_cluster_identifier() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            (
                "DBClusterIdentifier",
                "arn:aws:rds:us-east-1:000000000000:cluster:clu-1",
            ),
            ("EndpointType", "READER"),
        ],
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[("DBClusterIdentifier", "clu-1")],
    );
    assert!(
        body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "an endpoint created with the cluster ARN was unreachable by id: {body}"
    );
}

/// AWS reports a cluster's built-in writer and reader endpoints, which
/// is what makes `db-cluster-endpoint-type=reader` able to match --
/// CreateDBClusterEndpoint only ever makes CUSTOM ones.
#[test]
fn describe_db_cluster_endpoints_reports_the_built_in_endpoints() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<EndpointType>WRITER</EndpointType>"),
        "the cluster's writer endpoint is missing: {body}"
    );
    assert!(
        body.contains("<EndpointType>READER</EndpointType>"),
        "the cluster's reader endpoint is missing: {body}"
    );
    assert!(
        body.contains("<EndpointType>CUSTOM</EndpointType>"),
        "{body}"
    );

    // The documented filter values now select each kind.
    for (value, expect_id) in [("reader", false), ("custom", true)] {
        let body = body_of_action(
            &svc,
            "DescribeDBClusterEndpoints",
            &[
                ("Filters.Filter.1.Name", "db-cluster-endpoint-type"),
                ("Filters.Filter.1.Values.Value.1", value),
            ],
        );
        assert!(
            !body.contains("<DBClusterEndpoints>\n\n    </DBClusterEndpoints>"),
            "db-cluster-endpoint-type={value} selected nothing: {body}"
        );
        assert_eq!(
            body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
            expect_id,
            "db-cluster-endpoint-type={value} selected the wrong rows: {body}"
        );
    }

    // Built-ins follow the cluster, so they narrow with it.
    create_cluster(&svc, "clu-2");
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[("DBClusterIdentifier", "clu-2")],
    );
    assert!(
        body.contains("<DBClusterIdentifier>clu-2</DBClusterIdentifier>"),
        "{body}"
    );
    assert!(
        !body.contains("<DBClusterIdentifier>clu-1</DBClusterIdentifier>"),
        "another cluster's endpoints leaked in: {body}"
    );
}

/// A cluster the create can't resolve is rejected, not stored.
///
/// `requested_identifier` deliberately leaves a foreign or wrong-type
/// ARN whole; stored verbatim as the endpoint's `DBClusterIdentifier`
/// that endpoint would be orphaned for the rest of its life, matching no
/// cluster lookup. `DBClusterNotFoundFault` is declared on this
/// operation, so the create fails instead.
#[test]
fn create_cluster_endpoint_rejects_a_cluster_it_cannot_resolve() {
    let svc = svc();
    create_cluster(&svc, "clu-1");

    for cluster in [
        // Another account's cluster ARN: must not attach to this
        // account's same-named cluster, and must not be stored raw.
        "arn:aws:rds:us-east-1:999999999999:cluster:clu-1",
        // An ARN of the wrong resource type.
        "arn:aws:rds:us-east-1:000000000000:db:mydb",
        // A cluster that simply doesn't exist.
        "ghost",
    ] {
        match svc.handle_extra_action(&req(
            "CreateDBClusterEndpoint",
            &[
                ("DBClusterEndpointIdentifier", "ep-x"),
                ("DBClusterIdentifier", cluster),
                ("EndpointType", "READER"),
            ],
        )) {
            Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault", "for {cluster}"),
            Ok(_) => panic!("created an endpoint on an unresolvable cluster: {cluster}"),
        }
    }

    // Nothing was stored along the way.
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>"),
        "an orphaned endpoint was stored: {body}"
    );
}

/// A record persisted by an older build carries `COMPLETED`; the docs
/// promise lowercase, and a client filtering client-side on `completed`
/// would otherwise skip it.
#[test]
fn backtrack_status_reads_back_lowercase_for_a_legacy_record() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_backtracks".to_string())
            .or_default()
            .insert(
                "bt-legacy".to_string(),
                json!({
                    "BacktrackIdentifier": "bt-legacy",
                    "DBClusterIdentifier": "clu-1",
                    "Status": "COMPLETED",
                }),
            );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "clu-1")],
    );
    assert!(
        body.contains("<Status>completed</Status>"),
        "a legacy record read back uppercase: {body}"
    );
}

/// A built-in endpoint has no identifier, resource id or ARN of its own.
///
/// Borrowing the cluster's would make a lookup by identifier return rows
/// AWS doesn't -- and would hand a cleanup script that deletes every
/// identifier it sees the cluster's own name.
#[test]
fn built_in_endpoints_carry_no_identifier_of_their_own() {
    let svc = svc();
    create_cluster(&svc, "clu-1");

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<EndpointType>WRITER</EndpointType>"),
        "{body}"
    );
    assert!(
        body.contains("<EndpointType>READER</EndpointType>"),
        "{body}"
    );
    // Not an empty element either: that reads as an endpoint named "".
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>"),
        "a built-in reported an identifier: {body}"
    );
    assert!(
        !body.contains("<DBClusterEndpointArn>"),
        "a built-in reported an ARN it cannot own: {body}"
    );
    assert!(
        !body.contains("<DBClusterEndpointResourceIdentifier>"),
        "a built-in reported the cluster's resource id: {body}"
    );

    // So a lookup by the cluster's name as an ENDPOINT id finds nothing,
    // and deleting that name reports the declared fault rather than
    // pretending it removed a built-in.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[("DBClusterEndpointIdentifier", "clu-1")],
    );
    assert!(!body.contains("<EndpointType>"), "{body}");
    match svc.handle_extra_action(&req(
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "clu-1")],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterEndpointNotFoundFault"),
        Ok(_) => panic!("deleting a built-in endpoint reported success"),
    }
}

/// `DBClusterEndpoint.Status` is its own enum; a cluster's status has
/// values outside it.
#[test]
fn built_in_endpoint_status_stays_inside_the_endpoint_enum() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    for (cluster_status, expected) in [
        ("backing-up", "modifying"),
        ("upgrading", "modifying"),
        ("stopped", "inactive"),
        ("available", "available"),
    ] {
        {
            let state = svc.state_handle();
            let mut accounts = state.write();
            if let Some(entry) = accounts
                .default_mut()
                .extras
                .get_mut("clusters")
                .and_then(|m| m.get_mut("clu-1"))
                .and_then(|v| v.as_object_mut())
            {
                entry.insert("Status".to_string(), json!(cluster_status));
            }
        }
        let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
        assert!(
            body.contains(&format!("<Status>{expected}</Status>")),
            "cluster status {cluster_status} left the endpoint enum: {body}"
        );
    }
}

/// An endpoint persisted before `CustomEndpointType` was derived carries
/// the request's type and no custom type. Read verbatim it is
/// indistinguishable from a built-in.
#[test]
fn a_legacy_cluster_endpoint_still_reads_back_as_custom() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_endpoints".to_string())
            .or_default()
            .insert(
                "ep-legacy".to_string(),
                json!({
                    "DBClusterEndpointIdentifier": "ep-legacy",
                    "DBClusterIdentifier": "clu-1",
                    "Endpoint": "ep-legacy.cluster-custom.us-east-1.rds.amazonaws.com",
                    "EndpointType": "READER",
                    "Status": "available",
                }),
            );
    }

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<EndpointType>CUSTOM</EndpointType>"),
        "a created endpoint read back as a built-in: {body}"
    );
    assert!(
        body.contains("<CustomEndpointType>READER</CustomEndpointType>"),
        "the legacy row lost its type: {body}"
    );

    // And it is selected by the filter for what it is.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterEndpoints",
        &[
            ("Filters.Filter.1.Name", "db-cluster-endpoint-type"),
            ("Filters.Filter.1.Values.Value.1", "custom"),
        ],
    );
    assert!(
        body.contains("<DBClusterEndpointIdentifier>ep-legacy</DBClusterEndpointIdentifier>"),
        "db-cluster-endpoint-type=custom missed a created endpoint: {body}"
    );
}

/// The declared fault, not a silent overwrite: a retried create was
/// replacing the existing endpoint's members and ARN and reporting 200.
#[test]
fn create_cluster_endpoint_rejects_a_duplicate_identifier() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let params = [
        ("DBClusterEndpointIdentifier", "ep-1"),
        ("DBClusterIdentifier", "clu-1"),
        ("EndpointType", "READER"),
    ];
    ok_on(&svc, "CreateDBClusterEndpoint", &params);

    match svc.handle_extra_action(&req("CreateDBClusterEndpoint", &params)) {
        Err(err) => assert_eq!(err.code(), "DBClusterEndpointAlreadyExistsFault"),
        Ok(_) => panic!("a duplicate create overwrote the endpoint"),
    }
}

/// Every arm of the endpoint lifecycle addresses a row the same way, so
/// an ARN a caller read back from one call works on all of them.
#[test]
fn cluster_endpoint_lifecycle_accepts_the_arn_form_throughout() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let arn = "arn:aws:rds:us-east-1:000000000000:cluster-endpoint:ep-1";

    // Created BY ARN: the row must not be keyed by the ARN, or nothing
    // below can address it.
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", arn),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "the endpoint was stored under its ARN: {body}"
    );

    ok_on(
        &svc,
        "ModifyDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", arn),
            ("EndpointType", "ANY"),
        ],
    );

    // Deleting by ARN reports the endpoint as deleting, not as the
    // `available` it was a moment ago.
    let body = body_of_action(
        &svc,
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", arn)],
    );
    assert!(
        body.contains("<Status>deleting</Status>"),
        "the delete response reported a live endpoint: {body}"
    );
    assert!(
        body.contains("<CustomEndpointType>ANY</CustomEndpointType>"),
        "the delete response dropped the endpoint's fields: {body}"
    );
}

/// An empty identifier reaches the operation's DECLARED fault rather
/// than InvalidParameterValue, which the RDS model does not define.
#[test]
fn an_empty_endpoint_identifier_uses_the_declared_fault() {
    let svc = svc();
    for action in ["ModifyDBClusterEndpoint", "DeleteDBClusterEndpoint"] {
        for params in [
            vec![("DBClusterEndpointIdentifier", "")],
            vec![("EndpointType", "READER")],
        ] {
            match svc.handle_extra_action(&req(action, &params)) {
                Err(err) => assert_eq!(
                    err.code(),
                    "DBClusterEndpointNotFoundFault",
                    "{action} with {params:?}"
                ),
                Ok(_) => panic!("{action} succeeded with no identifier"),
            }
        }
    }
}

/// The create side of backtracks accepts the ARN its Describe accepts.
#[test]
fn backtrack_db_cluster_accepts_an_arn_identifier() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }

    ok_on(
        &svc,
        "BacktrackDBCluster",
        &[
            (
                "DBClusterIdentifier",
                "arn:aws:rds:us-east-1:000000000000:cluster:clu-1",
            ),
            ("BacktrackTo", "2026-01-01T00:00:00Z"),
        ],
    );

    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "clu-1")],
    );
    assert!(
        body.contains("<DBClusterBacktrack>"),
        "a backtrack created by ARN was filed under the ARN: {body}"
    );
}

/// Create still requires a name.
///
/// A row stored under "" renders with no identifier at all -- the same
/// shape that marks a cluster's built-in, undeletable endpoints.
#[test]
fn create_cluster_endpoint_requires_an_identifier() {
    let svc = svc();
    create_cluster(&svc, "clu-1");

    for params in [
        vec![
            ("DBClusterEndpointIdentifier", ""),
            ("DBClusterIdentifier", "clu-1"),
        ],
        vec![("DBClusterIdentifier", "clu-1")],
    ] {
        assert!(
            svc.handle_extra_action(&req("CreateDBClusterEndpoint", &params))
                .is_err(),
            "created a nameless endpoint with {params:?}"
        );
    }

    // Only the cluster's two built-ins are listed; no nameless row
    // joined them.
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert_eq!(
        body.matches("<DBClusterEndpointList>").count(),
        2,
        "a nameless endpoint was stored: {body}"
    );
}

/// The shard-group write paths reduce an ARN, like the read path.
#[test]
fn shard_group_lifecycle_accepts_the_arn_form() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let arn = "arn:aws:rds:us-east-1:000000000000:shard-group:sg-1";

    ok_on(
        &svc,
        "CreateDBShardGroup",
        &[
            ("DBShardGroupIdentifier", arn),
            ("DBClusterIdentifier", "clu-1"),
        ],
    );

    // Created by ARN, addressable by the bare id the Describe reduces to.
    let body = body_of_action(
        &svc,
        "DescribeDBShardGroups",
        &[("DBShardGroupIdentifier", "sg-1")],
    );
    assert!(
        body.contains("<DBShardGroupIdentifier>sg-1</DBShardGroupIdentifier>"),
        "a shard group created by ARN was filed under the ARN: {body}"
    );

    // And by the ARN itself, through the same reduction.
    ok_on(
        &svc,
        "ModifyDBShardGroup",
        &[("DBShardGroupIdentifier", arn)],
    );
    ok_on(
        &svc,
        "DeleteDBShardGroup",
        &[("DBShardGroupIdentifier", arn)],
    );
}

/// Built-in endpoints must not crowd every custom endpoint off page one.
///
/// Keyed with a global prefix they all sorted ahead of every custom
/// endpoint, so an account with enough clusters filled the first page
/// with built-ins -- and every caller of this operation predates its
/// pagination, so none of them read `Marker`.
#[test]
fn built_in_endpoints_do_not_monopolize_the_first_page() {
    let svc = svc();
    for n in 0..4 {
        create_cluster(&svc, &format!("clu-{n}"));
    }
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "clu-0-ep"),
            ("DBClusterIdentifier", "clu-0"),
            ("EndpointType", "READER"),
        ],
    );

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[("MaxRecords", "4")]);
    assert!(
        body.contains("<DBClusterEndpointIdentifier>clu-0-ep</DBClusterEndpointIdentifier>"),
        "the custom endpoint was pushed off the first page by built-ins: {body}"
    );
}

/// Deleting a cluster deletes what belongs to it.
///
/// An orphaned endpoint keeps appearing in listings for a cluster that
/// no longer exists, and -- now that create raises
/// DBClusterEndpointAlreadyExistsFault rather than overwriting -- makes
/// an ordinary destroy/apply cycle fail forever. Orphaned backtracks are
/// worse: the Describe matches on the cluster identifier alone, so a NEW
/// cluster of that name would report backtracks it never performed.
#[test]
fn deleting_a_cluster_deletes_its_endpoints_and_backtracks() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        if let Some(entry) = accounts
            .default_mut()
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut("clu-1"))
            .and_then(|v| v.as_object_mut())
        {
            entry.insert("Engine".to_string(), json!("aurora-mysql"));
            entry.insert("Status".to_string(), json!("available"));
        }
    }
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );
    ok_on(
        &svc,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("BacktrackTo", "2026-01-01T00:00:00Z"),
        ],
    );

    ok_on(&svc, "DeleteDBCluster", &[("DBClusterIdentifier", "clu-1")]);

    // Nothing of the deleted cluster is left listed.
    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "an endpoint outlived its cluster: {body}"
    );

    // Recreating both under the same names works -- the destroy/apply
    // cycle that the AlreadyExists guard would otherwise block forever.
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    // And the new cluster reports none of the old one's backtracks.
    let body = body_of_action(
        &svc,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "clu-1")],
    );
    assert!(
        !body.contains("<DBClusterBacktrack>"),
        "a recreated cluster inherited backtracks it never performed: {body}"
    );
}

/// An empty shard-group identifier is absent, not a row named "".
#[test]
fn an_empty_shard_group_identifier_is_absent() {
    let svc = svc();
    create_cluster(&svc, "clu-1");

    for action in [
        "CreateDBShardGroup",
        "ModifyDBShardGroup",
        "RebootDBShardGroup",
        "DeleteDBShardGroup",
    ] {
        assert!(
            svc.handle_extra_action(&req(
                action,
                &[
                    ("DBShardGroupIdentifier", ""),
                    ("DBClusterIdentifier", "clu-1"),
                ],
            ))
            .is_err(),
            "{action} accepted an empty identifier"
        );
    }

    // And nothing was stored under the empty key.
    let body = body_of_action(&svc, "DescribeDBShardGroups", &[]);
    assert!(
        !body.contains("<DBShardGroup>"),
        "an empty identifier stored a shard group: {body}"
    );
}

/// The pagination marker is base64, so the NUL in a built-in endpoint's
/// sort key never reaches the XML -- a literal U+0000 (or an `&#x0;`
/// character reference) is not legal in XML 1.0 and parsers reject it.
#[test]
fn the_pagination_marker_is_xml_safe_across_a_built_in_row() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );

    // Rows are identified by their Endpoint address: a built-in carries
    // no identifier, and the address is unique per row.
    let addresses = |body: &str| -> Vec<String> {
        body.split("<Endpoint>")
            .skip(1)
            .filter_map(|rest| rest.split("</Endpoint>").next())
            .map(str::to_string)
            .collect()
    };
    let unpaged = addresses(&body_of_action(&svc, "DescribeDBClusterEndpoints", &[]));
    assert_eq!(unpaged.len(), 3, "expected two built-ins and one custom");

    // MaxRecords=1 puts every page boundary on a row in turn, including
    // both built-ins.
    let mut marker: Option<String> = None;
    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0;
    loop {
        let mut params: Vec<(&str, &str)> = vec![("MaxRecords", "1")];
        let held;
        if let Some(value) = marker.as_deref() {
            held = value.to_string();
            params.push(("Marker", &held));
        }
        let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &params);
        assert!(
            !body.contains('\u{0}') && !body.contains("&#x0;") && !body.contains("&#0;"),
            "a NUL reached the response: {body:?}"
        );
        let page = addresses(&body);
        assert_eq!(page.len(), 1, "MaxRecords=1 returned {} rows", page.len());
        seen.extend(page);
        pages += 1;
        assert!(pages < 10, "pagination did not terminate");
        marker = body
            .split("<Marker>")
            .nth(1)
            .and_then(|rest| rest.split("</Marker>").next())
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let Some(value) = marker.as_deref() else {
            break;
        };
        // Base64 alphabet only.
        assert!(
            value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=')),
            "the marker is not base64: {value:?}"
        );
    }

    // Every row, once, in the unpaginated order -- a paginator that
    // skipped a built-in, repeated one, or reordered them fails here.
    assert_eq!(
        seen, unpaged,
        "the paged walk did not reproduce the unpaginated listing"
    );
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "a row was returned twice: {seen:?}"
    );
    assert_eq!(pages, 3, "expected three rows across three pages");
}

/// The cascade reaches a row written before identifiers were normalized.
///
/// Such a row stores the cluster's ARN, so an exact comparison leaves it
/// behind -- the orphan the cascade exists to prevent.
#[test]
fn deleting_a_cluster_removes_a_legacy_arn_keyed_endpoint() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .default_mut()
            .extras
            .entry("cluster_endpoints".to_string())
            .or_default()
            .insert(
                "ep-legacy".to_string(),
                json!({
                    "DBClusterEndpointIdentifier": "ep-legacy",
                    // As an older build stored it.
                    "DBClusterIdentifier":
                        "arn:aws:rds:us-east-1:000000000000:cluster:clu-1",
                    "Endpoint": "ep-legacy.cluster-custom.us-east-1.rds.amazonaws.com",
                    "EndpointType": "CUSTOM",
                    "CustomEndpointType": "READER",
                    "Status": "available",
                }),
            );
    }

    ok_on(&svc, "DeleteDBCluster", &[("DBClusterIdentifier", "clu-1")]);

    let body = body_of_action(&svc, "DescribeDBClusterEndpoints", &[]);
    assert!(
        !body.contains("<DBClusterEndpointIdentifier>ep-legacy</DBClusterEndpointIdentifier>"),
        "an ARN-keyed endpoint outlived its cluster: {body}"
    );

    // So recreating the pair under the same names still works.
    create_cluster(&svc, "clu-1");
    ok_on(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ep-legacy"),
            ("DBClusterIdentifier", "clu-1"),
            ("EndpointType", "READER"),
        ],
    );
}

/// Role association is recorded, reported, and refuses the duplicates
/// and absences the model declares faults for.
///
/// All four role operations were `xml_empty_action`: they accepted any
/// request, stored nothing, and answered 200 -- so a caller could attach
/// a role to a cluster that does not exist, and DescribeDBClusters never
/// reported the roles it had been told about.
#[test]
fn cluster_roles_are_recorded_and_reported() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let role = "arn:aws:iam::000000000000:role/s3-import";

    // A cluster that doesn't exist gets the declared fault, not a 200.
    match svc.handle_extra_action(&req(
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "ghost"), ("RoleArn", role)],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterNotFoundFault"),
        Ok(_) => panic!("attached a role to a cluster that does not exist"),
    }

    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    );

    // Reported by the listing, which never saw these before.
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert!(
        body.contains(&format!("<RoleArn>{role}</RoleArn>")),
        "the association was not reported: {body}"
    );
    assert!(
        body.contains("<FeatureName>s3Import</FeatureName>"),
        "{body}"
    );
    assert!(body.contains("<Status>ACTIVE</Status>"), "{body}");

    // The SAME pair twice is the declared conflict.
    match svc.handle_extra_action(&req(
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterRoleAlreadyExists"),
        Ok(_) => panic!("attached the same role and feature twice"),
    }

    // The same role for a DIFFERENT feature is a different association,
    // which is how one role gets attached for both import and export.
    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Export"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert_eq!(
        body.matches(&format!("<RoleArn>{role}</RoleArn>")).count(),
        2,
        "the second feature was rejected as a duplicate: {body}"
    );

    // Removing a role that was never attached is the declared absence.
    match svc.handle_extra_action(&req(
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", "arn:aws:iam::000000000000:role/other"),
        ],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterRoleNotFound"),
        Ok(_) => panic!("removed a role that was never attached"),
    }

    // Removing one feature leaves the other: matching on the ARN alone
    // deleted whichever entry came first.
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Export"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert!(
        body.contains("<FeatureName>s3Import</FeatureName>"),
        "{body}"
    );
    assert!(
        !body.contains("<FeatureName>s3Export</FeatureName>"),
        "the wrong association was removed: {body}"
    );

    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert!(
        !body.contains("<AssociatedRoles>"),
        "the association outlived its removal: {body}"
    );
    // And the key is GONE, not left as an empty array: snapshots clone
    // the cluster entry forward, so an empty key would ride along in
    // every later snapshot.
    let stored = extras_value(&svc, "clusters", "clu-1");
    assert!(
        stored.get("AssociatedRoles").is_none(),
        "removing the last role left an empty key behind: {stored}"
    );

    // AWS caps roles per cluster at five, and the Add op declares the
    // quota fault.
    for n in 0..5 {
        ok_on(
            &svc,
            "AddRoleToDBCluster",
            &[
                ("DBClusterIdentifier", "clu-1"),
                ("RoleArn", &format!("arn:aws:iam::000000000000:role/r{n}")),
            ],
        );
    }
    match svc.handle_extra_action(&req(
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", "arn:aws:iam::000000000000:role/one-too-many"),
        ],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterRoleQuotaExceeded"),
        Ok(_) => panic!("attached a sixth role"),
    }
}

/// A failed role request leaves the cluster untouched.
///
/// The roles array was materialized before the add/remove ran, so a
/// remove against a cluster with no roles wrote `"AssociatedRoles": []`
/// into the stored entry and THEN returned the fault --
/// CreateDBClusterSnapshot cloned that key forward into every later
/// snapshot.
#[test]
fn a_failed_role_request_does_not_write_to_the_cluster() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let before = extras_value(&svc, "clusters", "clu-1");
    assert!(before.get("AssociatedRoles").is_none());

    match svc.handle_extra_action(&req(
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", "arn:aws:iam::000000000000:role/never-attached"),
        ],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterRoleNotFound"),
        Ok(_) => panic!("removed a role that was never attached"),
    }

    let after = extras_value(&svc, "clusters", "clu-1");
    assert!(
        after.get("AssociatedRoles").is_none(),
        "a failed remove wrote the key into the cluster: {after}"
    );

    // An empty RoleArn is not stored as an association either. Requests
    // through `handle` never reach this -- prevalidate rejects the
    // missing required parameter first -- but an in-process caller can.
    // The Add path answers the way prevalidate answers the same request:
    // DBClusterRoleNotFoundFault is declared on Remove, not on Add, so
    // raising it here would put a shape on the wire that this operation
    // never returns.
    match svc.handle_extra_action(&req(
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", "")],
    )) {
        Err(err) => assert_eq!(err.code(), "InvalidParameterValue"),
        Ok(_) => panic!("stored an association with no role"),
    }
    let after = extras_value(&svc, "clusters", "clu-1");
    assert!(
        after.get("AssociatedRoles").is_none(),
        "an empty role was stored: {after}"
    );
}

/// FeatureName is optional on the cluster operations, so a remove that
/// names only the role works when that is unambiguous -- and refuses to
/// guess when it isn't.
#[test]
fn removing_a_cluster_role_without_a_feature_resolves_only_when_unambiguous() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let role = "arn:aws:iam::000000000000:role/s3";

    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    );

    // One association carries the ARN, so naming just the role is
    // unambiguous.
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", role)],
    );
    let stored = extras_value(&svc, "clusters", "clu-1");
    assert!(stored.get("AssociatedRoles").is_none(), "{stored}");

    // An explicitly EMPTY FeatureName is absent, not a feature named "":
    // treated as present it blocks this path and stores an empty
    // <FeatureName/> into the association.
    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    );
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", ""),
        ],
    );
    let stored = extras_value(&svc, "clusters", "clu-1");
    assert!(
        stored.get("AssociatedRoles").is_none(),
        "an empty FeatureName blocked the remove: {stored}"
    );

    // Two features on one role: naming only the role is ambiguous, and
    // guessing is how the ARN-only matching removed the wrong one.
    for feature in ["s3Import", "s3Export"] {
        ok_on(
            &svc,
            "AddRoleToDBCluster",
            &[
                ("DBClusterIdentifier", "clu-1"),
                ("RoleArn", role),
                ("FeatureName", feature),
            ],
        );
    }
    match svc.handle_extra_action(&req(
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", role)],
    )) {
        Err(err) => assert_eq!(err.code(), "DBClusterRoleNotFound"),
        Ok(_) => panic!("an ambiguous remove guessed an association"),
    }
    // Both survive it.
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert_eq!(
        body.matches(&format!("<RoleArn>{role}</RoleArn>")).count(),
        2,
        "an ambiguous remove deleted an association: {body}"
    );

    // Naming the feature still resolves exactly one.
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Export"),
        ],
    );
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert!(
        body.contains("<FeatureName>s3Import</FeatureName>"),
        "{body}"
    );
    assert!(
        !body.contains("<FeatureName>s3Export</FeatureName>"),
        "{body}"
    );
}

/// A feature-less association is an exact match, not a guess.
///
/// When a role is attached BOTH without a feature and with one, a remove
/// that names no feature identifies the feature-less association
/// exactly: (role, no feature) is a key one association carries. The
/// ambiguity fallback is for when no exact match exists, so it must not
/// fire here and must not touch the feature-bound association.
#[test]
fn removing_a_cluster_role_prefers_the_exact_feature_less_association() {
    let svc = svc();
    create_cluster(&svc, "clu-1");
    let role = "arn:aws:iam::000000000000:role/s3";

    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", role)],
    );
    ok_on(
        &svc,
        "AddRoleToDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("RoleArn", role),
            ("FeatureName", "s3Import"),
        ],
    );

    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", role)],
    );

    // The feature-bound one survives, and it is the only one left.
    let body = body_of_action(&svc, "DescribeDBClusters", &[]);
    assert_eq!(
        body.matches(&format!("<RoleArn>{role}</RoleArn>")).count(),
        1,
        "the wrong association was removed: {body}"
    );
    assert!(
        body.contains("<FeatureName>s3Import</FeatureName>"),
        "the feature-bound association was removed: {body}"
    );

    // And now that it is the only one carrying the ARN, naming just the
    // role resolves it through the unambiguous fallback.
    ok_on(
        &svc,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("RoleArn", role)],
    );
    let stored = extras_value(&svc, "clusters", "clu-1");
    assert!(stored.get("AssociatedRoles").is_none(), "{stored}");
}

#[test]
fn describe_db_shard_groups_honors_the_id_filter() {
    let svc = svc();
    for id in ["sg-1", "sg-2"] {
        ok_on(
            &svc,
            "CreateDBShardGroup",
            &[
                ("DBShardGroupIdentifier", id),
                ("DBClusterIdentifier", "clu-1"),
            ],
        );
    }

    let body = body_of_action(
        &svc,
        "DescribeDBShardGroups",
        &[
            ("Filters.Filter.1.Name", "db-shard-group-id"),
            ("Filters.Filter.1.Values.Value.1", "sg-2"),
        ],
    );
    assert!(body.contains("<DBShardGroupIdentifier>sg-2</DBShardGroupIdentifier>"));
    assert!(
        !body.contains("<DBShardGroupIdentifier>sg-1</DBShardGroupIdentifier>"),
        "the filter kept an unmatched shard group: {body}"
    );
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

/// A copy records where it came from, and `db-instance-id` uses that to
/// reach the source instance by ARN.
#[test]
fn copy_db_snapshot_records_the_source_and_filters_on_it() {
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        let other = accounts.get_or_create("999999999999");
        let mut shared = local_snapshot("snap-1", "their-db", "999999999999");
        shared
            .snapshot_attributes
            .insert("restore".to_string(), vec!["000000000000".to_string()]);
        other.snapshots.insert("snap-1".to_string(), shared);
    }

    let body = body_of_action(
        &svc,
        "CopyDBSnapshot",
        &[
            (
                "SourceDBSnapshotIdentifier",
                "arn:aws:rds:us-east-1:999999999999:snapshot:snap-1",
            ),
            ("TargetDBSnapshotIdentifier", "mycopy"),
        ],
    );
    // AWS reports the source as an ARN on a copy.
    assert!(
        body.contains(
            "<SourceDBSnapshotIdentifier>arn:aws:rds:us-east-1:999999999999:snapshot:snap-1</SourceDBSnapshotIdentifier>"
        ),
        "the copy didn't report its source: {body}"
    );

    // And it is recorded in state, which is what `db-instance-id`
    // matches the source instance's ARN against.
    let state = svc.state_handle();
    let accounts = state.read();
    let copy = accounts
        .get("000000000000")
        .and_then(|s| s.snapshots.get("mycopy"))
        .expect("copy not stored");
    assert_eq!(
        copy.source_db_snapshot_arn.as_deref(),
        Some("arn:aws:rds:us-east-1:999999999999:snapshot:snap-1")
    );
    // The instance still belongs to the original owner, which is why the
    // copy's own ARN can't be used to rebuild that instance's ARN.
    assert_eq!(copy.db_instance_identifier, "their-db");
    assert_eq!(
        copy.db_snapshot_arn,
        "arn:aws:rds:us-east-1:000000000000:snapshot:mycopy"
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

    // Walk every page and compare the COMPLETE sequence: non-overlapping
    // pages alone would still pass if the paginator skipped a row.
    fn ids_of(body: &str) -> Vec<String> {
        body.split("<DBClusterSnapshotIdentifier>")
            .skip(1)
            .filter_map(|rest| rest.split("</DBClusterSnapshotIdentifier>").next())
            .map(str::to_string)
            .collect()
    }

    let mut seen = ids_of(&first);
    let mut next = Some(marker.clone());
    while let Some(m) = next {
        let page = body_of_action(
            &svc,
            "DescribeDBClusterSnapshots",
            &[("MaxRecords", "2"), ("Marker", &m)],
        );
        seen.extend(ids_of(&page));
        next = page
            .split("<Marker>")
            .nth(1)
            .and_then(|rest| rest.split("</Marker>").next())
            .map(str::to_string);
    }

    let mut expected = ids_of(&body_of_action(&svc, "DescribeDBClusterSnapshots", &[]));
    assert_eq!(expected.len(), 5, "the unpaginated listing lost rows");
    assert_eq!(
        seen, expected,
        "paging did not reproduce the full listing in order"
    );
    expected.dedup();
    assert_eq!(expected.len(), 5, "the listing repeated a row");

    // Identical requests return identical order.
    let again = body_of_action(&svc, "DescribeDBClusterSnapshots", &[("MaxRecords", "2")]);
    assert_eq!(first, again, "listing order is not stable");
}

#[test]
fn describe_db_cluster_snapshots_treats_an_empty_marker_as_page_one() {
    // `Marker=` decodes to a position no row matches, which would return
    // an empty page rather than the first one.
    let svc = svc();
    seed_cluster_snapshot(&svc, "snap-1", "clu-1", "manual");

    let body = body_of_action(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("Marker", ""), ("MaxRecords", "")],
    );
    assert!(
        body.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"),
        "an empty marker returned an empty page: {body}"
    );
}

#[test]
fn copy_db_parameter_group_refuses_a_foreign_or_wrong_type_arn() {
    // An unconditional rsplit would trim either ARN to `mypg` and copy
    // THIS account's parameter group of that name, reporting success.
    let svc = svc();

    for source in [
        "arn:aws:rds:us-east-1:999999999999:pg:mypg",
        "arn:aws:rds:us-east-1:000000000000:cluster-pg:mypg",
        // An empty account field names nothing resolvable; treating it
        // as a bare id would copy the local group.
        "arn:aws:rds:us-east-1::pg:mypg",
    ] {
        let result = svc.handle_extra_action(&req(
            "CopyDBParameterGroup",
            &[
                ("SourceDBParameterGroupIdentifier", source),
                ("TargetDBParameterGroupIdentifier", "copy-pg"),
            ],
        ));
        match result {
            Err(err) => assert_eq!(err.code(), "DBParameterGroupNotFound", "source {source}"),
            Ok(_) => panic!("copied from {source}"),
        }
    }
}

#[test]
fn a_shared_cluster_snapshot_resolves_by_bare_id_when_the_request_widens() {
    // The existence check and the listing have to agree: with
    // SnapshotType=shared (or IncludeShared) a bare id reaches foreign
    // rows, so 404-ing it here would reject a row the listing returns.
    let svc = svc();
    {
        let state = svc.state_handle();
        let mut accounts = state.write();
        accounts
            .get_or_create("999999999999")
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

    for params in [
        vec![
            ("DBClusterSnapshotIdentifier", "shared-snap"),
            ("SnapshotType", "shared"),
        ],
        vec![
            ("DBClusterSnapshotIdentifier", "shared-snap"),
            ("IncludeShared", "true"),
        ],
    ] {
        let body = body_of_action(&svc, "DescribeDBClusterSnapshots", &params);
        assert!(
            body.contains("<DBClusterSnapshotIdentifier>shared-snap</DBClusterSnapshotIdentifier>"),
            "existence check rejected a row the listing returns, for {params:?}: {body}"
        );
    }

    // Without either, a bare id still names nothing this account owns.
    let result = svc.handle_extra_action(&req(
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "shared-snap")],
    ));
    match result {
        Err(err) => assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault"),
        Ok(_) => panic!("a bare id reached another account without widening"),
    }
}

#[test]
fn cluster_snapshot_attribute_ops_use_a_declared_fault_for_a_missing_param() {
    // `missing()` raises InvalidParameterValue, which is not a shape
    // anywhere in the RDS model -- an SDK client would see an unmodeled
    // failure.
    let svc = svc();
    for action in [
        "DescribeDBClusterSnapshotAttributes",
        "ModifyDBClusterSnapshotAttribute",
        "DeleteDBClusterSnapshot",
    ] {
        let result = svc.handle_extra_action(&req(action, &[]));
        match result {
            Err(err) => assert_eq!(
                err.code(),
                "DBClusterSnapshotNotFoundFault",
                "{action} raised {}",
                err.code()
            ),
            Ok(_) => panic!("{action} accepted a missing identifier"),
        }
    }
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
