//! Regression coverage for the CloudFormation `UpdateStack` data-loss bug on
//! stateful, container-backed resource types (#2380 follow-up).
//!
//! `update_resource` had dedicated in-place arms only for some resource types;
//! stateful ones without an arm (ElastiCache clusters, Aurora `DBCluster`,
//! Redshift/OpenSearch/DocDB/Neptune, EC2 instances) fell to
//! `reprovision_resource`, which DELETES and RE-CREATES the backing resource.
//! So a benign property or tag change on such a resource (e.g. `cdk deploy`
//! bumping an engine version or adding a tag) tore down the Redis/Aurora/...
//! backend and wiped its data while the stack still reported `UPDATE_COMPLETE`.
//! (#2380 fixed the UNCHANGED case; this covers the CHANGED case.)
//!
//! After the fix each of those types has an in-place `update_*` arm that
//! applies the change through the owning service's real modify path and keeps
//! the backing resource. These tests assert, without needing a container
//! runtime, that a CHANGED-property stack update updates the resource in place
//! rather than replacing it:
//!   * a distinctive out-of-band field set on the resource survives the update
//!     (it would be reset by a delete+recreate), and
//!   * the resource's stable server-minted identity (its resource id) is
//!     unchanged (a recreate would mint a new one).

mod helpers;

use helpers::TestServer;

async fn wait_for_status(server: &TestServer, stack: &str, want: &str) {
    let cfn = server.cloudformation_client().await;
    let got = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let cfn = cfn.clone();
        async move {
            let out = cfn.describe_stacks().stack_name(stack).send().await.ok()?;
            let status = out.stacks().first()?.stack_status()?;
            (status.as_str() == want).then_some(())
        }
    })
    .await;
    assert!(got.is_some(), "stack {stack} never reached {want}");
}

// --- AWS::ElastiCache::CacheCluster ---------------------------------------

const EC_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cache": {
      "Type": "AWS::ElastiCache::CacheCluster",
      "Properties": {
        "ClusterName": "cfn-ec-preserve",
        "Engine": "redis",
        "EngineVersion": "6.2",
        "CacheNodeType": "cache.t3.micro",
        "NumCacheNodes": 1
      }
    }
  }
}"#;

// Identical except EngineVersion bumped 6.2 -> 7.0 (a mutable, in-place change).
const EC_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cache": {
      "Type": "AWS::ElastiCache::CacheCluster",
      "Properties": {
        "ClusterName": "cfn-ec-preserve",
        "Engine": "redis",
        "EngineVersion": "7.0",
        "CacheNodeType": "cache.t3.micro",
        "NumCacheNodes": 1
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_elasticache_cache_cluster_is_in_place() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ec = server.elasticache_client().await;

    cfn.create_stack()
        .stack_name("ec-preserve")
        .template_body(EC_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "ec-preserve", "CREATE_COMPLETE").await;

    // Out-of-band config the template never carries: a distinctive maintenance
    // window. A delete+recreate would reset it to the (absent) template value.
    ec.modify_cache_cluster()
        .cache_cluster_id("cfn-ec-preserve")
        .preferred_maintenance_window("sun:23:00-mon:01:30")
        .send()
        .await
        .expect("modify_cache_cluster");

    // Sanity: v1 engine version and the out-of-band window are present.
    let before = ec
        .describe_cache_clusters()
        .cache_cluster_id("cfn-ec-preserve")
        .send()
        .await
        .expect("describe before");
    let c0 = before.cache_clusters().first().expect("cluster exists");
    assert_eq!(c0.engine_version(), Some("6.2"));
    assert_eq!(
        c0.preferred_maintenance_window(),
        Some("sun:23:00-mon:01:30")
    );

    // Change a mutable property (EngineVersion). Pre-fix this delete+recreated
    // the cluster, wiping its data and the out-of-band window.
    cfn.update_stack()
        .stack_name("ec-preserve")
        .template_body(EC_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "ec-preserve", "UPDATE_COMPLETE").await;

    let after = ec
        .describe_cache_clusters()
        .cache_cluster_id("cfn-ec-preserve")
        .send()
        .await
        .expect("describe after");
    let c1 = after
        .cache_clusters()
        .first()
        .expect("cluster still exists after update");
    // The template change was applied...
    assert_eq!(
        c1.engine_version(),
        Some("7.0"),
        "the mutable EngineVersion change should have been applied in place"
    );
    // ...and the out-of-band window survived -> in-place update, not replace.
    assert_eq!(
        c1.preferred_maintenance_window(),
        Some("sun:23:00-mon:01:30"),
        "out-of-band config must survive an in-place update; a delete+recreate would have reset it"
    );
}

// --- AWS::RDS::DBCluster --------------------------------------------------

const RDS_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cluster": {
      "Type": "AWS::RDS::DBCluster",
      "Properties": {
        "DBClusterIdentifier": "cfn-dbcluster-preserve",
        "Engine": "aurora-postgresql",
        "MasterUsername": "admin",
        "MasterUserPassword": "correcthorsebattery",
        "BackupRetentionPeriod": 1
      }
    }
  }
}"#;

// Identical except BackupRetentionPeriod 1 -> 7 (a mutable, in-place change).
const RDS_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cluster": {
      "Type": "AWS::RDS::DBCluster",
      "Properties": {
        "DBClusterIdentifier": "cfn-dbcluster-preserve",
        "Engine": "aurora-postgresql",
        "MasterUsername": "admin",
        "MasterUserPassword": "correcthorsebattery",
        "BackupRetentionPeriod": 7
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_rds_db_cluster_is_in_place() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let rds = server.rds_client().await;

    cfn.create_stack()
        .stack_name("rds-preserve")
        .template_body(RDS_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "rds-preserve", "CREATE_COMPLETE").await;

    // Record the server-minted resource id -- the cluster's stable identity. A
    // delete+recreate would mint a brand-new one.
    let before = rds
        .describe_db_clusters()
        .db_cluster_identifier("cfn-dbcluster-preserve")
        .send()
        .await
        .expect("describe before");
    let c0 = before.db_clusters().first().expect("cluster exists");
    let resource_id_before = c0
        .db_cluster_resource_id()
        .expect("resource id present")
        .to_string();
    assert_eq!(c0.backup_retention_period(), Some(1));

    // Change a mutable property. Pre-fix this delete+recreated the Aurora
    // cluster, wiping its data and minting a new resource id.
    cfn.update_stack()
        .stack_name("rds-preserve")
        .template_body(RDS_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "rds-preserve", "UPDATE_COMPLETE").await;

    let after = rds
        .describe_db_clusters()
        .db_cluster_identifier("cfn-dbcluster-preserve")
        .send()
        .await
        .expect("describe after");
    let c1 = after
        .db_clusters()
        .first()
        .expect("cluster still exists after update");
    // The template change was applied...
    assert_eq!(
        c1.backup_retention_period(),
        Some(7),
        "the mutable BackupRetentionPeriod change should have been applied in place"
    );
    // ...and the stable resource id is unchanged -> in-place, not replaced.
    assert_eq!(
        c1.db_cluster_resource_id(),
        Some(resource_id_before.as_str()),
        "resource id must be stable across an in-place update; a delete+recreate would mint a new one"
    );
}

// --- AWS::Glue::Database / AWS::Glue::Table -------------------------------
// A benign Description change on a Glue Database/Table used to reprovision
// (delete+create), dropping every contained table/partition. These assert the
// out-of-band catalog data survives an in-place update.

const GLUE_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Db": {
      "Type": "AWS::Glue::Database",
      "Properties": {
        "CatalogId": "000000000000",
        "DatabaseInput": { "Name": "cfn_glue_preserve", "Description": "v1" }
      }
    },
    "Tbl": {
      "Type": "AWS::Glue::Table",
      "Properties": {
        "CatalogId": "000000000000",
        "DatabaseName": "cfn_glue_preserve",
        "TableInput": { "Name": "events", "Description": "t-v1" }
      }
    }
  }
}"#;

// Identical except both Descriptions bumped (mutable, in-place changes).
const GLUE_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Db": {
      "Type": "AWS::Glue::Database",
      "Properties": {
        "CatalogId": "000000000000",
        "DatabaseInput": { "Name": "cfn_glue_preserve", "Description": "v2" }
      }
    },
    "Tbl": {
      "Type": "AWS::Glue::Table",
      "Properties": {
        "CatalogId": "000000000000",
        "DatabaseName": "cfn_glue_preserve",
        "TableInput": { "Name": "events", "Description": "t-v2" }
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_glue_database_and_table_preserve_contents() {
    use aws_sdk_glue::types::{Column, PartitionInput, StorageDescriptor};

    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let glue = server.glue_client().await;

    cfn.create_stack()
        .stack_name("glue-preserve")
        .template_body(GLUE_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "glue-preserve", "CREATE_COMPLETE").await;

    // Out-of-band: add a partition to the CFN-created table. A delete+recreate
    // of the table (or its database) would drop this partition.
    glue.create_partition()
        .database_name("cfn_glue_preserve")
        .table_name("events")
        .partition_input(
            PartitionInput::builder()
                .values("2026-01-01")
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location("s3://bucket/events/2026-01-01/")
                        .columns(
                            Column::builder()
                                .name("id")
                                .r#type("string")
                                .build()
                                .expect("column"),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create_partition");

    // Change a mutable property on BOTH the database and the table.
    cfn.update_stack()
        .stack_name("glue-preserve")
        .template_body(GLUE_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "glue-preserve", "UPDATE_COMPLETE").await;

    // The description changes were applied in place...
    let db = glue
        .get_database()
        .name("cfn_glue_preserve")
        .send()
        .await
        .expect("get_database")
        .database
        .expect("database present");
    assert_eq!(db.description(), Some("v2"));

    let tbl = glue
        .get_table()
        .database_name("cfn_glue_preserve")
        .name("events")
        .send()
        .await
        .expect("get_table")
        .table
        .expect("table present");
    assert_eq!(tbl.description(), Some("t-v2"));

    // ...and the out-of-band partition survived -> in-place, not reprovision.
    let parts = glue
        .get_partitions()
        .database_name("cfn_glue_preserve")
        .table_name("events")
        .send()
        .await
        .expect("get_partitions")
        .partitions
        .unwrap_or_default();
    assert_eq!(
        parts.len(),
        1,
        "the out-of-band partition must survive an in-place Glue update; a reprovision would have dropped it"
    );
}

// --- AWS::Timestream::Table ----------------------------------------------
// A RetentionProperties change on a Timestream table used to reprovision,
// deleting every ingested record. This asserts the records survive.

const TS_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Db": {
      "Type": "AWS::Timestream::Database",
      "Properties": { "DatabaseName": "cfn_ts_preserve" }
    },
    "Tbl": {
      "Type": "AWS::Timestream::Table",
      "Properties": {
        "DatabaseName": "cfn_ts_preserve",
        "TableName": "cpu",
        "RetentionProperties": {
          "MemoryStoreRetentionPeriodInHours": "24",
          "MagneticStoreRetentionPeriodInDays": "7"
        }
      }
    }
  }
}"#;

// Identical except MagneticStoreRetentionPeriodInDays 7 -> 30 (mutable).
const TS_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Db": {
      "Type": "AWS::Timestream::Database",
      "Properties": { "DatabaseName": "cfn_ts_preserve" }
    },
    "Tbl": {
      "Type": "AWS::Timestream::Table",
      "Properties": {
        "DatabaseName": "cfn_ts_preserve",
        "TableName": "cpu",
        "RetentionProperties": {
          "MemoryStoreRetentionPeriodInHours": "24",
          "MagneticStoreRetentionPeriodInDays": "30"
        }
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_timestream_table_preserves_records() {
    use aws_sdk_timestreamwrite::types::{Dimension, MeasureValueType, Record, TimeUnit};

    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let write = server.timestream_write_client().await;
    let query = server.timestream_query_client().await;

    cfn.create_stack()
        .stack_name("ts-preserve")
        .template_body(TS_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "ts-preserve", "CREATE_COMPLETE").await;

    // Out-of-band: ingest records into the CFN-created table.
    let rec = |host: &str, value: &str, t: &str| {
        Record::builder()
            .dimensions(
                Dimension::builder()
                    .name("host")
                    .value(host)
                    .build()
                    .expect("dimension"),
            )
            .measure_name("cpu")
            .measure_value(value)
            .measure_value_type(MeasureValueType::Double)
            .time(t)
            .time_unit(TimeUnit::Milliseconds)
            .build()
    };
    write
        .write_records()
        .database_name("cfn_ts_preserve")
        .table_name("cpu")
        .records(rec("host-a", "42.5", "1700000000000"))
        .records(rec("host-b", "13.25", "1700000001000"))
        .send()
        .await
        .expect("write_records");

    // Change a mutable retention property. Pre-fix this delete+recreated the
    // table, wiping every ingested record.
    cfn.update_stack()
        .stack_name("ts-preserve")
        .template_body(TS_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "ts-preserve", "UPDATE_COMPLETE").await;

    // The ingested records survived -> in-place, not reprovision.
    let count = query
        .query()
        .query_string(r#"SELECT COUNT(*) FROM "cfn_ts_preserve"."cpu""#)
        .send()
        .await
        .expect("query count");
    assert_eq!(
        count.rows()[0].data()[0].scalar_value(),
        Some("2"),
        "ingested records must survive an in-place Timestream table update; a reprovision would have wiped them"
    );
}
