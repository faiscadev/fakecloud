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

use aws_sdk_cloudformation::types::Capability;
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

// --- AWS::AutoScaling::AutoScalingGroup -----------------------------------
//
// The reprovision fallback would delete the group (terminating EVERY running
// instance) and recreate it (launching a brand-new set) on a benign capacity
// change -- churning every instance id/IP and breaking ELB target
// registrations. AWS applies the change in place, reconciling instances by the
// DELTA. This asserts the ORIGINAL instance ids survive a `DesiredCapacity`
// bump (only the added instance is new); a delete+recreate would replace all.

const ASG_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "LC": {
      "Type": "AWS::AutoScaling::LaunchConfiguration",
      "Properties": { "ImageId": "ami-0a1b2c3d4e5f60001", "InstanceType": "t3.micro" }
    },
    "ASG": {
      "Type": "AWS::AutoScaling::AutoScalingGroup",
      "Properties": {
        "AutoScalingGroupName": "cfn-asg-preserve",
        "MinSize": "1", "MaxSize": "5", "DesiredCapacity": "2",
        "LaunchConfigurationName": { "Ref": "LC" },
        "AvailabilityZones": ["us-east-1a"]
      }
    }
  }
}"#;

// Identical except DesiredCapacity 2 -> 3 (a mutable, in-place change).
const ASG_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "LC": {
      "Type": "AWS::AutoScaling::LaunchConfiguration",
      "Properties": { "ImageId": "ami-0a1b2c3d4e5f60001", "InstanceType": "t3.micro" }
    },
    "ASG": {
      "Type": "AWS::AutoScaling::AutoScalingGroup",
      "Properties": {
        "AutoScalingGroupName": "cfn-asg-preserve",
        "MinSize": "1", "MaxSize": "5", "DesiredCapacity": "3",
        "LaunchConfigurationName": { "Ref": "LC" },
        "AvailabilityZones": ["us-east-1a"]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_autoscaling_group_reconciles_by_delta() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let asg = aws_sdk_autoscaling::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("asg-preserve")
        .template_body(ASG_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "asg-preserve", "CREATE_COMPLETE").await;

    // Reconciliation to desired capacity runs in a detached task; poll until the
    // group reports its 2 instances and capture their ids -- these are the
    // instances a delete+recreate would terminate.
    let ids_before = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let asg = asg.clone();
        async move {
            let out = asg
                .describe_auto_scaling_groups()
                .auto_scaling_group_names("cfn-asg-preserve")
                .send()
                .await
                .ok()?;
            let g = out.auto_scaling_groups().first()?;
            (g.instances().len() == 2).then(|| {
                g.instances()
                    .iter()
                    .filter_map(|i| i.instance_id().map(String::from))
                    .collect::<Vec<_>>()
            })
        }
    })
    .await
    .expect("ASG reconciled to 2 instances");

    // Bump DesiredCapacity 2 -> 3. Pre-fix this delete+recreated the whole group,
    // churning every instance id.
    cfn.update_stack()
        .stack_name("asg-preserve")
        .template_body(ASG_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "asg-preserve", "UPDATE_COMPLETE").await;

    // The group reconciles to 3 instances BY DELTA: the 2 original ids survive
    // and exactly one new instance is added.
    let ids_after = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let asg = asg.clone();
        async move {
            let out = asg
                .describe_auto_scaling_groups()
                .auto_scaling_group_names("cfn-asg-preserve")
                .send()
                .await
                .ok()?;
            let g = out.auto_scaling_groups().first()?;
            (g.desired_capacity() == Some(3) && g.instances().len() == 3).then(|| {
                g.instances()
                    .iter()
                    .filter_map(|i| i.instance_id().map(String::from))
                    .collect::<Vec<_>>()
            })
        }
    })
    .await
    .expect("ASG reconciled to 3 instances after in-place update");

    for id in &ids_before {
        assert!(
            ids_after.contains(id),
            "original instance {id} must survive an in-place DesiredCapacity update; \
             a delete+recreate would have churned every id. before={ids_before:?} after={ids_after:?}"
        );
    }
    assert_eq!(
        ids_after.len(),
        3,
        "delta reconcile should add exactly one instance, not replace the set"
    );
}

// --- AWS::Backup::BackupVault ---------------------------------------------
//
// `delete_backup_vault` removes the vault AND its `recovery_points`, so the
// reprovision fallback (delete + recreate) would wipe every stored recovery
// point AND mint a fresh `creation_date` on a benign tag change. The in-place
// arm mutates the stored record, preserving the server-minted `creation_date`
// (and, by construction on the untouched record, the `recovery_points`).

const VAULT_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Vault": {
      "Type": "AWS::Backup::BackupVault",
      "Properties": {
        "BackupVaultName": "cfn-vault-preserve",
        "BackupVaultTags": { "env": "staging" }
      }
    }
  }
}"#;

// Identical except the tag value staging -> prod (a mutable, in-place change).
const VAULT_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Vault": {
      "Type": "AWS::Backup::BackupVault",
      "Properties": {
        "BackupVaultName": "cfn-vault-preserve",
        "BackupVaultTags": { "env": "prod" }
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_backup_vault_is_in_place() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let backup = server.backup_client().await;

    cfn.create_stack()
        .stack_name("vault-preserve")
        .template_body(VAULT_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "vault-preserve", "CREATE_COMPLETE").await;

    // Record the server-minted creation date -- the vault's stable identity. A
    // delete+recreate would mint a new one (and drop any recovery points).
    let before = backup
        .describe_backup_vault()
        .backup_vault_name("cfn-vault-preserve")
        .send()
        .await
        .expect("describe before");
    let creation_before = before.creation_date().expect("creation date present");

    // Change a mutable property (a tag value). Pre-fix this delete+recreated the
    // vault, wiping its recovery points and minting a new creation date.
    cfn.update_stack()
        .stack_name("vault-preserve")
        .template_body(VAULT_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "vault-preserve", "UPDATE_COMPLETE").await;

    let after = backup
        .describe_backup_vault()
        .backup_vault_name("cfn-vault-preserve")
        .send()
        .await
        .expect("describe after");
    // The vault still exists and its stable creation date is unchanged -> the
    // update was in place, not a destructive delete+recreate.
    assert_eq!(
        after.creation_date(),
        Some(creation_before),
        "creation date must be stable across an in-place update; a delete+recreate would mint a new one (and drop recovery points)"
    );

    // ...and the tag change was actually applied.
    let tags = backup
        .list_tags()
        .resource_arn(after.backup_vault_arn().expect("vault arn"))
        .send()
        .await
        .expect("list_tags");
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("prod"),
        "the mutable tag change should have been applied in place"
    );
}

// --- AWS::IAM::User -------------------------------------------------------
//
// Without an in-place arm the reprovision fallback would `delete_iam_user` +
// re-create on a benign tag change: that WIPES every access key the user held
// and mints a new `user_id`. This asserts an out-of-band access key and the
// stable `user_id` both survive a tag-only `UpdateStack`.

const USER_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Worker": {
      "Type": "AWS::IAM::User",
      "Properties": {
        "UserName": "cfn-preserve-worker",
        "Tags": [{"Key": "env", "Value": "staging"}]
      }
    }
  }
}"#;

// Identical except the tag value staging -> prod (a mutable, in-place change).
const USER_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Worker": {
      "Type": "AWS::IAM::User",
      "Properties": {
        "UserName": "cfn-preserve-worker",
        "Tags": [{"Key": "env", "Value": "prod"}]
      }
    }
  }
}"#;

#[tokio::test]
async fn cfn_update_iam_user_preserves_access_keys() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let iam = server.iam_client().await;

    cfn.create_stack()
        .stack_name("user-preserve")
        .template_body(USER_TEMPLATE_V1)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "user-preserve", "CREATE_COMPLETE").await;

    // The server-minted user id is the user's stable identity; a delete+recreate
    // would mint a new one.
    let before = iam
        .get_user()
        .user_name("cfn-preserve-worker")
        .send()
        .await
        .expect("get_user before");
    let user_id_before = before.user().expect("user").user_id().to_string();

    // Out-of-band: mint an access key the template never carries. A
    // delete+recreate would wipe it.
    let key = iam
        .create_access_key()
        .user_name("cfn-preserve-worker")
        .send()
        .await
        .expect("create_access_key");
    let access_key_id = key
        .access_key()
        .expect("access key")
        .access_key_id()
        .to_string();

    // Change a mutable property (a tag value). Pre-fix this delete+recreated the
    // user, wiping the access key and minting a new user id.
    cfn.update_stack()
        .stack_name("user-preserve")
        .template_body(USER_TEMPLATE_V2)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "user-preserve", "UPDATE_COMPLETE").await;

    // The user id is unchanged -> in-place, not a destructive recreate.
    let after = iam
        .get_user()
        .user_name("cfn-preserve-worker")
        .send()
        .await
        .expect("get_user after");
    assert_eq!(
        after.user().expect("user").user_id(),
        user_id_before,
        "user id must be stable across an in-place update; a recreate would mint a new one"
    );

    // The out-of-band access key survived the update.
    let keys = iam
        .list_access_keys()
        .user_name("cfn-preserve-worker")
        .send()
        .await
        .expect("list_access_keys");
    assert!(
        keys.access_key_metadata()
            .iter()
            .any(|k| k.access_key_id() == Some(access_key_id.as_str())),
        "the out-of-band access key must survive an in-place IAM user update; a reprovision would have wiped it"
    );

    // ...and the tag change was actually applied.
    let tags = iam
        .list_user_tags()
        .user_name("cfn-preserve-worker")
        .send()
        .await
        .expect("list_user_tags");
    assert_eq!(
        tags.tags()
            .iter()
            .find(|t| t.key() == "env")
            .map(|t| t.value()),
        Some("prod"),
        "the mutable tag change should have been applied in place"
    );
}

// --- AWS::Cognito::IdentityPool -------------------------------------------
//
// Without an in-place arm the reprovision fallback would
// `delete_cognito_identity_pool` + re-create on a benign name change: that
// mints a brand-new `<region>:<uuid>` pool id AND cascade-drops the pool's
// separately-managed role attachment. This asserts the pool id stays stable and
// an out-of-band role attachment survives a name-only `UpdateStack`.

const POOL_TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Pool": {
      "Type": "AWS::Cognito::IdentityPool",
      "Properties": {
        "IdentityPoolName": "cfn_preserve_pool_v1",
        "AllowUnauthenticatedIdentities": true
      }
    }
  },
  "Outputs": {
    "PoolId": {"Value": {"Ref": "Pool"}}
  }
}"#;

// Identical except the pool name is bumped (a mutable, in-place change).
const POOL_TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Pool": {
      "Type": "AWS::Cognito::IdentityPool",
      "Properties": {
        "IdentityPoolName": "cfn_preserve_pool_v2",
        "AllowUnauthenticatedIdentities": true
      }
    }
  },
  "Outputs": {
    "PoolId": {"Value": {"Ref": "Pool"}}
  }
}"#;

async fn stack_output(server: &TestServer, stack: &str, key: &str) -> String {
    let cfn = server.cloudformation_client().await;
    let out = cfn
        .describe_stacks()
        .stack_name(stack)
        .send()
        .await
        .expect("describe_stacks");
    out.stacks()
        .first()
        .expect("stack")
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("output {key} not found"))
        .to_string()
}

#[tokio::test]
async fn cfn_update_cognito_identity_pool_preserves_role_attachment() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let cognito_identity = server.cognito_identity_client().await;
    let iam = server.iam_client().await;

    // A role to attach to the pool out-of-band.
    let trust_doc = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": "cognito-identity.amazonaws.com"},
            "Action": "sts:AssumeRoleWithWebIdentity"
        }]
    }"#;
    let role = iam
        .create_role()
        .role_name("CfnPreservePoolUnauthRole")
        .assume_role_policy_document(trust_doc)
        .send()
        .await
        .expect("create unauth role");
    let role_arn = role.role().unwrap().arn().to_string();

    cfn.create_stack()
        .stack_name("pool-preserve")
        .template_body(POOL_TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "pool-preserve", "CREATE_COMPLETE").await;

    let pool_id = stack_output(&server, "pool-preserve", "PoolId").await;
    assert!(
        pool_id.contains(':'),
        "identity pool id should be `<region>:<uuid>`: {pool_id}"
    );

    // Out-of-band: attach roles to the pool (a separate SetIdentityPoolRoles
    // record). A delete+recreate would cascade-drop this attachment.
    use std::collections::HashMap;
    let mut roles = HashMap::new();
    roles.insert("authenticated".to_string(), role_arn.clone());
    roles.insert("unauthenticated".to_string(), role_arn.clone());
    cognito_identity
        .set_identity_pool_roles()
        .identity_pool_id(&pool_id)
        .set_roles(Some(roles))
        .send()
        .await
        .expect("set identity pool roles");

    // Change a mutable property (the pool name). Pre-fix this delete+recreated
    // the pool, minting a new id and dropping the role attachment.
    cfn.update_stack()
        .stack_name("pool-preserve")
        .template_body(POOL_TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "pool-preserve", "UPDATE_COMPLETE").await;

    // The pool id is unchanged -> in-place, not a destructive recreate.
    let pool_id_after = stack_output(&server, "pool-preserve", "PoolId").await;
    assert_eq!(
        pool_id_after, pool_id,
        "identity pool id must be stable across an in-place update; a recreate would mint a new `<region>:<uuid>`"
    );

    // The name change was actually applied.
    let described = cognito_identity
        .describe_identity_pool()
        .identity_pool_id(&pool_id)
        .send()
        .await
        .expect("describe identity pool");
    assert_eq!(
        described.identity_pool_name(),
        "cfn_preserve_pool_v2",
        "the mutable name change should have been applied in place"
    );

    // The out-of-band role attachment survived the update.
    let got_roles = cognito_identity
        .get_identity_pool_roles()
        .identity_pool_id(&pool_id)
        .send()
        .await
        .expect("get identity pool roles");
    assert_eq!(
        got_roles
            .roles()
            .and_then(|m| m.get("unauthenticated"))
            .map(String::as_str),
        Some(role_arn.as_str()),
        "the out-of-band role attachment must survive an in-place identity pool update; a reprovision would have cascade-dropped it"
    );
}
