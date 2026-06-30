//! CloudFormation `ExecuteChangeSet` / `DeleteStack` back container resources
//! with REAL containers and reap them on delete, exactly like the `CreateStack`
//! path (#2031-#2034). `cdk deploy`, `sam deploy`, and `aws cloudformation
//! deploy` all provision through CreateChangeSet + ExecuteChangeSet (not raw
//! CreateStack), so without the changeset-side drain a CFN-provisioned RDS /
//! ElastiCache / ECS / ASG resource sat at `creating` forever and a stack
//! delete leaked the backing container.
//!
//! Robust to CI with no container runtime the same way the existing
//! `cloudformation_*_backing` tests are: with no runtime the record is inserted
//! as `available` immediately; with a runtime the detached drain flips it from
//! `creating` to `available`. Either way the resource reaches `available`, so we
//! poll for that. On delete the in-memory record is removed in both modes
//! (and, with a runtime, the backing container is stopped), so we assert the
//! resource is no longer described.

mod helpers;

use helpers::TestServer;

/// An RDS instance added to an existing stack via the change-set path (the CDK /
/// SAM / `aws cloudformation deploy` update flow) must reach `available`, and a
/// stack delete must remove it -- the create-side container drain + delete-side
/// teardown reaching the changeset path (bug-audit 0.1 / 0.3).
#[tokio::test]
async fn execute_change_set_provisions_and_deletes_rds_instance() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let rds = aws_sdk_rds::Client::new(&server.aws_config().await);

    // Stack starts with a single SQS queue (no container backing).
    let base_template = r#"{
        "Resources": {
            "Seed": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-rds-seed"}
            }
        }
    }"#;
    cfn.create_stack()
        .stack_name("cs-rds-stack")
        .template_body(base_template)
        .send()
        .await
        .expect("create_stack");

    // Change set adds an RDS DB instance (keeping the queue).
    let with_rds = r#"{
        "Resources": {
            "Seed": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cs-rds-seed"}
            },
            "Db": {
                "Type": "AWS::RDS::DBInstance",
                "Properties": {
                    "DBInstanceIdentifier": "cs-rds-instance",
                    "DBInstanceClass": "db.t4g.micro",
                    "Engine": "postgres",
                    "EngineVersion": "16.0",
                    "MasterUsername": "admin",
                    "MasterUserPassword": "hunter2-secret",
                    "AllocatedStorage": "20"
                }
            }
        }
    }"#;
    let cs = cfn
        .create_change_set()
        .stack_name("cs-rds-stack")
        .change_set_name("add-rds")
        .template_body(with_rds)
        .send()
        .await
        .expect("create_change_set");
    let cs_id = cs.id().expect("change set id").to_string();

    cfn.execute_change_set()
        .change_set_name(&cs_id)
        .send()
        .await
        .expect("execute_change_set");

    // The instance must reach `available` -- this is the bug: before the
    // changeset-side drain it stayed `creating` forever whenever a runtime was
    // wired (and was never backed by a real container).
    let available = helpers::wait_until(std::time::Duration::from_secs(120), || {
        let rds = rds.clone();
        async move {
            let out = rds
                .describe_db_instances()
                .db_instance_identifier("cs-rds-instance")
                .send()
                .await
                .ok()?;
            let inst = out.db_instances().first()?;
            (inst.db_instance_status() == Some("available")).then_some(true)
        }
    })
    .await;
    assert_eq!(
        available,
        Some(true),
        "changeset-provisioned RDS instance must reach available"
    );

    // Stack delete must remove the instance (and, with a runtime, stop its
    // container instead of leaking it).
    cfn.delete_stack()
        .stack_name("cs-rds-stack")
        .send()
        .await
        .expect("delete_stack");

    let gone = helpers::wait_until(std::time::Duration::from_secs(120), || {
        let rds = rds.clone();
        async move {
            match rds
                .describe_db_instances()
                .db_instance_identifier("cs-rds-instance")
                .send()
                .await
            {
                // DBInstanceNotFound -> deleted.
                Err(_) => Some(true),
                // Still present -> not yet deleted.
                Ok(out) if out.db_instances().is_empty() => Some(true),
                Ok(_) => None,
            }
        }
    })
    .await;
    assert_eq!(
        gone,
        Some(true),
        "stack delete must remove the changeset-provisioned RDS instance"
    );
}

/// Same propagation gap for a first-time stack created entirely through a
/// `CREATE` change set (the `aws cloudformation deploy` / SAM / CDK first
/// deploy). The ElastiCache cluster must reach `available` on execute and be
/// gone after the stack delete.
#[tokio::test]
async fn create_change_set_provisions_and_deletes_elasticache_cluster() {
    use aws_sdk_cloudformation::types::ChangeSetType;

    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ec = aws_sdk_elasticache::Client::new(&server.aws_config().await);

    let template = r#"{
        "Resources": {
            "Cache": {
                "Type": "AWS::ElastiCache::CacheCluster",
                "Properties": {
                    "ClusterName": "cs-ec-cluster",
                    "CacheNodeType": "cache.t4g.micro",
                    "Engine": "redis",
                    "EngineVersion": "7.1",
                    "NumCacheNodes": 1,
                    "Port": 6379
                }
            }
        }
    }"#;

    cfn.create_change_set()
        .stack_name("cs-ec-stack")
        .change_set_name("create-ec")
        .change_set_type(ChangeSetType::Create)
        .template_body(template)
        .send()
        .await
        .expect("create_change_set");

    cfn.execute_change_set()
        .stack_name("cs-ec-stack")
        .change_set_name("create-ec")
        .send()
        .await
        .expect("execute_change_set");

    let available = helpers::wait_until(std::time::Duration::from_secs(120), || {
        let ec = ec.clone();
        async move {
            let out = ec
                .describe_cache_clusters()
                .cache_cluster_id("cs-ec-cluster")
                .send()
                .await
                .ok()?;
            let cluster = out.cache_clusters().first()?;
            (cluster.cache_cluster_status() == Some("available")).then_some(true)
        }
    })
    .await;
    assert_eq!(
        available,
        Some(true),
        "changeset-provisioned cache cluster must reach available"
    );

    cfn.delete_stack()
        .stack_name("cs-ec-stack")
        .send()
        .await
        .expect("delete_stack");

    let gone = helpers::wait_until(std::time::Duration::from_secs(120), || {
        let ec = ec.clone();
        async move {
            match ec
                .describe_cache_clusters()
                .cache_cluster_id("cs-ec-cluster")
                .send()
                .await
            {
                Err(_) => Some(true),
                Ok(out) if out.cache_clusters().is_empty() => Some(true),
                Ok(_) => None,
            }
        }
    })
    .await;
    assert_eq!(
        gone,
        Some(true),
        "stack delete must remove the changeset-provisioned cache cluster"
    );
}
