//! CloudFormation-provisioned AWS::ElastiCache::CacheCluster and
//! ReplicationGroup back themselves with a REAL Redis container (the same
//! container the direct CreateCacheCluster/CreateReplicationGroup path spawns),
//! instead of the phantom metadata they used to insert at CFN time.
//!
//! The backing is started in a detached task after CreateStack returns (so the
//! call never blocks on a container boot/pull), which flips the record from
//! `creating` to `available`. With no ElastiCache runtime wired (CI /
//! metadata-only) the record is inserted as `available` immediately, matching
//! the direct API path. Either way the resource reaches `available`, so the
//! test polls for that and is meaningful in both modes.

mod helpers;

use aws_sdk_cloudformation::types::{Capability, OnFailure};
use helpers::TestServer;

const TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Cluster": {
      "Type": "AWS::ElastiCache::CacheCluster",
      "Properties": {
        "ClusterName": "cfn-cc-real",
        "CacheNodeType": "cache.t4g.micro",
        "Engine": "redis",
        "EngineVersion": "7.1",
        "NumCacheNodes": 1,
        "Port": 6379,
        "PreferredAvailabilityZone": "us-east-1a"
      }
    },
    "Repl": {
      "Type": "AWS::ElastiCache::ReplicationGroup",
      "Properties": {
        "ReplicationGroupId": "cfn-rg-real",
        "ReplicationGroupDescription": "real backing",
        "CacheNodeType": "cache.t4g.micro",
        "Engine": "redis",
        "EngineVersion": "7.1",
        "NumCacheClusters": 2,
        "AutomaticFailoverEnabled": true,
        "Port": 6379
      }
    }
  },
  "Outputs": {
    "ClusterId": {"Value": {"Ref": "Cluster"}},
    "ReplId": {"Value": {"Ref": "Repl"}}
  }
}"#;

#[tokio::test]
async fn cfn_elasticache_resources_reach_available() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let ec = aws_sdk_elasticache::Client::new(&server.aws_config().await);

    cfn.create_stack()
        .stack_name("ec-backing-stack")
        .template_body(TEMPLATE)
        .capabilities(Capability::CapabilityIam)
        .on_failure(OnFailure::Rollback)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("ec-backing-stack")
        .send()
        .await
        .expect("describe_stacks");
    let stack = described.stacks().first().expect("stack present");
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // The backing container boots in a detached task after CreateStack returns,
    // flipping the cluster from `creating` to `available`; with no runtime it is
    // `available` immediately. Poll for it either way.
    let cluster_available = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let ec = ec.clone();
        async move {
            let out = ec
                .describe_cache_clusters()
                .cache_cluster_id("cfn-cc-real")
                .send()
                .await
                .ok()?;
            let cluster = out.cache_clusters().first()?;
            (cluster.cache_cluster_status() == Some("available")).then_some(true)
        }
    })
    .await;
    assert_eq!(
        cluster_available,
        Some(true),
        "CFN-provisioned cache cluster must reach available"
    );

    let rg_available = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let ec = ec.clone();
        async move {
            let out = ec
                .describe_replication_groups()
                .replication_group_id("cfn-rg-real")
                .send()
                .await
                .ok()?;
            let rg = out.replication_groups().first()?;
            (rg.status() == Some("available")).then_some(true)
        }
    })
    .await;
    assert_eq!(
        rg_available,
        Some(true),
        "CFN-provisioned replication group must reach available"
    );

    cfn.delete_stack()
        .stack_name("ec-backing-stack")
        .send()
        .await
        .expect("delete_stack");
}
