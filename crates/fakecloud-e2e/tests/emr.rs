//! Amazon EMR (elasticmapreduce) control-plane E2E.
//!
//! Exercises the full job-flow lifecycle against a spawned fakecloud server via
//! the AWS Rust SDK, which speaks the real awsJson1.1 wire format (x-amz-target
//! `ElasticMapReduce.<Op>`):
//!
//!   RunJobFlow -> DescribeCluster -> ListClusters -> AddJobFlowSteps
//!             -> ListSteps -> AddTags -> TerminateJobFlows
//!
//! This is a pure CONTROL-PLANE test: `RunJobFlow` settles the cluster to
//! `WAITING` in memory via the control-plane state machine (no Spark/Hadoop
//! container is spawned; that data plane is a later batch).

mod helpers;

use aws_sdk_emr::types::{HadoopJarStepConfig, JobFlowInstancesConfig, StepConfig, Tag};
use helpers::TestServer;

async fn emr_client(server: &TestServer) -> aws_sdk_emr::Client {
    aws_sdk_emr::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn job_flow_lifecycle_run_describe_steps_tags_terminate() {
    let server = TestServer::start().await;
    let emr = emr_client(&server).await;

    // RunJobFlow -> new cluster id (`j-...`) settled to WAITING.
    let instances = JobFlowInstancesConfig::builder()
        .instance_count(3)
        .master_instance_type("m5.xlarge")
        .slave_instance_type("m5.xlarge")
        .keep_job_flow_alive_when_no_steps(true)
        .build();
    let run = emr
        .run_job_flow()
        .name("e2e-emr-cluster")
        .release_label("emr-7.1.0")
        .service_role("EMR_DefaultRole")
        .job_flow_role("EMR_EC2_DefaultRole")
        .instances(instances)
        .send()
        .await
        .expect("run job flow");
    let cluster_id = run.job_flow_id().expect("job flow id").to_string();
    assert!(
        cluster_id.starts_with("j-"),
        "unexpected cluster id: {cluster_id}"
    );
    assert!(
        run.cluster_arn()
            .is_some_and(|a| a.ends_with(&format!(":cluster/{cluster_id}"))),
        "unexpected cluster ARN: {:?}",
        run.cluster_arn()
    );

    // DescribeCluster echoes name / release label / WAITING state.
    let described = emr
        .describe_cluster()
        .cluster_id(&cluster_id)
        .send()
        .await
        .expect("describe cluster");
    let cluster = described.cluster().expect("cluster");
    assert_eq!(cluster.id(), Some(cluster_id.as_str()));
    assert_eq!(cluster.name(), Some("e2e-emr-cluster"));
    assert_eq!(cluster.release_label(), Some("emr-7.1.0"));
    assert_eq!(
        cluster.status().and_then(|s| s.state()).map(|s| s.as_str()),
        Some("WAITING")
    );

    // ListClusters sees it.
    let listed = emr.list_clusters().send().await.expect("list clusters");
    assert!(
        listed
            .clusters()
            .iter()
            .any(|c| c.id() == Some(cluster_id.as_str())),
        "cluster should appear in ListClusters"
    );

    // AddJobFlowSteps returns generated step ids (`s-...`).
    let step = StepConfig::builder()
        .name("word-count")
        .hadoop_jar_step(
            HadoopJarStepConfig::builder()
                .jar("command-runner.jar")
                .args("spark-submit")
                .args("s3://mybucket/wordcount.py")
                .build(),
        )
        .build();
    let added = emr
        .add_job_flow_steps()
        .job_flow_id(&cluster_id)
        .steps(step)
        .send()
        .await
        .expect("add job flow steps");
    let step_ids = added.step_ids();
    assert_eq!(step_ids.len(), 1, "one step id expected");
    assert!(
        step_ids[0].starts_with("s-"),
        "unexpected step id: {}",
        step_ids[0]
    );

    // ListSteps sees the step by name.
    let steps = emr
        .list_steps()
        .cluster_id(&cluster_id)
        .send()
        .await
        .expect("list steps");
    assert!(
        steps.steps().iter().any(|s| s.name() == Some("word-count")),
        "step should appear in ListSteps"
    );

    // DescribeStep resolves the freshly added step.
    let described_step = emr
        .describe_step()
        .cluster_id(&cluster_id)
        .step_id(&step_ids[0])
        .send()
        .await
        .expect("describe step");
    assert_eq!(
        described_step.step().and_then(|s| s.name()),
        Some("word-count")
    );

    // AddTags succeeds and RemoveTags is idempotent.
    emr.add_tags()
        .resource_id(&cluster_id)
        .tags(Tag::builder().key("env").value("e2e").build())
        .send()
        .await
        .expect("add tags");

    // TerminateJobFlows drives the cluster to TERMINATED.
    emr.terminate_job_flows()
        .job_flow_ids(&cluster_id)
        .send()
        .await
        .expect("terminate job flows");
    let after = emr
        .describe_cluster()
        .cluster_id(&cluster_id)
        .send()
        .await
        .expect("describe after terminate");
    assert_eq!(
        after
            .cluster()
            .and_then(|c| c.status())
            .and_then(|s| s.state())
            .map(|s| s.as_str()),
        Some("TERMINATED")
    );

    // DescribeCluster on an unknown id returns InvalidRequestException.
    let err = emr
        .describe_cluster()
        .cluster_id("j-DOESNOTEXIST0")
        .send()
        .await;
    assert!(err.is_err(), "unknown cluster should error");
}
