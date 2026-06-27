//! AWS Batch control plane: compute environment, job queue, job definition,
//! scheduling policy, and the job control plane (SubmitJob -> Describe ->
//! Cancel). Real container-backed job execution lands in a later batch.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn batch_control_plane_end_to_end() {
    let s = TestServer::start().await;
    let batch = aws_sdk_batch::Client::new(&s.aws_config().await);

    // Compute environment.
    batch
        .create_compute_environment()
        .compute_environment_name("ce1")
        .r#type(aws_sdk_batch::types::CeType::Managed)
        .send()
        .await
        .expect("create CE");
    let ces = batch.describe_compute_environments().send().await.unwrap();
    let ce = ces.compute_environments();
    assert_eq!(ce.len(), 1);
    assert_eq!(ce[0].compute_environment_name(), Some("ce1"));
    assert_eq!(ce[0].status().map(|s| s.as_str()), Some("VALID"));

    // Job queue.
    batch
        .create_job_queue()
        .job_queue_name("q1")
        .priority(1)
        .compute_environment_order(
            aws_sdk_batch::types::ComputeEnvironmentOrder::builder()
                .order(1)
                .compute_environment("ce1")
                .build(),
        )
        .send()
        .await
        .expect("create JQ");

    // Job definition (revisioned).
    let jd = batch
        .register_job_definition()
        .job_definition_name("jd1")
        .r#type(aws_sdk_batch::types::JobDefinitionType::Container)
        .send()
        .await
        .expect("register JD");
    assert_eq!(jd.revision(), Some(1));
    let jd2 = batch
        .register_job_definition()
        .job_definition_name("jd1")
        .r#type(aws_sdk_batch::types::JobDefinitionType::Container)
        .send()
        .await
        .unwrap();
    assert_eq!(jd2.revision(), Some(2));

    // Submit a job: control-plane only, parked at SUBMITTED.
    let job = batch
        .submit_job()
        .job_name("j1")
        .job_queue("q1")
        .job_definition("jd1:1")
        .send()
        .await
        .expect("submit job");
    let job_id = job.job_id().unwrap().to_string();

    let desc = batch.describe_jobs().jobs(&job_id).send().await.unwrap();
    assert_eq!(desc.jobs().len(), 1);
    assert_eq!(
        desc.jobs()[0].status().map(|s| s.as_str()),
        Some("SUBMITTED")
    );

    // Cancel moves it to FAILED.
    batch
        .cancel_job()
        .job_id(&job_id)
        .reason("test cleanup")
        .send()
        .await
        .unwrap();
    let after = batch.describe_jobs().jobs(&job_id).send().await.unwrap();
    assert_eq!(after.jobs()[0].status().map(|s| s.as_str()), Some("FAILED"));
}
