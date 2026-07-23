//! End-to-end tests for the Amazon SageMaker control plane, driven through the
//! real `aws-sdk-sagemaker` client against a live fakecloud server. Exercises
//! the model / endpoint-config control plane and tagging end to end: create a
//! model -> describe / list it (round-tripping the execution role ARN and
//! asserting the server-generated `CreationTime` epoch-second timestamp
//! deserialises), create an endpoint config, and tag / list-tags / delete-tags
//! a resource by ARN.

use aws_sdk_sagemaker::types::{ContainerDefinition, ProductionVariant, Tag};
use fakecloud_testkit::TestServer;

async fn sagemaker_client(server: &TestServer) -> aws_sdk_sagemaker::Client {
    let conf = aws_sdk_sagemaker::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_sagemaker::Client::from_conf(conf)
}

#[tokio::test]
async fn sagemaker_control_plane_lifecycle() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    // --- Model (name-addressed) ---
    let role = "arn:aws:iam::000000000000:role/SageMakerRole";
    let created = client
        .create_model()
        .model_name("my-model")
        .execution_role_arn(role)
        .primary_container(
            ContainerDefinition::builder()
                .image("123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest")
                .build(),
        )
        .send()
        .await
        .expect("create_model");
    let model_arn = created.model_arn().expect("model_arn").to_string();
    assert!(
        model_arn.contains(":model/my-model"),
        "unexpected model arn: {model_arn}"
    );

    // Describe round-trips the execution role and carries a numeric CreationTime.
    let described = client
        .describe_model()
        .model_name("my-model")
        .send()
        .await
        .expect("describe_model");
    assert_eq!(described.model_name(), Some("my-model"));
    assert_eq!(described.execution_role_arn(), Some(role));
    assert_eq!(described.model_arn(), Some(model_arn.as_str()));
    // The awsJson1.1 epoch-second timestamp must deserialise into a DateTime.
    let creation = described.creation_time().expect("creation_time present");
    assert!(creation.secs() > 0, "creation time should be a real epoch");

    // List surfaces the model summary with its required Name/Arn/CreationTime.
    let listed = client.list_models().send().await.expect("list_models");
    assert!(listed
        .models()
        .iter()
        .any(|m| m.model_name() == Some("my-model")
            && m.model_arn().unwrap_or_default().contains("my-model")));

    // --- Endpoint config ---
    client
        .create_endpoint_config()
        .endpoint_config_name("my-endpoint-config")
        .production_variants(
            ProductionVariant::builder()
                .variant_name("AllTraffic")
                .model_name("my-model")
                .initial_instance_count(1)
                .instance_type(aws_sdk_sagemaker::types::ProductionVariantInstanceType::MlM5Large)
                .build(),
        )
        .send()
        .await
        .expect("create_endpoint_config");
    let described_cfg = client
        .describe_endpoint_config()
        .endpoint_config_name("my-endpoint-config")
        .send()
        .await
        .expect("describe_endpoint_config");
    assert_eq!(
        described_cfg.endpoint_config_name(),
        Some("my-endpoint-config")
    );
    assert!(described_cfg
        .endpoint_config_arn()
        .unwrap_or_default()
        .contains("my-endpoint-config"));

    // --- Tagging by ARN ---
    client
        .add_tags()
        .resource_arn(&model_arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .send()
        .await
        .expect("add_tags");
    let tags = client
        .list_tags()
        .resource_arn(&model_arn)
        .send()
        .await
        .expect("list_tags");
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("prod")));

    client
        .delete_tags()
        .resource_arn(&model_arn)
        .tag_keys("env")
        .send()
        .await
        .expect("delete_tags");
    let tags = client
        .list_tags()
        .resource_arn(&model_arn)
        .send()
        .await
        .expect("list_tags after delete");
    assert!(tags.tags().is_empty(), "tags should be cleared");

    // Delete the model; describing it afterwards is a ResourceNotFound.
    client
        .delete_model()
        .model_name("my-model")
        .send()
        .await
        .expect("delete_model");
    let err = client
        .describe_model()
        .model_name("my-model")
        .send()
        .await
        .expect_err("describe after delete should fail");
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

/// `StartPipelineExecution` is the only creation path for a pipeline execution;
/// the started execution must then be resolvable by Describe / List.
#[tokio::test]
async fn sagemaker_pipeline_execution_round_trip() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    let started = client
        .start_pipeline_execution()
        .pipeline_name("my-pipeline")
        .pipeline_execution_display_name("run-1")
        .send()
        .await
        .expect("start_pipeline_execution");
    let exec_arn = started
        .pipeline_execution_arn()
        .expect("pipeline_execution_arn")
        .to_string();
    assert!(
        exec_arn.contains(":pipeline/my-pipeline/execution/"),
        "unexpected execution arn: {exec_arn}"
    );

    // Describe resolves the started execution by its minted ARN.
    let described = client
        .describe_pipeline_execution()
        .pipeline_execution_arn(&exec_arn)
        .send()
        .await
        .expect("describe_pipeline_execution");
    assert_eq!(described.pipeline_execution_arn(), Some(exec_arn.as_str()));
    assert!(described
        .pipeline_arn()
        .unwrap_or_default()
        .contains(":pipeline/my-pipeline"));

    // List surfaces the execution summary for the pipeline.
    let listed = client
        .list_pipeline_executions()
        .pipeline_name("my-pipeline")
        .send()
        .await
        .expect("list_pipeline_executions");
    assert!(
        listed
            .pipeline_execution_summaries()
            .iter()
            .any(|s| s.pipeline_execution_arn() == Some(exec_arn.as_str())),
        "started execution should appear in the summaries"
    );
}

/// Stop*/Start* lifecycle ops must advance the target resource's status so a
/// subsequent Describe reflects the transition instead of the default state.
#[tokio::test]
async fn sagemaker_notebook_instance_stop_start_transitions_status() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    client
        .create_notebook_instance()
        .notebook_instance_name("nb-1")
        .instance_type(aws_sdk_sagemaker::types::InstanceType::MlT2Medium)
        .role_arn("arn:aws:iam::000000000000:role/SageMakerRole")
        .send()
        .await
        .expect("create_notebook_instance");

    client
        .stop_notebook_instance()
        .notebook_instance_name("nb-1")
        .send()
        .await
        .expect("stop_notebook_instance");
    let stopped = client
        .describe_notebook_instance()
        .notebook_instance_name("nb-1")
        .send()
        .await
        .expect("describe after stop");
    assert_eq!(
        stopped.notebook_instance_status(),
        Some(&aws_sdk_sagemaker::types::NotebookInstanceStatus::Stopped),
        "StopNotebookInstance must move status to Stopped"
    );

    client
        .start_notebook_instance()
        .notebook_instance_name("nb-1")
        .send()
        .await
        .expect("start_notebook_instance");
    let started = client
        .describe_notebook_instance()
        .notebook_instance_name("nb-1")
        .send()
        .await
        .expect("describe after start");
    assert_eq!(
        started.notebook_instance_status(),
        Some(&aws_sdk_sagemaker::types::NotebookInstanceStatus::InService),
        "StartNotebookInstance must move status to InService"
    );
}

/// `AddAssociation` is the only creation path for a lineage edge; the edge must
/// then be listed and deletable.
#[tokio::test]
async fn sagemaker_association_round_trip() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    let src = "arn:aws:sagemaker:us-east-1:000000000000:experiment/exp";
    let dst = "arn:aws:sagemaker:us-east-1:000000000000:artifact/art";
    client
        .add_association()
        .source_arn(src)
        .destination_arn(dst)
        .send()
        .await
        .expect("add_association");

    let listed = client
        .list_associations()
        .send()
        .await
        .expect("list_associations");
    assert!(
        listed
            .association_summaries()
            .iter()
            .any(|a| a.source_arn() == Some(src) && a.destination_arn() == Some(dst)),
        "added association should appear in the summaries"
    );

    // Delete removes exactly that edge.
    client
        .delete_association()
        .source_arn(src)
        .destination_arn(dst)
        .send()
        .await
        .expect("delete_association");
    let listed = client
        .list_associations()
        .send()
        .await
        .expect("list_associations after delete");
    assert!(
        !listed
            .association_summaries()
            .iter()
            .any(|a| a.source_arn() == Some(src)),
        "deleted association should be gone"
    );
}

/// A List operation's `NameContains` filter must narrow the result to matching
/// records rather than returning the whole family.
#[tokio::test]
async fn sagemaker_list_name_contains_filter() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;
    let role = "arn:aws:iam::000000000000:role/r";

    for name in ["alpha-model", "beta-model"] {
        client
            .create_model()
            .model_name(name)
            .execution_role_arn(role)
            .send()
            .await
            .expect("create_model");
    }

    let listed = client
        .list_models()
        .name_contains("alpha")
        .send()
        .await
        .expect("list_models with NameContains");
    let names: Vec<&str> = listed
        .models()
        .iter()
        .filter_map(|m| m.model_name())
        .collect();
    assert!(
        names.contains(&"alpha-model"),
        "filtered list should contain alpha-model, got {names:?}"
    );
    assert!(
        !names.contains(&"beta-model"),
        "NameContains=alpha should exclude beta-model, got {names:?}"
    );
}

// HyperPod cluster node-set management: BatchAddClusterNodes persists nodes that
// ListClusterNodes reflects, BatchReplaceClusterNodes swaps the underlying
// instance, and BatchDeleteClusterNodes removes them.
#[tokio::test]
async fn sagemaker_batch_cluster_nodes_round_trip() {
    use aws_sdk_sagemaker::types::AddClusterNodeSpecification;

    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    client
        .batch_add_cluster_nodes()
        .cluster_name("hp-cluster")
        .nodes_to_add(
            AddClusterNodeSpecification::builder()
                .instance_group_name("workers")
                .increment_target_count_by(2)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("batch_add_cluster_nodes");

    let listed = client
        .list_cluster_nodes()
        .cluster_name("hp-cluster")
        .send()
        .await
        .expect("list_cluster_nodes");
    let nodes = listed.cluster_node_summaries();
    assert_eq!(nodes.len(), 2, "two nodes should be listed after add");
    let node_id = nodes[0]
        .node_logical_id()
        .expect("node_logical_id")
        .to_string();
    let old_instance = nodes[0].instance_id().expect("instance_id").to_string();

    // Replace swaps the underlying instance, keeping the logical id.
    client
        .batch_replace_cluster_nodes()
        .cluster_name("hp-cluster")
        .node_logical_ids(node_id.clone())
        .send()
        .await
        .expect("batch_replace_cluster_nodes");
    let listed = client
        .list_cluster_nodes()
        .cluster_name("hp-cluster")
        .send()
        .await
        .expect("list_cluster_nodes");
    let replaced = listed
        .cluster_node_summaries()
        .iter()
        .find(|n| n.node_logical_id() == Some(node_id.as_str()))
        .expect("replaced node still present");
    assert_ne!(
        replaced.instance_id(),
        Some(old_instance.as_str()),
        "replace should mint a new InstanceId"
    );

    // Delete removes the node; the set shrinks to one.
    client
        .batch_delete_cluster_nodes()
        .cluster_name("hp-cluster")
        .node_logical_ids(node_id)
        .send()
        .await
        .expect("batch_delete_cluster_nodes");
    let listed = client
        .list_cluster_nodes()
        .cluster_name("hp-cluster")
        .send()
        .await
        .expect("list_cluster_nodes");
    assert_eq!(listed.cluster_node_summaries().len(), 1);
}

// AssociateTrialComponent makes a component visible under a scoped
// ListTrialComponents(TrialName=…); DisassociateTrialComponent removes it.
#[tokio::test]
async fn sagemaker_trial_component_association_round_trip() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    client
        .create_trial()
        .trial_name("t1")
        .experiment_name("e1")
        .send()
        .await
        .expect("create_trial");
    client
        .create_trial_component()
        .trial_component_name("tc1")
        .send()
        .await
        .expect("create_trial_component");

    client
        .associate_trial_component()
        .trial_component_name("tc1")
        .trial_name("t1")
        .send()
        .await
        .expect("associate_trial_component");

    let scoped = client
        .list_trial_components()
        .trial_name("t1")
        .send()
        .await
        .expect("list_trial_components scoped");
    let names: Vec<&str> = scoped
        .trial_component_summaries()
        .iter()
        .filter_map(|c| c.trial_component_name())
        .collect();
    assert_eq!(
        names,
        vec!["tc1"],
        "scoped list should return the associated component"
    );

    client
        .disassociate_trial_component()
        .trial_component_name("tc1")
        .trial_name("t1")
        .send()
        .await
        .expect("disassociate_trial_component");
    let scoped = client
        .list_trial_components()
        .trial_name("t1")
        .send()
        .await
        .expect("list_trial_components scoped");
    assert!(
        scoped.trial_component_summaries().is_empty(),
        "component should no longer be associated with the trial"
    );
}

// SendPipelineExecutionStepSuccess resolves a waiting callback step and echoes
// the pipeline execution ARN.
#[tokio::test]
async fn sagemaker_send_pipeline_step_success_returns_execution_arn() {
    let server = TestServer::start().await;
    let client = sagemaker_client(&server).await;

    let out = client
        .send_pipeline_execution_step_success()
        .callback_token("cbtoken001")
        .send()
        .await
        .expect("send_pipeline_execution_step_success");
    assert!(
        out.pipeline_execution_arn()
            .unwrap_or_default()
            .contains(":pipeline/"),
        "expected a pipeline execution arn, got {:?}",
        out.pipeline_execution_arn()
    );
}
