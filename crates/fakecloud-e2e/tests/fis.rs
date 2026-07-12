//! End-to-end tests for AWS Fault Injection Simulator (FIS), driven through the
//! real `aws-sdk-fis` client against a live fakecloud server. Exercises the
//! experiment-template CRUD, the deterministic experiment lifecycle
//! (`initiating` -> `running` -> `completed`, settled on read), `StopExperiment`,
//! ARN-keyed tagging, and the AWS-provided actions catalog.

use aws_sdk_fis::types::{
    CreateExperimentTemplateActionInput, CreateExperimentTemplateStopConditionInput,
    CreateExperimentTemplateTargetInput,
};
use fakecloud_testkit::TestServer;

async fn fis_client(server: &TestServer) -> aws_sdk_fis::Client {
    aws_sdk_fis::Client::new(&server.aws_config().await)
}

async fn create_template(fis: &aws_sdk_fis::Client) -> String {
    let target = CreateExperimentTemplateTargetInput::builder()
        .resource_type("aws:ec2:instance")
        .resource_arns("arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0")
        .selection_mode("ALL")
        .build()
        .unwrap();
    let action = CreateExperimentTemplateActionInput::builder()
        .action_id("aws:ec2:stop-instances")
        .targets("Instances", "myInstances")
        .build()
        .unwrap();
    let stop = CreateExperimentTemplateStopConditionInput::builder()
        .source("none")
        .build()
        .unwrap();
    let out = fis
        .create_experiment_template()
        .client_token("token-create-1")
        .description("stop an instance")
        .role_arn("arn:aws:iam::000000000000:role/fis-experiment-role")
        .stop_conditions(stop)
        .targets("myInstances", target)
        .actions("stopIt", action)
        .tags("team", "chaos")
        .send()
        .await
        .expect("create_experiment_template");
    let tpl = out.experiment_template().expect("template in response");
    let id = tpl.id().expect("template id").to_string();
    assert!(id.starts_with("EXT"), "unexpected id shape: {id}");
    assert_eq!(tpl.description(), Some("stop an instance"));
    id
}

#[tokio::test]
async fn experiment_template_crud_and_lifecycle() {
    let server = TestServer::start().await;
    let fis = fis_client(&server).await;

    // Create -> Get -> List.
    let template_id = create_template(&fis).await;

    let got = fis
        .get_experiment_template()
        .id(&template_id)
        .send()
        .await
        .expect("get_experiment_template");
    let tpl = got.experiment_template().unwrap();
    assert!(tpl.arn().unwrap().contains(":experiment-template/"));
    assert_eq!(
        tpl.experiment_options()
            .unwrap()
            .account_targeting()
            .unwrap()
            .as_str(),
        "single-account"
    );

    let listed = fis
        .list_experiment_templates()
        .send()
        .await
        .expect("list_experiment_templates");
    assert_eq!(listed.experiment_templates().len(), 1);

    // Start the experiment; it begins `initiating`.
    let started = fis
        .start_experiment()
        .client_token("token-start-1")
        .experiment_template_id(&template_id)
        .send()
        .await
        .expect("start_experiment");
    let exp = started.experiment().unwrap();
    let experiment_id = exp.id().unwrap().to_string();
    assert!(experiment_id.starts_with("EXP"));
    assert_eq!(
        exp.state().unwrap().status().unwrap().as_str(),
        "initiating"
    );

    // First read settles `initiating` -> `running`.
    let running = fis
        .get_experiment()
        .id(&experiment_id)
        .send()
        .await
        .expect("get_experiment");
    assert_eq!(
        running
            .experiment()
            .unwrap()
            .state()
            .unwrap()
            .status()
            .unwrap()
            .as_str(),
        "running"
    );

    // Next read settles `running` -> `completed`.
    let completed = fis
        .get_experiment()
        .id(&experiment_id)
        .send()
        .await
        .expect("get_experiment");
    assert_eq!(
        completed
            .experiment()
            .unwrap()
            .state()
            .unwrap()
            .status()
            .unwrap()
            .as_str(),
        "completed"
    );

    // The experiment appears in ListExperiments, filtered by template id.
    let exps = fis
        .list_experiments()
        .experiment_template_id(&template_id)
        .send()
        .await
        .expect("list_experiments");
    assert_eq!(exps.experiments().len(), 1);

    // Tag the template by ARN, read the tags back, then untag.
    let arn = fis
        .get_experiment_template()
        .id(&template_id)
        .send()
        .await
        .unwrap()
        .experiment_template()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();
    fis.tag_resource()
        .resource_arn(&arn)
        .tags("owner", "sre")
        .send()
        .await
        .expect("tag_resource");
    let tags = fis
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    assert_eq!(
        tags.tags().unwrap().get("owner").map(String::as_str),
        Some("sre")
    );
    fis.untag_resource()
        .resource_arn(&arn)
        .tag_keys("owner")
        .send()
        .await
        .expect("untag_resource");
    let tags = fis
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(!tags.tags().unwrap().contains_key("owner"));

    // Delete the template; it is then gone.
    fis.delete_experiment_template()
        .id(&template_id)
        .send()
        .await
        .expect("delete_experiment_template");
    let err = fis
        .get_experiment_template()
        .id(&template_id)
        .send()
        .await
        .expect_err("template should be gone");
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

#[tokio::test]
async fn stop_experiment_transitions_to_stopping() {
    let server = TestServer::start().await;
    let fis = fis_client(&server).await;
    let template_id = create_template(&fis).await;

    let started = fis
        .start_experiment()
        .client_token("token-start-2")
        .experiment_template_id(&template_id)
        .send()
        .await
        .expect("start_experiment");
    let experiment_id = started.experiment().unwrap().id().unwrap().to_string();

    let stopped = fis
        .stop_experiment()
        .id(&experiment_id)
        .send()
        .await
        .expect("stop_experiment");
    let status = stopped
        .experiment()
        .unwrap()
        .state()
        .unwrap()
        .status()
        .unwrap()
        .as_str()
        .to_string();
    // The experiment is either stopping (just requested) or already settled to
    // stopped on the read inside StopExperiment's reconcile.
    assert!(
        matches!(status.as_str(), "stopping" | "stopped"),
        "unexpected stop status: {status}"
    );
}

#[tokio::test]
async fn actions_catalog_is_served() {
    let server = TestServer::start().await;
    let fis = fis_client(&server).await;

    let actions = fis.list_actions().send().await.expect("list_actions");
    assert!(!actions.actions().is_empty());

    let got = fis
        .get_action()
        .id("aws:ec2:stop-instances")
        .send()
        .await
        .expect("get_action");
    let action = got.action().unwrap();
    assert_eq!(action.id(), Some("aws:ec2:stop-instances"));
    assert!(action
        .arn()
        .unwrap()
        .contains("::action/aws:ec2:stop-instances"));

    let err = fis
        .get_action()
        .id("aws:none:does-not-exist")
        .send()
        .await
        .expect_err("unknown action");
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}
