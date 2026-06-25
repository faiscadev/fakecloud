//! Application Auto Scaling conformance tests.

mod helpers;

use aws_sdk_applicationautoscaling::primitives::DateTime as AwsDateTime;
use aws_sdk_applicationautoscaling::types::{
    PolicyType, ScalableDimension, ScalableTargetAction, ServiceNamespace,
    StepScalingPolicyConfiguration,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

async fn register_target(
    server: &TestServer,
    namespace: ServiceNamespace,
    resource_id: &str,
    dimension: ScalableDimension,
) -> String {
    let aas = server.application_autoscaling_client().await;
    aas.register_scalable_target()
        .service_namespace(namespace)
        .resource_id(resource_id)
        .scalable_dimension(dimension)
        .min_capacity(1)
        .max_capacity(10)
        .send()
        .await
        .unwrap()
        .scalable_target_arn()
        .map(str::to_owned)
        .unwrap()
}

#[test_action(
    "application-autoscaling",
    "RegisterScalableTarget",
    checksum = "0b1f2866"
)]
#[tokio::test]
async fn aas_register_scalable_target() {
    let server = TestServer::start().await;
    let aas = server.application_autoscaling_client().await;
    aas.register_scalable_target()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-register")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .min_capacity(1)
        .max_capacity(5)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DescribeScalableTargets",
    checksum = "3140f281"
)]
#[tokio::test]
async fn aas_describe_scalable_targets() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-describe",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    server
        .application_autoscaling_client()
        .await
        .describe_scalable_targets()
        .service_namespace(ServiceNamespace::Ecs)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DeregisterScalableTarget",
    checksum = "d847fa99"
)]
#[tokio::test]
async fn aas_deregister_scalable_target() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-deregister",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    server
        .application_autoscaling_client()
        .await
        .deregister_scalable_target()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-deregister")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .send()
        .await
        .unwrap();
}

#[test_action("application-autoscaling", "PutScalingPolicy", checksum = "484f4d2f")]
#[tokio::test]
async fn aas_put_scaling_policy() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-policy",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    server
        .application_autoscaling_client()
        .await
        .put_scaling_policy()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-policy")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .policy_name("conf-step")
        .policy_type(PolicyType::StepScaling)
        .step_scaling_policy_configuration(
            StepScalingPolicyConfiguration::builder()
                .adjustment_type(
                    aws_sdk_applicationautoscaling::types::AdjustmentType::ChangeInCapacity,
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DescribeScalingPolicies",
    checksum = "3858be0e"
)]
#[tokio::test]
async fn aas_describe_scaling_policies() {
    let server = TestServer::start().await;
    server
        .application_autoscaling_client()
        .await
        .describe_scaling_policies()
        .service_namespace(ServiceNamespace::Ecs)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DeleteScalingPolicy",
    checksum = "4446ccc4"
)]
#[tokio::test]
async fn aas_delete_scaling_policy() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-delpolicy",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    let aas = server.application_autoscaling_client().await;
    aas.put_scaling_policy()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-delpolicy")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .policy_name("doomed")
        .policy_type(PolicyType::StepScaling)
        .step_scaling_policy_configuration(
            StepScalingPolicyConfiguration::builder()
                .adjustment_type(
                    aws_sdk_applicationautoscaling::types::AdjustmentType::ChangeInCapacity,
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    aas.delete_scaling_policy()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-delpolicy")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .policy_name("doomed")
        .send()
        .await
        .unwrap();
}

#[test_action("application-autoscaling", "PutScheduledAction", checksum = "cbca3b80")]
#[tokio::test]
async fn aas_put_scheduled_action() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-sched",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    server
        .application_autoscaling_client()
        .await
        .put_scheduled_action()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-sched")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .scheduled_action_name("nightly")
        .schedule("cron(0 23 * * ? *)")
        .scalable_target_action(
            ScalableTargetAction::builder()
                .min_capacity(2)
                .max_capacity(8)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DescribeScheduledActions",
    checksum = "8cd55ba1"
)]
#[tokio::test]
async fn aas_describe_scheduled_actions() {
    let server = TestServer::start().await;
    server
        .application_autoscaling_client()
        .await
        .describe_scheduled_actions()
        .service_namespace(ServiceNamespace::Ecs)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DeleteScheduledAction",
    checksum = "092dfd6c"
)]
#[tokio::test]
async fn aas_delete_scheduled_action() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-delsched",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    let aas = server.application_autoscaling_client().await;
    aas.put_scheduled_action()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-delsched")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .scheduled_action_name("doomed")
        .schedule("cron(0 23 * * ? *)")
        .send()
        .await
        .unwrap();
    aas.delete_scheduled_action()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-delsched")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .scheduled_action_name("doomed")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "DescribeScalingActivities",
    checksum = "f9501c29"
)]
#[tokio::test]
async fn aas_describe_scaling_activities() {
    let server = TestServer::start().await;
    server
        .application_autoscaling_client()
        .await
        .describe_scaling_activities()
        .service_namespace(ServiceNamespace::Ecs)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "GetPredictiveScalingForecast",
    checksum = "25f2f97e"
)]
#[tokio::test]
async fn aas_get_predictive_scaling_forecast() {
    let server = TestServer::start().await;
    register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-pred",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    let aas = server.application_autoscaling_client().await;
    aas.put_scaling_policy()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-pred")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .policy_name("pred")
        .policy_type(PolicyType::PredictiveScaling)
        .send()
        .await
        .unwrap();
    let now = AwsDateTime::from_secs(1_700_000_000);
    let later = AwsDateTime::from_secs(1_700_000_000 + 86_400);
    aas.get_predictive_scaling_forecast()
        .service_namespace(ServiceNamespace::Ecs)
        .resource_id("service/cluster/conf-pred")
        .scalable_dimension(ScalableDimension::EcsServiceDesiredCount)
        .policy_name("pred")
        .start_time(now)
        .end_time(later)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "application-autoscaling",
    "ListTagsForResource",
    checksum = "026159b0"
)]
#[tokio::test]
async fn aas_list_tags_for_resource() {
    let server = TestServer::start().await;
    let arn = register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-listtags",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    server
        .application_autoscaling_client()
        .await
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

#[test_action("application-autoscaling", "TagResource", checksum = "e169fa63")]
#[tokio::test]
async fn aas_tag_resource() {
    let server = TestServer::start().await;
    let arn = register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-tag",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    let mut tags = std::collections::HashMap::new();
    tags.insert("env".to_string(), "conf".to_string());
    server
        .application_autoscaling_client()
        .await
        .tag_resource()
        .resource_arn(arn)
        .set_tags(Some(tags))
        .send()
        .await
        .unwrap();
}

#[test_action("application-autoscaling", "UntagResource", checksum = "51b6803a")]
#[tokio::test]
async fn aas_untag_resource() {
    let server = TestServer::start().await;
    let arn = register_target(
        &server,
        ServiceNamespace::Ecs,
        "service/cluster/conf-untag",
        ScalableDimension::EcsServiceDesiredCount,
    )
    .await;
    let aas = server.application_autoscaling_client().await;
    let mut tags = std::collections::HashMap::new();
    tags.insert("k".to_string(), "v".to_string());
    aas.tag_resource()
        .resource_arn(&arn)
        .set_tags(Some(tags))
        .send()
        .await
        .unwrap();
    aas.untag_resource()
        .resource_arn(arn)
        .tag_keys("k")
        .send()
        .await
        .unwrap();
}
