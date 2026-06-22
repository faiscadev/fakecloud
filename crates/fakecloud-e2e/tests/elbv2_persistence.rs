mod helpers;

use aws_sdk_elasticloadbalancingv2::types::{
    LoadBalancerTypeEnum, ProtocolEnum, TargetDescription, TargetTypeEnum,
};
use helpers::TestServer;

/// Load balancers, target groups, registered targets, and listeners survive a
/// restart in persistent mode. (Target health is re-derived by the prober.)
#[tokio::test]
async fn persistence_round_trip_lb_tg_listener() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let elbv2 = server.elbv2_client().await;

    let lb_arn = elbv2
        .create_load_balancer()
        .name("persist-alb")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();

    let tg_arn = elbv2
        .create_target_group()
        .name("persist-tg")
        .protocol(ProtocolEnum::Http)
        .port(80)
        .target_type(TargetTypeEnum::Ip)
        .send()
        .await
        .unwrap()
        .target_groups()
        .first()
        .unwrap()
        .target_group_arn()
        .unwrap()
        .to_string();

    elbv2
        .register_targets()
        .target_group_arn(&tg_arn)
        .targets(TargetDescription::builder().id("10.0.0.5").port(80).build())
        .send()
        .await
        .unwrap();

    elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(
            aws_sdk_elasticloadbalancingv2::types::Action::builder()
                .r#type(aws_sdk_elasticloadbalancingv2::types::ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let elbv2 = server.elbv2_client().await;

    // Load balancer survives.
    let lbs = elbv2
        .describe_load_balancers()
        .load_balancer_arns(&lb_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        lbs.load_balancers().first().unwrap().load_balancer_name(),
        Some("persist-alb")
    );

    // Target group + registered target survive.
    let th = elbv2
        .describe_target_health()
        .target_group_arn(&tg_arn)
        .send()
        .await
        .unwrap();
    assert!(th
        .target_health_descriptions()
        .iter()
        .any(|d| d.target().and_then(|t| t.id()) == Some("10.0.0.5")));

    // Listener survives.
    let listeners = elbv2
        .describe_listeners()
        .load_balancer_arn(&lb_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(listeners.listeners().len(), 1);
}

/// A deleted load balancer stays gone after restart.
#[tokio::test]
async fn persistence_delete_lb_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let elbv2 = server.elbv2_client().await;

    let lb_arn = elbv2
        .create_load_balancer()
        .name("ephemeral-alb")
        .r#type(LoadBalancerTypeEnum::Application)
        .send()
        .await
        .unwrap()
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    elbv2
        .delete_load_balancer()
        .load_balancer_arn(&lb_arn)
        .send()
        .await
        .unwrap();

    server.restart().await;
    let elbv2 = server.elbv2_client().await;

    let lbs = elbv2.describe_load_balancers().send().await.unwrap();
    assert!(!lbs
        .load_balancers()
        .iter()
        .any(|lb| lb.load_balancer_name() == Some("ephemeral-alb")));
}
