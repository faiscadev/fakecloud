//! Container data-plane reachability: what a workload in a container (or a
//! load balancer in front of one) can actually reach, not just what the
//! control plane reports.
//!
//! - an ALB in front of an `awsvpc` ECS service routes to the task, whose
//!   ENI IP comes from the service's subnet;
//! - an ALB in front of an EC2 instance routes to a port the instance opened
//!   after boot;
//! - an EC2 instance reaches IMDS at `169.254.169.254` with its own identity
//!   and instance-profile credentials, including from user-data;
//! - an ECS task pulls an ECR image when fakecloud says it runs in a
//!   container (the pull is the host engine's, so it must not use the
//!   sibling host alias).

mod helpers;

use std::time::Duration;

use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerDefinition, LoadBalancer, NetworkConfiguration,
    NetworkMode, PortMapping,
};
use aws_sdk_elasticloadbalancingv2::types::{
    Action, ActionTypeEnum, LoadBalancerSchemeEnum, ProtocolEnum, TargetDescription, TargetTypeEnum,
};
use base64::Engine;
use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("skipping {test}: docker is not available");
    false
}

fn docker(args: &[&str]) -> std::process::Output {
    std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("spawn docker")
}

/// The container backing an instance, via the `fakecloud-ec2=<id>` label.
fn container_for(instance_id: &str) -> String {
    let out = docker(&[
        "ps",
        "-q",
        "--filter",
        &format!("label=fakecloud-ec2={instance_id}"),
    ]);
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

async fn wait_for_bound_port(server: &TestServer, lb_arn: &str) -> u16 {
    let url = format!("{}/_fakecloud/elbv2/load-balancers", server.endpoint());
    let client = reqwest::Client::new();
    for _ in 0..80 {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                let port = v["loadBalancers"].as_array().and_then(|arr| {
                    arr.iter()
                        .find(|lb| lb["arn"].as_str() == Some(lb_arn))
                        .and_then(|lb| lb["boundPort"].as_u64())
                });
                if let Some(p) = port {
                    return p as u16;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("data plane never bound a port for {lb_arn}");
}

/// An internal ALB with an HTTP listener forwarding to a fresh target group
/// of `target_type` on `port`. Returns `(lb_arn, tg_arn)`.
async fn alb_with_target_group(
    elbv2: &aws_sdk_elasticloadbalancingv2::Client,
    name: &str,
    target_type: TargetTypeEnum,
    port: i32,
) -> (String, String) {
    let lb_arn = elbv2
        .create_load_balancer()
        .name(name)
        .scheme(LoadBalancerSchemeEnum::Internal)
        .send()
        .await
        .expect("create_load_balancer")
        .load_balancers()[0]
        .load_balancer_arn()
        .unwrap()
        .to_string();
    let tg_arn = elbv2
        .create_target_group()
        .name(format!("{name}-tg"))
        .protocol(ProtocolEnum::Http)
        .port(port)
        .target_type(target_type)
        .health_check_protocol(ProtocolEnum::Http)
        .health_check_path("/")
        .health_check_interval_seconds(5)
        .health_check_timeout_seconds(2)
        .healthy_threshold_count(2)
        .unhealthy_threshold_count(2)
        .send()
        .await
        .expect("create_target_group")
        .target_groups()[0]
        .target_group_arn()
        .unwrap()
        .to_string();
    elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::Forward)
                .target_group_arn(&tg_arn)
                .build(),
        )
        .send()
        .await
        .expect("create_listener");
    (lb_arn, tg_arn)
}

/// Poll the ALB until a request through it returns 200 with `want` in the
/// body, or panic with the last status and the target health.
async fn wait_for_alb_200(
    elbv2: &aws_sdk_elasticloadbalancingv2::Client,
    tg_arn: &str,
    alb_port: u16,
    want: &str,
    attempts: u32,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut last = String::new();
    for _ in 0..attempts {
        match client
            .get(format!("http://127.0.0.1:{alb_port}/"))
            .send()
            .await
        {
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                if status == 200 && body.contains(want) {
                    return;
                }
                last = format!("{status} {body}");
            }
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let health = elbv2
        .describe_target_health()
        .target_group_arn(tg_arn)
        .send()
        .await
        .map(|h| format!("{:?}", h.target_health_descriptions()))
        .unwrap_or_default();
    panic!("ALB never returned 200 with {want:?}; last: {last}; targets: {health}");
}

/// Create a VPC + subnet and return `(subnet_id, cidr)`.
async fn subnet(ec2: &aws_sdk_ec2::Client, vpc_cidr: &str, cidr: &str) -> String {
    let vpc = ec2
        .create_vpc()
        .cidr_block(vpc_cidr)
        .send()
        .await
        .expect("create_vpc")
        .vpc()
        .unwrap()
        .vpc_id()
        .unwrap()
        .to_string();
    ec2.create_subnet()
        .vpc_id(vpc)
        .cidr_block(cidr)
        .send()
        .await
        .expect("create_subnet")
        .subnet()
        .unwrap()
        .subnet_id()
        .unwrap()
        .to_string()
}

fn ip_in_24(ip: &str, prefix: &str) -> bool {
    ip.starts_with(prefix)
        && ip
            .rsplit('.')
            .next()
            .and_then(|o| o.parse::<u8>().ok())
            .is_some_and(|o| (4..=254).contains(&o))
}

#[tokio::test]
async fn alb_routes_to_awsvpc_ecs_service_task() {
    if !require_docker_or_skip("alb_routes_to_awsvpc_ecs_service_task") {
        return;
    }
    let server = TestServer::start().await;
    let ec2 = server.ec2_client().await;
    let ecs = server.ecs_client().await;
    let elbv2 = server.elbv2_client().await;

    let subnet_id = subnet(&ec2, "10.42.0.0/16", "10.42.7.0/24").await;
    let (lb_arn, tg_arn) =
        alb_with_target_group(&elbv2, "awsvpc-alb", TargetTypeEnum::Ip, 80).await;

    ecs.create_cluster()
        .cluster_name("reach")
        .send()
        .await
        .expect("create_cluster");
    ecs.register_task_definition()
        .family("reach-web")
        .network_mode(NetworkMode::Awsvpc)
        .container_definitions(
            ContainerDefinition::builder()
                .name("web")
                .image("public.ecr.aws/docker/library/busybox:1.36")
                .essential(true)
                .port_mappings(PortMapping::builder().container_port(80).build())
                .entry_point("sh")
                .command("-c")
                .command(
                    "mkdir -p /www && echo awsvpc-task-says-hi > /www/index.html \
                     && exec httpd -f -p 80 -h /www",
                )
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition");
    ecs.create_service()
        .cluster("reach")
        .service_name("web")
        .task_definition("reach-web")
        .desired_count(1)
        .load_balancers(
            LoadBalancer::builder()
                .target_group_arn(&tg_arn)
                .container_name("web")
                .container_port(80)
                .build(),
        )
        .network_configuration(
            NetworkConfiguration::builder()
                .awsvpc_configuration(
                    AwsVpcConfiguration::builder()
                        .subnets(&subnet_id)
                        .assign_public_ip(AssignPublicIp::Disabled)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create_service");

    let alb_port = wait_for_bound_port(&server, &lb_arn).await;
    wait_for_alb_200(&elbv2, &tg_arn, alb_port, "awsvpc-task-says-hi", 180).await;

    // The ENI IP DescribeTasks reports is from the service's subnet, and is
    // exactly the target the service registered.
    let tasks = ecs
        .list_tasks()
        .cluster("reach")
        .service_name("web")
        .send()
        .await
        .expect("list_tasks");
    let desc = ecs
        .describe_tasks()
        .cluster("reach")
        .set_tasks(Some(tasks.task_arns().to_vec()))
        .send()
        .await
        .expect("describe_tasks");
    let eni = desc.tasks()[0]
        .attachments()
        .iter()
        .find(|a| a.r#type() == Some("eni"))
        .expect("eni attachment")
        .clone();
    let detail = |name: &str| {
        eni.details()
            .iter()
            .find(|d| d.name() == Some(name))
            .and_then(|d| d.value())
            .map(str::to_string)
    };
    let ip = detail("privateIPv4Address").expect("privateIPv4Address");
    assert!(ip_in_24(&ip, "10.42.7."), "ENI IP {ip} outside the subnet");
    assert_eq!(detail("subnetId").as_deref(), Some(subnet_id.as_str()));
    assert!(detail("networkInterfaceId").is_some_and(|v| v.starts_with("eni-")));
    let health = elbv2
        .describe_target_health()
        .target_group_arn(&tg_arn)
        .send()
        .await
        .unwrap();
    let target = health.target_health_descriptions()[0].target().unwrap();
    assert_eq!(target.id(), Some(ip.as_str()));
    assert_eq!(target.port(), Some(80));

    ecs.delete_service()
        .cluster("reach")
        .service("web")
        .force(true)
        .send()
        .await
        .ok();
}

#[tokio::test]
async fn alb_routes_to_ec2_instance_port() {
    if !require_docker_or_skip("alb_routes_to_ec2_instance_port") {
        return;
    }
    let server = TestServer::start_with_env(&[(
        "FAKECLOUD_EC2_DEFAULT_IMAGE",
        "public.ecr.aws/docker/library/busybox:1.36",
    )])
    .await;
    let ec2 = server.ec2_client().await;
    let elbv2 = server.elbv2_client().await;

    // The instance opens port 8080 from user-data, after boot.
    let user_data = base64::engine::general_purpose::STANDARD.encode(
        "mkdir -p /www && echo instance-says-hi > /www/index.html && httpd -p 8080 -h /www\n",
    );
    let instance_id = ec2
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .user_data(user_data)
        .send()
        .await
        .expect("run_instances")
        .instances()[0]
        .instance_id()
        .unwrap()
        .to_string();

    let (lb_arn, tg_arn) =
        alb_with_target_group(&elbv2, "instance-alb", TargetTypeEnum::Instance, 8080).await;
    elbv2
        .register_targets()
        .target_group_arn(&tg_arn)
        .targets(
            TargetDescription::builder()
                .id(&instance_id)
                .port(8080)
                .build(),
        )
        .send()
        .await
        .expect("register_targets");

    let alb_port = wait_for_bound_port(&server, &lb_arn).await;
    wait_for_alb_200(&elbv2, &tg_arn, alb_port, "instance-says-hi", 180).await;

    // Terminating the instance takes its forwarder with it.
    ec2.terminate_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .expect("terminate_instances");
    let mut gone = false;
    for _ in 0..60 {
        let out = docker(&[
            "ps",
            "-aq",
            "--filter",
            &format!("label=fakecloud-ec2-fwd={instance_id}"),
        ]);
        if String::from_utf8_lossy(&out.stdout).trim().is_empty() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(gone, "forwarder outlived the terminated instance");
}

#[tokio::test]
async fn ec2_instance_reaches_imds_with_its_identity_and_role() {
    if !require_docker_or_skip("ec2_instance_reaches_imds_with_its_identity_and_role") {
        return;
    }
    let server = TestServer::start_with_env(&[(
        "FAKECLOUD_EC2_DEFAULT_IMAGE",
        "public.ecr.aws/docker/library/alpine:3.20",
    )])
    .await;
    let ec2 = server.ec2_client().await;
    let iam = server.iam_client().await;

    iam.create_role()
        .role_name("imds-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .expect("create_role");
    iam.create_instance_profile()
        .instance_profile_name("imds-profile")
        .send()
        .await
        .expect("create_instance_profile");
    iam.add_role_to_instance_profile()
        .instance_profile_name("imds-profile")
        .role_name("imds-role")
        .send()
        .await
        .expect("add_role_to_instance_profile");

    // User-data reads IMDS first thing, as cloud-init scripts do; the boot
    // waits for the proxy so this finds it.
    let user_data = base64::engine::general_purpose::STANDARD.encode(
        "wget -q -T 5 -O /tmp/imds-id http://169.254.169.254/latest/meta-data/instance-id\n",
    );
    let instance_id = ec2
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .user_data(user_data)
        .iam_instance_profile(
            aws_sdk_ec2::types::IamInstanceProfileSpecification::builder()
                .name("imds-profile")
                .build(),
        )
        .send()
        .await
        .expect("run_instances")
        .instances()[0]
        .instance_id()
        .unwrap()
        .to_string();

    let mut container = String::new();
    for _ in 0..120 {
        container = container_for(&instance_id);
        if !container.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(!container.is_empty(), "instance container never started");

    let fetch = |path: &str| {
        let out = docker(&[
            "exec",
            &container,
            "wget",
            "-q",
            "-T",
            "3",
            "-O",
            "-",
            &format!("http://169.254.169.254{path}"),
        ]);
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let mut id = None;
    for _ in 0..120 {
        id = fetch("/latest/meta-data/instance-id");
        if id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(id.as_deref(), Some(instance_id.as_str()));

    assert_eq!(
        fetch("/latest/meta-data/iam/security-credentials/").as_deref(),
        Some("imds-role")
    );
    let creds: serde_json::Value = serde_json::from_str(
        &fetch("/latest/meta-data/iam/security-credentials/imds-role").expect("credentials"),
    )
    .expect("credentials json");
    assert_eq!(creds["Code"], "Success");
    assert!(creds["AccessKeyId"].as_str().is_some_and(|k| !k.is_empty()));
    let doc: serde_json::Value = serde_json::from_str(
        &fetch("/latest/dynamic/instance-identity/document").expect("identity document"),
    )
    .expect("identity json");
    assert_eq!(doc["instanceId"], instance_id.as_str());

    // User-data ran after IMDS was up.
    let mut from_user_data = String::new();
    for _ in 0..60 {
        let out = docker(&["exec", &container, "cat", "/tmp/imds-id"]);
        from_user_data = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !from_user_data.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(from_user_data, instance_id);

    ec2.terminate_instances()
        .instance_ids(&instance_id)
        .send()
        .await
        .ok();
}

/// fakecloud told it runs in a container still pulls ECR images for ECS
/// tasks: the host engine performs the pull, so it must use the registry
/// host (loopback), not the sibling alias, which the host can't resolve on
/// Linux and Docker Desktop won't use for a plain-HTTP registry.
#[tokio::test]
async fn ecs_pulls_ecr_image_when_fakecloud_is_containerized() {
    if !require_docker_or_skip("ecs_pulls_ecr_image_when_fakecloud_is_containerized") {
        return;
    }
    let server = TestServer::start_with_env(&[("FAKECLOUD_IN_CONTAINER", "1")]).await;
    let port: u16 = server
        .endpoint()
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("port");

    server
        .ecr_client()
        .await
        .create_repository()
        .repository_name("incontainer-pull")
        .send()
        .await
        .expect("create_repository");

    // Push a seed image to fakecloud ECR over loopback, then drop the local
    // tag so the task can only get it from the registry.
    const SEED: &str = "public.ecr.aws/docker/library/alpine:3.20";
    let mut pulled = false;
    for attempt in 0..5u64 {
        if docker(&["image", "inspect", SEED]).status.success()
            || docker(&["pull", "-q", SEED]).status.success()
        {
            pulled = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(5 * (attempt + 1))).await;
    }
    assert!(pulled, "could not pull {SEED}");
    let local = format!("127.0.0.1:{port}/incontainer-pull:v1");
    assert!(docker(&["tag", SEED, &local]).status.success());
    let auth_dir = tempfile::tempdir().unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode("AWS:seed");
    std::fs::write(
        auth_dir.path().join("config.json"),
        serde_json::json!({"auths": {format!("127.0.0.1:{port}"): {"auth": auth}}}).to_string(),
    )
    .unwrap();
    let push = std::process::Command::new("docker")
        .env("DOCKER_CONFIG", auth_dir.path())
        .args(["push", &local])
        .output()
        .expect("docker push");
    assert!(
        push.status.success(),
        "push failed: {}",
        String::from_utf8_lossy(&push.stderr)
    );
    docker(&["rmi", &local]);
    let aws_uri = "123456789012.dkr.ecr.us-east-1.amazonaws.com/incontainer-pull:v1";
    docker(&["rmi", aws_uri]);

    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("incontainer")
        .send()
        .await
        .expect("create_cluster");
    ecs.register_task_definition()
        .family("incontainer-task")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image(aws_uri)
                .essential(true)
                .entry_point("/bin/sh")
                .command("-c")
                .command("echo pulled-through-loopback")
                .build(),
        )
        .send()
        .await
        .expect("register_task_definition");
    let arn = ecs
        .run_task()
        .cluster("incontainer")
        .task_definition("incontainer-task")
        .send()
        .await
        .expect("run_task")
        .tasks()[0]
        .task_arn()
        .unwrap()
        .to_string();

    let mut stopped = None;
    for _ in 0..240 {
        let desc = ecs
            .describe_tasks()
            .cluster("incontainer")
            .tasks(&arn)
            .send()
            .await
            .expect("describe_tasks");
        let task = desc.tasks()[0].clone();
        if task.last_status() == Some("STOPPED") {
            stopped = Some(task);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let task = stopped.expect("task never stopped");
    assert_ne!(
        task.stop_code().map(|c| c.as_str()),
        Some("TaskFailedToStart"),
        "image pull failed: {:?}",
        task.stopped_reason()
    );
    assert_eq!(task.containers()[0].exit_code(), Some(0));
}
