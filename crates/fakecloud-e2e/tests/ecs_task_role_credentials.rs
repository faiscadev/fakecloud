//! ECS task-role credentials: `GET /_fakecloud/ecs/creds/{task_id}` vends the
//! task role's session, named after the task, that verifies under
//! `--verify-sigv4` and acts as the role under `--iam strict`; revoked once
//! the task stops. Inside the task it is served where the ECS agent serves
//! it: `http://169.254.170.2` + `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, so
//! an unmodified AWS SDK / CLI in the container resolves the task role.
//! RegisterTaskDefinition refuses roles ECS tasks cannot assume.

mod helpers;

use std::time::Duration;

use aws_sdk_ecs::types::ContainerDefinition;
use helpers::TestServer;

const ECS_TASKS_TRUST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
const EC2_TRUST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

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

async fn wait_status(ecs: &aws_sdk_ecs::Client, cluster: &str, arn: &str, want: &str) {
    for _ in 0..240 {
        let desc = ecs
            .describe_tasks()
            .cluster(cluster)
            .tasks(arn)
            .send()
            .await
            .unwrap();
        let status = desc.tasks()[0]
            .last_status()
            .unwrap_or_default()
            .to_string();
        if status == want {
            return;
        }
        assert!(
            want != "RUNNING" || status != "STOPPED",
            "task {arn} stopped before running: {:?}",
            desc.tasks()[0].stopped_reason()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("task {arn} never reached {want}");
}

async fn fetch_creds(server: &TestServer, task_id: &str) -> (u16, serde_json::Value) {
    fetch_creds_with(server, task_id, None).await
}

async fn fetch_creds_with(
    server: &TestServer,
    task_id: &str,
    authorization: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut req = reqwest::Client::new().get(format!(
        "{}/_fakecloud/ecs/creds/{task_id}",
        server.endpoint()
    ));
    if let Some(token) = authorization {
        req = req.header("Authorization", token);
    }
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// Ask the main port for the agent's relative-URI surface
/// (`Host: 169.254.170.2`) from the host itself, i.e. not from a container
/// network.
async fn fetch_link_local_from_host(
    server: &TestServer,
    task_id: &str,
) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .get(format!("{}/v2/credentials/{task_id}", server.endpoint()))
        .header("Host", "169.254.170.2")
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// The credentials JSON the task container printed (`CREDS=<json>`), read
/// live from its container log.
async fn creds_printed_by_container(task_id: &str) -> serde_json::Value {
    let cli = helpers::container_cli();
    for _ in 0..120 {
        let ps = std::process::Command::new(&cli)
            .args([
                "ps",
                "-a",
                "--filter",
                &format!("name=^{task_id}"),
                "--format",
                "{{.ID}}",
            ])
            .output()
            .unwrap();
        if let Some(id) = String::from_utf8_lossy(&ps.stdout).lines().next() {
            let logs = std::process::Command::new(&cli)
                .args(["logs", id])
                .output()
                .unwrap();
            let out = String::from_utf8_lossy(&logs.stdout).into_owned();
            if let Some(json) = out.lines().find_map(|l| l.strip_prefix("CREDS=")) {
                return serde_json::from_str(json).expect("container printed credentials JSON");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("task container never printed its credentials");
}

fn sdk_config(server: &TestServer, creds: &serde_json::Value) -> aws_config::SdkConfig {
    let credentials = aws_credential_types::Credentials::new(
        creds["AccessKeyId"].as_str().unwrap(),
        creds["SecretAccessKey"].as_str().unwrap(),
        Some(creds["Token"].as_str().unwrap().to_string()),
        None,
        "ecs-task",
    );
    aws_config::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(credentials),
        )
        .build()
}

/// SDK config signed with the reserved root-bypass credentials, which skip
/// SigV4 verification and IAM enforcement.
async fn root_config(server: &TestServer) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await
}

fn assert_not_found(status: u16, body: &serde_json::Value) {
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        &serde_json::json!({
            "code": "InvalidIdInRequest",
            "message": "CredentialsV2Request: Credentials not found",
            "HTTPErrorCode": 400,
        })
    );
}

/// A running task's credentials are its role's session named after the task:
/// they verify under --verify-sigv4, are evaluated as the role under
/// --iam strict, are reachable from inside the container, and stop working
/// once the task stops.
#[tokio::test]
async fn running_task_gets_its_role_session_until_it_stops() {
    if !require_docker_or_skip("running_task_gets_its_role_session_until_it_stops") {
        return;
    }
    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
        ("FAKECLOUD_IAM", "strict"),
    ])
    .await;
    let root = root_config(&server).await;
    let iam = aws_sdk_iam::Client::new(&root);
    let ecs = aws_sdk_ecs::Client::new(&root);

    let role = iam
        .create_role()
        .role_name("app-task-role")
        .assume_role_policy_document(ECS_TASKS_TRUST)
        .send()
        .await
        .unwrap();
    let role_arn = role.role().unwrap().arn().to_string();
    iam.put_role_policy()
        .role_name("app-task-role")
        .policy_name("list-queues")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:ListQueues","sts:GetCallerIdentity"],"Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap();

    ecs.create_cluster()
        .cluster_name("creds-cluster")
        .send()
        .await
        .unwrap();
    ecs.register_task_definition()
        .family("creds-family")
        .task_role_arn(&role_arn)
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:3.20")
                .essential(true)
                .command("sh")
                .command("-c")
                // Hold port 80 the way a web app would (the agent address
                // must not take it), fetch the credentials from the agent's
                // link-local address the way an SDK in the task would, then
                // keep running so the host can use them while the task lives.
                .command(
                    "nc -l -p 80 -e true & sleep 1; \
                     echo PORT80=$(netstat -ltn | grep -q ':80 ' && echo up || echo down); \
                     c=$(wget -qO- \"http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI\"); \
                     echo \"$c\" | grep -o '\"RoleArn\":\"[^\"]*\"'; echo \"CREDS=$c\"; \
                     sleep 300",
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let run = ecs
        .run_task()
        .cluster("creds-cluster")
        .task_definition("creds-family")
        .send()
        .await
        .unwrap();
    let task_arn = run.tasks()[0].task_arn().unwrap().to_string();
    let task_id = task_arn.rsplit('/').next().unwrap().to_string();
    wait_status(&ecs, "creds-cluster", &task_arn, "RUNNING").await;

    // Under --iam strict, knowing the task ID is not enough from outside the
    // task: the full-URI endpoint wants the task's
    // AWS_CONTAINER_AUTHORIZATION_TOKEN (only full-URI tasks get one), and
    // the agent's relative-URI surface answers only container-network peers
    // (as on ECS, where only the task's network reaches 169.254.170.2).
    let (status, body) = fetch_creds(&server, &task_id).await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["code"], "AccessDenied");
    let (status, body) = fetch_creds_with(&server, &task_id, Some("not-the-token")).await;
    assert_eq!(status, 401, "{body}");
    let (status, body) = fetch_link_local_from_host(&server, &task_id).await;
    assert_eq!(status, 401, "{body}");

    // The container itself gets them through 169.254.170.2.
    let creds = creds_printed_by_container(&task_id).await;
    assert_eq!(creds["RoleArn"].as_str(), Some(role_arn.as_str()));
    for field in ["AccessKeyId", "SecretAccessKey", "Token", "Expiration"] {
        assert!(creds[field].is_string(), "missing {field}: {creds}");
    }

    // The container reached the endpoint at 169.254.170.2 + the injected
    // relative URI, with its own listener on port 80 up.
    let expected = format!("PORT80=up\n\"RoleArn\":\"{role_arn}\"");
    let mut in_container = false;
    for _ in 0..60 {
        let logs: serde_json::Value = reqwest::Client::new()
            .get(format!(
                "{}/_fakecloud/ecs/tasks/{task_id}/logs",
                server.endpoint()
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if logs["logs"]
            .as_str()
            .unwrap_or_default()
            .contains(&expected)
        {
            in_container = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Captured logs are only guaranteed once the container exits, so the
    // in-container fetch is checked again after the stop below.

    let task_config = sdk_config(&server, &creds);
    let identity = aws_sdk_sts::Client::new(&task_config)
        .get_caller_identity()
        .send()
        .await
        .expect("task credentials verify under --verify-sigv4");
    assert_eq!(
        identity.arn(),
        Some(format!("arn:aws:sts::123456789012:assumed-role/app-task-role/{task_id}").as_str())
    );

    // Under --iam strict the session acts as the role: its policy allows
    // ListQueues (and GetCallerIdentity) and nothing else.
    let sqs = aws_sdk_sqs::Client::new(&task_config);
    sqs.list_queues()
        .send()
        .await
        .expect("the task role allows sqs:ListQueues");
    let denied = sqs
        .create_queue()
        .queue_name("not-allowed")
        .send()
        .await
        .expect_err("the task role does not allow sqs:CreateQueue");
    let code = denied
        .into_service_error()
        .meta()
        .code()
        .map(str::to_string);
    assert!(
        matches!(
            code.as_deref(),
            Some("AccessDenied") | Some("AccessDeniedException")
        ),
        "{code:?}"
    );

    ecs.stop_task()
        .cluster("creds-cluster")
        .task(&task_arn)
        .send()
        .await
        .unwrap();
    wait_status(&ecs, "creds-cluster", &task_arn, "STOPPED").await;

    if !in_container {
        let logs: serde_json::Value = reqwest::Client::new()
            .get(format!(
                "{}/_fakecloud/ecs/tasks/{task_id}/logs",
                server.endpoint()
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            logs["logs"]
                .as_str()
                .unwrap_or_default()
                .contains(&expected),
            "container did not fetch its credentials: {logs}"
        );
    }

    // The session the stopped task was handed no longer authenticates.
    let mut revoked = false;
    for _ in 0..20 {
        if aws_sdk_sts::Client::new(&task_config)
            .get_caller_identity()
            .send()
            .await
            .is_err()
        {
            revoked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(revoked, "stopped task's credentials still authenticate");
}

/// The AWS CLI in a task container, with no credentials configured, resolves
/// the task role through its default credential chain: the agent's relative
/// URI against `169.254.170.2`, which the task's network routes to fakecloud.
/// (A plain-HTTP full URI on `host.docker.internal` is refused by every SDK.)
#[tokio::test]
async fn sdk_in_task_resolves_the_task_role_from_the_link_local_endpoint() {
    if !require_docker_or_skip("sdk_in_task_resolves_the_task_role_from_the_link_local_endpoint") {
        return;
    }
    let server = TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await;
    let root = root_config(&server).await;
    let iam = aws_sdk_iam::Client::new(&root);
    let ecs = aws_sdk_ecs::Client::new(&root);

    let role_arn = iam
        .create_role()
        .role_name("cli-task-role")
        .assume_role_policy_document(ECS_TASKS_TRUST)
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();

    ecs.create_cluster()
        .cluster_name("sdk-creds-cluster")
        .send()
        .await
        .unwrap();
    ecs.register_task_definition()
        .family("sdk-creds-family")
        .task_role_arn(&role_arn)
        .container_definitions(
            ContainerDefinition::builder()
                .name("cli")
                .image("public.ecr.aws/aws-cli/aws-cli:2.37.6")
                .essential(true)
                // fakecloud rewrites loopback URLs in the environment to the
                // host alias the container reaches it at.
                .environment(
                    aws_sdk_ecs::types::KeyValuePair::builder()
                        .name("AWS_ENDPOINT_URL")
                        .value(server.endpoint())
                        .build(),
                )
                .environment(
                    aws_sdk_ecs::types::KeyValuePair::builder()
                        .name("AWS_DEFAULT_REGION")
                        .value("us-east-1")
                        .build(),
                )
                .entry_point("sh")
                .command("-c")
                .command(
                    "echo RELATIVE_URI=[$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI] \
                     FULL_URI=[$AWS_CONTAINER_CREDENTIALS_FULL_URI]; \
                     aws sts get-caller-identity --query Arn --output text",
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let run = ecs
        .run_task()
        .cluster("sdk-creds-cluster")
        .task_definition("sdk-creds-family")
        .send()
        .await
        .unwrap();
    let task_arn = run.tasks()[0].task_arn().unwrap().to_string();
    let task_id = task_arn.rsplit('/').next().unwrap().to_string();
    wait_status(&ecs, "sdk-creds-cluster", &task_arn, "STOPPED").await;

    let task = ecs
        .describe_tasks()
        .cluster("sdk-creds-cluster")
        .tasks(&task_arn)
        .send()
        .await
        .unwrap()
        .tasks()[0]
        .clone();
    let logs: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/_fakecloud/ecs/tasks/{task_id}/logs",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let logs = logs["logs"].as_str().unwrap_or_default().to_string();
    assert_eq!(
        task.containers()[0].exit_code(),
        Some(0),
        "aws sts get-caller-identity failed in the task: {logs}"
    );
    assert!(
        logs.contains(&format!(
            "RELATIVE_URI=[/v2/credentials/{task_id}] FULL_URI=[]"
        )),
        "{logs}"
    );
    assert!(
        logs.contains(&format!(
            "arn:aws:sts::123456789012:assumed-role/cli-task-role/{task_id}"
        )),
        "{logs}"
    );
}

/// Run a one-container task with a task role whose container (`<name>-app`)
/// prints its output and exits, and return `(task_id, captured logs)` once
/// it stopped.
async fn run_role_task_to_completion(
    server: &TestServer,
    name: &str,
    network_mode: Option<aws_sdk_ecs::types::NetworkMode>,
    script: &str,
) -> (String, String) {
    let root = root_config(server).await;
    let iam = aws_sdk_iam::Client::new(&root);
    let ecs = aws_sdk_ecs::Client::new(&root);
    let role_arn = iam
        .create_role()
        .role_name(format!("{name}-role"))
        .assume_role_policy_document(ECS_TASKS_TRUST)
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();
    ecs.create_cluster()
        .cluster_name(name)
        .send()
        .await
        .unwrap();
    let mut register = ecs
        .register_task_definition()
        .family(name)
        .task_role_arn(&role_arn)
        .container_definitions(
            ContainerDefinition::builder()
                .name(format!("{name}-app"))
                .image("public.ecr.aws/docker/library/alpine:3.20")
                .essential(true)
                .command("sh")
                .command("-c")
                .command(script)
                .build(),
        );
    if let Some(mode) = network_mode {
        register = register.network_mode(mode);
    }
    register.send().await.unwrap();
    let run = ecs
        .run_task()
        .cluster(name)
        .task_definition(name)
        .send()
        .await
        .unwrap();
    let task_arn = run.tasks()[0].task_arn().unwrap().to_string();
    let task_id = task_arn.rsplit('/').next().unwrap().to_string();
    wait_status(&ecs, name, &task_arn, "STOPPED").await;
    let logs: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/_fakecloud/ecs/tasks/{task_id}/logs",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        task_id,
        logs["logs"].as_str().unwrap_or_default().to_string(),
    )
}

/// Fetches the credentials at the agent address + relative URI and the task
/// metadata at `ECS_CONTAINER_METADATA_URI_V4`, which names fakecloud by the
/// runtime's host alias: a container joining its holder's namespace shares
/// the holder's `/etc/hosts`, `--add-host` entry included.
const LINK_LOCAL_PROBE: &str =
    "wget -qO- \"http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI\" \
     | grep -o '\"RoleArn\":\"[^\"]*\"'; \
     echo RELATIVE_URI=[$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI]; \
     wget -qO- \"$ECS_CONTAINER_METADATA_URI_V4\" >/dev/null && echo META=ok";

/// A bridge-mode task-role container keeps reaching fakecloud by the host
/// alias (task metadata) while its credentials come from 169.254.170.2.
#[tokio::test]
async fn bridge_task_reaches_the_link_local_endpoint_and_the_host_alias() {
    if !require_docker_or_skip("bridge_task_reaches_the_link_local_endpoint_and_the_host_alias") {
        return;
    }
    let server = TestServer::start().await;
    let (task_id, logs) = run_role_task_to_completion(
        &server,
        "bridge-creds",
        Some(aws_sdk_ecs::types::NetworkMode::Bridge),
        LINK_LOCAL_PROBE,
    )
    .await;
    assert!(
        logs.contains("\"RoleArn\":\"arn:aws:iam::123456789012:role/bridge-creds-role\""),
        "{logs}"
    );
    assert!(
        logs.contains(&format!("RELATIVE_URI=[/v2/credentials/{task_id}]")),
        "{logs}"
    );
    assert!(logs.contains("META=ok"), "{logs}");
}

/// A `none`-mode task has no network: as on ECS it gets the relative URI but
/// nothing answers it, and fakecloud starts no holder for it.
#[tokio::test]
async fn none_mode_task_gets_the_relative_uri_and_no_network() {
    if !require_docker_or_skip("none_mode_task_gets_the_relative_uri_and_no_network") {
        return;
    }
    let server = TestServer::start().await;
    // Watch the task while it runs (holders are removed when it stops): its
    // container must run with `--network none` and no holder may exist.
    let watch = async {
        let docker = |args: Vec<String>| async move {
            let out = tokio::process::Command::new("docker")
                .args(&args)
                .output()
                .await
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        for _ in 0..300 {
            let holders = docker(vec![
                "ps".into(),
                "-a".into(),
                "--filter".into(),
                "label=fakecloud-ecs-netns-for=none-creds-app".into(),
                "-q".into(),
            ])
            .await;
            assert_eq!(holders, "", "a holder was started for a none-mode task");
            let app = docker(vec![
                "ps".into(),
                "--filter".into(),
                "label=fakecloud-ecs-container=none-creds-app".into(),
                "-q".into(),
            ])
            .await;
            if let Some(id) = app.lines().next() {
                let mode = docker(vec![
                    "inspect".into(),
                    "-f".into(),
                    "{{.HostConfig.NetworkMode}}".into(),
                    id.to_string(),
                ])
                .await;
                if !mode.is_empty() {
                    return mode;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("the none-mode task container was never seen running");
    };
    let ((task_id, logs), network_mode) = tokio::join!(
        run_role_task_to_completion(
            &server,
            "none-creds",
            Some(aws_sdk_ecs::types::NetworkMode::None),
            "echo RELATIVE_URI=[$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI] \
             FULL_URI=[$AWS_CONTAINER_CREDENTIALS_FULL_URI]; \
             if [ -e /sys/class/net/eth0 ]; then echo ETH0=yes; else echo ETH0=no; fi; \
             wget -T 2 -qO- \"http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI\" \
             && echo CREDS=reachable || echo CREDS=unreachable; \
             sleep 5",
        ),
        watch
    );
    assert_eq!(network_mode, "none");
    assert!(
        logs.contains(&format!(
            "RELATIVE_URI=[/v2/credentials/{task_id}] FULL_URI=[]"
        )),
        "{logs}"
    );
    assert!(logs.contains("ETH0=no"), "{logs}");
    assert!(logs.contains("CREDS=unreachable"), "{logs}");
}

/// An `awsvpc` task reaches the link-local endpoint too: the namespace
/// holder joins the per-task network in the container's place.
#[tokio::test]
async fn awsvpc_task_reaches_the_link_local_endpoint() {
    if !require_docker_or_skip("awsvpc_task_reaches_the_link_local_endpoint") {
        return;
    }
    let server = TestServer::start().await;
    let (task_id, logs) = run_role_task_to_completion(
        &server,
        "awsvpc-creds",
        Some(aws_sdk_ecs::types::NetworkMode::Awsvpc),
        LINK_LOCAL_PROBE,
    )
    .await;
    assert!(logs.contains("META=ok"), "{logs}");
    assert!(
        logs.contains("\"RoleArn\":\"arn:aws:iam::123456789012:role/awsvpc-creds-role\""),
        "{logs}"
    );
    assert!(
        logs.contains(&format!("RELATIVE_URI=[/v2/credentials/{task_id}]")),
        "{logs}"
    );
}

/// When the task's network can't be set up (here: a helper image with no
/// NAT tooling), the task still runs, with the full URI of fakecloud's
/// endpoint instead of the relative one.
#[tokio::test]
async fn task_falls_back_to_the_full_uri_when_the_namespace_cannot_be_set_up() {
    if !require_docker_or_skip(
        "task_falls_back_to_the_full_uri_when_the_namespace_cannot_be_set_up",
    ) {
        return;
    }
    // busybox has a shell but neither nft, iptables nor apk.
    let server = TestServer::start_with_env(&[(
        "FAKECLOUD_ECS_CREDS_HELPER_IMAGE",
        "public.ecr.aws/docker/library/busybox:1.36",
    )])
    .await;
    let (task_id, logs) = run_role_task_to_completion(
        &server,
        "fallback-creds",
        None,
        "echo RELATIVE_URI=[$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI]; \
         wget -qO- \"$AWS_CONTAINER_CREDENTIALS_FULL_URI\" | grep -o '\"RoleArn\":\"[^\"]*\"'",
    )
    .await;
    assert!(logs.contains("RELATIVE_URI=[]"), "{logs}");
    assert!(
        logs.contains("\"RoleArn\":\"arn:aws:iam::123456789012:role/fallback-creds-role\""),
        "{task_id}: {logs}"
    );
}

/// A task without a task role has no credentials, and neither does an ID no
/// task has: both answered like the ECS agent.
#[tokio::test]
async fn task_without_role_and_unknown_id_get_the_agent_error() {
    let server = TestServer::start().await;
    let (status, body) = fetch_creds(&server, "0123456789abcdef0123456789abcdef").await;
    assert_not_found(status, &body);

    if !require_docker_or_skip("task_without_role_and_unknown_id_get_the_agent_error") {
        return;
    }
    let ecs = server.ecs_client().await;
    ecs.create_cluster()
        .cluster_name("norole-cluster")
        .send()
        .await
        .unwrap();
    ecs.register_task_definition()
        .family("norole-family")
        .container_definitions(
            ContainerDefinition::builder()
                .name("app")
                .image("public.ecr.aws/docker/library/alpine:3.20")
                .essential(true)
                .command("sh")
                .command("-c")
                .command(
                    "echo FULL_URI=[$AWS_CONTAINER_CREDENTIALS_FULL_URI] \
                     RELATIVE_URI=[$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI]; sleep 300",
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let run = ecs
        .run_task()
        .cluster("norole-cluster")
        .task_definition("norole-family")
        .send()
        .await
        .unwrap();
    let task_arn = run.tasks()[0].task_arn().unwrap().to_string();
    let task_id = task_arn.rsplit('/').next().unwrap().to_string();
    wait_status(&ecs, "norole-cluster", &task_arn, "RUNNING").await;

    let (status, body) = fetch_creds(&server, &task_id).await;
    assert_not_found(status, &body);

    ecs.stop_task()
        .cluster("norole-cluster")
        .task(&task_arn)
        .send()
        .await
        .unwrap();
    wait_status(&ecs, "norole-cluster", &task_arn, "STOPPED").await;
    let logs: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/_fakecloud/ecs/tasks/{task_id}/logs",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // As on AWS, a task with no role gets no credentials URI at all.
    assert!(
        logs["logs"]
            .as_str()
            .unwrap_or_default()
            .contains("FULL_URI=[] RELATIVE_URI=[]"),
        "{logs}"
    );
}

/// RegisterTaskDefinition refuses a task or execution role ECS tasks cannot
/// assume (trust policy without `ecs-tasks.amazonaws.com`) and, under IAM
/// enforcement, another account's role, with ECS's ClientException.
#[tokio::test]
async fn register_task_definition_refuses_roles_ecs_cannot_assume() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let root = root_config(&server).await;
    let iam = aws_sdk_iam::Client::new(&root);
    let ecs = aws_sdk_ecs::Client::new(&root);

    let untrusted = iam
        .create_role()
        .role_name("ec2-only")
        .assume_role_policy_document(EC2_TRUST)
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();
    let trusted = iam
        .create_role()
        .role_name("ecs-ok")
        .assume_role_policy_document(ECS_TASKS_TRUST)
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();

    let register = |task_role: String, exec_role: String| {
        ecs.register_task_definition()
            .family("role-checked")
            .task_role_arn(task_role)
            .execution_role_arn(exec_role)
            .container_definitions(
                ContainerDefinition::builder()
                    .name("app")
                    .image("public.ecr.aws/docker/library/alpine:3.20")
                    .build(),
            )
            .send()
    };

    let foreign = "arn:aws:iam::999999999999:role/elsewhere".to_string();
    for (task_role, exec_role, refused) in [
        (untrusted.clone(), trusted.clone(), &untrusted),
        (trusted.clone(), untrusted.clone(), &untrusted),
        (foreign.clone(), trusted.clone(), &foreign),
    ] {
        let err = register(task_role, exec_role)
            .await
            .expect_err("role ECS cannot assume must be refused");
        let status = err.raw_response().map(|r| r.status().as_u16());
        let err = err.into_service_error();
        assert!(err.is_client_exception(), "{err:?}");
        assert_eq!(
            err.meta().message(),
            Some(
                format!(
                    "ECS was unable to assume the role '{refused}' that was provided for this task. \
                     Please verify that the role being passed has the proper trust relationship and \
                     permissions and that your IAM user has permissions to pass this role."
                )
                .as_str()
            )
        );
        assert_eq!(status, Some(400));
    }

    register(trusted.clone(), trusted)
        .await
        .expect("roles that trust ecs-tasks.amazonaws.com register");
}
