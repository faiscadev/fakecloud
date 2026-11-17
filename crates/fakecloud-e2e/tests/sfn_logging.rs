//! Step Functions execution logging delivered to CloudWatch Logs, in the
//! format recorded against AWS: one JSON object per history event, in
//! `states/<machine>/<hour>/00000000` for STANDARD and
//! `states/<machine>/<minute>/00000000` for EXPRESS executions.

mod helpers;

use aws_sdk_sfn::error::ProvideErrorMetadata;
use aws_sdk_sfn::types::{CloudWatchLogsLogGroup, LogDestination, LogLevel, LoggingConfiguration};
use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

const ROLE: &str = "arn:aws:iam::123456789012:role/sfn-role";

fn logging(group: &str, level: LogLevel, include_data: bool) -> LoggingConfiguration {
    LoggingConfiguration::builder()
        .level(level)
        .include_execution_data(include_data)
        .destinations(
            LogDestination::builder()
                .cloud_watch_logs_log_group(
                    CloudWatchLogsLogGroup::builder()
                        .log_group_arn(format!(
                            "arn:aws:logs:us-east-1:123456789012:log-group:{group}:*"
                        ))
                        .build(),
                )
                .build(),
        )
        .build()
}

async fn create_machine(
    client: &aws_sdk_sfn::Client,
    name: &str,
    machine_type: aws_sdk_sfn::types::StateMachineType,
    definition: Value,
    logging: LoggingConfiguration,
) -> String {
    client
        .create_state_machine()
        .name(name)
        .r#type(machine_type)
        .role_arn(ROLE)
        .definition(definition.to_string())
        .logging_configuration(logging)
        .send()
        .await
        .unwrap()
        .state_machine_arn()
        .to_string()
}

async fn wait_done(client: &aws_sdk_sfn::Client, arn: &str) {
    for _ in 0..200 {
        let status = client
            .describe_execution()
            .execution_arn(arn)
            .send()
            .await
            .unwrap()
            .status()
            .as_str()
            .to_string();
        if status != "RUNNING" {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("{arn} did not finish");
}

/// Every `(stream, message)` in the group, waiting until at least `n` arrive.
async fn log_lines(
    logs: &aws_sdk_cloudwatchlogs::Client,
    group: &str,
    n: usize,
) -> Vec<(String, String)> {
    let mut lines = Vec::new();
    for _ in 0..100 {
        lines.clear();
        let streams = logs
            .describe_log_streams()
            .log_group_name(group)
            .send()
            .await
            .unwrap();
        for stream in streams.log_streams() {
            let name = stream.log_stream_name().unwrap().to_string();
            let events = logs
                .get_log_events()
                .log_group_name(group)
                .log_stream_name(&name)
                .start_from_head(true)
                .send()
                .await
                .unwrap();
            for event in events.events() {
                lines.push((name.clone(), event.message().unwrap().to_string()));
            }
        }
        if lines.len() >= n {
            return lines;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("only {} log lines in {group}: {lines:?}", lines.len());
}

// A STANDARD execution logs every event to its hourly stream, its fields in
// the recorded order and its data included.
#[tokio::test]
async fn a_standard_execution_logs_every_event_as_recorded() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("/sfn/standard")
        .send()
        .await
        .unwrap();
    let machine = create_machine(
        &client,
        "logged-standard",
        aws_sdk_sfn::types::StateMachineType::Standard,
        json!({"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}),
        logging("/sfn/standard", LogLevel::All, true),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .name("run")
        .input(r#"{"k":1}"#)
        .send()
        .await
        .unwrap();
    wait_done(&client, started.execution_arn()).await;

    let lines = log_lines(&logs, "/sfn/standard", 4).await;
    let types: Vec<String> = lines
        .iter()
        .map(|(_, m)| {
            serde_json::from_str::<Value>(m).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        types,
        [
            "ExecutionStarted",
            "PassStateEntered",
            "PassStateExited",
            "ExecutionSucceeded"
        ]
    );
    let (stream, first) = &lines[0];
    let hour = regex_free_hour(stream);
    assert_eq!(stream, &format!("states/logged-standard/{hour}/00000000"));
    assert_eq!(hour.split('-').count(), 4, "{stream}");
    let event: Value = serde_json::from_str(first).unwrap();
    assert_eq!(event["details"]["input"], r#"{"k":1}"#);
    assert_eq!(
        event["details"]["inputDetails"],
        json!({"truncated": false})
    );
    assert_eq!(event["details"]["roleArn"], ROLE);
    assert_eq!(event["execution_arn"], started.execution_arn());
    assert!(
        first.starts_with(
            r#"{"details":{"input":"{\"k\":1}","inputDetails":{"truncated":false},"roleArn":"#
        ),
        "{first}"
    );
    assert!(
        first.contains(r#","redrive_count":"0","id":"1","type":"ExecutionStarted","previous_event_id":"0","event_timestamp":""#),
        "{first}"
    );
    assert!(event["event_timestamp"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .is_ok());
}

/// The `<date>` of `states/<machine>/<date>/00000000`.
fn regex_free_hour(stream: &str) -> String {
    stream.split('/').nth(2).unwrap().to_string()
}

// At ERROR, an EXPRESS execution logs its failure only, by the minute, and
// without its data unless asked.
#[tokio::test]
async fn an_express_execution_logs_its_failure_at_error() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("/sfn/express")
        .send()
        .await
        .unwrap();
    let machine = create_machine(
        &client,
        "logged-express",
        aws_sdk_sfn::types::StateMachineType::Express,
        json!({"StartAt":"P","States":{
            "P":{"Type":"Pass","Next":"F"},
            "F":{"Type":"Fail","Error":"Boom","Cause":"why"}
        }}),
        logging("/sfn/express", LogLevel::Error, false),
    )
    .await;
    client
        .start_execution()
        .state_machine_arn(&machine)
        .input(r#"{"k":1}"#)
        .send()
        .await
        .unwrap();
    let lines = log_lines(&logs, "/sfn/express", 2).await;
    let events: Vec<Value> = lines
        .iter()
        .map(|(_, m)| serde_json::from_str(m).unwrap())
        .collect();
    assert_eq!(events.len(), 2, "{lines:?}");
    assert_eq!(events[0]["type"], "FailStateEntered");
    assert_eq!(events[0]["details"], json!({"name": "F"}));
    assert_eq!(events[1]["type"], "ExecutionFailed");
    assert_eq!(
        events[1]["details"],
        json!({"cause": "why", "error": "Boom"})
    );
    let minute = regex_free_hour(&lines[0].0);
    assert_eq!(minute.split('-').count(), 5, "{}", lines[0].0);
}

// A map run's children log under the parent's machine, their first and last
// events naming the map run and the parent.
#[tokio::test]
async fn map_run_children_log_under_the_parent_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("/sfn/dmap")
        .send()
        .await
        .unwrap();
    let machine = create_machine(
        &client,
        "logged-dmap",
        aws_sdk_sfn::types::StateMachineType::Standard,
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map","Label":"lbl",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},
                "StartAt":"P","States":{"P":{"Type":"Pass","End":true}}
            },
            "End":true
        }}}),
        logging("/sfn/dmap", LogLevel::All, true),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(r#"["a"]"#)
        .send()
        .await
        .unwrap();
    wait_done(&client, started.execution_arn()).await;
    let run_arn = client
        .list_map_runs()
        .execution_arn(started.execution_arn())
        .send()
        .await
        .unwrap()
        .map_runs()[0]
        .map_run_arn()
        .to_string();

    // The parent's 7 events and the child's 4.
    let lines = log_lines(&logs, "/sfn/dmap", 11).await;
    let child: Vec<(String, Value)> = lines
        .iter()
        .map(|(s, m)| (s.clone(), serde_json::from_str::<Value>(m).unwrap()))
        .filter(|(_, e)| e["execution_arn"] != started.execution_arn())
        .collect();
    assert_eq!(child.len(), 4, "{lines:?}");
    for (stream, _) in &child {
        assert!(stream.starts_with("states/logged-dmap/"), "{stream}");
        assert_eq!(regex_free_hour(stream).split('-').count(), 5, "{stream}");
    }
    for (index, (_, event)) in child.iter().enumerate() {
        let bookend = index == 0 || index == child.len() - 1;
        assert_eq!(
            event.get("map_run_arn").and_then(Value::as_str),
            bookend.then_some(run_arn.as_str()),
            "{event}"
        );
        assert_eq!(
            event.get("parent_execution_arn").and_then(Value::as_str),
            bookend.then_some(started.execution_arn()),
            "{event}"
        );
    }
    let map_run_started = lines
        .iter()
        .map(|(_, m)| serde_json::from_str::<Value>(m).unwrap())
        .find(|e| e["type"] == "MapRunStarted")
        .unwrap();
    assert_eq!(map_run_started["details"], json!({}));
}

// Stopping the parent logs its abort and the abort of each map run child it
// stopped, the child's naming its map run and parent.
#[tokio::test]
async fn a_stopped_map_run_logs_its_aborted_children() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("/sfn/stopped")
        .send()
        .await
        .unwrap();
    let machine = create_machine(
        &client,
        "logged-stopped",
        aws_sdk_sfn::types::StateMachineType::Standard,
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map","Label":"lbl",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"W","States":{"W":{"Type":"Wait","Seconds":60,"End":true}}
            },
            "End":true
        }}}),
        logging("/sfn/stopped", LogLevel::Error, false),
    )
    .await;
    let started = client
        .start_execution()
        .state_machine_arn(&machine)
        .input(r#"["a"]"#)
        .send()
        .await
        .unwrap();
    let parent = started.execution_arn();
    let mut run_arn = None;
    for _ in 0..100 {
        let runs = client
            .list_map_runs()
            .execution_arn(parent)
            .send()
            .await
            .unwrap();
        if let Some(run) = runs.map_runs().first() {
            let children = client
                .list_executions()
                .map_run_arn(run.map_run_arn())
                .send()
                .await
                .unwrap();
            if !children.executions().is_empty() {
                run_arn = Some(run.map_run_arn().to_string());
                break;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    let run_arn = run_arn.expect("the map run started no child");
    client
        .stop_execution()
        .execution_arn(parent)
        .error("Halt")
        .cause("enough")
        .send()
        .await
        .unwrap();

    let lines = log_lines(&logs, "/sfn/stopped", 2).await;
    let events: Vec<Value> = lines
        .iter()
        .map(|(_, m)| serde_json::from_str(m).unwrap())
        .collect();
    assert_eq!(events.len(), 2, "{lines:?}");
    assert!(
        events.iter().all(|e| e["type"] == "ExecutionAborted"),
        "{lines:?}"
    );
    let parent_event = events
        .iter()
        .find(|e| e["execution_arn"] == parent)
        .unwrap();
    assert_eq!(
        parent_event["details"],
        json!({"cause": "enough", "error": "Halt"})
    );
    let child = events
        .iter()
        .find(|e| e["execution_arn"] != parent)
        .unwrap();
    assert_eq!(child["map_run_arn"], run_arn.as_str());
    assert_eq!(child["parent_execution_arn"], parent);
    assert_eq!(
        child["details"]["cause"],
        format!("The map run execution [{run_arn}] which was managing this resource was aborted")
    );
}

// A logging configuration naming a log group that doesn't exist is refused, as
// recorded against AWS.
#[tokio::test]
async fn a_missing_log_group_is_refused() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let definition = json!({"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}).to_string();
    let expected = (
        Some("InvalidLoggingConfiguration"),
        Some("Invalid Logging Configuration: Log Destination not found."),
    );
    let err = client
        .create_state_machine()
        .name("logs-nowhere")
        .role_arn(ROLE)
        .definition(&definition)
        .logging_configuration(logging("/sfn/nope", LogLevel::All, false))
        .send()
        .await
        .unwrap_err();
    assert_eq!((err.code(), err.message()), expected);

    let machine = client
        .create_state_machine()
        .name("logs-later")
        .role_arn(ROLE)
        .definition(&definition)
        .send()
        .await
        .unwrap()
        .state_machine_arn()
        .to_string();
    let err = client
        .update_state_machine()
        .state_machine_arn(&machine)
        .logging_configuration(logging("/sfn/nope", LogLevel::Error, false))
        .send()
        .await
        .unwrap_err();
    assert_eq!((err.code(), err.message()), expected);
    // Logging off names nothing to check.
    client
        .update_state_machine()
        .state_machine_arn(&machine)
        .logging_configuration(logging("/sfn/nope", LogLevel::Off, false))
        .send()
        .await
        .unwrap();
}

// An HTTP API stage's access logs reach its CloudWatch Logs group.
#[tokio::test]
async fn http_api_access_logs_reach_cloudwatch_logs() {
    let server = TestServer::start().await;
    let apigw = server.apigatewayv2_client().await;
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("/apigw/access")
        .send()
        .await
        .unwrap();
    let api_id = apigw
        .create_api()
        .name("logged")
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::Http)
        .send()
        .await
        .unwrap()
        .api_id
        .unwrap();
    let integration_id = apigw
        .create_integration()
        .api_id(&api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::Mock)
        .send()
        .await
        .unwrap()
        .integration_id
        .unwrap();
    apigw
        .create_route()
        .api_id(&api_id)
        .route_key("GET /hello")
        .target(format!("integrations/{integration_id}"))
        .send()
        .await
        .unwrap();
    apigw
        .create_stage()
        .api_id(&api_id)
        .stage_name("prod")
        .auto_deploy(true)
        .access_log_settings(
            aws_sdk_apigatewayv2::types::AccessLogSettings::builder()
                .destination_arn("arn:aws:logs:us-east-1:123456789012:log-group:/apigw/access")
                .format("$context.httpMethod $context.routeKey $context.status")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let status = reqwest::Client::new()
        .get(format!(
            "{}/_aws/execute-api/{api_id}/prod/hello",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 200);
    let lines = log_lines(&logs, "/apigw/access", 1).await;
    assert_eq!(lines[0].1, "GET GET /hello 200", "{lines:?}");
}
