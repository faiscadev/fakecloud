//! CloudFormation provisions an AWS::Pipes::Pipe as a real record in the
//! `pipes` service control plane: it reads back through DescribePipe as RUNNING,
//! exposes its ARN via GetAtt, and is actually executed by the Pipes runner
//! (a source message reaches the target). Deleting the stack removes the pipe.

mod helpers;

use helpers::TestServer;

// Two SQS queues and a pipe wiring source -> target. The pipe's Source/Target
// resolve to the queues' ARNs via GetAtt; the stack output surfaces the pipe
// ARN so the test can assert GetAtt works.
const TEMPLATE: &str = r#"{
  "Resources": {
    "SrcQ": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "cfn-pipe-src" } },
    "TgtQ": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "cfn-pipe-tgt" } },
    "Pipe": {
      "Type": "AWS::Pipes::Pipe",
      "Properties": {
        "Name": "cfn-pipe",
        "RoleArn": "arn:aws:iam::000000000000:role/pipe-role",
        "Source": { "Fn::GetAtt": ["SrcQ", "Arn"] },
        "Target": { "Fn::GetAtt": ["TgtQ", "Arn"] }
      }
    }
  },
  "Outputs": {
    "PipeArn": { "Value": { "Fn::GetAtt": ["Pipe", "Arn"] } }
  }
}"#;

async fn queue_url(sqs: &aws_sdk_sqs::Client, name: &str) -> String {
    sqs.get_queue_url()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn cfn_provisions_and_runs_pipe() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("pipe-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("pipe-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // GetAtt Arn surfaced as a stack output.
    let pipe_arn = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("PipeArn"))
        .and_then(|o| o.output_value())
        .expect("PipeArn output");
    assert!(pipe_arn.contains(":pipe/cfn-pipe"), "got {pipe_arn}");

    // The pipe exists in the pipes service and settled to RUNNING.
    let described_pipe = pipes
        .describe_pipe()
        .name("cfn-pipe")
        .send()
        .await
        .expect("DescribePipe");
    assert_eq!(
        described_pipe.current_state(),
        Some(&aws_sdk_pipes::types::PipeState::Running)
    );
    assert_eq!(described_pipe.arn(), Some(pipe_arn));

    // The provisioned pipe is actually executed: a source message reaches the
    // target queue.
    let src_url = queue_url(&sqs, "cfn-pipe-src").await;
    let tgt_url = queue_url(&sqs, "cfn-pipe-tgt").await;
    sqs.send_message()
        .queue_url(&src_url)
        .message_body("cfn-routed")
        .send()
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut delivered = None;
    while std::time::Instant::now() < deadline {
        let r = sqs
            .receive_message()
            .queue_url(&tgt_url)
            .wait_time_seconds(0)
            .send()
            .await
            .unwrap();
        if let Some(m) = r.messages().first() {
            delivered = m.body().map(str::to_string);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let body = delivered.expect("CFN-provisioned pipe delivered to target");
    let event: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(event["body"], "cfn-routed");

    // Deleting the stack removes the pipe.
    cfn.delete_stack()
        .stack_name("pipe-stack")
        .send()
        .await
        .unwrap();
    let gone = pipes.describe_pipe().name("cfn-pipe").send().await;
    assert!(gone.is_err(), "stack delete should remove the pipe");
}
