use std::collections::HashMap;
use std::sync::Arc;

/// Cross-service message delivery.
///
/// Services use this to deliver messages to other services without
/// direct dependencies between service crates. The server wires up
/// the delivery functions at startup.
pub struct DeliveryBus {
    /// Deliver a message to an SQS queue by ARN.
    sqs_sender: Option<Arc<dyn SqsDelivery>>,
    /// Publish a message to an SNS topic by ARN.
    sns_sender: Option<Arc<dyn SnsDelivery>>,
    /// Put an event onto an EventBridge bus.
    eventbridge_sender: Option<Arc<dyn EventBridgeDelivery>>,
    /// Invoke a Lambda function by ARN.
    lambda_invoker: Option<Arc<dyn LambdaDelivery>>,
    /// Put records to a Kinesis Data Stream by ARN.
    kinesis_sender: Option<Arc<dyn KinesisDelivery>>,
    /// Start Step Functions executions.
    stepfunctions_starter: Option<Arc<dyn StepFunctionsDelivery>>,
}

/// Message attribute for SQS delivery from SNS.
#[derive(Debug, Clone)]
pub struct SqsMessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<String>,
}

/// Trait for delivering messages to SQS queues.
pub trait SqsDelivery: Send + Sync {
    fn deliver_to_queue(
        &self,
        queue_arn: &str,
        message_body: &str,
        attributes: &HashMap<String, String>,
    );

    /// Deliver with message attributes and FIFO fields
    fn deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        // Default implementation: fall back to simple delivery
        let _ = (message_attributes, message_group_id, message_dedup_id);
        self.deliver_to_queue(queue_arn, message_body, &HashMap::new());
    }
}

/// Trait for publishing messages to SNS topics.
pub trait SnsDelivery: Send + Sync {
    fn publish_to_topic(&self, topic_arn: &str, message: &str, subject: Option<&str>);
}

/// Trait for putting events onto an EventBridge bus from cross-service integrations.
pub trait EventBridgeDelivery: Send + Sync {
    /// Put an event onto the specified event bus.
    /// The implementation should handle rule matching and target delivery.
    fn put_event(&self, source: &str, detail_type: &str, detail: &str, event_bus_name: &str);
}

/// Trait for invoking Lambda functions from cross-service integrations.
pub trait LambdaDelivery: Send + Sync {
    /// Invoke a Lambda function with the given payload.
    /// The function is identified by ARN. Returns the response bytes on success.
    fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>;
}

/// Trait for putting records to Kinesis Data Streams.
pub trait KinesisDelivery: Send + Sync {
    /// Put a record to a Kinesis stream identified by ARN.
    /// The data should be base64-encoded. partition_key is used for shard distribution.
    fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str);
}

/// Trait for starting Step Functions executions from cross-service integrations.
pub trait StepFunctionsDelivery: Send + Sync {
    /// Start a state machine execution with the given input.
    /// The state machine is identified by ARN.
    fn start_execution(&self, state_machine_arn: &str, input: &str);
}

impl DeliveryBus {
    pub fn new() -> Self {
        Self {
            sqs_sender: None,
            sns_sender: None,
            eventbridge_sender: None,
            lambda_invoker: None,
            kinesis_sender: None,
            stepfunctions_starter: None,
        }
    }

    pub fn with_sqs(mut self, sender: Arc<dyn SqsDelivery>) -> Self {
        self.sqs_sender = Some(sender);
        self
    }

    pub fn with_sns(mut self, sender: Arc<dyn SnsDelivery>) -> Self {
        self.sns_sender = Some(sender);
        self
    }

    pub fn with_eventbridge(mut self, sender: Arc<dyn EventBridgeDelivery>) -> Self {
        self.eventbridge_sender = Some(sender);
        self
    }

    pub fn with_lambda(mut self, invoker: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_invoker = Some(invoker);
        self
    }

    pub fn with_kinesis(mut self, sender: Arc<dyn KinesisDelivery>) -> Self {
        self.kinesis_sender = Some(sender);
        self
    }

    pub fn with_stepfunctions(mut self, starter: Arc<dyn StepFunctionsDelivery>) -> Self {
        self.stepfunctions_starter = Some(starter);
        self
    }

    /// Send a message to an SQS queue identified by ARN.
    pub fn send_to_sqs(
        &self,
        queue_arn: &str,
        message_body: &str,
        attributes: &HashMap<String, String>,
    ) {
        if let Some(ref sender) = self.sqs_sender {
            sender.deliver_to_queue(queue_arn, message_body, attributes);
        }
    }

    /// Send a message to an SQS queue with message attributes and FIFO fields.
    pub fn send_to_sqs_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        if let Some(ref sender) = self.sqs_sender {
            sender.deliver_to_queue_with_attrs(
                queue_arn,
                message_body,
                message_attributes,
                message_group_id,
                message_dedup_id,
            );
        }
    }

    /// Publish a message to an SNS topic identified by ARN.
    pub fn publish_to_sns(&self, topic_arn: &str, message: &str, subject: Option<&str>) {
        if let Some(ref sender) = self.sns_sender {
            sender.publish_to_topic(topic_arn, message, subject);
        }
    }

    /// Put an event onto an EventBridge bus.
    pub fn put_event_to_eventbridge(
        &self,
        source: &str,
        detail_type: &str,
        detail: &str,
        event_bus_name: &str,
    ) {
        if let Some(ref sender) = self.eventbridge_sender {
            sender.put_event(source, detail_type, detail, event_bus_name);
        }
    }

    /// Invoke a Lambda function identified by ARN.
    pub async fn invoke_lambda(
        &self,
        function_arn: &str,
        payload: &str,
    ) -> Option<Result<Vec<u8>, String>> {
        if let Some(ref invoker) = self.lambda_invoker {
            Some(invoker.invoke_lambda(function_arn, payload).await)
        } else {
            None
        }
    }

    /// Put a record to a Kinesis stream identified by ARN.
    pub fn send_to_kinesis(&self, stream_arn: &str, data: &str, partition_key: &str) {
        if let Some(ref sender) = self.kinesis_sender {
            sender.put_record(stream_arn, data, partition_key);
        }
    }

    /// Start a Step Functions execution identified by state machine ARN.
    pub fn start_stepfunctions_execution(&self, state_machine_arn: &str, input: &str) {
        if let Some(ref starter) = self.stepfunctions_starter {
            starter.start_execution(state_machine_arn, input);
        }
    }
}

impl Default for DeliveryBus {
    fn default() -> Self {
        Self::new()
    }
}
