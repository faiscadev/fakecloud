use chrono::Utc;
use std::collections::HashMap;
use uuid::Uuid;

use fakecloud_dynamodb::state::{
    AttributeDefinition, DynamoTable, KeySchemaElement, ProvisionedThroughput, SharedDynamoDbState,
};
use fakecloud_eventbridge::state::{EventRule, SharedEventBridgeState};
use fakecloud_iam::state::{IamPolicy, IamRole, PolicyVersion, SharedIamState};
use fakecloud_logs::state::SharedLogsState;
use fakecloud_s3::state::{S3Bucket, SharedS3State};
use fakecloud_sns::state::{SharedSnsState, SnsSubscription, SnsTopic};
use fakecloud_sqs::state::{SharedSqsState, SqsQueue};
use fakecloud_ssm::state::{SharedSsmState, SsmParameter};

use crate::state::StackResource;
use crate::template::ResourceDefinition;

/// Holds references to all service states so CloudFormation can provision resources.
pub struct ResourceProvisioner {
    pub sqs_state: SharedSqsState,
    pub sns_state: SharedSnsState,
    pub ssm_state: SharedSsmState,
    pub iam_state: SharedIamState,
    pub s3_state: SharedS3State,
    pub eventbridge_state: SharedEventBridgeState,
    pub dynamodb_state: SharedDynamoDbState,
    pub logs_state: SharedLogsState,
    pub account_id: String,
    pub region: String,
}

impl ResourceProvisioner {
    /// Create a resource and return the StackResource with physical ID.
    pub fn create_resource(&self, resource: &ResourceDefinition) -> Result<StackResource, String> {
        let result = match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self.create_sqs_queue(resource),
            "AWS::SNS::Topic" => self.create_sns_topic(resource),
            "AWS::SNS::Subscription" => self.create_sns_subscription(resource),
            "AWS::SSM::Parameter" => self.create_ssm_parameter(resource),
            "AWS::IAM::Role" => self.create_iam_role(resource),
            "AWS::IAM::Policy" => self.create_iam_policy(resource),
            "AWS::S3::Bucket" => self.create_s3_bucket(resource),
            "AWS::Events::Rule" => self.create_eventbridge_rule(resource),
            "AWS::DynamoDB::Table" => self.create_dynamodb_table(resource),
            "AWS::Logs::LogGroup" => self.create_log_group(resource),
            other => Err(format!("Unsupported resource type: {other}")),
        };

        result.map(|physical_id| StackResource {
            logical_id: resource.logical_id.clone(),
            physical_id,
            resource_type: resource.resource_type.clone(),
            status: "CREATE_COMPLETE".to_string(),
        })
    }

    /// Delete a previously created resource.
    pub fn delete_resource(&self, resource: &StackResource) -> Result<(), String> {
        match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self.delete_sqs_queue(&resource.physical_id),
            "AWS::SNS::Topic" => self.delete_sns_topic(&resource.physical_id),
            "AWS::SNS::Subscription" => self.delete_sns_subscription(&resource.physical_id),
            "AWS::SSM::Parameter" => self.delete_ssm_parameter(&resource.physical_id),
            "AWS::IAM::Role" => self.delete_iam_role(&resource.physical_id),
            "AWS::IAM::Policy" => self.delete_iam_policy(&resource.physical_id),
            "AWS::S3::Bucket" => self.delete_s3_bucket(&resource.physical_id),
            "AWS::Events::Rule" => self.delete_eventbridge_rule(&resource.physical_id),
            "AWS::DynamoDB::Table" => self.delete_dynamodb_table(&resource.physical_id),
            "AWS::Logs::LogGroup" => self.delete_log_group(&resource.physical_id),
            other => Err(format!("Unsupported resource type: {other}")),
        }
    }

    // --- SQS ---

    fn create_sqs_queue(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let queue_name = props
            .get("QueueName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut state = self.sqs_state.write();
        let queue_url = format!("http://localhost:4566/{}/{}", state.account_id, queue_name);
        let arn = format!(
            "arn:aws:sqs:{}:{}:{}",
            state.region, state.account_id, queue_name
        );

        let is_fifo = queue_name.ends_with(".fifo");
        let mut attributes = HashMap::new();
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if k != "QueueName" {
                    if let Some(s) = v.as_str() {
                        attributes.insert(k.clone(), s.to_string());
                    } else if let Some(n) = v.as_i64() {
                        attributes.insert(k.clone(), n.to_string());
                    }
                }
            }
        }

        let queue = SqsQueue {
            queue_name: queue_name.to_string(),
            queue_url: queue_url.clone(),
            arn,
            created_at: Utc::now(),
            messages: std::collections::VecDeque::new(),
            inflight: Vec::new(),
            attributes,
            is_fifo,
            dedup_cache: HashMap::new(),
            redrive_policy: None,
            tags: HashMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: HashMap::new(),
        };

        state
            .name_to_url
            .insert(queue_name.to_string(), queue_url.clone());
        state.queues.insert(queue_url.clone(), queue);

        Ok(queue_url)
    }

    fn delete_sqs_queue(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.sqs_state.write();
        if let Some(queue) = state.queues.remove(physical_id) {
            state.name_to_url.remove(&queue.queue_name);
        }
        Ok(())
    }

    // --- SNS ---

    fn create_sns_topic(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let topic_name = props
            .get("TopicName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut state = self.sns_state.write();
        let topic_arn = format!(
            "arn:aws:sns:{}:{}:{}",
            state.region, state.account_id, topic_name
        );

        let topic = SnsTopic {
            topic_arn: topic_arn.clone(),
            name: topic_name.to_string(),
            attributes: HashMap::new(),
            tags: Vec::new(),
            is_fifo: topic_name.ends_with(".fifo"),
            created_at: Utc::now(),
        };

        state.topics.insert(topic_arn.clone(), topic);
        Ok(topic_arn)
    }

    fn delete_sns_topic(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.sns_state.write();
        state.topics.remove(physical_id);
        // Also remove subscriptions for this topic
        state
            .subscriptions
            .retain(|_, sub| sub.topic_arn != physical_id);
        Ok(())
    }

    // --- SNS Subscription ---

    fn create_sns_subscription(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let topic_arn = props
            .get("TopicArn")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires TopicArn")?;
        let protocol = props
            .get("Protocol")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Protocol")?;
        let endpoint = props
            .get("Endpoint")
            .and_then(|v| v.as_str())
            .ok_or("SNS Subscription requires Endpoint")?;

        let mut state = self.sns_state.write();

        // Validate that the topic exists
        if !state.topics.contains_key(topic_arn) {
            return Err(format!("Topic ARN does not exist: {topic_arn}"));
        }

        let sub_arn = format!("{}:{}", topic_arn, Uuid::new_v4());

        let subscription = SnsSubscription {
            subscription_arn: sub_arn.clone(),
            topic_arn: topic_arn.to_string(),
            protocol: protocol.to_string(),
            endpoint: endpoint.to_string(),
            owner: state.account_id.clone(),
            attributes: HashMap::new(),
            confirmed: true,
        };

        state.subscriptions.insert(sub_arn.clone(), subscription);
        Ok(sub_arn)
    }

    fn delete_sns_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.sns_state.write();
        state.subscriptions.remove(physical_id);
        Ok(())
    }

    // --- SSM ---

    fn create_ssm_parameter(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Name")?;
        let value = props
            .get("Value")
            .and_then(|v| v.as_str())
            .ok_or("SSM Parameter requires Value")?;
        let param_type = props
            .get("Type")
            .and_then(|v| v.as_str())
            .unwrap_or("String");

        let mut state = self.ssm_state.write();
        let arn = format!(
            "arn:aws:ssm:{}:{}:parameter{}",
            state.region,
            state.account_id,
            if name.starts_with('/') {
                name.to_string()
            } else {
                format!("/{name}")
            }
        );

        let parameter = SsmParameter {
            name: name.to_string(),
            value: value.to_string(),
            param_type: param_type.to_string(),
            version: 1,
            arn: arn.clone(),
            last_modified: Utc::now(),
            history: Vec::new(),
            tags: HashMap::new(),
            labels: HashMap::new(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            allowed_pattern: None,
            key_id: None,
            data_type: "text".to_string(),
            tier: "Standard".to_string(),
            policies: None,
        };

        state.parameters.insert(name.to_string(), parameter);
        Ok(name.to_string())
    }

    fn delete_ssm_parameter(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.ssm_state.write();
        state.parameters.remove(physical_id);
        Ok(())
    }

    // --- IAM Role ---

    fn create_iam_role(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let role_name = props
            .get("RoleName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let assume_role_policy = props
            .get("AssumeRolePolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut state = self.iam_state.write();
        let role_id = format!(
            "FKIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:role{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            role_name
        );

        let role = IamRole {
            role_name: role_name.to_string(),
            role_id,
            arn: arn.clone(),
            path: path.to_string(),
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };

        state.roles.insert(role_name.to_string(), role);
        Ok(arn)
    }

    fn delete_iam_role(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.iam_state.write();
        // physical_id is the ARN; find the role name
        let role_name = state
            .roles
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = role_name {
            state.roles.remove(&name);
            state.role_policies.remove(&name);
            state.role_inline_policies.remove(&name);
        }
        Ok(())
    }

    // --- IAM Policy ---

    fn create_iam_policy(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut state = self.iam_state.write();
        let policy_id = format!(
            "FSIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:policy{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            policy_name
        );

        let now = Utc::now();
        let policy = IamPolicy {
            policy_name: policy_name.to_string(),
            policy_id,
            arn: arn.clone(),
            path: path.to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            created_at: now,
            tags: Vec::new(),
            default_version_id: "v1".to_string(),
            versions: vec![PolicyVersion {
                version_id: "v1".to_string(),
                document: policy_document,
                is_default: true,
                created_at: now,
            }],
            next_version_num: 2,
            attachment_count: 0,
        };

        state.policies.insert(arn.clone(), policy);
        Ok(arn)
    }

    fn delete_iam_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.iam_state.write();
        state.policies.remove(physical_id);
        Ok(())
    }

    // --- S3 ---

    fn create_s3_bucket(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let bucket_name = props
            .get("BucketName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut state = self.s3_state.write();
        let bucket = S3Bucket::new(bucket_name, &state.region, &state.account_id);
        state.buckets.insert(bucket_name.to_string(), bucket);
        Ok(bucket_name.to_string())
    }

    fn delete_s3_bucket(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.s3_state.write();
        state.buckets.remove(physical_id);
        Ok(())
    }

    // --- EventBridge ---

    fn create_eventbridge_rule(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let rule_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);
        let event_bus_name = props
            .get("EventBusName")
            .and_then(|v| v.as_str())
            .unwrap_or("default");

        let mut state = self.eventbridge_state.write();

        // Validate that the event bus exists
        if !state.buses.contains_key(event_bus_name) {
            return Err(format!("Event bus does not exist: {event_bus_name}"));
        }

        let arn = if event_bus_name == "default" {
            format!(
                "arn:aws:events:{}:{}:rule/{}",
                state.region, state.account_id, rule_name
            )
        } else {
            format!(
                "arn:aws:events:{}:{}:rule/{}/{}",
                state.region, state.account_id, event_bus_name, rule_name
            )
        };

        let rule = EventRule {
            name: rule_name.to_string(),
            arn: arn.clone(),
            event_bus_name: event_bus_name.to_string(),
            event_pattern: props.get("EventPattern").map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            }),
            schedule_expression: props
                .get("ScheduleExpression")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            state: props
                .get("State")
                .and_then(|v| v.as_str())
                .unwrap_or("ENABLED")
                .to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            role_arn: props
                .get("RoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            managed_by: None,
            created_by: None,
            targets: Vec::new(),
            tags: HashMap::new(),
            last_fired: None,
        };

        state
            .rules
            .insert((event_bus_name.to_string(), rule_name.to_string()), rule);
        Ok(arn)
    }

    fn delete_eventbridge_rule(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.eventbridge_state.write();
        // physical_id is the ARN; find the rule key
        let key = state
            .rules
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(k, _)| k.clone());
        if let Some(k) = key {
            state.rules.remove(&k);
        }
        Ok(())
    }

    // --- DynamoDB ---

    fn create_dynamodb_table(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let table_name = props
            .get("TableName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let mut key_schema = Vec::new();
        if let Some(ks) = props.get("KeySchema").and_then(|v| v.as_array()) {
            for item in ks {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let key_type = item
                    .get("KeyType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("HASH")
                    .to_string();
                key_schema.push(KeySchemaElement {
                    attribute_name: attr_name,
                    key_type,
                });
            }
        }

        let mut attribute_definitions = Vec::new();
        if let Some(defs) = props.get("AttributeDefinitions").and_then(|v| v.as_array()) {
            for item in defs {
                let attr_name = item
                    .get("AttributeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let attr_type = item
                    .get("AttributeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("S")
                    .to_string();
                attribute_definitions.push(AttributeDefinition {
                    attribute_name: attr_name,
                    attribute_type: attr_type,
                });
            }
        }

        let billing_mode = props
            .get("BillingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("PAY_PER_REQUEST")
            .to_string();

        let provisioned_throughput = if billing_mode == "PROVISIONED" {
            if let Some(pt) = props.get("ProvisionedThroughput") {
                ProvisionedThroughput {
                    read_capacity_units: pt
                        .get("ReadCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                    write_capacity_units: pt
                        .get("WriteCapacityUnits")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(5),
                }
            } else {
                ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }
            }
        } else {
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }
        };

        let mut state = self.dynamodb_state.write();
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, table_name
        );

        let table = DynamoTable {
            name: table_name.to_string(),
            arn: arn.clone(),
            key_schema,
            attribute_definitions,
            provisioned_throughput,
            items: Vec::new(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: HashMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode,
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
        };

        state.tables.insert(table_name.to_string(), table);
        Ok(arn)
    }

    fn delete_dynamodb_table(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.dynamodb_state.write();
        // physical_id is the ARN; find the table name
        let table_name = state
            .tables
            .iter()
            .find(|(_, t)| t.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = table_name {
            state.tables.remove(&name);
        }
        Ok(())
    }

    // --- CloudWatch Logs ---

    fn create_log_group(&self, resource: &ResourceDefinition) -> Result<String, String> {
        let props = &resource.properties;
        let log_group_name = props
            .get("LogGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let retention_in_days = props
            .get("RetentionInDays")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let mut state = self.logs_state.write();
        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:*",
            state.region, state.account_id, log_group_name
        );

        let log_group = fakecloud_logs::state::LogGroup {
            name: log_group_name.to_string(),
            arn: arn.clone(),
            creation_time: Utc::now().timestamp_millis(),
            retention_in_days,
            kms_key_id: None,
            stored_bytes: 0,
            log_streams: HashMap::new(),
            tags: HashMap::new(),
            subscription_filters: Vec::new(),
        };

        state
            .log_groups
            .insert(log_group_name.to_string(), log_group);
        Ok(arn)
    }

    fn delete_log_group(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.logs_state.write();
        // physical_id is the ARN; find the log group name
        let name = state
            .log_groups
            .iter()
            .find(|(_, g)| g.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = name {
            state.log_groups.remove(&name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_provisioner() -> ResourceProvisioner {
        ResourceProvisioner {
            sqs_state: Arc::new(RwLock::new(fakecloud_sqs::state::SqsState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ))),
            sns_state: Arc::new(RwLock::new(fakecloud_sns::state::SnsState::new(
                "123456789012",
                "us-east-1",
            ))),
            ssm_state: Arc::new(RwLock::new(fakecloud_ssm::state::SsmState::new(
                "123456789012",
                "us-east-1",
            ))),
            iam_state: Arc::new(RwLock::new(fakecloud_iam::state::IamState::new(
                "123456789012",
            ))),
            s3_state: Arc::new(RwLock::new(fakecloud_s3::state::S3State::new(
                "123456789012",
                "us-east-1",
            ))),
            eventbridge_state: Arc::new(RwLock::new(
                fakecloud_eventbridge::state::EventBridgeState::new("123456789012", "us-east-1"),
            )),
            dynamodb_state: Arc::new(RwLock::new(fakecloud_dynamodb::state::DynamoDbState::new(
                "123456789012",
                "us-east-1",
            ))),
            logs_state: Arc::new(RwLock::new(fakecloud_logs::state::LogsState::new(
                "123456789012",
                "us-east-1",
            ))),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    fn make_resource(
        resource_type: &str,
        logical_id: &str,
        props: serde_json::Value,
    ) -> ResourceDefinition {
        ResourceDefinition {
            logical_id: logical_id.to_string(),
            resource_type: resource_type.to_string(),
            properties: props,
        }
    }

    #[test]
    fn sns_subscription_rejects_nonexistent_topic() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::SNS::Subscription",
            "MySub",
            serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:NonExistent",
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn sns_subscription_succeeds_when_topic_exists() {
        let prov = make_provisioner();
        // First create the topic
        let topic = make_resource(
            "AWS::SNS::Topic",
            "MyTopic",
            serde_json::json!({ "TopicName": "my-topic" }),
        );
        let topic_result = prov.create_resource(&topic);
        assert!(topic_result.is_ok());
        let topic_arn = topic_result.unwrap().physical_id;

        // Now create subscription referencing that topic
        let sub = make_resource(
            "AWS::SNS::Subscription",
            "MySub",
            serde_json::json!({
                "TopicArn": topic_arn,
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:123456789012:my-queue"
            }),
        );
        let result = prov.create_resource(&sub);
        assert!(result.is_ok());
    }

    #[test]
    fn eventbridge_rule_arn_default_bus_omits_bus_name() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource).unwrap();
        // For default bus, ARN should be rule/<name> without /default/
        assert_eq!(
            result.physical_id,
            "arn:aws:events:us-east-1:123456789012:rule/my-rule"
        );
        assert!(!result.physical_id.contains("rule/default/"));
    }

    #[test]
    fn eventbridge_rule_arn_custom_bus_includes_bus_name() {
        let prov = make_provisioner();
        // Create a custom bus first
        {
            let mut state = prov.eventbridge_state.write();
            state.buses.insert(
                "custom-bus".to_string(),
                fakecloud_eventbridge::state::EventBus {
                    name: "custom-bus".to_string(),
                    arn: "arn:aws:events:us-east-1:123456789012:event-bus/custom-bus".to_string(),
                    policy: None,
                    creation_time: Utc::now(),
                    last_modified_time: Utc::now(),
                    description: None,
                    kms_key_identifier: None,
                    dead_letter_config: None,
                    tags: HashMap::new(),
                },
            );
        }
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "EventBusName": "custom-bus",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource).unwrap();
        assert_eq!(
            result.physical_id,
            "arn:aws:events:us-east-1:123456789012:rule/custom-bus/my-rule"
        );
    }

    #[test]
    fn eventbridge_rule_rejects_nonexistent_bus() {
        let prov = make_provisioner();
        let resource = make_resource(
            "AWS::Events::Rule",
            "MyRule",
            serde_json::json!({
                "Name": "my-rule",
                "EventBusName": "nonexistent-bus",
                "ScheduleExpression": "rate(1 hour)"
            }),
        );
        let result = prov.create_resource(&resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }
}
