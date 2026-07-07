//! The AWS-provided static FIS catalogs: the predefined `actions`
//! (`ListActions` / `GetAction`) and `targetResourceTypes`
//! (`ListTargetResourceTypes` / `GetTargetResourceType`).
//!
//! These are AWS-owned reference data (not per-account, not mutable), so they
//! live as a fixed table rather than in account state. The action ARN carries an
//! empty account field (`arn:aws:fis:{region}::action/{id}`), matching AWS.

use serde_json::{json, Map, Value};

use crate::shared::action_arn;

/// One catalog action: id, description, its parameters (name, description,
/// required), and its target roles (role name -> resource type).
struct ActionDef {
    id: &'static str,
    description: &'static str,
    /// (parameter name, description, required)
    parameters: &'static [(&'static str, &'static str, bool)],
    /// (target role name, target resource type)
    targets: &'static [(&'static str, &'static str)],
}

/// The predefined AWS FIS action catalog. A faithful, representative slice of
/// AWS's action library across EC2 / ECS / EKS / RDS / SSM / Lambda / network /
/// CloudWatch / DynamoDB / the FIS API-fault actions.
const ACTIONS: &[ActionDef] = &[
    ActionDef {
        id: "aws:ec2:reboot-instances",
        description: "Reboot the specified Amazon EC2 instances.",
        parameters: &[],
        targets: &[("Instances", "aws:ec2:instance")],
    },
    ActionDef {
        id: "aws:ec2:stop-instances",
        description: "Stop the specified Amazon EC2 instances.",
        parameters: &[
            (
                "startInstancesAfterDuration",
                "The time to wait before restarting the instances (ISO 8601 duration).",
                false,
            ),
            (
                "completeIfInstancesTerminated",
                "Complete the action if the target instances are terminated.",
                false,
            ),
        ],
        targets: &[("Instances", "aws:ec2:instance")],
    },
    ActionDef {
        id: "aws:ec2:terminate-instances",
        description: "Terminate the specified Amazon EC2 instances.",
        parameters: &[],
        targets: &[("Instances", "aws:ec2:instance")],
    },
    ActionDef {
        id: "aws:ec2:send-spot-instance-interruptions",
        description: "Interrupt the specified Amazon EC2 Spot Instances.",
        parameters: &[(
            "durationBeforeInterruption",
            "The time to wait before the interruption (ISO 8601 duration).",
            false,
        )],
        targets: &[("SpotInstances", "aws:ec2:spot-instance")],
    },
    ActionDef {
        id: "aws:ec2:asg-insufficient-instance-capacity-error",
        description: "Inject an insufficient instance capacity error on the target Auto Scaling groups.",
        parameters: &[
            (
                "availabilityZoneIdentifiers",
                "The Availability Zones for the error.",
                true,
            ),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("percentage", "The percentage of calls to fail.", false),
        ],
        targets: &[("AutoScalingGroups", "aws:ec2:autoscaling-group")],
    },
    ActionDef {
        id: "aws:ecs:drain-container-instances",
        description: "Drain the specified percentage of Amazon ECS container instances.",
        parameters: &[
            (
                "drainagePercentage",
                "The percentage of container instances to drain.",
                true,
            ),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("ClusterContainerInstances", "aws:ecs:cluster")],
    },
    ActionDef {
        id: "aws:ecs:stop-task",
        description: "Stop the specified Amazon ECS tasks.",
        parameters: &[],
        targets: &[("Tasks", "aws:ecs:task")],
    },
    ActionDef {
        id: "aws:ecs:task-cpu-stress",
        description: "Run CPU stress on the specified Amazon ECS tasks.",
        parameters: &[
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("percent", "The percentage of CPU to stress.", false),
            ("workers", "The number of stress workers.", false),
            (
                "installDependencies",
                "Install required dependencies onto the task.",
                false,
            ),
        ],
        targets: &[("Tasks", "aws:ecs:task")],
    },
    ActionDef {
        id: "aws:eks:terminate-nodegroup-instances",
        description: "Terminate the specified percentage of Amazon EKS node group instances.",
        parameters: &[(
            "instanceTerminationPercentage",
            "The percentage of instances to terminate.",
            true,
        )],
        targets: &[("Nodegroups", "aws:eks:nodegroup")],
    },
    ActionDef {
        id: "aws:eks:pod-delete",
        description: "Delete the specified Amazon EKS pods.",
        parameters: &[
            ("kubernetesServiceAccount", "The Kubernetes service account.", true),
            ("gracePeriodSeconds", "The grace period before deletion.", false),
        ],
        targets: &[("Pods", "aws:eks:pod")],
    },
    ActionDef {
        id: "aws:eks:pod-cpu-stress",
        description: "Run CPU stress on the specified Amazon EKS pods.",
        parameters: &[
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("kubernetesServiceAccount", "The Kubernetes service account.", true),
            ("percent", "The percentage of CPU to stress.", false),
        ],
        targets: &[("Pods", "aws:eks:pod")],
    },
    ActionDef {
        id: "aws:rds:reboot-db-instances",
        description: "Reboot the specified Amazon RDS DB instances.",
        parameters: &[(
            "forceFailover",
            "Force a failover during the reboot.",
            false,
        )],
        targets: &[("DBInstances", "aws:rds:db-instance")],
    },
    ActionDef {
        id: "aws:rds:failover-db-cluster",
        description: "Fail over the specified Amazon RDS DB clusters.",
        parameters: &[],
        targets: &[("Clusters", "aws:rds:cluster")],
    },
    ActionDef {
        id: "aws:ssm:send-command",
        description: "Run the specified SSM document on the target instances.",
        parameters: &[
            ("documentArn", "The Amazon Resource Name (ARN) of the document.", true),
            ("documentParameters", "The parameters for the document.", false),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            (
                "documentVersion",
                "The version of the document to run.",
                false,
            ),
        ],
        targets: &[("Instances", "aws:ssm:managed-instance")],
    },
    ActionDef {
        id: "aws:ssm:start-automation-execution",
        description: "Start the specified SSM automation execution.",
        parameters: &[
            ("documentArn", "The Amazon Resource Name (ARN) of the document.", true),
            ("documentParameters", "The parameters for the automation.", false),
            (
                "maxDuration",
                "The maximum duration of the automation (ISO 8601 duration).",
                false,
            ),
        ],
        targets: &[],
    },
    ActionDef {
        id: "aws:fis:inject-api-internal-error",
        description: "Inject an internal error into the target AWS API calls made with the specified IAM role.",
        parameters: &[
            ("service", "The AWS service namespace.", true),
            ("operations", "The API operations to affect.", true),
            ("percentage", "The percentage of calls to affect.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("Roles", "aws:iam:role")],
    },
    ActionDef {
        id: "aws:fis:inject-api-throttle-error",
        description: "Inject a throttling error into the target AWS API calls made with the specified IAM role.",
        parameters: &[
            ("service", "The AWS service namespace.", true),
            ("operations", "The API operations to affect.", true),
            ("percentage", "The percentage of calls to affect.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("Roles", "aws:iam:role")],
    },
    ActionDef {
        id: "aws:fis:inject-api-unavailable-error",
        description: "Inject an unavailable error into the target AWS API calls made with the specified IAM role.",
        parameters: &[
            ("service", "The AWS service namespace.", true),
            ("operations", "The API operations to affect.", true),
            ("percentage", "The percentage of calls to affect.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("Roles", "aws:iam:role")],
    },
    ActionDef {
        id: "aws:fis:wait",
        description: "Wait for the specified duration.",
        parameters: &[(
            "duration",
            "The duration to wait (ISO 8601 duration).",
            true,
        )],
        targets: &[],
    },
    ActionDef {
        id: "aws:network:disrupt-connectivity",
        description: "Disrupt network connectivity to the target subnets.",
        parameters: &[
            ("scope", "The connectivity scope to disrupt.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("Subnets", "aws:ec2:subnet")],
    },
    ActionDef {
        id: "aws:network:route-table-disrupt-connectivity",
        description: "Disrupt connectivity by modifying the target subnet route tables.",
        parameters: &[
            ("service", "The AWS service to disrupt connectivity to.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[("Subnets", "aws:ec2:subnet")],
    },
    ActionDef {
        id: "aws:cloudwatch:assert-alarm-state",
        description: "Assert that the specified CloudWatch alarms are in one of the specified states.",
        parameters: &[
            ("alarmArns", "The Amazon Resource Names (ARNs) of the alarms.", true),
            ("alarmStates", "The expected alarm states.", true),
        ],
        targets: &[],
    },
    ActionDef {
        id: "aws:dynamodb:global-table-pause-replication",
        description: "Pause replication for the target Amazon DynamoDB global table.",
        parameters: &[(
            "duration",
            "The duration of the action (ISO 8601 duration).",
            true,
        )],
        targets: &[("Tables", "aws:dynamodb:global-table")],
    },
    ActionDef {
        id: "aws:lambda:invocation-add-delay",
        description: "Add a delay to the invocations of the target Lambda functions.",
        parameters: &[
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("startupDelayMilliseconds", "The delay to add, in milliseconds.", true),
            ("invocationPercentage", "The percentage of invocations to affect.", false),
        ],
        targets: &[("Functions", "aws:lambda:function")],
    },
    ActionDef {
        id: "aws:lambda:invocation-error",
        description: "Cause the invocations of the target Lambda functions to return an error.",
        parameters: &[
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("invocationPercentage", "The percentage of invocations to affect.", false),
        ],
        targets: &[("Functions", "aws:lambda:function")],
    },
    ActionDef {
        id: "aws:s3:bucket-pause-replication",
        description: "Pause replication for the target Amazon S3 buckets.",
        parameters: &[
            ("duration", "The duration of the action (ISO 8601 duration).", true),
            ("region", "The AWS Region to pause replication in.", true),
        ],
        targets: &[("Buckets", "aws:s3:bucket")],
    },
    ActionDef {
        id: "aws:arc:start-zonal-autoshift",
        description: "Start a zonal autoshift for the target resources away from an Availability Zone.",
        parameters: &[
            ("availabilityZoneIdentifier", "The Availability Zone to shift away from.", true),
            ("duration", "The duration of the action (ISO 8601 duration).", true),
        ],
        targets: &[],
    },
];

/// One catalog target resource type: resource type id, description, and its
/// parameters (name -> description).
struct ResourceTypeDef {
    resource_type: &'static str,
    description: &'static str,
    /// (parameter name, description)
    parameters: &'static [(&'static str, &'static str)],
}

/// The predefined AWS FIS target resource type catalog.
const RESOURCE_TYPES: &[ResourceTypeDef] = &[
    ResourceTypeDef {
        resource_type: "aws:ec2:instance",
        description: "An Amazon EC2 instance.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ec2:spot-instance",
        description: "An Amazon EC2 Spot Instance.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ec2:subnet",
        description: "An Amazon VPC subnet.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ec2:autoscaling-group",
        description: "An Amazon EC2 Auto Scaling group.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ecs:cluster",
        description: "An Amazon ECS cluster.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ecs:task",
        description: "An Amazon ECS task.",
        parameters: &[(
            "cluster",
            "The Amazon Resource Name (ARN) of the ECS cluster.",
        )],
    },
    ResourceTypeDef {
        resource_type: "aws:eks:nodegroup",
        description: "An Amazon EKS node group.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:eks:pod",
        description: "An Amazon EKS pod.",
        parameters: &[
            ("namespace", "The Kubernetes namespace."),
            ("selectorType", "The type of selector for the pods."),
            ("selectorValue", "The value of the selector for the pods."),
        ],
    },
    ResourceTypeDef {
        resource_type: "aws:rds:db-instance",
        description: "An Amazon RDS DB instance.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:rds:cluster",
        description: "An Amazon RDS DB cluster.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:iam:role",
        description: "An IAM role.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:ssm:managed-instance",
        description: "An AWS Systems Manager managed instance.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:cloudwatch:alarm",
        description: "An Amazon CloudWatch alarm.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:dynamodb:global-table",
        description: "An Amazon DynamoDB global table.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:lambda:function",
        description: "An AWS Lambda function.",
        parameters: &[],
    },
    ResourceTypeDef {
        resource_type: "aws:s3:bucket",
        description: "An Amazon S3 bucket.",
        parameters: &[],
    },
];

/// Build the full `Action` wire object for a catalog action in a given region.
fn action_value(def: &ActionDef, region: &str) -> Value {
    let mut parameters = Map::new();
    for (name, desc, required) in def.parameters {
        parameters.insert(
            (*name).to_string(),
            json!({ "description": desc, "required": required }),
        );
    }
    let mut targets = Map::new();
    for (role, resource_type) in def.targets {
        targets.insert(
            (*role).to_string(),
            json!({ "resourceType": resource_type }),
        );
    }
    json!({
        "id": def.id,
        "arn": action_arn(region, def.id),
        "description": def.description,
        "parameters": Value::Object(parameters),
        "targets": Value::Object(targets),
        "tags": Value::Object(Map::new()),
    })
}

/// The `ActionSummary` wire object for a catalog action (list projection).
fn action_summary(def: &ActionDef, region: &str) -> Value {
    let mut targets = Map::new();
    for (role, resource_type) in def.targets {
        targets.insert(
            (*role).to_string(),
            json!({ "resourceType": resource_type }),
        );
    }
    json!({
        "id": def.id,
        "arn": action_arn(region, def.id),
        "description": def.description,
        "targets": Value::Object(targets),
        "tags": Value::Object(Map::new()),
    })
}

/// Every `Action` in the catalog (full form), for internal lookup.
pub fn get_action(region: &str, id: &str) -> Option<Value> {
    ACTIONS
        .iter()
        .find(|d| d.id == id)
        .map(|d| action_value(d, region))
}

/// Every `ActionSummary` in the catalog, sorted by id for stable pagination.
pub fn list_action_summaries(region: &str) -> Vec<Value> {
    let mut out: Vec<Value> = ACTIONS.iter().map(|d| action_summary(d, region)).collect();
    out.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    out
}

/// The full `TargetResourceType` wire object for a catalog resource type.
pub fn get_resource_type(resource_type: &str) -> Option<Value> {
    RESOURCE_TYPES
        .iter()
        .find(|d| d.resource_type == resource_type)
        .map(|d| {
            let mut parameters = Map::new();
            for (name, desc) in d.parameters {
                parameters.insert((*name).to_string(), json!({ "description": desc }));
            }
            json!({
                "resourceType": d.resource_type,
                "description": d.description,
                "parameters": Value::Object(parameters),
            })
        })
}

/// Every `TargetResourceTypeSummary` in the catalog, sorted for stable paging.
pub fn list_resource_type_summaries() -> Vec<Value> {
    let mut out: Vec<Value> = RESOURCE_TYPES
        .iter()
        .map(|d| json!({ "resourceType": d.resource_type, "description": d.description }))
        .collect();
    out.sort_by(|a, b| a["resourceType"].as_str().cmp(&b["resourceType"].as_str()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_action_resolves_with_aws_arn() {
        let a = get_action("us-east-1", "aws:ec2:stop-instances").unwrap();
        assert_eq!(
            a["arn"],
            json!("arn:aws:fis:us-east-1::action/aws:ec2:stop-instances")
        );
        assert_eq!(
            a["targets"]["Instances"]["resourceType"],
            "aws:ec2:instance"
        );
    }

    #[test]
    fn unknown_action_is_none() {
        assert!(get_action("us-east-1", "aws:ec2:nope").is_none());
    }

    #[test]
    fn catalog_is_non_empty_and_sorted() {
        let s = list_action_summaries("us-east-1");
        assert!(!s.is_empty());
        let ids: Vec<&str> = s.iter().map(|a| a["id"].as_str().unwrap()).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn resource_type_resolves() {
        assert!(get_resource_type("aws:ec2:instance").is_some());
        assert!(get_resource_type("aws:none:none").is_none());
    }
}
