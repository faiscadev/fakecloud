//! probe `service_detection` (audit-2026-05-19).

use super::*;

/// Map service names to their protocol.
pub(super) fn service_protocol(service_name: &str) -> Protocol {
    match service_name {
        "sqs" => Protocol::Query,
        "sns" => Protocol::Query,
        "iam" => Protocol::Query,
        "sts" => Protocol::Query,
        "cloudformation" => Protocol::Query,
        "ssm" => Protocol::Json {
            target_prefix: "AmazonSSM",
        },
        "events" => Protocol::Json {
            target_prefix: "AWSEvents",
        },
        "dynamodb" => Protocol::Json {
            target_prefix: "DynamoDB_20120810",
        },
        "dynamodbstreams" => Protocol::Json {
            target_prefix: "DynamoDBStreams_20120810",
        },
        "secretsmanager" => Protocol::Json {
            target_prefix: "secretsmanager",
        },
        "logs" => Protocol::Json {
            target_prefix: "Logs_20140328",
        },
        "kms" => Protocol::Json {
            target_prefix: "TrentService",
        },
        "cognito-idp" => Protocol::Json {
            target_prefix: "AWSCognitoIdentityProviderService",
        },
        "cognito-identity" => Protocol::Json {
            target_prefix: "AWSCognitoIdentityService",
        },
        "kinesis" => Protocol::Json {
            target_prefix: "Kinesis_20131202",
        },
        "kinesisanalyticsv2" => Protocol::Json {
            target_prefix: "KinesisAnalytics_20180523",
        },
        "ecr" => Protocol::Json {
            target_prefix: "AmazonEC2ContainerRegistry_V20150921",
        },
        "ecs" => Protocol::Json {
            target_prefix: "AmazonEC2ContainerServiceV20141113",
        },
        // Smithy service_name for Step Functions is `states`; SDK calls it SFN.
        "states" => Protocol::Json {
            target_prefix: "AWSStepFunctions",
        },
        "organizations" => Protocol::Json {
            target_prefix: "AWSOrganizationsV20161128",
        },
        "acm" => Protocol::Json {
            target_prefix: "CertificateManager",
        },
        "acm-pca" => Protocol::Json {
            target_prefix: "ACMPrivateCA",
        },
        "route53resolver" => Protocol::Json {
            target_prefix: "Route53Resolver",
        },
        "config" => Protocol::Json {
            target_prefix: "StarlingDoveService",
        },
        "application-autoscaling" => Protocol::Json {
            target_prefix: "AnyScaleFrontendService",
        },
        "wafv2" => Protocol::Json {
            target_prefix: "AWSWAF_20190729",
        },
        "athena" => Protocol::Json {
            target_prefix: "AmazonAthena",
        },
        "firehose" => Protocol::Json {
            target_prefix: "Firehose_20150804",
        },
        "glue" => Protocol::Json {
            target_prefix: "AWSGlue",
        },
        "emr" => Protocol::Json {
            target_prefix: "ElasticMapReduce",
        },
        "textract" => Protocol::Json {
            target_prefix: "Textract",
        },
        // Amazon Transcribe: awsJson1.1 (speech-to-text control plane).
        "transcribe" => Protocol::Json {
            target_prefix: "Transcribe",
        },
        "cloudcontrolapi" => Protocol::Json {
            target_prefix: "CloudApiService",
        },
        // Resource Groups Tagging API: awsJson1.1.
        "tagging" => Protocol::Json {
            target_prefix: "ResourceGroupsTaggingAPI_20170126",
        },
        // MemoryDB: awsJson1.1 (Redis/Valkey control plane).
        "memorydb" => Protocol::Json {
            target_prefix: "AmazonMemoryDB",
        },
        // Cloud Map (servicediscovery): awsJson1.1.
        "servicediscovery" => Protocol::Json {
            target_prefix: "Route53AutoNaming_v20170314",
        },
        // Database Migration Service: awsJson1.1.
        "dms" => Protocol::Json {
            target_prefix: "AmazonDMSv20160101",
        },
        // CloudTrail: awsJson1.1.
        "cloudtrail" => Protocol::Json {
            // Real SDK/terraform clients send the short Smithy shape name.
            target_prefix: "CloudTrail_20131101",
        },
        // Cost Explorer: awsJson1.1.
        "ce" => Protocol::Json {
            // Real SDK/terraform clients send the short Smithy shape name.
            target_prefix: "AWSInsightsIndexService",
        },
        // Transfer Family: awsJson1.1.
        "transfer" => Protocol::Json {
            target_prefix: "TransferService",
        },
        // AWS CodeBuild: awsJson1.1.
        "codebuild" => Protocol::Json {
            target_prefix: "CodeBuild_20161006",
        },
        // AWS CodeCommit: awsJson1.1.
        "codecommit" => Protocol::Json {
            target_prefix: "CodeCommit_20150413",
        },
        // IAM Identity Center Identity Store: awsJson1.1.
        "identitystore" => Protocol::Json {
            target_prefix: "AWSIdentityStore",
        },
        // IAM Identity Center SSO Admin: awsJson1.1.
        "sso" => Protocol::Json {
            target_prefix: "SWBExternalService",
        },
        // Verified Permissions: awsJson1.0.
        "verifiedpermissions" => Protocol::Json {
            target_prefix: "VerifiedPermissions",
        },
        // CodeConnections (successor to CodeStar Connections): awsJson1.0.
        "codeconnections" => Protocol::Json {
            target_prefix: "CodeConnections_20231201",
        },
        // AWS CodeDeploy: awsJson1.1.
        "codedeploy" => Protocol::Json {
            target_prefix: "CodeDeploy_20141006",
        },
        // AWS CodePipeline: awsJson1.1.
        "codepipeline" => Protocol::Json {
            target_prefix: "CodePipeline_20150709",
        },
        // CloudWatch Metrics & Alarms speaks the awsQuery protocol (sigv4
        // service name `monitoring`), distinct from CloudWatch Logs (`logs`,
        // awsJson1.1).
        "monitoring" => Protocol::Query,
        "s3" => Protocol::Rest,
        "eks" => Protocol::Rest,
        // Amazon S3 Glacier: restJson1, account-scoped paths, custom headers.
        "glacier" => Protocol::Rest,
        // AWS Backup: restJson1 control plane (@http traits + JSON bodies).
        "backup" => Protocol::Rest,
        // AWS Resource Access Manager: restJson1 control plane.
        "ram" => Protocol::Rest,
        // Amazon S3 Tables: restJson1 control plane (path-labelled URIs).
        "s3tables" => Protocol::Rest,
        // AWS Lake Formation: restJson1 governance control plane over Glue.
        "lakeformation" => Protocol::Rest,
        // Amazon Elasticsearch Service (2015) + Amazon OpenSearch Service
        // (2021): both restJson1, both sign as `es`. Two probe ids so each
        // API's operation set is exercised at its own path version prefix;
        // the server normalizes the `opensearch` credential scope to `es`.
        "es" => Protocol::Rest,
        "opensearch" => Protocol::Rest,
        "lambda" => Protocol::Rest,
        // API Gateway v1 (REST APIs) and v2 (HTTP APIs) are separate
        // Smithy models with distinct `service_name` entries in
        // `service-map.json`. fakecloud's facade routes both behind the
        // single SigV4 service identifier `apigateway`, but probing
        // keeps them separate. restJson1 wire format for both.
        "apigateway" | "apigatewayv1" | "apigatewayv2" => Protocol::Rest,
        // restJson1 services — REST routing with @http traits + JSON bodies.
        "ses" => Protocol::Rest,
        "bedrock" => Protocol::Rest,
        "bedrock-runtime" => Protocol::Rest,
        "bedrock-agent" => Protocol::Rest,
        "bedrock-agent-runtime" => Protocol::Rest,
        "scheduler" => Protocol::Rest,
        // EventBridge Pipes: restJson1 control plane (@http traits + JSON bodies).
        "pipes" => Protocol::Rest,
        // AWS Fault Injection Simulator: restJson1 control plane.
        "fis" => Protocol::Rest,
        // RDS Data API: restJson1, runs real SQL on the backing RDS container.
        "rds-data" => Protocol::Rest,
        // Aurora DSQL: restJson1 control plane (clusters, streams, policies).
        "dsql" => Protocol::Rest,
        // Resource Groups: restJson1 control plane (groups, queries, tagging).
        "resource-groups" => Protocol::Rest,
        // Account Management: restJson1 control plane (contacts, regions, email).
        "account" => Protocol::Rest,
        // AWS AppConfig control plane + AppConfig Data plane: both restJson1,
        // both sign as `appconfig`. Two probe ids so each model-service's
        // operation set is exercised; the server normalizes the
        // `appconfigdata` credential scope to `appconfig`.
        "appconfig" => Protocol::Rest,
        "appconfigdata" => Protocol::Rest,
        // AWS CodeArtifact: restJson1 control plane (`@http` method + path
        // routing over domains, repositories, packages, and package groups).
        "codeartifact" => Protocol::Rest,
        // Amazon EFS: restJson1 control plane (path-labelled `@http` URIs over
        // file systems, mount targets, and access points). Signs as
        // `elasticfilesystem`.
        "elasticfilesystem" => Protocol::Rest,
        // Amazon MQ: restJson1 control plane (path-labelled `@http` URIs over
        // brokers, configurations, users, and tags). Signs as `mq`.
        "mq" => Protocol::Rest,
        // Amazon MSK (Managed Streaming for Apache Kafka): restJson1 control
        // plane (path-labelled `@http` URIs over clusters, configurations,
        // operations, replicators, VPC connections, and topics). Signs as `kafka`.
        "kafka" => Protocol::Rest,
        // Amazon MWAA (Managed Workflows for Apache Airflow): restJson1 control
        // plane (path-labelled `@http` URIs over environments, access tokens,
        // and tags). Signs as `airflow`; the probe signs with `mwaa`.
        "mwaa" => Protocol::Rest,
        // AWS X-Ray: restJson1 control plane + trace data plane (fixed
        // `POST /<Op>` URIs; JSON bodies). Signs as `xray`.
        "xray" => Protocol::Rest,
        "appsync" => Protocol::Rest,
        // AWS Amplify: restJson1 hosting control plane (path-labelled `@http`
        // URIs over apps, branches, domains, webhooks, jobs; JSON bodies).
        // Signs as `amplify`.
        "amplify" => Protocol::Rest,
        // REST-XML services — distinct wire format from restJson1 but the
        // probe uses the same `@http` trait-driven URL builder for both
        // and reads response bodies as opaque text.
        "route53" => Protocol::Rest,
        "cloudfront" => Protocol::Rest,
        // awsQuery services — RDS, ElastiCache, ELBv2 — explicitly listed
        // for clarity instead of relying on the default fall-through.
        "rds" => Protocol::Query,
        "elasticache" => Protocol::Query,
        "elasticbeanstalk" => Protocol::Query,
        "elasticloadbalancing" => Protocol::Query,
        // EC2 speaks the `ec2Query` protocol: form-encoded `Action=` requests
        // and flattened-XML responses. Wire-compatible with the Query probe
        // path (the response field-presence validator keys on top-level
        // members, not the flattened `<item>` list-element names).
        "ec2" => Protocol::Query,
        _ => Protocol::Query,
    }
}

/// Services whose wire format is awsJson1.x but which carry the
/// `aws.protocols#awsQueryCompatible` trait, so their `__type` values
/// follow the awsQuery `<Code>` convention (preserved for legacy
/// SDK compatibility). The shape-level `awsQueryError` trait is the
/// authoritative wire code for these services even though they speak
/// JSON. Currently SQS is the only such service in the vendored models.
pub(super) fn is_aws_query_compatible_service(service_name: &str) -> bool {
    matches!(service_name, "sqs")
}
