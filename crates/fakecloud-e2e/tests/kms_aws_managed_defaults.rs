//! Resources encrypted at rest without a caller-named key report the account's
//! AWS-managed KMS key for their service (`alias/aws/<service>`), through the
//! direct API and through CloudFormation alike, and the reported ARN is a real
//! key: KMS `DescribeKey` resolves it to an AWS-managed key that the service's
//! alias targets.

mod helpers;

use aws_sdk_cloudformation::types::Capability;
use helpers::TestServer;

/// `key_arn` resolves via DescribeKey to the AWS-managed key protecting
/// `alias` (`alias/aws/<service>`): KeyManager AWS, and the key the alias
/// itself resolves to.
async fn assert_aws_managed_key(kms: &aws_sdk_kms::Client, key_arn: &str, alias: &str) {
    let meta = kms
        .describe_key()
        .key_id(key_arn)
        .send()
        .await
        .unwrap_or_else(|e| panic!("DescribeKey {key_arn}: {e:?}"))
        .key_metadata
        .expect("key metadata");
    assert_eq!(meta.arn(), Some(key_arn));
    assert_eq!(
        meta.key_manager().map(|m| m.as_str()),
        Some("AWS"),
        "{key_arn} is not AWS-managed"
    );
    let aliased = kms
        .describe_key()
        .key_id(alias)
        .send()
        .await
        .unwrap_or_else(|e| panic!("DescribeKey {alias}: {e:?}"))
        .key_metadata
        .and_then(|m| m.arn);
    assert_eq!(
        aliased.as_deref(),
        Some(key_arn),
        "{key_arn} is not the {alias} key"
    );
    assert!(
        meta.description()
            .unwrap_or_default()
            .starts_with("Default key that protects my "),
        "{key_arn}: {:?}",
        meta.description()
    );
}

#[tokio::test]
async fn api_created_resources_report_aws_managed_keys() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_KAFKA_DISABLE_BACKEND", "1")]).await;
    let cfg = server.aws_config().await;
    let kms = aws_sdk_kms::Client::new(&cfg);

    // EFS: Encrypted=true without KmsKeyId.
    let efs = aws_sdk_efs::Client::new(&cfg);
    let fs = efs
        .create_file_system()
        .creation_token("enc-default")
        .encrypted(true)
        .send()
        .await
        .expect("create file system");
    let efs_key = fs.kms_key_id().expect("KmsKeyId").to_string();
    assert_aws_managed_key(&kms, &efs_key, "alias/aws/elasticfilesystem").await;
    // Every default-encrypted file system shares the one managed key.
    let fs2 = efs
        .create_file_system()
        .creation_token("enc-default-2")
        .encrypted(true)
        .send()
        .await
        .expect("create second file system");
    assert_eq!(fs2.kms_key_id(), Some(efs_key.as_str()));

    // Timestream: CreateDatabase without KmsKeyId.
    let ts = server.timestream_write_client().await;
    let db = ts
        .create_database()
        .database_name("kms-default")
        .send()
        .await
        .expect("create database")
        .database
        .expect("database");
    let ts_key = db.kms_key_id().expect("KmsKeyId").to_string();
    assert_aws_managed_key(&kms, &ts_key, "alias/aws/timestream").await;

    // MSK: a provisioned cluster without a data-volume key.
    let kafka = aws_sdk_kafka::Client::new(&cfg);
    let arn = kafka
        .create_cluster()
        .cluster_name("kms-default")
        .kafka_version("3.6.0")
        .number_of_broker_nodes(2)
        .broker_node_group_info(
            aws_sdk_kafka::types::BrokerNodeGroupInfo::builder()
                .client_subnets("subnet-0123456789abcdef0")
                .client_subnets("subnet-0123456789abcdef1")
                .instance_type("kafka.m5.large")
                .build(),
        )
        .send()
        .await
        .expect("create cluster")
        .cluster_arn
        .expect("cluster arn");
    let info = kafka
        .describe_cluster()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("describe cluster")
        .cluster_info
        .expect("cluster info");
    let kafka_key = info
        .encryption_info()
        .and_then(|e| e.encryption_at_rest())
        .and_then(|r| r.data_volume_kms_key_id())
        .map(str::to_string)
        .expect("dataVolumeKMSKeyId");
    assert_aws_managed_key(&kms, &kafka_key, "alias/aws/kafka").await;

    // CodeArtifact: CreateDomain without encryptionKey.
    let ca = aws_sdk_codeartifact::Client::new(&cfg);
    let domain = ca
        .create_domain()
        .domain("kms-default")
        .send()
        .await
        .expect("create domain")
        .domain
        .expect("domain");
    let ca_key = domain.encryption_key().expect("encryptionKey").to_string();
    assert_aws_managed_key(&kms, &ca_key, "alias/aws/codeartifact").await;

    // CodeCommit: CreateRepository without kmsKeyId.
    let cc = aws_sdk_codecommit::Client::new(&cfg);
    let repo = cc
        .create_repository()
        .repository_name("kms-default")
        .send()
        .await
        .expect("create repository")
        .repository_metadata
        .expect("metadata");
    let cc_key = repo.kms_key_id().expect("kmsKeyId").to_string();
    assert_aws_managed_key(&kms, &cc_key, "alias/aws/codecommit").await;
}

const STACK: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Fs": {"Type": "AWS::EFS::FileSystem", "Properties": {"Encrypted": true}},
    "Db": {"Type": "AWS::Timestream::Database", "Properties": {"DatabaseName": "cfn-kms-default"}},
    "Repo": {"Type": "AWS::CodeCommit::Repository", "Properties": {"RepositoryName": "cfn-kms-default"}}
  },
  "Outputs": {
    "FsId": {"Value": {"Ref": "Fs"}}
  }
}"#;

#[tokio::test]
async fn stack_resources_report_the_same_aws_managed_keys_as_the_api() {
    let server = TestServer::start().await;
    let cfg = server.aws_config().await;
    let kms = aws_sdk_kms::Client::new(&cfg);
    let efs = aws_sdk_efs::Client::new(&cfg);
    let ts = server.timestream_write_client().await;
    let cc = aws_sdk_codecommit::Client::new(&cfg);

    let cfn = server.cloudformation_client().await;
    cfn.create_stack()
        .stack_name("kms-defaults")
        .template_body(STACK)
        .capabilities(Capability::CapabilityIam)
        .send()
        .await
        .expect("create stack");
    let stack = cfn
        .describe_stacks()
        .stack_name("kms-defaults")
        .send()
        .await
        .expect("describe stacks");
    let stack = &stack.stacks()[0];
    assert_eq!(
        stack.stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE"),
        "{:?}",
        stack.stack_status_reason()
    );
    let fs_id = stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some("FsId"))
        .and_then(|o| o.output_value())
        .expect("FsId output")
        .to_string();

    let stack_fs = efs
        .describe_file_systems()
        .file_system_id(&fs_id)
        .send()
        .await
        .expect("describe stack file system");
    let stack_efs_key = stack_fs.file_systems()[0]
        .kms_key_id()
        .expect("KmsKeyId")
        .to_string();
    assert_aws_managed_key(&kms, &stack_efs_key, "alias/aws/elasticfilesystem").await;
    // The API path reports the very same key.
    let api_fs = efs
        .create_file_system()
        .creation_token("api-after-stack")
        .encrypted(true)
        .send()
        .await
        .expect("create api file system");
    assert_eq!(api_fs.kms_key_id(), Some(stack_efs_key.as_str()));

    let db = ts
        .describe_database()
        .database_name("cfn-kms-default")
        .send()
        .await
        .expect("describe stack database")
        .database
        .expect("database");
    let stack_ts_key = db.kms_key_id().expect("KmsKeyId").to_string();
    assert_aws_managed_key(&kms, &stack_ts_key, "alias/aws/timestream").await;

    let repo = cc
        .get_repository()
        .repository_name("cfn-kms-default")
        .send()
        .await
        .expect("get stack repository")
        .repository_metadata
        .expect("metadata");
    let stack_cc_key = repo.kms_key_id().expect("kmsKeyId").to_string();
    assert_aws_managed_key(&kms, &stack_cc_key, "alias/aws/codecommit").await;
}

/// AWS-managed keys exist per account AND region: the same service's default
/// key differs between regions, each key ARN is in its own region and
/// partition, and KMS in that region resolves it and lists its alias.
#[tokio::test]
async fn aws_managed_keys_are_per_region() {
    let server = TestServer::start().await;
    let mut keys = Vec::new();
    for (region, prefix) in [
        ("us-east-1", "arn:aws:kms:us-east-1:"),
        ("eu-west-1", "arn:aws:kms:eu-west-1:"),
        ("cn-north-1", "arn:aws-cn:kms:cn-north-1:"),
    ] {
        let cfg = server.aws_config_in(region).await;
        let efs = aws_sdk_efs::Client::new(&cfg);
        let key = efs
            .create_file_system()
            .creation_token(format!("regional-{region}"))
            .encrypted(true)
            .send()
            .await
            .unwrap_or_else(|e| panic!("create file system in {region}: {e:?}"))
            .kms_key_id
            .expect("KmsKeyId");
        assert!(key.starts_with(prefix), "{region}: {key}");
        let kms = aws_sdk_kms::Client::new(&cfg);
        assert_aws_managed_key(&kms, &key, "alias/aws/elasticfilesystem").await;
        keys.push(key);
    }
    keys.sort();
    keys.dedup();
    assert_eq!(
        keys.len(),
        3,
        "each region has its own managed key: {keys:?}"
    );
}
