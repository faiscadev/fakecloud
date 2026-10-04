//! SSM parameters and Secrets Manager secrets are regional resources: the same
//! name exists independently in every region, list calls only see the request
//! region, ARNs name the resource's own region (byte-identical across calls),
//! and an ARN of another region is not found from a client of this region. A
//! replicated secret exists as a read-only replica in its replica region.

mod helpers;

use helpers::TestServer;

async fn ssm_in(server: &TestServer, region: &str) -> aws_sdk_ssm::Client {
    aws_sdk_ssm::Client::new(&server.aws_config_in(region).await)
}

async fn sm_in(server: &TestServer, region: &str) -> aws_sdk_secretsmanager::Client {
    aws_sdk_secretsmanager::Client::new(&server.aws_config_in(region).await)
}

#[tokio::test]
async fn ssm_parameters_are_region_scoped() {
    let server = TestServer::start().await;
    let east = ssm_in(&server, "us-east-1").await;
    let west = ssm_in(&server, "eu-west-1").await;

    for (client, value) in [(&east, "east-value"), (&west, "west-value")] {
        client
            .put_parameter()
            .name("/app/db/url")
            .value(value)
            .r#type(aws_sdk_ssm::types::ParameterType::String)
            .send()
            .await
            .expect("PutParameter");
    }
    west.put_parameter()
        .name("/app/db/west-only")
        .value("w")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .expect("PutParameter west-only");

    for (client, region, value) in [
        (&east, "us-east-1", "east-value"),
        (&west, "eu-west-1", "west-value"),
    ] {
        let param = client
            .get_parameter()
            .name("/app/db/url")
            .send()
            .await
            .expect("GetParameter")
            .parameter
            .unwrap();
        assert_eq!(param.value(), Some(value));
        assert_eq!(param.version(), 1, "each region has its own version line");
        let arn = format!("arn:aws:ssm:{region}:123456789012:parameter/app/db/url");
        assert_eq!(param.arn(), Some(arn.as_str()));
        // The ARN round-trips byte-identical through DescribeParameters too.
        let described = client
            .describe_parameters()
            .parameter_filters(
                aws_sdk_ssm::types::ParameterStringFilter::builder()
                    .key("Name")
                    .values("/app/db/url")
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .expect("DescribeParameters");
        assert_eq!(described.parameters()[0].arn(), Some(arn.as_str()));
    }

    let by_path = |client: aws_sdk_ssm::Client| async move {
        let mut names: Vec<String> = client
            .get_parameters_by_path()
            .path("/app/db")
            .send()
            .await
            .expect("GetParametersByPath")
            .parameters()
            .iter()
            .map(|p| p.name().unwrap().to_string())
            .collect();
        names.sort();
        names
    };
    assert_eq!(by_path(east.clone()).await, vec!["/app/db/url"]);
    assert_eq!(
        by_path(west.clone()).await,
        vec!["/app/db/url", "/app/db/west-only"]
    );

    // An ARN of the other region does not resolve from this region.
    let west_arn = "arn:aws:ssm:eu-west-1:123456789012:parameter/app/db/west-only";
    let err = east
        .get_parameter()
        .name(west_arn)
        .send()
        .await
        .expect_err("another region's parameter ARN is not visible");
    assert!(
        err.into_service_error().is_parameter_not_found(),
        "expected ParameterNotFound"
    );

    // AWS-owned public parameters exist in every region.
    for client in [&east, &west] {
        let p = client
            .get_parameter()
            .name("/aws/service/global-infrastructure/regions/us-east-1/longName")
            .send()
            .await
            .expect("public parameter")
            .parameter
            .unwrap();
        assert_eq!(p.value(), Some("US East (N. Virginia)"));
    }

    // Deleting in one region leaves the other untouched.
    east.delete_parameter()
        .name("/app/db/url")
        .send()
        .await
        .expect("DeleteParameter");
    let still = west
        .get_parameter()
        .name("/app/db/url")
        .send()
        .await
        .expect("west parameter survives")
        .parameter
        .unwrap();
    assert_eq!(still.value(), Some("west-value"));
}

#[tokio::test]
async fn ssm_secretsmanager_reference_reads_the_request_region() {
    let server = TestServer::start().await;
    for (region, value) in [("us-east-1", "east-secret"), ("eu-west-1", "west-secret")] {
        sm_in(&server, region)
            .await
            .create_secret()
            .name("ref/db")
            .secret_string(value)
            .send()
            .await
            .expect("CreateSecret");
    }
    for (region, value) in [("us-east-1", "east-secret"), ("eu-west-1", "west-secret")] {
        let p = ssm_in(&server, region)
            .await
            .get_parameter()
            .name("/aws/reference/secretsmanager/ref/db")
            .with_decryption(true)
            .send()
            .await
            .expect("Secrets Manager reference")
            .parameter
            .unwrap();
        assert_eq!(p.value(), Some(value));
    }
}

#[tokio::test]
async fn secrets_are_region_scoped() {
    let server = TestServer::start().await;
    let east = sm_in(&server, "us-east-1").await;
    let west = sm_in(&server, "eu-west-1").await;

    let east_arn = east
        .create_secret()
        .name("svc/password")
        .secret_string("east-pw")
        .send()
        .await
        .expect("CreateSecret east")
        .arn
        .unwrap();
    let west_arn = west
        .create_secret()
        .name("svc/password")
        .secret_string("west-pw")
        .send()
        .await
        .expect("CreateSecret west (same name, other region)")
        .arn
        .unwrap();
    assert!(
        east_arn.starts_with("arn:aws:secretsmanager:us-east-1:123456789012:secret:svc/password-")
    );
    assert!(
        west_arn.starts_with("arn:aws:secretsmanager:eu-west-1:123456789012:secret:svc/password-")
    );

    for (client, value, arn) in [(&east, "east-pw", &east_arn), (&west, "west-pw", &west_arn)] {
        let got = client
            .get_secret_value()
            .secret_id("svc/password")
            .send()
            .await
            .expect("GetSecretValue by name");
        assert_eq!(got.secret_string(), Some(value));
        assert_eq!(got.arn(), Some(arn.as_str()));
        let listed: Vec<String> = client
            .list_secrets()
            .send()
            .await
            .expect("ListSecrets")
            .secret_list()
            .iter()
            .map(|s| s.arn().unwrap().to_string())
            .collect();
        assert_eq!(listed, vec![arn.clone()]);
        let described = client
            .describe_secret()
            .secret_id(arn)
            .send()
            .await
            .expect("DescribeSecret by own-region ARN");
        assert_eq!(described.arn(), Some(arn.as_str()));
    }

    // GetSecretValue by the other region's ARN fails, as on AWS.
    let err = east
        .get_secret_value()
        .secret_id(&west_arn)
        .send()
        .await
        .expect_err("another region's secret ARN is not visible");
    assert!(
        err.into_service_error().is_resource_not_found_exception(),
        "expected ResourceNotFoundException"
    );
}

#[tokio::test]
async fn replicated_secret_is_a_read_only_replica_in_the_replica_region() {
    let server = TestServer::start().await;
    let east = sm_in(&server, "us-east-1").await;
    let west = sm_in(&server, "eu-west-1").await;

    let primary_arn = east
        .create_secret()
        .name("replicated")
        .secret_string("v1")
        .send()
        .await
        .expect("CreateSecret")
        .arn
        .unwrap();
    let out = east
        .replicate_secret_to_regions()
        .secret_id("replicated")
        .add_replica_regions(
            aws_sdk_secretsmanager::types::ReplicaRegionType::builder()
                .region("eu-west-1")
                .build(),
        )
        .send()
        .await
        .expect("ReplicateSecretToRegions");
    let status = &out.replication_status()[0];
    assert_eq!(status.region(), Some("eu-west-1"));
    assert_eq!(
        status.status(),
        Some(&aws_sdk_secretsmanager::types::StatusType::InSync)
    );

    let replica_arn = primary_arn.replace(":us-east-1:", ":eu-west-1:");
    let replica = west
        .describe_secret()
        .secret_id("replicated")
        .send()
        .await
        .expect("replica is describable from its region");
    assert_eq!(replica.arn(), Some(replica_arn.as_str()));
    assert_eq!(replica.primary_region(), Some("us-east-1"));

    east.put_secret_value()
        .secret_id("replicated")
        .secret_string("v2")
        .send()
        .await
        .expect("PutSecretValue on the primary");
    let got = west
        .get_secret_value()
        .secret_id(&replica_arn)
        .send()
        .await
        .expect("GetSecretValue on the replica");
    assert_eq!(got.secret_string(), Some("v2"));

    let err = west
        .put_secret_value()
        .secret_id("replicated")
        .secret_string("nope")
        .send()
        .await
        .expect_err("a replica is read-only");
    assert!(err.into_service_error().is_invalid_request_exception());

    east.remove_regions_from_replication()
        .secret_id("replicated")
        .remove_replica_regions("eu-west-1")
        .send()
        .await
        .expect("RemoveRegionsFromReplication");
    let err = west
        .describe_secret()
        .secret_id("replicated")
        .send()
        .await
        .expect_err("removing the region deletes the replica");
    assert!(err.into_service_error().is_resource_not_found_exception());
}
