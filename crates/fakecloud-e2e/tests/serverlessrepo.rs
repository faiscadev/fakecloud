//! End-to-end tests for the AWS Serverless Application Repository, driven
//! through the real `aws-sdk-serverlessapplicationrepository` client against a
//! live fakecloud server. Exercises the control plane end to end: create an
//! application with a SAM template -> get it back (verifying the parsed
//! parameter definitions) -> create a new version -> list versions -> put a
//! sharing policy -> get the policy -> create a CloudFormation template -> get
//! the template (verifying the PREPARING -> ACTIVE settle).

use aws_sdk_serverlessapplicationrepository::types::ApplicationPolicyStatement;
use fakecloud_testkit::TestServer;

const SAM_TEMPLATE: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Transform": "AWS::Serverless-2016-10-31",
  "Parameters": {
    "BucketName": {
      "Type": "String",
      "Default": "my-bucket",
      "Description": "The S3 bucket name"
    }
  },
  "Resources": {
    "ProcessorFunction": {
      "Type": "AWS::Serverless::Function",
      "Properties": {
        "Handler": "index.handler",
        "Runtime": "python3.12",
        "Policies": ["AmazonS3ReadOnlyAccess"],
        "Environment": { "Variables": { "BUCKET": { "Ref": "BucketName" } } }
      }
    }
  }
}"#;

async fn sar_client(server: &TestServer) -> aws_sdk_serverlessapplicationrepository::Client {
    let conf =
        aws_sdk_serverlessapplicationrepository::config::Builder::from(&server.aws_config().await)
            .build();
    aws_sdk_serverlessapplicationrepository::Client::from_conf(conf)
}

#[tokio::test]
async fn serverlessrepo_full_lifecycle() {
    let server = TestServer::start().await;
    let client = sar_client(&server).await;

    // --- Create application with an initial version + SAM template ---
    let created = client
        .create_application()
        .name("e2e-sar-app")
        .author("Jane Developer")
        .description("An end-to-end serverless app")
        .home_page_url("https://example.com/app")
        .labels("data")
        .spdx_license_id("MIT")
        .semantic_version("1.0.0")
        .template_body(SAM_TEMPLATE)
        .send()
        .await
        .expect("create_application");
    let app_id = created.application_id().expect("applicationId").to_string();
    assert!(app_id.contains(":applications/e2e-sar-app"));
    assert_eq!(created.author(), Some("Jane Developer"));
    assert_eq!(created.home_page_url(), Some("https://example.com/app"));
    // The seeded version should carry the parsed parameter definitions.
    let version = created.version().expect("initial version");
    assert_eq!(version.semantic_version(), Some("1.0.0"));
    assert_eq!(version.resources_supported(), Some(true));
    let params = version.parameter_definitions();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0].name(), Some("BucketName"));
    assert_eq!(params[0].default_value(), Some("my-bucket"));
    assert!(version
        .required_capabilities()
        .iter()
        .any(|c| c.as_str() == "CAPABILITY_IAM"));

    // --- Get application (round-trips create inputs + parsed params) ---
    let got = client
        .get_application()
        .application_id(&app_id)
        .send()
        .await
        .expect("get_application");
    assert_eq!(got.name(), Some("e2e-sar-app"));
    assert_eq!(got.author(), Some("Jane Developer"));
    let got_params = got.version().expect("version").parameter_definitions();
    assert_eq!(got_params[0].name(), Some("BucketName"));

    // --- Update application metadata ---
    let updated = client
        .update_application()
        .application_id(&app_id)
        .description("Updated description")
        .send()
        .await
        .expect("update_application");
    assert_eq!(updated.description(), Some("Updated description"));

    // --- Create a new version ---
    let new_version = client
        .create_application_version()
        .application_id(&app_id)
        .semantic_version("2.0.0")
        .template_body(SAM_TEMPLATE)
        .source_code_url("https://github.com/example/app")
        .send()
        .await
        .expect("create_application_version");
    assert_eq!(new_version.semantic_version(), Some("2.0.0"));
    assert_eq!(
        new_version.source_code_url(),
        Some("https://github.com/example/app")
    );
    assert_eq!(new_version.parameter_definitions().len(), 1);

    // --- List versions ---
    let versions = client
        .list_application_versions()
        .application_id(&app_id)
        .send()
        .await
        .expect("list_application_versions");
    let semvers: Vec<Option<&str>> = versions
        .versions()
        .iter()
        .map(|v| v.semantic_version())
        .collect();
    assert!(semvers.contains(&Some("1.0.0")));
    assert!(semvers.contains(&Some("2.0.0")));

    // --- Put application policy (sharing) ---
    let statement = ApplicationPolicyStatement::builder()
        .actions("Deploy")
        .principals("123456789012")
        .principal_org_ids("o-abcd1234")
        .build();
    let put = client
        .put_application_policy()
        .application_id(&app_id)
        .statements(statement)
        .send()
        .await
        .expect("put_application_policy");
    assert_eq!(put.statements().len(), 1);
    assert!(!put.statements()[0]
        .statement_id()
        .unwrap_or_default()
        .is_empty());

    // --- Get application policy ---
    let policy = client
        .get_application_policy()
        .application_id(&app_id)
        .send()
        .await
        .expect("get_application_policy");
    assert_eq!(policy.statements().len(), 1);
    assert_eq!(policy.statements()[0].actions()[0], "Deploy");

    // --- Create a CloudFormation template (PREPARING) ---
    let template = client
        .create_cloud_formation_template()
        .application_id(&app_id)
        .semantic_version("2.0.0")
        .send()
        .await
        .expect("create_cloud_formation_template");
    let template_id = template.template_id().expect("templateId").to_string();
    assert_eq!(
        template.status(),
        Some(&aws_sdk_serverlessapplicationrepository::types::Status::Preparing)
    );
    assert!(template
        .template_url()
        .unwrap_or_default()
        .starts_with("http"));

    // --- Get the CloudFormation template (settles to ACTIVE) ---
    let fetched = client
        .get_cloud_formation_template()
        .application_id(&app_id)
        .template_id(&template_id)
        .send()
        .await
        .expect("get_cloud_formation_template");
    assert_eq!(
        fetched.status(),
        Some(&aws_sdk_serverlessapplicationrepository::types::Status::Active)
    );

    // --- List applications includes ours ---
    let apps = client
        .list_applications()
        .send()
        .await
        .expect("list_applications");
    assert!(apps
        .applications()
        .iter()
        .any(|a| a.application_id() == Some(app_id.as_str())));

    // --- Delete application ---
    client
        .delete_application()
        .application_id(&app_id)
        .send()
        .await
        .expect("delete_application");
    let after = client
        .get_application()
        .application_id(&app_id)
        .send()
        .await;
    assert!(after.is_err(), "application should be gone after delete");
}
