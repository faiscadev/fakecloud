//! CloudFormation provisions the four AWS::ElasticBeanstalk::* types as real
//! records in the `elasticbeanstalk` service control plane: an Application, an
//! ApplicationVersion, a ConfigurationTemplate and an Environment all read back
//! through their matching Describe* calls, expose the documented Ref value
//! (application name / version label / template name / environment name) and,
//! for the Environment, the EndpointURL attribute via Fn::GetAtt. Dependency
//! order is exercised via `Ref` on the Application. Deleting the stack removes
//! (terminates) every resource.

mod helpers;

use helpers::TestServer;

// One Application plus a version, a configuration template and an environment
// inside it. Every child's ApplicationName resolves from `Ref` on the
// Application, so the children can only be provisioned after it -- exercising
// dependency ordering. Outputs surface each resource's Ref and the
// environment's EndpointURL GetAtt so the test can assert intrinsic-function
// resolution.
const TEMPLATE: &str = r#"{
  "Resources": {
    "MyApp": {
      "Type": "AWS::ElasticBeanstalk::Application",
      "Properties": {
        "ApplicationName": "cfn-eb-app",
        "Description": "provisioned by cfn"
      }
    },
    "MyVersion": {
      "Type": "AWS::ElasticBeanstalk::ApplicationVersion",
      "Properties": {
        "ApplicationName": { "Ref": "MyApp" },
        "Description": "cfn version one",
        "SourceBundle": { "S3Bucket": "cfn-eb-bucket", "S3Key": "app.zip" }
      }
    },
    "MyTemplate": {
      "Type": "AWS::ElasticBeanstalk::ConfigurationTemplate",
      "Properties": {
        "ApplicationName": { "Ref": "MyApp" },
        "Description": "cfn template",
        "SolutionStackName": "64bit Amazon Linux 2 v3.5.0 running Docker",
        "OptionSettings": [
          {
            "Namespace": "aws:autoscaling:launchconfiguration",
            "OptionName": "InstanceType",
            "Value": "t3.small"
          }
        ]
      }
    },
    "MyEnv": {
      "Type": "AWS::ElasticBeanstalk::Environment",
      "Properties": {
        "ApplicationName": { "Ref": "MyApp" },
        "EnvironmentName": "cfn-eb-env",
        "SolutionStackName": "64bit Amazon Linux 2 v3.5.0 running Docker",
        "CNAMEPrefix": "cfn-eb-env",
        "Tags": [ { "Key": "env", "Value": "test" } ]
      }
    }
  },
  "Outputs": {
    "AppRef":      { "Value": { "Ref": "MyApp" } },
    "VersionRef":  { "Value": { "Ref": "MyVersion" } },
    "TemplateRef": { "Value": { "Ref": "MyTemplate" } },
    "EnvRef":      { "Value": { "Ref": "MyEnv" } },
    "EnvEndpoint": { "Value": { "Fn::GetAtt": ["MyEnv", "EndpointURL"] } }
  }
}"#;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> &'a str {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("missing output {key}"))
}

#[tokio::test]
async fn cfn_provisions_elastic_beanstalk_resources() {
    // FAKECLOUD_EB_SETTLE_MS=0 keeps the direct EB environment lifecycle snappy;
    // the CFN provisioner writes an already-settled (Ready) environment, so the
    // env var only matters for any direct API paths.
    let s = TestServer::start_with_env(&[("FAKECLOUD_EB_SETTLE_MS", "0")]).await;
    let cfn = s.cloudformation_client().await;
    let eb = s.elasticbeanstalk_client().await;

    cfn.create_stack()
        .stack_name("eb-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("eb-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Ref resolution (verified against the AWS resource specs) ---
    assert_eq!(output(stack, "AppRef"), "cfn-eb-app");
    // VersionLabel/TemplateName were omitted, so they default to the logical id.
    assert_eq!(output(stack, "VersionRef"), "MyVersion");
    assert_eq!(output(stack, "TemplateRef"), "MyTemplate");
    assert_eq!(output(stack, "EnvRef"), "cfn-eb-env");

    // --- Environment EndpointURL GetAtt ---
    let endpoint = output(stack, "EnvEndpoint");
    assert!(
        endpoint.contains("awseb-") && endpoint.ends_with(".elb.amazonaws.com"),
        "endpoint url shape: {endpoint}"
    );

    // --- The Application exists in the service ---
    let apps = eb
        .describe_applications()
        .application_names("cfn-eb-app")
        .send()
        .await
        .expect("DescribeApplications");
    let app = apps
        .applications()
        .iter()
        .find(|a| a.application_name() == Some("cfn-eb-app"))
        .expect("application present");
    assert_eq!(app.description(), Some("provisioned by cfn"));

    // --- The ApplicationVersion exists ---
    let versions = eb
        .describe_application_versions()
        .application_name("cfn-eb-app")
        .send()
        .await
        .expect("DescribeApplicationVersions");
    assert!(
        versions
            .application_versions()
            .iter()
            .any(|v| v.version_label() == Some("MyVersion")),
        "expected version MyVersion"
    );

    // --- The ConfigurationTemplate exists (and copied the option setting) ---
    let settings = eb
        .describe_configuration_settings()
        .application_name("cfn-eb-app")
        .template_name("MyTemplate")
        .send()
        .await
        .expect("DescribeConfigurationSettings");
    let cfg = settings
        .configuration_settings()
        .first()
        .expect("configuration settings present");
    assert_eq!(cfg.application_name(), Some("cfn-eb-app"));
    assert_eq!(cfg.template_name(), Some("MyTemplate"));
    assert!(
        cfg.option_settings().iter().any(|o| {
            o.namespace() == Some("aws:autoscaling:launchconfiguration")
                && o.option_name() == Some("InstanceType")
                && o.value() == Some("t3.small")
        }),
        "expected the InstanceType option setting to round-trip"
    );

    // --- The Environment exists, is Ready and exposes the same endpoint ---
    let envs = eb
        .describe_environments()
        .environment_names("cfn-eb-env")
        .send()
        .await
        .expect("DescribeEnvironments");
    let env = envs
        .environments()
        .iter()
        .find(|e| e.environment_name() == Some("cfn-eb-env"))
        .expect("environment present");
    assert_eq!(env.application_name(), Some("cfn-eb-app"));
    assert_eq!(env.status().map(|s| s.as_str()), Some("Ready"));
    assert_eq!(env.endpoint_url(), Some(endpoint));

    // --- Deleting the stack removes / terminates every resource ---
    cfn.delete_stack()
        .stack_name("eb-stack")
        .send()
        .await
        .unwrap();

    let apps_gone = eb
        .describe_applications()
        .application_names("cfn-eb-app")
        .send()
        .await
        .expect("DescribeApplications after delete");
    assert!(
        apps_gone.applications().is_empty(),
        "stack delete should remove the application"
    );

    // A default DescribeEnvironments excludes Terminated environments.
    let envs_gone = eb
        .describe_environments()
        .environment_names("cfn-eb-env")
        .send()
        .await
        .expect("DescribeEnvironments after delete");
    assert!(
        envs_gone.environments().is_empty(),
        "stack delete should terminate the environment"
    );
}
