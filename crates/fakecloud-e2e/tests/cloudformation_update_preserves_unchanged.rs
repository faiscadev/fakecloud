//! Regression coverage for the CloudFormation `UpdateStack` /
//! `ExecuteChangeSet` "reprovision-unchanged" data-loss bug.
//!
//! `apply_resource_updates` used to feed EVERY resource present in both the old
//! and new template to `update_resource`, regardless of whether its properties
//! changed. For any resource type without a dedicated in-place `update_*` arm
//! (e.g. `AWS::Glue::Database`), that routed to `reprovision_resource`, which
//! deletes and recreates the backing resource. So a single UNRELATED change to
//! a stack (here: adding an SNS topic) tore down and recreated every unchanged
//! resource — cascading a Glue database delete that wiped any table created in
//! it, while the stack still reported `UPDATE_COMPLETE`.
//!
//! After the fix, a resource whose resolved definition is unchanged is left
//! completely untouched: the table created in the Glue database survives the
//! unrelated update.

mod helpers;

use helpers::TestServer;

const TEMPLATE_V1: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "GlueDb": {
      "Type": "AWS::Glue::Database",
      "Properties": {
        "DatabaseInput": {"Name": "cfn-preserve-db", "Description": "keep me"}
      }
    }
  }
}"#;

// Identical Glue database, plus one unrelated new resource (an SNS topic).
const TEMPLATE_V2: &str = r#"{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "GlueDb": {
      "Type": "AWS::Glue::Database",
      "Properties": {
        "DatabaseInput": {"Name": "cfn-preserve-db", "Description": "keep me"}
      }
    },
    "UnrelatedTopic": {
      "Type": "AWS::SNS::Topic",
      "Properties": {"TopicName": "cfn-preserve-unrelated"}
    }
  }
}"#;

async fn wait_for_status(server: &TestServer, stack: &str, want: &str) {
    let cfn = server.cloudformation_client().await;
    let got = helpers::wait_until(std::time::Duration::from_secs(10), || {
        let cfn = cfn.clone();
        async move {
            let out = cfn.describe_stacks().stack_name(stack).send().await.ok()?;
            let status = out.stacks().first()?.stack_status()?;
            (status.as_str() == want).then_some(())
        }
    })
    .await;
    assert!(got.is_some(), "stack {stack} never reached {want}");
}

#[tokio::test]
async fn cfn_update_leaves_unchanged_resource_untouched() {
    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;

    cfn.create_stack()
        .stack_name("preserve")
        .template_body(TEMPLATE_V1)
        .send()
        .await
        .expect("create_stack");
    wait_for_status(&server, "preserve", "CREATE_COMPLETE").await;

    // Create a table in the Glue database out-of-band (the way real usage adds
    // data the CFN provisioner never sees).
    let glue = server.glue_client().await;
    glue.create_table()
        .database_name("cfn-preserve-db")
        .table_input(
            aws_sdk_glue::types::TableInput::builder()
                .name("keep-me")
                .build()
                .expect("table_input"),
        )
        .send()
        .await
        .expect("create_table");

    // Sanity: the table exists before the update.
    glue.get_table()
        .database_name("cfn-preserve-db")
        .name("keep-me")
        .send()
        .await
        .expect("get_table before update");

    // Apply an UNRELATED change (add an SNS topic). The Glue database is
    // identical between the two templates and must not be reprovisioned.
    cfn.update_stack()
        .stack_name("preserve")
        .template_body(TEMPLATE_V2)
        .send()
        .await
        .expect("update_stack");
    wait_for_status(&server, "preserve", "UPDATE_COMPLETE").await;

    // The out-of-band table must survive: pre-fix, the database was
    // delete+recreated and this 404s.
    let table = glue
        .get_table()
        .database_name("cfn-preserve-db")
        .name("keep-me")
        .send()
        .await
        .expect("table must survive an unrelated stack update");
    assert_eq!(table.table().map(|t| t.name()), Some("keep-me"));

    // The unrelated resource was actually added.
    let sns = server.sns_client().await;
    let topics = sns.list_topics().send().await.expect("list_topics");
    assert!(
        topics.topics().iter().any(|t| t
            .topic_arn()
            .is_some_and(|a| a.ends_with(":cfn-preserve-unrelated"))),
        "the unrelated SNS topic should have been provisioned by the update"
    );
}
