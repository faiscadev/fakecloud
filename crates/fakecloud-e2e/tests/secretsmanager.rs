mod helpers;

use aws_sdk_secretsmanager::types::Tag;
use helpers::TestServer;

#[tokio::test]
async fn secretsmanager_create_get_delete() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    // Create
    let resp = client
        .create_secret()
        .name("test/db-password")
        .secret_string("supersecret123")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.name().unwrap(), "test/db-password");
    assert!(resp.arn().unwrap().contains("test/db-password"));
    let version_id = resp.version_id().unwrap().to_string();
    assert!(!version_id.is_empty());

    // Get
    let resp = client
        .get_secret_value()
        .secret_id("test/db-password")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "supersecret123");
    assert_eq!(resp.name().unwrap(), "test/db-password");

    // Delete (force)
    client
        .delete_secret()
        .secret_id("test/db-password")
        .force_delete_without_recovery(true)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_secret_value()
        .secret_id("test/db-password")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn secretsmanager_put_secret_value_versioning() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("versioned")
        .secret_string("version1")
        .send()
        .await
        .unwrap();

    // Put new version
    let resp = client
        .put_secret_value()
        .secret_id("versioned")
        .secret_string("version2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "versioned");

    // Get should return version2
    let resp = client
        .get_secret_value()
        .secret_id("versioned")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "version2");

    // List versions
    let resp = client
        .list_secret_version_ids()
        .secret_id("versioned")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.versions().len(), 2);
}

#[tokio::test]
async fn secretsmanager_delete_and_restore() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("restorable")
        .secret_string("myvalue")
        .send()
        .await
        .unwrap();

    // Soft delete
    let resp = client
        .delete_secret()
        .secret_id("restorable")
        .send()
        .await
        .unwrap();
    assert!(resp.deletion_date().is_some());

    // Get should fail
    let result = client
        .get_secret_value()
        .secret_id("restorable")
        .send()
        .await;
    assert!(result.is_err());

    // Restore
    client
        .restore_secret()
        .secret_id("restorable")
        .send()
        .await
        .unwrap();

    // Get should work again
    let resp = client
        .get_secret_value()
        .secret_id("restorable")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "myvalue");
}

#[tokio::test]
async fn secretsmanager_list_secrets() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    for name in &["secret-a", "secret-b", "secret-c"] {
        client
            .create_secret()
            .name(*name)
            .secret_string("val")
            .send()
            .await
            .unwrap();
    }

    let resp = client.list_secrets().send().await.unwrap();
    assert_eq!(resp.secret_list().len(), 3);
}

#[tokio::test]
async fn secretsmanager_tags() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("tagged-secret")
        .secret_string("val")
        .send()
        .await
        .unwrap();

    // Tag
    client
        .tag_resource()
        .secret_id("tagged-secret")
        .tags(
            Tag::builder()
                .key("environment")
                .value("production")
                .build(),
        )
        .tags(Tag::builder().key("team").value("backend").build())
        .send()
        .await
        .unwrap();

    // Describe to check tags
    let resp = client
        .describe_secret()
        .secret_id("tagged-secret")
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 2);

    // Untag
    client
        .untag_resource()
        .secret_id("tagged-secret")
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_secret()
        .secret_id("tagged-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 1);
}

#[tokio::test]
async fn secretsmanager_describe_secret() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("described")
        .secret_string("value")
        .description("A test secret")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_secret()
        .secret_id("described")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "described");
    assert_eq!(resp.description().unwrap(), "A test secret");
    assert!(resp.created_date().is_some());
    assert!(!resp.version_ids_to_stages().unwrap().is_empty());
}

#[tokio::test]
async fn secretsmanager_duplicate_create_fails() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("dup-secret")
        .secret_string("val")
        .send()
        .await
        .unwrap();

    let result = client
        .create_secret()
        .name("dup-secret")
        .secret_string("val2")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn secretsmanager_batch_get_secret_value() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    // Create 3 secrets
    for (name, val) in &[
        ("batch-one", "value-one"),
        ("batch-two", "value-two"),
        ("batch-three", "value-three"),
    ] {
        client
            .create_secret()
            .name(*name)
            .secret_string(*val)
            .send()
            .await
            .unwrap();
    }

    // Batch get all 3
    let resp = client
        .batch_get_secret_value()
        .secret_id_list("batch-one")
        .secret_id_list("batch-two")
        .secret_id_list("batch-three")
        .send()
        .await
        .unwrap();

    let values = resp.secret_values();
    assert_eq!(values.len(), 3);

    let names: Vec<&str> = values.iter().filter_map(|v| v.name()).collect();
    assert!(names.contains(&"batch-one"));
    assert!(names.contains(&"batch-two"));
    assert!(names.contains(&"batch-three"));

    // Verify actual values
    for sv in values {
        match sv.name().unwrap() {
            "batch-one" => assert_eq!(sv.secret_string().unwrap(), "value-one"),
            "batch-two" => assert_eq!(sv.secret_string().unwrap(), "value-two"),
            "batch-three" => assert_eq!(sv.secret_string().unwrap(), "value-three"),
            other => panic!("unexpected secret: {other}"),
        }
    }

    // No errors
    assert!(resp.errors().is_empty());
}

#[tokio::test]
async fn secretsmanager_get_random_password() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    // Default length (32)
    let resp = client.get_random_password().send().await.unwrap();
    let pw = resp.random_password().unwrap();
    assert_eq!(pw.len(), 32);

    // Custom length
    let resp = client
        .get_random_password()
        .password_length(64)
        .send()
        .await
        .unwrap();
    let pw = resp.random_password().unwrap();
    assert_eq!(pw.len(), 64);

    // Exclude uppercase, numbers, punctuation -> only lowercase
    let resp = client
        .get_random_password()
        .password_length(50)
        .exclude_uppercase(true)
        .exclude_numbers(true)
        .exclude_punctuation(true)
        .require_each_included_type(false)
        .send()
        .await
        .unwrap();
    let pw = resp.random_password().unwrap();
    assert_eq!(pw.len(), 50);
    assert!(
        pw.chars().all(|c| c.is_ascii_lowercase()),
        "expected only lowercase, got: {pw}"
    );

    // Exclude specific characters
    let resp = client
        .get_random_password()
        .password_length(100)
        .exclude_characters("aeiou0123456789")
        .exclude_punctuation(true)
        .require_each_included_type(false)
        .send()
        .await
        .unwrap();
    let pw = resp.random_password().unwrap();
    assert_eq!(pw.len(), 100);
    for ch in "aeiou0123456789".chars() {
        assert!(
            !pw.contains(ch),
            "password should not contain '{ch}', got: {pw}"
        );
    }

    // Too short -> error
    let result = client.get_random_password().password_length(3).send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn secretsmanager_update_secret_then_get() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("update-me")
        .secret_string("original")
        .description("old description")
        .send()
        .await
        .unwrap();

    // Update description only (no new value)
    client
        .update_secret()
        .secret_id("update-me")
        .description("new description")
        .send()
        .await
        .unwrap();

    // Describe to verify description changed
    let resp = client
        .describe_secret()
        .secret_id("update-me")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.description().unwrap(), "new description");

    // Value should still be original
    let resp = client
        .get_secret_value()
        .secret_id("update-me")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "original");

    // Update with a new value
    client
        .update_secret()
        .secret_id("update-me")
        .secret_string("updated-value")
        .send()
        .await
        .unwrap();

    // Get should return updated value
    let resp = client
        .get_secret_value()
        .secret_id("update-me")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "updated-value");
}

#[tokio::test]
async fn secretsmanager_put_get_resource_policy() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;

    client
        .create_secret()
        .name("policy-test")
        .secret_string("secret-value")
        .send()
        .await
        .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}"#;

    // Put resource policy
    client
        .put_resource_policy()
        .secret_id("policy-test")
        .resource_policy(policy)
        .send()
        .await
        .unwrap();

    // Get resource policy
    let resp = client
        .get_resource_policy()
        .secret_id("policy-test")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.name().unwrap(), "policy-test");
    assert_eq!(resp.resource_policy().unwrap(), policy);

    // Delete resource policy
    client
        .delete_resource_policy()
        .secret_id("policy-test")
        .send()
        .await
        .unwrap();

    // Get again - should have no policy
    let resp = client
        .get_resource_policy()
        .secret_id("policy-test")
        .send()
        .await
        .unwrap();
    assert!(resp.resource_policy().is_none());
}
