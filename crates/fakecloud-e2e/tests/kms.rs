mod helpers;

use aws_sdk_kms::primitives::Blob;
use helpers::TestServer;

#[tokio::test]
async fn kms_create_describe_list_keys() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    // Create key
    let resp = client
        .create_key()
        .description("test key")
        .send()
        .await
        .unwrap();
    let metadata = resp.key_metadata().unwrap();
    assert!(metadata.key_id().starts_with(|c: char| c.is_alphanumeric()));
    assert!(metadata.arn().unwrap().contains(":key/"));
    assert!(metadata.enabled());

    let key_id = metadata.key_id().to_string();

    // Describe key
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(
        desc.key_metadata().unwrap().description().unwrap(),
        "test key"
    );

    // List keys
    let list = client.list_keys().send().await.unwrap();
    assert_eq!(list.keys().len(), 1);
    assert_eq!(list.keys()[0].key_id().unwrap(), key_id);
}

#[tokio::test]
async fn kms_enable_disable_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    // Disable
    client.disable_key().key_id(&key_id).send().await.unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(!desc.key_metadata().unwrap().enabled());

    // Enable
    client.enable_key().key_id(&key_id).send().await.unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(desc.key_metadata().unwrap().enabled());
}

#[tokio::test]
async fn kms_schedule_key_deletion() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let del_resp = client
        .schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.key_id().unwrap(), key_id);
    assert!(del_resp.deletion_date().is_some());

    // Key should now be pending deletion
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(
        desc.key_metadata().unwrap().key_state(),
        Some(&aws_sdk_kms::types::KeyState::PendingDeletion)
    );
}

#[tokio::test]
async fn kms_encrypt_decrypt_roundtrip() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let plaintext = b"Hello, KMS!";

    // Encrypt
    let enc = client
        .encrypt()
        .key_id(&key_id)
        .plaintext(Blob::new(plaintext.to_vec()))
        .send()
        .await
        .unwrap();
    let ciphertext = enc.ciphertext_blob().unwrap().clone();

    // Ciphertext should be different from plaintext
    assert_ne!(ciphertext.as_ref(), plaintext);

    // Decrypt
    let dec = client
        .decrypt()
        .ciphertext_blob(ciphertext)
        .send()
        .await
        .unwrap();
    let decrypted = dec.plaintext().unwrap();
    assert_eq!(decrypted.as_ref(), plaintext);
}

#[tokio::test]
async fn kms_encrypt_with_disabled_key_fails() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    client.disable_key().key_id(&key_id).send().await.unwrap();

    let result = client
        .encrypt()
        .key_id(&key_id)
        .plaintext(Blob::new(b"test".to_vec()))
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn kms_generate_data_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let dk = client
        .generate_data_key()
        .key_id(&key_id)
        .key_spec(aws_sdk_kms::types::DataKeySpec::Aes256)
        .send()
        .await
        .unwrap();

    assert!(dk.plaintext().is_some());
    assert!(dk.ciphertext_blob().is_some());
    assert!(dk.key_id().is_some());

    // The ciphertext should be decryptable
    let dec = client
        .decrypt()
        .ciphertext_blob(dk.ciphertext_blob().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert_eq!(
        dec.plaintext().unwrap().as_ref(),
        dk.plaintext().unwrap().as_ref()
    );
}

#[tokio::test]
async fn kms_generate_data_key_without_plaintext() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let dk = client
        .generate_data_key_without_plaintext()
        .key_id(&key_id)
        .key_spec(aws_sdk_kms::types::DataKeySpec::Aes256)
        .send()
        .await
        .unwrap();

    // Should have ciphertext but no plaintext
    assert!(dk.ciphertext_blob().is_some());
    assert!(dk.key_id().is_some());
}

#[tokio::test]
async fn kms_alias_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    // Create alias
    client
        .create_alias()
        .alias_name("alias/my-key")
        .target_key_id(&key_id)
        .send()
        .await
        .unwrap();

    // List aliases
    let list = client.list_aliases().send().await.unwrap();
    assert!(list
        .aliases()
        .iter()
        .any(|a| a.alias_name().unwrap() == "alias/my-key"));

    // Describe key by alias
    let desc = client
        .describe_key()
        .key_id("alias/my-key")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.key_metadata().unwrap().key_id(), key_id);

    // Encrypt using alias
    let enc = client
        .encrypt()
        .key_id("alias/my-key")
        .plaintext(Blob::new(b"alias-encrypted".to_vec()))
        .send()
        .await
        .unwrap();
    assert!(enc.ciphertext_blob().is_some());

    // Delete alias
    client
        .delete_alias()
        .alias_name("alias/my-key")
        .send()
        .await
        .unwrap();

    let list = client.list_aliases().send().await.unwrap();
    assert!(!list
        .aliases()
        .iter()
        .any(|a| a.alias_name().unwrap() == "alias/my-key"));
}

#[tokio::test]
async fn kms_tag_untag_list_tags() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    use aws_sdk_kms::types::Tag;
    client
        .tag_resource()
        .key_id(&key_id)
        .tags(
            Tag::builder()
                .tag_key("env")
                .tag_value("prod")
                .build()
                .unwrap(),
        )
        .tags(
            Tag::builder()
                .tag_key("team")
                .tag_value("security")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let tags = client
        .list_resource_tags()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 2);

    client
        .untag_resource()
        .key_id(&key_id)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_resource_tags()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].tag_key(), "env");
}

#[tokio::test]
async fn kms_describe_nonexistent_key_fails() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let result = client
        .describe_key()
        .key_id("00000000-0000-0000-0000-000000000000")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn kms_create_duplicate_alias_fails() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    client
        .create_alias()
        .alias_name("alias/dup-test")
        .target_key_id(&key_id)
        .send()
        .await
        .unwrap();

    let result = client
        .create_alias()
        .alias_name("alias/dup-test")
        .target_key_id(&key_id)
        .send()
        .await;
    assert!(result.is_err());
}
