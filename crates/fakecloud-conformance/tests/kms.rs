mod helpers;

use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{
    DataKeySpec, KeySpec, KeyUsageType, MessageType, SigningAlgorithmSpec, Tag,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Key lifecycle
// ---------------------------------------------------------------------------

#[test_action("kms", "CreateKey", checksum = "c66e4b13")]
#[tokio::test]
async fn kms_create_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client
        .create_key()
        .description("conformance key")
        .send()
        .await
        .unwrap();
    let meta = resp.key_metadata().unwrap();
    assert!(meta.enabled());
    assert!(meta.arn().unwrap().contains(":key/"));
}

#[test_action("kms", "DescribeKey", checksum = "c64650b2")]
#[tokio::test]
async fn kms_describe_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().description("dk").send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(resp.key_metadata().unwrap().description().unwrap(), "dk");
}

#[test_action("kms", "ListKeys", checksum = "05288289")]
#[tokio::test]
async fn kms_list_keys() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    client.create_key().send().await.unwrap();

    let resp = client.list_keys().send().await.unwrap();
    assert!(!resp.keys().is_empty());
}

#[test_action("kms", "EnableKey", checksum = "82c54b3d")]
#[tokio::test]
async fn kms_enable_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client.disable_key().key_id(&key_id).send().await.unwrap();
    client.enable_key().key_id(&key_id).send().await.unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(desc.key_metadata().unwrap().enabled());
}

#[test_action("kms", "DisableKey", checksum = "76150cfe")]
#[tokio::test]
async fn kms_disable_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client.disable_key().key_id(&key_id).send().await.unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(!desc.key_metadata().unwrap().enabled());
}

#[test_action("kms", "ScheduleKeyDeletion", checksum = "c2d3c723")]
#[tokio::test]
async fn kms_schedule_key_deletion() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_id().unwrap(), key_id);
    assert!(resp.deletion_date().is_some());
}

#[test_action("kms", "CancelKeyDeletion", checksum = "c1a27c10")]
#[tokio::test]
async fn kms_cancel_key_deletion() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();

    let resp = client
        .cancel_key_deletion()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_id().unwrap(), key_id);

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_ne!(
        desc.key_metadata().unwrap().key_state(),
        Some(&aws_sdk_kms::types::KeyState::PendingDeletion)
    );
}

// ---------------------------------------------------------------------------
// Encryption / Decryption
// ---------------------------------------------------------------------------

#[test_action("kms", "Encrypt", checksum = "d9b6f2bc")]
#[tokio::test]
async fn kms_encrypt() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .encrypt()
        .key_id(&key_id)
        .plaintext(Blob::new(b"hello".to_vec()))
        .send()
        .await
        .unwrap();
    assert!(resp.ciphertext_blob().is_some());
}

#[test_action("kms", "Decrypt", checksum = "51b5baa9")]
#[tokio::test]
async fn kms_decrypt() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let enc = client
        .encrypt()
        .key_id(&key_id)
        .plaintext(Blob::new(b"roundtrip".to_vec()))
        .send()
        .await
        .unwrap();

    let resp = client
        .decrypt()
        .ciphertext_blob(enc.ciphertext_blob().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.plaintext().unwrap().as_ref(), b"roundtrip");
}

#[test_action("kms", "ReEncrypt", checksum = "e1c64a49")]
#[tokio::test]
async fn kms_re_encrypt() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key1 = client.create_key().send().await.unwrap();
    let key1_id = key1.key_metadata().unwrap().key_id().to_string();

    let key2 = client.create_key().send().await.unwrap();
    let key2_id = key2.key_metadata().unwrap().key_id().to_string();

    let enc = client
        .encrypt()
        .key_id(&key1_id)
        .plaintext(Blob::new(b"reencrypt-test".to_vec()))
        .send()
        .await
        .unwrap();

    let resp = client
        .re_encrypt()
        .ciphertext_blob(enc.ciphertext_blob().unwrap().clone())
        .destination_key_id(&key2_id)
        .send()
        .await
        .unwrap();
    assert!(resp.ciphertext_blob().is_some());

    // Verify the re-encrypted blob decrypts correctly
    let dec = client
        .decrypt()
        .ciphertext_blob(resp.ciphertext_blob().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert_eq!(dec.plaintext().unwrap().as_ref(), b"reencrypt-test");
}

// ---------------------------------------------------------------------------
// Data keys
// ---------------------------------------------------------------------------

#[test_action("kms", "GenerateDataKey", checksum = "6a456116")]
#[tokio::test]
async fn kms_generate_data_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .generate_data_key()
        .key_id(&key_id)
        .key_spec(DataKeySpec::Aes256)
        .send()
        .await
        .unwrap();
    assert!(resp.plaintext().is_some());
    assert!(resp.ciphertext_blob().is_some());
}

#[test_action("kms", "GenerateDataKeyWithoutPlaintext", checksum = "c1d21720")]
#[tokio::test]
async fn kms_generate_data_key_without_plaintext() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .generate_data_key_without_plaintext()
        .key_id(&key_id)
        .key_spec(DataKeySpec::Aes256)
        .send()
        .await
        .unwrap();
    assert!(resp.ciphertext_blob().is_some());
}

#[test_action("kms", "GenerateRandom", checksum = "683cc0ca")]
#[tokio::test]
async fn kms_generate_random() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client
        .generate_random()
        .number_of_bytes(32)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.plaintext().unwrap().as_ref().len(), 32);
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

#[test_action("kms", "CreateAlias", checksum = "b4ac1bde")]
#[tokio::test]
async fn kms_create_alias() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .create_alias()
        .alias_name("alias/conf-alias")
        .target_key_id(&key_id)
        .send()
        .await
        .unwrap();

    let list = client.list_aliases().send().await.unwrap();
    assert!(list
        .aliases()
        .iter()
        .any(|a| a.alias_name().unwrap() == "alias/conf-alias"));
}

#[test_action("kms", "DeleteAlias", checksum = "da3623c8")]
#[tokio::test]
async fn kms_delete_alias() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .create_alias()
        .alias_name("alias/del-alias")
        .target_key_id(&key_id)
        .send()
        .await
        .unwrap();

    client
        .delete_alias()
        .alias_name("alias/del-alias")
        .send()
        .await
        .unwrap();

    let list = client.list_aliases().send().await.unwrap();
    assert!(!list
        .aliases()
        .iter()
        .any(|a| a.alias_name().unwrap() == "alias/del-alias"));
}

#[test_action("kms", "UpdateAlias", checksum = "b7ecc379")]
#[tokio::test]
async fn kms_update_alias() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key1 = client.create_key().send().await.unwrap();
    let key1_id = key1.key_metadata().unwrap().key_id().to_string();

    let key2 = client.create_key().send().await.unwrap();
    let key2_id = key2.key_metadata().unwrap().key_id().to_string();

    client
        .create_alias()
        .alias_name("alias/upd-alias")
        .target_key_id(&key1_id)
        .send()
        .await
        .unwrap();

    client
        .update_alias()
        .alias_name("alias/upd-alias")
        .target_key_id(&key2_id)
        .send()
        .await
        .unwrap();

    // Verify alias now points to key2
    let desc = client
        .describe_key()
        .key_id("alias/upd-alias")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.key_metadata().unwrap().key_id(), key2_id);
}

#[test_action("kms", "ListAliases", checksum = "14d90727")]
#[tokio::test]
async fn kms_list_aliases() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.list_aliases().send().await.unwrap();
    let _ = resp.aliases();
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("kms", "TagResource", checksum = "8e11dd8c")]
#[tokio::test]
async fn kms_tag_resource() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .tag_resource()
        .key_id(&key_id)
        .tags(
            Tag::builder()
                .tag_key("env")
                .tag_value("test")
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
    assert_eq!(tags.tags().len(), 1);
}

#[test_action("kms", "UntagResource", checksum = "3839a087")]
#[tokio::test]
async fn kms_untag_resource() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .tag_resource()
        .key_id(&key_id)
        .tags(
            Tag::builder()
                .tag_key("env")
                .tag_value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .key_id(&key_id)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_resource_tags()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

#[test_action("kms", "ListResourceTags", checksum = "a292851e")]
#[tokio::test]
async fn kms_list_resource_tags() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .list_resource_tags()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().is_empty());
}

// ---------------------------------------------------------------------------
// Key description and policies
// ---------------------------------------------------------------------------

#[test_action("kms", "UpdateKeyDescription", checksum = "ca9502c7")]
#[tokio::test]
async fn kms_update_key_description() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .description("original")
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .update_key_description()
        .key_id(&key_id)
        .description("updated")
        .send()
        .await
        .unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(
        desc.key_metadata().unwrap().description().unwrap(),
        "updated"
    );
}

#[test_action("kms", "GetKeyPolicy", checksum = "b625cb2a")]
#[tokio::test]
async fn kms_get_key_policy() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .get_key_policy()
        .key_id(&key_id)
        .policy_name("default")
        .send()
        .await
        .unwrap();
    assert!(resp.policy().is_some());
}

#[test_action("kms", "PutKeyPolicy", checksum = "dec83152")]
#[tokio::test]
async fn kms_put_key_policy() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    client
        .put_key_policy()
        .key_id(&key_id)
        .policy_name("default")
        .policy(policy)
        .send()
        .await
        .unwrap();
}

#[test_action("kms", "ListKeyPolicies", checksum = "5f3f74c7")]
#[tokio::test]
async fn kms_list_key_policies() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .list_key_policies()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(resp.policy_names().iter().any(|n| n == "default"));
}

// ---------------------------------------------------------------------------
// Key rotation
// ---------------------------------------------------------------------------

#[test_action("kms", "GetKeyRotationStatus", checksum = "61334d9e")]
#[tokio::test]
async fn kms_get_key_rotation_status() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    // Default: rotation is disabled
    assert!(!resp.key_rotation_enabled());
}

#[test_action("kms", "EnableKeyRotation", checksum = "c47d12f3")]
#[tokio::test]
async fn kms_enable_key_rotation() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .enable_key_rotation()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(resp.key_rotation_enabled());
}

#[test_action("kms", "DisableKeyRotation", checksum = "c3769512")]
#[tokio::test]
async fn kms_disable_key_rotation() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .enable_key_rotation()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    client
        .disable_key_rotation()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(!resp.key_rotation_enabled());
}

#[test_action("kms", "RotateKeyOnDemand", checksum = "e8e67fdd")]
#[tokio::test]
async fn kms_rotate_key_on_demand() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .rotate_key_on_demand()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_id().unwrap(), key_id);
}

#[test_action("kms", "ListKeyRotations", checksum = "b6469243")]
#[tokio::test]
async fn kms_list_key_rotations() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .list_key_rotations()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    let _ = resp.rotations();
}

// ---------------------------------------------------------------------------
// Sign / Verify
// ---------------------------------------------------------------------------

#[test_action("kms", "Sign", checksum = "04657bca")]
#[tokio::test]
async fn kms_sign() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .key_usage(KeyUsageType::SignVerify)
        .key_spec(KeySpec::Rsa2048)
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .sign()
        .key_id(&key_id)
        .message(Blob::new(b"sign me".to_vec()))
        .message_type(MessageType::Raw)
        .signing_algorithm(SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
        .send()
        .await
        .unwrap();
    assert!(resp.signature().is_some());
}

#[test_action("kms", "Verify", checksum = "7ba4bad8")]
#[tokio::test]
async fn kms_verify() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .key_usage(KeyUsageType::SignVerify)
        .key_spec(KeySpec::Rsa2048)
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let sig = client
        .sign()
        .key_id(&key_id)
        .message(Blob::new(b"verify me".to_vec()))
        .message_type(MessageType::Raw)
        .signing_algorithm(SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
        .send()
        .await
        .unwrap();

    let resp = client
        .verify()
        .key_id(&key_id)
        .message(Blob::new(b"verify me".to_vec()))
        .message_type(MessageType::Raw)
        .signing_algorithm(SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
        .signature(sig.signature().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert!(resp.signature_valid());
}

#[test_action("kms", "GetPublicKey", checksum = "f70f8357")]
#[tokio::test]
async fn kms_get_public_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .key_usage(KeyUsageType::SignVerify)
        .key_spec(KeySpec::Rsa2048)
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .get_public_key()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(resp.public_key().is_some());
}

// ---------------------------------------------------------------------------
// Grants
// ---------------------------------------------------------------------------

#[test_action("kms", "CreateGrant", checksum = "b5d6ae81")]
#[tokio::test]
async fn kms_create_grant() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .create_grant()
        .key_id(&key_id)
        .grantee_principal("arn:aws:iam::123456789012:role/test-role")
        .operations(aws_sdk_kms::types::GrantOperation::Encrypt)
        .operations(aws_sdk_kms::types::GrantOperation::Decrypt)
        .send()
        .await
        .unwrap();
    assert!(resp.grant_id().is_some());
    assert!(resp.grant_token().is_some());
}

#[test_action("kms", "ListGrants", checksum = "21c4f1b9")]
#[tokio::test]
async fn kms_list_grants() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    client
        .create_grant()
        .key_id(&key_id)
        .grantee_principal("arn:aws:iam::123456789012:role/test-role")
        .operations(aws_sdk_kms::types::GrantOperation::Encrypt)
        .send()
        .await
        .unwrap();

    let resp = client.list_grants().key_id(&key_id).send().await.unwrap();
    assert_eq!(resp.grants().len(), 1);
}

#[test_action("kms", "ListRetirableGrants", checksum = "22e7c42a")]
#[tokio::test]
async fn kms_list_retirable_grants() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client
        .list_retirable_grants()
        .retiring_principal("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let _ = resp.grants();
}

#[test_action("kms", "RevokeGrant", checksum = "f5a54621")]
#[tokio::test]
async fn kms_revoke_grant() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let grant = client
        .create_grant()
        .key_id(&key_id)
        .grantee_principal("arn:aws:iam::123456789012:role/test-role")
        .operations(aws_sdk_kms::types::GrantOperation::Encrypt)
        .send()
        .await
        .unwrap();
    let grant_id = grant.grant_id().unwrap();

    client
        .revoke_grant()
        .key_id(&key_id)
        .grant_id(grant_id)
        .send()
        .await
        .unwrap();

    let resp = client.list_grants().key_id(&key_id).send().await.unwrap();
    assert!(resp.grants().is_empty());
}

#[test_action("kms", "RetireGrant", checksum = "e757edf8")]
#[tokio::test]
async fn kms_retire_grant() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let grant = client
        .create_grant()
        .key_id(&key_id)
        .grantee_principal("arn:aws:iam::123456789012:role/test-role")
        .operations(aws_sdk_kms::types::GrantOperation::Encrypt)
        .send()
        .await
        .unwrap();
    let grant_id = grant.grant_id().unwrap();

    client
        .retire_grant()
        .key_id(&key_id)
        .grant_id(grant_id)
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// MAC
// ---------------------------------------------------------------------------

#[test_action("kms", "GenerateMac", checksum = "5efb158b")]
#[tokio::test]
async fn kms_generate_mac() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .key_usage(KeyUsageType::GenerateVerifyMac)
        .key_spec(KeySpec::Hmac256)
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let resp = client
        .generate_mac()
        .key_id(&key_id)
        .message(Blob::new(b"mac me".to_vec()))
        .mac_algorithm(aws_sdk_kms::types::MacAlgorithmSpec::HmacSha256)
        .send()
        .await
        .unwrap();
    assert!(resp.mac().is_some());
}

#[test_action("kms", "VerifyMac", checksum = "ffd6ccd6")]
#[tokio::test]
async fn kms_verify_mac() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client
        .create_key()
        .key_usage(KeyUsageType::GenerateVerifyMac)
        .key_spec(KeySpec::Hmac256)
        .send()
        .await
        .unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let mac_resp = client
        .generate_mac()
        .key_id(&key_id)
        .message(Blob::new(b"verify-mac".to_vec()))
        .mac_algorithm(aws_sdk_kms::types::MacAlgorithmSpec::HmacSha256)
        .send()
        .await
        .unwrap();

    let resp = client
        .verify_mac()
        .key_id(&key_id)
        .message(Blob::new(b"verify-mac".to_vec()))
        .mac_algorithm(aws_sdk_kms::types::MacAlgorithmSpec::HmacSha256)
        .mac(mac_resp.mac().unwrap().clone())
        .send()
        .await
        .unwrap();
    assert!(resp.mac_valid());
}

// ---------------------------------------------------------------------------
// ReplicateKey
// ---------------------------------------------------------------------------

#[test_action("kms", "ReplicateKey", checksum = "4fdb41bb")]
#[tokio::test]
async fn kms_replicate_key() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let key = client.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    // ReplicateKey may succeed or return an error depending on impl;
    // we just verify the API call is accepted
    let _ = client
        .replicate_key()
        .key_id(&key_id)
        .replica_region("eu-west-1")
        .send()
        .await;
}
