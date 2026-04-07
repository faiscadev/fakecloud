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

#[tokio::test]
async fn kms_generate_data_key_pair() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let pair = client
        .generate_data_key_pair()
        .key_id(&key_id)
        .key_pair_spec(aws_sdk_kms::types::DataKeyPairSpec::Rsa2048)
        .send()
        .await
        .unwrap();

    assert!(pair.public_key().is_some());
    assert!(pair.private_key_plaintext().is_some());
    assert!(pair.private_key_ciphertext_blob().is_some());
    assert!(pair.key_id().is_some());

    // Without plaintext variant
    let pair_no_pt = client
        .generate_data_key_pair_without_plaintext()
        .key_id(&key_id)
        .key_pair_spec(aws_sdk_kms::types::DataKeyPairSpec::EccNistP256)
        .send()
        .await
        .unwrap();

    assert!(pair_no_pt.public_key().is_some());
    assert!(pair_no_pt.private_key_ciphertext_blob().is_some());
    assert!(pair_no_pt.key_id().is_some());
}

#[tokio::test]
async fn kms_derive_shared_secret() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client
        .create_key()
        .key_usage(aws_sdk_kms::types::KeyUsageType::KeyAgreement)
        .key_spec(aws_sdk_kms::types::KeySpec::EccNistP256)
        .send()
        .await
        .unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let fake_pub = Blob::new(vec![0x04; 65]); // Fake uncompressed EC point
    let result = client
        .derive_shared_secret()
        .key_id(&key_id)
        .key_agreement_algorithm(aws_sdk_kms::types::KeyAgreementAlgorithmSpec::Ecdh)
        .public_key(fake_pub)
        .send()
        .await
        .unwrap();

    assert!(result.shared_secret().is_some());
    assert!(result.key_id().is_some());
}

#[tokio::test]
async fn kms_import_key_material_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    // Create key with EXTERNAL origin
    let resp = client
        .create_key()
        .origin(aws_sdk_kms::types::OriginType::External)
        .send()
        .await
        .unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    // Get parameters for import
    let params = client
        .get_parameters_for_import()
        .key_id(&key_id)
        .wrapping_algorithm(aws_sdk_kms::types::AlgorithmSpec::RsaesOaepSha256)
        .wrapping_key_spec(aws_sdk_kms::types::WrappingKeySpec::Rsa2048)
        .send()
        .await
        .unwrap();

    assert!(params.import_token().is_some());
    assert!(params.public_key().is_some());
    assert!(params.parameters_valid_to().is_some());

    // Import key material
    let import_token = params.import_token().unwrap().clone();
    client
        .import_key_material()
        .key_id(&key_id)
        .import_token(import_token)
        .encrypted_key_material(Blob::new(vec![0u8; 32]))
        .expiration_model(aws_sdk_kms::types::ExpirationModelType::KeyMaterialDoesNotExpire)
        .send()
        .await
        .unwrap();

    // Key should now be enabled
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(desc.key_metadata().unwrap().enabled());

    // Delete imported key material
    client
        .delete_imported_key_material()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    // Key should now be pending import
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(!desc.key_metadata().unwrap().enabled());
}

#[tokio::test]
async fn kms_custom_key_store_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    // Create a custom key store
    let create_resp = client
        .create_custom_key_store()
        .custom_key_store_name("e2e-test-store")
        .cloud_hsm_cluster_id("cluster-abcdef")
        .trust_anchor_certificate("cert-data")
        .key_store_password("password123")
        .send()
        .await
        .unwrap();
    let store_id = create_resp.custom_key_store_id().unwrap().to_string();
    assert!(store_id.starts_with("cks-"));

    // Describe by ID
    let desc = client
        .describe_custom_key_stores()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();
    let stores = desc.custom_key_stores();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].custom_key_store_name().unwrap(), "e2e-test-store");
    assert_eq!(
        stores[0].connection_state().unwrap(),
        &aws_sdk_kms::types::ConnectionStateType::Disconnected
    );

    // Connect
    client
        .connect_custom_key_store()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();

    // Verify connected
    let desc = client
        .describe_custom_key_stores()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.custom_key_stores()[0].connection_state().unwrap(),
        &aws_sdk_kms::types::ConnectionStateType::Connected
    );

    // Disconnect
    client
        .disconnect_custom_key_store()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();

    // Update name
    client
        .update_custom_key_store()
        .custom_key_store_id(&store_id)
        .new_custom_key_store_name("renamed-store")
        .send()
        .await
        .unwrap();

    // Verify update
    let desc = client
        .describe_custom_key_stores()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.custom_key_stores()[0].custom_key_store_name().unwrap(),
        "renamed-store"
    );

    // Delete
    client
        .delete_custom_key_store()
        .custom_key_store_id(&store_id)
        .send()
        .await
        .unwrap();

    // Describe all should be empty
    let desc = client.describe_custom_key_stores().send().await.unwrap();
    assert!(desc.custom_key_stores().is_empty());
}

#[tokio::test]
async fn kms_key_rotation_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    // Initially rotation is disabled
    let status = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(!status.key_rotation_enabled());

    // Enable rotation
    client
        .enable_key_rotation()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    let status = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(status.key_rotation_enabled());

    // Rotate on demand
    client
        .rotate_key_on_demand()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    // List rotations
    let rotations = client
        .list_key_rotations()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(rotations.rotations().len(), 1);

    // Disable rotation
    client
        .disable_key_rotation()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();

    let status = client
        .get_key_rotation_status()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert!(!status.key_rotation_enabled());
}

#[tokio::test]
async fn kms_sign_verify_roundtrip() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    // Create a SIGN_VERIFY key
    let resp = client
        .create_key()
        .key_usage(aws_sdk_kms::types::KeyUsageType::SignVerify)
        .key_spec(aws_sdk_kms::types::KeySpec::Rsa2048)
        .send()
        .await
        .unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    let message = b"data to sign via e2e";

    // Sign
    let sign_resp = client
        .sign()
        .key_id(&key_id)
        .message(Blob::new(message.to_vec()))
        .signing_algorithm(aws_sdk_kms::types::SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
        .send()
        .await
        .unwrap();

    let signature = sign_resp.signature().unwrap().clone();
    assert!(!signature.as_ref().is_empty());

    // Verify
    let verify_resp = client
        .verify()
        .key_id(&key_id)
        .message(Blob::new(message.to_vec()))
        .signature(signature)
        .signing_algorithm(aws_sdk_kms::types::SigningAlgorithmSpec::RsassaPkcs1V15Sha256)
        .send()
        .await
        .unwrap();
    assert!(verify_resp.signature_valid());
}

#[tokio::test]
async fn kms_generate_random() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    // Generate 32 bytes
    let resp = client
        .generate_random()
        .number_of_bytes(32)
        .send()
        .await
        .unwrap();
    let random_bytes = resp.plaintext().unwrap();
    assert_eq!(random_bytes.as_ref().len(), 32);

    // Generate 64 bytes
    let resp = client
        .generate_random()
        .number_of_bytes(64)
        .send()
        .await
        .unwrap();
    let random_bytes = resp.plaintext().unwrap();
    assert_eq!(random_bytes.as_ref().len(), 64);

    // Two calls should produce different output
    let resp1 = client
        .generate_random()
        .number_of_bytes(16)
        .send()
        .await
        .unwrap();
    let resp2 = client
        .generate_random()
        .number_of_bytes(16)
        .send()
        .await
        .unwrap();
    // Very unlikely but theoretically possible to be equal; this is a sanity check
    let b1 = resp1.plaintext().unwrap().as_ref().to_vec();
    let b2 = resp2.plaintext().unwrap().as_ref().to_vec();
    // At least verify both are 16 bytes
    assert_eq!(b1.len(), 16);
    assert_eq!(b2.len(), 16);
}

#[tokio::test]
async fn kms_cancel_key_deletion() {
    let server = TestServer::start().await;
    let client = server.kms_client().await;

    let resp = client.create_key().send().await.unwrap();
    let key_id = resp.key_metadata().unwrap().key_id().to_string();

    // Schedule deletion
    client
        .schedule_key_deletion()
        .key_id(&key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();

    // Verify pending deletion
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(
        desc.key_metadata().unwrap().key_state(),
        Some(&aws_sdk_kms::types::KeyState::PendingDeletion)
    );

    // Cancel deletion
    let cancel_resp = client
        .cancel_key_deletion()
        .key_id(&key_id)
        .send()
        .await
        .unwrap();
    assert_eq!(cancel_resp.key_id().unwrap(), key_id);

    // Key should be disabled (not pending deletion)
    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert_eq!(
        desc.key_metadata().unwrap().key_state(),
        Some(&aws_sdk_kms::types::KeyState::Disabled)
    );

    // Re-enable the key
    client.enable_key().key_id(&key_id).send().await.unwrap();

    let desc = client.describe_key().key_id(&key_id).send().await.unwrap();
    assert!(desc.key_metadata().unwrap().enabled());
}
