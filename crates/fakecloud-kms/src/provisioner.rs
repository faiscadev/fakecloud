//! Public helpers for provisioning KMS keys from outside the
//! `fakecloud-kms` crate (e.g. CloudFormation `AWS::KMS::Key` /
//! `AWS::KMS::ReplicaKey`). This module exposes a single
//! [`build_kms_key`] factory that mirrors `CreateKey`'s behaviour
//! without going through the AWS request/response machinery, plus
//! [`provision_key`] / [`provision_replica_key`] / [`provision_alias`]
//! which take a `SharedKmsState` write lock and insert the resulting
//! records.
//!
//! Keeping the asymmetric keypair generation, signing-algorithm
//! tables, and policy defaulting in one place means CFN provisioning
//! produces keys that are byte-compatible with what `CreateKey`
//! itself would have produced — no asymmetric stubs, no missing
//! `signing_algorithms`, no missing public key DER for `GetPublicKey`.

use std::collections::BTreeMap;

use chrono::Utc;
use fakecloud_aws::arn::Arn;
use uuid::Uuid;

use super::asym;
use super::asym_ecdsa;
use super::helpers::{
    default_key_policy, encryption_algorithms_for_key, mac_algorithms_for_key_spec, rand_bytes,
    signing_algorithms_for_key_spec,
};
use crate::state::{KmsAlias, KmsKey, SharedKmsState};

/// Inputs accepted by [`build_kms_key`]. Mirrors the subset of
/// `AWS::KMS::Key` properties + `CreateKey` request fields that
/// fakecloud honours. Defaults match AWS: `key_usage =
/// ENCRYPT_DECRYPT`, `key_spec = SYMMETRIC_DEFAULT`, `origin =
/// AWS_KMS`, `enabled = true`.
#[derive(Debug, Clone)]
pub struct KeyCreationInput {
    pub description: String,
    pub key_usage: String,
    pub key_spec: String,
    pub origin: String,
    pub enabled: bool,
    pub multi_region: bool,
    pub key_rotation_enabled: bool,
    pub policy: Option<String>,
    pub tags: BTreeMap<String, String>,
}

impl Default for KeyCreationInput {
    fn default() -> Self {
        Self {
            description: String::new(),
            key_usage: "ENCRYPT_DECRYPT".to_string(),
            key_spec: "SYMMETRIC_DEFAULT".to_string(),
            origin: "AWS_KMS".to_string(),
            enabled: true,
            multi_region: false,
            key_rotation_enabled: false,
            policy: None,
            tags: BTreeMap::new(),
        }
    }
}

/// AWS KMS key specs accepted across `CreateKey` / CFN.
pub const VALID_KEY_SPECS: &[&str] = &[
    "SYMMETRIC_DEFAULT",
    "RSA_2048",
    "RSA_3072",
    "RSA_4096",
    "ECC_NIST_P256",
    "ECC_NIST_P384",
    "ECC_NIST_P521",
    "ECC_SECG_P256K1",
    "HMAC_224",
    "HMAC_256",
    "HMAC_384",
    "HMAC_512",
    "SM2",
];

/// AWS KMS key usages accepted across `CreateKey` / CFN.
pub const VALID_KEY_USAGES: &[&str] = &[
    "ENCRYPT_DECRYPT",
    "SIGN_VERIFY",
    "GENERATE_VERIFY_MAC",
    "KEY_AGREEMENT",
];

/// Validate that a `(key_usage, key_spec)` pair is supported by
/// fakecloud's KMS implementation. Mirrors the constraints the real
/// AWS service enforces: e.g. only HMAC specs allow
/// `GENERATE_VERIFY_MAC`, and only ECC specs allow `KEY_AGREEMENT`.
pub fn validate_key_usage_for_spec(key_usage: &str, key_spec: &str) -> Result<(), String> {
    if !VALID_KEY_USAGES.contains(&key_usage) {
        return Err(format!("Unsupported KeyUsage: {key_usage}"));
    }
    if !VALID_KEY_SPECS.contains(&key_spec) {
        return Err(format!("Unsupported KeySpec: {key_spec}"));
    }
    let is_symmetric = key_spec == "SYMMETRIC_DEFAULT";
    let is_hmac = key_spec.starts_with("HMAC_");
    let is_rsa = key_spec.starts_with("RSA_");
    let is_ecc = key_spec.starts_with("ECC_");
    let is_sm2 = key_spec == "SM2";
    match key_usage {
        "ENCRYPT_DECRYPT" => {
            if !(is_symmetric || is_rsa || is_sm2) {
                return Err(format!(
                    "KeySpec {key_spec} does not support KeyUsage ENCRYPT_DECRYPT"
                ));
            }
        }
        "SIGN_VERIFY" => {
            if !(is_rsa || is_ecc || is_sm2) {
                return Err(format!(
                    "KeySpec {key_spec} does not support KeyUsage SIGN_VERIFY"
                ));
            }
        }
        "GENERATE_VERIFY_MAC" => {
            if !is_hmac {
                return Err(format!(
                    "KeySpec {key_spec} does not support KeyUsage GENERATE_VERIFY_MAC"
                ));
            }
        }
        "KEY_AGREEMENT" => {
            if !is_ecc {
                return Err(format!(
                    "KeySpec {key_spec} does not support KeyUsage KEY_AGREEMENT"
                ));
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Build a fully-populated [`KmsKey`] for the given inputs. Generates
/// asymmetric keypairs (RSA / ECDSA) when the spec calls for it,
/// computes the matching `signing_algorithms` /
/// `encryption_algorithms` / `mac_algorithms` tables, and falls back
/// to [`default_key_policy`] when no policy is supplied.
///
/// `region` and `account_id` are used to mint the ARN. Caller is
/// responsible for inserting the result into the appropriate
/// `KmsState.keys` map under `key_id`.
pub fn build_kms_key(
    region: &str,
    account_id: &str,
    input: &KeyCreationInput,
) -> Result<KmsKey, String> {
    validate_key_usage_for_spec(&input.key_usage, &input.key_spec)?;

    let key_id = if input.multi_region {
        format!("mrk-{}", Uuid::new_v4().as_simple())
    } else {
        Uuid::new_v4().to_string()
    };
    let arn = Arn::new("kms", region, account_id, &format!("key/{key_id}")).to_string();
    let now = Utc::now().timestamp() as f64;

    let signing_algs = if input.key_usage == "SIGN_VERIFY" {
        signing_algorithms_for_key_spec(&input.key_spec)
    } else {
        None
    };
    let encryption_algs = encryption_algorithms_for_key(&input.key_usage, &input.key_spec);
    let mac_algs = if input.key_usage == "GENERATE_VERIFY_MAC" {
        mac_algorithms_for_key_spec(&input.key_spec)
    } else {
        None
    };

    let mut asym_priv: Option<Vec<u8>> = None;
    let mut asym_pub: Option<Vec<u8>> = None;
    if let Some((p, k)) =
        asym::generate_keypair(&input.key_spec).map_err(|e| format!("rsa keygen failed: {e}"))?
    {
        asym_priv = Some(p);
        asym_pub = Some(k);
    } else if let Some((p, k)) = asym_ecdsa::generate_keypair(&input.key_spec)
        .map_err(|e| format!("ecdsa keygen failed: {e}"))?
    {
        asym_priv = Some(p);
        asym_pub = Some(k);
    }

    let policy = input
        .policy
        .clone()
        .unwrap_or_else(|| default_key_policy(account_id));

    Ok(KmsKey {
        key_id,
        arn,
        creation_date: now,
        description: input.description.clone(),
        enabled: input.enabled,
        key_usage: input.key_usage.clone(),
        key_spec: input.key_spec.clone(),
        key_manager: "CUSTOMER".to_string(),
        key_state: if input.enabled { "Enabled" } else { "Disabled" }.to_string(),
        deletion_date: None,
        tags: input.tags.clone(),
        policy,
        key_rotation_enabled: input.key_rotation_enabled,
        origin: input.origin.clone(),
        multi_region: input.multi_region,
        rotations: Vec::new(),
        signing_algorithms: signing_algs,
        encryption_algorithms: encryption_algs,
        mac_algorithms: mac_algs,
        custom_key_store_id: None,
        imported_key_material: false,
        imported_material_bytes: None,
        private_key_seed: rand_bytes(32),
        primary_region: None,
        asymmetric_private_key_der: asym_priv,
        asymmetric_public_key_der: asym_pub,
    })
}

/// Build and insert a key into shared state. Returns
/// `(key_id, arn)`.
pub fn provision_key(
    state: &SharedKmsState,
    account_id: &str,
    input: &KeyCreationInput,
) -> Result<(String, String), String> {
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let region = s.region.clone();
    let key = build_kms_key(&region, account_id, input)?;
    let key_id = key.key_id.clone();
    let arn = key.arn.clone();
    s.keys.insert(key_id.clone(), key);
    Ok((key_id, arn))
}

/// Provision a multi-region replica of an existing primary key.
/// Looks the primary up in the same account state, validates that
/// it's `MultiRegion=true`, mints a `mrk-replica-` id (fakecloud is
/// single-region so colliding IDs would overwrite the primary), and
/// inserts the replica with `primary_region` set so `DescribeKey`
/// reports `MultiRegionKeyType=REPLICA`.
pub fn provision_replica_key(
    state: &SharedKmsState,
    account_id: &str,
    primary_arn: &str,
    description: Option<String>,
    enabled: bool,
    policy: Option<String>,
    tags: BTreeMap<String, String>,
) -> Result<(String, String), String> {
    let parts: Vec<&str> = primary_arn.split(':').collect();
    if parts.len() < 6 {
        return Err(format!("Invalid PrimaryKeyArn: {primary_arn}"));
    }
    let primary_region = parts[3].to_string();
    let key_id = parts[5]
        .strip_prefix("key/")
        .ok_or_else(|| format!("PrimaryKeyArn missing key/ segment: {primary_arn}"))?
        .to_string();

    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let region = s.region.clone();
    // Source must be a multi-region key in the primary account; look
    // it up either by raw key_id (when the primary lives in this
    // state's region) or via the region-keyed slot.
    let source_storage_keys = [key_id.clone(), format!("{primary_region}:{key_id}")];
    let source = source_storage_keys
        .iter()
        .find_map(|k| s.keys.get(k).cloned())
        .ok_or_else(|| format!("Primary key {primary_arn} does not exist"))?;
    if !source.multi_region {
        return Err(format!(
            "Primary key {primary_arn} is not a multi-region key"
        ));
    }

    let replica_key_id = format!("mrk-replica-{}", Uuid::new_v4().as_simple());
    let replica_arn =
        Arn::new("kms", &region, account_id, &format!("key/{replica_key_id}")).to_string();
    let mut replica = source;
    replica.key_id = replica_key_id.clone();
    replica.arn = replica_arn.clone();
    if let Some(d) = description {
        if !d.is_empty() {
            replica.description = d;
        }
    }
    replica.enabled = enabled;
    replica.key_state = if enabled { "Enabled" } else { "Disabled" }.to_string();
    if let Some(p) = policy {
        if !p.is_empty() {
            replica.policy = p;
        }
    }
    if !tags.is_empty() {
        replica.tags.extend(tags);
    }
    replica.deletion_date = None;
    replica.key_rotation_enabled = false;
    replica.multi_region = true;
    replica.rotations = Vec::new();
    replica.custom_key_store_id = None;
    replica.imported_key_material = false;
    replica.imported_material_bytes = None;
    replica.primary_region = Some(primary_region);

    s.keys.insert(replica_key_id.clone(), replica);
    Ok((replica_key_id, replica_arn))
}

/// Insert (or replace) an alias pointing at `target_key_id`. Resolves
/// `target_input` against either a raw key id or a key ARN. Returns
/// the alias name on success.
pub fn provision_alias(
    state: &SharedKmsState,
    account_id: &str,
    alias_name: &str,
    target_input: &str,
) -> Result<String, String> {
    if !alias_name.starts_with("alias/") {
        return Err(format!(
            "AliasName must start with 'alias/'; got '{alias_name}'"
        ));
    }
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let target_key_id = if s.keys.contains_key(target_input) {
        target_input.to_string()
    } else if let Some(id) = target_input
        .strip_prefix("arn:aws:kms:")
        .and_then(|rest| rest.split(":key/").nth(1))
    {
        if s.keys.contains_key(id) {
            id.to_string()
        } else {
            return Err(format!("KMS key '{target_input}' does not exist"));
        }
    } else {
        return Err(format!("KMS key '{target_input}' does not exist"));
    };
    let alias_arn = Arn::new("kms", &s.region, &s.account_id, alias_name).to_string();
    let alias = KmsAlias {
        alias_name: alias_name.to_string(),
        alias_arn,
        target_key_id,
        creation_date: Utc::now().timestamp() as f64,
    };
    s.aliases.insert(alias_name.to_string(), alias);
    Ok(alias_name.to_string())
}

/// Mutable updates to apply to an existing key. Each `Option` field
/// is "leave alone if `None`, replace with this value if `Some`",
/// mirroring the AWS update semantics where unspecified fields are
/// untouched. Properties that AWS treats as immutable (`KeySpec`,
/// `KeyUsage`, `Origin`, `MultiRegion`) aren't representable here —
/// the caller is expected to detect those changes and trigger
/// resource replacement.
#[derive(Debug, Default, Clone)]
pub struct KeyUpdate {
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub key_rotation_enabled: Option<bool>,
    pub policy: Option<String>,
    pub tags: Option<BTreeMap<String, String>>,
}

/// Apply a [`KeyUpdate`] to the key with `key_id`. Returns an error
/// if the key isn't found in `state`.
pub fn update_key_properties(
    state: &SharedKmsState,
    account_id: &str,
    key_id: &str,
    update: KeyUpdate,
) -> Result<(), String> {
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let key = s
        .keys
        .get_mut(key_id)
        .ok_or_else(|| format!("Key '{key_id}' does not exist"))?;
    if let Some(d) = update.description {
        key.description = d;
    }
    if let Some(e) = update.enabled {
        key.enabled = e;
        key.key_state = if e { "Enabled" } else { "Disabled" }.to_string();
    }
    if let Some(r) = update.key_rotation_enabled {
        key.key_rotation_enabled = r;
    }
    if let Some(p) = update.policy {
        if !p.is_empty() {
            key.policy = p;
        }
    }
    if let Some(t) = update.tags {
        key.tags = t;
    }
    Ok(())
}

/// Repoint an existing alias at a different target key. Used by
/// `update_resource` for `AWS::KMS::Alias` when only `TargetKeyId`
/// changes.
pub fn update_alias_target(
    state: &SharedKmsState,
    account_id: &str,
    alias_name: &str,
    target_input: &str,
) -> Result<(), String> {
    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id);
    let target_key_id = if s.keys.contains_key(target_input) {
        target_input.to_string()
    } else if let Some(id) = target_input
        .strip_prefix("arn:aws:kms:")
        .and_then(|rest| rest.split(":key/").nth(1))
    {
        if s.keys.contains_key(id) {
            id.to_string()
        } else {
            return Err(format!("KMS key '{target_input}' does not exist"));
        }
    } else {
        return Err(format!("KMS key '{target_input}' does not exist"));
    };
    let alias = s
        .aliases
        .get_mut(alias_name)
        .ok_or_else(|| format!("Alias '{alias_name}' does not exist"))?;
    alias.target_key_id = target_key_id;
    Ok(())
}
