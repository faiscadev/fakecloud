use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedKmsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<KmsState>>>;

/// A KMS key's ARN, in the partition of `region`.
pub fn kms_key_arn(region: &str, account_id: &str, key_id: &str) -> String {
    fakecloud_aws::arn::Arn::regional("kms", region, account_id, &format!("key/{key_id}"))
        .to_string()
}

/// A KMS alias's ARN (`alias_name` is the full `alias/<name>`), in the
/// partition of `region`.
pub fn kms_alias_arn(region: &str, account_id: &str, alias_name: &str) -> String {
    fakecloud_aws::arn::Arn::regional("kms", region, account_id, alias_name).to_string()
}

/// The `<region>:<account>:<resource>` part of a KMS ARN in any partition,
/// split into its fields. `None` when `arn` is not a KMS ARN.
pub fn parse_kms_arn(arn: &str) -> Option<(&str, &str, &str)> {
    let rest = fakecloud_aws::arn::arn_resource(arn, "kms")?;
    let (region, after_region) = rest.split_once(':')?;
    let (account, resource) = after_region.split_once(':')?;
    Some((region, account, resource))
}

impl fakecloud_core::multi_account::AccountState for KmsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// Aliases by region, then by alias name (`alias/<name>`). An alias is a
/// regional resource: the same name can exist independently in every region,
/// pointing at a key in that region. The inner map is name-ordered, which is
/// the order `ListAliases` pages through.
pub type RegionalAliases = BTreeMap<String, BTreeMap<String, KmsAlias>>;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "KmsStateRepr")]
pub struct KmsState {
    pub account_id: String,
    pub region: String,
    pub keys: BTreeMap<String, KmsKey>,
    /// See [`RegionalAliases`]; use the `alias*` accessors rather than the
    /// map directly.
    pub aliases: RegionalAliases,
    pub grants: Vec<KmsGrant>,
    pub custom_key_stores: BTreeMap<String, CustomKeyStore>,
    /// Per-account master key bytes (32 bytes for AES-256-GCM) used to
    /// wrap plaintext in the AWS-shaped ciphertext blob format. Generated
    /// lazily on first encrypt and persisted alongside the rest of the
    /// state so that ciphertexts produced before a server restart still
    /// decrypt afterwards.
    #[serde(default = "default_master_key_bytes_on_load")]
    pub master_key_bytes: Vec<u8>,
    /// In-flight RSA wrapping keypairs handed out by GetParametersForImport
    /// and consumed by ImportKeyMaterial to RSA-OAEP-unwrap the encrypted
    /// key material. Keyed by the import token bytes returned to the
    /// caller; entries are removed after a successful import.
    #[serde(default)]
    pub import_wrapping_keys: BTreeMap<String, ImportWrapEntry>,
    /// AWS-managed keys minted for other services, by
    /// [`aws_managed_key_slot`] (region + `alias/aws/<service>`) -> key id.
    /// AWS-managed keys exist once per account AND region; each region's
    /// `alias/aws/<service>` alias targets the key recorded here for it.
    #[serde(default)]
    pub aws_managed_keys: BTreeMap<String, String>,
    /// Whether this account's state predates the KMS hook binding each
    /// service's encryption context into its ciphertexts. Such a snapshot
    /// may hold service ciphertexts (SQS messages, S3 objects, secrets)
    /// sealed with no context, which the hook keeps decrypting; state
    /// created since never accepts a context-free ciphertext where a context
    /// is given, as KMS does not. `false` for new state, `true` for a
    /// snapshot written before the field existed.
    #[serde(default)]
    pub legacy_context_free_ciphertexts: bool,
}

/// The persisted shape of [`KmsState`]. Identical except that `aliases` may
/// also be the pre-regional form (one account-wide map keyed by alias name),
/// which [`From`] migrates into [`RegionalAliases`].
#[derive(serde::Deserialize)]
struct KmsStateRepr {
    account_id: String,
    region: String,
    keys: BTreeMap<String, KmsKey>,
    aliases: PersistedAliases,
    grants: Vec<KmsGrant>,
    custom_key_stores: BTreeMap<String, CustomKeyStore>,
    #[serde(default = "default_master_key_bytes_on_load")]
    master_key_bytes: Vec<u8>,
    #[serde(default)]
    import_wrapping_keys: BTreeMap<String, ImportWrapEntry>,
    #[serde(default)]
    aws_managed_keys: BTreeMap<String, String>,
    /// Missing from snapshots written before the hook bound encryption
    /// contexts: those may hold context-free service ciphertexts.
    #[serde(default = "legacy_ciphertexts_on_load")]
    legacy_context_free_ciphertexts: bool,
}

fn legacy_ciphertexts_on_load() -> bool {
    true
}

/// `aliases` as found in a snapshot: region -> name -> alias (schema 3+), or
/// the account-wide name -> alias map written before aliases were regional.
/// The two never parse as each other: a legacy value is an alias object, a
/// regional value is a map of alias objects.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PersistedAliases {
    Regional(RegionalAliases),
    Legacy(BTreeMap<String, KmsAlias>),
}

impl From<KmsStateRepr> for KmsState {
    fn from(r: KmsStateRepr) -> Self {
        let mut state = KmsState {
            account_id: r.account_id,
            region: r.region,
            keys: r.keys,
            aliases: RegionalAliases::new(),
            grants: r.grants,
            custom_key_stores: r.custom_key_stores,
            master_key_bytes: r.master_key_bytes,
            import_wrapping_keys: r.import_wrapping_keys,
            aws_managed_keys: r.aws_managed_keys,
            legacy_context_free_ciphertexts: r.legacy_context_free_ciphertexts,
        };
        match r.aliases {
            PersistedAliases::Regional(aliases) => state.aliases = aliases,
            PersistedAliases::Legacy(aliases) => state.migrate_legacy_aliases(aliases),
        }
        crate::hook::migrate_aws_managed_key_policies(&mut state);
        state
    }
}

/// The [`KmsState::aws_managed_keys`] slot of the AWS-managed key behind
/// `alias_name` (`alias/aws/<service>`) in `region`.
pub fn aws_managed_key_slot(region: &str, alias_name: &str) -> String {
    format!("{region}/{alias_name}")
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportWrapEntry {
    /// PKCS#8 DER-encoded RSA-2048 private key half of the wrapping
    /// keypair. The corresponding SubjectPublicKeyInfo DER was returned
    /// to the caller in `GetParametersForImport.PublicKey` so they can
    /// RSA-OAEP-encrypt the key material under it.
    pub private_key_der: Vec<u8>,
    /// CMK key id this token is bound to. ImportKeyMaterial rejects the
    /// token when it doesn't match the CMK in the request.
    pub key_id: String,
}

fn default_master_key_bytes() -> Vec<u8> {
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::aead::OsRng;
    let mut bytes = vec![0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
}

/// serde `default` for [`KmsState::master_key_bytes`], invoked ONLY when a
/// persisted snapshot is deserialized WITHOUT the field — e.g. a state file
/// written by a fakecloud build predating per-account master keys. Silently
/// regenerating the key would make every ciphertext blob produced by that
/// older build undecryptable (`Decrypt` -> `InvalidCiphertextException`) with
/// no indication why, so we log a loud warning to surface the data-integrity
/// break. Fresh accounts go through [`KmsState::new`], which calls
/// [`default_master_key_bytes`] directly and stays quiet — only the on-load
/// default path warns.
fn default_master_key_bytes_on_load() -> Vec<u8> {
    tracing::warn!(
        "KMS state snapshot was missing the per-account master key; generating \
         a fresh one. Ciphertext produced before this upgrade can NO LONGER be \
         decrypted (Decrypt will fail with InvalidCiphertextException)."
    );
    default_master_key_bytes()
}

impl KmsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            keys: BTreeMap::new(),
            aliases: BTreeMap::new(),
            grants: Vec::new(),
            custom_key_stores: BTreeMap::new(),
            master_key_bytes: default_master_key_bytes(),
            import_wrapping_keys: BTreeMap::new(),
            aws_managed_keys: BTreeMap::new(),
            legacy_context_free_ciphertexts: false,
        }
    }

    /// The alias `name` (`alias/<name>`) in `region`.
    pub fn alias(&self, region: &str, name: &str) -> Option<&KmsAlias> {
        self.aliases.get(region)?.get(name)
    }

    pub fn alias_mut(&mut self, region: &str, name: &str) -> Option<&mut KmsAlias> {
        self.aliases.get_mut(region)?.get_mut(name)
    }

    /// Insert (or replace) `alias` in `region`.
    pub fn insert_alias(&mut self, region: &str, alias: KmsAlias) {
        self.aliases
            .entry(region.to_string())
            .or_default()
            .insert(alias.alias_name.clone(), alias);
    }

    pub fn remove_alias(&mut self, region: &str, name: &str) -> Option<KmsAlias> {
        let in_region = self.aliases.get_mut(region)?;
        let removed = in_region.remove(name);
        if in_region.is_empty() {
            self.aliases.remove(region);
        }
        removed
    }

    /// Every alias in `region`, in alias-name order.
    pub fn aliases_in(&self, region: &str) -> impl Iterator<Item = &KmsAlias> {
        self.aliases
            .get(region)
            .into_iter()
            .flat_map(|m| m.values())
    }

    /// Drop every alias, in any region, for which `keep` returns `false`.
    pub fn retain_aliases(&mut self, mut keep: impl FnMut(&KmsAlias) -> bool) {
        for in_region in self.aliases.values_mut() {
            in_region.retain(|_, a| keep(a));
        }
        self.aliases.retain(|_, in_region| !in_region.is_empty());
    }

    /// Resolve a key id, key ARN, alias name or alias ARN to the stored key
    /// id. Aliases are regional: an alias name resolves in `region` (the
    /// caller's region), an alias ARN in the region it names.
    pub fn resolve_key_id(&self, region: &str, key_id_or_arn: &str) -> Option<String> {
        // Direct key ID
        if self.keys.contains_key(key_id_or_arn) {
            return Some(key_id_or_arn.to_string());
        }

        if let Some((arn_region, _account, resource)) = parse_kms_arn(key_id_or_arn) {
            if let Some(id) = resource.strip_prefix("key/") {
                // Multi-region replicas are stored under a region-scoped
                // composite key ("{region}:{id}") because the primary and
                // every replica share the same bare id. Resolve the ARN's
                // own region first so a DescribeKey on a replica ARN returns
                // the replica entry, not the primary that also matches `id`.
                let scoped = format!("{arn_region}:{id}");
                if self.keys.contains_key(&scoped) {
                    return Some(scoped);
                }
                if self.keys.contains_key(id) {
                    return Some(id.to_string());
                }
            }
            // alias ARN: arn:<partition>:kms:<region>:<account>:alias/<name>
            if resource.starts_with("alias/") {
                return self.alias_target(arn_region, resource).map(str::to_string);
            }
            return None;
        }

        if key_id_or_arn.starts_with("alias/") {
            return self.alias_target(region, key_id_or_arn).map(str::to_string);
        }

        None
    }

    /// The ARN of the key `key_id_or_arn` resolves to (see
    /// [`Self::resolve_key_id`]).
    pub fn resolve_key_arn(&self, region: &str, key_id_or_arn: &str) -> Option<&str> {
        let id = self.resolve_key_id(region, key_id_or_arn)?;
        self.keys.get(&id).map(|k| k.arn.as_str())
    }

    /// The key id `name` resolves to in `region`.
    pub fn alias_target(&self, region: &str, name: &str) -> Option<&str> {
        self.alias(region, name).map(|a| a.target_key_id.as_str())
    }

    /// Move a pre-regional, account-wide alias map into [`Self::aliases`].
    /// Each alias lands in the region of the key it targets (an alias and its
    /// key always share a region), falling back to the region of its own ARN
    /// and then the account's default region; its ARN is rebuilt for that
    /// region. Every AWS-managed key recorded per region then gets its
    /// region's `alias/aws/<service>` alias, and every migrated
    /// `alias/aws/<service>` alias is recorded as its region's AWS-managed
    /// key, so both lookups agree in every region.
    fn migrate_legacy_aliases(&mut self, legacy: BTreeMap<String, KmsAlias>) {
        for (name, mut alias) in legacy {
            let region = self
                .keys
                .get(&alias.target_key_id)
                .and_then(|k| parse_kms_arn(&k.arn))
                .or_else(|| parse_kms_arn(&alias.alias_arn))
                .map(|(region, _, _)| region.to_string())
                .filter(|r| !r.is_empty())
                .unwrap_or_else(|| self.region.clone());
            alias.alias_name = name;
            alias.alias_arn = kms_alias_arn(&region, &self.account_id, &alias.alias_name);
            if alias.alias_name.starts_with("alias/aws/") {
                // The region's recorded AWS-managed key wins: it is what
                // services in that region have been reporting.
                alias.target_key_id = self
                    .aws_managed_keys
                    .entry(aws_managed_key_slot(&region, &alias.alias_name))
                    .or_insert_with(|| alias.target_key_id.clone())
                    .clone();
            }
            self.insert_alias(&region, alias);
        }
        let managed: Vec<(String, String)> = self
            .aws_managed_keys
            .iter()
            .map(|(slot, key_id)| (slot.clone(), key_id.clone()))
            .collect();
        for (slot, key_id) in managed {
            let Some((region, name)) = slot.split_once('/') else {
                continue;
            };
            if self.alias(region, name).is_some() {
                continue;
            }
            let creation_date = self.keys.get(&key_id).map_or(0.0, |k| k.creation_date);
            let alias = KmsAlias {
                alias_name: name.to_string(),
                alias_arn: kms_alias_arn(region, &self.account_id, name),
                target_key_id: key_id,
                creation_date,
            };
            self.insert_alias(region, alias);
        }
    }

    pub fn reset(&mut self) {
        self.keys.clear();
        self.aliases.clear();
        self.grants.clear();
        self.custom_key_stores.clear();
        self.aws_managed_keys.clear();
        // A reset starts every service afresh, so no context-free service
        // ciphertext from before can be presented any more.
        self.legacy_context_free_ciphertexts = false;
        // Keep the master key across resets so ciphertexts still decrypt.
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsKey {
    pub key_id: String,
    pub arn: String,
    pub creation_date: f64,
    pub description: String,
    pub enabled: bool,
    pub key_usage: String,
    pub key_spec: String,
    pub key_manager: String,
    pub key_state: String,
    pub deletion_date: Option<f64>,
    pub tags: BTreeMap<String, String>,
    pub policy: String,
    pub key_rotation_enabled: bool,
    /// Customer-specified rotation cadence from EnableKeyRotation
    /// (`RotationPeriodInDays`, 90..2560). `None` means AWS's 365-day
    /// default. Echoed by GetKeyRotationStatus.
    #[serde(default)]
    pub rotation_period_in_days: Option<i32>,
    pub origin: String,
    pub multi_region: bool,
    pub rotations: Vec<KeyRotation>,
    pub signing_algorithms: Option<Vec<String>>,
    pub encryption_algorithms: Option<Vec<String>>,
    pub mac_algorithms: Option<Vec<String>>,
    pub custom_key_store_id: Option<String>,
    pub imported_key_material: bool,
    /// Raw bytes of imported key material (used as AES key for encrypt/decrypt).
    pub imported_material_bytes: Option<Vec<u8>>,
    /// Deterministic seed for the key (used for DeriveSharedSecret).
    pub private_key_seed: Vec<u8>,
    pub primary_region: Option<String>,
    /// PKCS#8 DER-encoded private key, populated for asymmetric specs
    /// (RSA_2048/3072/4096, ECC_*) at CreateKey time. None for
    /// symmetric / HMAC specs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asymmetric_private_key_der: Option<Vec<u8>>,
    /// SubjectPublicKeyInfo DER-encoded public key. Returned by
    /// GetPublicKey verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asymmetric_public_key_der: Option<Vec<u8>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsAlias {
    pub alias_name: String,
    pub alias_arn: String,
    pub target_key_id: String,
    pub creation_date: f64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsGrant {
    pub grant_id: String,
    pub grant_token: String,
    pub key_id: String,
    pub grantee_principal: String,
    pub retiring_principal: Option<String>,
    pub operations: Vec<String>,
    pub constraints: Option<serde_json::Value>,
    pub name: Option<String>,
    pub creation_date: f64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyRotation {
    pub key_id: String,
    pub rotation_date: f64,
    pub rotation_type: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CustomKeyStore {
    pub custom_key_store_id: String,
    pub custom_key_store_name: String,
    pub custom_key_store_type: String,
    pub cloud_hsm_cluster_id: Option<String>,
    pub trust_anchor_certificate: Option<String>,
    pub connection_state: String,
    pub creation_date: f64,
    pub xks_proxy_uri_endpoint: Option<String>,
    pub xks_proxy_uri_path: Option<String>,
    pub xks_proxy_vpc_endpoint_service_name: Option<String>,
    pub xks_proxy_connectivity: Option<String>,
}

/// On-disk snapshot envelope for KMS state. Versioned so format
/// changes fail loudly on upgrade.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<KmsState>>,
    #[serde(default)]
    pub state: Option<KmsState>,
}

/// 3: aliases are regional (`aliases` is region -> name -> alias). Schema 2
/// snapshots, whose `aliases` is one account-wide map, migrate on load.
pub const KMS_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_empty_collections() {
        let state = KmsState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.keys.is_empty());
        assert!(state.aliases.is_empty());
        assert!(state.grants.is_empty());
        assert!(state.custom_key_stores.is_empty());
    }

    #[test]
    fn reset_clears_collections() {
        let mut state = KmsState::new("123456789012", "us-east-1");
        state.insert_alias(
            "us-east-1",
            KmsAlias {
                alias_name: "alias/test".to_string(),
                alias_arn: "arn".to_string(),
                target_key_id: "k".to_string(),
                creation_date: 0.0,
            },
        );
        assert!(!state.aliases.is_empty());
        state.reset();
        assert!(state.aliases.is_empty());
    }

    #[test]
    fn snapshot_with_master_key_preserves_it() {
        // A snapshot that carries the master key must round-trip it byte-for-byte
        // so ciphertext produced before a restart still decrypts.
        let mut original = KmsState::new("123456789012", "us-east-1");
        original.master_key_bytes = vec![7u8; 32];
        let json = serde_json::to_string(&original).unwrap();
        let loaded: KmsState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.master_key_bytes, vec![7u8; 32]);
    }

    #[test]
    fn snapshot_missing_master_key_is_defaulted_on_load() {
        // An older snapshot with no `master_key_bytes` (predating per-account
        // master keys) must still deserialize, with a freshly generated 32-byte
        // key via the on-load default (which also logs a loud warning that the
        // prior ciphertext is now undecryptable).
        let json = r#"{
            "account_id": "123456789012",
            "region": "us-east-1",
            "keys": {},
            "aliases": {},
            "grants": [],
            "custom_key_stores": {}
        }"#;
        let loaded: KmsState = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.master_key_bytes.len(), 32);
        assert!(loaded.import_wrapping_keys.is_empty());
    }

    fn legacy_key_json(key_id: &str, region: &str, manager: &str) -> serde_json::Value {
        let mut key = serde_json::to_value(KmsKey {
            key_id: key_id.to_string(),
            arn: kms_key_arn(region, "123456789012", key_id),
            creation_date: 1.0,
            description: String::new(),
            enabled: true,
            key_usage: "ENCRYPT_DECRYPT".into(),
            key_spec: "SYMMETRIC_DEFAULT".into(),
            key_manager: manager.into(),
            key_state: "Enabled".into(),
            deletion_date: None,
            tags: BTreeMap::new(),
            policy: String::new(),
            key_rotation_enabled: false,
            rotation_period_in_days: None,
            origin: "AWS_KMS".into(),
            multi_region: false,
            rotations: Vec::new(),
            signing_algorithms: None,
            encryption_algorithms: None,
            mac_algorithms: None,
            custom_key_store_id: None,
            imported_key_material: false,
            imported_material_bytes: None,
            private_key_seed: Vec::new(),
            primary_region: None,
            asymmetric_private_key_der: None,
            asymmetric_public_key_der: None,
        })
        .unwrap();
        key.as_object_mut()
            .unwrap()
            .remove("rotation_period_in_days");
        key
    }

    fn legacy_alias_json(name: &str, arn_region: &str, target: &str) -> serde_json::Value {
        serde_json::json!({
            "alias_name": name,
            "alias_arn": kms_alias_arn(arn_region, "123456789012", name),
            "target_key_id": target,
            "creation_date": 2.0,
        })
    }

    /// A schema-2 snapshot's account-wide alias map migrates into regional
    /// aliases on load: each alias moves to its key's region (with its ARN
    /// rebuilt there), the per-region AWS-managed keys recorded by #2598 gain
    /// their region's `alias/aws/<service>` alias, and ListAliases order
    /// (AliasName within a region) is preserved.
    #[test]
    fn legacy_account_wide_aliases_migrate_into_their_key_regions() {
        let json = serde_json::json!({
            "schema_version": 2,
            "accounts": {
                "default_account_id": "123456789012",
                "region": "us-east-1",
                "endpoint": "",
                "accounts": {
                    "123456789012": {
                        "account_id": "123456789012",
                        "region": "us-east-1",
                        "keys": {
                            "east": legacy_key_json("east", "us-east-1", "CUSTOMER"),
                            "west": legacy_key_json("west", "eu-west-1", "CUSTOMER"),
                            "managed-east": legacy_key_json("managed-east", "us-east-1", "AWS"),
                            "managed-west": legacy_key_json("managed-west", "eu-west-1", "AWS"),
                        },
                        "aliases": {
                            "alias/b-east": legacy_alias_json("alias/b-east", "us-east-1", "east"),
                            "alias/a-east": legacy_alias_json("alias/a-east", "us-east-1", "east"),
                            // Created through a us-east-1 client but naming a
                            // eu-west-1 key: it belongs with its key.
                            "alias/west": legacy_alias_json("alias/west", "us-east-1", "west"),
                            // Target no longer exists: stays in the ARN's region.
                            "alias/orphan": legacy_alias_json("alias/orphan", "ap-south-1", "gone"),
                            "alias/aws/timestream": legacy_alias_json(
                                "alias/aws/timestream", "us-east-1", "managed-east"
                            ),
                        },
                        "grants": [],
                        "custom_key_stores": {},
                        "master_key_bytes": vec![1u8; 32],
                        "aws_managed_keys": {
                            "eu-west-1/alias/aws/timestream": "managed-west",
                        },
                    }
                }
            }
        });
        let snapshot: KmsSnapshot = serde_json::from_value(json).unwrap();
        let accounts = snapshot.accounts.unwrap();
        let s = accounts.get("123456789012").unwrap();

        let names = |region: &str| -> Vec<String> {
            s.aliases_in(region).map(|a| a.alias_name.clone()).collect()
        };
        assert_eq!(
            names("us-east-1"),
            vec!["alias/a-east", "alias/aws/timestream", "alias/b-east"]
        );
        assert_eq!(
            names("eu-west-1"),
            vec!["alias/aws/timestream", "alias/west"]
        );
        assert_eq!(names("ap-south-1"), vec!["alias/orphan"]);

        let west = s.alias("eu-west-1", "alias/west").unwrap();
        assert_eq!(west.target_key_id, "west");
        assert_eq!(
            west.alias_arn,
            "arn:aws:kms:eu-west-1:123456789012:alias/west"
        );
        assert_eq!(west.creation_date, 2.0);
        assert_eq!(
            s.alias_target("us-east-1", "alias/aws/timestream"),
            Some("managed-east")
        );
        let managed_west = s.alias("eu-west-1", "alias/aws/timestream").unwrap();
        assert_eq!(managed_west.target_key_id, "managed-west");
        assert_eq!(
            managed_west.alias_arn,
            "arn:aws:kms:eu-west-1:123456789012:alias/aws/timestream"
        );
        assert_eq!(
            s.aws_managed_keys
                .get(&aws_managed_key_slot("us-east-1", "alias/aws/timestream"))
                .map(String::as_str),
            Some("managed-east")
        );
        assert_eq!(
            s.resolve_key_id("eu-west-1", "alias/west").as_deref(),
            Some("west")
        );
        assert_eq!(s.resolve_key_id("us-east-1", "alias/west"), None);
        assert_eq!(s.master_key_bytes, vec![1u8; 32]);

        // The migrated state writes the regional shape, which reloads as is.
        let written = serde_json::to_value(s).unwrap();
        assert!(written["aliases"]["eu-west-1"]["alias/west"].is_object());
        let reloaded: KmsState = serde_json::from_value(written).unwrap();
        assert_eq!(
            reloaded
                .aliases_in("us-east-1")
                .map(|a| a.alias_name.as_str())
                .collect::<Vec<_>>(),
            vec!["alias/a-east", "alias/aws/timestream", "alias/b-east"]
        );
        assert_eq!(
            reloaded.alias_target("eu-west-1", "alias/west"),
            Some("west")
        );
    }

    #[test]
    fn regional_alias_accessors() {
        let mut state = KmsState::new("123456789012", "us-east-1");
        let alias = |region: &str, target: &str| KmsAlias {
            alias_name: "alias/app".into(),
            alias_arn: kms_alias_arn(region, "123456789012", "alias/app"),
            target_key_id: target.into(),
            creation_date: 0.0,
        };
        state.insert_alias("us-east-1", alias("us-east-1", "k1"));
        state.insert_alias("eu-west-1", alias("eu-west-1", "k2"));
        assert_eq!(state.alias_target("us-east-1", "alias/app"), Some("k1"));
        assert_eq!(state.alias_target("eu-west-1", "alias/app"), Some("k2"));
        state.retain_aliases(|a| a.target_key_id != "k1");
        assert!(state.alias("us-east-1", "alias/app").is_none());
        assert!(!state.aliases.contains_key("us-east-1"));
        assert!(state.remove_alias("eu-west-1", "alias/app").is_some());
        assert!(state.aliases.is_empty());
    }
}
