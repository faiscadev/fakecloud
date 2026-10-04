//! Generic multi-account state container.
//!
//! Wraps a `HashMap<AccountId, T>` so each AWS account gets its own isolated
//! state instance. Accounts are created lazily via [`MultiAccountState::get_or_create`]
//! the first time a request targets them — matching the design in #381 where
//! "an account exists because a credential resolves to it."

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

/// Trait implemented by per-service state structs that participate in
/// multi-account isolation.
pub trait AccountState: Sized {
    /// Create a fresh, empty state for the given account.
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self;

    /// Called after a new account state is created via [`MultiAccountState::get_or_create`],
    /// with a reference to an existing sibling state. Services can override
    /// this to propagate shared resources (e.g. body caches) to the new state.
    fn inherit_from(&mut self, _sibling: &Self) {}
}

/// Account-partitioned state container.
///
/// Holds one `T` per account id. The `default_account_id` is pre-created at
/// startup so unauthenticated requests (which fall back to `--account-id`)
/// always have a state to land in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiAccountState<T> {
    default_account_id: String,
    region: String,
    endpoint: String,
    accounts: HashMap<String, T>,
}

impl<T: AccountState> MultiAccountState<T> {
    /// Create a new container, pre-populating the default account.
    pub fn new(default_account_id: &str, region: &str, endpoint: &str) -> Self {
        let mut accounts = HashMap::new();
        accounts.insert(
            default_account_id.to_string(),
            T::new_for_account(default_account_id, region, endpoint),
        );
        Self {
            default_account_id: default_account_id.to_string(),
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            accounts,
        }
    }

    /// Project account states while preserving the container's routing defaults.
    pub fn map<U>(&self, mut f: impl FnMut(&T) -> U) -> MultiAccountState<U> {
        MultiAccountState {
            default_account_id: self.default_account_id.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            accounts: self
                .accounts
                .iter()
                .map(|(k, v)| (k.clone(), f(v)))
                .collect(),
        }
    }

    /// Consume the container, converting every account state while keeping
    /// the container's routing defaults. Used by snapshot migrations that
    /// change the per-account state type.
    pub fn map_into<U>(self, mut f: impl FnMut(&str, T) -> U) -> MultiAccountState<U> {
        MultiAccountState {
            default_account_id: self.default_account_id,
            region: self.region,
            endpoint: self.endpoint,
            accounts: self
                .accounts
                .into_iter()
                .map(|(k, v)| {
                    let mapped = f(&k, v);
                    (k, mapped)
                })
                .collect(),
        }
    }

    /// Get or lazily create the state for `account_id`.
    ///
    /// When a new account is created, [`AccountState::inherit_from`] is called
    /// with the default account's state so services can propagate shared
    /// resources (e.g. body caches).
    pub fn get_or_create(&mut self, account_id: &str) -> &mut T {
        if !self.accounts.contains_key(account_id) {
            let mut state = T::new_for_account(account_id, &self.region, &self.endpoint);
            // Let the new state inherit shared resources from the default account.
            if let Some(sibling) = self.accounts.get(&self.default_account_id) {
                state.inherit_from(sibling);
            }
            self.accounts.insert(account_id.to_string(), state);
        }
        self.accounts.get_mut(account_id).unwrap()
    }

    /// Get or lazily create the state for `account_id`, then run `init` on
    /// the newly created state. The callback is only invoked when the account
    /// is freshly created, not on subsequent lookups.
    pub fn get_or_create_with<F>(&mut self, account_id: &str, init: F) -> &mut T
    where
        F: FnOnce(&mut T),
    {
        if !self.accounts.contains_key(account_id) {
            let mut state = T::new_for_account(account_id, &self.region, &self.endpoint);
            init(&mut state);
            self.accounts.insert(account_id.to_string(), state);
        }
        self.accounts.get_mut(account_id).unwrap()
    }

    /// Read-only lookup. Returns `None` if the account has never been seen.
    pub fn get(&self, account_id: &str) -> Option<&T> {
        self.accounts.get(account_id)
    }

    /// Mutable lookup without auto-creation.
    pub fn get_mut(&mut self, account_id: &str) -> Option<&mut T> {
        self.accounts.get_mut(account_id)
    }

    /// Iterate over all account states (read-only).
    pub fn iter(&self) -> impl Iterator<Item = (&str, &T)> {
        self.accounts.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Iterate over all account states (mutable).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut T)> {
        self.accounts.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// The default account id configured via `--account-id`.
    pub fn default_account_id(&self) -> &str {
        &self.default_account_id
    }

    /// Mutable reference to the default account's state (always exists).
    pub fn default_mut(&mut self) -> &mut T {
        self.accounts.get_mut(&self.default_account_id).unwrap()
    }

    /// Reference to the default account's state (always exists).
    pub fn default_ref(&self) -> &T {
        self.accounts.get(&self.default_account_id).unwrap()
    }

    /// Reset all accounts back to empty state. The default account is
    /// recreated; all other accounts are dropped.
    pub fn reset(&mut self) {
        self.accounts.clear();
        self.accounts.insert(
            self.default_account_id.clone(),
            T::new_for_account(&self.default_account_id, &self.region, &self.endpoint),
        );
    }

    /// Find the first account whose state satisfies `predicate` and return
    /// the account id. Useful for resolving globally-unique resources (e.g.
    /// S3 bucket names) back to their owning account.
    pub fn find_account<F>(&self, predicate: F) -> Option<&str>
    where
        F: Fn(&T) -> bool,
    {
        self.accounts
            .iter()
            .find(|(_, v)| predicate(v))
            .map(|(k, _)| k.as_str())
    }

    /// Number of accounts with state.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Region shared by all accounts.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Endpoint shared by all accounts.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// One account's state for a regional service, partitioned by region.
///
/// Regional AWS services (SQS, DynamoDB, Lambda, ...) keep an independent set
/// of resources in every region: the same queue, table or function name can
/// exist in two regions at once, and a request only ever sees the resources of
/// the region it is sent to. Wrapping a service's per-account state `T` in
/// `RegionalState<T>` (and the container in [`MultiRegionState`]) gives every
/// (account, region) pair its own `T`, created the first time a request
/// targets it.
///
/// Global services (IAM, Route 53, CloudFront, Organizations, the S3 bucket
/// namespace) stay account-scoped and do not use this wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionalState<T> {
    account_id: String,
    /// The server's configured region.
    default_region: String,
    endpoint: String,
    /// Per-region state, keyed by region name.
    #[serde(default = "BTreeMap::new")]
    regions: BTreeMap<String, T>,
}

impl<T> RegionalState<T> {
    /// An account with no regional state yet.
    pub fn new(account_id: &str, default_region: &str, endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            default_region: default_region.to_string(),
            endpoint: endpoint.to_string(),
            regions: BTreeMap::new(),
        }
    }

    /// The account this state belongs to.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// The server's configured region.
    pub fn default_region(&self) -> &str {
        &self.default_region
    }

    /// The server endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The account's state in `region`, `None` when nothing has touched it.
    pub fn region(&self, region: &str) -> Option<&T> {
        self.regions.get(region)
    }

    /// The account's state in `region` without creating it.
    pub fn get_region_mut(&mut self, region: &str) -> Option<&mut T> {
        self.regions.get_mut(region)
    }

    /// Every region the account has state in.
    pub fn regions(&self) -> impl Iterator<Item = (&str, &T)> {
        self.regions.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Every region the account has state in (mutable).
    pub fn regions_mut(&mut self) -> impl Iterator<Item = (&str, &mut T)> {
        self.regions.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// Replace (or add) the state of one region.
    pub fn insert_region(&mut self, region: &str, state: T) -> Option<T> {
        self.regions.insert(region.to_string(), state)
    }

    /// Drop every region's state.
    pub fn clear(&mut self) {
        self.regions.clear();
    }
}

impl<T: AccountState> RegionalState<T> {
    /// The account's state in `region`, created empty on first use. A new
    /// region inherits shared resources (see [`AccountState::inherit_from`])
    /// from a region the account already has.
    pub fn region_mut(&mut self, region: &str) -> &mut T {
        if !self.regions.contains_key(region) {
            let mut state = T::new_for_account(&self.account_id, region, &self.endpoint);
            if let Some(sibling) = self.regions.values().next() {
                state.inherit_from(sibling);
            }
            self.regions.insert(region.to_string(), state);
        }
        self.regions.get_mut(region).expect("inserted above")
    }

    /// The state for `region` given as an optional ARN region: `None` (an ARN
    /// without a region, or a record without an ARN) lands in the server's
    /// default region. Used by snapshot migrations.
    pub fn region_or_default_mut(&mut self, region: Option<&str>) -> &mut T {
        let region = region
            .filter(|r| !r.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| self.default_region.clone());
        self.region_mut(&region)
    }
}

impl<T: AccountState> AccountState for RegionalState<T> {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self::new(account_id, region, endpoint)
    }

    fn inherit_from(&mut self, sibling: &Self) {
        if let Some(shared) = sibling.regions.values().next() {
            for state in self.regions.values_mut() {
                state.inherit_from(shared);
            }
        }
    }
}

/// Account- and region-partitioned state container for a regional service.
pub type MultiRegionState<T> = MultiAccountState<RegionalState<T>>;

impl<T: AccountState> MultiAccountState<RegionalState<T>> {
    /// The state of `account_id` in `region`, `None` when either has never
    /// been touched. Never creates anything, so reads and misses leave no
    /// empty region behind.
    pub fn regional(&self, account_id: &str, region: &str) -> Option<&T> {
        self.get(account_id).and_then(|a| a.region(region))
    }

    /// The state of `account_id` in `region`, creating both on first use.
    pub fn regional_mut(&mut self, account_id: &str, region: &str) -> &mut T {
        self.get_or_create(account_id).region_mut(region)
    }

    /// The state of `account_id` in `region` without creating either.
    pub fn regional_get_mut(&mut self, account_id: &str, region: &str) -> Option<&mut T> {
        self.get_mut(account_id)
            .and_then(|a| a.get_region_mut(region))
    }

    /// The state of the account and region an ARN names, without creating
    /// either. `None` when the ARN carries no account or region.
    pub fn by_arn(&self, arn: &str) -> Option<&T> {
        let account = fakecloud_aws::arn::account_of(arn)?;
        let region = fakecloud_aws::arn::region_of(arn)?;
        self.regional(account, region)
    }

    /// Mutable [`Self::by_arn`].
    pub fn by_arn_mut(&mut self, arn: &str) -> Option<&mut T> {
        let account = fakecloud_aws::arn::account_of(arn)?.to_string();
        let region = fakecloud_aws::arn::region_of(arn)?.to_string();
        self.regional_get_mut(&account, &region)
    }

    /// The default account's state in the server's default region, created
    /// on first use.
    pub fn default_regional_mut(&mut self) -> &mut T {
        let region = self.region().to_string();
        self.default_mut().region_mut(&region)
    }

    /// The default account's state in the server's default region, `None`
    /// until something creates it.
    pub fn default_regional(&self) -> Option<&T> {
        self.default_ref().region(self.region())
    }

    /// Every (account, region, state) triple.
    pub fn iter_regional(&self) -> impl Iterator<Item = (&str, &str, &T)> {
        self.iter()
            .flat_map(|(account, a)| a.regions().map(move |(region, s)| (account, region, s)))
    }

    /// Every (account, region, state) triple (mutable).
    pub fn iter_regional_mut(&mut self) -> impl Iterator<Item = (&str, &str, &mut T)> {
        self.iter_mut()
            .flat_map(|(account, a)| a.regions_mut().map(move |(region, s)| (account, region, s)))
    }
}

/// Migration of a service's pre-regional, account-wide state into per-region
/// states, for loading snapshots written before the service was
/// region-partitioned.
pub trait SplitByRegion: AccountState {
    /// Move every resource of this account-wide state into the region it
    /// belongs to in `into`: the region its ARN (or region-bearing URL) names,
    /// and the server's default region for anything that names none (see
    /// [`RegionalState::region_or_default_mut`]).
    fn split_by_region(self, into: &mut RegionalState<Self>);
}

impl<T: SplitByRegion> RegionalState<T> {
    /// Split one account's legacy, account-wide state into regions.
    pub fn from_legacy(account_id: &str, default_region: &str, endpoint: &str, legacy: T) -> Self {
        let mut regional = Self::new(account_id, default_region, endpoint);
        legacy.split_by_region(&mut regional);
        regional
    }
}

impl<T: SplitByRegion> MultiAccountState<T> {
    /// Convert a legacy account-partitioned container into an (account,
    /// region)-partitioned one, splitting every account's state by region.
    pub fn into_regional(self) -> MultiRegionState<T> {
        let region = self.region.clone();
        let endpoint = self.endpoint.clone();
        self.map_into(|account, state| {
            RegionalState::from_legacy(account, &region, &endpoint, state)
        })
    }
}

/// Versioned on-disk snapshot of a regional service's state.
///
/// `accounts` holds every account's per-region state. `state` is only set
/// when a legacy single-account snapshot is migrated: that one account's
/// state split by region, for the loader to merge into its own container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionalSnapshot<T> {
    pub schema_version: u32,
    #[serde(default = "none")]
    pub accounts: Option<MultiRegionState<T>>,
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub state: Option<RegionalState<T>>,
}

fn none<T>() -> Option<T> {
    None
}

impl<T> RegionalSnapshot<T> {
    /// A snapshot of the whole container at `schema_version`.
    pub fn of(schema_version: u32, accounts: MultiRegionState<T>) -> Self {
        Self {
            schema_version,
            accounts: Some(accounts),
            state: None,
        }
    }
}

#[derive(Deserialize)]
struct SnapshotVersionProbe {
    schema_version: u32,
}

#[derive(Deserialize)]
#[serde(bound = "T: serde::de::DeserializeOwned")]
struct LegacySnapshot<T> {
    #[serde(default = "none")]
    accounts: Option<MultiAccountState<T>>,
    #[serde(default = "none")]
    state: Option<T>,
}

/// Parse a regional service's snapshot. `current` is the first schema
/// version that stores per-region state: an older snapshot (one state per
/// account, or one single-account `state`) is migrated with
/// [`SplitByRegion`]; `legacy_single` splits a single-account state (it knows
/// the state's own account, region and endpoint fields). A snapshot newer
/// than `current` comes back with its on-disk `schema_version` and no state,
/// for the caller to refuse.
pub fn parse_regional_snapshot<T>(
    bytes: &[u8],
    current: u32,
    legacy_single: impl FnOnce(T) -> RegionalState<T>,
) -> Result<RegionalSnapshot<T>, serde_json::Error>
where
    T: SplitByRegion + serde::de::DeserializeOwned,
{
    let SnapshotVersionProbe { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > current {
        return Ok(RegionalSnapshot {
            schema_version,
            accounts: None,
            state: None,
        });
    }
    if schema_version == current {
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacySnapshot<T> = serde_json::from_slice(bytes)?;
    Ok(RegionalSnapshot {
        schema_version: current,
        accounts: legacy.accounts.map(MultiAccountState::into_regional),
        state: legacy.state.map(legacy_single),
    })
}

/// The region an ARN names, or `default` when it names none. Convenience for
/// [`SplitByRegion`] implementations.
pub fn arn_region_or<'a>(arn: &'a str, default: &'a str) -> &'a str {
    fakecloud_aws::arn::region_of(arn).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestState {
        account_id: String,
        items: Vec<String>,
    }

    impl AccountState for TestState {
        fn new_for_account(account_id: &str, _region: &str, _endpoint: &str) -> Self {
            Self {
                account_id: account_id.to_string(),
                items: Vec::new(),
            }
        }
    }

    #[test]
    fn default_account_exists_on_creation() {
        let mas: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        assert_eq!(mas.account_count(), 1);
        assert!(mas.get("111111111111").is_some());
    }

    #[test]
    fn get_or_create_makes_new_account() {
        let mut mas: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        let state = mas.get_or_create("222222222222");
        assert_eq!(state.account_id, "222222222222");
        assert_eq!(mas.account_count(), 2);
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let mas: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        assert!(mas.get("999999999999").is_none());
    }

    #[test]
    fn reset_clears_all_but_default() {
        let mut mas: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        mas.get_or_create("222222222222");
        mas.get_or_create("333333333333");
        assert_eq!(mas.account_count(), 3);
        mas.reset();
        assert_eq!(mas.account_count(), 1);
        assert!(mas.get("111111111111").is_some());
        assert!(mas.get("222222222222").is_none());
    }

    #[test]
    fn iter_visits_all_accounts() {
        let mut mas: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        mas.get_or_create("222222222222");
        let ids: Vec<&str> = mas.iter().map(|(id, _)| id).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"111111111111"));
        assert!(ids.contains(&"222222222222"));
    }

    impl SplitByRegion for TestState {
        fn split_by_region(self, into: &mut RegionalState<Self>) {
            for item in self.items {
                // items are ARNs or plain names
                let region = fakecloud_aws::arn::region_of(&item).map(str::to_string);
                into.region_or_default_mut(region.as_deref())
                    .items
                    .push(item);
            }
        }
    }

    #[test]
    fn regional_state_isolates_regions() {
        let mut mrs: MultiRegionState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        mrs.regional_mut("111111111111", "us-east-1")
            .items
            .push("q".into());
        mrs.regional_mut("111111111111", "eu-west-1")
            .items
            .push("q".into());
        assert_eq!(
            mrs.regional("111111111111", "us-east-1").unwrap().items,
            ["q"]
        );
        assert_eq!(
            mrs.regional("111111111111", "eu-west-1").unwrap().items,
            ["q"]
        );
        assert!(mrs.regional("111111111111", "ap-south-1").is_none());
        assert!(mrs.regional("222222222222", "us-east-1").is_none());
        // Reads never create a region.
        assert_eq!(mrs.iter_regional().count(), 2);
    }

    #[test]
    fn by_arn_resolves_account_and_region() {
        let mut mrs: MultiRegionState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        mrs.regional_mut("222222222222", "eu-west-1")
            .items
            .push("x".into());
        let s = mrs
            .by_arn("arn:aws:sqs:eu-west-1:222222222222:x")
            .expect("state");
        assert_eq!(s.account_id, "222222222222");
        assert!(mrs.by_arn("arn:aws:sqs:us-east-1:222222222222:x").is_none());
        assert!(mrs.by_arn("arn:aws:iam::222222222222:role/x").is_none());
    }

    #[test]
    fn regional_state_round_trips_through_json() {
        let mut mrs: MultiRegionState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        mrs.regional_mut("111111111111", "eu-west-1")
            .items
            .push("q".into());
        let json = serde_json::to_string(&mrs).unwrap();
        let back: MultiRegionState<TestState> = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.regional("111111111111", "eu-west-1").unwrap().items,
            ["q"]
        );
    }

    #[test]
    fn legacy_state_splits_by_arn_region() {
        let mut legacy: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        let acct = legacy.default_mut();
        acct.items
            .push("arn:aws:sqs:eu-west-1:111111111111:a".into());
        acct.items
            .push("arn:aws:sqs:us-east-1:111111111111:b".into());
        acct.items.push("plain".into());
        let regional = legacy.into_regional();
        assert_eq!(
            regional
                .regional("111111111111", "eu-west-1")
                .unwrap()
                .items,
            ["arn:aws:sqs:eu-west-1:111111111111:a"]
        );
        assert_eq!(
            regional
                .regional("111111111111", "us-east-1")
                .unwrap()
                .items,
            ["arn:aws:sqs:us-east-1:111111111111:b", "plain"]
        );
        assert_eq!(regional.region(), "us-east-1");
        assert_eq!(regional.default_account_id(), "111111111111");
    }

    #[test]
    fn default_regional_mut_uses_server_region() {
        let mut mrs: MultiRegionState<TestState> =
            MultiAccountState::new("111111111111", "eu-central-1", "http://localhost:4566");
        mrs.default_regional_mut().items.push("z".into());
        assert!(mrs.regional("111111111111", "eu-central-1").is_some());
    }

    #[test]
    fn parse_regional_snapshot_migrates_legacy_and_reports_newer() {
        let mut legacy: MultiAccountState<TestState> =
            MultiAccountState::new("111111111111", "us-east-1", "http://localhost:4566");
        legacy
            .default_mut()
            .items
            .push("arn:aws:sqs:eu-west-1:111111111111:a".into());
        let bytes =
            serde_json::to_vec(&serde_json::json!({"schema_version": 1, "accounts": legacy}))
                .unwrap();
        let snap = parse_regional_snapshot::<TestState>(&bytes, 2, |_| unreachable!()).unwrap();
        assert_eq!(snap.schema_version, 2);
        let accounts = snap.accounts.unwrap();
        assert_eq!(
            accounts
                .regional("111111111111", "eu-west-1")
                .unwrap()
                .items
                .len(),
            1
        );

        let current = serde_json::to_vec(&RegionalSnapshot::of(2, accounts)).unwrap();
        let again = parse_regional_snapshot::<TestState>(&current, 2, |_| unreachable!()).unwrap();
        assert!(again
            .accounts
            .unwrap()
            .regional("111111111111", "eu-west-1")
            .is_some());

        let single = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "state": {"account_id": "111111111111", "items": ["x"]}
        }))
        .unwrap();
        let snap = parse_regional_snapshot::<TestState>(&single, 2, |s| {
            RegionalState::from_legacy("111111111111", "us-east-1", "", s)
        })
        .unwrap();
        assert_eq!(
            snap.state.unwrap().region("us-east-1").unwrap().items,
            ["x"]
        );

        let newer = parse_regional_snapshot::<TestState>(
            br#"{"schema_version": 9}"#,
            2,
            |_| unreachable!(),
        )
        .unwrap();
        assert_eq!(newer.schema_version, 9);
        assert!(newer.accounts.is_none());
    }
}
