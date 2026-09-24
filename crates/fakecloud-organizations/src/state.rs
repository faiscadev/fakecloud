use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Shared, cross-account registry of every organization in the process.
/// An AWS org is not per-account state (it spans accounts), so this is
/// NOT wrapped in `MultiAccountState` — but it is not a singleton
/// either: any account that belongs to no organization can create its
/// own, and the organizations are fully independent of each other.
pub type SharedOrganizationsState = Arc<RwLock<OrganizationsRegistry>>;

/// Every organization in the process, keyed by organization id
/// (`o-...`). An account belongs to at most one organization, which is
/// what makes [`OrganizationsRegistry::org_of_account`] well defined and
/// lets most handlers resolve "the caller's organization" in one step.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OrganizationsRegistry {
    orgs: BTreeMap<String, OrganizationState>,
}

/// A GovCloud mirror lives in the `aws-us-gov` partition.
fn is_gov_cloud(account: &MemberAccount) -> bool {
    account.arn.starts_with("arn:aws-us-gov:")
}

/// Resolve a handshake or transfer target to an account id.
///
/// An `ACCOUNT` target already is one. An `EMAIL` target stores the
/// address itself, which is only resolvable when it follows the
/// `<account-id>@example.com` form fakecloud mints for accounts it
/// creates (see `enroll_account_if_missing` and `complete_create_account`).
/// A genuinely external address names an account fakecloud has never
/// seen, so it stays unresolvable — the invitation can be read and
/// cancelled by its source, but no caller can prove it is the target.
///
/// This is the id-only form. Prefer
/// [`OrganizationsRegistry::resolve_target_account`], which also matches
/// the address a member account was actually registered with — an
/// account created with `CreateAccount(Email = "team@corp.com")` keeps
/// that address, not a synthetic one.
pub fn target_account_id(target_kind: &str, target: &str) -> Option<String> {
    if target_kind != "EMAIL" {
        return Some(target.to_string());
    }
    let local = target.strip_suffix("@example.com")?;
    if local.len() == 12 && local.chars().all(|c| c.is_ascii_digit()) {
        Some(local.to_string())
    } else {
        None
    }
}

impl From<OrganizationState> for OrganizationsRegistry {
    fn from(org: OrganizationState) -> Self {
        let mut registry = Self::default();
        registry.insert(org);
        registry
    }
}

impl OrganizationsRegistry {
    pub fn is_empty(&self) -> bool {
        self.orgs.is_empty()
    }

    /// Does any organization hold a handshake that is past due?
    ///
    /// Cheap enough to run under the read lock on every request, so the
    /// write lock the sweep needs is only ever taken when there is
    /// something to expire.
    pub fn has_stale_handshakes(&self, now: DateTime<Utc>) -> bool {
        self.orgs.values().any(|org| {
            org.handshakes.values().any(|h| {
                matches!(h.state.as_str(), "OPEN" | "REQUESTED") && h.expiration_timestamp <= now
            })
        })
    }

    /// Expire every past-due handshake in every organization, and
    /// return how many moved.
    pub fn expire_stale_handshakes(&mut self, now: DateTime<Utc>) -> usize {
        self.orgs
            .values_mut()
            .map(|org| org.expire_stale_handshakes(now))
            .sum()
    }

    pub fn len(&self) -> usize {
        self.orgs.len()
    }

    /// Every organization, ordered by id so callers that render or hash
    /// the whole registry are deterministic.
    pub fn iter(&self) -> impl Iterator<Item = &OrganizationState> {
        self.orgs.values()
    }

    pub fn contains_org(&self, org_id: &str) -> bool {
        self.orgs.contains_key(org_id)
    }

    pub fn org_by_id(&self, org_id: &str) -> Option<&OrganizationState> {
        self.orgs.get(org_id)
    }

    pub fn org_by_id_mut(&mut self, org_id: &str) -> Option<&mut OrganizationState> {
        self.orgs.get_mut(org_id)
    }

    /// The organization `account_id` belongs to, management account
    /// included. `None` for a standalone account — which is every
    /// account until it creates an organization, is invited into one and
    /// accepts, or is created through `CreateAccount`.
    pub fn org_of_account(&self, account_id: &str) -> Option<&OrganizationState> {
        self.orgs
            .values()
            .find(|org| org.accounts.contains_key(account_id))
    }

    pub fn org_of_account_mut(&mut self, account_id: &str) -> Option<&mut OrganizationState> {
        self.orgs
            .values_mut()
            .find(|org| org.accounts.contains_key(account_id))
    }

    /// The organization that owns handshake `handshake_id`. Handshakes
    /// are looked up by id rather than through the caller's own
    /// organization because the account answering an invitation is by
    /// definition not yet a member of the inviting organization.
    pub fn org_of_handshake(&self, handshake_id: &str) -> Option<&OrganizationState> {
        self.orgs
            .values()
            .find(|org| org.handshakes.contains_key(handshake_id))
    }

    pub fn org_of_handshake_mut(&mut self, handshake_id: &str) -> Option<&mut OrganizationState> {
        self.orgs
            .values_mut()
            .find(|org| org.handshakes.contains_key(handshake_id))
    }

    /// The organization holding `CreateAccount` request `request_id`.
    /// Request ids are globally unique, so the background completion
    /// tick finds its own request without carrying the org id.
    pub fn org_of_create_account_request(&self, request_id: &str) -> Option<&OrganizationState> {
        self.orgs
            .values()
            .find(|org| org.create_account_requests.contains_key(request_id))
    }

    pub fn org_of_create_account_request_mut(
        &mut self,
        request_id: &str,
    ) -> Option<&mut OrganizationState> {
        self.orgs
            .values_mut()
            .find(|org| org.create_account_requests.contains_key(request_id))
    }

    /// The only organization, when there is exactly one. `None` both
    /// when there are none and when there are several — a caller that
    /// means "the" organization has to say which once more than one
    /// exists, rather than silently getting an arbitrary pick.
    pub fn sole(&self) -> Option<&OrganizationState> {
        match self.orgs.len() {
            1 => self.orgs.values().next(),
            _ => None,
        }
    }

    pub fn sole_mut(&mut self) -> Option<&mut OrganizationState> {
        match self.orgs.len() {
            1 => self.orgs.values_mut().next(),
            _ => None,
        }
    }

    /// Resolve a handshake or transfer target to an account id, matching
    /// an `EMAIL` target against the address the members of `within` are
    /// actually registered with before falling back to the synthetic
    /// form. Without the address lookup, a member created with a real
    /// email is invisible to the "one organization per account" guards,
    /// which could then open an invitation for an account already
    /// enrolled.
    ///
    /// The lookup is deliberately scoped to ONE organization. Scanning
    /// every organization would turn an invitation into an
    /// email-to-account-id oracle: a caller could name an address, be
    /// told "already a member of an organization", and read back a
    /// 12-digit id belonging to an organization it has no relationship
    /// with.
    pub fn resolve_target_account(
        &self,
        target_kind: &str,
        target: &str,
        within: &str,
    ) -> Option<String> {
        // The registered address wins over the synthetic form, as the doc
        // above says: an account created with
        // `CreateAccount(Email = "222222222222@example.com")` gets a random
        // id, so decoding the address as if it spelled one would resolve
        // to an account that does not exist and let an invitation open for
        // a member already enrolled.
        // Scoped to ONE organization on purpose: resolving an address
        // against every organization would let the caller read a foreign
        // account id back out of the "already a member" error. The
        // boolean matcher may look wider, because it only ever confirms
        // an account the caller already named.
        if target_kind == "EMAIL" {
            if let Some(account) = self.orgs.get(within).and_then(|org| {
                org.accounts
                    .values()
                    .find(|a| a.email == target && !is_gov_cloud(a) && a.status != "SUSPENDED")
            }) {
                return Some(account.id.clone());
            }
        }
        target_account_id(target_kind, target)
    }

    /// Mint an account id unused by ANY organization in the process, and
    /// not already reserved by an in-flight `CreateAccount`. `besides`
    /// excludes ids minted moments ago that are not recorded yet --
    /// `CreateGovCloudAccount` mints two in a row.
    pub fn next_account_id_besides(&self, besides: &[&str]) -> String {
        OrganizationState::mint_account_id(|id| {
            besides.contains(&id)
                || self.orgs.values().any(|org| {
                    org.accounts.contains_key(id)
                        || org.create_account_requests.values().any(|req| {
                            req.account_id.as_deref() == Some(id)
                                || req.gov_cloud_account_id.as_deref() == Some(id)
                        })
                })
        })
    }

    /// Mint an account id unused by ANY organization in the process.
    pub fn next_account_id(&self) -> String {
        self.next_account_id_besides(&[])
    }

    /// The account registered with `email`, if any organization has one.
    /// Used to decide whether an address names a real account or should
    /// fall back to the synthetic `<account-id>@example.com` decode.
    pub fn account_registered_with(&self, email: &str) -> Option<String> {
        self.orgs
            .values()
            .flat_map(|org| org.accounts.values())
            // A GovCloud mirror shares its commercial twin's address by
            // design; the commercial account is the one an address names.
            .find(|account| {
                account.email == email && !is_gov_cloud(account) && account.status != "SUSPENDED"
            })
            .map(|account| account.id.clone())
    }

    /// Like [`Self::email_in_use`], for the in-flight request
    /// `request_id`: its own reservation must not count against it, and
    /// only requests made BEFORE it do. Counting every other in-flight
    /// request made whichever tick fired first fail itself, so the
    /// caller that asked first was the one refused.
    pub fn email_in_use_besides(&self, email: &str, request_id: &str) -> bool {
        let mine = self
            .orgs
            .values()
            .find_map(|org| org.create_account_requests.get(request_id));
        self.orgs
            .values()
            .flat_map(|org| org.accounts.values())
            .any(|account| account.email == email && account.status != "SUSPENDED")
            || self.orgs.values().any(|org| {
                org.create_account_requests.iter().any(|(id, req)| {
                    id != request_id
                        && Self::reservation_holds(req, email)
                        && mine.is_some_and(|m| {
                            (req.requested_timestamp, id.as_str())
                                < (m.requested_timestamp, request_id)
                        })
                })
            })
    }

    /// True when `email` is the synthetic `<account-id>@example.com`
    /// form of an account OTHER than `for_account`.
    ///
    /// fakecloud mints those addresses for the accounts it creates, so
    /// they are reserved for the id they spell whether or not that
    /// account exists yet. Letting an unrelated account register one
    /// meant two live accounts shared an address -- through
    /// `CreateAccount`, or through `CreateOrganization`, whose
    /// management account takes its own synthetic address -- and
    /// `account_registered_with` then answered with whichever
    /// organization sorted first, which decides who may accept an
    /// EMAIL-targeted handshake.
    pub fn email_reserved_for_other(email: &str, for_account: &str) -> bool {
        target_account_id("EMAIL", email).is_some_and(|spelled| spelled != for_account)
    }

    /// Does this in-flight request hold `email`?
    ///
    /// A request whose address is the synthetic form of an id OTHER than
    /// the one it reserved is already doomed -- the completion tick
    /// fails it with `EMAIL_ALREADY_EXISTS` -- so it must not hold the
    /// address meanwhile. Otherwise anyone could park another account's
    /// address for the length of the creation delay, blocking that
    /// account's own `CreateOrganization`.
    fn reservation_holds(req: &CreateAccountStatus, email: &str) -> bool {
        if req.state != "IN_PROGRESS" || req.pending_email.as_deref() != Some(email) {
            return false;
        }
        !req.account_id
            .as_deref()
            .is_some_and(|mine| Self::email_reserved_for_other(email, mine))
    }

    /// True when any account already uses `email`. AWS requires an
    /// address to be unused (`EMAIL_ALREADY_EXISTS`), and this
    /// resolution is authorization-relevant -- it decides who may accept
    /// an `EMAIL`-targeted invitation -- so a duplicate would make that
    /// answer depend on id ordering.
    pub fn email_in_use(&self, email: &str) -> bool {
        self.orgs
            .values()
            .flat_map(|org| org.accounts.values())
            // A closed account keeps its record but releases its address:
            // `CloseAccount` (and the CloudFormation delete that calls it)
            // only suspends, so counting those would make a deleted stack
            // impossible to re-deploy.
            .any(|account| account.email == email && account.status != "SUSPENDED")
            || self.orgs.values().any(|org| {
                org.create_account_requests
                    .values()
                    .any(|req| Self::reservation_holds(req, email))
            })
    }

    /// Does `target` (as declared by `target_kind`) name `account_id`?
    ///
    /// This is the single answer used by every party gate -- handshakes,
    /// responsibility transfers, and the account's own handshake
    /// listing. Two matchers that disagreed let an account accept an
    /// invitation it could then neither read nor act on.
    ///
    /// An `ACCOUNT` target names the id directly. An `EMAIL` target
    /// resolves to the account REGISTERED with that address if any
    /// organization has one, and otherwise decodes the synthetic
    /// `<account-id>@example.com` form fakecloud mints. That precedence
    /// is what keeps one address naming one account: accepting both
    /// readings let an invitation be accepted by an account it was never
    /// addressed to. The consequence is that registering an address
    /// elsewhere takes over its synthetic reading, so an invitation open
    /// to the spelled-out id stops matching -- rare, and the safe
    /// direction to fail.
    ///
    /// The registered lookup spans organizations, which is safe here
    /// because this only ever CONFIRMS an account the caller already
    /// named; it never hands one back. Resolvers that return an id --
    /// `resolve_target_account` -- stay scoped to one organization so
    /// they cannot be read as an oracle.
    pub fn account_matches_target(
        &self,
        target_kind: &str,
        target: &str,
        account_id: &str,
    ) -> bool {
        if target_kind == "EMAIL" {
            // A registered address names its own account and nothing else.
            // Allowing the synthetic decode as well let one address name
            // two accounts, so an account other than the intended target
            // could read and accept the invitation.
            if let Some(registered) = self.account_registered_with(target) {
                return registered == account_id;
            }
        }
        target_account_id(target_kind, target).as_deref() == Some(account_id)
    }

    /// The organization that stores responsibility transfer `id`. A
    /// transfer is recorded once, in the source organization, but both
    /// management accounts are parties to it.
    pub fn org_of_responsibility_transfer(&self, id: &str) -> Option<&OrganizationState> {
        self.orgs
            .values()
            .find(|org| org.responsibility_transfers.contains_key(id))
    }

    /// Insert an organization, keyed by its own id.
    pub fn insert(&mut self, org: OrganizationState) {
        self.orgs.insert(org.org_id.clone(), org);
    }

    pub fn remove(&mut self, org_id: &str) -> Option<OrganizationState> {
        self.orgs.remove(org_id)
    }

    pub fn clear(&mut self) {
        self.orgs.clear();
    }

    /// True when `account_id` is already claimed by some organization --
    /// enrolled in one, or RESERVED by an in-flight `CreateAccount` that
    /// has not finished enrolling it yet.
    ///
    /// Guards both `CreateOrganization` and the invite/accept path: an
    /// account can never be in two organizations at once. Ignoring the
    /// reservation let an account created by one organization create its
    /// own during the completion delay, after which the background tick
    /// enrolled it and it was in two.
    pub fn account_is_enrolled(&self, account_id: &str) -> bool {
        self.org_of_account(account_id).is_some() || self.account_is_reserved(account_id)
    }

    /// The organization that already claims `account_id`, if it is one
    /// other than `org_id`. Reservation-aware, so an id an in-flight
    /// `CreateAccount` is about to enroll counts as claimed.
    pub fn claimed_by_other_org(&self, account_id: &str, org_id: &str) -> Option<String> {
        if let Some(org) = self.org_of_account(account_id) {
            return (org.org_id != org_id).then(|| org.org_id.clone());
        }
        self.orgs
            .values()
            .find(|org| {
                org.org_id != org_id
                    && org.create_account_requests.values().any(|req| {
                        req.state == "IN_PROGRESS"
                            && (req.account_id.as_deref() == Some(account_id)
                                || req.gov_cloud_account_id.as_deref() == Some(account_id))
                    })
            })
            .map(|org| org.org_id.clone())
    }

    fn account_is_reserved(&self, account_id: &str) -> bool {
        self.orgs.values().any(|org| {
            org.create_account_requests.values().any(|req| {
                req.state == "IN_PROGRESS"
                    && (req.account_id.as_deref() == Some(account_id)
                        || req.gov_cloud_account_id.as_deref() == Some(account_id))
            })
        })
    }
}

pub const FEATURE_SET_ALL: &str = "ALL";
pub const FEATURE_SET_CONSOLIDATED_BILLING: &str = "CONSOLIDATED_BILLING";

pub const POLICY_TYPE_SCP: &str = "SERVICE_CONTROL_POLICY";

/// Stable ID of the AWS-managed FullAWSAccess SCP. Matches AWS's
/// documented identifier so SDK callers can reference it by name.
pub const FULL_AWS_ACCESS_POLICY_ID: &str = "p-FullAWSAccess";
pub const FULL_AWS_ACCESS_POLICY_NAME: &str = "FullAWSAccess";
pub const FULL_AWS_ACCESS_POLICY_DESCRIPTION: &str = "Allows access to every operation";
pub const FULL_AWS_ACCESS_POLICY_CONTENT: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

/// On-disk snapshot envelope for Organizations state. Versioned so format
/// changes fail loudly on upgrade rather than silently mis-parsing.
///
/// v1 held a single optional organization; v2 holds the whole registry.
/// The v1 field is still read so an existing snapshot keeps loading — it
/// folds into the registry as one organization — but is never written
/// again.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationsSnapshot {
    pub schema_version: u32,
    /// v1 only. Retained for reading old snapshots; `None` in v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization: Option<OrganizationState>,
    #[serde(default)]
    pub organizations: OrganizationsRegistry,
}

impl OrganizationsSnapshot {
    /// The registry this snapshot describes, folding a v1 single
    /// organization into the v2 shape.
    pub fn into_registry(self) -> OrganizationsRegistry {
        let mut registry = self.organizations;
        if let Some(org) = self.organization {
            registry.insert(org);
        }
        registry
    }
}

pub const ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrganizationState {
    pub org_id: String,
    pub org_arn: String,
    pub management_account_id: String,
    pub management_account_arn: String,
    pub management_account_email: String,
    pub feature_set: String,
    pub root_id: String,
    pub root_arn: String,
    pub root_name: String,
    pub created_at: DateTime<Utc>,
    pub ous: BTreeMap<String, OrganizationalUnit>,
    pub accounts: BTreeMap<String, MemberAccount>,
    pub policies: BTreeMap<String, Policy>,
    /// target_id -> attached policy ids. Targets are root id, OU id, or account id.
    pub attachments: BTreeMap<String, HashSet<String>>,
    /// `CreateAccount` / `CreateGovCloudAccount` request statuses keyed
    /// by request id (`car-...`). Lifecycles transition through
    /// `IN_PROGRESS` -> `SUCCEEDED` (or `FAILED`) and remain queryable
    /// via `DescribeCreateAccountStatus` and `ListCreateAccountStatus`.
    #[serde(default)]
    pub create_account_requests: BTreeMap<String, CreateAccountStatus>,
    /// `InviteAccountToOrganization` handshakes keyed by id (`h-...`).
    /// Lifecycles transition `REQUESTED` -> `OPEN` (peer side) ->
    /// `ACCEPTED` / `DECLINED` / `CANCELED` / `EXPIRED`.
    #[serde(default)]
    pub handshakes: BTreeMap<String, Handshake>,
    /// AWS service principals enabled via `EnableAWSServiceAccess`,
    /// keyed by the principal hostname (eg. `config.amazonaws.com`)
    /// with the value being the moment the principal was first enabled.
    /// `ListAWSServiceAccessForOrganization` surfaces the timestamp as
    /// `DateEnabled`. Re-enabling an already-trusted service is a no-op
    /// — the original timestamp is preserved (matches AWS behavior).
    #[serde(default)]
    pub trusted_services: BTreeMap<String, DateTime<Utc>>,
    /// Service principal -> set of member account ids registered as
    /// delegated administrators for that service.
    #[serde(default)]
    pub delegated_administrators: BTreeMap<String, BTreeMap<String, DelegatedAdministrator>>,
    /// Policy types currently `ENABLED` for the org's root. SCP is
    /// auto-enabled at bootstrap; everything else flips through
    /// EnablePolicyType / DisablePolicyType.
    #[serde(default = "default_enabled_policy_types")]
    pub enabled_policy_types: HashSet<String>,
    /// Tag bag keyed by resource id (account id, OU id, root id, policy id).
    #[serde(default)]
    pub resource_tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Resource policy attached to the org via `PutResourcePolicy`.
    /// AWS Organizations supports a single resource policy per org;
    /// `None` means unset (the default).
    #[serde(default)]
    pub resource_policy: Option<String>,
    /// Billing-responsibility transfers keyed by id (`rt-...`), created
    /// via `InviteOrganizationToTransferResponsibility` and queried via
    /// the Describe / List{Inbound,Outbound} ops.
    #[serde(default)]
    pub responsibility_transfers: BTreeMap<String, ResponsibilityTransfer>,
}

fn default_enabled_policy_types() -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(POLICY_TYPE_SCP.to_string());
    s
}

impl OrganizationState {
    /// Bootstrap a new organization with `management_account_id` as the
    /// management account. Creates the root OU, seeds the AWS-managed
    /// `FullAWSAccess` SCP, and auto-attaches it to root (matching AWS's
    /// default behavior).
    pub fn bootstrap(management_account_id: &str) -> Self {
        let now = Utc::now();
        let org_id = format!("o-{}", random_id(10));
        // AWS root ids are 4-32 chars. Four was fine while only one
        // organization could exist; across a registry it collides at
        // 1/65536, which would let one organization's root id validate as
        // a target in another.
        let root_id = format!("r-{}", random_id(12));
        let org_arn = format!(
            "arn:aws:organizations::{}:organization/{}",
            management_account_id, org_id
        );
        let root_arn = format!(
            "arn:aws:organizations::{}:root/{}/{}",
            management_account_id, org_id, root_id
        );
        let mgmt_arn = format!(
            "arn:aws:organizations::{}:account/{}/{}",
            management_account_id, org_id, management_account_id
        );

        let mut policies = BTreeMap::new();
        policies.insert(
            FULL_AWS_ACCESS_POLICY_ID.to_string(),
            Policy {
                id: FULL_AWS_ACCESS_POLICY_ID.to_string(),
                arn: format!(
                    "arn:aws:organizations::aws:policy/service_control_policy/{}",
                    FULL_AWS_ACCESS_POLICY_ID
                ),
                name: FULL_AWS_ACCESS_POLICY_NAME.to_string(),
                description: FULL_AWS_ACCESS_POLICY_DESCRIPTION.to_string(),
                policy_type: POLICY_TYPE_SCP.to_string(),
                aws_managed: true,
                content: FULL_AWS_ACCESS_POLICY_CONTENT.to_string(),
            },
        );

        let mut attachments: BTreeMap<String, HashSet<String>> = BTreeMap::new();
        attachments
            .entry(root_id.clone())
            .or_default()
            .insert(FULL_AWS_ACCESS_POLICY_ID.to_string());

        let mut accounts = BTreeMap::new();
        accounts.insert(
            management_account_id.to_string(),
            MemberAccount {
                id: management_account_id.to_string(),
                arn: mgmt_arn.clone(),
                email: format!("{}@example.com", management_account_id),
                name: format!("Account {}", management_account_id),
                status: "ACTIVE".to_string(),
                joined_method: "INVITED".to_string(),
                joined_timestamp: now,
                parent_id: root_id.clone(),
            },
        );

        Self {
            org_id,
            org_arn,
            management_account_id: management_account_id.to_string(),
            management_account_arn: mgmt_arn,
            management_account_email: format!("{}@example.com", management_account_id),
            feature_set: FEATURE_SET_ALL.to_string(),
            root_id,
            root_arn,
            root_name: "Root".to_string(),
            created_at: now,
            ous: BTreeMap::new(),
            accounts,
            policies,
            attachments,
            create_account_requests: BTreeMap::new(),
            handshakes: BTreeMap::new(),
            trusted_services: BTreeMap::new(),
            delegated_administrators: BTreeMap::new(),
            enabled_policy_types: default_enabled_policy_types(),
            resource_tags: BTreeMap::new(),
            resource_policy: None,
            responsibility_transfers: BTreeMap::new(),
        }
    }

    /// Replace `resource_id`'s tag set with `tags`. Used for both
    /// adding new tag keys and overwriting existing ones; matches
    /// `TagResource` semantics.
    pub fn set_resource_tags(&mut self, resource_id: &str, tags: &[(String, String)]) {
        let entry = self
            .resource_tags
            .entry(resource_id.to_string())
            .or_default();
        for (k, v) in tags {
            entry.insert(k.clone(), v.clone());
        }
    }

    /// Drop `tag_keys` from `resource_id`'s tag set. No-op if absent.
    pub fn untag_resource(&mut self, resource_id: &str, tag_keys: &[String]) {
        if let Some(entry) = self.resource_tags.get_mut(resource_id) {
            for k in tag_keys {
                entry.remove(k);
            }
        }
    }

    /// Read `resource_id`'s tag set (alphabetical by key).
    pub fn list_resource_tags(&self, resource_id: &str) -> Vec<(String, String)> {
        self.resource_tags
            .get(resource_id)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Find the immediate parent of `child_id` (account or OU). Returns
    /// `(parent_id, parent_type)` matching the
    /// `(ROOT|ORGANIZATIONAL_UNIT)` AWS shape.
    pub fn parent_of(&self, child_id: &str) -> Option<(String, String)> {
        if let Some(account) = self.accounts.get(child_id) {
            return Some((
                account.parent_id.clone(),
                parent_type_for(self, &account.parent_id),
            ));
        }
        if let Some(ou) = self.ous.get(child_id) {
            return Some((ou.parent_id.clone(), parent_type_for(self, &ou.parent_id)));
        }
        None
    }

    /// List immediate children of `parent_id`. `child_type` is one of
    /// `ACCOUNT` or `ORGANIZATIONAL_UNIT`; AWS only allows one type per
    /// `ListChildren` call.
    pub fn list_children(&self, parent_id: &str, child_type: &str) -> Vec<String> {
        match child_type {
            "ACCOUNT" => self
                .accounts
                .values()
                .filter(|a| a.parent_id == parent_id)
                .map(|a| a.id.clone())
                .collect(),
            "ORGANIZATIONAL_UNIT" => self
                .ous
                .values()
                .filter(|o| o.parent_id == parent_id)
                .map(|o| o.id.clone())
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Promote a `CONSOLIDATED_BILLING` org to `ALL`. Idempotent.
    /// Real AWS Organizations sends an invitation to every member to
    /// confirm the upgrade; we shortcut to immediate success.
    pub fn enable_all_features(&mut self) {
        self.feature_set = FEATURE_SET_ALL.to_string();
    }

    /// Mark `policy_type` as enabled on the root.
    pub fn enable_policy_type(&mut self, policy_type: &str) {
        self.enabled_policy_types.insert(policy_type.to_string());
    }

    /// Mark `policy_type` as disabled on the root. Refuses to drop
    /// SCP — real Organizations doesn't allow it once an org exists.
    pub fn disable_policy_type(&mut self, policy_type: &str) -> Result<(), OrgError> {
        if policy_type == POLICY_TYPE_SCP {
            return Err(OrgError::PolicyTypeNotSupported(
                "SCP cannot be disabled".to_string(),
            ));
        }
        self.enabled_policy_types.remove(policy_type);
        Ok(())
    }

    /// List policy types in stable alphabetical order, with each
    /// type's enabled state.
    pub fn list_policy_type_statuses(&self) -> Vec<(String, String)> {
        let known = [
            POLICY_TYPE_SCP,
            "TAG_POLICY",
            "BACKUP_POLICY",
            "AISERVICES_OPT_OUT_POLICY",
            "RESOURCE_CONTROL_POLICY",
        ];
        let mut out = Vec::new();
        for t in known {
            let status = if self.enabled_policy_types.contains(t) {
                "ENABLED"
            } else {
                "DISABLED"
            };
            out.push((t.to_string(), status.to_string()));
        }
        out
    }

    /// Mint an account id unused by this organization.
    ///
    /// Prefer [`OrganizationsRegistry::next_account_id`], which checks
    /// every organization: an account id names one account process-wide,
    /// and a collision across organizations would break the "at most one
    /// organization per account" invariant.
    pub fn next_account_id(&self) -> String {
        Self::mint_account_id(|id| self.accounts.contains_key(id))
    }

    fn mint_account_id(taken: impl Fn(&str) -> bool) -> String {
        loop {
            let mut id = String::with_capacity(12);
            for _ in 0..12 {
                let u = Uuid::new_v4();
                let byte = u.as_bytes()[0];
                id.push(((byte % 10) + b'0') as char);
            }
            if !id.starts_with('0') && !taken(&id) {
                return id;
            }
        }
    }

    /// Begin a `CreateAccount` (or `CreateGovCloudAccount`) request.
    /// Reserves the new 12-digit account id and records an
    /// `IN_PROGRESS` `CreateAccountStatus` keyed by request id, but does
    /// NOT enroll the account into `self.accounts` yet — that happens
    /// when `complete_create_account` runs (mirroring AWS's async
    /// CreateAccount where the account only appears in `ListAccounts`
    /// after the status flips to `SUCCEEDED`).
    ///
    /// The caller is expected to spawn a background task that calls
    /// `complete_create_account(request_id)` after a synthetic delay so
    /// pollers can observe the IN_PROGRESS -> SUCCEEDED transition.
    /// `account_id` is minted by the caller, which holds the registry and
    /// can therefore guarantee the id is unused process-wide rather than
    /// only within this organization.
    pub fn begin_create_account(
        &mut self,
        email: &str,
        name: &str,
        account_id: String,
        gov_cloud_paired_id: Option<String>,
    ) -> CreateAccountStatus {
        let now = Utc::now();
        let request_id = format!("car-{}", random_id(20));
        let new_account_id = account_id;
        let status = CreateAccountStatus {
            id: request_id.clone(),
            account_id: Some(new_account_id),
            account_name: name.to_string(),
            state: "IN_PROGRESS".to_string(),
            requested_timestamp: now,
            completed_timestamp: None,
            failure_reason: None,
            gov_cloud_account_id: gov_cloud_paired_id,
            pending_email: Some(email.to_string()),
        };
        self.create_account_requests
            .insert(request_id, status.clone());
        status
    }

    /// Flip an `IN_PROGRESS` request to `SUCCEEDED` and enroll the
    /// reserved account id into `self.accounts` (plus the GovCloud
    /// paired id, if any). Idempotent: if the request is already
    /// terminal this is a no-op. Returns the updated status, or `None`
    /// if `request_id` is unknown.
    pub fn complete_create_account(&mut self, request_id: &str) -> Option<CreateAccountStatus> {
        let now = Utc::now();
        let (account_id, account_name, email, gov_cloud_id) = {
            let status = self.create_account_requests.get(request_id)?;
            if status.state != "IN_PROGRESS" {
                return Some(status.clone());
            }
            (
                status.account_id.clone()?,
                status.account_name.clone(),
                status.pending_email.clone().unwrap_or_default(),
                status.gov_cloud_account_id.clone(),
            )
        };

        // Enroll the commercial account.
        let arn = format!(
            "arn:aws:organizations::{}:account/{}/{}",
            self.management_account_id, self.org_id, account_id
        );
        self.accounts.insert(
            account_id.clone(),
            MemberAccount {
                id: account_id.clone(),
                arn,
                email: email.clone(),
                name: account_name.clone(),
                status: "ACTIVE".to_string(),
                joined_method: "CREATED".to_string(),
                joined_timestamp: now,
                parent_id: self.root_id.clone(),
            },
        );

        // Enroll the GovCloud paired account too. Real AWS creates a
        // mirror in the GovCloud partition; we keep the paired id
        // visible in `ListAccounts` so callers can see both.
        if let Some(gov_id) = &gov_cloud_id {
            let gov_arn = format!(
                "arn:aws-us-gov:organizations::{}:account/{}/{}",
                self.management_account_id, self.org_id, gov_id
            );
            // AWS creates the GovCloud account from the same owner
            // address, so the response carries it. The pair is the one
            // legitimate case of two accounts sharing an address;
            // `account_registered_with` skips the mirror so resolution
            // still lands on exactly one account.
            self.accounts.insert(
                gov_id.clone(),
                MemberAccount {
                    id: gov_id.clone(),
                    arn: gov_arn,
                    email,
                    name: account_name,
                    status: "ACTIVE".to_string(),
                    joined_method: "CREATED".to_string(),
                    joined_timestamp: now,
                    parent_id: self.root_id.clone(),
                },
            );
        }

        let status = self.create_account_requests.get_mut(request_id)?;
        status.state = "SUCCEEDED".to_string();
        status.completed_timestamp = Some(now);
        status.pending_email = None;
        Some(status.clone())
    }

    /// Mark an `IN_PROGRESS` request as `FAILED` with the given reason.
    /// Used for synthetic failure injection in tests; real AWS sets a
    /// reason like `EMAIL_ALREADY_EXISTS`. No accounts are enrolled.
    pub fn fail_create_account(
        &mut self,
        request_id: &str,
        reason: &str,
    ) -> Option<CreateAccountStatus> {
        let status = self.create_account_requests.get_mut(request_id)?;
        if status.state != "IN_PROGRESS" {
            return Some(status.clone());
        }
        status.state = "FAILED".to_string();
        status.completed_timestamp = Some(Utc::now());
        status.failure_reason = Some(reason.to_string());
        status.pending_email = None;
        let snapshot = status.clone();
        // `CreateAccount` applies create-time tags to the reserved id
        // straight away, on the old assumption that every request ends in
        // SUCCEEDED. A failed request's id never becomes an account, so
        // those tags would otherwise linger on an id `ListAccounts` does
        // not know and AWS answers for with `TargetNotFoundException`.
        if let Some(account_id) = &snapshot.account_id {
            self.resource_tags.remove(account_id);
        }
        if let Some(gov_id) = &snapshot.gov_cloud_account_id {
            self.resource_tags.remove(gov_id);
        }
        Some(snapshot)
    }

    /// Issue a new pending invitation handshake to `target_account_id`.
    /// Idempotent: a duplicate live handshake to the same account
    /// returns `DuplicateHandshakeForAccount`.
    pub fn invite_account(
        &mut self,
        source_account_id: &str,
        target_kind: &str,
        target_account_id: &str,
        target_email: Option<String>,
        notes: Option<String>,
    ) -> Result<Handshake, OrgError> {
        // Compare against the resolved account id: an EMAIL target records
        // the address, which is never a key in `accounts` and never equal
        // to another handshake's ACCOUNT-form target. Without this, an
        // account already enrolled could be re-invited by email, and one
        // account could hold two live invitations under its two spellings.
        // The kind is the caller's declared `Target.Type`, so this agrees
        // with the cross-organization guard in the service layer rather
        // than re-deriving a different answer from the string's shape.
        // Same order as `OrganizationsRegistry::resolve_target_account`:
        // a registered address names its own account, and the synthetic
        // decode is only a fallback. The reverse order resolved to an
        // account nobody owns, so the already-a-member and
        // duplicate-handshake guards below both missed.
        let resolved = self
            .accounts
            .values()
            .find(|account| {
                target_kind == "EMAIL"
                    && account.email == target_account_id
                    && !is_gov_cloud(account)
                    && account.status != "SUSPENDED"
            })
            .map(|account| account.id.clone())
            .or_else(|| self::target_account_id(target_kind, target_account_id));
        if let Some(target) = &resolved {
            if self.accounts.contains_key(target) {
                return Err(OrgError::AccountAlreadyMember(target.clone()));
            }
        }
        for h in self.handshakes.values() {
            // Only another membership INVITE collides. A
            // TRANSFER_RESPONSIBILITY handshake to the same account is a
            // different arrangement entirely, and AWS keeps the two
            // handshake actions independent.
            if h.action != "INVITE" {
                continue;
            }
            let same_target = h.target_account_id == target_account_id
                || (resolved.is_some()
                    && self::target_account_id(&h.target_kind, &h.target_account_id) == resolved);
            if same_target && matches!(h.state.as_str(), "REQUESTED" | "OPEN") {
                return Err(OrgError::DuplicateHandshakeForAccount(
                    target_account_id.to_string(),
                ));
            }
        }
        let now = Utc::now();
        let id = format!("h-{}", random_id(32));
        let arn = format!(
            "arn:aws:organizations::{}:handshake/{}/invite/{}",
            self.management_account_id, self.org_id, id
        );
        let kind = target_kind.to_string();
        let handshake = Handshake {
            id: id.clone(),
            arn,
            action: "INVITE".to_string(),
            state: "OPEN".to_string(),
            requested_timestamp: now,
            expiration_timestamp: now + chrono::Duration::days(15),
            source_account_id: source_account_id.to_string(),
            target_account_id: target_account_id.to_string(),
            target_email,
            target_kind: kind,
            notes,
            organization_id: self.org_id.clone(),
            // An INVITE carries no responsibility transfer.
            responsibility_transfer_id: None,
        };
        self.handshakes.insert(id, handshake.clone());
        Ok(handshake)
    }

    /// Move a live handshake from `OPEN` into `new_state`. Caller decides
    /// whether the transition is allowed for the current API caller —
    /// this just enforces lifecycle (open -> terminal). The original
    /// `ExpirationTimestamp` is preserved (it's the 15-day deadline,
    /// not a resolved-at marker).
    /// `enrolling_account` is the account the caller's party gate already
    /// proved to be the target. Re-deriving it here from the stored
    /// target -- which the gate resolves registry-aware and this could
    /// only resolve id-only -- let the two disagree: an accept could
    /// enroll a phantom account nobody created, or report ACCEPTED while
    /// enrolling nobody.
    pub fn resolve_handshake(
        &mut self,
        id: &str,
        new_state: &str,
        enrolling_account: Option<&str>,
        enrolling_email: Option<String>,
    ) -> Result<Handshake, OrgError> {
        let handshake = self
            .handshakes
            .get_mut(id)
            .ok_or_else(|| OrgError::HandshakeNotFound(id.to_string()))?;
        if !matches!(handshake.state.as_str(), "OPEN" | "REQUESTED") {
            return Err(OrgError::HandshakeAlreadyResolved(handshake.state.clone()));
        }
        if !matches!(new_state, "ACCEPTED" | "DECLINED" | "CANCELED" | "EXPIRED") {
            return Err(OrgError::InvalidHandshakeState(new_state.to_string()));
        }
        handshake.state = new_state.to_string();
        let snapshot = handshake.clone();
        // Only an INVITE enrolls the target. A TRANSFER_RESPONSIBILITY
        // handshake targets the management account of ANOTHER organization,
        // so enrolling on accept would put that account in two
        // organizations at once.
        // An EMAIL target records the address, so resolve it to the account
        // it names — enrolling the raw field would key a member account by
        // an email string.
        let enrolling = if new_state == "ACCEPTED" && snapshot.action == "INVITE" {
            enrolling_account
                .map(str::to_string)
                .filter(|target| !self.accounts.contains_key(target))
        } else {
            None
        };
        if let Some(target) = enrolling {
            let now = Utc::now();
            let arn = format!(
                "arn:aws:organizations::{}:account/{}/{}",
                self.management_account_id, self.org_id, target
            );
            // The caller supplies the address: an account's address must
            // be unique across the whole registry, which this organization
            // cannot see on its own.
            let email = enrolling_email
                .clone()
                .or_else(|| snapshot.target_email.clone())
                .unwrap_or_else(|| format!("{target}@example.com"));
            self.accounts.insert(
                target.clone(),
                MemberAccount {
                    id: target.clone(),
                    arn,
                    email,
                    name: format!("Account {target}"),
                    status: "ACTIVE".to_string(),
                    joined_method: "INVITED".to_string(),
                    joined_timestamp: now,
                    parent_id: self.root_id.clone(),
                },
            );
        }
        // A TRANSFER_RESPONSIBILITY handshake carries a responsibility
        // transfer. Resolving the handshake without moving the transfer
        // left two sources of truth disagreeing: the handshake ACCEPTED,
        // the transfer still REQUESTED with a live handshake id.
        if snapshot.action == "TRANSFER_RESPONSIBILITY" {
            if let Some(transfer) = self
                .responsibility_transfers
                .values_mut()
                .find(|t| t.active_handshake_id.as_deref() == Some(id))
            {
                transfer.status = new_state.to_string();
                transfer.active_handshake_id = None;
                // An ACCEPTED transfer is starting, not ending -- stamping
                // an end time here reported it as simultaneously active and
                // already over.
                if matches!(new_state, "DECLINED" | "CANCELED" | "EXPIRED") {
                    transfer.end_timestamp = Some(Utc::now());
                }
            }
        }
        Ok(snapshot)
    }

    /// Flip every handshake past its `ExpirationTimestamp` to
    /// `EXPIRED`, and return how many moved.
    ///
    /// AWS gives a handshake 15 days and expires it on its own; nothing
    /// here ever did, so an overdue invitation stayed `OPEN` and
    /// acceptable forever, and the `ExpirationTimestamp` fakecloud
    /// reported was decoration. Expiry runs through `resolve_handshake`
    /// like every other terminal transition, so a responsibility
    /// transfer riding an expired handshake ends with it.
    pub fn expire_stale_handshakes(&mut self, now: DateTime<Utc>) -> usize {
        let stale: Vec<(String, DateTime<Utc>)> = self
            .handshakes
            .values()
            .filter(|h| {
                matches!(h.state.as_str(), "OPEN" | "REQUESTED") && h.expiration_timestamp <= now
            })
            .map(|h| (h.id.clone(), h.expiration_timestamp))
            .collect();
        for (id, deadline) in &stale {
            // Bind the transfer BEFORE resolving: `resolve_handshake`
            // clears the `active_handshake_id` that identifies it.
            let riding = self
                .responsibility_transfers
                .values()
                .find(|t| t.active_handshake_id.as_deref() == Some(id.as_str()))
                .map(|t| t.id.clone());
            // The state check above is exactly the one `resolve_handshake`
            // re-applies, so this cannot fail.
            let _ = self.resolve_handshake(id, "EXPIRED", None, None);
            // A transfer ends when its handshake lapsed, not when the
            // sweep noticed. `resolve_handshake` stamps "now", which is
            // right for a decline or a cancel -- somebody acted at that
            // instant -- but an idle process would otherwise report an
            // `EndTimestamp` days after the `ExpirationTimestamp` on the
            // same record.
            if let Some(transfer) = riding.and_then(|id| self.responsibility_transfers.get_mut(&id))
            {
                transfer.end_timestamp = Some(*deadline);
            }
        }
        stale.len()
    }

    /// Every handshake this organization holds.
    ///
    /// Filtering by target is deliberately NOT offered here: deciding
    /// whether a target names an account needs the registry, so callers
    /// filter with [`OrganizationsRegistry::account_matches_target`]. An
    /// id-only filter here would silently encode a different rule.
    pub fn list_handshakes(&self) -> Vec<Handshake> {
        self.handshakes.values().cloned().collect()
    }

    /// Mark `service_principal` as a trusted service. Idempotent: the
    /// originally-recorded `DateEnabled` is preserved on repeat calls
    /// (matches AWS Organizations).
    pub fn enable_aws_service_access(&mut self, service_principal: &str) {
        self.trusted_services
            .entry(service_principal.to_string())
            .or_insert_with(Utc::now);
    }

    /// Drop `service_principal` from the trusted set. Also removes any
    /// delegated administrators registered for that principal — real
    /// Organizations rejects DisableAWSServiceAccess if delegates exist;
    /// we mirror that gate via a separate `disable_aws_service_access`
    /// returning `Err` when delegates are still registered.
    pub fn disable_aws_service_access(&mut self, service_principal: &str) -> Result<(), OrgError> {
        if let Some(delegates) = self.delegated_administrators.get(service_principal) {
            if !delegates.is_empty() {
                return Err(OrgError::DelegatedAdministratorAlreadyRegistered(
                    service_principal.to_string(),
                ));
            }
        }
        self.trusted_services.remove(service_principal);
        self.delegated_administrators.remove(service_principal);
        Ok(())
    }

    /// Iterate enabled trusted services in alphabetical order, paired
    /// with the `DateEnabled` timestamp captured at first enable.
    pub fn list_trusted_services(&self) -> Vec<(String, DateTime<Utc>)> {
        // BTreeMap iterates in key order, so this is already alphabetical.
        self.trusted_services
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Register `account_id` as a delegated administrator for the given
    /// `service_principal`. Requires the service to already be trusted
    /// (matches AWS Organizations) and the account to already be a member.
    pub fn register_delegated_administrator(
        &mut self,
        account_id: &str,
        service_principal: &str,
    ) -> Result<DelegatedAdministrator, OrgError> {
        if !self.accounts.contains_key(account_id) {
            return Err(OrgError::AccountNotFound(account_id.to_string()));
        }
        if !self.trusted_services.contains_key(service_principal) {
            return Err(OrgError::AWSServiceAccessNotEnabled(
                service_principal.to_string(),
            ));
        }
        let entry = self
            .delegated_administrators
            .entry(service_principal.to_string())
            .or_default();
        if entry.contains_key(account_id) {
            return Err(OrgError::DelegatedAdministratorAlreadyRegistered(
                account_id.to_string(),
            ));
        }
        let admin = DelegatedAdministrator {
            account_id: account_id.to_string(),
            service_principal: service_principal.to_string(),
            registered_at: Utc::now(),
        };
        entry.insert(account_id.to_string(), admin.clone());
        Ok(admin)
    }

    /// Drop a delegated administrator registration.
    pub fn deregister_delegated_administrator(
        &mut self,
        account_id: &str,
        service_principal: &str,
    ) -> Result<(), OrgError> {
        // Both arms name the ACCOUNT, which is what the error says and
        // what AWS reports. Naming the service principal when no
        // account is registered for it at all rendered "Account
        // config.amazonaws.com is not registered as a delegated
        // administrator."
        let registered = self
            .delegated_administrators
            .get_mut(service_principal)
            .is_some_and(|entry| entry.remove(account_id).is_some());
        if !registered {
            return Err(OrgError::DelegatedAdministratorNotRegistered(
                account_id.to_string(),
            ));
        }
        Ok(())
    }

    /// Is `account_id` a delegated administrator for any service?
    pub fn is_delegated_administrator(&self, account_id: &str) -> bool {
        self.delegated_administrators
            .values()
            .any(|admins| admins.contains_key(account_id))
    }

    /// List delegated administrators, optionally filtered by service.
    pub fn list_delegated_administrators(
        &self,
        service_principal_filter: Option<&str>,
    ) -> Vec<DelegatedAdministrator> {
        let mut out = Vec::new();
        for (svc, admins) in &self.delegated_administrators {
            if let Some(filter) = service_principal_filter {
                if filter != svc {
                    continue;
                }
            }
            for admin in admins.values() {
                out.push(admin.clone());
            }
        }
        out
    }

    /// List service principals that `account_id` is a delegated admin
    /// for, paired with the per-service `DelegationEnabledDate`.
    /// Iteration order is alphabetical by service principal (the
    /// `delegated_administrators` BTreeMap key order).
    pub fn list_delegated_services_for_account(
        &self,
        account_id: &str,
    ) -> Vec<(String, DateTime<Utc>)> {
        let mut out = Vec::new();
        for (svc, admins) in &self.delegated_administrators {
            if let Some(admin) = admins.get(account_id) {
                out.push((svc.clone(), admin.registered_at));
            }
        }
        out
    }

    /// Look up a `CreateAccountStatus` by its `car-...` id without
    /// mutating it. The deferred completion is driven by a background
    /// tokio task in `OrganizationsService::create_account`, not by
    /// `DescribeCreateAccountStatus`, so this is a pure read.
    pub fn describe_create_account(&self, request_id: &str) -> Option<CreateAccountStatus> {
        self.create_account_requests.get(request_id).cloned()
    }

    /// Mark `account_id` as `SUSPENDED` (mirrors `CloseAccount`). The
    /// account stays enrolled in the org so `ListAccounts` still shows
    /// it, matching real AWS retention semantics.
    pub fn close_account(&mut self, account_id: &str) -> Result<(), OrgError> {
        if account_id == self.management_account_id {
            return Err(OrgError::AccountChangesNotAllowed(account_id.to_string()));
        }
        let account = self
            .accounts
            .get_mut(account_id)
            .ok_or_else(|| OrgError::AccountNotFound(account_id.to_string()))?;
        account.status = "SUSPENDED".to_string();
        Ok(())
    }

    /// Remove a member account from the organization. The management
    /// account cannot be removed.
    pub fn remove_account(&mut self, account_id: &str) -> Result<(), OrgError> {
        if account_id == self.management_account_id {
            return Err(OrgError::AccountChangesNotAllowed(account_id.to_string()));
        }
        if self.accounts.remove(account_id).is_none() {
            return Err(OrgError::AccountNotFound(account_id.to_string()));
        }
        // Detach any direct policy attachments for the now-orphan id.
        self.attachments.remove(account_id);
        // Tags are keyed by account id, so an untagged id would come back
        // wearing them if the account is ever re-enrolled. `ListAccounts`
        // does not know the id meanwhile, which is the same reason
        // `fail_create_account` drops the tags it reserved.
        self.resource_tags.remove(account_id);
        // And drop every delegated-administrator registration it held.
        // The registration is an organization's grant to one of its OWN
        // members, so it cannot outlive the membership: leaving it behind
        // meant `ListDelegatedServicesForAccount` still answered for an
        // account the organization no longer contains, and -- now that
        // the registration unlocks the organization's read operations --
        // an account that left and was later re-invited came back holding
        // delegated-administrator authority nobody had granted it.
        for admins in self.delegated_administrators.values_mut() {
            admins.remove(account_id);
        }
        self.delegated_administrators
            .retain(|_, admins| !admins.is_empty());
        Ok(())
    }

    /// Returns `true` iff `account_id` is the management account.
    pub fn is_management(&self, account_id: &str) -> bool {
        account_id == self.management_account_id
    }

    /// Enroll `account_id` into the root OU as a member of the
    /// organization if not already known. No-op when the account is
    /// already enrolled anywhere in the tree. Backs the opt-in
    /// `organizationId` of `/_fakecloud/iam/create-admin`, which is the
    /// shortcut equivalent of an invite/accept handshake. Bootstrapping
    /// an admin without naming an organization never calls this — a
    /// freshly vended account must stay standalone, not silently inherit
    /// another organization's SCPs, metadata and stack-set targeting
    /// (#2543).
    pub fn enroll_account_if_missing(&mut self, account_id: &str) {
        if self.accounts.contains_key(account_id) {
            return;
        }
        let arn = format!(
            "arn:aws:organizations::{}:account/{}/{}",
            self.management_account_id, self.org_id, account_id
        );
        self.accounts.insert(
            account_id.to_string(),
            MemberAccount {
                id: account_id.to_string(),
                arn,
                email: format!("{}@example.com", account_id),
                name: format!("Account {}", account_id),
                status: "ACTIVE".to_string(),
                joined_method: "INVITED".to_string(),
                joined_timestamp: Utc::now(),
                parent_id: self.root_id.clone(),
            },
        );
    }

    /// Create a new OU under `parent_id` (which must be the root or
    /// another existing OU). Returns the created OU on success.
    ///
    /// Errors:
    /// - `ParentNotFoundException` — `parent_id` does not exist in
    ///   this org (neither root nor a known OU).
    /// - `DuplicateOrganizationalUnitException` — another OU with the
    ///   same name already lives directly under `parent_id`.
    pub fn create_ou(
        &mut self,
        parent_id: &str,
        name: &str,
    ) -> Result<OrganizationalUnit, OrgError> {
        if parent_id != self.root_id && !self.ous.contains_key(parent_id) {
            return Err(OrgError::ParentNotFound(parent_id.to_string()));
        }
        let dup = self
            .ous
            .values()
            .any(|ou| ou.parent_id == parent_id && ou.name == name);
        if dup {
            return Err(OrgError::DuplicateOrganizationalUnit(name.to_string()));
        }
        let root_suffix = self.root_id.strip_prefix("r-").unwrap_or(&self.root_id);
        let id = format!("ou-{}-{}", root_suffix, random_id(8));
        let arn = format!(
            "arn:aws:organizations::{}:ou/{}/{}",
            self.management_account_id, self.org_id, id
        );
        let ou = OrganizationalUnit {
            id: id.clone(),
            arn,
            name: name.to_string(),
            parent_id: parent_id.to_string(),
        };
        self.ous.insert(id, ou.clone());
        Ok(ou)
    }

    /// Rename an existing OU.
    pub fn rename_ou(
        &mut self,
        ou_id: &str,
        new_name: &str,
    ) -> Result<OrganizationalUnit, OrgError> {
        let parent_id = self
            .ous
            .get(ou_id)
            .ok_or_else(|| OrgError::OrganizationalUnitNotFound(ou_id.to_string()))?
            .parent_id
            .clone();
        let dup = self
            .ous
            .values()
            .any(|ou| ou.id != ou_id && ou.parent_id == parent_id && ou.name == new_name);
        if dup {
            return Err(OrgError::DuplicateOrganizationalUnit(new_name.to_string()));
        }
        let ou = self.ous.get_mut(ou_id).unwrap();
        ou.name = new_name.to_string();
        Ok(ou.clone())
    }

    /// Delete an OU. Fails with `OrganizationalUnitNotEmptyException`
    /// if the OU contains any child OUs or member accounts.
    pub fn delete_ou(&mut self, ou_id: &str) -> Result<(), OrgError> {
        if !self.ous.contains_key(ou_id) {
            return Err(OrgError::OrganizationalUnitNotFound(ou_id.to_string()));
        }
        let has_child_ou = self.ous.values().any(|ou| ou.parent_id == ou_id);
        let has_account = self.accounts.values().any(|a| a.parent_id == ou_id);
        if has_child_ou || has_account {
            return Err(OrgError::OrganizationalUnitNotEmpty(ou_id.to_string()));
        }
        // Detach all policies from the deleted target so stale pointers
        // don't survive.
        self.attachments.remove(ou_id);
        self.ous.remove(ou_id);
        Ok(())
    }

    /// Move an account between OUs.
    ///
    /// Errors:
    /// - `AccountNotFoundException`
    /// - `SourceParentNotFoundException` when `source_parent` is not
    ///   the account's current parent
    /// - `DestinationParentNotFoundException` when `dest_parent` is
    ///   not root or a known OU
    pub fn move_account(
        &mut self,
        account_id: &str,
        source_parent: &str,
        dest_parent: &str,
    ) -> Result<(), OrgError> {
        let account = self
            .accounts
            .get_mut(account_id)
            .ok_or_else(|| OrgError::AccountNotFound(account_id.to_string()))?;
        if account.parent_id != source_parent {
            return Err(OrgError::SourceParentNotFound(source_parent.to_string()));
        }
        let dest_exists = dest_parent == self.root_id || self.ous.contains_key(dest_parent);
        if !dest_exists {
            return Err(OrgError::DestinationParentNotFound(dest_parent.to_string()));
        }
        account.parent_id = dest_parent.to_string();
        Ok(())
    }

    /// Create a customer-managed SCP. Returns the created policy on
    /// success.
    ///
    /// Errors:
    /// - `PolicyTypeNotSupportedException` — `policy_type` isn't SCP.
    /// - `MalformedPolicyDocumentException` — `content` doesn't parse
    ///   as JSON.
    /// - `DuplicatePolicyException` — another SCP with the same name.
    pub fn create_policy(
        &mut self,
        name: &str,
        description: &str,
        content: &str,
        policy_type: &str,
    ) -> Result<Policy, OrgError> {
        if !is_supported_policy_type(policy_type) {
            return Err(OrgError::PolicyTypeNotSupported(policy_type.to_string()));
        }
        if serde_json::from_str::<serde_json::Value>(content).is_err() {
            return Err(OrgError::MalformedPolicyDocument);
        }
        let dup = self
            .policies
            .values()
            .any(|p| p.policy_type == policy_type && p.name == name);
        if dup {
            return Err(OrgError::DuplicatePolicy(name.to_string()));
        }
        let id = format!("p-{}", random_id(8));
        let arn = format!(
            "arn:aws:organizations::{}:policy/{}/{}/{}",
            self.management_account_id,
            self.org_id,
            policy_type_path_segment(policy_type),
            id,
        );
        let policy = Policy {
            id: id.clone(),
            arn,
            name: name.to_string(),
            description: description.to_string(),
            policy_type: policy_type.to_string(),
            aws_managed: false,
            content: content.to_string(),
        };
        self.policies.insert(id, policy.clone());
        Ok(policy)
    }

    /// Update an existing customer-managed SCP. Any `Option::Some`
    /// field overrides the stored value; `None` leaves it untouched.
    /// AWS-managed policies (e.g. `FullAWSAccess`) are immutable.
    pub fn update_policy(
        &mut self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
        content: Option<&str>,
    ) -> Result<Policy, OrgError> {
        let policy = self
            .policies
            .get(id)
            .ok_or_else(|| OrgError::PolicyNotFound(id.to_string()))?;
        if policy.aws_managed {
            return Err(OrgError::PolicyChangesNotAllowed(id.to_string()));
        }
        let policy_type = policy.policy_type.clone();
        if let Some(new_name) = name {
            let dup = self
                .policies
                .values()
                .any(|p| p.id != id && p.policy_type == policy_type && p.name == new_name);
            if dup {
                return Err(OrgError::DuplicatePolicy(new_name.to_string()));
            }
        }
        if let Some(c) = content {
            if serde_json::from_str::<serde_json::Value>(c).is_err() {
                return Err(OrgError::MalformedPolicyDocument);
            }
        }
        let policy = self.policies.get_mut(id).unwrap();
        if let Some(n) = name {
            policy.name = n.to_string();
        }
        if let Some(d) = description {
            policy.description = d.to_string();
        }
        if let Some(c) = content {
            policy.content = c.to_string();
        }
        Ok(policy.clone())
    }

    /// Delete a customer-managed SCP. Fails with `PolicyInUseException`
    /// if the policy is still attached to any target.
    pub fn delete_policy(&mut self, id: &str) -> Result<(), OrgError> {
        let policy = self
            .policies
            .get(id)
            .ok_or_else(|| OrgError::PolicyNotFound(id.to_string()))?;
        if policy.aws_managed {
            return Err(OrgError::PolicyChangesNotAllowed(id.to_string()));
        }
        let attached = self.attachments.values().any(|set| set.contains(id));
        if attached {
            return Err(OrgError::PolicyInUse(id.to_string()));
        }
        self.policies.remove(id);
        Ok(())
    }

    /// Verify `target_id` is one of root, an OU, or a member account.
    pub fn target_exists(&self, target_id: &str) -> bool {
        target_id == self.root_id
            || self.ous.contains_key(target_id)
            || self.accounts.contains_key(target_id)
    }

    /// Type tag for target listings (`ROOT`, `ORGANIZATIONAL_UNIT`,
    /// `ACCOUNT`). Returns `None` when the target is unknown.
    pub fn target_type(&self, target_id: &str) -> Option<&'static str> {
        if target_id == self.root_id {
            Some("ROOT")
        } else if self.ous.contains_key(target_id) {
            Some("ORGANIZATIONAL_UNIT")
        } else if self.accounts.contains_key(target_id) {
            Some("ACCOUNT")
        } else {
            None
        }
    }

    /// Attach a policy to a target. No-op if already attached (AWS
    /// treats re-attach as success; matches the documented idempotent
    /// behaviour).
    pub fn attach_policy(&mut self, policy_id: &str, target_id: &str) -> Result<(), OrgError> {
        if !self.policies.contains_key(policy_id) {
            return Err(OrgError::PolicyNotFound(policy_id.to_string()));
        }
        if !self.target_exists(target_id) {
            return Err(OrgError::TargetNotFound(target_id.to_string()));
        }
        self.attachments
            .entry(target_id.to_string())
            .or_default()
            .insert(policy_id.to_string());
        Ok(())
    }

    /// Detach a policy from a target.
    ///
    /// Errors:
    /// - `PolicyNotFoundException`
    /// - `TargetNotFoundException`
    /// - `PolicyNotAttachedException` — policy is known but not
    ///   attached to `target_id`.
    pub fn detach_policy(&mut self, policy_id: &str, target_id: &str) -> Result<(), OrgError> {
        if !self.policies.contains_key(policy_id) {
            return Err(OrgError::PolicyNotFound(policy_id.to_string()));
        }
        if !self.target_exists(target_id) {
            return Err(OrgError::TargetNotFound(target_id.to_string()));
        }
        let set = self
            .attachments
            .get_mut(target_id)
            .ok_or_else(|| OrgError::PolicyNotAttached(policy_id.to_string()))?;
        if !set.remove(policy_id) {
            return Err(OrgError::PolicyNotAttached(policy_id.to_string()));
        }
        if set.is_empty() {
            self.attachments.remove(target_id);
        }
        Ok(())
    }

    /// All SCPs attached directly to `target_id` (no inheritance).
    pub fn policies_for_target(&self, target_id: &str) -> Result<Vec<&Policy>, OrgError> {
        if !self.target_exists(target_id) {
            return Err(OrgError::TargetNotFound(target_id.to_string()));
        }
        let ids = match self.attachments.get(target_id) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        Ok(ids.iter().filter_map(|id| self.policies.get(id)).collect())
    }

    /// All targets that carry a direct attachment of `policy_id`. Each
    /// entry pairs the target id with its type tag so callers can
    /// render the full AWS response shape.
    pub fn targets_for_policy(
        &self,
        policy_id: &str,
    ) -> Result<Vec<(&str, &str, &'static str)>, OrgError> {
        if !self.policies.contains_key(policy_id) {
            return Err(OrgError::PolicyNotFound(policy_id.to_string()));
        }
        let mut out = Vec::new();
        for (target_id, set) in &self.attachments {
            if set.contains(policy_id) {
                let ttype = self
                    .target_type(target_id)
                    .expect("attachment target must still exist");
                let name = match ttype {
                    "ROOT" => self.root_name.as_str(),
                    "ORGANIZATIONAL_UNIT" => self
                        .ous
                        .get(target_id)
                        .map(|o| o.name.as_str())
                        .unwrap_or(""),
                    "ACCOUNT" => self
                        .accounts
                        .get(target_id)
                        .map(|a| a.name.as_str())
                        .unwrap_or(""),
                    _ => "",
                };
                out.push((target_id.as_str(), name, ttype));
            }
        }
        Ok(out)
    }
}

/// Typed errors used by organization state mutations so the service
/// layer can translate each into the correct AWS exception code.
#[derive(Debug)]
pub enum OrgError {
    ParentNotFound(String),
    DuplicateOrganizationalUnit(String),
    OrganizationalUnitNotFound(String),
    OrganizationalUnitNotEmpty(String),
    AccountNotFound(String),
    SourceParentNotFound(String),
    DestinationParentNotFound(String),
    PolicyNotFound(String),
    DuplicatePolicy(String),
    MalformedPolicyDocument,
    PolicyTypeNotSupported(String),
    PolicyChangesNotAllowed(String),
    PolicyInUse(String),
    PolicyNotAttached(String),
    TargetNotFound(String),
    AccountChangesNotAllowed(String),
    CreateAccountStatusNotFound(String),
    HandshakeNotFound(String),
    HandshakeAlreadyResolved(String),
    InvalidHandshakeState(String),
    InvalidHandshakeParty(String),
    DuplicateHandshakeForAccount(String),
    AccountAlreadyMember(String),
    /// The account belongs to a DIFFERENT organization. An account can
    /// be in at most one organization, so it must leave (or be removed
    /// from) that one before another can invite it.
    AccountInAnotherOrganization(String),
    AWSServiceAccessNotEnabled(String),
    DelegatedAdministratorAlreadyRegistered(String),
    DelegatedAdministratorNotRegistered(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DelegatedAdministrator {
    pub account_id: String,
    pub service_principal: String,
    pub registered_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateAccountStatus {
    pub id: String,
    pub account_id: Option<String>,
    pub account_name: String,
    pub state: String,
    pub requested_timestamp: DateTime<Utc>,
    #[serde(default)]
    pub completed_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub failure_reason: Option<String>,
    #[serde(default)]
    pub gov_cloud_account_id: Option<String>,
    /// Email captured by `BeginCreateAccount`. Held here only while the
    /// request is `IN_PROGRESS` so the deferred completion task can
    /// stamp the right email on the resulting `MemberAccount`. Cleared
    /// when the status flips to a terminal state. Not part of the
    /// public API surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_email: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrganizationalUnit {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub parent_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberAccount {
    pub id: String,
    pub arn: String,
    pub email: String,
    pub name: String,
    pub status: String,
    pub joined_method: String,
    pub joined_timestamp: DateTime<Utc>,
    pub parent_id: String,
}

/// `InviteAccountToOrganization` handshake. Captures both parties so
/// `ListHandshakesForAccount` can filter by destination, and stores
/// the resolved state plus when each transition happened.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Handshake {
    pub id: String,
    pub arn: String,
    pub action: String,
    pub state: String,
    pub requested_timestamp: DateTime<Utc>,
    pub expiration_timestamp: DateTime<Utc>,
    pub source_account_id: String,
    pub target_account_id: String,
    pub target_email: Option<String>,
    pub target_kind: String,
    pub notes: Option<String>,
    pub organization_id: String,
    /// The responsibility transfer this handshake carries, for
    /// `TRANSFER_RESPONSIBILITY` handshakes only.
    ///
    /// The link has to live on the handshake because the transfer's own
    /// `active_handshake_id` is cleared the moment the handshake
    /// resolves -- reading the link from that side made an ACCEPTED
    /// handshake report no transfer at all.
    #[serde(default)]
    pub responsibility_transfer_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Policy {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub description: String,
    pub policy_type: String,
    pub aws_managed: bool,
    pub content: String,
}

/// A billing-responsibility transfer between organizations, created via
/// `InviteOrganizationToTransferResponsibility`. Transfers ride on a
/// handshake (the invited org accepts/declines it) and progress through
/// the `ResponsibilityTransferStatus` lifecycle
/// (`REQUESTED` -> `ACCEPTED` / `DECLINED` / `CANCELED` / `EXPIRED` /
/// `WITHDRAWN`). `direction` records whether the transfer left this org
/// (`OUTBOUND`) or arrived at it (`INBOUND`), which drives the
/// `List{Inbound,Outbound}ResponsibilityTransfers` filters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsibilityTransfer {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub transfer_type: String,
    pub status: String,
    pub direction: String,
    pub source_management_account_id: String,
    pub source_management_account_email: String,
    pub target_management_account_id: String,
    pub target_management_account_email: String,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: Option<DateTime<Utc>>,
    pub active_handshake_id: Option<String>,
}

/// Resolve `parent_id` to the AWS-shape parent type:
/// - root id (starts with `r-`) -> "ROOT"
/// - everything else -> "ORGANIZATIONAL_UNIT"
fn parent_type_for(_org: &OrganizationState, parent_id: &str) -> String {
    if parent_id.starts_with("r-") {
        "ROOT".to_string()
    } else {
        "ORGANIZATIONAL_UNIT".to_string()
    }
}

/// AWS Organizations policy types we support: SCP plus the four
/// non-SCP types that share the same dispatch path. Real Organizations
/// also has SCP enabled at bootstrap and the rest opt-in via
/// EnablePolicyType, but `create_policy` doesn't enforce that
/// enablement gate (mirroring AWS, which lets you create the policy
/// before enabling it on the root).
fn is_supported_policy_type(policy_type: &str) -> bool {
    matches!(
        policy_type,
        POLICY_TYPE_SCP
            | "TAG_POLICY"
            | "BACKUP_POLICY"
            | "AISERVICES_OPT_OUT_POLICY"
            | "RESOURCE_CONTROL_POLICY"
    )
}

/// Map the policy type to the lowercase ARN path segment AWS uses.
/// Example: `SERVICE_CONTROL_POLICY` -> `service_control_policy`.
fn policy_type_path_segment(policy_type: &str) -> &'static str {
    match policy_type {
        POLICY_TYPE_SCP => "service_control_policy",
        "TAG_POLICY" => "tag_policy",
        "BACKUP_POLICY" => "backup_policy",
        "AISERVICES_OPT_OUT_POLICY" => "aiservices_opt_out_policy",
        "RESOURCE_CONTROL_POLICY" => "resource_control_policy",
        _ => "policy",
    }
}

/// Generate a lowercase alphanumeric ID fragment of `len` characters.
/// Used for org/root/OU/policy IDs. Pulled from a UUID v4 so the PRNG
/// is the one already pulled in by the rest of fakecloud.
pub fn random_id(len: usize) -> String {
    let mut out = String::with_capacity(len);
    while out.len() < len {
        let u = Uuid::new_v4().simple().to_string();
        for ch in u.chars() {
            if out.len() >= len {
                break;
            }
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_has_root_and_full_aws_access() {
        let org = OrganizationState::bootstrap("111111111111");
        assert_eq!(org.management_account_id, "111111111111");
        assert!(org.org_id.starts_with("o-"));
        assert!(org.root_id.starts_with("r-"));
        assert_eq!(org.feature_set, FEATURE_SET_ALL);

        let full = org
            .policies
            .get(FULL_AWS_ACCESS_POLICY_ID)
            .expect("FullAWSAccess auto-seeded");
        assert!(full.aws_managed);
        assert_eq!(full.policy_type, POLICY_TYPE_SCP);

        let root_attachments = org.attachments.get(&org.root_id).expect("root attachments");
        assert!(root_attachments.contains(FULL_AWS_ACCESS_POLICY_ID));
    }

    #[test]
    fn bootstrap_enrolls_management_account_in_root() {
        let org = OrganizationState::bootstrap("222222222222");
        let mgmt = org.accounts.get("222222222222").unwrap();
        assert_eq!(mgmt.parent_id, org.root_id);
        assert_eq!(mgmt.status, "ACTIVE");
    }

    #[test]
    fn is_management_distinguishes_accounts() {
        let org = OrganizationState::bootstrap("111111111111");
        assert!(org.is_management("111111111111"));
        assert!(!org.is_management("222222222222"));
    }

    #[test]
    fn random_id_has_requested_length() {
        for len in [4, 8, 10, 16, 32] {
            let id = random_id(len);
            assert_eq!(id.len(), len);
        }
    }

    /// A v1 snapshot holds a single organization under `organization`.
    /// It must fold into the registry on load — silently producing an
    /// empty registry would drop the user's entire organization on the
    /// first restart after upgrading.
    #[test]
    fn a_v1_snapshot_folds_into_the_registry() {
        let raw = serde_json::json!({
            "schema_version": 1,
            "organization": OrganizationState::bootstrap("111111111111"),
        });
        let snapshot: OrganizationsSnapshot = serde_json::from_value(raw).unwrap();
        assert_eq!(snapshot.schema_version, 1);
        let registry = snapshot.into_registry();
        assert_eq!(registry.len(), 1);
        assert!(registry.org_of_account("111111111111").is_some());
    }

    /// A v2 snapshot round-trips every organization, and the legacy
    /// single-organization field is no longer written.
    #[test]
    fn a_v2_snapshot_round_trips_every_organization() {
        let mut registry = OrganizationsRegistry::default();
        registry.insert(OrganizationState::bootstrap("111111111111"));
        registry.insert(OrganizationState::bootstrap("222222222222"));

        let snapshot = OrganizationsSnapshot {
            schema_version: ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION,
            organization: None,
            organizations: registry,
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        assert!(
            !String::from_utf8_lossy(&bytes).contains("\"organization\":"),
            "v2 must not write the legacy single-organization field"
        );

        let loaded: OrganizationsSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(loaded.schema_version, 2);
        let restored = loaded.into_registry();
        assert_eq!(restored.len(), 2);
        assert!(restored.org_of_account("111111111111").is_some());
        assert!(restored.org_of_account("222222222222").is_some());
    }

    /// An empty registry survives a round trip as an empty registry, not
    /// as a missing field that fails to parse.
    #[test]
    fn an_empty_registry_round_trips() {
        let snapshot = OrganizationsSnapshot {
            schema_version: ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION,
            organization: None,
            organizations: OrganizationsRegistry::default(),
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let loaded: OrganizationsSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert!(loaded.into_registry().is_empty());
    }

    #[test]
    fn enroll_account_if_missing_adds_to_root() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        let member = org.accounts.get("222222222222").expect("enrolled");
        assert_eq!(member.parent_id, org.root_id);
    }

    #[test]
    fn enroll_account_if_missing_is_idempotent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("111111111111");
        assert_eq!(org.accounts.len(), 1);
    }

    #[test]
    fn create_ou_rejects_unknown_parent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org.create_ou("ou-nope", "team").unwrap_err();
        assert!(matches!(err, OrgError::ParentNotFound(_)));
    }

    #[test]
    fn create_ou_rejects_duplicate_name_under_same_parent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        org.create_ou(&root, "engineering").unwrap();
        let err = org.create_ou(&root, "engineering").unwrap_err();
        assert!(matches!(err, OrgError::DuplicateOrganizationalUnit(_)));
    }

    #[test]
    fn create_ou_allows_same_name_under_different_parents() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let parent = org.create_ou(&root, "top").unwrap();
        // Same leaf name under a different parent OU must succeed.
        org.create_ou(&parent.id, "engineering").unwrap();
        org.create_ou(&root, "engineering").unwrap();
    }

    #[test]
    fn delete_ou_rejects_non_empty_with_accounts() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "team").unwrap();
        org.enroll_account_if_missing("222222222222");
        org.move_account("222222222222", &root, &ou.id).unwrap();
        let err = org.delete_ou(&ou.id).unwrap_err();
        assert!(matches!(err, OrgError::OrganizationalUnitNotEmpty(_)));
    }

    #[test]
    fn delete_ou_rejects_non_empty_with_child_ou() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let parent = org.create_ou(&root, "parent").unwrap();
        org.create_ou(&parent.id, "child").unwrap();
        let err = org.delete_ou(&parent.id).unwrap_err();
        assert!(matches!(err, OrgError::OrganizationalUnitNotEmpty(_)));
    }

    #[test]
    fn delete_ou_clears_attachments() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "team").unwrap();
        org.attachments
            .entry(ou.id.clone())
            .or_default()
            .insert("p-custom".to_string());
        org.delete_ou(&ou.id).unwrap();
        assert!(!org.attachments.contains_key(&ou.id));
    }

    #[test]
    fn move_account_enforces_source_parent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "team").unwrap();
        org.enroll_account_if_missing("222222222222");
        let err = org.move_account("222222222222", &ou.id, &root).unwrap_err();
        assert!(matches!(err, OrgError::SourceParentNotFound(_)));
    }

    #[test]
    fn move_account_rejects_unknown_destination() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let err = org
            .move_account("111111111111", &root, "ou-nope")
            .unwrap_err();
        assert!(matches!(err, OrgError::DestinationParentNotFound(_)));
    }

    #[test]
    fn rename_ou_rejects_duplicate() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let a = org.create_ou(&root, "a").unwrap();
        let b = org.create_ou(&root, "b").unwrap();
        let err = org.rename_ou(&b.id, "a").unwrap_err();
        assert!(matches!(err, OrgError::DuplicateOrganizationalUnit(_)));
        // Renaming in place is fine.
        org.rename_ou(&a.id, "a").unwrap();
    }

    const CONTENT_ALL: &str =
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

    #[test]
    fn create_policy_assigns_id_and_arn() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let p = org
            .create_policy("AllowAll", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        assert!(p.id.starts_with("p-"));
        assert!(p.arn.contains("service_control_policy"));
        assert_eq!(p.policy_type, POLICY_TYPE_SCP);
        assert!(!p.aws_managed);
    }

    #[test]
    fn create_policy_rejects_unrecognized_type() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .create_policy("x", "d", CONTENT_ALL, "NONSENSE_POLICY")
            .unwrap_err();
        assert!(matches!(err, OrgError::PolicyTypeNotSupported(_)));
    }

    #[test]
    fn create_policy_rejects_malformed_json() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .create_policy("x", "d", "not-json", POLICY_TYPE_SCP)
            .unwrap_err();
        assert!(matches!(err, OrgError::MalformedPolicyDocument));
    }

    #[test]
    fn create_policy_duplicate_name_rejected() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.create_policy("AllowAll", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let err = org
            .create_policy("AllowAll", "other", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap_err();
        assert!(matches!(err, OrgError::DuplicatePolicy(_)));
    }

    #[test]
    fn update_policy_overrides_fields() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let p = org
            .create_policy("a", "old", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let updated = org
            .update_policy(&p.id, Some("b"), Some("new"), None)
            .unwrap();
        assert_eq!(updated.name, "b");
        assert_eq!(updated.description, "new");
        assert_eq!(updated.content, CONTENT_ALL);
    }

    #[test]
    fn update_policy_rejects_aws_managed() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .update_policy(FULL_AWS_ACCESS_POLICY_ID, Some("x"), None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::PolicyChangesNotAllowed(_)));
    }

    #[test]
    fn update_policy_rejects_malformed_content() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let p = org
            .create_policy("a", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let err = org
            .update_policy(&p.id, None, None, Some("{bad"))
            .unwrap_err();
        assert!(matches!(err, OrgError::MalformedPolicyDocument));
    }

    #[test]
    fn update_policy_duplicate_name_rejected() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let a = org
            .create_policy("a", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let b = org
            .create_policy("b", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let err = org.update_policy(&b.id, Some("a"), None, None).unwrap_err();
        assert!(matches!(err, OrgError::DuplicatePolicy(_)));
        // Rename to its own name is fine (idempotent).
        org.update_policy(&a.id, Some("a"), None, None).unwrap();
    }

    #[test]
    fn delete_policy_rejects_in_use() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        org.attach_policy(&p.id, &root).unwrap();
        let err = org.delete_policy(&p.id).unwrap_err();
        assert!(matches!(err, OrgError::PolicyInUse(_)));
    }

    #[test]
    fn delete_policy_rejects_aws_managed() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org.delete_policy(FULL_AWS_ACCESS_POLICY_ID).unwrap_err();
        assert!(matches!(err, OrgError::PolicyChangesNotAllowed(_)));
    }

    #[test]
    fn attach_detach_roundtrip() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let ou = org.create_ou(&root, "team").unwrap();
        org.attach_policy(&p.id, &ou.id).unwrap();
        let targets = org.targets_for_policy(&p.id).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, ou.id);
        assert_eq!(targets[0].2, "ORGANIZATIONAL_UNIT");
        org.detach_policy(&p.id, &ou.id).unwrap();
        assert!(org.targets_for_policy(&p.id).unwrap().is_empty());
    }

    #[test]
    fn attach_rejects_unknown_target_and_policy() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let err = org.attach_policy(&p.id, "ou-bogus").unwrap_err();
        assert!(matches!(err, OrgError::TargetNotFound(_)));
        let root = org.root_id.clone();
        let err = org.attach_policy("p-bogus", &root).unwrap_err();
        assert!(matches!(err, OrgError::PolicyNotFound(_)));
    }

    #[test]
    fn detach_unattached_policy_fails() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        let err = org.detach_policy(&p.id, &root).unwrap_err();
        assert!(matches!(err, OrgError::PolicyNotAttached(_)));
    }

    #[test]
    fn policies_for_target_returns_attached_only() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        org.attach_policy(&p.id, &root).unwrap();
        // Root starts with FullAWSAccess + new p attached.
        let list = org.policies_for_target(&root).unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn policies_for_target_unknown_target() {
        let org = OrganizationState::bootstrap("111111111111");
        let err = org.policies_for_target("ou-bogus").unwrap_err();
        assert!(matches!(err, OrgError::TargetNotFound(_)));
    }

    #[test]
    fn targets_for_policy_identifies_target_types() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "team").unwrap();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        org.enroll_account_if_missing("222222222222");
        org.attach_policy(&p.id, &root).unwrap();
        org.attach_policy(&p.id, &ou.id).unwrap();
        org.attach_policy(&p.id, "222222222222").unwrap();
        let mut types: Vec<_> = org
            .targets_for_policy(&p.id)
            .unwrap()
            .into_iter()
            .map(|(_, _, t)| t)
            .collect();
        types.sort();
        assert_eq!(types, vec!["ACCOUNT", "ORGANIZATIONAL_UNIT", "ROOT"]);
    }

    #[test]
    fn enable_aws_service_access_is_idempotent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enable_aws_service_access("config.amazonaws.com");
        let first = org.list_trusted_services()[0].1;
        org.enable_aws_service_access("config.amazonaws.com");
        let trusted = org.list_trusted_services();
        assert_eq!(trusted.len(), 1);
        assert_eq!(trusted[0].0, "config.amazonaws.com");
        // Second call must NOT overwrite the original DateEnabled.
        assert_eq!(trusted[0].1, first);
    }

    #[test]
    fn disable_aws_service_access_blocked_when_delegates_exist() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        org.enable_aws_service_access("ssm.amazonaws.com");
        org.register_delegated_administrator("222222222222", "ssm.amazonaws.com")
            .unwrap();
        let err = org
            .disable_aws_service_access("ssm.amazonaws.com")
            .unwrap_err();
        assert!(matches!(
            err,
            OrgError::DelegatedAdministratorAlreadyRegistered(_)
        ));
    }

    #[test]
    fn disable_aws_service_access_drops_when_clean() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enable_aws_service_access("ssm.amazonaws.com");
        org.disable_aws_service_access("ssm.amazonaws.com").unwrap();
        assert!(org.list_trusted_services().is_empty());
    }

    #[test]
    fn register_delegated_administrator_requires_trusted_service() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        let err = org
            .register_delegated_administrator("222222222222", "ssm.amazonaws.com")
            .unwrap_err();
        assert!(matches!(err, OrgError::AWSServiceAccessNotEnabled(_)));
    }

    #[test]
    fn list_delegated_services_for_account_returns_principals() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        org.enable_aws_service_access("ssm.amazonaws.com");
        org.enable_aws_service_access("config.amazonaws.com");
        org.register_delegated_administrator("222222222222", "ssm.amazonaws.com")
            .unwrap();
        org.register_delegated_administrator("222222222222", "config.amazonaws.com")
            .unwrap();
        let names: Vec<String> = org
            .list_delegated_services_for_account("222222222222")
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec!["config.amazonaws.com", "ssm.amazonaws.com"]);
    }

    #[test]
    fn deregister_delegated_administrator_removes_entry() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enroll_account_if_missing("222222222222");
        org.enable_aws_service_access("ssm.amazonaws.com");
        org.register_delegated_administrator("222222222222", "ssm.amazonaws.com")
            .unwrap();
        org.deregister_delegated_administrator("222222222222", "ssm.amazonaws.com")
            .unwrap();
        assert!(org
            .list_delegated_services_for_account("222222222222")
            .is_empty());
    }

    #[test]
    fn enable_all_features_promotes_feature_set() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.feature_set = FEATURE_SET_CONSOLIDATED_BILLING.to_string();
        org.enable_all_features();
        assert_eq!(org.feature_set, FEATURE_SET_ALL);
    }

    #[test]
    fn enable_policy_type_idempotent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enable_policy_type("TAG_POLICY");
        org.enable_policy_type("TAG_POLICY");
        let statuses = org.list_policy_type_statuses();
        let tag = statuses.iter().find(|(t, _)| t == "TAG_POLICY").unwrap();
        assert_eq!(tag.1, "ENABLED");
    }

    #[test]
    fn disable_policy_type_drops_to_disabled() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.enable_policy_type("TAG_POLICY");
        org.disable_policy_type("TAG_POLICY").unwrap();
        let statuses = org.list_policy_type_statuses();
        let tag = statuses.iter().find(|(t, _)| t == "TAG_POLICY").unwrap();
        assert_eq!(tag.1, "DISABLED");
    }

    #[test]
    fn disable_policy_type_refuses_scp() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org.disable_policy_type(POLICY_TYPE_SCP).unwrap_err();
        assert!(matches!(err, OrgError::PolicyTypeNotSupported(_)));
    }

    #[test]
    fn create_policy_accepts_tag_policy_type() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let p = org
            .create_policy("MyTags", "tag rules", CONTENT_ALL, "TAG_POLICY")
            .unwrap();
        assert_eq!(p.policy_type, "TAG_POLICY");
        assert!(p.arn.contains("/tag_policy/"));
    }

    #[test]
    fn create_policy_accepts_backup_resource_aiopt_out() {
        let mut org = OrganizationState::bootstrap("111111111111");
        for kind in [
            "BACKUP_POLICY",
            "AISERVICES_OPT_OUT_POLICY",
            "RESOURCE_CONTROL_POLICY",
        ] {
            let p = org
                .create_policy(&format!("p-{kind}"), "d", CONTENT_ALL, kind)
                .unwrap();
            assert_eq!(p.policy_type, kind);
        }
    }

    #[test]
    fn create_policy_rejects_unknown_type() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .create_policy("p", "d", CONTENT_ALL, "UNKNOWN_POLICY")
            .unwrap_err();
        assert!(matches!(err, OrgError::PolicyTypeNotSupported(_)));
    }

    #[test]
    fn list_policy_type_statuses_includes_all_known_types() {
        let org = OrganizationState::bootstrap("111111111111");
        let statuses = org.list_policy_type_statuses();
        let types: Vec<_> = statuses.iter().map(|(t, _)| t.as_str()).collect();
        assert!(types.contains(&"SERVICE_CONTROL_POLICY"));
        assert!(types.contains(&"TAG_POLICY"));
        assert!(types.contains(&"BACKUP_POLICY"));
        assert!(types.contains(&"AISERVICES_OPT_OUT_POLICY"));
        assert!(types.contains(&"RESOURCE_CONTROL_POLICY"));
    }

    #[test]
    fn set_resource_tags_overwrites_existing_keys() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.set_resource_tags(
            "111111111111",
            &[("env".into(), "dev".into()), ("team".into(), "core".into())],
        );
        org.set_resource_tags("111111111111", &[("env".into(), "prod".into())]);
        let tags = org.list_resource_tags("111111111111");
        let env = tags.iter().find(|(k, _)| k == "env").unwrap();
        assert_eq!(env.1, "prod");
    }

    #[test]
    fn untag_resource_drops_only_listed_keys() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.set_resource_tags(
            "111111111111",
            &[("env".into(), "dev".into()), ("team".into(), "core".into())],
        );
        org.untag_resource("111111111111", &["env".into()]);
        let keys: Vec<_> = org
            .list_resource_tags("111111111111")
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["team"]);
    }

    #[test]
    fn parent_of_account_returns_root() {
        let org = OrganizationState::bootstrap("111111111111");
        let (parent, kind) = org.parent_of("111111111111").unwrap();
        assert_eq!(parent, org.root_id);
        assert_eq!(kind, "ROOT");
    }

    #[test]
    fn list_children_separates_accounts_from_ous() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "Engineering").unwrap();
        org.enroll_account_if_missing("222222222222");
        let accounts = org.list_children(&org.root_id, "ACCOUNT");
        assert!(accounts.contains(&"222222222222".to_string()));
        let ous = org.list_children(&org.root_id, "ORGANIZATIONAL_UNIT");
        assert!(ous.contains(&ou.id));
    }

    #[test]
    fn duplicate_policy_check_scoped_per_type() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.create_policy("Same", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        // Same name allowed in a different policy type.
        org.create_policy("Same", "d", CONTENT_ALL, "TAG_POLICY")
            .unwrap();
        // Same name + same type is rejected.
        let err = org
            .create_policy("Same", "d", CONTENT_ALL, "TAG_POLICY")
            .unwrap_err();
        assert!(matches!(err, OrgError::DuplicatePolicy(_)));
    }

    #[test]
    fn attach_is_idempotent() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let root = org.root_id.clone();
        let p = org
            .create_policy("p", "d", CONTENT_ALL, POLICY_TYPE_SCP)
            .unwrap();
        org.attach_policy(&p.id, &root).unwrap();
        org.attach_policy(&p.id, &root).unwrap();
        let targets = org.targets_for_policy(&p.id).unwrap();
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn invite_account_creates_open_handshake() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "ACCOUNT", "222222222222", None, None)
            .unwrap();
        assert_eq!(h.state, "OPEN");
        assert!(h.id.starts_with("h-"));
        assert!(org.handshakes.contains_key(&h.id));
    }

    #[test]
    fn invite_rejects_existing_member() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .invite_account("111111111111", "ACCOUNT", "111111111111", None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::AccountAlreadyMember(_)));
    }

    #[test]
    fn duplicate_open_invite_rejected() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.invite_account("111111111111", "ACCOUNT", "333333333333", None, None)
            .unwrap();
        let err = org
            .invite_account("111111111111", "ACCOUNT", "333333333333", None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::DuplicateHandshakeForAccount(_)));
    }

    #[test]
    fn accept_handshake_enrolls_account() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "ACCOUNT", "444444444444", None, None)
            .unwrap();
        assert!(!org.accounts.contains_key("444444444444"));
        let resolved = org
            .resolve_handshake(&h.id, "ACCEPTED", Some("444444444444"), None)
            .unwrap();
        assert_eq!(resolved.state, "ACCEPTED");
        let acct = org.accounts.get("444444444444").unwrap();
        assert_eq!(acct.joined_method, "INVITED");
    }

    #[test]
    fn decline_handshake_does_not_enroll() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "ACCOUNT", "555555555555", None, None)
            .unwrap();
        let resolved = org
            .resolve_handshake(&h.id, "DECLINED", None, None)
            .unwrap();
        assert_eq!(resolved.state, "DECLINED");
        assert!(!org.accounts.contains_key("555555555555"));
    }

    #[test]
    fn resolve_handshake_terminal_locked() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "ACCOUNT", "666666666666", None, None)
            .unwrap();
        org.resolve_handshake(&h.id, "ACCEPTED", Some("666666666666"), None)
            .unwrap();
        let err = org
            .resolve_handshake(&h.id, "DECLINED", None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::HandshakeAlreadyResolved(_)));
    }
}
