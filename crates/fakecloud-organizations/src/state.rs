use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Shared, cross-account singleton. `None` until `CreateOrganization`
/// runs; at most one organization exists per fakecloud process. An AWS
/// org is not per-account state (it spans accounts), so this is NOT
/// wrapped in `MultiAccountState`.
pub type SharedOrganizationsState = Arc<RwLock<Option<OrganizationState>>>;

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
/// changes fail loudly on upgrade rather than silently mis-parsing. The whole
/// org is a single optional value (`None` until `CreateOrganization`).
#[derive(Clone, Serialize, Deserialize)]
pub struct OrganizationsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub organization: Option<OrganizationState>,
}

pub const ORGANIZATIONS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

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
        let root_id = format!("r-{}", random_id(4));
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

    /// Allocate the next pseudo-random 12-digit account id that's not
    /// already a member. Mirrors AWS's account-id format (numeric,
    /// 12 digits, no leading zero stripping).
    pub fn next_account_id(&self) -> String {
        loop {
            let mut id = String::with_capacity(12);
            for _ in 0..12 {
                let u = Uuid::new_v4();
                let byte = u.as_bytes()[0];
                id.push(((byte % 10) + b'0') as char);
            }
            if !id.starts_with('0') && !self.accounts.contains_key(&id) {
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
    pub fn begin_create_account(
        &mut self,
        email: &str,
        name: &str,
        gov_cloud_paired_id: Option<String>,
    ) -> CreateAccountStatus {
        let now = Utc::now();
        let request_id = format!("car-{}", random_id(20));
        let new_account_id = self.next_account_id();
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
        Some(status.clone())
    }

    /// Issue a new pending invitation handshake to `target_account_id`.
    /// Idempotent: a duplicate live handshake to the same account
    /// returns `DuplicateHandshakeForAccount`.
    pub fn invite_account(
        &mut self,
        source_account_id: &str,
        target_account_id: &str,
        target_email: Option<String>,
        notes: Option<String>,
    ) -> Result<Handshake, OrgError> {
        if self.accounts.contains_key(target_account_id) {
            return Err(OrgError::AccountAlreadyMember(
                target_account_id.to_string(),
            ));
        }
        for h in self.handshakes.values() {
            if h.target_account_id == target_account_id
                && matches!(h.state.as_str(), "REQUESTED" | "OPEN")
            {
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
        let kind = if target_account_id.chars().all(|c| c.is_ascii_digit()) {
            "ACCOUNT".to_string()
        } else {
            "EMAIL".to_string()
        };
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
        };
        self.handshakes.insert(id, handshake.clone());
        Ok(handshake)
    }

    /// Move a live handshake from `OPEN` into `new_state`. Caller decides
    /// whether the transition is allowed for the current API caller —
    /// this just enforces lifecycle (open -> terminal). The original
    /// `ExpirationTimestamp` is preserved (it's the 15-day deadline,
    /// not a resolved-at marker).
    pub fn resolve_handshake(&mut self, id: &str, new_state: &str) -> Result<Handshake, OrgError> {
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
        if new_state == "ACCEPTED" && !self.accounts.contains_key(&snapshot.target_account_id) {
            let now = Utc::now();
            let arn = format!(
                "arn:aws:organizations::{}:account/{}/{}",
                self.management_account_id, self.org_id, snapshot.target_account_id
            );
            let email = snapshot
                .target_email
                .clone()
                .unwrap_or_else(|| format!("{}@example.com", snapshot.target_account_id));
            self.accounts.insert(
                snapshot.target_account_id.clone(),
                MemberAccount {
                    id: snapshot.target_account_id.clone(),
                    arn,
                    email,
                    name: format!("Account {}", snapshot.target_account_id),
                    status: "ACTIVE".to_string(),
                    joined_method: "INVITED".to_string(),
                    joined_timestamp: now,
                    parent_id: self.root_id.clone(),
                },
            );
        }
        Ok(snapshot)
    }

    /// Return a clone of every handshake currently tracked for the org,
    /// optionally filtered by destination account id.
    pub fn list_handshakes(&self, only_target_account: Option<&str>) -> Vec<Handshake> {
        self.handshakes
            .values()
            .filter(|h| match only_target_account {
                Some(acct) => h.target_account_id == acct,
                None => true,
            })
            .cloned()
            .collect()
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
        let entry = self
            .delegated_administrators
            .get_mut(service_principal)
            .ok_or_else(|| {
                OrgError::DelegatedAdministratorNotRegistered(service_principal.to_string())
            })?;
        if entry.remove(account_id).is_none() {
            return Err(OrgError::DelegatedAdministratorNotRegistered(
                account_id.to_string(),
            ));
        }
        Ok(())
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
        Ok(())
    }

    /// Returns `true` iff `account_id` is the management account.
    pub fn is_management(&self, account_id: &str) -> bool {
        account_id == self.management_account_id
    }

    /// Enroll `account_id` into the root OU as a member of the
    /// organization if not already known. No-op when the account is
    /// already enrolled anywhere in the tree. Used as the
    /// auto-enrollment hook when a new IAM admin bootstraps via
    /// `/_fakecloud/iam/create-admin` while an organization exists.
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
            .invite_account("111111111111", "222222222222", None, None)
            .unwrap();
        assert_eq!(h.state, "OPEN");
        assert!(h.id.starts_with("h-"));
        assert!(org.handshakes.contains_key(&h.id));
    }

    #[test]
    fn invite_rejects_existing_member() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let err = org
            .invite_account("111111111111", "111111111111", None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::AccountAlreadyMember(_)));
    }

    #[test]
    fn duplicate_open_invite_rejected() {
        let mut org = OrganizationState::bootstrap("111111111111");
        org.invite_account("111111111111", "333333333333", None, None)
            .unwrap();
        let err = org
            .invite_account("111111111111", "333333333333", None, None)
            .unwrap_err();
        assert!(matches!(err, OrgError::DuplicateHandshakeForAccount(_)));
    }

    #[test]
    fn accept_handshake_enrolls_account() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "444444444444", None, None)
            .unwrap();
        assert!(!org.accounts.contains_key("444444444444"));
        let resolved = org.resolve_handshake(&h.id, "ACCEPTED").unwrap();
        assert_eq!(resolved.state, "ACCEPTED");
        let acct = org.accounts.get("444444444444").unwrap();
        assert_eq!(acct.joined_method, "INVITED");
    }

    #[test]
    fn decline_handshake_does_not_enroll() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "555555555555", None, None)
            .unwrap();
        let resolved = org.resolve_handshake(&h.id, "DECLINED").unwrap();
        assert_eq!(resolved.state, "DECLINED");
        assert!(!org.accounts.contains_key("555555555555"));
    }

    #[test]
    fn resolve_handshake_terminal_locked() {
        let mut org = OrganizationState::bootstrap("111111111111");
        let h = org
            .invite_account("111111111111", "666666666666", None, None)
            .unwrap();
        org.resolve_handshake(&h.id, "ACCEPTED").unwrap();
        let err = org.resolve_handshake(&h.id, "DECLINED").unwrap_err();
        assert!(matches!(err, OrgError::HandshakeAlreadyResolved(_)));
    }
}
