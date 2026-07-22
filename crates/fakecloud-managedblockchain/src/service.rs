//! Amazon Managed Blockchain (`managedblockchain`) restJson1 dispatch + handlers.
//!
//! The full 27-operation Managed Blockchain control plane. Requests are routed
//! to an operation by HTTP method + `@http` URI path under `/`; path labels are
//! captured positionally (percent-decoded, so an ARN label whose slashes/colons
//! arrive percent-encoded survives intact) and query parameters are read from
//! the raw query string. State is account-partitioned and persisted; each
//! resource is stored as its already-output-valid wire object so `Get*` echoes
//! exactly what `Create*` persisted.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::shared;
use crate::state::{ManagedBlockchainData, SharedManagedBlockchainState};

/// Every operation name in the Amazon Managed Blockchain Smithy model (27).
pub const MANAGEDBLOCKCHAIN_ACTIONS: &[&str] = &[
    "CreateAccessor",
    "CreateMember",
    "CreateNetwork",
    "CreateNode",
    "CreateProposal",
    "DeleteAccessor",
    "DeleteMember",
    "DeleteNode",
    "GetAccessor",
    "GetMember",
    "GetNetwork",
    "GetNode",
    "GetProposal",
    "ListAccessors",
    "ListInvitations",
    "ListMembers",
    "ListNetworks",
    "ListNodes",
    "ListProposalVotes",
    "ListProposals",
    "ListTagsForResource",
    "RejectInvitation",
    "TagResource",
    "UntagResource",
    "UpdateMember",
    "UpdateNode",
    "VoteOnProposal",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateAccessor",
    "CreateMember",
    "CreateNetwork",
    "CreateNode",
    "CreateProposal",
    "DeleteAccessor",
    "DeleteMember",
    "DeleteNode",
    "RejectInvitation",
    "TagResource",
    "UntagResource",
    "UpdateMember",
    "UpdateNode",
    "VoteOnProposal",
];

pub struct ManagedBlockchainService {
    state: SharedManagedBlockchainState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ManagedBlockchainService {
    pub fn new(state: SharedManagedBlockchainState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Settle any in-flight resource lifecycle transition for the account:
    /// members and nodes created `CREATING` settle to `AVAILABLE`, and any
    /// `IN_PROGRESS` proposal whose `ExpirationDate` has passed settles to
    /// `EXPIRED`. Returns `true` if a transition fired (so the caller persists).
    fn reconcile(&self, account: &str) -> bool {
        let now = shared::iso_now();
        let mut guard = self.state.write();
        let data = guard.get_or_create(account);
        let mut changed = false;
        for m in data.members.values_mut() {
            if promote_creating(m) {
                changed = true;
            }
        }
        for n in data.nodes.values_mut() {
            if promote_creating(n) {
                changed = true;
            }
        }
        for p in data.proposals.values_mut() {
            if let Some(obj) = p.as_object_mut() {
                let status = obj.get("Status").and_then(Value::as_str).unwrap_or("");
                let expired = obj
                    .get("ExpirationDate")
                    .and_then(Value::as_str)
                    .map(|e| e < now.as_str())
                    .unwrap_or(false);
                if status == "IN_PROGRESS" && expired {
                    obj.insert("Status".into(), json!("EXPIRED"));
                    changed = true;
                }
            }
        }
        changed
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let del = m == Method::DELETE;
        let patch = m == Method::PATCH;
        let l = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["networks"] if post => ("CreateNetwork", vec![]),
            ["networks"] if get => ("ListNetworks", vec![]),
            ["networks", nid] if get => ("GetNetwork", l(&[nid])),
            ["networks", nid, "members"] if post => ("CreateMember", l(&[nid])),
            ["networks", nid, "members"] if get => ("ListMembers", l(&[nid])),
            ["networks", nid, "members", mid] if get => ("GetMember", l(&[nid, mid])),
            ["networks", nid, "members", mid] if patch => ("UpdateMember", l(&[nid, mid])),
            ["networks", nid, "members", mid] if del => ("DeleteMember", l(&[nid, mid])),
            ["networks", nid, "nodes"] if post => ("CreateNode", l(&[nid])),
            ["networks", nid, "nodes"] if get => ("ListNodes", l(&[nid])),
            ["networks", nid, "nodes", ndid] if get => ("GetNode", l(&[nid, ndid])),
            ["networks", nid, "nodes", ndid] if patch => ("UpdateNode", l(&[nid, ndid])),
            ["networks", nid, "nodes", ndid] if del => ("DeleteNode", l(&[nid, ndid])),
            ["networks", nid, "proposals"] if post => ("CreateProposal", l(&[nid])),
            ["networks", nid, "proposals"] if get => ("ListProposals", l(&[nid])),
            ["networks", nid, "proposals", pid] if get => ("GetProposal", l(&[nid, pid])),
            ["networks", nid, "proposals", pid, "votes"] if get => {
                ("ListProposalVotes", l(&[nid, pid]))
            }
            ["networks", nid, "proposals", pid, "votes"] if post => {
                ("VoteOnProposal", l(&[nid, pid]))
            }
            ["accessors"] if post => ("CreateAccessor", vec![]),
            ["accessors"] if get => ("ListAccessors", vec![]),
            ["accessors", aid] if get => ("GetAccessor", l(&[aid])),
            ["accessors", aid] if del => ("DeleteAccessor", l(&[aid])),
            ["invitations"] if get => ("ListInvitations", vec![]),
            ["invitations", iid] if del => ("RejectInvitation", l(&[iid])),
            ["tags", arn] if post => ("TagResource", l(&[arn])),
            ["tags", arn] if get => ("ListTagsForResource", l(&[arn])),
            ["tags", arn] if del => ("UntagResource", l(&[arn])),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for ManagedBlockchainService {
    fn service_name(&self) -> &str {
        "managedblockchain"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let (result, settled) = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if settled || (MUTATING.contains(&action) && success) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        MANAGEDBLOCKCHAIN_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl ManagedBlockchainService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> (Result<AwsResponse, AwsServiceError>, bool) {
        let body = match parse_body(req) {
            Ok(b) => b,
            Err(e) => return (Err(e), false),
        };
        if let Err(e) = crate::validate::validate_input(action, &body) {
            return (Err(e), false);
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        let settled = self.reconcile(&ctx.account);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        let result = match action {
            "CreateNetwork" => self.create_network(&ctx, &body),
            "GetNetwork" => self.get_network(&ctx, a(0)),
            "ListNetworks" => self.list_networks(&ctx, &q),
            "CreateMember" => self.create_member(&ctx, a(0), &body),
            "GetMember" => self.get_member(&ctx, a(0), a(1)),
            "UpdateMember" => self.update_member(&ctx, a(0), a(1), &body),
            "DeleteMember" => self.delete_member(&ctx, a(0), a(1)),
            "ListMembers" => self.list_members(&ctx, a(0), &q),
            "CreateNode" => self.create_node(&ctx, a(0), &body),
            "GetNode" => self.get_node(&ctx, a(0), a(1)),
            "UpdateNode" => self.update_node(&ctx, a(0), a(1), &body),
            "DeleteNode" => self.delete_node(&ctx, a(0), a(1)),
            "ListNodes" => self.list_nodes(&ctx, a(0), &q),
            "CreateProposal" => self.create_proposal(&ctx, a(0), &body),
            "GetProposal" => self.get_proposal(&ctx, a(0), a(1)),
            "ListProposals" => self.list_proposals(&ctx, a(0), &q),
            "ListProposalVotes" => self.list_proposal_votes(&ctx, a(0), a(1), &q),
            "VoteOnProposal" => self.vote_on_proposal(&ctx, a(0), a(1), &body),
            "CreateAccessor" => self.create_accessor(&ctx, &body),
            "GetAccessor" => self.get_accessor(&ctx, a(0)),
            "DeleteAccessor" => self.delete_accessor(&ctx, a(0)),
            "ListAccessors" => self.list_accessors(&ctx, &q),
            "ListInvitations" => self.list_invitations(&ctx, &q),
            "RejectInvitation" => self.reject_invitation(&ctx, a(0)),
            "TagResource" => self.tag_resource(&ctx, a(0), &body),
            "UntagResource" => self.untag_resource(&ctx, a(0), &q),
            "ListTagsForResource" => self.list_tags(&ctx, a(0)),
            _ => Err(AwsServiceError::action_not_implemented(
                "managedblockchain",
                action,
            )),
        };
        (result, settled)
    }

    // ------------------------------ Networks ----------------------------

    fn create_network(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let now = shared::iso_now();
        let network_id = shared::new_network_id();
        let framework = str_or(body, "Framework", "HYPERLEDGER_FABRIC");
        let member_id = shared::new_member_id();

        let mut net = Map::new();
        net.insert("Id".into(), json!(network_id));
        net.insert("Name".into(), json!(str_or(body, "Name", "")));
        net.insert("Framework".into(), json!(framework));
        net.insert(
            "FrameworkVersion".into(),
            json!(str_or(body, "FrameworkVersion", "")),
        );
        net.insert("Status".into(), json!("AVAILABLE"));
        net.insert("CreationDate".into(), json!(now));
        net.insert("Arn".into(), json!(shared::network_arn(&network_id)));
        net.insert(
            "VpcEndpointServiceName".into(),
            json!(format!(
                "com.amazonaws.{}.managedblockchain.{}",
                ctx.region,
                network_id.to_lowercase()
            )),
        );
        echo(&mut net, body, &["Description", "VotingPolicy"]);
        net.insert(
            "FrameworkAttributes".into(),
            network_framework_attributes(&framework, &network_id, body, &ctx.region),
        );

        let is_fabric = framework == "HYPERLEDGER_FABRIC";

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_tags(data, &shared::network_arn(&network_id), body);
        data.networks.insert(network_id.clone(), Value::Object(net));

        // Hyperledger Fabric networks atomically create the requested first
        // member; Ethereum networks do not.
        let mut out = json!({ "NetworkId": network_id });
        if is_fabric {
            let member_config = body
                .get("MemberConfiguration")
                .cloned()
                .unwrap_or(json!({}));
            let member = build_member(ctx, &network_id, &member_id, &member_config, &now, true);
            let arn = shared::member_arn(&ctx.region, &ctx.account, &member_id);
            store_tags(data, &arn, &member_config);
            data.members.insert(member_id.clone(), member);
            out.as_object_mut()
                .unwrap()
                .insert("MemberId".into(), json!(member_id));
        }
        ok(out)
    }

    fn get_network(&self, ctx: &Ctx, network_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.networks.get(network_id)) {
            Some(n) => {
                let arn = shared::network_arn(network_id);
                ok(json!({ "Network": with_tags(data.unwrap(), &arn, n) }))
            }
            None => Err(not_found(&format!("Network {network_id} was not found."))),
        }
    }

    fn list_networks(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_list_query(
            q,
            10,
            &[
                ("framework", crate::validate::FRAMEWORK),
                ("status", NETWORK_STATUS),
            ],
        )?;
        let name = query_one(q, "name");
        let framework = query_one(q, "framework");
        let status = query_one(q, "status");
        let guard = self.state.read();
        let mut networks: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.networks
                    .values()
                    .filter(|n| {
                        opt_eq(n, "Name", name)
                            && opt_eq(n, "Framework", framework)
                            && opt_eq(n, "Status", status)
                    })
                    .map(network_summary)
                    .collect()
            })
            .unwrap_or_default();
        networks.sort_by(|a, b| summary_id(a).cmp(summary_id(b)));
        let (page, next) = paginate(networks, q)?;
        let mut out = json!({ "Networks": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------ Members -----------------------------

    fn create_member(
        &self,
        ctx: &Ctx,
        network_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        let invitation_id = str_or(body, "InvitationId", "");
        let now = shared::iso_now();
        let member_id = shared::new_member_id();
        let member_config = body
            .get("MemberConfiguration")
            .cloned()
            .unwrap_or(json!({}));

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.networks.contains_key(network_id) {
            return Err(not_found(&format!("Network {network_id} was not found.")));
        }
        // The invitation must exist AND still be PENDING; joining consumes it by
        // flipping it to ACCEPTED so the same invitation cannot mint a second
        // member. A REJECTED / EXPIRED / already-ACCEPTED invitation is rejected.
        match data
            .invitations
            .get(&invitation_id)
            .and_then(|inv| inv.get("Status"))
            .and_then(Value::as_str)
        {
            Some("PENDING") => {
                if let Some(obj) = data
                    .invitations
                    .get_mut(&invitation_id)
                    .and_then(Value::as_object_mut)
                {
                    obj.insert("Status".into(), json!("ACCEPTED"));
                }
            }
            _ => {
                return Err(invalid_request(&format!(
                    "Invitation {invitation_id} was not found or is not pending."
                )));
            }
        }
        let member = build_member(ctx, network_id, &member_id, &member_config, &now, true);
        let arn = shared::member_arn(&ctx.region, &ctx.account, &member_id);
        store_tags(data, &arn, &member_config);
        data.members.insert(member_id.clone(), member);
        ok(json!({ "MemberId": member_id }))
    }

    fn get_member(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        member_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.members.get(member_id)) {
            Some(m) => {
                let arn = shared::member_arn(&ctx.region, &ctx.account, member_id);
                ok(json!({ "Member": with_tags(data.unwrap(), &arn, m) }))
            }
            None => Err(not_found(&format!("Member {member_id} was not found."))),
        }
    }

    fn update_member(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        member_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(m) = data
            .members
            .get_mut(member_id)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Member {member_id} was not found.")));
        };
        if let Some(lpc) = body.get("LogPublishingConfiguration") {
            m.insert("LogPublishingConfiguration".into(), lpc.clone());
        }
        ok_empty()
    }

    fn delete_member(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        member_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(m) = data
            .members
            .get_mut(member_id)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Member {member_id} was not found.")));
        };
        m.insert("Status".into(), json!("DELETED"));
        ok_empty()
    }

    fn list_members(
        &self,
        ctx: &Ctx,
        network_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        validate_list_query(q, 20, &[("status", MEMBER_STATUS)])?;
        let name = query_one(q, "name");
        let status = query_one(q, "status");
        let guard = self.state.read();
        let mut members: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.members
                    .values()
                    .filter(|m| {
                        opt_eq(m, "NetworkId", Some(network_id))
                            && opt_eq(m, "Name", name)
                            && opt_eq(m, "Status", status)
                    })
                    .map(|m| member_summary(m, &ctx.account, &ctx.region))
                    .collect()
            })
            .unwrap_or_default();
        members.sort_by(|a, b| summary_id(a).cmp(summary_id(b)));
        let (page, next) = paginate(members, q)?;
        let mut out = json!({ "Members": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------- Nodes ------------------------------

    fn create_node(
        &self,
        ctx: &Ctx,
        network_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        let now = shared::iso_now();
        let node_id = shared::new_node_id();
        let member_id = body
            .get("MemberId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let node_config = body.get("NodeConfiguration").cloned().unwrap_or(json!({}));

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.networks.contains_key(network_id) {
            return Err(not_found(&format!("Network {network_id} was not found.")));
        }
        let framework = data
            .networks
            .get(network_id)
            .and_then(|n| n.get("Framework"))
            .and_then(Value::as_str)
            .unwrap_or("HYPERLEDGER_FABRIC")
            .to_string();
        let node = build_node(
            ctx,
            network_id,
            member_id.as_deref(),
            &node_id,
            &node_config,
            &framework,
            &now,
        );
        let arn = shared::node_arn(&ctx.region, &ctx.account, &node_id);
        store_tags(data, &arn, body);
        data.nodes.insert(node_id.clone(), node);
        ok(json!({ "NodeId": node_id }))
    }

    fn get_node(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        node_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.nodes.get(node_id)) {
            Some(n) => {
                let arn = shared::node_arn(&ctx.region, &ctx.account, node_id);
                ok(json!({ "Node": with_tags(data.unwrap(), &arn, n) }))
            }
            None => Err(not_found(&format!("Node {node_id} was not found."))),
        }
    }

    fn update_node(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        node_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(n) = data.nodes.get_mut(node_id).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("Node {node_id} was not found.")));
        };
        if let Some(lpc) = body.get("LogPublishingConfiguration") {
            n.insert("LogPublishingConfiguration".into(), lpc.clone());
        }
        ok_empty()
    }

    fn delete_node(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        node_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(n) = data.nodes.get_mut(node_id).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("Node {node_id} was not found.")));
        };
        n.insert("Status".into(), json!("DELETED"));
        ok_empty()
    }

    fn list_nodes(
        &self,
        ctx: &Ctx,
        network_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        validate_list_query(q, 20, &[("status", NODE_STATUS)])?;
        // `memberId` is a `ResourceIdString` (1..=32); validate it even when
        // present-but-empty (a too-short negative variant), which `query_one`
        // would otherwise drop.
        if let Some((_, mid)) = q.iter().find(|(k, _)| k == "memberId") {
            check_id(mid, "MemberId")?;
        }
        let member_id = query_one(q, "memberId");
        let status = query_one(q, "status");
        let guard = self.state.read();
        let mut nodes: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.nodes
                    .values()
                    .filter(|n| {
                        opt_eq(n, "NetworkId", Some(network_id))
                            && opt_eq(n, "MemberId", member_id)
                            && opt_eq(n, "Status", status)
                    })
                    .map(node_summary)
                    .collect()
            })
            .unwrap_or_default();
        nodes.sort_by(|a, b| summary_id(a).cmp(summary_id(b)));
        let (page, next) = paginate(nodes, q)?;
        let mut out = json!({ "Nodes": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    // ----------------------------- Proposals ----------------------------

    fn create_proposal(
        &self,
        ctx: &Ctx,
        network_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let now = shared::iso_now();
        let proposal_id = shared::new_proposal_id();
        let member_id = str_or(body, "MemberId", "");
        let actions = body.get("Actions").cloned().unwrap_or(json!({}));

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.networks.contains_key(network_id) {
            return Err(not_found(&format!("Network {network_id} was not found.")));
        }
        let proposed_by_name = data
            .members
            .get(&member_id)
            .and_then(|m| m.get("Name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let duration = proposal_duration_hours(data.networks.get(network_id));
        let outstanding = eligible_member_count(data, network_id);
        let expiration = (chrono::Utc::now() + chrono::Duration::hours(duration))
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();

        let mut p = Map::new();
        p.insert("ProposalId".into(), json!(proposal_id));
        p.insert("NetworkId".into(), json!(network_id));
        p.insert("Actions".into(), actions);
        p.insert("ProposedByMemberId".into(), json!(member_id));
        p.insert("ProposedByMemberName".into(), json!(proposed_by_name));
        p.insert("Status".into(), json!("IN_PROGRESS"));
        p.insert("CreationDate".into(), json!(now));
        p.insert("ExpirationDate".into(), json!(expiration));
        p.insert("YesVoteCount".into(), json!(0));
        p.insert("NoVoteCount".into(), json!(0));
        p.insert("OutstandingVoteCount".into(), json!(outstanding));
        p.insert(
            "Arn".into(),
            json!(shared::proposal_arn(
                &ctx.region,
                &ctx.account,
                &proposal_id
            )),
        );
        if let Some(d) = body.get("Description") {
            p.insert("Description".into(), d.clone());
        }
        store_tags(
            data,
            &shared::proposal_arn(&ctx.region, &ctx.account, &proposal_id),
            body,
        );
        data.proposals.insert(proposal_id.clone(), Value::Object(p));
        data.votes.insert(proposal_id.clone(), Vec::new());
        ok(json!({ "ProposalId": proposal_id }))
    }

    fn get_proposal(
        &self,
        ctx: &Ctx,
        _network_id: &str,
        proposal_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.proposals.get(proposal_id)) {
            Some(p) => {
                let arn = shared::proposal_arn(&ctx.region, &ctx.account, proposal_id);
                ok(json!({ "Proposal": with_tags(data.unwrap(), &arn, p) }))
            }
            None => Err(not_found(&format!("Proposal {proposal_id} was not found."))),
        }
    }

    fn list_proposals(
        &self,
        ctx: &Ctx,
        network_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        validate_list_query(q, 100, &[])?;
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        if let Some(d) = data {
            if !d.networks.contains_key(network_id) {
                return Err(not_found(&format!("Network {network_id} was not found.")));
            }
        }
        let mut proposals: Vec<Value> = data
            .map(|d| {
                d.proposals
                    .values()
                    .filter(|p| opt_eq(p, "NetworkId", Some(network_id)))
                    .map(proposal_summary)
                    .collect()
            })
            .unwrap_or_default();
        proposals.sort_by(|a, b| {
            a.get("ProposalId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .cmp(b.get("ProposalId").and_then(Value::as_str).unwrap_or(""))
        });
        let (page, next) = paginate(proposals, q)?;
        let mut out = json!({ "Proposals": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    fn list_proposal_votes(
        &self,
        ctx: &Ctx,
        network_id: &str,
        proposal_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_id(network_id, "NetworkId")?;
        check_id(proposal_id, "ProposalId")?;
        validate_list_query(q, 100, &[])?;
        let guard = self.state.read();
        let votes: Vec<Value> = guard
            .get(&ctx.account)
            .and_then(|d| d.votes.get(proposal_id))
            .cloned()
            .unwrap_or_default();
        let (page, next) = paginate(votes, q)?;
        let mut out = json!({ "ProposalVotes": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    fn vote_on_proposal(
        &self,
        ctx: &Ctx,
        network_id: &str,
        proposal_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let voter = str_or(body, "VoterMemberId", "");
        let vote = str_or(body, "Vote", "");

        let mut guard = self.state.write();
        // Collect the invitations to materialise across accounts once the
        // proposal is decided; execute them after the caller's borrow ends.
        let mut invitations_to_send: Vec<(String, Value)> = Vec::new();
        {
            let region = ctx.region.clone();
            let data = guard.get_or_create(&ctx.account);
            let network = data.networks.get(network_id).cloned();
            if network.is_none() {
                return Err(not_found(&format!("Network {network_id} was not found.")));
            }
            if !data.members.contains_key(&voter) {
                return Err(not_found(&format!("Member {voter} was not found.")));
            }
            let member_name = data
                .members
                .get(&voter)
                .and_then(|m| m.get("Name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();

            {
                let proposal = data
                    .proposals
                    .get(proposal_id)
                    .ok_or_else(|| not_found(&format!("Proposal {proposal_id} was not found.")))?;
                let status = proposal.get("Status").and_then(Value::as_str).unwrap_or("");
                if status != "IN_PROGRESS" {
                    return Err(illegal_action(&format!(
                        "Proposal {proposal_id} is not open for voting (status {status})."
                    )));
                }
            }

            let votes = data.votes.entry(proposal_id.to_string()).or_default();
            if votes
                .iter()
                .any(|v| v.get("MemberId").and_then(Value::as_str) == Some(voter.as_str()))
            {
                return Err(illegal_action(&format!(
                    "Member {voter} has already voted on proposal {proposal_id}."
                )));
            }
            votes.push(json!({
                "Vote": vote,
                "MemberId": voter,
                "MemberName": member_name,
            }));
            let yes = votes
                .iter()
                .filter(|v| v.get("Vote").and_then(Value::as_str) == Some("YES"))
                .count() as i64;
            let no = votes
                .iter()
                .filter(|v| v.get("Vote").and_then(Value::as_str) == Some("NO"))
                .count() as i64;

            let total = eligible_member_count(data, network_id);
            let (threshold, comparator) = threshold_policy(network.as_ref());
            let outstanding = (total - yes - no).max(0);
            let decision = decide_proposal(yes, outstanding, total, threshold, &comparator);

            // Apply the decision to the proposal + gather any materialisations.
            let actions = data
                .proposals
                .get(proposal_id)
                .and_then(|p| p.get("Actions"))
                .cloned()
                .unwrap_or(json!({}));
            if let Some(p) = data
                .proposals
                .get_mut(proposal_id)
                .and_then(Value::as_object_mut)
            {
                p.insert("YesVoteCount".into(), json!(yes));
                p.insert("NoVoteCount".into(), json!(no));
                p.insert("OutstandingVoteCount".into(), json!(outstanding));
                p.insert("Status".into(), json!(decision.clone()));
            }

            if decision == "APPROVED" {
                // Removals target members in this account/network.
                if let Some(removals) = actions.get("Removals").and_then(Value::as_array) {
                    for r in removals {
                        if let Some(mid) = r.get("MemberId").and_then(Value::as_str) {
                            if let Some(m) =
                                data.members.get_mut(mid).and_then(Value::as_object_mut)
                            {
                                m.insert("Status".into(), json!("DELETED"));
                            }
                        }
                    }
                }
                // Invitations become new `Invitation` records in the invited
                // principal's account.
                if let Some(invites) = actions.get("Invitations").and_then(Value::as_array) {
                    let net = network.as_ref().unwrap();
                    for inv in invites {
                        if let Some(principal) = inv.get("Principal").and_then(Value::as_str) {
                            let invitation = build_invitation(&region, principal, net);
                            invitations_to_send.push((principal.to_string(), invitation));
                        }
                    }
                }
            }
        }

        for (principal, invitation) in invitations_to_send {
            let inv_id = invitation
                .get("InvitationId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let acct = guard.get_or_create(&principal);
            acct.invitations.insert(inv_id, invitation);
        }

        ok_empty()
    }

    // ------------------------------ Accessors ---------------------------

    fn create_accessor(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let now = shared::iso_now();
        let accessor_id = shared::new_accessor_id();
        let billing_token = shared::new_billing_token();
        let network_type = body
            .get("NetworkType")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut acc = Map::new();
        acc.insert("Id".into(), json!(accessor_id));
        acc.insert(
            "Type".into(),
            json!(str_or(body, "AccessorType", "BILLING_TOKEN")),
        );
        acc.insert("BillingToken".into(), json!(billing_token));
        acc.insert("Status".into(), json!("AVAILABLE"));
        acc.insert("CreationDate".into(), json!(now));
        acc.insert(
            "Arn".into(),
            json!(shared::accessor_arn(
                &ctx.region,
                &ctx.account,
                &accessor_id
            )),
        );
        if let Some(nt) = &network_type {
            acc.insert("NetworkType".into(), json!(nt));
        }

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        store_tags(
            data,
            &shared::accessor_arn(&ctx.region, &ctx.account, &accessor_id),
            body,
        );
        data.accessors
            .insert(accessor_id.clone(), Value::Object(acc));

        let mut out = json!({ "AccessorId": accessor_id, "BillingToken": billing_token });
        if let Some(nt) = network_type {
            out["NetworkType"] = json!(nt);
        }
        ok(out)
    }

    fn get_accessor(&self, ctx: &Ctx, accessor_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard.get(&ctx.account);
        match data.and_then(|d| d.accessors.get(accessor_id)) {
            Some(a) => {
                let arn = shared::accessor_arn(&ctx.region, &ctx.account, accessor_id);
                ok(json!({ "Accessor": with_tags(data.unwrap(), &arn, a) }))
            }
            None => Err(not_found(&format!("Accessor {accessor_id} was not found."))),
        }
    }

    fn delete_accessor(
        &self,
        ctx: &Ctx,
        accessor_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(a) = data
            .accessors
            .get_mut(accessor_id)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!("Accessor {accessor_id} was not found.")));
        };
        a.insert("Status".into(), json!("PENDING_DELETION"));
        ok_empty()
    }

    fn list_accessors(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_list_query(
            q,
            50,
            &[("networkType", crate::validate::ACCESSOR_NETWORK_TYPE)],
        )?;
        let network_type = query_one(q, "networkType");
        let guard = self.state.read();
        let mut accessors: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.accessors
                    .values()
                    .filter(|a| opt_eq(a, "NetworkType", network_type))
                    .map(accessor_summary)
                    .collect()
            })
            .unwrap_or_default();
        accessors.sort_by(|a, b| summary_id(a).cmp(summary_id(b)));
        let (page, next) = paginate(accessors, q)?;
        let mut out = json!({ "Accessors": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    // ----------------------------- Invitations --------------------------

    fn list_invitations(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_list_query(q, 100, &[])?;
        let guard = self.state.read();
        let mut invitations: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.invitations.values().cloned().collect())
            .unwrap_or_default();
        invitations.sort_by(|a, b| {
            a.get("InvitationId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .cmp(b.get("InvitationId").and_then(Value::as_str).unwrap_or(""))
        });
        let (page, next) = paginate(invitations, q)?;
        let mut out = json!({ "Invitations": page });
        if let Some(n) = next {
            out["NextToken"] = json!(n);
        }
        ok(out)
    }

    fn reject_invitation(
        &self,
        ctx: &Ctx,
        invitation_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(inv) = data
            .invitations
            .get_mut(invitation_id)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!(
                "Invitation {invitation_id} was not found."
            )));
        };
        let status = inv.get("Status").and_then(Value::as_str).unwrap_or("");
        if status != "PENDING" {
            return Err(illegal_action(&format!(
                "Invitation {invitation_id} is not pending (status {status})."
            )));
        }
        inv.insert("Status".into(), json!("REJECTED"));
        ok_empty()
    }

    // -------------------------------- Tags ------------------------------

    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let entry = data.tags.entry(arn.to_string()).or_default();
        if let Some(tags) = body.get("Tags").and_then(Value::as_object) {
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        ok_empty()
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(arn) {
            for (k, v) in q {
                if k == "tagKeys" {
                    entry.remove(v);
                }
            }
            if entry.is_empty() {
                data.tags.remove(arn);
            }
        }
        ok_empty()
    }

    fn list_tags(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        check_arn(arn)?;
        let guard = self.state.read();
        let tags = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        ok(json!({ "Tags": tags }))
    }
}

// ============================= builders ==============================

/// Build a `Member` wire object. Fabric members carry a CA endpoint + admin
/// username in their framework attributes.
fn build_member(
    ctx: &Ctx,
    network_id: &str,
    member_id: &str,
    config: &Value,
    now: &str,
    fabric: bool,
) -> Value {
    let admin_username = config
        .get("FrameworkConfiguration")
        .and_then(|f| f.get("Fabric"))
        .and_then(|f| f.get("AdminUsername"))
        .and_then(Value::as_str)
        .unwrap_or("admin")
        .to_string();
    let mut m = Map::new();
    m.insert("NetworkId".into(), json!(network_id));
    m.insert("Id".into(), json!(member_id));
    m.insert(
        "Name".into(),
        json!(config.get("Name").and_then(Value::as_str).unwrap_or("")),
    );
    m.insert("Status".into(), json!("CREATING"));
    m.insert("CreationDate".into(), json!(now));
    m.insert(
        "Arn".into(),
        json!(shared::member_arn(&ctx.region, &ctx.account, member_id)),
    );
    if let Some(d) = config.get("Description") {
        m.insert("Description".into(), d.clone());
    }
    if let Some(lpc) = config.get("LogPublishingConfiguration") {
        m.insert("LogPublishingConfiguration".into(), lpc.clone());
    }
    if let Some(kms) = config.get("KmsKeyArn") {
        m.insert("KmsKeyArn".into(), kms.clone());
    }
    if fabric {
        m.insert(
            "FrameworkAttributes".into(),
            json!({
                "Fabric": {
                    "AdminUsername": admin_username,
                    "CaEndpoint": shared::fabric_ca_endpoint(member_id, &ctx.region),
                }
            }),
        );
    }
    Value::Object(m)
}

/// Build a `Node` wire object, deriving the framework-specific endpoints.
fn build_node(
    ctx: &Ctx,
    network_id: &str,
    member_id: Option<&str>,
    node_id: &str,
    config: &Value,
    framework: &str,
    now: &str,
) -> Value {
    let mut n = Map::new();
    n.insert("NetworkId".into(), json!(network_id));
    if let Some(mid) = member_id {
        n.insert("MemberId".into(), json!(mid));
    }
    n.insert("Id".into(), json!(node_id));
    n.insert(
        "InstanceType".into(),
        json!(config
            .get("InstanceType")
            .and_then(Value::as_str)
            .unwrap_or("")),
    );
    if let Some(az) = config.get("AvailabilityZone") {
        n.insert("AvailabilityZone".into(), az.clone());
    } else {
        n.insert("AvailabilityZone".into(), json!(format!("{}a", ctx.region)));
    }
    if let Some(sdb) = config.get("StateDB") {
        n.insert("StateDB".into(), sdb.clone());
    }
    n.insert("Status".into(), json!("CREATING"));
    n.insert("CreationDate".into(), json!(now));
    n.insert(
        "Arn".into(),
        json!(shared::node_arn(&ctx.region, &ctx.account, node_id)),
    );
    if let Some(lpc) = config.get("LogPublishingConfiguration") {
        n.insert("LogPublishingConfiguration".into(), lpc.clone());
    }
    let attrs = if framework == "ETHEREUM" {
        json!({
            "Ethereum": {
                "HttpEndpoint": shared::ethereum_http_endpoint(node_id, &ctx.region),
                "WebSocketEndpoint": shared::ethereum_ws_endpoint(node_id, &ctx.region),
            }
        })
    } else {
        json!({
            "Fabric": {
                "PeerEndpoint": shared::fabric_peer_endpoint(node_id, &ctx.region),
                "PeerEventEndpoint": shared::fabric_peer_event_endpoint(node_id, &ctx.region),
            }
        })
    };
    n.insert("FrameworkAttributes".into(), attrs);
    Value::Object(n)
}

/// Build an `Invitation` wire object for a materialised proposal invitation.
fn build_invitation(region: &str, principal: &str, network: &Value) -> Value {
    let id = shared::new_invitation_id();
    let now = shared::iso_now();
    let expiration = (chrono::Utc::now() + chrono::Duration::days(7))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    json!({
        "InvitationId": id,
        "CreationDate": now,
        "ExpirationDate": expiration,
        "Status": "PENDING",
        "NetworkSummary": network_summary(network),
        "Arn": shared::invitation_arn(region, principal, &id),
    })
}

/// The network-level `FrameworkAttributes` (Fabric ordering endpoint + edition,
/// or Ethereum chain id).
fn network_framework_attributes(
    framework: &str,
    network_id: &str,
    body: &Value,
    region: &str,
) -> Value {
    if framework == "ETHEREUM" {
        // Ethereum network chain id is derived from the framework version.
        let chain_id = match body.get("FrameworkVersion").and_then(Value::as_str) {
            Some("1.0") | Some("mainnet") => "1",
            _ => "1",
        };
        json!({ "Ethereum": { "ChainId": chain_id } })
    } else {
        let edition = body
            .get("FrameworkConfiguration")
            .and_then(|f| f.get("Fabric"))
            .and_then(|f| f.get("Edition"))
            .and_then(Value::as_str)
            .unwrap_or("STARTER");
        json!({
            "Fabric": {
                "OrderingServiceEndpoint": shared::fabric_ordering_endpoint(network_id, region),
                "Edition": edition,
            }
        })
    }
}

// ============================= summaries =============================

fn network_summary(n: &Value) -> Value {
    let mut s = Map::new();
    for k in [
        "Id",
        "Name",
        "Description",
        "Framework",
        "FrameworkVersion",
        "Status",
        "CreationDate",
        "Arn",
    ] {
        if let Some(v) = n.get(k) {
            s.insert(k.to_string(), v.clone());
        }
    }
    Value::Object(s)
}

fn member_summary(m: &Value, account: &str, region: &str) -> Value {
    let id = m.get("Id").and_then(Value::as_str).unwrap_or("");
    let mut s = Map::new();
    for k in ["Id", "Name", "Description", "Status", "CreationDate"] {
        if let Some(v) = m.get(k) {
            s.insert(k.to_string(), v.clone());
        }
    }
    s.insert("IsOwned".into(), json!(true));
    s.insert("Arn".into(), json!(shared::member_arn(region, account, id)));
    Value::Object(s)
}

fn node_summary(n: &Value) -> Value {
    let mut s = Map::new();
    for k in [
        "Id",
        "Status",
        "CreationDate",
        "AvailabilityZone",
        "InstanceType",
        "Arn",
    ] {
        if let Some(v) = n.get(k) {
            s.insert(k.to_string(), v.clone());
        }
    }
    Value::Object(s)
}

fn proposal_summary(p: &Value) -> Value {
    let mut s = Map::new();
    for k in [
        "ProposalId",
        "Description",
        "ProposedByMemberId",
        "ProposedByMemberName",
        "Status",
        "CreationDate",
        "ExpirationDate",
        "Arn",
    ] {
        if let Some(v) = p.get(k) {
            s.insert(k.to_string(), v.clone());
        }
    }
    Value::Object(s)
}

fn accessor_summary(a: &Value) -> Value {
    let mut s = Map::new();
    for k in ["Id", "Type", "Status", "CreationDate", "Arn", "NetworkType"] {
        if let Some(v) = a.get(k) {
            s.insert(k.to_string(), v.clone());
        }
    }
    Value::Object(s)
}

// ============================= voting logic ==========================

/// Extract `(ThresholdPercentage, ThresholdComparator)` from a network's voting
/// policy, defaulting to a simple majority (50 %, GREATER_THAN).
fn threshold_policy(network: Option<&Value>) -> (i64, String) {
    let policy = network
        .and_then(|n| n.get("VotingPolicy"))
        .and_then(|v| v.get("ApprovalThresholdPolicy"));
    let threshold = policy
        .and_then(|p| p.get("ThresholdPercentage"))
        .and_then(Value::as_i64)
        .unwrap_or(50);
    let comparator = policy
        .and_then(|p| p.get("ThresholdComparator"))
        .and_then(Value::as_str)
        .unwrap_or("GREATER_THAN")
        .to_string();
    (threshold, comparator)
}

/// The `ProposalDurationInHours` from a network's voting policy (default 24).
fn proposal_duration_hours(network: Option<&Value>) -> i64 {
    network
        .and_then(|n| n.get("VotingPolicy"))
        .and_then(|v| v.get("ApprovalThresholdPolicy"))
        .and_then(|p| p.get("ProposalDurationInHours"))
        .and_then(Value::as_i64)
        .unwrap_or(24)
}

/// Number of members eligible to vote in a network (not deleted).
fn eligible_member_count(data: &ManagedBlockchainData, network_id: &str) -> i64 {
    data.members
        .values()
        .filter(|m| {
            m.get("NetworkId").and_then(Value::as_str) == Some(network_id)
                && m.get("Status").and_then(Value::as_str) != Some("DELETED")
        })
        .count() as i64
}

/// Decide a proposal's status from the current tally. Approves as soon as the
/// yes votes meet the threshold of the total electorate; rejects once approval
/// is arithmetically impossible even if every outstanding vote were `YES`.
fn decide_proposal(
    yes: i64,
    outstanding: i64,
    total: i64,
    threshold: i64,
    comparator: &str,
) -> String {
    if total <= 0 {
        return "IN_PROGRESS".to_string();
    }
    let meets = |y: i64| {
        let pct = (y as f64) * 100.0 / (total as f64);
        match comparator {
            "GREATER_THAN_OR_EQUAL_TO" => pct >= threshold as f64,
            _ => pct > threshold as f64,
        }
    };
    if meets(yes) {
        "APPROVED".to_string()
    } else if !meets(yes + outstanding) {
        "REJECTED".to_string()
    } else {
        "IN_PROGRESS".to_string()
    }
}

/// Promote a `CREATING` member/node wire object to `AVAILABLE`. Returns `true`
/// on change.
fn promote_creating(v: &mut Value) -> bool {
    if let Some(obj) = v.as_object_mut() {
        if obj.get("Status").and_then(Value::as_str) == Some("CREATING") {
            obj.insert("Status".into(), json!("AVAILABLE"));
            return true;
        }
    }
    false
}

// ============================= helpers ==============================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn ok_empty() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::OK, "{}"))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| invalid_request(&format!("The request body is malformed: {e}")))
}

fn invalid_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn illegal_action(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "IllegalActionException", msg)
}

/// Reject a path-label ARN that is absent or not an ARN with
/// `InvalidRequestException`.
fn check_arn(arn: &str) -> Result<(), AwsServiceError> {
    if arn.is_empty() || !arn.starts_with("arn:") {
        return Err(invalid_request(
            "The request failed because it is missing a valid resource ARN.",
        ));
    }
    Ok(())
}

fn str_or(body: &Value, key: &str, default: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

/// Copy each named optional member from `body` into `out` verbatim when present
/// and non-null.
fn echo(out: &mut Map<String, Value>, body: &Value, keys: &[&str]) {
    for key in keys {
        if let Some(v) = body.get(*key) {
            if !v.is_null() {
                out.insert((*key).to_string(), v.clone());
            }
        }
    }
}

/// Whether an object's string member equals a filter (or the filter is `None`).
fn opt_eq(v: &Value, key: &str, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => v.get(key).and_then(Value::as_str) == Some(f),
    }
}

fn summary_id(v: &Value) -> &str {
    v.get("Id").and_then(Value::as_str).unwrap_or("")
}

/// Store the create-time `Tags` map (if any) under a resource ARN.
fn store_tags(data: &mut ManagedBlockchainData, arn: &str, body: &Value) {
    if let Some(tags) = body.get("Tags").and_then(Value::as_object) {
        if tags.is_empty() {
            return;
        }
        let entry = data.tags.entry(arn.to_string()).or_default();
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
    }
}

/// Attach the resource's `Tags` (from the ARN-keyed tag store) to a clone of its
/// wire object for a read response.
fn with_tags(data: &ManagedBlockchainData, arn: &str, obj: &Value) -> Value {
    let mut out = obj.clone();
    let tags = data.tags.get(arn).cloned().unwrap_or_default();
    if let Some(o) = out.as_object_mut() {
        o.insert("Tags".into(), json!(tags));
    }
    out
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Managed Blockchain resource ids (`ResourceIdString`) are 1..=32 chars. The
/// pagination token (`PaginationToken`) is at most 128 chars.
const ID_MAX_LEN: usize = 32;
const TOKEN_MAX_LEN: usize = 128;

pub const NETWORK_STATUS: &[&str] = &[
    "CREATING",
    "AVAILABLE",
    "CREATE_FAILED",
    "DELETING",
    "DELETED",
];
pub const MEMBER_STATUS: &[&str] = &[
    "CREATING",
    "AVAILABLE",
    "CREATE_FAILED",
    "UPDATING",
    "DELETING",
    "DELETED",
    "INACCESSIBLE_ENCRYPTION_KEY",
];
pub const NODE_STATUS: &[&str] = &[
    "CREATING",
    "AVAILABLE",
    "UNHEALTHY",
    "CREATE_FAILED",
    "UPDATING",
    "DELETING",
    "DELETED",
    "FAILED",
    "INACCESSIBLE_ENCRYPTION_KEY",
];

/// Reject a path-label resource id whose length is outside `1..=32`, or which
/// still carries an unfilled `{Placeholder}` (an omitted required label), with
/// `InvalidRequestException`.
fn check_id(id: &str, field: &str) -> Result<(), AwsServiceError> {
    let n = id.chars().count();
    if !(1..=ID_MAX_LEN).contains(&n) {
        return Err(invalid_request(&format!(
            "'{field}' length must be between 1 and {ID_MAX_LEN}, got {n}."
        )));
    }
    if id.contains('{') || id.contains('}') {
        return Err(invalid_request(&format!(
            "'{field}' is not a valid resource id: {id}"
        )));
    }
    Ok(())
}

/// Validate the common list query params: `maxResults` (within `1..=cap`),
/// `nextToken` (<= 128 chars), plus any `(param, allowed-enum)` constraints.
fn validate_list_query(
    q: &[(String, String)],
    cap: i64,
    enums: &[(&str, &[&str])],
) -> Result<(), AwsServiceError> {
    if let Some(v) = query_one(q, "maxResults") {
        let n: i64 = v
            .parse()
            .map_err(|_| invalid_request("maxResults must be an integer."))?;
        if n < 1 || n > cap {
            return Err(invalid_request(&format!(
                "maxResults must be between 1 and {cap}, got {n}."
            )));
        }
    }
    if let Some(t) = query_one(q, "nextToken") {
        if t.chars().count() > TOKEN_MAX_LEN {
            return Err(invalid_request(&format!(
                "nextToken must be at most {TOKEN_MAX_LEN} characters."
            )));
        }
    }
    for (key, allowed) in enums {
        if let Some(v) = query_one(q, key) {
            if !allowed.contains(&v) {
                return Err(invalid_request(&format!(
                    "'{key}' must be one of [{}], got '{v}'.",
                    allowed.join(", ")
                )));
            }
        }
    }
    Ok(())
}

/// Paginate wire objects using the request's `maxResults` / `nextToken` query
/// params (`nextToken` is a plain decimal offset). Range/length validation is
/// performed up front by `validate_list_query`.
fn paginate(
    items: Vec<Value>,
    q: &[(String, String)],
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let max = query_one(q, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let start = start.min(items.len());
    let end = start.saturating_add(max).min(items.len());
    let page: Vec<Value> = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    Ok((page, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> ManagedBlockchainService {
        ManagedBlockchainService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error response"),
            Err(e) => e,
        }
    }

    fn fabric_network(s: &ManagedBlockchainService) -> (String, String) {
        let resp = s
            .create_network(
                &ctx(),
                &json!({
                    "ClientRequestToken": "t",
                    "Name": "net",
                    "Framework": "HYPERLEDGER_FABRIC",
                    "FrameworkVersion": "2.2",
                    "FrameworkConfiguration": { "Fabric": { "Edition": "STARTER" } },
                    "VotingPolicy": { "ApprovalThresholdPolicy": {
                        "ThresholdPercentage": 50,
                        "ProposalDurationInHours": 24,
                        "ThresholdComparator": "GREATER_THAN"
                    } },
                    "MemberConfiguration": {
                        "Name": "member1",
                        "FrameworkConfiguration": { "Fabric": {
                            "AdminUsername": "admin", "AdminPassword": "Password123!"
                        } }
                    },
                    "Description": "desc"
                }),
            )
            .unwrap();
        let out = body_of(&resp);
        (
            out["NetworkId"].as_str().unwrap().to_string(),
            out["MemberId"].as_str().unwrap().to_string(),
        )
    }

    #[test]
    fn create_network_atomically_creates_first_member() {
        let s = svc();
        let (net_id, member_id) = fabric_network(&s);
        assert!(net_id.starts_with("n-"));
        assert!(member_id.starts_with("m-"));

        // GetNetwork echoes name + description + status AVAILABLE.
        let g = body_of(&s.get_network(&ctx(), &net_id).unwrap());
        assert_eq!(g["Network"]["Name"], "net");
        assert_eq!(g["Network"]["Description"], "desc");
        assert_eq!(g["Network"]["Status"], "AVAILABLE");
        assert_eq!(
            g["Network"]["FrameworkAttributes"]["Fabric"]["Edition"],
            "STARTER"
        );

        // GetMember settles to AVAILABLE on read and carries a CA endpoint.
        s.reconcile(&ctx().account);
        let m = body_of(&s.get_member(&ctx(), &net_id, &member_id).unwrap());
        assert_eq!(m["Member"]["Status"], "AVAILABLE");
        assert!(m["Member"]["FrameworkAttributes"]["Fabric"]["CaEndpoint"].is_string());
    }

    #[test]
    fn node_lifecycle() {
        let s = svc();
        let (net_id, _member) = fabric_network(&s);
        let created = body_of(
            &s.create_node(
                &ctx(),
                &net_id,
                &json!({
                    "ClientRequestToken": "t2",
                    "NodeConfiguration": { "InstanceType": "bc.t3.small", "StateDB": "CouchDB" }
                }),
            )
            .unwrap(),
        );
        let node_id = created["NodeId"].as_str().unwrap().to_string();
        assert!(node_id.starts_with("nd-"));
        s.reconcile(&ctx().account);
        let g = body_of(&s.get_node(&ctx(), &net_id, &node_id).unwrap());
        assert_eq!(g["Node"]["Status"], "AVAILABLE");
        assert!(g["Node"]["FrameworkAttributes"]["Fabric"]["PeerEndpoint"].is_string());
        // Delete flips to DELETED.
        s.delete_node(&ctx(), &net_id, &node_id).unwrap();
        let g2 = body_of(&s.get_node(&ctx(), &net_id, &node_id).unwrap());
        assert_eq!(g2["Node"]["Status"], "DELETED");
    }

    #[test]
    fn proposal_vote_approves_and_materialises_invitation() {
        let s = svc();
        let (net_id, member_id) = fabric_network(&s);
        // Propose inviting our own account so ListInvitations can see it.
        let prop = body_of(
            &s.create_proposal(
                &ctx(),
                &net_id,
                &json!({
                    "ClientRequestToken": "t3",
                    "MemberId": member_id,
                    "Actions": { "Invitations": [ { "Principal": "000000000000" } ] }
                }),
            )
            .unwrap(),
        );
        let proposal_id = prop["ProposalId"].as_str().unwrap().to_string();
        // One member, threshold 50% GREATER_THAN -> a single YES = 100% approves.
        s.vote_on_proposal(
            &ctx(),
            &net_id,
            &proposal_id,
            &json!({ "VoterMemberId": member_id, "Vote": "YES" }),
        )
        .unwrap();
        let g = body_of(&s.get_proposal(&ctx(), &net_id, &proposal_id).unwrap());
        assert_eq!(g["Proposal"]["Status"], "APPROVED");
        assert_eq!(g["Proposal"]["YesVoteCount"], 1);
        // Votes are recorded.
        let votes = body_of(
            &s.list_proposal_votes(&ctx(), &net_id, &proposal_id, &[])
                .unwrap(),
        );
        assert_eq!(votes["ProposalVotes"].as_array().unwrap().len(), 1);
        // The invitation was materialised.
        let invs = body_of(&s.list_invitations(&ctx(), &[]).unwrap());
        assert_eq!(invs["Invitations"].as_array().unwrap().len(), 1);
        assert_eq!(invs["Invitations"][0]["Status"], "PENDING");
    }

    #[test]
    fn create_member_requires_pending_invitation_and_existing_network() {
        // CreateMember accepted any invitation regardless of status and reused
        // it indefinitely, and neither CreateMember nor CreateNode verified the
        // network existed (bug-hunt).
        let s = svc();
        let (net_id, member_id) = fabric_network(&s);
        // Materialize a PENDING invitation for our own account.
        let prop = body_of(
            &s.create_proposal(
                &ctx(),
                &net_id,
                &json!({
                    "ClientRequestToken": "tp",
                    "MemberId": member_id,
                    "Actions": { "Invitations": [ { "Principal": "000000000000" } ] }
                }),
            )
            .unwrap(),
        );
        let pid = prop["ProposalId"].as_str().unwrap().to_string();
        s.vote_on_proposal(
            &ctx(),
            &net_id,
            &pid,
            &json!({ "VoterMemberId": member_id, "Vote": "YES" }),
        )
        .unwrap();
        let invs = body_of(&s.list_invitations(&ctx(), &[]).unwrap());
        let inv_id = invs["Invitations"][0]["InvitationId"]
            .as_str()
            .unwrap()
            .to_string();

        let member_body = json!({
            "InvitationId": inv_id,
            "MemberConfiguration": {
                "Name": "member2",
                "FrameworkConfiguration": { "Fabric": {
                    "AdminUsername": "admin", "AdminPassword": "Password123!"
                } }
            }
        });

        // First join succeeds and consumes the invitation.
        let created = body_of(&s.create_member(&ctx(), &net_id, &member_body).unwrap());
        assert!(created["MemberId"].as_str().unwrap().starts_with("m-"));

        // Reusing the now-ACCEPTED invitation is rejected.
        let err = expect_err(s.create_member(&ctx(), &net_id, &member_body));
        assert!(format!("{err:?}").contains("InvalidRequestException"));

        // CreateMember / CreateNode against a nonexistent network 404.
        let err2 = expect_err(s.create_member(&ctx(), "n-DOESNOTEXIST0001", &member_body));
        assert!(format!("{err2:?}").contains("ResourceNotFoundException"));
        let err3 = expect_err(s.create_node(
            &ctx(),
            "n-DOESNOTEXIST0001",
            &json!({
                "ClientRequestToken": "tn",
                "NodeConfiguration": { "InstanceType": "bc.t3.small", "StateDB": "CouchDB" }
            }),
        ));
        assert!(format!("{err3:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn duplicate_vote_is_illegal_action() {
        let s = svc();
        let (net_id, member_id) = fabric_network(&s);
        let prop = body_of(
            &s.create_proposal(
                &ctx(),
                &net_id,
                &json!({
                    "ClientRequestToken": "t",
                    "MemberId": member_id,
                    "Actions": { "Removals": [ { "MemberId": "m-OTHER" } ] }
                }),
            )
            .unwrap(),
        );
        let pid = prop["ProposalId"].as_str().unwrap().to_string();
        s.vote_on_proposal(
            &ctx(),
            &net_id,
            &pid,
            &json!({ "VoterMemberId": member_id, "Vote": "YES" }),
        )
        .unwrap();
        // Proposal already APPROVED -> further votes are illegal.
        let err = expect_err(s.vote_on_proposal(
            &ctx(),
            &net_id,
            &pid,
            &json!({ "VoterMemberId": member_id, "Vote": "NO" }),
        ));
        assert!(format!("{err:?}").contains("IllegalActionException"));
    }

    #[test]
    fn accessor_lifecycle() {
        let s = svc();
        let created = body_of(
            &s.create_accessor(
                &ctx(),
                &json!({ "ClientRequestToken": "t", "AccessorType": "BILLING_TOKEN", "NetworkType": "ETHEREUM_MAINNET" }),
            )
            .unwrap(),
        );
        let id = created["AccessorId"].as_str().unwrap().to_string();
        assert!(created["BillingToken"].is_string());
        let g = body_of(&s.get_accessor(&ctx(), &id).unwrap());
        assert_eq!(g["Accessor"]["Status"], "AVAILABLE");
        assert_eq!(g["Accessor"]["NetworkType"], "ETHEREUM_MAINNET");
        s.delete_accessor(&ctx(), &id).unwrap();
        let g2 = body_of(&s.get_accessor(&ctx(), &id).unwrap());
        assert_eq!(g2["Accessor"]["Status"], "PENDING_DELETION");
    }

    #[test]
    fn list_networks_filters_and_paginates() {
        let s = svc();
        fabric_network(&s);
        fabric_network(&s);
        let all = body_of(&s.list_networks(&ctx(), &[]).unwrap());
        assert_eq!(all["Networks"].as_array().unwrap().len(), 2);
        // Paginate.
        let page1 = body_of(
            &s.list_networks(&ctx(), &[("maxResults".into(), "1".into())])
                .unwrap(),
        );
        assert_eq!(page1["Networks"].as_array().unwrap().len(), 1);
        assert!(page1["NextToken"].is_string());
        // Filter by framework.
        let eth = body_of(
            &s.list_networks(&ctx(), &[("framework".into(), "ETHEREUM".into())])
                .unwrap(),
        );
        assert_eq!(eth["Networks"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn unknown_network_is_resource_not_found() {
        let s = svc();
        let err = expect_err(s.get_network(&ctx(), "n-NOPE"));
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn tag_untag_round_trips() {
        let s = svc();
        let (net_id, _m) = fabric_network(&s);
        let arn = shared::network_arn(&net_id);
        s.tag_resource(&ctx(), &arn, &json!({ "Tags": { "k": "v" } }))
            .unwrap();
        let listed = body_of(&s.list_tags(&ctx(), &arn).unwrap());
        assert_eq!(listed["Tags"]["k"], "v");
        s.untag_resource(&ctx(), &arn, &[("tagKeys".into(), "k".into())])
            .unwrap();
        let after = body_of(&s.list_tags(&ctx(), &arn).unwrap());
        assert!(after["Tags"].as_object().unwrap().is_empty());
    }
}
