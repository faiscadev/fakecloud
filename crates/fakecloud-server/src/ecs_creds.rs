//! The ECS task-role credentials endpoint (`GET /_fakecloud/ecs/creds/{task_id}`).
//!
//! A task whose task definition names a `taskRoleArn` gets
//! `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/v2/credentials/<task-id>`, which
//! its SDK resolves against the agent's link-local `http://169.254.170.2`; the
//! task's network namespace NATs that address to fakecloud, and
//! [`link_local_host_middleware`] answers it from this endpoint. (When that
//! namespace can't be set up the task falls back to
//! `AWS_CONTAINER_CREDENTIALS_FULL_URI` pointed here.) Like the ECS agent, the
//! endpoint hands out credentials for that role's session named after the
//! task ID (`assumed-role/<role>/<task-id>`), minted and registered like an
//! `AssumeRole` session so they verify under `--verify-sigv4` and are
//! evaluated as the role under `--iam`. Refetches within the validity window
//! return the same set; near expiry a fresh one is minted.
//!
//! Once the task stops its credentials are revoked (the sweep in
//! [`run_revocation_sweep`]), and the endpoint answers a stopped, unknown, or
//! role-less task the way the agent answers an ID it holds no credentials
//! for: HTTP 400 `{"code":"InvalidIdInRequest","message":"CredentialsV2Request:
//! Credentials not found","HTTPErrorCode":400}`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use fakecloud_ecs::SharedEcsState;
use fakecloud_iam::sts_service::container_creds::{
    revoke_sessions_named, ContainerCredentials, WorkloadCredentialCache,
    DEFAULT_CONTAINER_CREDENTIALS_DURATION,
};
use fakecloud_iam::SharedIamState;

/// Error prefix the ECS agent's v2 credentials handler puts on its messages.
const ERR_PREFIX: &str = "CredentialsV2Request: ";

/// How often stopped tasks' credentials are revoked.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Why the endpoint has no credentials for a request, as the agent reports it.
#[derive(Debug, PartialEq, Eq)]
pub enum CredentialsError {
    /// The request carried no task ID.
    NoId,
    /// No running task with that ID has a task role.
    NotFound,
    /// Under `--iam strict`, the request did not present the task's
    /// `AWS_CONTAINER_AUTHORIZATION_TOKEN` in its `Authorization` header.
    Unauthorized,
}

impl IntoResponse for CredentialsError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::NoId => (
                StatusCode::BAD_REQUEST,
                "NoIdInRequest",
                "No Credential ID in the request",
            ),
            Self::NotFound => (
                StatusCode::BAD_REQUEST,
                "InvalidIdInRequest",
                "Credentials not found",
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "AccessDenied",
                "Authorization token missing or invalid for this task",
            ),
        };
        (
            status,
            axum::Json(serde_json::json!({
                "code": code,
                "message": format!("{ERR_PREFIX}{message}"),
                "HTTPErrorCode": status.as_u16(),
            })),
        )
            .into_response()
    }
}

/// Serves task-role credentials for ECS tasks and revokes them once the task
/// stops.
pub struct EcsTaskCredentials {
    ecs: SharedEcsState,
    iam: SharedIamState,
    default_account_id: String,
    cache: WorkloadCredentialCache,
    /// Require the task's authorization token (`--iam strict`).
    require_authorization_token: bool,
}

/// The full ARN of a task's role. ECS accepts a bare role name for
/// `taskRoleArn`; it names a role in the task's own account and partition.
fn task_role_arn(task: &fakecloud_ecs::Task, task_account: &str) -> Option<String> {
    let role = task.task_role_arn.as_deref()?;
    if role.starts_with("arn:") {
        return Some(role.to_string());
    }
    Some(
        fakecloud_aws::arn::Arn::global("iam", task_account, &format!("role/{role}"))
            .with_partition(fakecloud_aws::arn::partition_of(&task.task_arn))
            .to_string(),
    )
}

fn cache_key(account_id: &str, task_id: &str) -> String {
    format!("{account_id}/{task_id}")
}

impl EcsTaskCredentials {
    /// An endpoint that requires no authorization token (`--iam` off/soft).
    #[cfg(test)]
    pub fn new(
        ecs: SharedEcsState,
        iam: SharedIamState,
        default_account_id: impl Into<String>,
    ) -> Arc<Self> {
        Self::with_authorization(ecs, iam, default_account_id, false)
    }

    /// Like [`EcsTaskCredentials::new`]; with `require_authorization_token`
    /// (set under `--iam strict`) a request must present the task's
    /// `AWS_CONTAINER_AUTHORIZATION_TOKEN` (injected into its containers) as
    /// its `Authorization` header. Otherwise anyone able to reach fakecloud
    /// and name a running task ID would walk away with its role's
    /// credentials; on ECS only the task's own network reaches the agent.
    pub fn with_authorization(
        ecs: SharedEcsState,
        iam: SharedIamState,
        default_account_id: impl Into<String>,
        require_authorization_token: bool,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            ecs,
            iam,
            default_account_id: default_account_id.into(),
            cache: WorkloadCredentialCache::new(),
            require_authorization_token,
        });
        this.revoke_persisted_sessions();
        this
    }

    /// Revoke the sessions of stopped tasks still registered from an earlier
    /// run. With persistence on, IAM state (and the sessions in it) survives
    /// a restart while the cache that would revoke them does not, and the
    /// restart stops every task it restores.
    fn revoke_persisted_sessions(&self) {
        let stopped: Vec<(String, String)> = {
            let accounts = self.ecs.read();
            accounts
                .iter()
                .flat_map(|(account_id, state)| {
                    state
                        .tasks
                        .iter()
                        .filter(|(_, t)| t.last_status == "STOPPED")
                        .filter_map(move |(id, t)| {
                            Some((task_role_arn(t, account_id)?, id.clone()))
                        })
                })
                .collect()
        };
        for (role_arn, task_id) in stopped {
            revoke_sessions_named(&self.iam, &self.default_account_id, &role_arn, &task_id);
        }
    }

    /// Credentials for the task role of the running task `task_id`.
    ///
    /// The ECS read lock is held until the credentials are minted, so a task
    /// cannot stop (and be swept) between the running check and the mint and
    /// still walk away with a fresh session.
    pub fn credentials(&self, task_id: &str) -> Result<ContainerCredentials, CredentialsError> {
        if task_id.is_empty() {
            return Err(CredentialsError::NoId);
        }
        let accounts = self.ecs.read();
        let (account_id, role_arn) = accounts
            .iter()
            .find_map(|(account_id, state)| {
                let task = state.tasks.get(task_id)?;
                (task.last_status != "STOPPED").then_some((account_id, task))
            })
            .and_then(|(account_id, task)| Some((account_id, task_role_arn(task, account_id)?)))
            .ok_or(CredentialsError::NotFound)?;
        let creds = self.cache.get_or_mint(
            &self.iam,
            account_id,
            &cache_key(account_id, task_id),
            &role_arn,
            task_id,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        drop(accounts);
        Ok(creds)
    }

    /// Revoke the credentials of every task that has stopped (or is gone).
    ///
    /// The ECS read lock is held until the revocation is done, so a task
    /// started (and handed credentials) while the sweep runs is never judged
    /// against a snapshot that predates it. Lock order, here and in
    /// `credentials`: ECS state, then the cache, then IAM.
    pub fn revoke_stopped(&self) {
        let accounts = self.ecs.read();
        let running: HashSet<String> = accounts
            .iter()
            .flat_map(|(account_id, state)| {
                state
                    .tasks
                    .iter()
                    .filter(|(_, t)| t.last_status != "STOPPED")
                    .map(move |(task_id, _)| cache_key(account_id, task_id))
            })
            .collect();
        self.cache
            .revoke_unless(&self.iam, |key| running.contains(key));
        drop(accounts);
    }

    /// Whether `authorization` (the request's `Authorization` header) may
    /// fetch `task_id`'s credentials.
    fn authorized(&self, task_id: &str, authorization: Option<&str>) -> bool {
        if !self.require_authorization_token {
            return true;
        }
        let expected = fakecloud_ecs::runtime::task_credentials_token(task_id);
        authorization
            .is_some_and(|got| constant_time_eq(got.trim().as_bytes(), expected.as_bytes()))
    }

    /// The full-URI endpoint's response for `task_id`, given the request's
    /// `Authorization` header (required under `--iam strict`).
    pub fn respond(&self, task_id: &str, authorization: Option<&str>) -> Response {
        if !task_id.is_empty() && !self.authorized(task_id, authorization) {
            return CredentialsError::Unauthorized.into_response();
        }
        self.respond_relative(task_id)
    }

    /// Whether a request for the agent's relative-URI surface that reached
    /// the main port from `peer` may be served. On ECS that address only
    /// answers inside the task's network, so under `--iam strict` it is
    /// served only to peers on a container network (a task's NAT rule makes
    /// its requests arrive from the container's bridge / pod address), not to
    /// arbitrary clients of the main port such as processes on the host
    /// itself. Outside strict mode every peer is served, as before. The
    /// dedicated `--imds-link-local` listener is not subject to this check.
    pub fn relative_uri_source_allowed(&self, peer: Option<std::net::IpAddr>) -> bool {
        !self.require_authorization_token || peer.is_some_and(is_container_network_address)
    }

    /// The agent's relative-URI (`169.254.170.2/v2/credentials/<id>`)
    /// response for `task_id`. As on ECS it takes no authorization token:
    /// the address is only routed from inside the task's own network
    /// namespace, and several SDKs (JS v2, Java v1) never send the token
    /// with a relative URI.
    pub fn respond_relative(&self, task_id: &str) -> Response {
        match self.credentials(task_id) {
            Ok(creds) => (StatusCode::OK, axum::Json(creds.to_container_json())).into_response(),
            Err(e) => e.into_response(),
        }
    }
}

/// The ECS agent's link-local credentials address. Task containers reach it
/// as `http://169.254.170.2` + `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
pub const LINK_LOCAL_HOST: &str = "169.254.170.2";

/// The agent's task-credentials path prefix (`/v2/credentials/<task-id>`).
const V2_CREDENTIALS_PATH: &str = "/v2/credentials";

/// Whether a request's `Host` names the agent's link-local address (port 80,
/// explicit or implied).
fn is_link_local_host(headers: &axum::http::HeaderMap) -> bool {
    let Some(host) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let host = host.trim();
    host == LINK_LOCAL_HOST
        || host
            .strip_prefix(LINK_LOCAL_HOST)
            .is_some_and(|rest| rest == ":80")
}

/// Answer a request addressed to the agent's link-local address the way the
/// agent does: task-role credentials at `/v2/credentials/<task-id>` (a missing
/// ID is `NoIdInRequest`), and nothing else.
fn respond_link_local(
    creds: &EcsTaskCredentials,
    method: &axum::http::Method,
    path: &str,
) -> Response {
    let Some(rest) = path.strip_prefix(V2_CREDENTIALS_PATH) else {
        return (StatusCode::NOT_FOUND, "404 page not found\n").into_response();
    };
    let task_id = match rest {
        "" | "/" => "",
        _ => match rest.strip_prefix('/') {
            Some(id) => id,
            // `/v2/credentialsXYZ` is a different path.
            None => return (StatusCode::NOT_FOUND, "404 page not found\n").into_response(),
        },
    };
    if method == axum::http::Method::HEAD {
        // Same status and headers as GET, no body.
        let (parts, _) = creds.respond_relative(task_id).into_parts();
        return Response::from_parts(parts, axum::body::Body::empty());
    }
    if method != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    creds.respond_relative(task_id)
}

/// The `Authorization` header of a credentials request, where the AWS SDKs
/// put `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
pub fn authorization_header(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Serve the ECS agent's link-local credentials surface on the main listener.
///
/// Task containers reach fakecloud at `169.254.170.2:80`: the task's network
/// namespace NATs that address to fakecloud's port (see the ECS runtime), so
/// the request arrives here with `Host: 169.254.170.2`. Such requests are
/// answered from the agent's surface; everything else falls through.
pub async fn link_local_host_middleware(
    axum::extract::State(creds): axum::extract::State<Arc<EcsTaskCredentials>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if is_link_local_host(req.headers()) {
        let peer = req
            .extensions()
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|c| c.0.ip());
        if !creds.relative_uri_source_allowed(peer) {
            tracing::warn!(
                target: "fakecloud::iam::audit",
                peer = ?peer,
                "agent credentials request on the main port from outside a container network; refused under --iam strict"
            );
            return CredentialsError::Unauthorized.into_response();
        }
        return respond_link_local(&creds, req.method(), req.uri().path());
    }
    next.run(req).await
}

/// Whether `ip` is an address a container network hands out: the private
/// IPv4 ranges Docker / Podman bridges and Kubernetes pod networks draw from
/// (10/8, 172.16/12, 192.168/16, 100.64/10 CGNAT) and IPv6 unique-local /
/// link-local. Loopback (the host itself) and public addresses are not.
fn is_container_network_address(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            v4.is_private() || (a == 100 && (64..128).contains(&b))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_container_network_address(IpAddr::V4(v4));
            }
            let first = v6.segments()[0];
            (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
    }
}

/// Revoke stopped tasks' credentials for as long as the server runs.
pub async fn run_revocation_sweep(creds: Arc<EcsTaskCredentials>) {
    let mut interval = tokio::time::interval(SWEEP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        creds.revoke_stopped();
    }
}

/// Test fixtures shared with the link-local listener's tests.
#[cfg(test)]
pub(crate) mod test_support {
    use fakecloud_ecs::SharedEcsState;

    /// Insert a RUNNING task `task_id` into `account` with the given role.
    pub(crate) fn add_task(ecs: &SharedEcsState, account: &str, task_id: &str, role: Option<&str>) {
        let task: fakecloud_ecs::Task = serde_json::from_value(serde_json::json!({
            "task_arn": format!("arn:aws:ecs:us-east-1:{account}:task/c/{task_id}"),
            "task_id": task_id,
            "cluster_arn": format!("arn:aws:ecs:us-east-1:{account}:cluster/c"),
            "cluster_name": "c",
            "task_definition_arn": format!("arn:aws:ecs:us-east-1:{account}:task-definition/f:1"),
            "family": "f",
            "revision": 1,
            "last_status": "RUNNING",
            "desired_status": "RUNNING",
            "launch_type": "FARGATE",
            "containers": [],
            "overrides": {},
            "connectivity": "CONNECTED",
            "created_at": chrono::Utc::now(),
            "task_role_arn": role,
            "tags": [],
        }))
        .expect("task deserializes");
        ecs.write()
            .get_or_create(account)
            .tasks
            .insert(task_id.to_string(), task);
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::add_task;
    use super::*;
    use fakecloud_core::auth::CredentialResolver;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_iam::credential_resolver::IamCredentialResolver;
    use parking_lot::RwLock;

    const ACCOUNT: &str = "123456789012";
    const ROLE: &str = "arn:aws:iam::123456789012:role/app-task-role";

    fn setup() -> (SharedEcsState, SharedIamState, Arc<EcsTaskCredentials>) {
        let ecs: SharedEcsState = Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "http://localhost:4566",
        )));
        let iam: SharedIamState = Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "http://localhost:4566",
        )));
        let creds = EcsTaskCredentials::new(ecs.clone(), iam.clone(), ACCOUNT);
        (ecs, iam, creds)
    }

    fn stop_task(ecs: &SharedEcsState, account: &str, task_id: &str) {
        ecs.write()
            .get_or_create(account)
            .tasks
            .get_mut(task_id)
            .unwrap()
            .last_status = "STOPPED".into();
    }

    fn resolves(iam: &SharedIamState, creds: &ContainerCredentials) -> bool {
        IamCredentialResolver::new(iam.clone())
            .resolve(&creds.access_key_id)
            .is_some()
    }

    #[test]
    fn running_task_gets_its_role_session_named_after_the_task() {
        let (ecs, iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "abc123", Some(ROLE));
        let creds = endpoint.credentials("abc123").expect("credentials");
        assert_eq!(creds.role_arn, ROLE);
        assert_eq!(
            creds.assumed_role_arn,
            "arn:aws:sts::123456789012:assumed-role/app-task-role/abc123"
        );
        assert!(creds.access_key_id.starts_with("FSIA"), "{creds:?}");
        let resolved = IamCredentialResolver::new(iam.clone())
            .resolve(&creds.access_key_id)
            .expect("registered");
        assert_eq!(resolved.secret_access_key, creds.secret_access_key);
        assert_eq!(resolved.principal.arn, creds.assumed_role_arn);

        // Refetches within the validity window reuse the same set.
        let again = endpoint.credentials("abc123").unwrap();
        assert_eq!(again.access_key_id, creds.access_key_id);
        let json = again.to_container_json();
        for field in [
            "AccessKeyId",
            "SecretAccessKey",
            "Token",
            "Expiration",
            "RoleArn",
        ] {
            assert!(json.get(field).is_some(), "missing {field}: {json}");
        }
    }

    #[test]
    fn stopped_task_is_revoked_and_refused() {
        let (ecs, iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "t1", Some(ROLE));
        add_task(&ecs, ACCOUNT, "t2", Some(ROLE));
        let c1 = endpoint.credentials("t1").unwrap();
        let c2 = endpoint.credentials("t2").unwrap();
        assert_ne!(c1.access_key_id, c2.access_key_id);

        stop_task(&ecs, ACCOUNT, "t1");
        endpoint.revoke_stopped();
        assert!(
            !resolves(&iam, &c1),
            "stopped task's creds still registered"
        );
        assert!(resolves(&iam, &c2), "running task's creds were revoked");
        assert_eq!(
            endpoint.credentials("t1").unwrap_err(),
            CredentialsError::NotFound
        );

        // A task removed from state (e.g. an ECS reset) is revoked too.
        ecs.write().get_or_create(ACCOUNT).tasks.clear();
        endpoint.revoke_stopped();
        assert!(!resolves(&iam, &c2));
    }

    #[test]
    fn roleless_unknown_and_empty_ids_are_refused_like_the_agent() {
        let (ecs, _iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "norole", None);
        assert_eq!(
            endpoint.credentials("norole").unwrap_err(),
            CredentialsError::NotFound
        );
        assert_eq!(
            endpoint.credentials("missing").unwrap_err(),
            CredentialsError::NotFound
        );
        assert_eq!(
            endpoint.credentials("").unwrap_err(),
            CredentialsError::NoId
        );
    }

    #[tokio::test]
    async fn error_body_matches_the_agent() {
        let (_ecs, _iam, endpoint) = setup();
        let resp = endpoint.respond("missing", None);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "InvalidIdInRequest",
                "message": "CredentialsV2Request: Credentials not found",
                "HTTPErrorCode": 400,
            })
        );
    }

    #[test]
    fn iam_reset_under_the_cache_mints_fresh_registered_creds() {
        let (ecs, iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "t", Some(ROLE));
        let before = endpoint.credentials("t").unwrap();
        iam.write().reset();
        let after = endpoint.credentials("t").unwrap();
        assert_ne!(before.access_key_id, after.access_key_id);
        assert!(resolves(&iam, &after));
    }

    #[test]
    fn task_in_another_account_mints_there() {
        let (ecs, iam, endpoint) = setup();
        let role = "arn:aws:iam::222222222222:role/other";
        add_task(&ecs, "222222222222", "x", Some(role));
        let creds = endpoint.credentials("x").unwrap();
        assert_eq!(
            creds.assumed_role_arn,
            "arn:aws:sts::222222222222:assumed-role/other/x"
        );
        assert!(resolves(&iam, &creds));
    }

    #[test]
    fn sessions_of_tasks_stopped_by_a_restart_are_revoked_on_startup() {
        let (ecs, iam, before_restart) = setup();
        add_task(&ecs, ACCOUNT, "t", Some(ROLE));
        add_task(&ecs, ACCOUNT, "still", Some(ROLE));
        let stale = before_restart.credentials("t").unwrap();
        let live = before_restart.credentials("still").unwrap();
        drop(before_restart);
        // The restart stops the restored task; IAM state kept its session.
        stop_task(&ecs, ACCOUNT, "t");
        let _after_restart = EcsTaskCredentials::new(ecs.clone(), iam.clone(), ACCOUNT);
        assert!(
            !resolves(&iam, &stale),
            "persisted session survived restart"
        );
        assert!(resolves(&iam, &live));
    }

    #[test]
    fn short_role_name_resolves_in_the_tasks_account() {
        let (ecs, iam, endpoint) = setup();
        add_task(&ecs, "222222222222", "x", Some("app-role"));
        let creds = endpoint.credentials("x").unwrap();
        assert_eq!(creds.role_arn, "arn:aws:iam::222222222222:role/app-role");
        assert_eq!(
            creds.assumed_role_arn,
            "arn:aws:sts::222222222222:assumed-role/app-role/x"
        );
        assert!(resolves(&iam, &creds));

        // And its persisted session is found there after a restart.
        stop_task(&ecs, "222222222222", "x");
        let _after_restart = EcsTaskCredentials::new(ecs.clone(), iam.clone(), ACCOUNT);
        assert!(!resolves(&iam, &creds));
    }

    #[tokio::test]
    async fn strict_mode_requires_the_tasks_token_on_the_full_uri_only() {
        let (ecs, iam, _) = setup();
        let endpoint = EcsTaskCredentials::with_authorization(ecs.clone(), iam, ACCOUNT, true);
        add_task(&ecs, ACCOUNT, "t", Some(ROLE));
        add_task(&ecs, ACCOUNT, "other", Some(ROLE));
        let token = fakecloud_ecs::runtime::task_credentials_token("t");

        // No token, or another task's token: refused before any mint.
        for auth in [
            None,
            Some("wrong".to_string()),
            Some(fakecloud_ecs::runtime::task_credentials_token("other")),
        ] {
            let resp = endpoint.respond("t", auth.as_deref());
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{auth:?}");
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["code"], "AccessDenied");
            assert_eq!(v["HTTPErrorCode"], 401);
        }
        // The task's own token: served.
        assert_eq!(endpoint.respond("t", Some(&token)).status(), StatusCode::OK);

        // The agent's relative-URI surface takes no token, as on ECS: it is
        // only reachable from the task's own network namespace.
        let app = app(endpoint);
        use tower::ServiceExt;
        let mut req = axum::http::Request::builder()
            .uri("/v2/credentials/t")
            .header(axum::http::header::HOST, LINK_LOCAL_HOST)
            .body(axum::body::Body::empty())
            .unwrap();
        req.extensions_mut().insert(axum::extract::ConnectInfo(
            "172.17.0.2:40000".parse::<std::net::SocketAddr>().unwrap(),
        ));
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn strict_mode_serves_the_agent_surface_only_to_container_networks() {
        let (ecs, iam, default) = setup();
        let strict = EcsTaskCredentials::with_authorization(ecs, iam, ACCOUNT, true);
        let ip = |s: &str| Some(s.parse::<std::net::IpAddr>().unwrap());
        for allowed in [
            "172.17.0.2",
            "172.31.255.1",
            "10.88.0.5",
            "10.244.1.7",
            "192.168.65.3",
            "100.64.0.9",
            "fd00::5",
            "fe80::1",
            "::ffff:172.18.0.4",
        ] {
            assert!(strict.relative_uri_source_allowed(ip(allowed)), "{allowed}");
        }
        for refused in [
            "127.0.0.1",
            "::1",
            "8.8.8.8",
            "172.32.0.1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                !strict.relative_uri_source_allowed(ip(refused)),
                "{refused}"
            );
        }
        assert!(!strict.relative_uri_source_allowed(None));
        // Outside strict mode every peer is served, as before.
        assert!(default.relative_uri_source_allowed(ip("127.0.0.1")));
        assert!(default.relative_uri_source_allowed(None));
    }

    #[tokio::test]
    async fn strict_middleware_refuses_host_loopback_and_serves_a_container_peer() {
        use tower::ServiceExt;
        let (ecs, iam, _) = setup();
        let endpoint = EcsTaskCredentials::with_authorization(ecs.clone(), iam, ACCOUNT, true);
        add_task(&ecs, ACCOUNT, "t", Some(ROLE));
        let app = app(endpoint);
        let req = |peer: &str| {
            let mut r = axum::http::Request::builder()
                .uri("/v2/credentials/t")
                .header(axum::http::header::HOST, LINK_LOCAL_HOST)
                .body(axum::body::Body::empty())
                .unwrap();
            r.extensions_mut().insert(axum::extract::ConnectInfo(
                peer.parse::<std::net::SocketAddr>().unwrap(),
            ));
            r
        };
        let resp = app.clone().oneshot(req("127.0.0.1:5555")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let resp = app.clone().oneshot(req("172.17.0.2:5555")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn token_not_required_by_default() {
        let (ecs, _iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "t", Some(ROLE));
        assert_eq!(endpoint.respond("t", None).status(), StatusCode::OK);
    }

    /// A router whose own routes answer `fallthrough`, behind the link-local
    /// middleware, so tests see which requests the middleware claims.
    fn app(endpoint: Arc<EcsTaskCredentials>) -> axum::Router {
        axum::Router::new()
            .fallback(|| async { "fallthrough" })
            .layer(axum::middleware::from_fn_with_state(
                endpoint,
                link_local_host_middleware,
            ))
    }

    async fn call(
        app: &axum::Router,
        method: &str,
        host: &str,
        path: &str,
    ) -> (StatusCode, String) {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(axum::http::header::HOST, host)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn link_local_host_serves_the_agents_v2_credentials_path() {
        let (ecs, _iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "task1", Some(ROLE));
        let app = app(endpoint);

        for host in ["169.254.170.2", "169.254.170.2:80"] {
            let (status, body) = call(&app, "GET", host, "/v2/credentials/task1").await;
            assert_eq!(status, StatusCode::OK, "{host}: {body}");
            let json: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(json["RoleArn"], ROLE);
            assert!(json["AccessKeyId"].is_string(), "{json}");
        }

        let (status, body) = call(&app, "GET", "169.254.170.2", "/v2/credentials/nope").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("InvalidIdInRequest"), "{body}");
        let (status, body) = call(&app, "GET", "169.254.170.2", "/v2/credentials/").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("NoIdInRequest"), "{body}");
        let (status, _) = call(&app, "GET", "169.254.170.2", "/v2/credentialsX").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // Only the agent surface lives on the link-local address.
        let (status, _) = call(&app, "GET", "169.254.170.2", "/").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = call(&app, "POST", "169.254.170.2", "/v2/credentials/task1").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        // HEAD: GET's status, no body.
        let (status, body) = call(&app, "HEAD", "169.254.170.2", "/v2/credentials/task1").await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
        // GET's headers carry no Content-Length to go stale (hyper computes
        // it from the body it actually sends).
        let creds = {
            let (ecs, _iam, endpoint) = setup();
            add_task(&ecs, ACCOUNT, "t", Some(ROLE));
            endpoint
        };
        let head = respond_link_local(&creds, &axum::http::Method::HEAD, "/v2/credentials/t");
        assert!(head
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .is_none());
        let (status, body) = call(&app, "HEAD", "169.254.170.2", "/v2/credentials/nope").await;
        assert_eq!((status, body.as_str()), (StatusCode::BAD_REQUEST, ""));
    }

    #[tokio::test]
    async fn other_hosts_fall_through_to_the_app() {
        let (ecs, _iam, endpoint) = setup();
        add_task(&ecs, ACCOUNT, "task1", Some(ROLE));
        let app = app(endpoint);
        for host in [
            "localhost:4566",
            "169.254.170.2:4566",
            "169.254.170.20",
            "v2.s3.localhost",
        ] {
            let (status, body) = call(&app, "GET", host, "/v2/credentials/task1").await;
            assert_eq!(
                (status, body.as_str()),
                (StatusCode::OK, "fallthrough"),
                "{host}"
            );
        }
    }
}
