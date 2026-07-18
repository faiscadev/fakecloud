//! EC2 Instance Metadata Service (IMDS) emulation on `/latest/*`.
//!
//! An app that reads credentials from IMDS (the EC2 instance-metadata service
//! at `http://169.254.169.254`) rather than through the SDK's container path can
//! run unmodified against fakecloud by pointing the SDK's IMDS client at the
//! fakecloud server: `AWS_EC2_METADATA_SERVICE_ENDPOINT=http://<host>:<port>/`.
//! The default credential chain then resolves via IMDS with no static keys.
//!
//! Both IMDSv1 (unauthenticated GET) and IMDSv2 (PUT a token, then GET with the
//! `X-aws-ec2-metadata-token` header) are supported. fakecloud does not enforce
//! the token -- it hands one out and accepts requests with or without it -- so
//! either SDK mode works.
//!
//! Credentials come from the same [`ContainerCredentialCache`] the
//! `/_fakecloud/credentials` endpoint uses, so both surfaces vend consistent,
//! IAM-registered credentials that verify under `--verify-sigv4`.
//!
//! ## Coexisting with an S3 bucket named `latest`
//!
//! `/latest/*` is served by a single catch-all that runs ahead of the S3/AWS
//! dispatch fallback. A path-style S3 request to a bucket literally named
//! `latest` (a legal bucket name) also lands on `/latest/<key>`. Two things keep
//! IMDS from shadowing it: any request that looks like S3 -- carrying an
//! `Authorization` header, `X-Amz-*` signing headers, or a presigned-URL
//! `X-Amz-Signature` query parameter -- is forwarded straight to the dispatcher;
//! and any `/latest/*` path that is not a recognized IMDS path also falls
//! through to the dispatcher. Real IMDS clients send none of those signing
//! markers and only hit the known metadata paths. The sole residual gap is an
//! *unsigned* (anonymous, non-presigned) S3 request whose key is exactly one of
//! the handful of IMDS paths (`api/token`, `meta-data/instance-id`, …) on a
//! bucket named `latest` -- vanishingly rare.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{ConnectInfo, Extension, Query, State},
    http::{header, HeaderMap, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_iam::sts_service::assumed_role_name;
use fakecloud_iam::sts_service::container_creds::{
    partition_of, ContainerCredentialCache, ContainerCredentials,
    DEFAULT_CONTAINER_CREDENTIALS_DURATION,
};
use fakecloud_iam::SharedIamState;

/// Valid IMDSv2 token TTL range (seconds), matching AWS.
const TOKEN_TTL_RANGE: std::ops::RangeInclusive<u32> = 1..=21600;

/// Shared state for the IMDS handlers.
#[derive(Clone)]
pub struct ImdsContext {
    pub iam: SharedIamState,
    pub cache: Arc<ContainerCredentialCache>,
    pub account_id: String,
    pub region: String,
    /// The IAM role ARN the instance profile represents.
    pub role_arn: String,
    /// The synthetic instance ID reported by the metadata service.
    pub instance_id: String,
}

impl ImdsContext {
    /// The role name (final ARN segment) advertised under
    /// `iam/security-credentials/`, matching what `AssumeRole` names the role.
    fn role_name(&self) -> &str {
        assumed_role_name(&self.role_arn)
    }

    /// The availability zone (region + `a`), e.g. `us-east-1a`.
    fn availability_zone(&self) -> String {
        format!("{}a", self.region)
    }

    /// Partition derived from the role ARN, so the instance-profile ARN matches
    /// the partition of the credentials' assumed-role principal.
    fn partition(&self) -> &str {
        partition_of(&self.role_arn)
    }

    /// Mint (or reuse cached) credentials for the instance role. Shared by the
    /// IMDS `security-credentials` path and the link-local ECS `/creds` surface,
    /// so every credential surface vends the same IAM-registered creds.
    pub(crate) fn credentials(&self) -> ContainerCredentials {
        self.cache.get_or_mint(
            &self.iam,
            &self.account_id,
            &self.role_arn,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        )
    }
}

/// Build the `/latest/*` IMDS router. A single catch-all keeps IMDS from
/// shadowing a path-style S3 bucket named `latest` (see module docs). The
/// context is shared behind an `Arc` so axum's per-request `State` clone is a
/// refcount bump, not a deep copy of the string fields.
pub fn routes(ctx: Arc<ImdsContext>) -> Router {
    Router::new()
        .route("/latest/{*rest}", any(handle))
        .with_state(ctx)
}

/// Build an IMDS-only `/latest/*` router for the link-local listener
/// (`169.254.169.254`). Unlike [`routes`], a non-IMDS path returns 404 rather
/// than falling through to the full AWS dispatcher -- the link-local address
/// exposes *only* the metadata surface, never the rest of the app.
pub fn link_local_router(ctx: Arc<ImdsContext>) -> Router {
    Router::new()
        .route("/latest/{*rest}", any(link_local_handle))
        .with_state(ctx)
}

/// The IMDS metadata paths, shared by the main and link-local handlers. Returns
/// `None` for a path that is not a recognized IMDS lookup.
fn serve_imds(ctx: &ImdsContext, req: &Request<Body>) -> Option<Response> {
    let creds_prefix = "/latest/meta-data/iam/security-credentials/";
    match (req.method(), req.uri().path()) {
        (&Method::PUT, "/latest/api/token") => Some(token(req.headers())),
        (&Method::GET, "/latest/meta-data/iam/security-credentials")
        | (&Method::GET, "/latest/meta-data/iam/security-credentials/") => {
            Some(text(ctx.role_name().to_string()))
        }
        (&Method::GET, p) if p.starts_with(creds_prefix) => {
            Some(security_credentials(ctx, &p[creds_prefix.len()..]))
        }
        (&Method::GET, "/latest/meta-data/iam/info") => Some(iam_info(ctx)),
        (&Method::GET, "/latest/dynamic/instance-identity/document") => {
            Some(identity_document(ctx))
        }
        (&Method::GET, "/latest/meta-data/instance-id") => Some(text(ctx.instance_id.clone())),
        (&Method::GET, "/latest/meta-data/placement/region") => Some(text(ctx.region.clone())),
        (&Method::GET, "/latest/meta-data/placement/availability-zone") => {
            Some(text(ctx.availability_zone()))
        }
        _ => None,
    }
}

/// Link-local handler: serve IMDS or 404 (no dispatch fallback -- the link-local
/// address must not expose the rest of the app).
async fn link_local_handle(State(ctx): State<Arc<ImdsContext>>, req: Request<Body>) -> Response {
    serve_imds(&ctx, &req).unwrap_or_else(|| (StatusCode::NOT_FOUND, "").into_response())
}

/// True if the request carries any AWS signing marker (a signed S3 request to a
/// `latest`-named bucket), so it must go to the dispatcher rather than IMDS.
/// Covers header auth (SigV4 `AWS4-…` / legacy SigV2 `AWS …` via the presence of
/// `Authorization`, plus `X-Amz-*` headers) and presigned URLs (SigV4 in the
/// query string). Real IMDS clients send none of these.
fn is_aws_signed(headers: &HeaderMap, query: &HashMap<String, String>) -> bool {
    headers.contains_key(header::AUTHORIZATION)
        || headers.contains_key("x-amz-date")
        || headers.contains_key("x-amz-content-sha256")
        || query.contains_key("X-Amz-Signature")
        || query.contains_key("X-Amz-Algorithm")
}

/// Catch-all for `/latest/*`: serve IMDS for genuine metadata lookups, and
/// forward everything else (signed S3, presigned S3, or any non-IMDS path) to
/// the normal dispatcher so a bucket named `latest` still works.
async fn handle(
    State(ctx): State<Arc<ImdsContext>>,
    connect: ConnectInfo<SocketAddr>,
    registry: Extension<Arc<ServiceRegistry>>,
    config: Extension<Arc<DispatchConfig>>,
    Query(query): Query<HashMap<String, String>>,
    req: Request<Body>,
) -> Response {
    // Serve IMDS only for an unsigned request on a recognized metadata path;
    // anything else (signed/presigned S3, or a non-IMDS `/latest/*` key) goes to
    // the dispatcher so a bucket named `latest` still works.
    if !is_aws_signed(req.headers(), &query) {
        if let Some(resp) = serve_imds(&ctx, &req) {
            return resp;
        }
    }
    dispatch::dispatch(connect, registry, config, Query(query), req).await
}

/// IMDSv2 session token. AWS requires `X-aws-ec2-metadata-token-ttl-seconds` in
/// `1..=21600` and answers 400 otherwise; the SDK echoes the token back on later
/// GETs. fakecloud does not validate the token itself, so any opaque value works.
fn token(req_headers: &HeaderMap) -> Response {
    let ttl = req_headers.get("x-aws-ec2-metadata-token-ttl-seconds");
    let ttl_ok = ttl
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok())
        .is_some_and(|n| TOKEN_TTL_RANGE.contains(&n));
    if !ttl_ok {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            "",
        )
            .into_response();
    }
    let tok = format!("fc{}", uuid::Uuid::new_v4().simple());
    let mut resp = tok.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain"),
    );
    if let Some(ttl) = ttl.cloned() {
        resp.headers_mut().insert(
            header::HeaderName::from_static("x-aws-ec2-metadata-token-ttl-seconds"),
            ttl,
        );
    }
    resp
}

/// Vend credentials for the instance role in the IMDS JSON shape. AWS returns
/// 404 for a role that is not the one attached, so only the advertised role
/// name is served.
fn security_credentials(ctx: &ImdsContext, role: &str) -> Response {
    if role != ctx.role_name() {
        return (StatusCode::NOT_FOUND, "").into_response();
    }
    let creds = ctx.credentials();
    Json(serde_json::json!({
        "Code": "Success",
        "LastUpdated": creds.issued_at_iso8601(),
        "Type": "AWS-HMAC",
        "AccessKeyId": creds.access_key_id,
        "SecretAccessKey": creds.secret_access_key,
        "Token": creds.session_token,
        "Expiration": creds.expiration_iso8601(),
    }))
    .into_response()
}

/// `iam/info` -- the instance profile association.
fn iam_info(ctx: &ImdsContext) -> Response {
    let creds = ctx.credentials();
    Json(serde_json::json!({
        "Code": "Success",
        "LastUpdated": creds.issued_at_iso8601(),
        "InstanceProfileArn": format!(
            "arn:{}:iam::{}:instance-profile/{}",
            ctx.partition(), ctx.account_id, ctx.role_name()
        ),
        "InstanceProfileId": "AIPAFAKECLOUDINSTPROF0",
    }))
    .into_response()
}

/// The instance identity document.
fn identity_document(ctx: &ImdsContext) -> Response {
    Json(serde_json::json!({
        "accountId": ctx.account_id,
        "architecture": "x86_64",
        "availabilityZone": ctx.availability_zone(),
        "imageId": "ami-0fakecloud00000000",
        "instanceId": ctx.instance_id,
        "instanceType": "t3.micro",
        "privateIp": "10.0.0.10",
        "region": ctx.region,
        "pendingTime": "2020-01-01T00:00:00Z",
        "version": "2017-09-30",
    }))
    .into_response()
}

fn text(body: String) -> Response {
    ([(header::CONTENT_TYPE, "text/plain")], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn ctx(region: &str, role_arn: &str) -> ImdsContext {
        ImdsContext {
            iam: std::sync::Arc::new(RwLock::new(MultiAccountState::new(
                "123456789012",
                region,
                "",
            ))),
            cache: Arc::new(ContainerCredentialCache::new()),
            account_id: "123456789012".to_string(),
            region: region.to_string(),
            role_arn: role_arn.to_string(),
            instance_id: "i-0123456789abcdef0".to_string(),
        }
    }

    #[test]
    fn role_name_drops_path() {
        let c = ctx("us-east-1", "arn:aws:iam::123456789012:role/team/app-role");
        assert_eq!(c.role_name(), "app-role");
    }

    #[test]
    fn availability_zone_appends_a() {
        assert_eq!(ctx("us-east-1", "x").availability_zone(), "us-east-1a");
        assert_eq!(ctx("eu-west-2", "x").availability_zone(), "eu-west-2a");
    }

    #[test]
    fn partition_follows_role_arn() {
        assert_eq!(
            ctx("us-east-1", "arn:aws:iam::123456789012:role/x").partition(),
            "aws"
        );
        assert_eq!(
            ctx("cn-north-1", "arn:aws-cn:iam::123456789012:role/x").partition(),
            "aws-cn"
        );
        assert_eq!(
            ctx("us-gov-west-1", "arn:aws-us-gov:iam::123456789012:role/x").partition(),
            "aws-us-gov"
        );
    }

    #[test]
    fn signed_requests_are_detected() {
        let empty = HashMap::new();
        // Plain unsigned request -> IMDS.
        assert!(!is_aws_signed(&HeaderMap::new(), &empty));

        // SigV4 header auth.
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            "AWS4-HMAC-SHA256 Credential=...".parse().unwrap(),
        );
        assert!(is_aws_signed(&h, &empty));

        // Legacy SigV2 header auth (any Authorization value counts).
        let mut h2 = HeaderMap::new();
        h2.insert(header::AUTHORIZATION, "AWS AKID:sig".parse().unwrap());
        assert!(is_aws_signed(&h2, &empty));

        // X-Amz-* signing header.
        let mut h3 = HeaderMap::new();
        h3.insert("x-amz-content-sha256", "abc".parse().unwrap());
        assert!(is_aws_signed(&h3, &empty));

        // Presigned URL (SigV4 in the query string, no signing headers).
        let mut q = HashMap::new();
        q.insert("X-Amz-Signature".to_string(), "deadbeef".to_string());
        assert!(is_aws_signed(&HeaderMap::new(), &q));
    }
}
