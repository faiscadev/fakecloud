// Operation handlers for AWS CodeArtifact (included into `service.rs`).
//
// Every handler reads query parameters from the raw query string (so repeated
// multi-value keys survive) and body fields from the parsed JSON body, then
// mutates or reads the account-partitioned state and returns response-shaped
// JSON. Timestamps are emitted as epoch-second numbers, matching restJson1.

use base64::Engine as _;
use chrono::{DateTime, Utc};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use fakecloud_core::pagination::paginate_checked;

// ------------------------------------------------------------------ helpers

const SEP: char = '\u{1}';

fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

/// First value for a query key, percent-decoded. Reads the raw query string so
/// repeated keys never collapse.
fn q(req: &AwsRequest, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    req.raw_query
        .split('&')
        .find(|pair| pair.strip_prefix(&prefix).is_some() || *pair == key)
        .map(|pair| match pair.strip_prefix(&prefix) {
            Some(v) => decode(v),
            None => String::new(),
        })
}

/// Required query parameter, else a `ValidationException`.
fn req_q(req: &AwsRequest, key: &str) -> Result<String, AwsServiceError> {
    match q(req, key) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(validation(format!("{key} is required"))),
    }
}

/// Validate the optional `domain-owner` query parameter (a 12-digit account
/// id), matching the model `@pattern`. A malformed value is a
/// `ValidationException`, as on AWS.
fn check_domain_owner(req: &AwsRequest) -> Result<(), AwsServiceError> {
    match q(req, "domain-owner") {
        Some(v) if !v.is_empty() && !crate::validate::is_account_id(&v) => {
            Err(validation(format!("Invalid domainOwner: {v}")))
        }
        _ => Ok(()),
    }
}

/// Enforce a string `@length` bound, as `ValidationException` on violation.
fn check_len(field: &str, s: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = s.chars().count();
    if len < min || len > max {
        return Err(validation(format!(
            "{field} must be between {min} and {max} characters"
        )));
    }
    Ok(())
}

/// Enforce the `maxResults` range (1..=1000) from either the `max-results`
/// query parameter or the `maxResults` body field.
fn check_max_results(req: &AwsRequest) -> Result<(), AwsServiceError> {
    if let Some(n) = q(req, "max-results").and_then(|s| s.parse::<i64>().ok()) {
        if !(1..=1000).contains(&n) {
            return Err(validation("maxResults must be between 1 and 1000"));
        }
    }
    if let Some(n) = body(req).get("maxResults").and_then(|v| v.as_i64()) {
        if !(1..=1000).contains(&n) {
            return Err(validation("maxResults must be between 1 and 1000"));
        }
    }
    Ok(())
}

/// Validate an `Arn` value against the model `@length` (1..=1011) and
/// non-whitespace `@pattern`.
fn check_arn(arn: &str) -> Result<(), AwsServiceError> {
    if arn.is_empty() || arn.chars().count() > 1011 || arn.chars().any(char::is_whitespace) {
        return Err(validation("Invalid resource ARN"));
    }
    Ok(())
}

fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn body_str(b: &Value, key: &str) -> Option<String> {
    b.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn req_body_str(b: &Value, key: &str) -> Result<String, AwsServiceError> {
    body_str(b, key).ok_or_else(|| validation(format!("{key} is required")))
}

fn ts(dt: DateTime<Utc>) -> Value {
    let secs = dt.timestamp() as f64 + dt.timestamp_subsec_millis() as f64 / 1000.0;
    json!(secs)
}

fn aws_err(code: &str, status: StatusCode, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, msg)
}

fn validation(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ValidationException", StatusCode::BAD_REQUEST, msg)
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ResourceNotFoundException", StatusCode::NOT_FOUND, msg)
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ConflictException", StatusCode::CONFLICT, msg)
}

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn revision() -> String {
    // A base64-ish opaque revision, matching AWS's style.
    base64::engine::general_purpose::STANDARD.encode(uuid::Uuid::new_v4().as_bytes())
}

fn domain_arn(region: &str, owner: &str, name: &str) -> String {
    format!("arn:aws:codeartifact:{region}:{owner}:domain/{name}")
}

fn repo_arn(region: &str, owner: &str, domain: &str, repo: &str) -> String {
    format!("arn:aws:codeartifact:{region}:{owner}:repository/{domain}/{repo}")
}

fn package_group_arn(region: &str, owner: &str, domain: &str, pattern: &str) -> String {
    format!("arn:aws:codeartifact:{region}:{owner}:package-group/{domain}{pattern}")
}

/// Build the repository endpoint URL a package manager is pointed at. Prefer the
/// emulator host from the request `Host` header so a client configured with the
/// returned URL talks to fakecloud rather than real AWS; fall back to the
/// AWS-format host only when no `Host` header is present. The URL path
/// (`/{format}/{repo}/`) stays AWS-accurate either way.
fn repo_endpoint(
    req: &AwsRequest,
    region: &str,
    owner: &str,
    domain: &str,
    repo: &str,
    format: &str,
) -> String {
    if let Some(host) = req
        .headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
    {
        let scheme = if req
            .headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("https"))
            .unwrap_or(false)
        {
            "https"
        } else {
            "http"
        };
        return format!("{scheme}://{host}/{format}/{repo}/");
    }
    format!("https://{domain}-{owner}.d.codeartifact.{region}.amazonaws.com/{format}/{repo}/")
}

fn pkg_key(domain: &str, repo: &str, format: &str, namespace: &str, package: &str) -> String {
    format!("{domain}{SEP}{repo}{SEP}{format}{SEP}{namespace}{SEP}{package}")
}

fn version_key(pkgkey: &str, version: &str) -> String {
    format!("{pkgkey}{SEP}{version}")
}

fn asset_key(verkey: &str, asset: &str) -> String {
    format!("{verkey}{SEP}{asset}")
}

/// Map an external-connection name (`public:npmjs`) to a package format.
fn connection_format(name: &str) -> &'static str {
    let n = name.to_ascii_lowercase();
    if n.contains("npm") {
        "npm"
    } else if n.contains("pypi") {
        "pypi"
    } else if n.contains("maven") {
        "maven"
    } else if n.contains("nuget") {
        "nuget"
    } else if n.contains("ruby") || n.contains("gems") {
        "ruby"
    } else if n.contains("crates") || n.contains("cargo") {
        "cargo"
    } else if n.contains("swift") {
        "swift"
    } else {
        "generic"
    }
}

/// Parse a tags list from either `[{key,value}]` (CodeArtifact wire form).
fn parse_tags(v: Option<&Value>) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(Value::Array(arr)) = v {
        for t in arr {
            let key = t.get("key").and_then(|k| k.as_str()).unwrap_or("");
            let val = t.get("value").and_then(|k| k.as_str()).unwrap_or("");
            if !key.is_empty() {
                out.push(json!({"key": key, "value": val}));
            }
        }
    }
    out
}

fn max_results(req: &AwsRequest) -> Option<usize> {
    q(req, "max-results")
        .or_else(|| q(req, "max_results"))
        .and_then(|s| s.parse::<usize>().ok())
        .or_else(|| body(req).get("maxResults").and_then(|v| v.as_u64()).map(|n| n as usize))
}

fn next_token(req: &AwsRequest) -> Option<String> {
    q(req, "next-token").or_else(|| body(req).get("nextToken").and_then(|v| v.as_str()).map(str::to_string))
}

/// Paginate a list of values into a `(page, Option<nextToken>)`.
fn page(
    items: Vec<Value>,
    req: &AwsRequest,
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    paginate_checked(
        &items,
        next_token(req).as_deref(),
        max_results(req).unwrap_or(1000),
    )
    .map_err(|_| validation("Invalid nextToken"))
}

fn validate_format(fmt: &str) -> Result<(), AwsServiceError> {
    if crate::validate::is_enum(crate::validate::PACKAGE_FORMAT, fmt) {
        Ok(())
    } else {
        Err(validation(format!("Invalid package format: {fmt}")))
    }
}

impl CodeArtifactService {
    // -------------------------------------------------------------- domains

    fn create_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req_q(req, "domain")?;
        if !crate::validate::is_domain_name(&name) {
            return Err(validation(format!("Invalid domain name: {name}")));
        }
        let b = body(req);
        let owner = req.account_id.clone();
        let region = req.region.clone();
        let arn = domain_arn(&region, &owner, &name);
        let encryption_key = body_str(&b, "encryptionKey")
            .unwrap_or_else(|| format!("arn:aws:kms:{region}:{owner}:key/{}", uuid::Uuid::new_v4()));
        let now = Utc::now();
        let desc = json!({
            "name": name,
            "owner": owner,
            "arn": arn,
            "status": "Active",
            "createdTime": ts(now),
            "encryptionKey": encryption_key,
            "repositoryCount": 0,
            "assetSizeBytes": 0,
            "s3BucketArn": format!("arn:aws:s3:::assets-{owner}-{region}"),
        });
        let tags = parse_tags(b.get("tags"));
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if acct.domains.contains_key(&name) {
            return Err(conflict(format!("Domain {name} already exists")));
        }
        acct.domains.insert(name.clone(), desc.clone());
        acct.domain_order.push(name);
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }
        ok(json!({ "domain": desc }))
    }

    fn describe_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        let name = req_q(req, "domain")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.domains.get(&name) {
            Some(d) => ok(json!({ "domain": d })),
            None => Err(not_found(format!("Domain {name} does not exist"))),
        }
    }

    fn delete_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req_q(req, "domain")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.domains.remove(&name) {
            Some(d) => {
                acct.domain_order.retain(|n| n != &name);
                acct.domain_policies.remove(&name);
                ok(json!({ "domain": summarize_domain(&d) }))
            }
            None => Err(not_found(format!("Domain {name} does not exist"))),
        }
    }

    fn list_domains(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_max_results(req)?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let items: Vec<Value> = acct
            .domain_order
            .iter()
            .filter_map(|n| acct.domains.get(n))
            .map(summarize_domain)
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("domains".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn put_domain_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let name = req_body_str(&b, "domain")?;
        let document = req_body_str(&b, "policyDocument")?;
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&name) {
            return Err(not_found(format!("Domain {name} does not exist")));
        }
        let policy = json!({
            "resourceArn": domain_arn(&region, &owner, &name),
            "revision": revision(),
            "document": document,
        });
        acct.domain_policies.insert(name, policy.clone());
        ok(json!({ "policy": policy }))
    }

    fn get_domain_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req_q(req, "domain")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&name) {
            return Err(not_found(format!("Domain {name} does not exist")));
        }
        match acct.domain_policies.get(&name) {
            Some(p) => ok(json!({ "policy": p })),
            None => Err(not_found(format!("Domain {name} has no permissions policy"))),
        }
    }

    fn delete_domain_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req_q(req, "domain")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&name) {
            return Err(not_found(format!("Domain {name} does not exist")));
        }
        match acct.domain_policies.remove(&name) {
            Some(p) => ok(json!({ "policy": p })),
            None => Err(not_found(format!("Domain {name} has no permissions policy"))),
        }
    }

    // --------------------------------------------------------- repositories

    fn create_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        if !crate::validate::is_repository_name(&repo) {
            return Err(validation(format!("Invalid repository name: {repo}")));
        }
        let b = body(req);
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let key = format!("{domain}/{repo}");
        let upstreams = parse_upstreams(b.get("upstreams"));
        let desc = json!({
            "name": repo,
            "administratorAccount": owner,
            "domainName": domain,
            "domainOwner": owner,
            "arn": repo_arn(&region, &owner, &domain, &repo),
            "description": body_str(&b, "description").unwrap_or_default(),
            "upstreams": upstreams,
            "externalConnections": [],
            "createdTime": ts(Utc::now()),
        });
        let tags = parse_tags(b.get("tags"));
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        if acct.repositories.contains_key(&key) {
            return Err(conflict(format!("Repository {repo} already exists")));
        }
        acct.repositories.insert(key.clone(), desc.clone());
        acct.repository_order.push(key);
        if !tags.is_empty() {
            acct.tags
                .insert(repo_arn(&region, &owner, &domain, &repo), tags);
        }
        bump_repo_count(acct, &domain, 1);
        ok(json!({ "repository": desc }))
    }

    fn describe_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.repositories.get(&key) {
            Some(r) => ok(json!({ "repository": r })),
            None => Err(not_found(format!("Repository {repo} does not exist"))),
        }
    }

    fn update_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let b = body(req);
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.repositories.get_mut(&key) else {
            return Err(not_found(format!("Repository {repo} does not exist")));
        };
        if let Some(d) = body_str(&b, "description") {
            desc["description"] = Value::String(d);
        }
        if let Some(u) = b.get("upstreams") {
            desc["upstreams"] = parse_upstreams(Some(u));
        }
        let out = desc.clone();
        ok(json!({ "repository": out }))
    }

    fn delete_repository(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.repositories.remove(&key) {
            Some(r) => {
                acct.repository_order.retain(|k| k != &key);
                acct.repository_policies.remove(&key);
                bump_repo_count(acct, &domain, -1);
                ok(json!({ "repository": summarize_repository(&r) }))
            }
            None => Err(not_found(format!("Repository {repo} does not exist"))),
        }
    }

    fn list_repositories(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_max_results(req)?;
        let prefix = q(req, "repository-prefix").unwrap_or_default();
        if !prefix.is_empty() {
            check_len("repositoryPrefix", &prefix, 2, 100)?;
        }
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let items: Vec<Value> = acct
            .repository_order
            .iter()
            .filter_map(|k| acct.repositories.get(k))
            .filter(|r| repo_name(r).starts_with(&prefix))
            .map(summarize_repository)
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("repositories".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn list_repositories_in_domain(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        check_max_results(req)?;
        let domain = req_q(req, "domain")?;
        let prefix = q(req, "repository-prefix").unwrap_or_default();
        if !prefix.is_empty() {
            check_len("repositoryPrefix", &prefix, 2, 100)?;
        }
        let dprefix = format!("{domain}/");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        let items: Vec<Value> = acct
            .repository_order
            .iter()
            .filter(|k| k.starts_with(&dprefix))
            .filter_map(|k| acct.repositories.get(k))
            .filter(|r| repo_name(r).starts_with(&prefix))
            .map(summarize_repository)
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("repositories".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn get_repository_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let format = req_q(req, "format")?;
        validate_format(&format)?;
        if let Some(et) = q(req, "endpointType") {
            if !crate::validate::is_enum(crate::validate::ENDPOINT_TYPE, &et) {
                return Err(validation(format!("Invalid endpoint type: {et}")));
            }
        }
        let key = format!("{domain}/{repo}");
        let owner = req.account_id.clone();
        let region = req.region.clone();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        ok(json!({ "repositoryEndpoint": repo_endpoint(req, &region, &owner, &domain, &repo, &format) }))
    }

    fn associate_external_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let connection = req_q(req, "external-connection")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.repositories.get_mut(&key) else {
            return Err(not_found(format!("Repository {repo} does not exist")));
        };
        let conns = ensure_array(desc, "externalConnections");
        if conns
            .iter()
            .any(|c| c.get("externalConnectionName").and_then(|v| v.as_str()) == Some(&connection))
        {
            return Err(conflict(format!(
                "External connection {connection} already associated"
            )));
        }
        conns.push(json!({
            "externalConnectionName": connection,
            "packageFormat": connection_format(&connection),
            "status": "Available",
        }));
        let out = desc.clone();
        ok(json!({ "repository": out }))
    }

    fn disassociate_external_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let connection = req_q(req, "external-connection")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.repositories.get_mut(&key) else {
            return Err(not_found(format!("Repository {repo} does not exist")));
        };
        let conns = ensure_array(desc, "externalConnections");
        conns.retain(|c| {
            c.get("externalConnectionName").and_then(|v| v.as_str()) != Some(&connection)
        });
        let out = desc.clone();
        ok(json!({ "repository": out }))
    }

    fn get_repository_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        match acct.repository_policies.get(&key) {
            Some(p) => ok(json!({ "policy": p })),
            None => Err(not_found(format!("Repository {repo} has no permissions policy"))),
        }
    }

    fn put_repository_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let b = body(req);
        let document = req_body_str(&b, "policyDocument")?;
        let key = format!("{domain}/{repo}");
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        let policy = json!({
            "resourceArn": repo_arn(&region, &owner, &domain, &repo),
            "revision": revision(),
            "document": document,
        });
        acct.repository_policies.insert(key, policy.clone());
        ok(json!({ "policy": policy }))
    }

    fn delete_repository_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let key = format!("{domain}/{repo}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        match acct.repository_policies.remove(&key) {
            Some(p) => ok(json!({ "policy": p })),
            None => Err(not_found(format!("Repository {repo} has no permissions policy"))),
        }
    }

    // ------------------------------------------------------------- packages

    fn list_packages(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_max_results(req)?;
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let format = q(req, "format");
        let namespace = q(req, "namespace");
        let prefix = q(req, "package-prefix").unwrap_or_default();
        let key = format!("{domain}/{repo}");
        let pk_prefix = format!("{domain}{SEP}{repo}{SEP}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        let items: Vec<Value> = acct
            .package_order
            .iter()
            .filter(|k| k.starts_with(&pk_prefix))
            .filter_map(|k| acct.packages.get(k))
            .filter(|p| {
                format
                    .as_ref()
                    .is_none_or(|f| p.get("format").and_then(|v| v.as_str()) == Some(f))
            })
            .filter(|p| {
                namespace
                    .as_ref()
                    .is_none_or(|n| p.get("namespace").and_then(|v| v.as_str()) == Some(n))
            })
            .filter(|p| {
                p.get("package")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .starts_with(&prefix)
            })
            .cloned()
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("packages".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn describe_package(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let key = pkg_key(&domain, &repo, &format, &namespace, &package);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.packages.get(&key) {
            Some(p) => ok(json!({ "package": {
                "format": p.get("format"),
                "namespace": p.get("namespace"),
                "name": package,
                "originConfiguration": p.get("originConfiguration"),
            } })),
            None => Err(not_found(format!("Package {package} does not exist"))),
        }
    }

    fn delete_package(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let key = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vprefix = format!("{key}{SEP}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.packages.remove(&key) {
            Some(p) => {
                acct.package_order.retain(|k| k != &key);
                acct.package_versions.retain(|k, _| !k.starts_with(&vprefix));
                acct.assets.retain(|k, _| !k.starts_with(&vprefix));
                acct.asset_content.retain(|k, _| !k.starts_with(&vprefix));
                ok(json!({ "deletedPackage": p }))
            }
            None => Err(not_found(format!("Package {package} does not exist"))),
        }
    }

    fn put_package_origin(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let b = body(req);
        let restrictions = b.get("restrictions").cloned().ok_or_else(|| {
            validation("restrictions is required")
        })?;
        validate_origin_restrictions(&restrictions)?;
        let key = pkg_key(&domain, &repo, &format, &namespace, &package);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(p) = acct.packages.get_mut(&key) else {
            return Err(not_found(format!("Package {package} does not exist")));
        };
        p["originConfiguration"] = json!({ "restrictions": restrictions });
        ok(json!({ "originConfiguration": { "restrictions": restrictions } }))
    }

    // ------------------------------------------------------ package versions

    fn list_package_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_max_results(req)?;
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        if let Some(s) = q(req, "sortBy") {
            if !crate::validate::is_enum(crate::validate::PACKAGE_VERSION_SORT, &s) {
                return Err(validation(format!("Invalid sortBy: {s}")));
            }
        }
        let status_filter = q(req, "status");
        if let Some(s) = &status_filter {
            if !crate::validate::is_enum(crate::validate::PACKAGE_VERSION_STATUS, s) {
                return Err(validation(format!("Invalid status: {s}")));
            }
        }
        if let Some(o) = q(req, "originType") {
            if !crate::validate::is_enum(crate::validate::PACKAGE_VERSION_ORIGIN_TYPE, &o) {
                return Err(validation(format!("Invalid originType: {o}")));
            }
        }
        let key = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vprefix = format!("{key}{SEP}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.packages.contains_key(&key) {
            return Err(not_found(format!("Package {package} does not exist")));
        }
        // The default display version is a package-level attribute (the latest
        // published version), independent of the `status` filter applied to the
        // returned list. Pick the most recently published version, breaking ties
        // on the higher version string, matching AWS's "latest" semantics rather
        // than the lexicographically-smallest BTreeMap key.
        let default_display = latest_published_version(acct, &vprefix);
        let versions: Vec<Value> = acct
            .package_versions
            .iter()
            .filter(|(k, _)| k.starts_with(&vprefix))
            .map(|(_, v)| v)
            .filter(|v| {
                status_filter
                    .as_ref()
                    .is_none_or(|s| v.get("status").and_then(|x| x.as_str()) == Some(s))
            })
            .map(summarize_version)
            .collect();
        let (page_items, token) = page(versions, req)?;
        let mut out = Map::new();
        if let Some(d) = default_display {
            out.insert("defaultDisplayVersion".into(), Value::String(d));
        }
        out.insert("format".into(), Value::String(format));
        if !namespace.is_empty() {
            out.insert("namespace".into(), Value::String(namespace));
        }
        out.insert("package".into(), Value::String(package));
        out.insert("versions".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn describe_package_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let version = req_q(req, "version")?;
        let vk = version_key(&pkg_key(&domain, &repo, &format, &namespace, &package), &version);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.package_versions.get(&vk) {
            Some(v) => ok(json!({ "packageVersion": v })),
            None => Err(not_found(format!("Package version {version} does not exist"))),
        }
    }

    fn delete_package_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.mutate_versions(req, VersionOp::Delete)
    }

    fn dispose_package_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.mutate_versions(req, VersionOp::Dispose)
    }

    fn update_package_versions_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        let target = req_body_str(&b, "targetStatus")?;
        if !crate::validate::is_enum(crate::validate::PACKAGE_VERSION_STATUS, &target) {
            return Err(validation(format!("Invalid targetStatus: {target}")));
        }
        self.mutate_versions(req, VersionOp::SetStatus(target))
    }

    /// Shared engine for the batch package-version mutators.
    fn mutate_versions(
        &self,
        req: &AwsRequest,
        op: VersionOp,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let b = body(req);
        let versions: Vec<String> = b
            .get("versions")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        if versions.is_empty() {
            return Err(validation("versions is required"));
        }
        // Optimistic-concurrency preconditions. `expectedStatus` requires the
        // current status to match; `versionRevisions` maps version -> expected
        // revision. A mismatch places the version in `failedVersions` with the
        // AWS error code and leaves it unchanged.
        let expected_status = body_str(&b, "expectedStatus");
        let version_revisions = b.get("versionRevisions").and_then(|v| v.as_object()).cloned();
        let key = pkg_key(&domain, &repo, &format, &namespace, &package);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.packages.contains_key(&key) {
            return Err(not_found(format!("Package {package} does not exist")));
        }
        let mut successful = Map::new();
        let mut failed = Map::new();
        for v in versions {
            let vk = version_key(&key, &v);
            let Some(desc) = acct.package_versions.get(&vk) else {
                failed.insert(
                    v.clone(),
                    json!({
                        "errorCode": "NOT_FOUND",
                        "errorMessage": format!("Package version {v} does not exist"),
                    }),
                );
                continue;
            };
            let rev = desc
                .get("revision")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let cur_status = desc
                .get("status")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            // Precondition: expected revision.
            if let Some(expected_rev) = version_revisions
                .as_ref()
                .and_then(|m| m.get(&v))
                .and_then(|x| x.as_str())
            {
                if expected_rev != rev {
                    failed.insert(
                        v.clone(),
                        json!({
                            "errorCode": "MISMATCHED_REVISION",
                            "errorMessage": format!(
                                "Package version {v} revision does not match expected revision"
                            ),
                        }),
                    );
                    continue;
                }
            }
            // Precondition: expected status.
            if let Some(exp) = &expected_status {
                if exp != &cur_status {
                    failed.insert(
                        v.clone(),
                        json!({
                            "errorCode": "MISMATCHED_STATUS",
                            "errorMessage": format!(
                                "Package version {v} status {cur_status} does not match expected status {exp}"
                            ),
                        }),
                    );
                    continue;
                }
            }
            let status = match &op {
                VersionOp::Delete => "Deleted",
                VersionOp::Dispose => "Disposed",
                VersionOp::SetStatus(s) => s.as_str(),
            };
            let aprefix = format!("{vk}{SEP}");
            match &op {
                VersionOp::Delete => {
                    // Deleting a version removes the version record along with
                    // its assets, asset bytes, and readme, mirroring
                    // `delete_package`'s cleanup so nothing stays downloadable.
                    acct.package_versions.remove(&vk);
                    acct.assets.retain(|k, _| !k.starts_with(&aprefix));
                    acct.asset_content.retain(|k, _| !k.starts_with(&aprefix));
                    acct.readmes.remove(&vk);
                }
                VersionOp::Dispose => {
                    // Dispose keeps the version record (marked Disposed) but
                    // deletes the asset bytes so the content is no longer
                    // downloadable, matching AWS.
                    if let Some(d) = acct.package_versions.get_mut(&vk) {
                        d["status"] = Value::String(status.to_string());
                    }
                    acct.asset_content.retain(|k, _| !k.starts_with(&aprefix));
                }
                VersionOp::SetStatus(_) => {
                    if let Some(d) = acct.package_versions.get_mut(&vk) {
                        d["status"] = Value::String(status.to_string());
                    }
                }
            }
            successful.insert(v, json!({ "revision": rev, "status": status }));
        }
        ok(json!({ "successfulVersions": successful, "failedVersions": failed }))
    }

    fn copy_package_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let source = req_q(req, "source-repository")?;
        let dest = req_q(req, "destination-repository")?;
        let format = req_q(req, "format")?;
        validate_format(&format)?;
        let namespace = q(req, "namespace").unwrap_or_default();
        let package = req_q(req, "package")?;
        let b = body(req);
        let versions: Vec<String> = b
            .get("versions")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .or_else(|| {
                b.get("versionRevisions")
                    .and_then(|v| v.as_object())
                    .map(|m| m.keys().cloned().collect())
            })
            .unwrap_or_default();
        // AWS requires exactly one of `versions` or `versionRevisions`; an empty
        // selection is a ValidationException, never an implicit "copy all".
        if versions.is_empty() {
            return Err(validation(
                "Either the versions or versionRevisions parameter must be provided",
            ));
        }
        let src_key = format!("{domain}/{source}");
        let dst_key = format!("{domain}/{dest}");
        let src_pkg = pkg_key(&domain, &source, &format, &namespace, &package);
        let dst_pkg = pkg_key(&domain, &dest, &format, &namespace, &package);
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&src_key) {
            return Err(not_found(format!("Repository {source} does not exist")));
        }
        if !acct.repositories.contains_key(&dst_key) {
            return Err(not_found(format!("Repository {dest} does not exist")));
        }
        ensure_package(acct, &dst_pkg, &format, &namespace, &package);
        let mut successful = Map::new();
        let mut failed = Map::new();
        for v in versions {
            let svk = version_key(&src_pkg, &v);
            match acct.package_versions.get(&svk).cloned() {
                Some(mut desc) => {
                    desc["status"] = Value::String("Published".into());
                    let dvk = version_key(&dst_pkg, &v);
                    acct.package_versions.insert(dvk.clone(), desc.clone());
                    // Carry the version's assets, asset bytes, and readme to the
                    // destination so GetPackageVersionAsset / ListPackageVersionAssets
                    // work against the copy, not just its metadata.
                    let sprefix = format!("{svk}{SEP}");
                    let copied: Vec<(String, Value, Option<String>)> = acct
                        .assets
                        .iter()
                        .filter(|(k, _)| k.starts_with(&sprefix))
                        .map(|(k, summary)| {
                            let asset_name = &k[sprefix.len()..];
                            (
                                asset_key(&dvk, asset_name),
                                summary.clone(),
                                acct.asset_content.get(k).cloned(),
                            )
                        })
                        .collect();
                    for (dak, summary, content) in copied {
                        acct.assets.insert(dak.clone(), summary);
                        if let Some(c) = content {
                            acct.asset_content.insert(dak, c);
                        }
                    }
                    if let Some(readme) = acct.readmes.get(&svk).cloned() {
                        acct.readmes.insert(dvk, readme);
                    }
                    let rev = desc.get("revision").and_then(|x| x.as_str()).unwrap_or("");
                    successful.insert(v, json!({ "revision": rev, "status": "Published" }));
                }
                None => {
                    failed.insert(
                        v.clone(),
                        json!({
                            "errorCode": "NOT_FOUND",
                            "errorMessage": format!("Package version {v} not found in source"),
                        }),
                    );
                }
            }
        }
        let _ = (region, owner);
        ok(json!({ "successfulVersions": successful, "failedVersions": failed }))
    }

    fn publish_package_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let repo = req_q(req, "repository")?;
        let format = req_q(req, "format")?;
        validate_format(&format)?;
        let namespace = q(req, "namespace").unwrap_or_default();
        let package = req_q(req, "package")?;
        let version = req_q(req, "version")?;
        let asset_name = req_q(req, "asset")?;
        let unfinished = q(req, "unfinished").map(|s| s == "true").unwrap_or(false);
        // `assetSHA256` is a required `@httpHeader("x-amz-content-sha256")`. It
        // must be a well-formed lowercase 64-char hex digest and must match the
        // SHA-256 the server computes over the uploaded body.
        let asset_sha256 = req
            .headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| validation("assetSHA256 (x-amz-content-sha256) header is required"))?;
        if !is_sha256_hex(&asset_sha256) {
            return Err(validation(
                "assetSHA256 must be a 64-character lowercase hexadecimal SHA-256 digest",
            ));
        }
        let content = req.body.to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let sha256 = hex_encode(&hasher.finalize());
        if asset_sha256 != sha256 {
            return Err(validation(
                "The provided assetSHA256 does not match the SHA-256 of the uploaded asset content",
            ));
        }
        let key = format!("{domain}/{repo}");
        let pk = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vk = version_key(&pk, &version);
        let ak = asset_key(&vk, &asset_name);
        let size = content.len() as u64;
        let status = if unfinished { "Unfinished" } else { "Published" };
        let rev = revision();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.repositories.contains_key(&key) {
            return Err(not_found(format!("Repository {repo} does not exist")));
        }
        // Republishing an existing asset is a ConflictException unless the
        // stored version is still Unfinished (an in-progress publish that may be
        // completed/overwritten). AWS rejects overwriting a Published asset.
        if acct.assets.contains_key(&ak) {
            let overwritable = acct
                .package_versions
                .get(&vk)
                .and_then(|d| d.get("status"))
                .and_then(|s| s.as_str())
                == Some("Unfinished");
            if !overwritable {
                return Err(conflict(format!(
                    "Package version {version} asset {asset_name} already exists"
                )));
            }
        }
        ensure_package(acct, &pk, &format, &namespace, &package);
        let asset = json!({
            "name": asset_name,
            "size": size,
            "hashes": { "SHA-256": sha256 },
        });
        acct.package_versions.insert(
            vk.clone(),
            json!({
                "format": format,
                "namespace": if namespace.is_empty() { Value::Null } else { Value::String(namespace.clone()) },
                "packageName": package,
                "version": version,
                "revision": rev,
                "status": status,
                "publishedTime": ts(Utc::now()),
                "origin": { "originType": "INTERNAL" },
            }),
        );
        acct.assets.insert(ak.clone(), asset.clone());
        acct.asset_content.insert(
            ak,
            base64::engine::general_purpose::STANDARD.encode(&content),
        );
        let mut out = Map::new();
        out.insert("format".into(), Value::String(format));
        if !namespace.is_empty() {
            out.insert("namespace".into(), Value::String(namespace));
        }
        out.insert("package".into(), Value::String(package));
        out.insert("version".into(), Value::String(version));
        out.insert("versionRevision".into(), Value::String(rev));
        out.insert("status".into(), Value::String(status.into()));
        out.insert("asset".into(), asset);
        ok(Value::Object(out))
    }

    fn get_package_version_readme(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let version = req_q(req, "version")?;
        let pk = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vk = version_key(&pk, &version);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.package_versions.get(&vk) else {
            return Err(not_found(format!("Package version {version} does not exist")));
        };
        let rev = desc.get("revision").cloned().unwrap_or(Value::Null);
        let readme = acct.readmes.get(&vk).cloned().unwrap_or_default();
        let mut out = Map::new();
        out.insert("format".into(), Value::String(format));
        if !namespace.is_empty() {
            out.insert("namespace".into(), Value::String(namespace));
        }
        out.insert("package".into(), Value::String(package));
        out.insert("version".into(), Value::String(version));
        out.insert("versionRevision".into(), rev);
        out.insert("readme".into(), Value::String(readme));
        ok(Value::Object(out))
    }

    fn get_package_version_asset(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let version = req_q(req, "version")?;
        let asset_name = req_q(req, "asset")?;
        let pk = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vk = version_key(&pk, &version);
        let ak = asset_key(&vk, &asset_name);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        // The version must still exist; a Disposed (or deleted) version has had
        // its asset bytes removed, so the asset is no longer downloadable.
        let Some(desc) = acct.package_versions.get(&vk) else {
            return Err(not_found(format!("Package version {version} does not exist")));
        };
        let rev = desc
            .get("revision")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let Some(b64) = acct.asset_content.get(&ak) else {
            return Err(not_found(format!("Asset {asset_name} does not exist")));
        };
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap_or_default();
        // The `@httpPayload` blob is raw bytes, not JSON: advertise
        // application/octet-stream rather than the awsJson default.
        let mut resp = AwsResponse::json(StatusCode::OK, bytes);
        resp.content_type = "application/octet-stream".to_string();
        set_header(&mut resp, "X-AssetName", &asset_name);
        set_header(&mut resp, "X-PackageVersion", &version);
        set_header(&mut resp, "X-PackageVersionRevision", &rev);
        Ok(resp)
    }

    fn list_package_version_assets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let version = req_q(req, "version")?;
        let pk = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vk = version_key(&pk, &version);
        let aprefix = format!("{vk}{SEP}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.package_versions.get(&vk) else {
            return Err(not_found(format!("Package version {version} does not exist")));
        };
        let rev = desc.get("revision").cloned().unwrap_or(Value::Null);
        let assets: Vec<Value> = acct
            .assets
            .iter()
            .filter(|(k, _)| k.starts_with(&aprefix))
            .map(|(_, v)| v.clone())
            .collect();
        let (page_items, token) = page(assets, req)?;
        let mut out = Map::new();
        out.insert("format".into(), Value::String(format));
        if !namespace.is_empty() {
            out.insert("namespace".into(), Value::String(namespace));
        }
        out.insert("package".into(), Value::String(package));
        out.insert("version".into(), Value::String(version));
        out.insert("versionRevision".into(), rev);
        out.insert("assets".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn list_package_version_dependencies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (domain, repo, format, namespace, package) = package_coords(req)?;
        let version = req_q(req, "version")?;
        let pk = pkg_key(&domain, &repo, &format, &namespace, &package);
        let vk = version_key(&pk, &version);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(desc) = acct.package_versions.get(&vk) else {
            return Err(not_found(format!("Package version {version} does not exist")));
        };
        let rev = desc.get("revision").cloned().unwrap_or(Value::Null);
        let mut out = Map::new();
        out.insert("format".into(), Value::String(format));
        if !namespace.is_empty() {
            out.insert("namespace".into(), Value::String(namespace));
        }
        out.insert("package".into(), Value::String(package));
        out.insert("version".into(), Value::String(version));
        out.insert("versionRevision".into(), rev);
        out.insert("dependencies".into(), Value::Array(Vec::new()));
        ok(Value::Object(out))
    }

    // ------------------------------------------------------- package groups

    fn create_package_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let b = body(req);
        let pattern = req_body_str(&b, "packageGroup")?;
        let plen = pattern.chars().count();
        if !(2..=520).contains(&plen) {
            return Err(validation("Invalid package group pattern length"));
        }
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let gkey = format!("{domain}{SEP}{pattern}");
        let desc = package_group_desc(
            &region,
            &owner,
            &domain,
            &pattern,
            body_str(&b, "contactInfo"),
            body_str(&b, "description"),
        );
        let tags = parse_tags(b.get("tags"));
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        if acct.package_groups.contains_key(&gkey) {
            return Err(conflict(format!("Package group {pattern} already exists")));
        }
        acct.package_groups.insert(gkey.clone(), desc.clone());
        acct.package_group_order.push(gkey);
        if !tags.is_empty() {
            acct.tags
                .insert(package_group_arn(&region, &owner, &domain, &pattern), tags);
        }
        ok(json!({ "packageGroup": desc }))
    }

    fn describe_package_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        let domain = req_q(req, "domain")?;
        let pattern = req_q(req, "package-group")?;
        let gkey = format!("{domain}{SEP}{pattern}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.package_groups.get(&gkey) {
            Some(g) => ok(json!({ "packageGroup": g })),
            None => Err(not_found(format!("Package group {pattern} does not exist"))),
        }
    }

    fn update_package_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        let domain = req_q(req, "domain")?;
        let b = body(req);
        let pattern = req_body_str(&b, "packageGroup")?;
        if let Some(c) = body_str(&b, "contactInfo") {
            check_len("contactInfo", &c, 0, 1000)?;
        }
        if let Some(d) = body_str(&b, "description") {
            check_len("description", &d, 0, 1000)?;
        }
        let gkey = format!("{domain}{SEP}{pattern}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(g) = acct.package_groups.get_mut(&gkey) else {
            return Err(not_found(format!("Package group {pattern} does not exist")));
        };
        if let Some(c) = body_str(&b, "contactInfo") {
            g["contactInfo"] = Value::String(c);
        }
        if let Some(d) = body_str(&b, "description") {
            g["description"] = Value::String(d);
        }
        let out = g.clone();
        ok(json!({ "packageGroup": out }))
    }

    fn delete_package_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let pattern = req_q(req, "package-group")?;
        let gkey = format!("{domain}{SEP}{pattern}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        match acct.package_groups.remove(&gkey) {
            Some(g) => {
                acct.package_group_order.retain(|k| k != &gkey);
                ok(json!({ "packageGroup": g }))
            }
            None => Err(not_found(format!("Package group {pattern} does not exist"))),
        }
    }

    fn list_package_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        check_max_results(req)?;
        let domain = req_q(req, "domain")?;
        let prefix = q(req, "prefix").unwrap_or_default();
        let gprefix = format!("{domain}{SEP}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        let items: Vec<Value> = acct
            .package_group_order
            .iter()
            .filter(|k| k.starts_with(&gprefix))
            .filter_map(|k| acct.package_groups.get(k))
            .filter(|g| {
                g.get("pattern")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .starts_with(&prefix)
            })
            .cloned()
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("packageGroups".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn list_sub_package_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let pattern = req_q(req, "package-group")?;
        let gkey = format!("{domain}{SEP}{pattern}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        if !acct.package_groups.contains_key(&gkey) {
            return Err(not_found(format!("Package group {pattern} does not exist")));
        }
        // Sub-groups are groups in the same domain whose pattern strictly
        // extends the parent pattern.
        let gprefix = format!("{domain}{SEP}");
        let items: Vec<Value> = acct
            .package_group_order
            .iter()
            .filter(|k| k.starts_with(&gprefix) && *k != &gkey)
            .filter_map(|k| acct.package_groups.get(k))
            .filter(|g| {
                g.get("pattern")
                    .and_then(|v| v.as_str())
                    .is_some_and(|p| p.starts_with(&pattern) && p != pattern)
            })
            .cloned()
            .collect();
        let (page_items, token) = page(items, req)?;
        let mut out = Map::new();
        out.insert("packageGroups".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn list_allowed_repositories_for_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        check_max_results(req)?;
        let domain = req_q(req, "domain")?;
        let pattern = req_q(req, "package-group")?;
        let restriction = req_q(req, "originRestrictionType")?;
        if !crate::validate::is_enum(crate::validate::ORIGIN_RESTRICTION_TYPE, &restriction) {
            return Err(validation(format!(
                "Invalid originRestrictionType: {restriction}"
            )));
        }
        let gkey = format!("{domain}{SEP}{pattern}");
        let akey = format!("{domain}{SEP}{pattern}{SEP}{restriction}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.package_groups.contains_key(&gkey) {
            return Err(not_found(format!("Package group {pattern} does not exist")));
        }
        let repos: Vec<Value> = acct
            .package_group_allowed
            .get(&akey)
            .map(|v| v.iter().map(|r| Value::String(r.clone())).collect())
            .unwrap_or_default();
        let (page_items, token) = page(repos, req)?;
        let mut out = Map::new();
        out.insert("allowedRepositories".into(), Value::Array(page_items));
        if let Some(t) = token {
            out.insert("nextToken".into(), Value::String(t));
        }
        ok(Value::Object(out))
    }

    fn get_associated_package_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let format = req_q(req, "format")?;
        validate_format(&format)?;
        let _package = req_q(req, "package")?;
        let region = req.region.clone();
        let owner = req.account_id.clone();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        // Absent a more specific match, the root package group `/*` is the
        // strong association every package inherits.
        let root = package_group_desc(&region, &owner, &domain, "/*", None, None);
        ok(json!({ "packageGroup": root, "associationType": "STRONG" }))
    }

    fn list_associated_packages(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let _pattern = req_q(req, "package-group")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        let mut out = Map::new();
        out.insert("packages".into(), Value::Array(Vec::new()));
        ok(Value::Object(out))
    }

    fn update_package_group_origin(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_domain_owner(req)?;
        let domain = req_q(req, "domain")?;
        let pattern = req_q(req, "package-group")?;
        let b = body(req);
        let gkey = format!("{domain}{SEP}{pattern}");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.package_groups.contains_key(&gkey) {
            return Err(not_found(format!("Package group {pattern} does not exist")));
        }
        // Validate every input BEFORE mutating any state, so an invalid mode or
        // restriction type can never leave a partially-applied update. Both the
        // allowed-repository restriction types and the restriction modes must be
        // recognised enum values.
        for field in ["addAllowedRepositories", "removeAllowedRepositories"] {
            if let Some(entries) = b.get(field).and_then(|v| v.as_array()) {
                for e in entries {
                    if let Some(rtype) = e.get("originRestrictionType").and_then(|v| v.as_str()) {
                        if !crate::validate::is_enum(
                            crate::validate::ORIGIN_RESTRICTION_TYPE,
                            rtype,
                        ) {
                            return Err(validation(format!(
                                "Invalid originRestrictionType: {rtype}"
                            )));
                        }
                    }
                }
            }
        }
        if let Some(restrictions) = b.get("restrictions").and_then(|v| v.as_object()) {
            for mode in restrictions.values().filter_map(|v| v.as_str()) {
                if !crate::validate::is_enum(crate::validate::ORIGIN_RESTRICTION_MODE, mode) {
                    return Err(validation(format!("Invalid restriction mode: {mode}")));
                }
            }
        }
        // All inputs validated -- now apply allowed-repository add/remove and
        // restriction-mode changes.
        let mut updates = Map::new();
        if let Some(add) = b.get("addAllowedRepositories").and_then(|v| v.as_array()) {
            apply_allowed(acct, &domain, &pattern, add, true, &mut updates);
        }
        if let Some(rem) = b.get("removeAllowedRepositories").and_then(|v| v.as_array()) {
            apply_allowed(acct, &domain, &pattern, rem, false, &mut updates);
        }
        if let Some(restrictions) = b.get("restrictions").and_then(|v| v.as_object()) {
            if let Some(g) = acct.package_groups.get_mut(&gkey) {
                let origin = ensure_object(ensure_object(g, "originConfiguration"), "restrictions")
                    .as_object_mut()
                    .expect("value was just set to an object");
                for (rtype, mode) in restrictions {
                    if let Some(mode_s) = mode.as_str() {
                        origin.insert(
                            rtype.clone(),
                            json!({
                                "mode": mode_s,
                                "effectiveMode": mode_s,
                                "repositoriesCount": 0,
                            }),
                        );
                    }
                }
            }
        }
        let g = acct.package_groups.get(&gkey).cloned().unwrap_or(Value::Null);
        ok(json!({ "packageGroup": g, "allowedRepositoryUpdates": updates }))
    }

    // ---------------------------------------------------------- auth + tags

    fn get_authorization_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let domain = req_q(req, "domain")?;
        let duration = q(req, "duration")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(43200);
        // AWS only accepts 0 (max/12h, bound to the caller's session) or a value
        // in [900, 43200]; anything else is a ValidationException.
        if duration != 0 && !(900..=43200).contains(&duration) {
            return Err(validation(
                "durationSeconds must be 0 or between 900 and 43200 seconds",
            ));
        }
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.domains.contains_key(&domain) {
            return Err(not_found(format!("Domain {domain} does not exist")));
        }
        let token = format!(
            "eyJ2ZXIiOj{}",
            base64::engine::general_purpose::STANDARD
                .encode(uuid::Uuid::new_v4().as_bytes())
                .replace('=', "")
        );
        let expiration = if duration == 0 {
            // Duration 0 means an unbounded token bound to the caller's session.
            Utc::now() + chrono::Duration::hours(12)
        } else {
            Utc::now() + chrono::Duration::seconds(duration)
        };
        ok(json!({ "authorizationToken": token, "expiration": ts(expiration) }))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_q(req, "resourceArn")?;
        check_arn(&arn)?;
        let b = body(req);
        let new_tags = parse_tags(b.get("tags"));
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !arn_exists(acct, &arn) {
            return Err(not_found(format!("Resource {arn} does not exist")));
        }
        let entry = acct.tags.entry(arn).or_default();
        for t in new_tags {
            let key = t.get("key").and_then(|k| k.as_str()).unwrap_or("").to_string();
            entry.retain(|e| e.get("key").and_then(|k| k.as_str()) != Some(&key));
            entry.push(t);
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_q(req, "resourceArn")?;
        check_arn(&arn)?;
        let b = body(req);
        let keys: Vec<String> = b
            .get("tagKeys")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !arn_exists(acct, &arn) {
            return Err(not_found(format!("Resource {arn} does not exist")));
        }
        if let Some(entry) = acct.tags.get_mut(&arn) {
            entry.retain(|t| {
                t.get("key")
                    .and_then(|k| k.as_str())
                    .is_none_or(|k| !keys.contains(&k.to_string()))
            });
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req_q(req, "resourceArn")?;
        check_arn(&arn)?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !arn_exists(acct, &arn) {
            return Err(not_found(format!("Resource {arn} does not exist")));
        }
        let tags = acct.tags.get(&arn).cloned().unwrap_or_default();
        ok(json!({ "tags": tags }))
    }
}

// -------------------------------------------------------- free-fn helpers

/// A batch package-version mutation kind.
enum VersionOp {
    Delete,
    Dispose,
    SetStatus(String),
}

fn set_header(resp: &mut AwsResponse, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (
        http::header::HeaderName::from_bytes(name.as_bytes()),
        http::header::HeaderValue::from_str(value),
    ) {
        resp.headers.insert(n, v);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// True when `s` is a well-formed lowercase 64-character hex SHA-256 digest,
/// matching the `SHA256` shape's `@length(64,64)` + `^[0-9a-f]+$` constraints.
fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Borrow `obj[key]` as a mutable array, coercing a missing or non-array value
/// to an empty array first so a persisted/deserialized record with an
/// unexpected shape never panics on the request path.
fn ensure_array<'a>(obj: &'a mut Value, key: &str) -> &'a mut Vec<Value> {
    if !obj.get(key).map(Value::is_array).unwrap_or(false) {
        obj[key] = Value::Array(Vec::new());
    }
    obj[key]
        .as_array_mut()
        .expect("value was just set to an array")
}

/// Borrow `obj[key]` as a mutable object, coercing a missing or non-object value
/// to an empty object first so a persisted/deserialized record with an
/// unexpected shape never panics on the request path. Composes for nested keys
/// (e.g. `originConfiguration.restrictions`) since it returns a `&mut Value`
/// that is guaranteed to be an object.
fn ensure_object<'a>(obj: &'a mut Value, key: &str) -> &'a mut Value {
    if !obj.get(key).map(Value::is_object).unwrap_or(false) {
        obj[key] = Value::Object(Map::new());
    }
    &mut obj[key]
}

/// True when `arn` resolves to an existing domain, repository, or package group
/// in this account (compared against the stored `arn` field of each resource).
fn arn_exists(acct: &crate::state::CodeArtifactState, arn: &str) -> bool {
    let matches = |v: &Value| v.get("arn").and_then(|a| a.as_str()) == Some(arn);
    acct.domains.values().any(matches)
        || acct.repositories.values().any(matches)
        || acct.package_groups.values().any(matches)
}

/// The latest published version for a package (keys under `vprefix`), by
/// most-recent `publishedTime`, breaking ties on the higher version string.
/// Returns `None` when the package has no published version.
fn latest_published_version(
    acct: &crate::state::CodeArtifactState,
    vprefix: &str,
) -> Option<String> {
    let mut best: Option<(f64, String)> = None;
    for v in acct
        .package_versions
        .iter()
        .filter(|(k, _)| k.starts_with(vprefix))
        .map(|(_, v)| v)
    {
        if v.get("status").and_then(|s| s.as_str()) != Some("Published") {
            continue;
        }
        let pt = v.get("publishedTime").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let ver = v
            .get("version")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let better = match &best {
            None => true,
            Some((bpt, bver)) => pt > *bpt || (pt == *bpt && ver > *bver),
        };
        if better {
            best = Some((pt, ver));
        }
    }
    best.map(|(_, ver)| ver)
}

/// Extract the five package coordinates from query parameters.
fn package_coords(
    req: &AwsRequest,
) -> Result<(String, String, String, String, String), AwsServiceError> {
    check_domain_owner(req)?;
    let domain = req_q(req, "domain")?;
    let repo = req_q(req, "repository")?;
    let format = req_q(req, "format")?;
    validate_format(&format)?;
    let namespace = q(req, "namespace").unwrap_or_default();
    let package = req_q(req, "package")?;
    Ok((domain, repo, format, namespace, package))
}

fn repo_name(r: &Value) -> &str {
    r.get("name").and_then(|v| v.as_str()).unwrap_or("")
}

/// Project a `DomainDescription` down to the `DomainSummary` fields.
fn summarize_domain(d: &Value) -> Value {
    json!({
        "name": d.get("name"),
        "owner": d.get("owner"),
        "arn": d.get("arn"),
        "status": d.get("status"),
        "createdTime": d.get("createdTime"),
        "encryptionKey": d.get("encryptionKey"),
    })
}

/// Project a `RepositoryDescription` down to the `RepositorySummary` fields.
fn summarize_repository(r: &Value) -> Value {
    json!({
        "name": r.get("name"),
        "administratorAccount": r.get("administratorAccount"),
        "domainName": r.get("domainName"),
        "domainOwner": r.get("domainOwner"),
        "arn": r.get("arn"),
        "description": r.get("description"),
        "createdTime": r.get("createdTime"),
    })
}

/// Project a `PackageVersionDescription` down to the `PackageVersionSummary`.
fn summarize_version(v: &Value) -> Value {
    json!({
        "version": v.get("version"),
        "revision": v.get("revision"),
        "status": v.get("status"),
        "origin": v.get("origin"),
    })
}

fn parse_upstreams(v: Option<&Value>) -> Value {
    let mut out = Vec::new();
    if let Some(Value::Array(arr)) = v {
        for u in arr {
            if let Some(name) = u.get("repositoryName").and_then(|x| x.as_str()) {
                out.push(json!({ "repositoryName": name }));
            }
        }
    }
    Value::Array(out)
}

fn bump_repo_count(acct: &mut crate::state::CodeArtifactState, domain: &str, delta: i64) {
    if let Some(d) = acct.domains.get_mut(domain) {
        let cur = d.get("repositoryCount").and_then(|v| v.as_i64()).unwrap_or(0);
        d["repositoryCount"] = json!((cur + delta).max(0));
    }
}

/// Ensure a package record exists for the coordinates, creating it if needed.
fn ensure_package(
    acct: &mut crate::state::CodeArtifactState,
    key: &str,
    format: &str,
    namespace: &str,
    package: &str,
) {
    if !acct.packages.contains_key(key) {
        acct.packages.insert(
            key.to_string(),
            json!({
                "format": format,
                "namespace": if namespace.is_empty() { Value::Null } else { Value::String(namespace.to_string()) },
                "package": package,
                "originConfiguration": {
                    "restrictions": { "publish": "ALLOW", "upstream": "ALLOW" }
                },
            }),
        );
        acct.package_order.push(key.to_string());
    }
}

fn validate_origin_restrictions(restrictions: &Value) -> Result<(), AwsServiceError> {
    for field in ["publish", "upstream"] {
        if let Some(v) = restrictions.get(field).and_then(|x| x.as_str()) {
            if !crate::validate::is_enum(crate::validate::ALLOW_BLOCK, v) {
                return Err(validation(format!("Invalid {field} restriction: {v}")));
            }
        }
    }
    Ok(())
}

/// Build a fresh `PackageGroupDescription` with default origin configuration.
fn package_group_desc(
    region: &str,
    owner: &str,
    domain: &str,
    pattern: &str,
    contact_info: Option<String>,
    description: Option<String>,
) -> Value {
    let mut restrictions = Map::new();
    for rtype in crate::validate::ORIGIN_RESTRICTION_TYPE {
        restrictions.insert(
            (*rtype).to_string(),
            json!({
                "mode": "INHERIT",
                "effectiveMode": "ALLOW",
                "repositoriesCount": 0,
            }),
        );
    }
    json!({
        "arn": package_group_arn(region, owner, domain, pattern),
        "pattern": pattern,
        "domainName": domain,
        "domainOwner": owner,
        "createdTime": ts(Utc::now()),
        "contactInfo": contact_info.unwrap_or_default(),
        "description": description.unwrap_or_default(),
        "originConfiguration": { "restrictions": restrictions },
        "parent": Value::Null,
    })
}

/// Apply an add/remove allowed-repository update, recording it in `updates`.
fn apply_allowed(
    acct: &mut crate::state::CodeArtifactState,
    domain: &str,
    pattern: &str,
    entries: &[Value],
    add: bool,
    updates: &mut Map<String, Value>,
) {
    for e in entries {
        let rtype = e
            .get("originRestrictionType")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let repos: Vec<String> = e
            .get("repositoryNames")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        if rtype.is_empty() {
            continue;
        }
        let akey = format!("{domain}{SEP}{pattern}{SEP}{rtype}");
        let list = acct.package_group_allowed.entry(akey).or_default();
        for r in &repos {
            if add {
                if !list.contains(r) {
                    list.push(r.clone());
                }
            } else {
                list.retain(|x| x != r);
            }
        }
        let update_type = if add { "ADDED" } else { "REMOVED" };
        let entry = updates
            .entry(rtype.to_string())
            .or_insert_with(|| json!({}));
        entry[update_type] = json!(repos);
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use crate::state::CodeArtifactState;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::HeaderMap;
    use std::collections::HashMap;

    fn svc() -> CodeArtifactService {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(MultiAccountState::<
            CodeArtifactState,
        >::new(
            "123456789012", "us-east-1", ""
        )));
        CodeArtifactService::new(state)
    }

    fn jbody(v: Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }

    fn mkreq(action: &str, raw_query: &str, body: Vec<u8>, headers: HeaderMap) -> AwsRequest {
        AwsRequest {
            service: "codeartifact".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "req-1".into(),
            headers,
            query_params: HashMap::new(),
            body: bytes::Bytes::from(body),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: raw_query.into(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn err_code(e: &AwsServiceError) -> String {
        match e {
            AwsServiceError::AwsError { code, .. } => code.clone(),
            other => panic!("expected AwsError, got {other:?}"),
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn sha_header(content: &[u8]) -> HeaderMap {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let sha = hex_encode(&hasher.finalize());
        let mut h = HeaderMap::new();
        h.insert("x-amz-content-sha256", sha.parse().unwrap());
        h
    }

    fn setup(svc: &CodeArtifactService, domain: &str, repo: &str) {
        svc.create_domain(&mkreq(
            "CreateDomain",
            &format!("domain={domain}"),
            jbody(json!({})),
            HeaderMap::new(),
        ))
        .unwrap();
        svc.create_repository(&mkreq(
            "CreateRepository",
            &format!("domain={domain}&repository={repo}"),
            jbody(json!({})),
            HeaderMap::new(),
        ))
        .unwrap();
    }

    fn publish(
        svc: &CodeArtifactService,
        domain: &str,
        repo: &str,
        pkg: &str,
        version: &str,
        asset: &str,
        content: &[u8],
    ) -> Result<AwsResponse, AwsServiceError> {
        let q = format!(
            "domain={domain}&repository={repo}&format=npm&package={pkg}&version={version}&asset={asset}"
        );
        svc.publish_package_version(&mkreq(
            "PublishPackageVersion",
            &q,
            content.to_vec(),
            sha_header(content),
        ))
    }

    fn get_asset(
        svc: &CodeArtifactService,
        domain: &str,
        repo: &str,
        pkg: &str,
        version: &str,
        asset: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let q = format!(
            "domain={domain}&repository={repo}&format=npm&package={pkg}&version={version}&asset={asset}"
        );
        svc.get_package_version_asset(&mkreq("GetPackageVersionAsset", &q, vec![], HeaderMap::new()))
    }

    // Defect 1: a deleted version's asset is no longer downloadable (404).
    #[test]
    fn deleted_version_asset_returns_404() {
        let s = svc();
        setup(&s, "d1", "r1");
        publish(&s, "d1", "r1", "p", "1.0.0", "p-1.0.0.tgz", b"hello").unwrap();
        // Sanity: downloadable before delete, with octet-stream content type.
        let resp = get_asset(&s, "d1", "r1", "p", "1.0.0", "p-1.0.0.tgz").unwrap();
        assert_eq!(resp.content_type, "application/octet-stream");
        assert_eq!(resp.body.expect_bytes(), b"hello");

        s.delete_package_versions(&mkreq(
            "DeletePackageVersions",
            "domain=d1&repository=r1&format=npm&package=p",
            jbody(json!({ "versions": ["1.0.0"] })),
            HeaderMap::new(),
        ))
        .unwrap();

        let err = get_asset(&s, "d1", "r1", "p", "1.0.0", "p-1.0.0.tgz").map(|_| ()).unwrap_err();
        assert_eq!(err_code(&err), "ResourceNotFoundException");
    }

    // Defect 1: a disposed version keeps its record but drops its asset bytes.
    #[test]
    fn disposed_version_asset_returns_404() {
        let s = svc();
        setup(&s, "d1", "r1");
        publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"bytes").unwrap();
        s.dispose_package_versions(&mkreq(
            "DisposePackageVersions",
            "domain=d1&repository=r1&format=npm&package=p",
            jbody(json!({ "versions": ["1.0.0"] })),
            HeaderMap::new(),
        ))
        .unwrap();
        // Version record still exists (status Disposed) but asset is gone.
        let desc = s
            .describe_package_version(&mkreq(
                "DescribePackageVersion",
                "domain=d1&repository=r1&format=npm&package=p&version=1.0.0",
                vec![],
                HeaderMap::new(),
            ))
            .unwrap();
        assert_eq!(body_json(&desc)["packageVersion"]["status"], "Disposed");
        let err = get_asset(&s, "d1", "r1", "p", "1.0.0", "a.tgz").map(|_| ()).unwrap_err();
        assert_eq!(err_code(&err), "ResourceNotFoundException");
    }

    // Defect 2: CopyPackageVersions carries the asset bytes to the copy.
    #[test]
    fn copy_carries_assets() {
        let s = svc();
        setup(&s, "d1", "src");
        setup_repo(&s, "d1", "dst");
        publish(&s, "d1", "src", "p", "1.0.0", "a.tgz", b"payload").unwrap();
        s.copy_package_versions(&mkreq(
            "CopyPackageVersions",
            "domain=d1&source-repository=src&destination-repository=dst&format=npm&package=p",
            jbody(json!({ "versions": ["1.0.0"] })),
            HeaderMap::new(),
        ))
        .unwrap();
        let resp = get_asset(&s, "d1", "dst", "p", "1.0.0", "a.tgz").unwrap();
        assert_eq!(resp.body.expect_bytes(), b"payload");
        // And the asset summary is listable on the copy.
        let listed = s
            .list_package_version_assets(&mkreq(
                "ListPackageVersionAssets",
                "domain=d1&repository=dst&format=npm&package=p&version=1.0.0",
                vec![],
                HeaderMap::new(),
            ))
            .unwrap();
        assert_eq!(
            body_json(&listed)["assets"].as_array().unwrap().len(),
            1,
            "copied version should carry its asset summary"
        );
    }

    fn setup_repo(svc: &CodeArtifactService, domain: &str, repo: &str) {
        svc.create_repository(&mkreq(
            "CreateRepository",
            &format!("domain={domain}&repository={repo}"),
            jbody(json!({})),
            HeaderMap::new(),
        ))
        .unwrap();
    }

    // Defect 3: assetSHA256 header is required + must be well-formed + must match.
    #[test]
    fn publish_requires_valid_matching_sha256() {
        let s = svc();
        setup(&s, "d1", "r1");
        let q = "domain=d1&repository=r1&format=npm&package=p&version=1.0.0&asset=a.tgz";
        // Missing header.
        let e = s
            .publish_package_version(&mkreq("PublishPackageVersion", q, b"x".to_vec(), HeaderMap::new()))
            .map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ValidationException");
        // Malformed (not 64 lowercase hex).
        let mut bad = HeaderMap::new();
        bad.insert("x-amz-content-sha256", "notahash".parse().unwrap());
        let e = s
            .publish_package_version(&mkreq("PublishPackageVersion", q, b"x".to_vec(), bad))
            .map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ValidationException");
        // Well-formed but mismatched digest.
        let mut wrong = HeaderMap::new();
        wrong.insert("x-amz-content-sha256", "a".repeat(64).parse().unwrap());
        let e = s
            .publish_package_version(&mkreq("PublishPackageVersion", q, b"x".to_vec(), wrong))
            .map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ValidationException");
        // Correct digest succeeds.
        publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"x").unwrap();
    }

    // Defect 4: republishing an existing published asset is a ConflictException.
    #[test]
    fn republish_existing_asset_conflicts() {
        let s = svc();
        setup(&s, "d1", "r1");
        publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"first").unwrap();
        let e = publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"second").map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ConflictException");
        // The original bytes are untouched.
        let resp = get_asset(&s, "d1", "r1", "p", "1.0.0", "a.tgz").unwrap();
        assert_eq!(resp.body.expect_bytes(), b"first");
    }

    // Defect 4: an Unfinished asset may be overwritten (completed).
    #[test]
    fn republish_unfinished_asset_allowed() {
        let s = svc();
        setup(&s, "d1", "r1");
        let content = b"draft";
        let q = "domain=d1&repository=r1&format=npm&package=p&version=1.0.0&asset=a.tgz&unfinished=true";
        s.publish_package_version(&mkreq(
            "PublishPackageVersion",
            q,
            content.to_vec(),
            sha_header(content),
        ))
        .unwrap();
        // Overwriting the unfinished asset with final bytes is allowed.
        publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"final").unwrap();
        let resp = get_asset(&s, "d1", "r1", "p", "1.0.0", "a.tgz").unwrap();
        assert_eq!(resp.body.expect_bytes(), b"final");
    }

    // Defect 5: CopyPackageVersions with no version selection is a ValidationException.
    #[test]
    fn copy_empty_selection_rejected() {
        let s = svc();
        setup(&s, "d1", "src");
        setup_repo(&s, "d1", "dst");
        let e = s
            .copy_package_versions(&mkreq(
                "CopyPackageVersions",
                "domain=d1&source-repository=src&destination-repository=dst&format=npm&package=p",
                jbody(json!({})),
                HeaderMap::new(),
            ))
            .map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ValidationException");
    }

    // Defect 6: expectedStatus mismatch places the version in failedVersions.
    #[test]
    fn expected_status_mismatch_fails_version() {
        let s = svc();
        setup(&s, "d1", "r1");
        publish(&s, "d1", "r1", "p", "1.0.0", "a.tgz", b"x").unwrap(); // status Published
        let resp = s
            .update_package_versions_status(&mkreq(
                "UpdatePackageVersionsStatus",
                "domain=d1&repository=r1&format=npm&package=p",
                jbody(json!({
                    "targetStatus": "Archived",
                    "versions": ["1.0.0"],
                    "expectedStatus": "Unfinished"
                })),
                HeaderMap::new(),
            ))
            .unwrap();
        let j = body_json(&resp);
        assert_eq!(
            j["failedVersions"]["1.0.0"]["errorCode"], "MISMATCHED_STATUS",
            "status precondition mismatch should fail the version"
        );
        assert!(j["successfulVersions"].as_object().unwrap().is_empty());
        // The version status is unchanged.
        let desc = s
            .describe_package_version(&mkreq(
                "DescribePackageVersion",
                "domain=d1&repository=r1&format=npm&package=p&version=1.0.0",
                vec![],
                HeaderMap::new(),
            ))
            .unwrap();
        assert_eq!(body_json(&desc)["packageVersion"]["status"], "Published");
    }

    // Defect 7: tagging a non-existent ARN is a ResourceNotFoundException.
    #[test]
    fn tag_nonexistent_resource_404() {
        let s = svc();
        setup(&s, "d1", "r1");
        let bad_arn = "arn:aws:codeartifact:us-east-1:123456789012:domain/nope";
        let e = s
            .tag_resource(&mkreq(
                "TagResource",
                &format!("resourceArn={}", urlencode(bad_arn)),
                jbody(json!({ "tags": [{ "key": "k", "value": "v" }] })),
                HeaderMap::new(),
            ))
            .map(|_| ()).unwrap_err();
        assert_eq!(err_code(&e), "ResourceNotFoundException");
        // Tagging the real domain ARN works.
        let good = "arn:aws:codeartifact:us-east-1:123456789012:domain/d1".to_string();
        s.tag_resource(&mkreq(
            "TagResource",
            &format!("resourceArn={}", urlencode(&good)),
            jbody(json!({ "tags": [{ "key": "k", "value": "v" }] })),
            HeaderMap::new(),
        ))
        .unwrap();
    }

    fn urlencode(s: &str) -> String {
        s.replace(':', "%3A").replace('/', "%2F")
    }

    // Defect 8: defaultDisplayVersion is the latest published, not the smallest.
    #[test]
    fn default_display_version_is_latest_published() {
        let s = svc();
        setup(&s, "d1", "r1");
        publish(&s, "d1", "r1", "p", "1.0.0", "a1.tgz", b"one").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        publish(&s, "d1", "r1", "p", "2.0.0", "a2.tgz", b"two").unwrap();
        let resp = s
            .list_package_versions(&mkreq(
                "ListPackageVersions",
                "domain=d1&repository=r1&format=npm&package=p",
                vec![],
                HeaderMap::new(),
            ))
            .unwrap();
        assert_eq!(body_json(&resp)["defaultDisplayVersion"], "2.0.0");
    }

    // Defect 9: durationSeconds must be 0 or in [900, 43200].
    #[test]
    fn authorization_token_duration_range() {
        let s = svc();
        setup(&s, "d1", "r1");
        let call = |dur: &str| {
            s.get_authorization_token(&mkreq(
                "GetAuthorizationToken",
                &format!("domain=d1&duration={dur}"),
                jbody(json!({})),
                HeaderMap::new(),
            ))
        };
        assert!(call("0").is_ok());
        assert!(call("900").is_ok());
        assert!(call("43200").is_ok());
        assert_eq!(err_code(&call("100").map(|_| ()).unwrap_err()), "ValidationException");
        assert_eq!(err_code(&call("-5").map(|_| ()).unwrap_err()), "ValidationException");
        assert_eq!(err_code(&call("50000").map(|_| ()).unwrap_err()), "ValidationException");
    }

    // Defect 13: a persisted repository with a non-array externalConnections
    // must not panic when associating a connection.
    #[test]
    fn associate_connection_tolerates_bad_shape() {
        let s = svc();
        setup(&s, "d1", "r1");
        {
            let mut g = s.state.write();
            let acct = g.get_or_create("123456789012");
            let repo = acct.repositories.get_mut("d1/r1").unwrap();
            repo["externalConnections"] = Value::Null;
        }
        let resp = s
            .associate_external_connection(&mkreq(
                "AssociateExternalConnection",
                "domain=d1&repository=r1&external-connection=public:npmjs",
                jbody(json!({})),
                HeaderMap::new(),
            ))
            .unwrap();
        let conns = body_json(&resp)["repository"]["externalConnections"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(conns, 1);
    }
}
