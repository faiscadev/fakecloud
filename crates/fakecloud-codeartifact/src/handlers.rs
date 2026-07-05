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

fn repo_endpoint(region: &str, owner: &str, domain: &str, repo: &str, format: &str) -> String {
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
        ok(json!({ "repositoryEndpoint": repo_endpoint(&region, &owner, &domain, &repo, &format) }))
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
        let conns = desc["externalConnections"]
            .as_array_mut()
            .expect("externalConnections is an array");
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
        let conns = desc["externalConnections"]
            .as_array_mut()
            .expect("externalConnections is an array");
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
        let default_display = versions
            .first()
            .and_then(|v| v.get("version"))
            .and_then(|v| v.as_str())
            .map(str::to_string);
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
            match acct.package_versions.get_mut(&vk) {
                Some(desc) => {
                    let rev = desc
                        .get("revision")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let status = match &op {
                        VersionOp::Delete => "Deleted",
                        VersionOp::Dispose => "Disposed",
                        VersionOp::SetStatus(s) => s.as_str(),
                    };
                    if matches!(op, VersionOp::Delete) {
                        acct.package_versions.remove(&vk);
                    } else {
                        desc["status"] = Value::String(status.to_string());
                    }
                    successful.insert(v, json!({ "revision": rev, "status": status }));
                }
                None => {
                    failed.insert(
                        v.clone(),
                        json!({
                            "errorCode": "NOT_FOUND",
                            "errorMessage": format!("Package version {v} does not exist"),
                        }),
                    );
                }
            }
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
        let src_versions: Vec<String> = if versions.is_empty() {
            let vprefix = format!("{src_pkg}{SEP}");
            acct.package_versions
                .keys()
                .filter(|k| k.starts_with(&vprefix))
                .filter_map(|k| k.rsplit(SEP).next().map(str::to_string))
                .collect()
        } else {
            versions
        };
        for v in src_versions {
            let svk = version_key(&src_pkg, &v);
            match acct.package_versions.get(&svk).cloned() {
                Some(mut desc) => {
                    desc["status"] = Value::String("Published".into());
                    acct.package_versions.insert(version_key(&dst_pkg, &v), desc.clone());
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
        let content = req.body.to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let sha256 = hex_encode(&hasher.finalize());
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
        let Some(b64) = acct.asset_content.get(&ak) else {
            return Err(not_found(format!("Asset {asset_name} does not exist")));
        };
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap_or_default();
        let rev = acct
            .package_versions
            .get(&vk)
            .and_then(|d| d.get("revision"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut resp = AwsResponse::json(StatusCode::OK, bytes);
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
        // Apply allowed-repository add/remove per restriction type.
        let mut updates = Map::new();
        if let Some(add) = b.get("addAllowedRepositories").and_then(|v| v.as_array()) {
            apply_allowed(acct, &domain, &pattern, add, true, &mut updates);
        }
        if let Some(rem) = b.get("removeAllowedRepositories").and_then(|v| v.as_array()) {
            apply_allowed(acct, &domain, &pattern, rem, false, &mut updates);
        }
        // Apply origin restriction mode changes.
        if let Some(restrictions) = b.get("restrictions").and_then(|v| v.as_object()) {
            for mode in restrictions.values().filter_map(|v| v.as_str()) {
                if !crate::validate::is_enum(crate::validate::ORIGIN_RESTRICTION_MODE, mode) {
                    return Err(validation(format!("Invalid restriction mode: {mode}")));
                }
            }
            if let Some(g) = acct.package_groups.get_mut(&gkey) {
                let origin = g["originConfiguration"]["restrictions"]
                    .as_object_mut()
                    .expect("restrictions map");
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
