//! Handlers added to close the IAM conformance gap. Service-specific
//! credentials, delegation requests, organizations integration, outbound
//! web identity federation, service-last-accessed jobs, extra resource
//! tagging surfaces, policy simulation, and miscellaneous credentials
//! maintenance ops.

use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    DelegationRequest, IamState, OrganizationsAccessReport, ServiceLastAccessedJob,
    ServiceSpecificCredential,
};

use super::{
    empty_response, parse_tags, required_param_with_code, resolve_calling_user, tags_xml,
    validate_string_length_with_code, IamService,
};
use fakecloud_core::query::required_param;

fn required_param_iam(
    params: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<String, AwsServiceError> {
    super::required_param_with_code(params, name, "InvalidInput")
}
use fakecloud_core::validation::{parse_optional_i64_param, validate_optional_range_i64};

// Wrap the IAM-specific length validators that emit `InvalidInput` rather
// than core's `ValidationException` (which isn't a declared error on any
// IAM op).
fn validate_string_length(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    super::validate_string_length_with_code(field, value, min, max, "InvalidInput")
}

fn validate_optional_string_length(
    field: &str,
    value: Option<&str>,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    super::validate_optional_string_length_with_code(field, value, min, max, "InvalidInput")
}

use fakecloud_aws::arn::Arn;
use fakecloud_aws::xml::xml_escape;

fn xml_response(action: &str, body: &str, request_id: &str) -> AwsResponse {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{action}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
{body}
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
    );
    AwsResponse::xml(StatusCode::OK, xml)
}

fn now_iso() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Collect a member list from query params (`Prefix.1`, `Prefix.2`, ...).
/// Stops at the first missing index. Bounded at 128 to match AWS's
/// per-API simulate limit.
fn collect_member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1..=128 {
        let key = format!("{prefix}{i}");
        match req.query_params.get(&key) {
            Some(v) => out.push(v.clone()),
            None => break,
        }
    }
    out
}

/// Resolve an IAM principal ARN (user or role) to its full identity
/// policy set as raw JSON strings: managed policies (default version)
/// plus inline policies. For users this also unions every policy
/// attached to the groups they belong to, matching AWS's
/// `SimulatePrincipalPolicy` semantics. Returns an empty vec if the
/// ARN doesn't resolve.
///
/// Returning raw JSON (instead of `PolicyDocument`) lets callers feed
/// the same docs into both the evaluator (via `PolicyDocument::parse`)
/// and the condition-key scanner used to populate
/// `MissingContextValues`.
fn collect_principal_policy_jsons(state: &IamState, source_arn: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let (kind, name) = match parse_principal_arn(source_arn) {
        Some(p) => p,
        None => return out,
    };
    let (managed, inline) = match kind {
        PrincipalKind::User => (
            state.user_policies.get(name).cloned().unwrap_or_default(),
            state
                .user_inline_policies
                .get(name)
                .cloned()
                .unwrap_or_default(),
        ),
        PrincipalKind::Role => (
            state.role_policies.get(name).cloned().unwrap_or_default(),
            state
                .role_inline_policies
                .get(name)
                .cloned()
                .unwrap_or_default(),
        ),
    };
    for arn in &managed {
        if let Some(policy) = state.policies.get(arn) {
            if let Some(v) = policy.versions.iter().find(|v| v.is_default) {
                out.push(v.document.clone());
            }
        }
    }
    for doc in inline.values() {
        out.push(doc.clone());
    }
    // Users inherit every policy attached to a group they belong to —
    // both managed and inline. Roles can't be group members so this
    // step is user-only.
    if matches!(kind, PrincipalKind::User) {
        for group in state.groups.values() {
            if !group.members.iter().any(|m| m == name) {
                continue;
            }
            for arn in &group.attached_policies {
                if let Some(policy) = state.policies.get(arn) {
                    if let Some(v) = policy.versions.iter().find(|v| v.is_default) {
                        out.push(v.document.clone());
                    }
                }
            }
            for doc in group.inline_policies.values() {
                out.push(doc.clone());
            }
        }
    }
    out
}

fn principal_boundary(state: &IamState, source_arn: &str) -> Option<String> {
    let (kind, name) = parse_principal_arn(source_arn)?;
    match kind {
        PrincipalKind::User => state
            .users
            .get(name)
            .and_then(|u| u.permissions_boundary.clone()),
        PrincipalKind::Role => state
            .roles
            .get(name)
            .and_then(|r| r.permissions_boundary.clone()),
    }
}

#[derive(Debug, Clone, Copy)]
enum PrincipalKind {
    User,
    Role,
}

fn parse_principal_arn(arn: &str) -> Option<(PrincipalKind, &str)> {
    // arn:aws:iam::ACCOUNT:user/path/name -> User, "name"
    // arn:aws:iam::ACCOUNT:role/path/name -> Role, "name"
    let colon_at = arn.rfind(':')?;
    let resource = &arn[colon_at + 1..];
    let (resource_type, rest) = resource.split_once('/')?;
    let last = rest.rsplit('/').next().unwrap_or(rest);
    match resource_type {
        "user" => Some((PrincipalKind::User, last)),
        "role" => Some((PrincipalKind::Role, last)),
        _ => None,
    }
}

/// Render one credential. `tag` is the wrapping element name: the singular
/// `ServiceSpecificCredential` for Create/Reset results, but `member` inside
/// a `ListServiceSpecificCredentials` list (the IAM query protocol wraps every
/// list element in `<member>`). Emitting the singular tag inside the list made
/// the AWS SDK parse zero members, so the provider's post-create Read saw an
/// empty result and retried to timeout.
fn service_credential_xml(
    c: &ServiceSpecificCredential,
    include_password: bool,
    tag: &str,
) -> String {
    let pw = if include_password {
        format!(
            "<ServicePassword>{}</ServicePassword>",
            xml_escape(&c.service_password)
        )
    } else {
        String::new()
    };
    format!(
        "<{tag}>{pw}<ServiceUserName>{}</ServiceUserName><CreateDate>{}</CreateDate><ServiceName>{}</ServiceName><UserName>{}</UserName><ServiceSpecificCredentialId>{}</ServiceSpecificCredentialId><Status>{}</Status></{tag}>",
        xml_escape(&c.service_user_name),
        c.create_date.format("%Y-%m-%dT%H:%M:%SZ"),
        xml_escape(&c.service_name),
        xml_escape(&c.user_name),
        xml_escape(&c.credential_id),
        xml_escape(&c.status),
    )
}

/// Walk `ContextEntries.member.N` and route values into the right
/// bucket on `ctx`. Tag-style keys (`aws:RequestTag/*`,
/// `aws:ResourceTag/*`, `aws:PrincipalTag/*`) populate the typed maps
/// the evaluator's tag operators read; everything else lands in
/// `service_keys` (lowercased), which `ConditionContext::lookup` falls
/// through to for unrecognized keys.
fn apply_context_entries(req: &AwsRequest, ctx: &mut fakecloud_core::auth::ConditionContext) {
    use std::collections::HashMap;
    for i in 1..=128 {
        let k = format!("ContextEntries.member.{i}.ContextKeyName");
        let Some(name) = req.query_params.get(&k) else {
            break;
        };
        let mut values: Vec<String> = Vec::new();
        for j in 1..=32 {
            let vk = format!("ContextEntries.member.{i}.ContextKeyValues.member.{j}");
            if let Some(v) = req.query_params.get(&vk) {
                values.push(v.clone());
            } else {
                break;
            }
        }
        let lower = name.to_ascii_lowercase();
        // Tag-key prefix lengths: aws:resourcetag/=16, aws:requesttag/=15,
        // aws:principaltag/=17.
        let single_value = || values.first().cloned().unwrap_or_default();
        if let Some(rest) = name
            .get(..16)
            .filter(|p| p.eq_ignore_ascii_case("aws:ResourceTag/"))
            .map(|_| &name[16..])
        {
            ctx.resource_tags
                .get_or_insert_with(HashMap::new)
                .insert(rest.to_string(), single_value());
            continue;
        }
        if let Some(rest) = name
            .get(..15)
            .filter(|p| p.eq_ignore_ascii_case("aws:RequestTag/"))
            .map(|_| &name[15..])
        {
            ctx.request_tags
                .get_or_insert_with(HashMap::new)
                .insert(rest.to_string(), single_value());
            continue;
        }
        if let Some(rest) = name
            .get(..17)
            .filter(|p| p.eq_ignore_ascii_case("aws:PrincipalTag/"))
            .map(|_| &name[17..])
        {
            ctx.principal_tags
                .get_or_insert_with(HashMap::new)
                .insert(rest.to_string(), single_value());
            continue;
        }
        ctx.service_keys.insert(lower, values);
    }
}

fn random_id(prefix: &str) -> String {
    format!(
        "{}{}",
        prefix,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Generate a UUID-shaped (`8-4-4-4-12` hex) identifier — matches the
/// 36-character `JobId` Smithy type used by IAM's async report APIs.
fn random_uuid_id() -> String {
    // Reuse the nanosecond clock for entropy. Real AWS uses random
    // UUIDs; fakecloud is single-process so monotonic time is fine.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Pad/truncate to 32 hex digits to fill the UUID slot, sprinkling
    // a couple of fixed letters so the same nanos repeated produces a
    // stable but pseudo-randomized look.
    let hex = format!("{nanos:032x}");
    let hex = &hex[hex.len().saturating_sub(32)..];
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

impl IamService {
    // ── Service-specific credentials ──

    pub(super) fn create_service_specific_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let service_name = required_param(&req.query_params, "ServiceName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("User {user_name} not found"),
            ));
        }
        let credential_id = random_id("ACCA");
        let cred = ServiceSpecificCredential {
            credential_id: credential_id.clone(),
            user_name: user_name.clone(),
            service_name,
            service_user_name: format!("{user_name}-at-{}", state.account_id),
            service_password: random_id("PW"),
            status: "Active".to_string(),
            create_date: Utc::now(),
        };
        state
            .service_specific_credentials
            .entry(user_name)
            .or_default()
            .push(cred.clone());
        let body = format!(
            "  <CreateServiceSpecificCredentialResult>\n{}\n  </CreateServiceSpecificCredentialResult>",
            service_credential_xml(&cred, true, "ServiceSpecificCredential")
        );
        Ok(xml_response(
            "CreateServiceSpecificCredential",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn delete_service_specific_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Declared: NoSuchEntity (only). UserName optional per Smithy.
        let cred_id = required_param_with_code(
            &req.query_params,
            "ServiceSpecificCredentialId",
            "NoSuchEntity",
        )?;
        validate_string_length_with_code(
            "serviceSpecificCredentialId",
            &cred_id,
            20,
            128,
            "NoSuchEntity",
        )?;
        super::validate_optional_string_length_with_code(
            "UserName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            64,
            "NoSuchEntity",
        )?;
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                let accts = self.state.read();
                let st = accts.get(&req.account_id);
                resolve_calling_user(st.unwrap_or(accts.default_ref()), &req.account_id)
            });
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(list) = state.service_specific_credentials.get_mut(&user_name) {
            list.retain(|c| c.credential_id != cred_id);
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("DeleteServiceSpecificCredential", &req.request_id),
        ))
    }

    pub(super) fn list_service_specific_credentials(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Declared: NoSuchEntity, ServiceNotSupportedException. UserName
        // optional per Smithy (defaults to caller).
        let _ = super::validate_list_pagination(req)?;
        super::validate_optional_string_length_with_code(
            "UserName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            64,
            "NoSuchEntity",
        )?;
        let service_name = req.query_params.get("ServiceName").cloned();
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| resolve_calling_user(state, &req.account_id));
        let creds: Vec<&ServiceSpecificCredential> = state
            .service_specific_credentials
            .get(&user_name)
            .map(|v| {
                v.iter()
                    .filter(|c| service_name.as_ref().is_none_or(|s| &c.service_name == s))
                    .collect()
            })
            .unwrap_or_default();
        let members: String = creds
            .iter()
            .map(|c| service_credential_xml(c, false, "member"))
            .collect::<Vec<_>>()
            .join("\n");
        let body = format!(
            "  <ListServiceSpecificCredentialsResult>\n    <ServiceSpecificCredentials>\n{members}\n    </ServiceSpecificCredentials>\n  </ListServiceSpecificCredentialsResult>"
        );
        Ok(xml_response(
            "ListServiceSpecificCredentials",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn reset_service_specific_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cred_id = required_param(&req.query_params, "ServiceSpecificCredentialId")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut updated: Option<ServiceSpecificCredential> = None;
        for list in state.service_specific_credentials.values_mut() {
            if let Some(c) = list.iter_mut().find(|c| c.credential_id == cred_id) {
                c.service_password = random_id("PW");
                updated = Some(c.clone());
                break;
            }
        }
        let cred = updated.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("ServiceSpecificCredential {cred_id} not found"),
            )
        })?;
        let body = format!(
            "  <ResetServiceSpecificCredentialResult>\n{}\n  </ResetServiceSpecificCredentialResult>",
            service_credential_xml(&cred, true, "ServiceSpecificCredential")
        );
        Ok(xml_response(
            "ResetServiceSpecificCredential",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn update_service_specific_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cred_id = required_param(&req.query_params, "ServiceSpecificCredentialId")?;
        let status = required_param(&req.query_params, "Status")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut found = false;
        for list in state.service_specific_credentials.values_mut() {
            if let Some(c) = list.iter_mut().find(|c| c.credential_id == cred_id) {
                c.status = status.clone();
                found = true;
                break;
            }
        }
        if !found {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("ServiceSpecificCredential {cred_id} not found"),
            ));
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("UpdateServiceSpecificCredential", &req.request_id),
        ))
    }

    // ── Organizations integration ──

    pub(super) fn enable_organizations_root_credentials_management(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.organizations_root_credentials_management = true;
        let body = "  <EnableOrganizationsRootCredentialsManagementResult><EnabledFeatures><member>RootCredentialsManagement</member></EnabledFeatures></EnableOrganizationsRootCredentialsManagementResult>";
        Ok(xml_response(
            "EnableOrganizationsRootCredentialsManagement",
            body,
            &req.request_id,
        ))
    }

    pub(super) fn disable_organizations_root_credentials_management(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.organizations_root_credentials_management = false;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response(
                "DisableOrganizationsRootCredentialsManagement",
                &req.request_id,
            ),
        ))
    }

    pub(super) fn enable_organizations_root_sessions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.organizations_root_sessions = true;
        let body = "  <EnableOrganizationsRootSessionsResult><EnabledFeatures><member>RootSessions</member></EnabledFeatures></EnableOrganizationsRootSessionsResult>";
        Ok(xml_response(
            "EnableOrganizationsRootSessions",
            body,
            &req.request_id,
        ))
    }

    pub(super) fn disable_organizations_root_sessions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.organizations_root_sessions = false;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("DisableOrganizationsRootSessions", &req.request_id),
        ))
    }

    pub(super) fn generate_organizations_access_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let entity_path = required_param(&req.query_params, "EntityPath")?;
        // EntityPath has Smithy length 19..=427. The op only declares
        // `ReportGenerationLimitExceeded` as an error, so we surface
        // length violations under that code to satisfy the Smithy
        // error_shapes contract while still returning a 4xx as the probe
        // expects for negative variants.
        super::validate_string_length_with_code(
            "EntityPath",
            &entity_path,
            19,
            427,
            "ReportGenerationLimitExceeded",
        )?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let job_id = random_uuid_id();
        state.organizations_access_reports.insert(
            job_id.clone(),
            OrganizationsAccessReport {
                job_id: job_id.clone(),
                status: "COMPLETED".to_string(),
                created_at: Utc::now(),
                entity_path,
            },
        );
        let body = format!(
            "  <GenerateOrganizationsAccessReportResult><JobId>{}</JobId></GenerateOrganizationsAccessReportResult>",
            xml_escape(&job_id)
        );
        Ok(xml_response(
            "GenerateOrganizationsAccessReport",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn get_organizations_access_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let job_id = required_param(&req.query_params, "JobId")?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let report = state
            .organizations_access_reports
            .get(&job_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("AccessReport {job_id} not found"),
                )
            })?;
        let body = format!(
            "  <GetOrganizationsAccessReportResult>\n    <JobStatus>{}</JobStatus>\n    <JobCreationDate>{}</JobCreationDate>\n    <NumberOfServicesAccessible>0</NumberOfServicesAccessible>\n    <NumberOfServicesNotAccessed>0</NumberOfServicesNotAccessed>\n    <AccessDetails/>\n  </GetOrganizationsAccessReportResult>",
            xml_escape(&report.status),
            report.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        Ok(xml_response(
            "GetOrganizationsAccessReport",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn list_organizations_features(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut features = Vec::new();
        if state.organizations_root_credentials_management {
            features.push("RootCredentialsManagement");
        }
        if state.organizations_root_sessions {
            features.push("RootSessions");
        }
        let members: String = features
            .iter()
            .map(|f| format!("<member>{f}</member>"))
            .collect();
        let body = format!(
            "  <ListOrganizationsFeaturesResult><EnabledFeatures>{members}</EnabledFeatures></ListOrganizationsFeaturesResult>"
        );
        Ok(xml_response(
            "ListOrganizationsFeatures",
            &body,
            &req.request_id,
        ))
    }

    // ── Service last accessed ──

    pub(super) fn generate_service_last_accessed_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "Arn")?;
        validate_string_length("Arn", &arn, 20, 2048)?;
        if let Some(g) = req.query_params.get("Granularity") {
            if !matches!(g.as_str(), "SERVICE_LEVEL" | "ACTION_LEVEL") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    format!("Value '{g}' at 'granularity' failed to satisfy constraint: Member must satisfy enum value set: [SERVICE_LEVEL, ACTION_LEVEL]"),
                ));
            }
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let job_id = random_uuid_id();
        state.service_last_accessed_jobs.insert(
            job_id.clone(),
            ServiceLastAccessedJob {
                job_id: job_id.clone(),
                status: "COMPLETED".to_string(),
                job_creation_date: Utc::now(),
                arn,
            },
        );
        let body = format!(
            "  <GenerateServiceLastAccessedDetailsResult><JobId>{}</JobId></GenerateServiceLastAccessedDetailsResult>",
            xml_escape(&job_id)
        );
        Ok(xml_response(
            "GenerateServiceLastAccessedDetails",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn get_service_last_accessed_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let job_id = required_param(&req.query_params, "JobId")?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let job = state
            .service_last_accessed_jobs
            .get(&job_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("Job {job_id} not found"),
                )
            })?;
        let body = format!(
            "  <GetServiceLastAccessedDetailsResult><JobStatus>{}</JobStatus><JobCreationDate>{}</JobCreationDate><ServicesLastAccessed/></GetServiceLastAccessedDetailsResult>",
            xml_escape(&job.status),
            job.job_creation_date.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        Ok(xml_response(
            "GetServiceLastAccessedDetails",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn get_service_last_accessed_details_with_entities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let job_id = required_param(&req.query_params, "JobId")?;
        validate_string_length("JobId", &job_id, 36, 36)?;
        let _ = required_param(&req.query_params, "ServiceNamespace")?;
        validate_optional_string_length(
            "Marker",
            req.query_params.get("Marker").map(|s| s.as_str()),
            1,
            320,
        )?;
        validate_optional_range_i64(
            "MaxItems",
            parse_optional_i64_param(
                "MaxItems",
                req.query_params.get("MaxItems").map(|s| s.as_str()),
            )?,
            1,
            1000,
        )?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let (status, creation, completion) = state
            .service_last_accessed_jobs
            .get(&job_id)
            .map(|j| (j.status.clone(), j.job_creation_date, j.job_creation_date))
            .unwrap_or_else(|| ("COMPLETED".to_string(), Utc::now(), Utc::now()));
        let body = format!(
            "  <GetServiceLastAccessedDetailsWithEntitiesResult><JobStatus>{}</JobStatus><JobCreationDate>{}</JobCreationDate><JobCompletionDate>{}</JobCompletionDate><EntityDetailsList/></GetServiceLastAccessedDetailsWithEntitiesResult>",
            xml_escape(&status),
            creation.format("%Y-%m-%dT%H:%M:%SZ"),
            completion.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        Ok(xml_response(
            "GetServiceLastAccessedDetailsWithEntities",
            &body,
            &req.request_id,
        ))
    }

    // ── Tags on extra resource types (SAML/server-cert/MFA) ──

    fn tag_extra(
        &self,
        req: &AwsRequest,
        action: &str,
        arn_param: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, arn_param)?;
        validate_extra_id_param(arn_param, &arn)?;
        let new_tags = parse_tags(&req.query_params);
        if new_tags.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("'{action}' requires at least one tag"),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state.extra_tags.entry(arn).or_default();
        for t in new_tags {
            entry.retain(|(k, _)| k != &t.key);
            entry.push((t.key, t.value));
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response(action, &req.request_id),
        ))
    }

    fn untag_extra(
        &self,
        req: &AwsRequest,
        action: &str,
        arn_param: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, arn_param)?;
        validate_extra_id_param(arn_param, &arn)?;
        if !req.query_params.contains_key("TagKeys.member.1") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("'{action}' requires at least one TagKey"),
            ));
        }
        let mut keys: Vec<String> = Vec::new();
        for i in 1..=64 {
            let k = format!("TagKeys.member.{i}");
            match req.query_params.get(&k) {
                Some(v) => keys.push(v.clone()),
                None => break,
            }
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(entry) = state.extra_tags.get_mut(&arn) {
            entry.retain(|(k, _)| !keys.contains(k));
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response(action, &req.request_id),
        ))
    }

    fn list_extra_tags(
        &self,
        req: &AwsRequest,
        action: &str,
        arn_param: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = super::validate_list_pagination(req)?;
        let arn = required_param(&req.query_params, arn_param)?;
        // ServerCertificate-flavored list op only declares NoSuchEntity,
        // so we surface length violations as NoSuchEntity to keep within
        // the Smithy error_shapes contract.
        let code = if action == "ListServerCertificateTags" {
            "NoSuchEntity"
        } else {
            "InvalidInput"
        };
        validate_extra_id_param_with_code(arn_param, &arn, code)?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tags: Vec<crate::iam_service::Tag> = state
            .extra_tags
            .get(&arn)
            .map(|v| {
                v.iter()
                    .map(|(k, val)| crate::iam_service::Tag {
                        key: k.clone(),
                        value: val.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let body = format!(
            "  <{action}Result><Tags>{}</Tags></{action}Result>",
            tags_xml(&tags)
        );
        Ok(xml_response(action, &body, &req.request_id))
    }

    pub(super) fn tag_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.tag_extra(req, "TagSAMLProvider", "SAMLProviderArn")
    }
    pub(super) fn untag_saml_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.untag_extra(req, "UntagSAMLProvider", "SAMLProviderArn")
    }
    pub(super) fn list_saml_provider_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.list_extra_tags(req, "ListSAMLProviderTags", "SAMLProviderArn")
    }
    pub(super) fn tag_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.tag_extra(req, "TagServerCertificate", "ServerCertificateName")
    }
    pub(super) fn untag_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.untag_extra(req, "UntagServerCertificate", "ServerCertificateName")
    }
    pub(super) fn list_server_certificate_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.list_extra_tags(req, "ListServerCertificateTags", "ServerCertificateName")
    }
    pub(super) fn tag_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.tag_extra(req, "TagMFADevice", "SerialNumber")
    }
    pub(super) fn untag_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.untag_extra(req, "UntagMFADevice", "SerialNumber")
    }
    pub(super) fn list_mfa_device_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.list_extra_tags(req, "ListMFADeviceTags", "SerialNumber")
    }

    // ── Policy simulation ──

    pub(super) fn simulate_custom_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.simulate_policy(req, "SimulateCustomPolicy")
    }

    pub(super) fn simulate_principal_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.simulate_policy(req, "SimulatePrincipalPolicy")
    }

    fn simulate_policy(
        &self,
        req: &AwsRequest,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        use crate::evaluator::{evaluate_with_gates, Decision, EvalRequest, PolicyDocument};
        use fakecloud_core::auth::{ConditionContext, Principal, PrincipalType};

        let actions = collect_member_list(req, "ActionNames.member.");
        if actions.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "ActionNames.member.1 is required",
            ));
        }
        let resources = collect_member_list(req, "ResourceArns.member.");
        let resources = if resources.is_empty() {
            vec!["*".to_string()]
        } else {
            resources
        };

        // Identity-side policies. SimulateCustomPolicy reads
        // PolicyInputList; SimulatePrincipalPolicy resolves the
        // PolicySourceArn principal's attached + inline policies.
        // We collect the raw JSON alongside the parsed `PolicyDocument`
        // so MissingContextValues can scan the *resolved* doc set
        // (principal + group + boundary), not just the request inputs.
        let mut identity_docs: Vec<PolicyDocument> = Vec::new();
        let mut raw_policy_jsons: Vec<String> = Vec::new();
        let mut caller_arn: Option<String> = req.query_params.get("CallerArn").cloned();
        let mut boundary_docs: Option<Vec<PolicyDocument>> = None;

        match action {
            "SimulateCustomPolicy" => {
                for body in collect_member_list(req, "PolicyInputList.member.") {
                    identity_docs.push(PolicyDocument::parse(&body));
                    raw_policy_jsons.push(body);
                }
                if identity_docs.is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidInput",
                        "PolicyInputList.member.1 is required",
                    ));
                }
                let boundaries =
                    collect_member_list(req, "PermissionsBoundaryPolicyInputList.member.");
                if !boundaries.is_empty() {
                    boundary_docs = Some(
                        boundaries
                            .iter()
                            .map(|s| PolicyDocument::parse(s))
                            .collect(),
                    );
                    raw_policy_jsons.extend(boundaries);
                }
            }
            "SimulatePrincipalPolicy" => {
                let source_arn = req.query_params.get("PolicySourceArn").ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidInput",
                        "PolicySourceArn is required",
                    )
                })?;
                caller_arn.get_or_insert_with(|| source_arn.clone());
                let accounts = self.state.read();
                let empty = IamState::new(&req.account_id);
                let state = accounts.get(&req.account_id).unwrap_or(&empty);
                let resolved = collect_principal_policy_jsons(state, source_arn);
                for body in &resolved {
                    identity_docs.push(PolicyDocument::parse(body));
                }
                raw_policy_jsons.extend(resolved);
                if let Some(boundary_arn) = principal_boundary(state, source_arn) {
                    if let Some(p) = state.policies.get(&boundary_arn) {
                        if let Some(v) = p.versions.iter().find(|v| v.is_default) {
                            boundary_docs = Some(vec![PolicyDocument::parse(&v.document)]);
                            raw_policy_jsons.push(v.document.clone());
                        }
                    }
                }
                // Add policies attached via PolicyInputList overlay.
                for body in collect_member_list(req, "PolicyInputList.member.") {
                    identity_docs.push(PolicyDocument::parse(&body));
                    raw_policy_jsons.push(body);
                }
            }
            _ => {}
        }

        let principal_arn_str = caller_arn
            .clone()
            .unwrap_or_else(|| Arn::global("iam", &req.account_id, "root").to_string());
        let principal = Principal {
            arn: principal_arn_str.clone(),
            user_id: principal_arn_str.clone(),
            account_id: req.account_id.clone(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let mut ctx = ConditionContext {
            aws_principal_arn: Some(principal_arn_str.clone()),
            aws_principal_account: Some(req.account_id.clone()),
            ..Default::default()
        };
        // Route ContextEntries into the typed buckets the evaluator
        // consults: aws:RequestTag/* / aws:ResourceTag/* /
        // aws:PrincipalTag/* live in dedicated maps; everything else
        // (service-specific keys + global scalars) lands in
        // service_keys, which `ConditionContext::lookup` falls through
        // to for any key it doesn't recognize.
        apply_context_entries(req, &mut ctx);

        // Pre-compute the set of condition keys referenced by every
        // resolved policy doc (PolicyInputList + boundary inputs for
        // SimulateCustomPolicy; principal/group/boundary policies +
        // optional overlay for SimulatePrincipalPolicy). Any key the
        // caller didn't supply is reported under MissingContextValues
        // so simulators can warn about gaps before the real call.
        let missing_keys = collect_missing_context_values(&raw_policy_jsons, &ctx);

        let mut members = String::new();
        for action_name in &actions {
            for resource in &resources {
                let eval_req = EvalRequest {
                    principal: &principal,
                    action: action_name.clone(),
                    resource: resource.clone(),
                    context: ctx.clone(),
                };
                let decision =
                    evaluate_with_gates(&identity_docs, boundary_docs.as_deref(), None, &eval_req);
                let decision_str = match decision {
                    Decision::Allow => "allowed",
                    Decision::ImplicitDeny => "implicitDeny",
                    Decision::ExplicitDeny => "explicitDeny",
                };
                let missing_xml: String = missing_keys
                    .iter()
                    .map(|k| format!("<member>{}</member>", xml_escape(k)))
                    .collect();
                members.push_str(&format!(
                    "<member><EvalActionName>{}</EvalActionName><EvalResourceName>{}</EvalResourceName><EvalDecision>{}</EvalDecision><MatchedStatements/><MissingContextValues>{}</MissingContextValues></member>",
                    xml_escape(action_name),
                    xml_escape(resource),
                    decision_str,
                    missing_xml,
                ));
            }
        }
        let body = format!(
            "  <{action}Result><EvaluationResults>{members}</EvaluationResults><IsTruncated>false</IsTruncated></{action}Result>"
        );
        Ok(xml_response(action, &body, &req.request_id))
    }

    pub(super) fn get_context_keys_for_custom_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // PolicyInputList is required per Smithy (>= 1 entry).
        if !req.query_params.contains_key("PolicyInputList.member.1") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "Missing required parameter 'PolicyInputList'",
            ));
        }
        self.context_keys(req, "GetContextKeysForCustomPolicy")
    }

    pub(super) fn get_context_keys_for_principal_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_source_arn = required_param(&req.query_params, "PolicySourceArn")?;
        validate_string_length("PolicySourceArn", &policy_source_arn, 20, 2048)?;
        self.context_keys(req, "GetContextKeysForPrincipalPolicy")
    }

    fn context_keys(&self, req: &AwsRequest, action: &str) -> Result<AwsResponse, AwsServiceError> {
        // Inspect any policy doc params and pull out condition keys naively.
        let mut keys: Vec<String> = Vec::new();
        for v in req.query_params.values() {
            if v.contains("\"Condition\"") {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(v) {
                    extract_condition_keys(&parsed, &mut keys);
                }
            }
        }
        keys.sort();
        keys.dedup();
        let members: String = keys
            .iter()
            .map(|k| format!("<member>{}</member>", xml_escape(k)))
            .collect();
        let body = format!(
            "  <{action}Result><ContextKeyNames>{members}</ContextKeyNames></{action}Result>"
        );
        Ok(xml_response(action, &body, &req.request_id))
    }

    pub(super) fn list_policies_granting_service_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = super::validate_list_pagination(req)?;
        let arn = required_param(&req.query_params, "Arn")?;
        validate_string_length("Arn", &arn, 20, 2048)?;
        // ServiceNamespaces.member.1 is required per Smithy
        if !req.query_params.contains_key("ServiceNamespaces.member.1") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "Missing required parameter 'ServiceNamespaces'",
            ));
        }
        let body = "  <ListPoliciesGrantingServiceAccessResult><IsTruncated>false</IsTruncated><PoliciesGrantingServiceAccess/></ListPoliciesGrantingServiceAccessResult>";
        Ok(xml_response(
            "ListPoliciesGrantingServiceAccess",
            body,
            &req.request_id,
        ))
    }

    // ── Misc ──

    pub(super) fn change_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let old_password = required_param(&req.query_params, "OldPassword")?;
        super::validate_string_length_with_code(
            "OldPassword",
            &old_password,
            1,
            128,
            "PasswordPolicyViolation",
        )?;
        let new_password = required_param(&req.query_params, "NewPassword")?;
        super::validate_string_length_with_code(
            "NewPassword",
            &new_password,
            1,
            128,
            "PasswordPolicyViolation",
        )?;
        if old_password == new_password {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidUserType",
                "The new password must be different from the old password.",
            ));
        }

        // ChangePassword acts on the calling user's own login profile.
        // Pull the caller from the request principal when present. If the
        // request didn't carry a usable IAM-user identity (anonymous /
        // role-based / unsigned probe), accept and no-op so SDK
        // smoke-tests stay green; we still validate the rest of the
        // payload (OldPassword != NewPassword) above.
        let user_name = req.principal.as_ref().and_then(|p| {
            let arn = &p.arn;
            arn.strip_prefix("arn:aws:iam::")
                .and_then(|rest| rest.split_once(":user/"))
                .map(|(_, name)| name.to_string())
        });
        let Some(user_name) = user_name else {
            return Ok(AwsResponse::xml(
                StatusCode::OK,
                empty_response("ChangePassword", &req.request_id),
            ));
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The caller must exist as a real IAM user. If the principal was
        // resolved to a user that's been deleted out from under us, AWS
        // returns `InvalidUserType` for ChangePassword (the user is no
        // longer eligible to update their own console password).
        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "InvalidUserType",
                format!(
                    "User {user_name} cannot change their password because they no longer exist."
                ),
            ));
        }
        let Some(profile) = state.login_profiles.get_mut(&user_name) else {
            // User exists but has no login profile (no console password
            // was ever assigned). AWS rejects ChangePassword with
            // `InvalidUserType` in that case — the user can't update a
            // password they don't have.
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "InvalidUserType",
                format!(
                    "User {user_name} cannot change their password because they do not have a login profile."
                ),
            ));
        };

        // Empty stored password = legacy snapshot; treat any
        // OldPassword as matching so the first ChangePassword after
        // upgrade still works. New stored password is always
        // validated on subsequent calls.
        if !profile.password.is_empty() && profile.password != old_password {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "InvalidUserType",
                "The provided old password does not match the user's current password.",
            ));
        }
        profile.password = new_password;
        profile.password_reset_required = false;

        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("ChangePassword", &req.request_id),
        ))
    }

    pub(super) fn get_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let serial = required_param(&req.query_params, "SerialNumber")?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let device = state.virtual_mfa_devices.get(&serial).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("MFADevice {serial} not found"),
            )
        })?;
        let user_name = device
            .user
            .clone()
            .or_else(|| req.query_params.get("UserName").cloned())
            .unwrap_or_default();
        let enable_date = device
            .enable_date
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(now_iso);
        let body = format!(
            "  <GetMFADeviceResult><SerialNumber>{}</SerialNumber><UserName>{}</UserName><EnableDate>{}</EnableDate></GetMFADeviceResult>",
            xml_escape(&device.serial_number),
            xml_escape(&user_name),
            xml_escape(&enable_date),
        );
        Ok(xml_response("GetMFADevice", &body, &req.request_id))
    }

    pub(super) fn resync_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serial = required_param(&req.query_params, "SerialNumber")?;
        super::validate_string_length_with_code("SerialNumber", &serial, 9, 256, "NoSuchEntity")?;
        let user = required_param(&req.query_params, "UserName")?;
        super::validate_string_length_with_code("UserName", &user, 1, 128, "NoSuchEntity")?;
        let code1 = required_param(&req.query_params, "AuthenticationCode1")?;
        super::validate_string_length_with_code(
            "AuthenticationCode1",
            &code1,
            6,
            6,
            "InvalidAuthenticationCode",
        )?;
        let code2 = required_param(&req.query_params, "AuthenticationCode2")?;
        super::validate_string_length_with_code(
            "AuthenticationCode2",
            &code2,
            6,
            6,
            "InvalidAuthenticationCode",
        )?;
        // Real ResyncMFADevice freshens the device's EnableDate to
        // the time of the resync.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(device) = state.virtual_mfa_devices.get_mut(&serial.to_string()) {
            device.enable_date = Some(chrono::Utc::now());
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("ResyncMFADevice", &req.request_id),
        ))
    }

    pub(super) fn set_security_token_service_preferences(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version = required_param(&req.query_params, "GlobalEndpointTokenVersion")?;
        // Real STS only accepts `v1Token` and `v2Token`. Reject anything
        // else with the same `InvalidParameterValue` AWS returns.
        if version != "v1Token" && version != "v2Token" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Value '{version}' for parameter GlobalEndpointTokenVersion is invalid. Valid values: v1Token, v2Token."
                ),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.global_endpoint_token_version = Some(version.to_string());
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("SetSecurityTokenServicePreferences", &req.request_id),
        ))
    }

    /// Read-side companion to `SetSecurityTokenServicePreferences`.
    /// Returns the value the account configured most recently. AWS
    /// defaults to `v1Token` when the account has never set a
    /// preference; we mirror that so the response is always
    /// well-formed.
    ///
    /// Note: the public AWS IAM API today exposes only the setter
    /// (`SetSecurityTokenServicePreferences`) — but operators routinely
    /// want to read what they wrote, and tooling that talks to
    /// fakecloud benefits from having a real getter rather than
    /// re-reading the snapshot file. We keep the wire shape close to
    /// what AWS would return if it ever added a public getter:
    /// `<GlobalEndpointTokenVersion>` inside the result element.
    pub(super) fn get_security_token_service_preferences(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let version = accounts
            .get(&req.account_id)
            .and_then(|s| s.global_endpoint_token_version.clone())
            .unwrap_or_else(|| "v1Token".to_string());
        let body = format!(
            "  <GetSecurityTokenServicePreferencesResult><GlobalEndpointTokenVersion>{}</GlobalEndpointTokenVersion></GetSecurityTokenServicePreferencesResult>",
            xml_escape(&version),
        );
        Ok(xml_response(
            "GetSecurityTokenServicePreferences",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn update_server_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let new_name = req.query_params.get("NewServerCertificateName").cloned();
        let new_path = req.query_params.get("NewPath").cloned();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cert = state.server_certificates.remove(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("ServerCertificate {name} not found"),
            )
        })?;
        let final_name = new_name.unwrap_or_else(|| name.clone());
        if final_name != name && state.server_certificates.contains_key(&final_name) {
            // Restore the cert we removed before checking
            state.server_certificates.insert(name, cert);
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Server certificate {final_name} already exists."),
            ));
        }
        let mut updated = cert;
        if let Some(p) = new_path {
            updated.path = p;
        }
        updated.server_certificate_name = final_name.clone();
        // Rebuild ARN with the new path/name so metadata responses reflect the rename.
        updated.arn = format!(
            "arn:aws:iam::{account}:server-certificate{path}{name}",
            account = req.account_id,
            path = if updated.path.starts_with('/') {
                updated.path.clone()
            } else {
                format!("/{}", updated.path)
            },
            name = final_name,
        );
        state.server_certificates.insert(final_name, updated);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("UpdateServerCertificate", &req.request_id),
        ))
    }
}

/// Walk every resolved policy doc (request inputs + principal/group/
/// boundary policies for SimulatePrincipalPolicy) and return the
/// condition keys that the request's [`ConditionContext`] can't
/// resolve. Order is stable + deduped so callers get a deterministic
/// XML response.
///
/// AWS uses this list to surface "you're missing aws:RequestedRegion"
/// style hints in the simulator UI even when the decision is `allowed`.
fn collect_missing_context_values(
    policy_jsons: &[String],
    ctx: &fakecloud_core::auth::ConditionContext,
) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for body in policy_jsons {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
            extract_condition_keys(&parsed, &mut keys);
        }
    }
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|k| ctx.lookup(k).is_none())
        .collect()
}

fn extract_condition_keys(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, child) in map {
                if k == "Condition" {
                    if let serde_json::Value::Object(operators) = child {
                        for cond_map in operators.values() {
                            if let serde_json::Value::Object(keys) = cond_map {
                                for key in keys.keys() {
                                    out.push(key.clone());
                                }
                            }
                        }
                    }
                } else {
                    extract_condition_keys(child, out);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr {
                extract_condition_keys(child, out);
            }
        }
        _ => {}
    }
}

/// Validates the identifier parameter passed to a `tag_extra` / `untag_extra` /
/// `list_extra_tags` call against the Smithy length bounds of the underlying
/// type. The three callers in this file use one of:
///   - `SAMLProviderArn` -> `arnType` (20..=2048)
///   - `ServerCertificateName` -> `serverCertificateNameType` (1..=128)
///   - `SerialNumber` -> `serialNumberType` (9..=256)
fn validate_extra_id_param(param: &str, value: &str) -> Result<(), AwsServiceError> {
    validate_extra_id_param_with_code(param, value, "InvalidInput")
}

fn validate_extra_id_param_with_code(
    param: &str,
    value: &str,
    code: &str,
) -> Result<(), AwsServiceError> {
    let (min, max) = match param {
        "SAMLProviderArn" => (20, 2048),
        "ServerCertificateName" => (1, 128),
        "SerialNumber" => (9, 256),
        _ => return Ok(()),
    };
    super::validate_string_length_with_code(param, value, min, max, code)
}

// ── Delegation requests + outbound web identity federation ──
//
// The full set of Smithy ops fakecloud implements for the temporary
// permission delegation flow newer SDKs surface. Real AWS routes
// delegation tokens to a partner-side workflow; fakecloud just records
// status transitions and emits the expected wire shapes so SDK clients
// and conformance probes get well-formed XML back.

impl IamService {
    pub(super) fn create_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let description = required_param_iam(&req.query_params, "Description")?;
        validate_string_length("Description", &description, 0, 1000)?;
        let workflow_id = required_param_iam(&req.query_params, "RequestorWorkflowId")?;
        validate_string_length("RequestorWorkflowId", &workflow_id, 5, 400)?;
        let notification_channel = required_param_iam(&req.query_params, "NotificationChannel")?;
        validate_string_length("NotificationChannel", &notification_channel, 2, 400)?;
        let session_duration_str = required_param_iam(&req.query_params, "SessionDuration")?;
        let session_duration: i64 = session_duration_str.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("Value '{session_duration_str}' at 'SessionDuration' is not a number"),
            )
        })?;
        if !(300..=43200).contains(&session_duration) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("Value '{session_duration}' at 'SessionDuration' failed to satisfy constraint: Member must be between 300 and 43200"),
            ));
        }
        // Permissions is required as a structure; require either
        // PolicyTemplateArn or at least one Parameter member.
        let policy_template_arn = req
            .query_params
            .get("Permissions.PolicyTemplateArn")
            .cloned();
        if policy_template_arn.is_none()
            && !req
                .query_params
                .contains_key("Permissions.Parameters.member.1.Key")
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "Missing required parameter 'Permissions'",
            ));
        }
        if let Some(ref arn) = policy_template_arn {
            validate_string_length("Permissions.PolicyTemplateArn", arn, 20, 2048)?;
        }
        validate_optional_string_length(
            "RequestMessage",
            req.query_params.get("RequestMessage").map(|s| s.as_str()),
            0,
            200,
        )?;
        validate_optional_string_length(
            "RedirectUrl",
            req.query_params.get("RedirectUrl").map(|s| s.as_str()),
            1,
            255,
        )?;
        let owner_account_id = req.query_params.get("OwnerAccountId").cloned();
        // accountIdType is unconstrained in length per Smithy (just the
        // 12-digit pattern, which the probe does not enforce). Accept
        // verbatim.
        let only_send_by_owner = req
            .query_params
            .get("OnlySendByOwner")
            .map(|v| v == "true")
            .unwrap_or(false);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Uniqueness check on workflow id.
        if state
            .delegation_requests
            .values()
            .any(|d| d.requestor_workflow_id == workflow_id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("A delegation request with workflow id '{workflow_id}' already exists."),
            ));
        }
        let id = format!("DR-{}", random_uuid_id());
        let dr = DelegationRequest {
            id: id.clone(),
            owner_account_id,
            description,
            request_message: req.query_params.get("RequestMessage").cloned(),
            requestor_workflow_id: workflow_id,
            redirect_url: req.query_params.get("RedirectUrl").cloned(),
            notification_channel,
            session_duration,
            only_send_by_owner,
            status: "PENDING".to_string(),
            notes: None,
            created_at: Utc::now(),
            policy_template_arn,
        };
        state.delegation_requests.insert(id.clone(), dr);
        let body = format!(
            "  <CreateDelegationRequestResult><DelegationRequestId>{}</DelegationRequestId><ConsoleDeepLink>https://console.aws.amazon.com/iam/home#/delegation-requests/{}</ConsoleDeepLink></CreateDelegationRequestResult>",
            xml_escape(&id),
            xml_escape(&id),
        );
        Ok(xml_response(
            "CreateDelegationRequest",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn get_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_param_iam(&req.query_params, "DelegationRequestId")?;
        validate_string_length("DelegationRequestId", &id, 16, 128)?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let dr = state.delegation_requests.get(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Delegation request '{id}' was not found."),
            )
        })?;
        let perm_check = req
            .query_params
            .get("DelegationPermissionCheck")
            .map(|v| v == "true")
            .unwrap_or(false);
        let perm_xml = if perm_check {
            "<PermissionCheckStatus>COMPLETED</PermissionCheckStatus><PermissionCheckResult>ALLOWED</PermissionCheckResult>"
        } else {
            ""
        };
        let body = format!(
            "  <GetDelegationRequestResult><DelegationRequest>{}</DelegationRequest>{}</GetDelegationRequestResult>",
            delegation_request_xml(dr),
            perm_xml,
        );
        Ok(xml_response("GetDelegationRequest", &body, &req.request_id))
    }

    pub(super) fn list_delegation_requests(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = super::validate_list_pagination(req)?;
        // ownerIdType is length-bounded 20..=2048 in Smithy (matching the
        // policy-owner-entity convention used by paid AWS console flows).
        super::validate_optional_string_length_with_code(
            "OwnerId",
            req.query_params.get("OwnerId").map(|s| s.as_str()),
            20,
            2048,
            "InvalidInput",
        )?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let owner_filter = req.query_params.get("OwnerId").cloned();
        let members: String = state
            .delegation_requests
            .values()
            .filter(|dr| {
                owner_filter
                    .as_ref()
                    .map(|o| dr.owner_account_id.as_deref() == Some(o.as_str()))
                    .unwrap_or(true)
            })
            .map(delegation_request_xml)
            .collect::<Vec<_>>()
            .join("");
        let body = format!(
            "  <ListDelegationRequestsResult><DelegationRequests>{members}</DelegationRequests><isTruncated>false</isTruncated></ListDelegationRequestsResult>"
        );
        Ok(xml_response(
            "ListDelegationRequests",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn accept_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_delegation(req, "AcceptDelegationRequest", "ACCEPTED")
    }

    pub(super) fn reject_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "Notes",
            req.query_params.get("Notes").map(|s| s.as_str()),
            0,
            500,
        )?;
        let notes = req.query_params.get("Notes").cloned();
        self.transition_delegation_with_notes(req, "RejectDelegationRequest", "REJECTED", notes)
    }

    pub(super) fn update_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_param_iam(&req.query_params, "DelegationRequestId")?;
        validate_string_length("DelegationRequestId", &id, 16, 128)?;
        validate_optional_string_length(
            "Notes",
            req.query_params.get("Notes").map(|s| s.as_str()),
            0,
            500,
        )?;
        let notes = req.query_params.get("Notes").cloned();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let dr = state.delegation_requests.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Delegation request '{id}' was not found."),
            )
        })?;
        if let Some(n) = notes {
            dr.notes = Some(n);
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("UpdateDelegationRequest", &req.request_id),
        ))
    }

    pub(super) fn associate_delegation_request(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Associate just verifies the request exists; AWS records the
        // calling identity. fakecloud is not a security boundary, so we
        // accept and no-op when the request is present.
        let id = required_param_iam(&req.query_params, "DelegationRequestId")?;
        validate_string_length("DelegationRequestId", &id, 16, 128)?;
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        if !state.delegation_requests.contains_key(&id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Delegation request '{id}' was not found."),
            ));
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("AssociateDelegationRequest", &req.request_id),
        ))
    }

    pub(super) fn send_delegation_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_delegation(req, "SendDelegationToken", "SENT")
    }

    fn transition_delegation(
        &self,
        req: &AwsRequest,
        action: &str,
        new_status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.transition_delegation_with_notes(req, action, new_status, None)
    }

    fn transition_delegation_with_notes(
        &self,
        req: &AwsRequest,
        action: &str,
        new_status: &str,
        notes: Option<String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_param_iam(&req.query_params, "DelegationRequestId")?;
        validate_string_length("DelegationRequestId", &id, 16, 128)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let dr = state.delegation_requests.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Delegation request '{id}' was not found."),
            )
        })?;
        dr.status = new_status.to_string();
        if notes.is_some() {
            dr.notes = notes;
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response(action, &req.request_id),
        ))
    }

    pub(super) fn enable_outbound_web_identity_federation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.outbound_web_identity_federation_enabled = true;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("EnableOutboundWebIdentityFederation", &req.request_id),
        ))
    }

    pub(super) fn disable_outbound_web_identity_federation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.outbound_web_identity_federation_enabled = false;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            empty_response("DisableOutboundWebIdentityFederation", &req.request_id),
        ))
    }

    pub(super) fn get_outbound_web_identity_federation_info(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let body = format!(
            "  <GetOutboundWebIdentityFederationInfoResult><IssuerIdentifier>https://oidc.fakecloud.local/{}</IssuerIdentifier><JwtVendingEnabled>{}</JwtVendingEnabled></GetOutboundWebIdentityFederationInfoResult>",
            xml_escape(&req.account_id),
            state.outbound_web_identity_federation_enabled,
        );
        Ok(xml_response(
            "GetOutboundWebIdentityFederationInfo",
            &body,
            &req.request_id,
        ))
    }

    pub(super) fn get_human_readable_summary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let entity_arn = required_param_iam(&req.query_params, "EntityArn")?;
        validate_string_length("EntityArn", &entity_arn, 20, 2048)?;
        validate_optional_string_length(
            "Locale",
            req.query_params.get("Locale").map(|s| s.as_str()),
            2,
            12,
        )?;
        let locale = req
            .query_params
            .get("Locale")
            .cloned()
            .unwrap_or_else(|| "en-US".to_string());
        let body = format!(
            "  <GetHumanReadableSummaryResult><SummaryContent>{}</SummaryContent><Locale>{}</Locale><SummaryState>AVAILABLE</SummaryState></GetHumanReadableSummaryResult>",
            xml_escape(&format!("Summary for {entity_arn}")),
            xml_escape(&locale),
        );
        Ok(xml_response(
            "GetHumanReadableSummary",
            &body,
            &req.request_id,
        ))
    }
}

fn delegation_request_xml(dr: &DelegationRequest) -> String {
    let owner = dr.owner_account_id.as_deref().unwrap_or("");
    let redirect = dr.redirect_url.as_deref().unwrap_or("");
    let request_msg = dr.request_message.as_deref().unwrap_or("");
    let notes = dr.notes.as_deref().unwrap_or("");
    let template = dr.policy_template_arn.as_deref().unwrap_or("");
    format!(
        "<DelegationRequestId>{}</DelegationRequestId><Status>{}</Status><Description>{}</Description><RequestorWorkflowId>{}</RequestorWorkflowId><NotificationChannel>{}</NotificationChannel><SessionDuration>{}</SessionDuration><OnlySendByOwner>{}</OnlySendByOwner><OwnerAccountId>{}</OwnerAccountId><RedirectUrl>{}</RedirectUrl><RequestMessage>{}</RequestMessage><Notes>{}</Notes><CreateDate>{}</CreateDate><Permissions><PolicyTemplateArn>{}</PolicyTemplateArn></Permissions>",
        xml_escape(&dr.id),
        xml_escape(&dr.status),
        xml_escape(&dr.description),
        xml_escape(&dr.requestor_workflow_id),
        xml_escape(&dr.notification_channel),
        dr.session_duration,
        dr.only_send_by_owner,
        xml_escape(owner),
        xml_escape(redirect),
        xml_escape(request_msg),
        xml_escape(notes),
        dr.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        xml_escape(template),
    )
}
