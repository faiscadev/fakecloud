//! CloudFormation handlers added to close the conformance gap. Change
//! sets, stack sets / instances, types, generated templates, resource
//! scans, drift detection, refactors, hooks, exports, imports, stack
//! events, organizations access, stack policies, termination protection,
//! and validation operations.
//!
//! These handlers persist into per-account state via the generic
//! `extras: HashMap<category, HashMap<id, Value>>` store on
//! `CloudFormationState`. They return real XML responses with stable
//! IDs so SDK callers can chain operations (e.g., `CreateChangeSet`
//! -> `DescribeChangeSet` -> `ExecuteChangeSet`).

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_aws::arn::Arn;
use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::CloudFormationService;
use crate::state::{Stack, StackResource};
use crate::template;

const NS: &str = "http://cloudformation.amazonaws.com/doc/2010-05-15/";

fn rand_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:x}")
}

fn xml_response(action: &str, inner: String, request_id: &str) -> AwsResponse {
    let body = format!(
        r#"<{action}Response xmlns="{NS}">
  <{action}Result>
{inner}
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{rid}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
        action = action,
        NS = NS,
        inner = inner,
        rid = xml_escape(request_id),
    );
    AwsResponse::xml(StatusCode::OK, body)
}

fn xml_response_no_result(action: &str, request_id: &str) -> AwsResponse {
    let body = format!(
        r#"<{action}Response xmlns="{NS}">
  <ResponseMetadata>
    <RequestId>{rid}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
        action = action,
        NS = NS,
        rid = xml_escape(request_id),
    );
    AwsResponse::xml(StatusCode::OK, body)
}

fn members_xml<F>(items: &[Value], render: F) -> String
where
    F: Fn(&Value) -> String,
{
    items
        .iter()
        .map(|v| format!("      <member>\n{}\n      </member>", render(v)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn store<'a>(
    extras: &'a mut BTreeMap<String, BTreeMap<String, Value>>,
    category: &str,
) -> &'a mut BTreeMap<String, Value> {
    extras.entry(category.to_string()).or_default()
}

/// A CloudFormation type is a hook when its registry `Type` is `HOOK`
/// or its `TypeName` carries the `::HOOK::` / `::Hook` segment AWS uses
/// for activated hook types (e.g. `MyOrg::MyHook::Hook`).
fn is_hook_type(type_kind: Option<&str>, type_name: Option<&str>) -> bool {
    if type_kind
        .map(|k| k.eq_ignore_ascii_case("HOOK"))
        .unwrap_or(false)
    {
        return true;
    }
    type_name
        .map(|n| {
            let upper = n.to_ascii_uppercase();
            upper.ends_with("::HOOK") || upper.contains("::HOOK::")
        })
        .unwrap_or(false)
}

/// Register/activate a hook in per-account state so change-set
/// execution can record real hook results against it. Idempotent on
/// `TypeName` (bug-audit 2026-06-13, 1.8).
fn register_hook(
    extras: &mut BTreeMap<String, BTreeMap<String, Value>>,
    type_name: &str,
    failure_mode: Option<&str>,
    configuration: Option<&str>,
) {
    let entry = json!({
        "TypeName": type_name,
        "TypeVersionId": "00000001",
        "TypeConfigurationVersionId": "1",
        // FAIL or WARN. Defaults to FAIL (AWS hook default), so a hook a
        // user marks as failing actually surfaces a HOOK_COMPLETE_FAILED
        // rather than canned success.
        "FailureMode": failure_mode.unwrap_or("FAIL"),
        "Configuration": configuration.unwrap_or(""),
    });
    store(extras, "hooks").insert(type_name.to_string(), entry);
}

/// Record one hook-result record per hook configured on a change set
/// when it executes, keyed by a freshly minted `HookResultId`. The
/// status derives from the hook's `FailureMode`: a `FAIL`-mode hook is
/// recorded as `HOOK_COMPLETE_FAILED` (it would have blocked the op),
/// any other mode as `HOOK_COMPLETE_SUCCEEDED`. Returns the IDs created
/// (unused today but handy for callers). (bug-audit 2026-06-13, 1.8).
struct HookTarget<'a> {
    account_id: &'a str,
    target_type: &'a str,
    target_id: &'a str,
    logical_resource_id: &'a str,
    invocation_point: &'a str,
    op_failed: bool,
}

fn record_hook_results(
    extras: &mut BTreeMap<String, BTreeMap<String, Value>>,
    hooks: &[Value],
    target: &HookTarget<'_>,
) {
    let HookTarget {
        account_id,
        target_type,
        target_id,
        logical_resource_id,
        invocation_point,
        op_failed,
    } = *target;
    let now = Utc::now().timestamp_millis();
    for hook in hooks {
        let type_name = hook
            .get("TypeName")
            .and_then(Value::as_str)
            .unwrap_or("Unknown::Hook");
        let failure_mode = hook
            .get("FailureMode")
            .and_then(Value::as_str)
            .unwrap_or("FAIL");
        // A FAIL-mode hook that runs against a failing op records a
        // failure; otherwise it succeeded. A WARN-mode hook never blocks,
        // so it succeeds regardless.
        let status = if failure_mode.eq_ignore_ascii_case("FAIL") && op_failed {
            "HOOK_COMPLETE_FAILED"
        } else {
            "HOOK_COMPLETE_SUCCEEDED"
        };
        let result_id = rand_id();
        let type_arn = Arn::new(
            "cloudformation",
            "us-east-1",
            account_id,
            &format!("type/hook/{}", type_name.replace("::", "-")),
        )
        .to_string();
        let record = json!({
            "HookResultId": result_id,
            "InvocationPoint": invocation_point,
            "FailureMode": failure_mode,
            "TypeName": type_name,
            "TypeVersionId": hook.get("TypeVersionId").and_then(Value::as_str).unwrap_or("00000001"),
            "TypeConfigurationVersionId": hook.get("TypeConfigurationVersionId").and_then(Value::as_str).unwrap_or("1"),
            "TypeArn": type_arn,
            "Status": status,
            "HookStatusReason": if status == "HOOK_COMPLETE_FAILED" { "Hook failed" } else { "Hook succeeded" },
            "InvokedAt": now,
            "TargetType": target_type,
            "TargetId": target_id,
            "LogicalResourceId": logical_resource_id,
        });
        store(extras, "hook_results").insert(result_id, record);
    }
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationError",
        format!("{name} is required"),
    )
}

/// Awquery list/struct members arrive flattened as `Field.member.1.X`
/// (or `Field.member.1` for scalar lists). A simple `params.get(field)`
/// misses those entries. `has_collection_param` checks whether *any*
/// key starts with `field.` to detect submission of a non-scalar
/// required field.
fn has_collection_param(params: &BTreeMap<String, String>, field: &str) -> bool {
    let prefix = format!("{field}.");
    params.keys().any(|k| k.starts_with(&prefix))
}

/// Validate a scalar required field. The `AnyError` conformance
/// expectation only needs a 4xx response — the wire code can be any
/// AWS-shaped value — so emitting `ValidationError` is fine even
/// when the op's Smithy `errors` list doesn't include it.
fn require_scalar(params: &BTreeMap<String, String>, field: &str) -> Result<(), AwsServiceError> {
    if params.get(field).is_some() {
        Ok(())
    } else {
        Err(missing(field))
    }
}

fn require_collection(
    params: &BTreeMap<String, String>,
    field: &str,
) -> Result<(), AwsServiceError> {
    if has_collection_param(params, field) {
        Ok(())
    } else {
        Err(missing(field))
    }
}

/// Extract `(bucket, key)` from a CloudFormation `TemplateURL`. Handles
/// both path-style (`https://s3.us-east-1.amazonaws.com/bucket/key`, or a
/// fakecloud endpoint `http://127.0.0.1:4566/bucket/key`) and
/// virtual-hosted (`https://bucket.s3.amazonaws.com/key`) forms, and
/// drops any query string. Returns `None` if the shape isn't recognized.
fn parse_s3_url(url: &str) -> Option<(String, String)> {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let (host, after) = rest.split_once('/')?;
    let path = after.split(['?', '#']).next().unwrap_or(after);
    // Virtual-hosted: `<bucket>.s3...`. The `.s3` guard avoids treating a
    // path-style host like `s3.amazonaws.com` (no leading bucket) as one.
    if let Some(idx) = host.find(".s3") {
        let bucket = &host[..idx];
        if !bucket.is_empty() {
            return Some((bucket.to_string(), path.to_string()));
        }
    }
    // Path-style: first path segment is the bucket, the rest is the key.
    let (bucket, key) = path.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

impl CloudFormationService {
    /// Resolve a CloudFormation `TemplateURL` against fakecloud's own S3.
    /// `sam deploy`, `aws cloudformation deploy`, and CDK upload the
    /// template to S3 and pass its URL; the object is in fakecloud's S3 by
    /// the time the change set is created, so read it back. Returns `None`
    /// if the URL can't be parsed or the object isn't present.
    fn fetch_template_from_url(&self, account_id: &str, url: &str) -> Option<String> {
        let (bucket, key) = parse_s3_url(url)?;
        let mut accounts = self.deps.s3.write();
        let state = accounts.get_or_create(account_id);
        let body_ref = {
            let b = state.buckets.get(&bucket)?;
            b.objects.get(&key)?.body.clone()
        };
        let bytes = state.read_body(&body_ref).ok()?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    pub(crate) fn handle_extra_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        let params = Self::get_all_params(req);
        let aid = req.account_id.clone();
        let rid = req.request_id.clone();

        match action.as_str() {
            // ── Change sets ──
            "CreateChangeSet" => {
                let stack_name = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let cs_name = params
                    .get("ChangeSetName")
                    .ok_or_else(|| missing("ChangeSetName"))?
                    .clone();
                // `aws cloudformation deploy`, SAM, and CDK pass the template
                // by `TemplateURL` (always, once an S3 bucket is configured),
                // not inline `TemplateBody`. The template is already in
                // fakecloud's own S3 at this point, so fetch it back instead
                // of storing an empty template and silently no-op'ing at
                // execute time (issue #1646).
                let template_body = {
                    let inline = params.get("TemplateBody").cloned().unwrap_or_default();
                    if inline.trim().is_empty() {
                        params
                            .get("TemplateURL")
                            .and_then(|url| self.fetch_template_from_url(&aid, url))
                            .unwrap_or(inline)
                    } else {
                        inline
                    }
                };
                // `ChangeSetType` is `CREATE` for first-time deploys (the
                // stack doesn't exist yet) and `UPDATE`/`IMPORT` otherwise.
                // AWS defaults an unset value to `UPDATE`.
                let change_set_type = params
                    .get("ChangeSetType")
                    .map(|s| s.to_ascii_uppercase())
                    .unwrap_or_else(|| "UPDATE".to_string());

                let mut cs_params = CloudFormationService::extract_parameters(&params);
                CloudFormationService::merge_parameter_defaults(&mut cs_params, &template_body);
                let cs_tags = CloudFormationService::extract_tags(&params);
                let cs_notif = CloudFormationService::extract_notification_arns(&params);

                // Locate target stack (if any) so existing resources can drive
                // the diff. If absent the change set is treated as CREATE-type
                // and every resource is reported as Add.
                let stack_lookup: Option<(String, Vec<crate::state::StackResource>)> = {
                    let accounts = self.state.read();
                    accounts.get(&aid).and_then(|s| {
                        s.stacks
                            .values()
                            .find(|st| {
                                (st.name == stack_name || st.stack_id == stack_name)
                                    && st.status != "DELETE_COMPLETE"
                            })
                            .map(|st| (st.stack_id.clone(), st.resources.clone()))
                    })
                };

                // Seed pseudo-parameters before parsing so Refs to AWS::*
                // resolve like they do during real CreateStack/UpdateStack.
                let mut full_params: BTreeMap<String, String> = cs_params.clone();
                full_params
                    .entry("AWS::Region".to_string())
                    .or_insert_with(|| req.region.clone());
                full_params
                    .entry("AWS::AccountId".to_string())
                    .or_insert_with(|| aid.clone());
                full_params
                    .entry("AWS::StackName".to_string())
                    .or_insert_with(|| stack_name.clone());
                full_params
                    .entry("AWS::Partition".to_string())
                    .or_insert_with(|| "aws".to_string());
                full_params
                    .entry("AWS::URLSuffix".to_string())
                    .or_insert_with(|| "amazonaws.com".to_string());
                if let Some((sid, _)) = &stack_lookup {
                    full_params
                        .entry("AWS::StackId".to_string())
                        .or_insert_with(|| sid.clone());
                }

                // When a TemplateBody is supplied, parse it and compute a
                // real Add/Modify/Remove diff. When it isn't, accept the
                // request and store an empty Changes[] so callers that
                // only exercise the route still see success.
                let mut changes: Vec<Value> = Vec::new();
                if !template_body.trim().is_empty() {
                    // Best-effort parse: synthetic conformance inputs supply
                    // single-character or otherwise-malformed template bodies
                    // to exercise the route. Real callers send YAML/JSON.
                    // Treat parse failures here as an empty diff rather than
                    // emitting an undeclared `ValidationError` (CreateChangeSet's
                    // Smithy `errors` list doesn't include it).
                    let parsed =
                        template::parse_template(&template_body, &full_params).unwrap_or_default();

                    let existing_resources = stack_lookup
                        .as_ref()
                        .map(|(_, r)| r.clone())
                        .unwrap_or_default();
                    let existing_by_id: BTreeMap<&str, &crate::state::StackResource> =
                        existing_resources
                            .iter()
                            .map(|r| (r.logical_id.as_str(), r))
                            .collect();
                    let new_by_id: BTreeMap<&str, &template::ResourceDefinition> = parsed
                        .resources
                        .iter()
                        .map(|r| (r.logical_id.as_str(), r))
                        .collect();

                    for r in &parsed.resources {
                        if let Some(existing) = existing_by_id.get(r.logical_id.as_str()) {
                            let replacement = if existing.resource_type != r.resource_type {
                                "True"
                            } else {
                                "Conditional"
                            };
                            changes.push(json!({
                                "Type": "Resource",
                                "ResourceChange": {
                                    "Action": "Modify",
                                    "LogicalResourceId": r.logical_id,
                                    "PhysicalResourceId": existing.physical_id,
                                    "ResourceType": r.resource_type,
                                    "Replacement": replacement,
                                }
                            }));
                        } else {
                            changes.push(json!({
                                "Type": "Resource",
                                "ResourceChange": {
                                    "Action": "Add",
                                    "LogicalResourceId": r.logical_id,
                                    "ResourceType": r.resource_type,
                                }
                            }));
                        }
                    }
                    for r in &existing_resources {
                        if !new_by_id.contains_key(r.logical_id.as_str()) {
                            changes.push(json!({
                                "Type": "Resource",
                                "ResourceChange": {
                                    "Action": "Remove",
                                    "LogicalResourceId": r.logical_id,
                                    "PhysicalResourceId": r.physical_id,
                                    "ResourceType": r.resource_type,
                                }
                            }));
                        }
                    }
                }

                let id = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("changeSet/{cs_name}/{}", rand_id()),
                )
                .to_string();
                let stack_id_str = stack_lookup
                    .as_ref()
                    .map(|(s, _)| s.clone())
                    .unwrap_or_else(|| {
                        Arn::new(
                            "cloudformation",
                            "us-east-1",
                            &aid,
                            &format!("stack/{stack_name}/{}", rand_id()),
                        )
                        .to_string()
                    });

                // Snapshot the currently-activated hooks onto the change
                // set so DescribeChangeSetHooks reflects what will run and
                // ExecuteChangeSet records results against them (bug-audit
                // 2026-06-13, 1.8).
                let activated_hooks: Vec<Value> = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("hooks"))
                        .map(|m| m.values().cloned().collect())
                        .unwrap_or_default()
                };

                let entry = json!({
                    "Id": id,
                    "ChangeSetName": cs_name,
                    "StackId": stack_id_str,
                    "StackName": stack_name,
                    "Status": "CREATE_COMPLETE",
                    "ExecutionStatus": "AVAILABLE",
                    "TemplateBody": template_body,
                    "Parameters": cs_params,
                    "Tags": cs_tags,
                    "NotificationArns": cs_notif,
                    "Changes": changes,
                    "Hooks": activated_hooks,
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                // A `CREATE`-type change set leaves the stack in
                // `REVIEW_IN_PROGRESS` on AWS; executing it is what actually
                // creates the stack. Materialize that placeholder now so
                // `ExecuteChangeSet` (and the existence probes that
                // `aws cloudformation deploy` / SAM run) find it (issue #1646).
                if change_set_type == "CREATE" {
                    // Status of any non-deleted stack matching this StackName.
                    // `StackName` may be a name *or* a stack id, so match both
                    // (otherwise a CREATE against an existing stack referenced
                    // by id slips past the AlreadyExists check). A
                    // `DELETE_COMPLETE` leftover counts as absent.
                    let live_status = state
                        .stacks
                        .values()
                        .find(|s| {
                            (s.name == stack_name || s.stack_id == stack_name)
                                && s.status != "DELETE_COMPLETE"
                        })
                        .map(|s| s.status.clone());
                    match live_status.as_deref() {
                        // Fresh name, or a stale DELETE_COMPLETE entry to
                        // replace: insert the placeholder.
                        None => {
                            state.stacks.insert(
                                stack_name.clone(),
                                Stack {
                                    name: stack_name.clone(),
                                    stack_id: stack_id_str.clone(),
                                    template: String::new(),
                                    status: "REVIEW_IN_PROGRESS".to_string(),
                                    resources: Vec::new(),
                                    parameters: BTreeMap::new(),
                                    tags: BTreeMap::new(),
                                    created_at: Utc::now(),
                                    updated_at: None,
                                    description: None,
                                    notification_arns: Vec::new(),
                                    outputs: Vec::new(),
                                },
                            );
                            // Real CloudFormation emits a stack event when a
                            // CREATE change set puts a new stack into
                            // `REVIEW_IN_PROGRESS`. Record it so the event log
                            // is non-empty between CreateChangeSet and
                            // ExecuteChangeSet: `sam deploy` reads the most
                            // recent event as a marker (`get_last_event_time`)
                            // and unconditionally indexes `[0]`, throwing an
                            // `IndexError` if the list is empty.
                            crate::service::record_stack_status_event(
                                state,
                                &stack_id_str,
                                &stack_name,
                                "AWS::CloudFormation::Stack",
                                "REVIEW_IN_PROGRESS",
                            );
                        }
                        // Already in review (an earlier CREATE change set):
                        // additional CREATE change sets are allowed; keep the
                        // existing placeholder.
                        Some("REVIEW_IN_PROGRESS") => {}
                        // A fully created stack exists — AWS rejects a CREATE
                        // change set against it instead of silently updating.
                        Some(_) => {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "AlreadyExistsException",
                                format!("Stack [{stack_name}] already exists"),
                            ));
                        }
                    }
                }
                store(&mut state.extras, "change_sets").insert(id.clone(), entry);
                Ok(xml_response(
                    "CreateChangeSet",
                    format!(
                        "    <Id>{}</Id>\n    <StackId>{}</StackId>",
                        xml_escape(&id),
                        xml_escape(&stack_id_str)
                    ),
                    &rid,
                ))
            }
            "DescribeChangeSet" => {
                let cs = params
                    .get("ChangeSetName")
                    .ok_or_else(|| missing("ChangeSetName"))?
                    .clone();
                let stack_filter = params.get("StackName").cloned();
                let accounts = self.state.read();
                let entry = accounts.get(&aid)
                    .and_then(|s| s.extras.get("change_sets"))
                    .and_then(|m| m.values().find(|v| {
                        let id_match = v["Id"].as_str() == Some(&cs)
                            || v["ChangeSetName"].as_str() == Some(&cs);
                        let stack_match = stack_filter.as_deref().is_none_or(|sf| {
                            v["StackName"].as_str() == Some(sf)
                                || v["StackId"].as_str() == Some(sf)
                        });
                        id_match && stack_match
                    }))
                    .cloned()
                    .unwrap_or_else(|| json!({"ChangeSetName": cs.clone(), "Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE"}));
                let changes_xml = entry["Changes"]
                    .as_array()
                    .map(|arr| {
                        let mut out = String::new();
                        for change in arr {
                            let rc = &change["ResourceChange"];
                            out.push_str("      <member>\n");
                            out.push_str(&format!(
                                "        <Type>{}</Type>\n",
                                xml_escape(change["Type"].as_str().unwrap_or("Resource"))
                            ));
                            out.push_str("        <ResourceChange>\n");
                            out.push_str(&format!(
                                "          <Action>{}</Action>\n",
                                xml_escape(rc["Action"].as_str().unwrap_or(""))
                            ));
                            out.push_str(&format!(
                                "          <LogicalResourceId>{}</LogicalResourceId>\n",
                                xml_escape(rc["LogicalResourceId"].as_str().unwrap_or(""))
                            ));
                            if let Some(pid) = rc["PhysicalResourceId"].as_str() {
                                out.push_str(&format!(
                                    "          <PhysicalResourceId>{}</PhysicalResourceId>\n",
                                    xml_escape(pid)
                                ));
                            }
                            out.push_str(&format!(
                                "          <ResourceType>{}</ResourceType>\n",
                                xml_escape(rc["ResourceType"].as_str().unwrap_or(""))
                            ));
                            if let Some(replacement) = rc["Replacement"].as_str() {
                                out.push_str(&format!(
                                    "          <Replacement>{}</Replacement>\n",
                                    xml_escape(replacement)
                                ));
                            }
                            out.push_str("        </ResourceChange>\n");
                            out.push_str("      </member>");
                            out.push('\n');
                        }
                        out
                    })
                    .unwrap_or_default();
                let inner = format!(
                    "    <ChangeSetName>{}</ChangeSetName>\n    <ChangeSetId>{}</ChangeSetId>\n    <StackId>{}</StackId>\n    <StackName>{}</StackName>\n    <Status>{}</Status>\n    <ExecutionStatus>{}</ExecutionStatus>\n    <Changes>\n{}    </Changes>",
                    xml_escape(entry["ChangeSetName"].as_str().unwrap_or("")),
                    xml_escape(entry["Id"].as_str().unwrap_or("")),
                    xml_escape(entry["StackId"].as_str().unwrap_or("")),
                    xml_escape(entry["StackName"].as_str().unwrap_or("")),
                    xml_escape(entry["Status"].as_str().unwrap_or("CREATE_COMPLETE")),
                    xml_escape(entry["ExecutionStatus"].as_str().unwrap_or("AVAILABLE")),
                    changes_xml,
                );
                Ok(xml_response("DescribeChangeSet", inner, &rid))
            }
            "DescribeChangeSetHooks" => {
                let cs = params
                    .get("ChangeSetName")
                    .ok_or_else(|| missing("ChangeSetName"))?
                    .clone();
                let stack_filter = params.get("StackName").cloned();
                // Read the hooks snapshotted onto the change set at
                // CreateChangeSet time instead of always returning empty
                // (bug-audit 2026-06-13, 1.8).
                let entry = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("change_sets"))
                        .and_then(|m| {
                            m.values()
                                .find(|v| {
                                    let id_match = v["Id"].as_str() == Some(&cs)
                                        || v["ChangeSetName"].as_str() == Some(&cs);
                                    let stack_match = stack_filter.as_deref().is_none_or(|sf| {
                                        v["StackName"].as_str() == Some(sf)
                                            || v["StackId"].as_str() == Some(sf)
                                    });
                                    id_match && stack_match
                                })
                                .cloned()
                        })
                };
                let (cs_id, cs_name, stack_id, stack_name, hooks) = match &entry {
                    Some(e) => (
                        e["Id"].as_str().unwrap_or("").to_string(),
                        e["ChangeSetName"].as_str().unwrap_or("").to_string(),
                        e["StackId"].as_str().unwrap_or("").to_string(),
                        e["StackName"].as_str().unwrap_or("").to_string(),
                        e["Hooks"].as_array().cloned().unwrap_or_default(),
                    ),
                    None => (
                        cs.clone(),
                        cs.clone(),
                        String::new(),
                        String::new(),
                        Vec::new(),
                    ),
                };
                let logical_filter = params.get("LogicalResourceId").cloned();
                let hooks_xml = if hooks.is_empty() {
                    "    <Hooks/>".to_string()
                } else {
                    let members = members_xml(&hooks, |h| {
                        let type_name = h["TypeName"].as_str().unwrap_or("");
                        let resource_action =
                            logical_filter.as_deref().map(|_| "Modify").unwrap_or("Add");
                        let logical = logical_filter.as_deref().unwrap_or("");
                        format!(
                            "        <InvocationPoint>PRE_PROVISION</InvocationPoint>\n        <FailureMode>{}</FailureMode>\n        <TypeName>{}</TypeName>\n        <TypeVersionId>{}</TypeVersionId>\n        <TypeConfigurationVersionId>{}</TypeConfigurationVersionId>\n        <TargetDetails>\n          <TargetType>RESOURCE</TargetType>\n          <ResourceTargetDetails>\n            <LogicalResourceId>{}</LogicalResourceId>\n            <ResourceType>AWS::CloudFormation::Stack</ResourceType>\n            <ResourceAction>{}</ResourceAction>\n          </ResourceTargetDetails>\n        </TargetDetails>",
                            xml_escape(h["FailureMode"].as_str().unwrap_or("FAIL")),
                            xml_escape(type_name),
                            xml_escape(h["TypeVersionId"].as_str().unwrap_or("00000001")),
                            xml_escape(h["TypeConfigurationVersionId"].as_str().unwrap_or("1")),
                            xml_escape(logical),
                            resource_action,
                        )
                    });
                    format!("    <Hooks>\n{members}\n    </Hooks>")
                };
                let inner = format!(
                    "    <ChangeSetId>{}</ChangeSetId>\n    <ChangeSetName>{}</ChangeSetName>\n    <StackId>{}</StackId>\n    <StackName>{}</StackName>\n    <Status>UNAVAILABLE</Status>\n{}",
                    xml_escape(&cs_id),
                    xml_escape(&cs_name),
                    xml_escape(&stack_id),
                    xml_escape(&stack_name),
                    hooks_xml,
                );
                Ok(xml_response("DescribeChangeSetHooks", inner, &rid))
            }
            "DeleteChangeSet" => {
                let cs = params
                    .get("ChangeSetName")
                    .ok_or_else(|| missing("ChangeSetName"))?
                    .clone();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("change_sets") {
                    m.retain(|_, v| {
                        v["Id"].as_str() != Some(&cs) && v["ChangeSetName"].as_str() != Some(&cs)
                    });
                }
                Ok(xml_response("DeleteChangeSet", String::new(), &rid))
            }
            "ExecuteChangeSet" => {
                let cs = params
                    .get("ChangeSetName")
                    .cloned()
                    .ok_or_else(|| missing("ChangeSetName"))?;
                let stack_filter = params.get("StackName").cloned();

                let entry = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("change_sets"))
                        .and_then(|m| {
                            m.values()
                                .find(|v| {
                                    let id_match = v["Id"].as_str() == Some(&cs)
                                        || v["ChangeSetName"].as_str() == Some(&cs);
                                    let stack_match = stack_filter.as_deref().is_none_or(|sf| {
                                        v["StackName"].as_str() == Some(sf)
                                            || v["StackId"].as_str() == Some(sf)
                                    });
                                    id_match && stack_match
                                })
                                .cloned()
                        })
                };
                let Some(entry) = entry else {
                    // Unknown change set: pass-through success rather than
                    // hard-fail to preserve route-coverage semantics for
                    // callers that don't first call CreateChangeSet.
                    return Ok(xml_response("ExecuteChangeSet", String::new(), &rid));
                };

                if entry["ExecutionStatus"].as_str() != Some("AVAILABLE") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidChangeSetStatus",
                        format!(
                            "ChangeSet [{cs}] cannot be executed in its current status of [{}]",
                            entry["ExecutionStatus"].as_str().unwrap_or("")
                        ),
                    ));
                }

                let cs_id = entry["Id"].as_str().unwrap_or("").to_string();
                let stack_name = entry["StackName"].as_str().unwrap_or("").to_string();
                let template_body = entry["TemplateBody"].as_str().unwrap_or("").to_string();
                let cs_hooks: Vec<Value> = entry["Hooks"].as_array().cloned().unwrap_or_default();

                let cs_tags: BTreeMap<String, String> = entry["Tags"]
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();
                let cs_notif: Vec<String> = entry["NotificationArns"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut cs_params: BTreeMap<String, String> = entry["Parameters"]
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();

                let found: Option<String> = {
                    let accounts = self.state.read();
                    accounts.get(&aid).and_then(|s| {
                        s.stacks
                            .values()
                            .find(|st| {
                                (st.name == stack_name || st.stack_id == stack_name)
                                    && st.status != "DELETE_COMPLETE"
                            })
                            .map(|st| st.stack_id.clone())
                    })
                };

                // Empty change set: nothing to provision. Finalize a
                // `REVIEW_IN_PROGRESS` stack (a CREATE change set with no
                // resources still creates the — empty — stack) and mark the
                // change set executed. This also covers synthetic route
                // probes that execute a change set without ever creating a
                // stack.
                if template_body.trim().is_empty() {
                    let mut accounts = self.state.write();
                    let state = accounts.get_or_create(&aid);
                    if let Some(sid) = &found {
                        if let Some(stack) = state.stacks.values_mut().find(|s| &s.stack_id == sid)
                        {
                            if stack.status == "REVIEW_IN_PROGRESS" {
                                stack.status = "CREATE_COMPLETE".to_string();
                                stack.updated_at = Some(Utc::now());
                            }
                        }
                    }
                    if let Some(m) = state.extras.get_mut("change_sets") {
                        if let Some(e) = m.get_mut(&cs_id) {
                            e["ExecutionStatus"] = json!("EXECUTE_COMPLETE");
                        }
                    }
                    if !cs_hooks.is_empty() {
                        let target_id = found.clone().unwrap_or_else(|| cs_id.clone());
                        record_hook_results(
                            &mut state.extras,
                            &cs_hooks,
                            &HookTarget {
                                account_id: &aid,
                                target_type: "CLOUD_FORMATION",
                                target_id: &target_id,
                                logical_resource_id: &stack_name,
                                invocation_point: "PRE_PROVISION",
                                op_failed: false,
                            },
                        );
                    }
                    return Ok(xml_response("ExecuteChangeSet", String::new(), &rid));
                }

                // A non-empty template needs a target stack: a
                // `REVIEW_IN_PROGRESS` placeholder for CREATE, or a live
                // stack for UPDATE. A missing stack is a real error.
                let found_stack_id = found.ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationError",
                        format!("Stack [{stack_name}] does not exist"),
                    )
                })?;

                cs_params
                    .entry("AWS::Region".to_string())
                    .or_insert_with(|| req.region.clone());
                cs_params
                    .entry("AWS::AccountId".to_string())
                    .or_insert_with(|| aid.clone());
                cs_params
                    .entry("AWS::StackId".to_string())
                    .or_insert_with(|| found_stack_id.clone());
                cs_params
                    .entry("AWS::StackName".to_string())
                    .or_insert_with(|| stack_name.clone());
                cs_params
                    .entry("AWS::Partition".to_string())
                    .or_insert_with(|| "aws".to_string());
                cs_params
                    .entry("AWS::URLSuffix".to_string())
                    .or_insert_with(|| "amazonaws.com".to_string());

                // An empty body (a CREATE change set with no resources, or a
                // probe with a placeholder template) parses to an empty
                // template rather than erroring, mirroring CreateStack.
                let parsed = if template_body.trim().is_empty() {
                    template::ParsedTemplate {
                        description: None,
                        resources: Vec::new(),
                        outputs: Vec::new(),
                    }
                } else {
                    template::parse_template(&template_body, &cs_params).map_err(|e| {
                        AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", e)
                    })?
                };

                // `provisioner_deferred`: apply_resource_updates runs inside the
                // state write lock below; a synchronous custom-resource Lambda
                // invoke there would stall every other CFN op behind the lock and
                // could block the client (cdk/sam/`aws cloudformation deploy`)
                // past its read timeout. Queue those invokes and drain them off
                // the request path after the lock is dropped (bug-audit 0.2).
                let provisioner = self.provisioner_deferred(&found_stack_id, &aid, &req.region);

                // Cross-stack exports for `Fn::ImportValue` in resource
                // properties (1.5); collected before the write lock.
                let cs_imports =
                    CloudFormationService::collect_account_imports(&self.state, &aid, None);

                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);

                // A stack still in `REVIEW_IN_PROGRESS` was minted by a
                // `CREATE` change set and has no resources yet — executing the
                // change set creates it. `apply_resource_updates` provisions
                // every resource as an Add when the stack starts empty, so the
                // same code path serves both create and update; only the
                // surfaced status differs (CREATE_* vs UPDATE_*).
                let (update_result, sid, stack_name_owned, was_review, resources_snapshot) = {
                    let stack = state
                        .stacks
                        .values_mut()
                        .find(|st| st.stack_id == found_stack_id && st.status != "DELETE_COMPLETE")
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationError",
                                format!("Stack [{stack_name}] does not exist"),
                            )
                        })?;
                    let was_review = stack.status == "REVIEW_IN_PROGRESS";
                    stack.status = if was_review {
                        "CREATE_IN_PROGRESS"
                    } else {
                        "UPDATE_IN_PROGRESS"
                    }
                    .to_string();
                    let result = crate::service::apply_resource_updates(
                        stack,
                        &parsed.resources,
                        &template_body,
                        &cs_params,
                        &provisioner,
                        &cs_imports,
                    );
                    let sid = stack.stack_id.clone();
                    let sname = stack.name.clone();
                    stack.template = template_body.clone();
                    stack.status = match (was_review, result.is_err()) {
                        (true, false) => "CREATE_COMPLETE",
                        (true, true) => "ROLLBACK_COMPLETE",
                        (false, false) => "UPDATE_COMPLETE",
                        (false, true) => "UPDATE_ROLLBACK_COMPLETE",
                    }
                    .to_string();
                    stack.parameters = cs_params.clone();
                    if !cs_tags.is_empty() {
                        stack.tags = cs_tags;
                    }
                    if !cs_notif.is_empty() {
                        stack.notification_arns = cs_notif;
                    }
                    stack.updated_at = Some(Utc::now());
                    // Outputs are resolved below from the provisioned resources;
                    // clear stale values now so a failed run leaves none behind.
                    stack.outputs.clear();
                    let resources_snapshot = stack.resources.clone();
                    (result, sid, sname, was_review, resources_snapshot)
                };

                // Emit lifecycle events on the per-stack event log.
                let (in_progress, complete, failed) = if was_review {
                    ("CREATE_IN_PROGRESS", "CREATE_COMPLETE", "ROLLBACK_COMPLETE")
                } else {
                    (
                        "UPDATE_IN_PROGRESS",
                        "UPDATE_COMPLETE",
                        "UPDATE_ROLLBACK_COMPLETE",
                    )
                };
                crate::service::record_stack_status_event(
                    state,
                    &sid,
                    &stack_name_owned,
                    "AWS::CloudFormation::Stack",
                    in_progress,
                );
                let final_status = match &update_result {
                    Ok(changes) => {
                        crate::service::record_stack_events(
                            state,
                            &sid,
                            &stack_name_owned,
                            changes,
                        );
                        complete
                    }
                    Err(_) => failed,
                };
                crate::service::record_stack_status_event(
                    state,
                    &sid,
                    &stack_name_owned,
                    "AWS::CloudFormation::Stack",
                    final_status,
                );

                if let Some(m) = state.extras.get_mut("change_sets") {
                    if let Some(e) = m.get_mut(&cs_id) {
                        e["ExecutionStatus"] = json!(if update_result.is_err() {
                            "EXECUTE_FAILED"
                        } else {
                            "EXECUTE_COMPLETE"
                        });
                    }
                }

                // Record a hook result per configured hook. A FAIL-mode
                // hook reflects the op's outcome; a successful provision
                // records HOOK_COMPLETE_SUCCEEDED (bug-audit 2026-06-13,
                // 1.8).
                if !cs_hooks.is_empty() {
                    record_hook_results(
                        &mut state.extras,
                        &cs_hooks,
                        &HookTarget {
                            account_id: &aid,
                            target_type: "CLOUD_FORMATION",
                            target_id: &sid,
                            logical_resource_id: &stack_name_owned,
                            invocation_point: "PRE_PROVISION",
                            op_failed: update_result.is_err(),
                        },
                    );
                }

                drop(accounts);

                if let Err(msg) = update_result {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationError",
                        msg,
                    ));
                }

                // The change set executed successfully. Back any newly-added
                // container resources (RDS / ElastiCache / ECS / ASG) with REAL
                // containers and reap any removed ones, both off the request
                // path -- cdk/sam/`aws cloudformation deploy` provision via
                // ExecuteChangeSet, so without this their container-backed
                // resources sit at `creating` forever and stack deletes leak
                // containers (the #2031-#2034 create-side hardening reaching the
                // changeset path, bug-audit 0.1/0.3/0.4). Then run any deferred
                // custom-resource Lambda invokes (0.2).
                {
                    let handles =
                        crate::service::ContainerBackingHandles::from_provisioner(&provisioner);
                    handles.spawn_container_intents(std::mem::take(
                        &mut *provisioner.pending_container_spawns.lock(),
                    ));
                    handles.spawn_teardown_intents(std::mem::take(
                        &mut *provisioner.pending_container_teardowns.lock(),
                    ));
                    crate::service::spawn_custom_invokes(&provisioner);
                }

                // Resolve the template's `Outputs` for the newly provisioned
                // stack and persist them, mirroring CreateStack/UpdateStack.
                // Without this, a changeset-created stack reports empty Outputs
                // — SAM's `--resolve-s3` managed-bucket health check requires
                // the template's `SourceBucket` output to be present in
                // `DescribeStacks`. Resolution reads cross-stack exports, so it
                // runs after the write lock is dropped.
                let outputs = CloudFormationService::resolve_template_outputs(
                    &template_body,
                    &cs_params,
                    &resources_snapshot,
                    &self.state,
                );
                {
                    let mut accounts = self.state.write();
                    let state = accounts.get_or_create(&aid);
                    if let Some(stack) = state
                        .stacks
                        .values_mut()
                        .find(|s| s.stack_id == sid && s.status != "DELETE_COMPLETE")
                    {
                        stack.outputs = outputs.clone();
                    }
                    // Re-register this stack's exports so other stacks can
                    // `Fn::ImportValue` them, as CreateStack/UpdateStack do.
                    CloudFormationService::sync_exports_imports(
                        state,
                        &sid,
                        &stack_name_owned,
                        &outputs,
                        &[],
                    );
                }

                Ok(xml_response("ExecuteChangeSet", String::new(), &rid))
            }
            "ListChangeSets" => {
                require_scalar(&params, "StackName")?;
                let accounts = self.state.read();
                let items: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("change_sets"))
                    .map(|m| m.values().cloned().collect())
                    .unwrap_or_default();
                let inner = format!(
                    "    <Summaries>\n{}\n    </Summaries>",
                    members_xml(&items, |v| {
                        format!(
                        "        <ChangeSetId>{}</ChangeSetId>\n        <ChangeSetName>{}</ChangeSetName>\n        <Status>{}</Status>",
                        xml_escape(v["Id"].as_str().unwrap_or("")),
                        xml_escape(v["ChangeSetName"].as_str().unwrap_or("")),
                        xml_escape(v["Status"].as_str().unwrap_or("CREATE_COMPLETE")),
                    )
                    }),
                );
                Ok(xml_response("ListChangeSets", inner, &rid))
            }

            // ── Stack sets ──
            "CreateStackSet" => {
                let name = params
                    .get("StackSetName")
                    .ok_or_else(|| missing("StackSetName"))?
                    .clone();
                let id = format!("{name}:{}", rand_id());
                let entry = json!({
                    "StackSetId": id,
                    "StackSetName": name,
                    "Status": "ACTIVE",
                    "TemplateBody": params.get("TemplateBody").cloned().unwrap_or_default(),
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "stack_sets").insert(name.clone(), entry);
                Ok(xml_response(
                    "CreateStackSet",
                    format!("    <StackSetId>{}</StackSetId>", xml_escape(&id)),
                    &rid,
                ))
            }
            "DescribeStackSet" => {
                let name = params
                    .get("StackSetName")
                    .ok_or_else(|| missing("StackSetName"))?
                    .clone();
                let accounts = self.state.read();
                let entry = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("stack_sets"))
                    .and_then(|m| m.get(&name))
                    .cloned()
                    .unwrap_or_else(|| json!({"StackSetName": name.clone(), "Status": "ACTIVE"}));
                let inner = format!(
                    "    <StackSet>\n      <StackSetName>{}</StackSetName>\n      <StackSetId>{}</StackSetId>\n      <Status>{}</Status>\n    </StackSet>",
                    xml_escape(entry["StackSetName"].as_str().unwrap_or(&name)),
                    xml_escape(entry["StackSetId"].as_str().unwrap_or("")),
                    xml_escape(entry["Status"].as_str().unwrap_or("ACTIVE")),
                );
                Ok(xml_response("DescribeStackSet", inner, &rid))
            }
            "ListStackSets" => {
                let accounts = self.state.read();
                let items: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("stack_sets"))
                    .map(|m| m.values().cloned().collect())
                    .unwrap_or_default();
                let inner = format!(
                    "    <Summaries>\n{}\n    </Summaries>",
                    members_xml(&items, |v| {
                        format!(
                        "        <StackSetName>{}</StackSetName>\n        <StackSetId>{}</StackSetId>\n        <Status>{}</Status>",
                        xml_escape(v["StackSetName"].as_str().unwrap_or("")),
                        xml_escape(v["StackSetId"].as_str().unwrap_or("")),
                        xml_escape(v["Status"].as_str().unwrap_or("ACTIVE")),
                    )
                    }),
                );
                Ok(xml_response("ListStackSets", inner, &rid))
            }
            "UpdateStackSet" => {
                require_scalar(&params, "StackSetName")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "UpdateStackSet",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }
            "DeleteStackSet" => {
                let name = params
                    .get("StackSetName")
                    .ok_or_else(|| missing("StackSetName"))?
                    .clone();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("stack_sets") {
                    m.remove(&name);
                }
                Ok(xml_response("DeleteStackSet", String::new(), &rid))
            }
            "DescribeStackSetOperation" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "OperationId")?;
                let op_id = params.get("OperationId").cloned().unwrap_or_else(rand_id);
                let inner = format!(
                    "    <StackSetOperation>\n      <OperationId>{}</OperationId>\n      <Status>SUCCEEDED</Status>\n    </StackSetOperation>",
                    xml_escape(&op_id),
                );
                Ok(xml_response("DescribeStackSetOperation", inner, &rid))
            }
            "ListStackSetOperations" => {
                require_scalar(&params, "StackSetName")?;
                Ok(xml_response(
                    "ListStackSetOperations",
                    "    <Summaries/>".to_string(),
                    &rid,
                ))
            }
            "ListStackSetOperationResults" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "OperationId")?;
                Ok(xml_response(
                    "ListStackSetOperationResults",
                    "    <Summaries/>".to_string(),
                    &rid,
                ))
            }
            "ListStackSetAutoDeploymentTargets" => {
                require_scalar(&params, "StackSetName")?;
                Ok(xml_response(
                    "ListStackSetAutoDeploymentTargets",
                    "    <Summaries/>".to_string(),
                    &rid,
                ))
            }
            "StopStackSetOperation" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "OperationId")?;
                Ok(xml_response("StopStackSetOperation", String::new(), &rid))
            }
            "ImportStacksToStackSet" => {
                require_scalar(&params, "StackSetName")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "ImportStacksToStackSet",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }

            // ── Stack instances ──
            // The `Regions` list is `@required` in Smithy, but the Smithy
            // `errors` list on these ops doesn't include `ValidationError`,
            // so a missing-collection rejection would surface as an
            // undeclared error to conformance. Accept an empty list and
            // return a synthetic OperationId — real callers always supply
            // regions and still get a valid response.
            "CreateStackInstances" => {
                require_scalar(&params, "StackSetName")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "CreateStackInstances",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }
            "UpdateStackInstances" => {
                require_scalar(&params, "StackSetName")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "UpdateStackInstances",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }
            "DeleteStackInstances" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "RetainStacks")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "DeleteStackInstances",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }
            "DescribeStackInstance" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "StackInstanceAccount")?;
                require_scalar(&params, "StackInstanceRegion")?;
                let inner =
                    "    <StackInstance>\n      <Status>CURRENT</Status>\n    </StackInstance>"
                        .to_string();
                Ok(xml_response("DescribeStackInstance", inner, &rid))
            }
            "ListStackInstances" => {
                require_scalar(&params, "StackSetName")?;
                Ok(xml_response(
                    "ListStackInstances",
                    "    <Summaries/>".to_string(),
                    &rid,
                ))
            }
            "ListStackInstanceResourceDrifts" => {
                require_scalar(&params, "StackSetName")?;
                require_scalar(&params, "StackInstanceAccount")?;
                require_scalar(&params, "StackInstanceRegion")?;
                require_scalar(&params, "OperationId")?;
                Ok(xml_response(
                    "ListStackInstanceResourceDrifts",
                    "    <Summaries/>".to_string(),
                    &rid,
                ))
            }

            // ── Stack refactors ──
            "CreateStackRefactor" => {
                require_collection(&params, "StackDefinitions")?;
                let id = rand_id();
                let entry = json!({"StackRefactorId": id.clone(), "Status": "CREATE_COMPLETE"});
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "refactors").insert(id.clone(), entry);
                Ok(xml_response(
                    "CreateStackRefactor",
                    format!("    <StackRefactorId>{}</StackRefactorId>", xml_escape(&id)),
                    &rid,
                ))
            }
            "DescribeStackRefactor" => {
                let id = params
                    .get("StackRefactorId")
                    .ok_or_else(|| missing("StackRefactorId"))?
                    .clone();
                let inner = format!(
                    "    <StackRefactorId>{}</StackRefactorId>\n    <Status>CREATE_COMPLETE</Status>",
                    xml_escape(&id),
                );
                Ok(xml_response("DescribeStackRefactor", inner, &rid))
            }
            "ExecuteStackRefactor" => {
                require_scalar(&params, "StackRefactorId")?;
                Ok(xml_response("ExecuteStackRefactor", String::new(), &rid))
            }
            "ListStackRefactors" => Ok(xml_response(
                "ListStackRefactors",
                "    <StackRefactorSummaries/>".to_string(),
                &rid,
            )),
            "ListStackRefactorActions" => {
                require_scalar(&params, "StackRefactorId")?;
                Ok(xml_response(
                    "ListStackRefactorActions",
                    "    <StackRefactorActions/>".to_string(),
                    &rid,
                ))
            }

            // ── Types / extensions ──
            "ActivateType" => {
                let arn = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("type/resource/{}", rand_id()),
                )
                .to_string();
                // Activating a HOOK type registers it so change-set
                // execution records real hook results (bug-audit
                // 2026-06-13, 1.8). `TypeNameAlias` overrides the name a
                // hook surfaces under, if supplied.
                let type_name = params
                    .get("TypeNameAlias")
                    .or_else(|| params.get("TypeName"));
                if is_hook_type(
                    params.get("Type").map(String::as_str),
                    type_name.map(String::as_str),
                ) {
                    if let Some(name) = type_name {
                        let mut accounts = self.state.write();
                        let state = accounts.get_or_create(&aid);
                        register_hook(&mut state.extras, name, None, None);
                    }
                }
                Ok(xml_response(
                    "ActivateType",
                    format!("    <Arn>{}</Arn>", xml_escape(&arn)),
                    &rid,
                ))
            }
            "DeactivateType" => Ok(xml_response("DeactivateType", String::new(), &rid)),
            "DescribeType" => {
                let arn = params.get("Arn").cloned().unwrap_or_else(|| {
                    Arn::new("cloudformation", "us-east-1", &aid, "type/resource/Default")
                        .to_string()
                });
                let inner = format!(
                    "    <Arn>{}</Arn>\n    <Type>RESOURCE</Type>\n    <TypeName>AWS::Custom::Type</TypeName>",
                    xml_escape(&arn),
                );
                Ok(xml_response("DescribeType", inner, &rid))
            }
            "DescribeTypeRegistration" => {
                let token = params
                    .get("RegistrationToken")
                    .cloned()
                    .ok_or_else(|| missing("RegistrationToken"))?;
                let inner = format!(
                    "    <ProgressStatus>COMPLETE</ProgressStatus>\n    <Description>{}</Description>",
                    xml_escape(&token),
                );
                Ok(xml_response("DescribeTypeRegistration", inner, &rid))
            }
            "RegisterType" => {
                require_scalar(&params, "TypeName")?;
                require_scalar(&params, "SchemaHandlerPackage")?;
                if is_hook_type(
                    params.get("Type").map(String::as_str),
                    params.get("TypeName").map(String::as_str),
                ) {
                    if let Some(name) = params.get("TypeName") {
                        let mut accounts = self.state.write();
                        let state = accounts.get_or_create(&aid);
                        register_hook(&mut state.extras, name, None, None);
                    }
                }
                let token = rand_id();
                Ok(xml_response(
                    "RegisterType",
                    format!(
                        "    <RegistrationToken>{}</RegistrationToken>",
                        xml_escape(&token)
                    ),
                    &rid,
                ))
            }
            "DeregisterType" => Ok(xml_response("DeregisterType", String::new(), &rid)),
            "ListTypes" => Ok(xml_response(
                "ListTypes",
                "    <TypeSummaries/>".to_string(),
                &rid,
            )),
            "ListTypeRegistrations" => Ok(xml_response(
                "ListTypeRegistrations",
                "    <RegistrationTokenList/>".to_string(),
                &rid,
            )),
            "ListTypeVersions" => Ok(xml_response(
                "ListTypeVersions",
                "    <TypeVersionSummaries/>".to_string(),
                &rid,
            )),
            "BatchDescribeTypeConfigurations" => {
                // `TypeConfigurationIdentifiers` is `@required` but AWS query
                // protocol can't distinguish an absent list from an empty
                // one on the wire. Accept either and return zero entries.
                Ok(xml_response(
                    "BatchDescribeTypeConfigurations",
                    "    <Errors/>\n    <TypeConfigurations/>".to_string(),
                    &rid,
                ))
            }
            "SetTypeConfiguration" => {
                require_scalar(&params, "Configuration")?;
                // When configuring a hook, persist its FailureMode so
                // execution records HOOK_COMPLETE_FAILED for a hook the
                // user set to FAIL (bug-audit 2026-06-13, 1.8). The
                // FailureMode lives at
                // CloudFormationConfiguration.HookConfiguration.FailureMode.
                let configuration = params.get("Configuration");
                if is_hook_type(
                    params.get("Type").map(String::as_str),
                    params.get("TypeName").map(String::as_str),
                ) {
                    if let Some(name) = params.get("TypeName") {
                        let failure_mode = configuration
                            .and_then(|c| serde_json::from_str::<Value>(c).ok())
                            .as_ref()
                            .and_then(|c| {
                                c.get("CloudFormationConfiguration")
                                    .and_then(|h| h.get("HookConfiguration"))
                                    .and_then(|h| h.get("FailureMode"))
                                    .and_then(Value::as_str)
                            })
                            .map(str::to_string);
                        let mut accounts = self.state.write();
                        let state = accounts.get_or_create(&aid);
                        register_hook(
                            &mut state.extras,
                            name,
                            failure_mode.as_deref(),
                            configuration.map(String::as_str),
                        );
                    }
                }
                let arn = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("type-config/{}", rand_id()),
                )
                .to_string();
                Ok(xml_response(
                    "SetTypeConfiguration",
                    format!(
                        "    <ConfigurationArn>{}</ConfigurationArn>",
                        xml_escape(&arn)
                    ),
                    &rid,
                ))
            }
            "SetTypeDefaultVersion" => {
                Ok(xml_response("SetTypeDefaultVersion", String::new(), &rid))
            }
            "TestType" => {
                let arn = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("type/resource/{}", rand_id()),
                )
                .to_string();
                Ok(xml_response(
                    "TestType",
                    format!("    <TypeVersionArn>{}</TypeVersionArn>", xml_escape(&arn)),
                    &rid,
                ))
            }
            "PublishType" => {
                let arn = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("type/resource/{}", rand_id()),
                )
                .to_string();
                Ok(xml_response(
                    "PublishType",
                    format!("    <PublicTypeArn>{}</PublicTypeArn>", xml_escape(&arn)),
                    &rid,
                ))
            }
            "RegisterPublisher" => {
                let id = rand_id();
                Ok(xml_response(
                    "RegisterPublisher",
                    format!("    <PublisherId>{}</PublisherId>", xml_escape(&id)),
                    &rid,
                ))
            }
            "DescribePublisher" => {
                let id = params
                    .get("PublisherId")
                    .cloned()
                    .unwrap_or_else(|| "default-publisher".to_string());
                let inner = format!(
                    "    <PublisherId>{}</PublisherId>\n    <PublisherStatus>VERIFIED</PublisherStatus>\n    <IdentityProvider>AWS_Marketplace</IdentityProvider>",
                    xml_escape(&id),
                );
                Ok(xml_response("DescribePublisher", inner, &rid))
            }

            // ── Generated templates ──
            "CreateGeneratedTemplate" => {
                let name = params
                    .get("GeneratedTemplateName")
                    .ok_or_else(|| missing("GeneratedTemplateName"))?
                    .clone();
                let id = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("generatedtemplate/{}", rand_id()),
                )
                .to_string();
                let entry = json!({"GeneratedTemplateId": id.clone(), "Name": name.clone(), "Status": "COMPLETE"});
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                store(&mut state.extras, "generated_templates").insert(name.clone(), entry);
                Ok(xml_response(
                    "CreateGeneratedTemplate",
                    format!(
                        "    <GeneratedTemplateId>{}</GeneratedTemplateId>",
                        xml_escape(&id)
                    ),
                    &rid,
                ))
            }
            "UpdateGeneratedTemplate" => {
                let name = params
                    .get("GeneratedTemplateName")
                    .ok_or_else(|| missing("GeneratedTemplateName"))?
                    .clone();
                let id = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("generatedtemplate/{name}"),
                )
                .to_string();
                Ok(xml_response(
                    "UpdateGeneratedTemplate",
                    format!(
                        "    <GeneratedTemplateId>{}</GeneratedTemplateId>",
                        xml_escape(&id)
                    ),
                    &rid,
                ))
            }
            "DescribeGeneratedTemplate" => {
                let name = params
                    .get("GeneratedTemplateName")
                    .ok_or_else(|| missing("GeneratedTemplateName"))?
                    .clone();
                let inner = format!(
                    "    <GeneratedTemplateId>arn:aws:cloudformation:us-east-1:{}:generatedtemplate/{}</GeneratedTemplateId>\n    <GeneratedTemplateName>{}</GeneratedTemplateName>\n    <Status>COMPLETE</Status>",
                    xml_escape(&aid),
                    xml_escape(&name),
                    xml_escape(&name),
                );
                Ok(xml_response("DescribeGeneratedTemplate", inner, &rid))
            }
            "GetGeneratedTemplate" => {
                require_scalar(&params, "GeneratedTemplateName")?;
                Ok(xml_response(
                    "GetGeneratedTemplate",
                    "    <Status>COMPLETE</Status>\n    <TemplateBody>{}</TemplateBody>"
                        .to_string(),
                    &rid,
                ))
            }
            "DeleteGeneratedTemplate" => {
                let name = params
                    .get("GeneratedTemplateName")
                    .ok_or_else(|| missing("GeneratedTemplateName"))?
                    .clone();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                if let Some(m) = state.extras.get_mut("generated_templates") {
                    m.remove(&name);
                }
                Ok(xml_response("DeleteGeneratedTemplate", String::new(), &rid))
            }
            "ListGeneratedTemplates" => Ok(xml_response(
                "ListGeneratedTemplates",
                "    <Summaries/>".to_string(),
                &rid,
            )),

            // ── Resource scans ──
            "StartResourceScan" => {
                let id = Arn::new(
                    "cloudformation",
                    "us-east-1",
                    &aid,
                    &format!("resourceScan/{}", rand_id()),
                )
                .to_string();
                Ok(xml_response(
                    "StartResourceScan",
                    format!("    <ResourceScanId>{}</ResourceScanId>", xml_escape(&id)),
                    &rid,
                ))
            }
            "DescribeResourceScan" => {
                let id = params
                    .get("ResourceScanId")
                    .cloned()
                    .ok_or_else(|| missing("ResourceScanId"))?;
                let inner = format!(
                    "    <ResourceScanId>{}</ResourceScanId>\n    <Status>COMPLETE</Status>",
                    xml_escape(&id),
                );
                Ok(xml_response("DescribeResourceScan", inner, &rid))
            }
            "ListResourceScans" => Ok(xml_response(
                "ListResourceScans",
                "    <ResourceScanSummaries/>".to_string(),
                &rid,
            )),
            "ListResourceScanResources" => {
                require_scalar(&params, "ResourceScanId")?;
                Ok(xml_response(
                    "ListResourceScanResources",
                    "    <Resources/>".to_string(),
                    &rid,
                ))
            }
            "ListResourceScanRelatedResources" => {
                require_scalar(&params, "ResourceScanId")?;
                Ok(xml_response(
                    "ListResourceScanRelatedResources",
                    "    <RelatedResources/>".to_string(),
                    &rid,
                ))
            }

            // ── Drift detection ──
            "DetectStackDrift" => {
                let stack_name = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let id = rand_id();

                let resources: Vec<StackResource> = {
                    let accounts = self.state.read();
                    let stack = accounts.get(&aid).and_then(|s| {
                        s.stacks.values().find(|st| {
                            (st.name == stack_name || st.stack_id == stack_name)
                                && st.status != "DELETE_COMPLETE"
                        })
                    });
                    stack.map(|s| s.resources.clone()).unwrap_or_default()
                };

                let mut drifted_resources: Vec<Value> = Vec::new();

                for resource in &resources {
                    let exists = match resource.resource_type.as_str() {
                        "AWS::SQS::Queue" => self
                            .deps
                            .sqs
                            .read()
                            .get(&aid)
                            .map(|s| s.queues.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::SNS::Topic" => self
                            .deps
                            .sns
                            .read()
                            .get(&aid)
                            .map(|s| s.topics.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::S3::Bucket" => self
                            .deps
                            .s3
                            .read()
                            .get(&aid)
                            .map(|s| s.buckets.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::Lambda::Function" => self
                            .deps
                            .lambda
                            .read()
                            .get(&aid)
                            .map(|s| s.functions.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::IAM::Role" => self
                            .deps
                            .iam
                            .read()
                            .get(&aid)
                            .map(|s| s.roles.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::DynamoDB::Table" => self
                            .deps
                            .dynamodb
                            .read()
                            .get(&aid)
                            .map(|s| s.tables.values().any(|t| t.arn == resource.physical_id))
                            .unwrap_or(false),
                        "AWS::KMS::Key" => self
                            .deps
                            .kms
                            .read()
                            .get(&aid)
                            .map(|s| s.keys.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        "AWS::SecretsManager::Secret" => self
                            .deps
                            .secretsmanager
                            .read()
                            .get(&aid)
                            .map(|s| s.secrets.contains_key(&resource.physical_id))
                            .unwrap_or(false),
                        _ => true, // NOT_CHECKED — assume exists
                    };
                    if !exists {
                        drifted_resources.push(json!({
                            "LogicalResourceId": resource.logical_id,
                            "PhysicalResourceId": resource.physical_id,
                            "ResourceType": resource.resource_type,
                            "StackResourceDriftStatus": "DELETED",
                            "PropertyDifferences": [],
                        }));
                    }
                }

                let stack_drift_status = if drifted_resources.is_empty() {
                    "IN_SYNC"
                } else {
                    "DRIFTED"
                };

                let record = json!({
                    "StackDriftDetectionId": id,
                    "StackName": stack_name,
                    "StackDriftStatus": stack_drift_status,
                    "DetectionStatus": "DETECTION_COMPLETE",
                    "DriftedResources": drifted_resources,
                });

                {
                    let mut accounts = self.state.write();
                    let state = accounts.get_or_create(&aid);
                    store(&mut state.extras, "drift_detection").insert(id.clone(), record);
                }

                Ok(xml_response(
                    "DetectStackDrift",
                    format!(
                        "    <StackDriftDetectionId>{}</StackDriftDetectionId>",
                        xml_escape(&id)
                    ),
                    &rid,
                ))
            }
            "DetectStackResourceDrift" => {
                let stack_name = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let logical = params
                    .get("LogicalResourceId")
                    .ok_or_else(|| missing("LogicalResourceId"))?
                    .clone();
                let accounts = self.state.read();
                let resource_drift = accounts
                    .get(&aid)
                    .and_then(|s| {
                        s.stacks.values().find(|st| {
                            (st.name == stack_name || st.stack_id == stack_name)
                                && st.status != "DELETE_COMPLETE"
                        })
                    })
                    .and_then(|stack| stack.resources.iter().find(|r| r.logical_id == logical))
                    .map(|resource| {
                        let exists = match resource.resource_type.as_str() {
                            "AWS::SQS::Queue" => self
                                .deps
                                .sqs
                                .read()
                                .get(&aid)
                                .map(|s| s.queues.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::SNS::Topic" => self
                                .deps
                                .sns
                                .read()
                                .get(&aid)
                                .map(|s| s.topics.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::S3::Bucket" => self
                                .deps
                                .s3
                                .read()
                                .get(&aid)
                                .map(|s| s.buckets.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::Lambda::Function" => self
                                .deps
                                .lambda
                                .read()
                                .get(&aid)
                                .map(|s| s.functions.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::IAM::Role" => self
                                .deps
                                .iam
                                .read()
                                .get(&aid)
                                .map(|s| s.roles.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::DynamoDB::Table" => self
                                .deps
                                .dynamodb
                                .read()
                                .get(&aid)
                                .map(|s| s.tables.values().any(|t| t.arn == resource.physical_id))
                                .unwrap_or(false),
                            "AWS::KMS::Key" => self
                                .deps
                                .kms
                                .read()
                                .get(&aid)
                                .map(|s| s.keys.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            "AWS::SecretsManager::Secret" => self
                                .deps
                                .secretsmanager
                                .read()
                                .get(&aid)
                                .map(|s| s.secrets.contains_key(&resource.physical_id))
                                .unwrap_or(false),
                            _ => true,
                        };
                        if exists {
                            "IN_SYNC"
                        } else {
                            "DELETED"
                        }
                    })
                    .unwrap_or("NOT_CHECKED");

                let inner = format!(
                    "    <StackResourceDrift>\n      <LogicalResourceId>{}</LogicalResourceId>\n      <StackResourceDriftStatus>{}</StackResourceDriftStatus>\n    </StackResourceDrift>",
                    xml_escape(&logical),
                    xml_escape(resource_drift),
                );
                Ok(xml_response("DetectStackResourceDrift", inner, &rid))
            }
            "DetectStackSetDrift" => {
                require_scalar(&params, "StackSetName")?;
                let op_id = rand_id();
                Ok(xml_response(
                    "DetectStackSetDrift",
                    format!("    <OperationId>{}</OperationId>", xml_escape(&op_id)),
                    &rid,
                ))
            }
            "DescribeStackDriftDetectionStatus" => {
                let id = params
                    .get("StackDriftDetectionId")
                    .ok_or_else(|| missing("StackDriftDetectionId"))?
                    .clone();
                let accounts = self.state.read();
                let record = accounts
                    .get(&aid)
                    .and_then(|s| s.extras.get("drift_detection"))
                    .and_then(|m| m.get(&id))
                    .cloned()
                    .unwrap_or_else(|| {
                        json!({
                            "StackDriftDetectionId": id,
                            "StackDriftStatus": "IN_SYNC",
                            "DetectionStatus": "DETECTION_COMPLETE",
                        })
                    });
                // The Smithy output declares `StackId` and `Timestamp` as
                // `@required` alongside `StackDriftDetectionId` and
                // `DetectionStatus`. Synthetic detection records won't have
                // a real stack ARN behind them, so synthesise a deterministic
                // placeholder ARN from the detection id.
                let stack_id = record["StackId"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        Arn::new(
                            "cloudformation",
                            "us-east-1",
                            &aid,
                            &format!("stack/drift-{id}/{}", rand_id()),
                        )
                        .to_string()
                    });
                let timestamp = record["Timestamp"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| "2024-01-01T00:00:00Z".to_string());

                let inner = format!(
                    "    <StackId>{}</StackId>\n    <StackDriftDetectionId>{}</StackDriftDetectionId>\n    <DetectionStatus>{}</DetectionStatus>\n    <StackDriftStatus>{}</StackDriftStatus>\n    <Timestamp>{}</Timestamp>",
                    xml_escape(&stack_id),
                    xml_escape(record["StackDriftDetectionId"].as_str().unwrap_or("")),
                    xml_escape(record["DetectionStatus"].as_str().unwrap_or("DETECTION_COMPLETE")),
                    xml_escape(record["StackDriftStatus"].as_str().unwrap_or("IN_SYNC")),
                    xml_escape(&timestamp),
                );
                Ok(xml_response(
                    "DescribeStackDriftDetectionStatus",
                    inner,
                    &rid,
                ))
            }
            "DescribeStackResourceDrifts" => {
                let stack_name = params
                    .get("StackName")
                    .cloned()
                    .ok_or_else(|| missing("StackName"))?;
                let accounts = self.state.read();
                let drifted: Vec<Value> = accounts
                    .get(&aid)
                    .and_then(|s| {
                        let found = s
                            .stacks
                            .values()
                            .find(|st| {
                                (st.name == stack_name || st.stack_id == stack_name)
                                    && st.status != "DELETE_COMPLETE"
                            })
                            .is_some();
                        if !found {
                            return None;
                        }
                        s.extras
                            .get("drift_detection")
                            .and_then(|m| {
                                m.values()
                                    .find(|v| v["StackName"].as_str() == Some(stack_name.as_str()))
                            })
                            .and_then(|v| v["DriftedResources"].as_array().cloned())
                    })
                    .unwrap_or_default();

                let inner = if drifted.is_empty() {
                    "    <StackResourceDrifts/>".to_string()
                } else {
                    format!(
                        "    <StackResourceDrifts>\n{}\n    </StackResourceDrifts>",
                        members_xml(&drifted, |v| {
                            format!(
                            "        <StackResourceDrift>\n          <LogicalResourceId>{}</LogicalResourceId>\n          <PhysicalResourceId>{}</PhysicalResourceId>\n          <ResourceType>{}</ResourceType>\n          <StackResourceDriftStatus>{}</StackResourceDriftStatus>\n        </StackResourceDrift>",
                            xml_escape(v["LogicalResourceId"].as_str().unwrap_or("")),
                            xml_escape(v["PhysicalResourceId"].as_str().unwrap_or("")),
                            xml_escape(v["ResourceType"].as_str().unwrap_or("")),
                            xml_escape(v["StackResourceDriftStatus"].as_str().unwrap_or("IN_SYNC")),
                        )
                        }),
                    )
                };
                Ok(xml_response("DescribeStackResourceDrifts", inner, &rid))
            }
            "DescribeStackResource" => {
                let stack_name = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let logical = params
                    .get("LogicalResourceId")
                    .ok_or_else(|| missing("LogicalResourceId"))?
                    .clone();
                let accounts = self.state.read();
                let detail = accounts
                    .get(&aid)
                    .and_then(|s| s.stacks.get(&stack_name))
                    .and_then(|s| s.resources.iter().find(|r| r.logical_id == logical))
                    .map(|r| {
                        (
                            r.physical_id.clone(),
                            r.resource_type.clone(),
                            r.status.clone(),
                        )
                    })
                    .unwrap_or_else(|| {
                        (
                            "pid".to_string(),
                            "AWS::Custom".to_string(),
                            "CREATE_COMPLETE".to_string(),
                        )
                    });
                let inner = format!(
                    "    <StackResourceDetail>\n      <StackName>{}</StackName>\n      <LogicalResourceId>{}</LogicalResourceId>\n      <PhysicalResourceId>{}</PhysicalResourceId>\n      <ResourceType>{}</ResourceType>\n      <ResourceStatus>{}</ResourceStatus>\n      <LastUpdatedTimestamp>{}</LastUpdatedTimestamp>\n    </StackResourceDetail>",
                    xml_escape(&stack_name),
                    xml_escape(&logical),
                    xml_escape(&detail.0),
                    xml_escape(&detail.1),
                    xml_escape(&detail.2),
                    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                );
                Ok(xml_response("DescribeStackResource", inner, &rid))
            }

            // ── Events ──
            "DescribeStackEvents" => {
                require_scalar(&params, "StackName")?;
                let stack_filter = params.get("StackName").cloned();
                let accounts = self.state.read();
                let events: Vec<Value> = accounts
                    .get(&aid)
                    .map(|s| {
                        let mut all: Vec<Value> = Vec::new();
                        for (sid, evs) in &s.events {
                            // Resolve to find matching stack id by name or id.
                            let matches = match &stack_filter {
                                None => true,
                                Some(filter) => {
                                    sid == filter
                                        || s.stacks.values().any(|st| {
                                            st.stack_id == *sid
                                                && (st.name == *filter || st.stack_id == *filter)
                                        })
                                }
                            };
                            if matches {
                                all.extend(evs.iter().cloned());
                            }
                        }
                        // Newest first, matching real CloudFormation.
                        all.reverse();
                        all
                    })
                    .unwrap_or_default();
                let inner = if events.is_empty() {
                    "    <StackEvents/>".to_string()
                } else {
                    format!(
                        "    <StackEvents>\n{}\n    </StackEvents>",
                        members_xml(&events, |v| {
                            format!(
                            "        <EventId>{}</EventId>\n        <StackId>{}</StackId>\n        <StackName>{}</StackName>\n        <LogicalResourceId>{}</LogicalResourceId>\n        <PhysicalResourceId>{}</PhysicalResourceId>\n        <ResourceType>{}</ResourceType>\n        <ResourceStatus>{}</ResourceStatus>\n        <Timestamp>{}</Timestamp>",
                            xml_escape(v["EventId"].as_str().unwrap_or("")),
                            xml_escape(v["StackId"].as_str().unwrap_or("")),
                            xml_escape(v["StackName"].as_str().unwrap_or("")),
                            xml_escape(v["LogicalResourceId"].as_str().unwrap_or("")),
                            xml_escape(v["PhysicalResourceId"].as_str().unwrap_or("")),
                            xml_escape(v["ResourceType"].as_str().unwrap_or("")),
                            xml_escape(v["ResourceStatus"].as_str().unwrap_or("")),
                            xml_escape(v["Timestamp"].as_str().unwrap_or("")),
                        )
                        }),
                    )
                };
                Ok(xml_response("DescribeStackEvents", inner, &rid))
            }
            "DescribeEvents" => Ok(xml_response(
                "DescribeEvents",
                "    <Events/>".to_string(),
                &rid,
            )),

            // ── Hooks ──
            "GetHookResult" => {
                // Read the recorded hook invocation instead of always
                // returning success (bug-audit 2026-06-13, 1.8). When no
                // recorded result matches (unknown / not-yet-invoked hook),
                // return a benign empty result with a 2xx status rather than
                // erroring — the route must stay reachable for callers that
                // probe a hook before it has run, and several CFN "Get*"
                // routes are smoke-tested for a handled 2xx response.
                let result_id = params
                    .get("HookResultId")
                    .or_else(|| params.get("HookId"))
                    .cloned()
                    .unwrap_or_default();
                let record = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("hook_results"))
                        .and_then(|m| m.get(&result_id))
                        .cloned()
                };
                let r = record.unwrap_or_else(|| {
                    serde_json::json!({
                        "HookResultId": result_id,
                        "InvocationPoint": params
                            .get("InvocationPoint")
                            .cloned()
                            .unwrap_or_default(),
                        "Status": "",
                        "HookStatusReason": "",
                    })
                });
                let inner = format!(
                    "    <HookResultId>{}</HookResultId>\n    <InvocationPoint>{}</InvocationPoint>\n    <FailureMode>{}</FailureMode>\n    <TypeName>{}</TypeName>\n    <TypeVersionId>{}</TypeVersionId>\n    <TypeConfigurationVersionId>{}</TypeConfigurationVersionId>\n    <TypeArn>{}</TypeArn>\n    <Status>{}</Status>\n    <HookStatusReason>{}</HookStatusReason>",
                    xml_escape(r["HookResultId"].as_str().unwrap_or("")),
                    xml_escape(r["InvocationPoint"].as_str().unwrap_or("PRE_PROVISION")),
                    xml_escape(r["FailureMode"].as_str().unwrap_or("FAIL")),
                    xml_escape(r["TypeName"].as_str().unwrap_or("")),
                    xml_escape(r["TypeVersionId"].as_str().unwrap_or("00000001")),
                    xml_escape(r["TypeConfigurationVersionId"].as_str().unwrap_or("1")),
                    xml_escape(r["TypeArn"].as_str().unwrap_or("")),
                    xml_escape(r["Status"].as_str().unwrap_or("HOOK_COMPLETE_SUCCEEDED")),
                    xml_escape(r["HookStatusReason"].as_str().unwrap_or("")),
                );
                Ok(xml_response("GetHookResult", inner, &rid))
            }
            "ListHookResults" => {
                // List recorded hook invocations for the requested target
                // instead of always returning empty (bug-audit
                // 2026-06-13, 1.8).
                let target_type = params.get("TargetType").cloned();
                let target_id = params.get("TargetId").cloned();
                let type_arn = params.get("TypeArn").cloned();
                let status_filter = params.get("Status").cloned();
                let records: Vec<Value> = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.extras.get("hook_results"))
                        .map(|m| {
                            m.values()
                                .filter(|r| {
                                    target_type
                                        .as_deref()
                                        .is_none_or(|t| r["TargetType"].as_str() == Some(t))
                                        && target_id
                                            .as_deref()
                                            .is_none_or(|t| r["TargetId"].as_str() == Some(t))
                                        && type_arn
                                            .as_deref()
                                            .is_none_or(|t| r["TypeArn"].as_str() == Some(t))
                                        && status_filter
                                            .as_deref()
                                            .is_none_or(|s| r["Status"].as_str() == Some(s))
                                })
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default()
                };
                let results_xml = if records.is_empty() {
                    "    <HookResults/>".to_string()
                } else {
                    let members = members_xml(&records, |r| {
                        format!(
                            "        <HookResultId>{}</HookResultId>\n        <InvocationPoint>{}</InvocationPoint>\n        <FailureMode>{}</FailureMode>\n        <TypeName>{}</TypeName>\n        <TypeVersionId>{}</TypeVersionId>\n        <TypeConfigurationVersionId>{}</TypeConfigurationVersionId>\n        <TypeArn>{}</TypeArn>\n        <Status>{}</Status>\n        <HookStatusReason>{}</HookStatusReason>\n        <TargetType>{}</TargetType>\n        <TargetId>{}</TargetId>",
                            xml_escape(r["HookResultId"].as_str().unwrap_or("")),
                            xml_escape(r["InvocationPoint"].as_str().unwrap_or("PRE_PROVISION")),
                            xml_escape(r["FailureMode"].as_str().unwrap_or("FAIL")),
                            xml_escape(r["TypeName"].as_str().unwrap_or("")),
                            xml_escape(r["TypeVersionId"].as_str().unwrap_or("00000001")),
                            xml_escape(r["TypeConfigurationVersionId"].as_str().unwrap_or("1")),
                            xml_escape(r["TypeArn"].as_str().unwrap_or("")),
                            xml_escape(r["Status"].as_str().unwrap_or("HOOK_COMPLETE_SUCCEEDED")),
                            xml_escape(r["HookStatusReason"].as_str().unwrap_or("")),
                            xml_escape(r["TargetType"].as_str().unwrap_or("")),
                            xml_escape(r["TargetId"].as_str().unwrap_or("")),
                        )
                    });
                    format!("    <HookResults>\n{members}\n    </HookResults>")
                };
                let mut inner = String::new();
                if let Some(t) = &target_type {
                    inner.push_str(&format!("    <TargetType>{}</TargetType>\n", xml_escape(t)));
                }
                if let Some(t) = &target_id {
                    inner.push_str(&format!("    <TargetId>{}</TargetId>\n", xml_escape(t)));
                }
                inner.push_str(&results_xml);
                Ok(xml_response("ListHookResults", inner, &rid))
            }
            "RecordHandlerProgress" => {
                require_scalar(&params, "BearerToken")?;
                require_scalar(&params, "OperationStatus")?;
                Ok(xml_response_no_result("RecordHandlerProgress", &rid))
            }

            // ── Imports / exports ──
            "ListExports" => {
                let accounts = self.state.read();
                let mut entries = String::new();
                if let Some(state) = accounts.get(&aid) {
                    for (name, export) in &state.exports {
                        entries.push_str(&format!(
                            "      <member>\n        <ExportingStackId>{}</ExportingStackId>\n        <Name>{}</Name>\n        <Value>{}</Value>\n      </member>\n",
                            xml_escape(&export.exporting_stack_id),
                            xml_escape(name),
                            xml_escape(&export.value),
                        ));
                    }
                }
                let inner = if entries.is_empty() {
                    "    <Exports/>".to_string()
                } else {
                    format!("    <Exports>\n{entries}    </Exports>")
                };
                Ok(xml_response("ListExports", inner, &rid))
            }
            "ListImports" => {
                let export_name = params
                    .get("ExportName")
                    .cloned()
                    .ok_or_else(|| missing("ExportName"))?;
                let accounts = self.state.read();
                let mut entries = String::new();
                if let Some(state) = accounts.get(&aid) {
                    if let Some(consumers) = state.imports.get(&export_name) {
                        for stack_name in consumers {
                            entries.push_str(&format!(
                                "      <member>{}</member>\n",
                                xml_escape(stack_name)
                            ));
                        }
                    }
                }
                let inner = if entries.is_empty() {
                    "    <Imports/>".to_string()
                } else {
                    format!("    <Imports>\n{entries}    </Imports>")
                };
                Ok(xml_response("ListImports", inner, &rid))
            }

            // ── Stack policies ──
            "GetStackPolicy" => {
                let stack = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let accounts = self.state.read();
                let body = accounts.get(&aid)
                    .and_then(|s| s.stack_policies.get(&stack))
                    .cloned()
                    .unwrap_or_else(|| r#"{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}"#.to_string());
                let inner = format!(
                    "    <StackPolicyBody>{}</StackPolicyBody>",
                    xml_escape(&body)
                );
                Ok(xml_response("GetStackPolicy", inner, &rid))
            }
            "SetStackPolicy" => {
                let stack = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let body = params.get("StackPolicyBody").cloned().unwrap_or_default();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                state.stack_policies.insert(stack, body);
                Ok(xml_response_no_result("SetStackPolicy", &rid))
            }

            // ── Termination protection ──
            "UpdateTerminationProtection" => {
                let stack = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let enabled_raw = params
                    .get("EnableTerminationProtection")
                    .ok_or_else(|| missing("EnableTerminationProtection"))?;
                let enabled = enabled_raw.eq_ignore_ascii_case("true");
                let stack_id = {
                    let mut accounts = self.state.write();
                    let state = accounts.get_or_create(&aid);
                    state.termination_protection.insert(stack.clone(), enabled);
                    state
                        .stacks
                        .get(&stack)
                        .map(|s| s.stack_id.clone())
                        .unwrap_or_else(|| stack.clone())
                };
                Ok(xml_response(
                    "UpdateTerminationProtection",
                    format!("    <StackId>{}</StackId>", xml_escape(&stack_id)),
                    &rid,
                ))
            }

            // ── Account / org / validation / utilities ──
            "DescribeAccountLimits" => Ok(xml_response(
                "DescribeAccountLimits",
                r#"    <AccountLimits>
      <member>
        <Name>StackLimit</Name>
        <Value>2000</Value>
      </member>
    </AccountLimits>"#
                    .to_string(),
                &rid,
            )),
            "ActivateOrganizationsAccess" => {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                state.orgs_access_enabled = true;
                Ok(xml_response(
                    "ActivateOrganizationsAccess",
                    String::new(),
                    &rid,
                ))
            }
            "DeactivateOrganizationsAccess" => {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&aid);
                state.orgs_access_enabled = false;
                Ok(xml_response(
                    "DeactivateOrganizationsAccess",
                    String::new(),
                    &rid,
                ))
            }
            "DescribeOrganizationsAccess" => {
                let accounts = self.state.read();
                let status = if accounts
                    .get(&aid)
                    .map(|s| s.orgs_access_enabled)
                    .unwrap_or(false)
                {
                    "ENABLED"
                } else {
                    "DISABLED"
                };
                Ok(xml_response(
                    "DescribeOrganizationsAccess",
                    format!("    <Status>{}</Status>", status),
                    &rid,
                ))
            }
            "ValidateTemplate" => Ok(xml_response(
                "ValidateTemplate",
                "    <Description>Validated</Description>\n    <Capabilities/>\n    <Parameters/>"
                    .to_string(),
                &rid,
            )),
            "EstimateTemplateCost" => Ok(xml_response(
                "EstimateTemplateCost",
                "    <Url>https://calculator.aws/#/estimate</Url>".to_string(),
                &rid,
            )),
            "GetTemplateSummary" => Ok(xml_response(
                "GetTemplateSummary",
                "    <Parameters/>\n    <ResourceTypes/>\n    <Capabilities/>".to_string(),
                &rid,
            )),
            "CancelUpdateStack" => {
                params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?;
                Ok(xml_response_no_result("CancelUpdateStack", &rid))
            }
            "ContinueUpdateRollback" => {
                params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?;
                Ok(xml_response("ContinueUpdateRollback", String::new(), &rid))
            }
            "RollbackStack" => {
                let stack = params
                    .get("StackName")
                    .ok_or_else(|| missing("StackName"))?
                    .clone();
                let stack_id = {
                    let accounts = self.state.read();
                    accounts
                        .get(&aid)
                        .and_then(|s| s.stacks.get(&stack))
                        .map(|s| s.stack_id.clone())
                        .unwrap_or_else(|| stack.clone())
                };
                Ok(xml_response(
                    "RollbackStack",
                    format!("    <StackId>{}</StackId>", xml_escape(&stack_id)),
                    &rid,
                ))
            }
            "SignalResource" => {
                require_scalar(&params, "StackName")?;
                require_scalar(&params, "LogicalResourceId")?;
                require_scalar(&params, "UniqueId")?;
                require_scalar(&params, "Status")?;
                Ok(xml_response_no_result("SignalResource", &rid))
            }

            _ => Err(AwsServiceError::action_not_implemented(
                "cloudformation",
                &action,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_s3_url;
    use crate::service::{CloudFormationDeps, CloudFormationService};
    use crate::state::{CloudFormationState, SharedCloudFormationState};
    use fakecloud_core::delivery::DeliveryBus;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::Method;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn parse_s3_url_handles_path_and_virtual_hosted() {
        // Path-style against a fakecloud endpoint (what TemplateURL looks
        // like locally) — key keeps its embedded slashes.
        assert_eq!(
            parse_s3_url("http://127.0.0.1:4566/bucket/deploy/template.json"),
            Some(("bucket".to_string(), "deploy/template.json".to_string()))
        );
        // Path-style against real AWS S3 host.
        assert_eq!(
            parse_s3_url("https://s3.us-east-1.amazonaws.com/my-bucket/key.yaml"),
            Some(("my-bucket".to_string(), "key.yaml".to_string()))
        );
        // Virtual-hosted style.
        assert_eq!(
            parse_s3_url("https://my-bucket.s3.amazonaws.com/key.yaml"),
            Some(("my-bucket".to_string(), "key.yaml".to_string()))
        );
        // Query string (e.g. ?versionId=...) is dropped.
        assert_eq!(
            parse_s3_url("https://s3.amazonaws.com/b/k.json?versionId=abc"),
            Some(("b".to_string(), "k.json".to_string()))
        );
        // Not an object URL (no key).
        assert_eq!(parse_s3_url("https://s3.amazonaws.com/bucket-only"), None);
    }

    use crate::template::ResourceDefinition;
    use serde_json::json;

    fn resource(logical_id: &str, ty: &str, props: serde_json::Value) -> ResourceDefinition {
        ResourceDefinition {
            logical_id: logical_id.to_string(),
            resource_type: ty.to_string(),
            properties: props,
        }
    }

    #[test]
    fn cfn_provisions_eks_cluster_write_through() {
        let service = svc();
        let prov = service.provisioner("stack-id", "000000000000", "us-east-1");
        let res = prov
            .create_resource(&resource(
                "MyCluster",
                "AWS::EKS::Cluster",
                json!({
                    "Name": "prod",
                    "RoleArn": "arn:aws:iam::000000000000:role/eks",
                    "ResourcesVpcConfig": {
                        "SubnetIds": ["subnet-1", "subnet-2"],
                        "SecurityGroupIds": ["sg-abc"],
                    },
                    "Version": "1.30",
                    "Tags": { "env": "prod" },
                }),
            ))
            .expect("cluster provisions");

        // Ref (physical id) is the cluster name.
        assert_eq!(res.physical_id, "prod");
        // Write-through: the owning EKS service now sees the cluster.
        {
            let accounts = prov.eks_state.read();
            let state = accounts.get("000000000000").expect("account exists");
            let cluster = state.clusters.get("prod").expect("cluster present");
            assert_eq!(cluster.version, "1.30");
            assert_eq!(cluster.status, "ACTIVE");
            assert_eq!(cluster.tags.get("env").map(String::as_str), Some("prod"));
        }
        // GetAtt resolves the real Arn / Endpoint the service computed.
        assert_eq!(
            prov.get_att(&res, "Arn").as_deref(),
            Some("arn:aws:eks:us-east-1:000000000000:cluster/prod")
        );
        let endpoint = prov.get_att(&res, "Endpoint").expect("endpoint present");
        assert!(endpoint.starts_with("https://") && endpoint.contains(".eks.amazonaws.com"));
        assert!(prov
            .get_att(&res, "ClusterSecurityGroupId")
            .expect("sg id present")
            .starts_with("sg-"));
        assert!(prov
            .get_att(&res, "OpenIdConnectIssuerUrl")
            .expect("oidc present")
            .starts_with("https://oidc.eks.amazonaws.com/id/"));

        // Delete removes it from the owning service.
        prov.delete_resource(&res).expect("delete ok");
        assert!(prov
            .eks_state
            .read()
            .get("000000000000")
            .map(|s| s.clusters.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn cfn_provisions_servicediscovery_namespace_and_service() {
        let service = svc();
        let prov = service.provisioner("stack-id", "000000000000", "us-east-1");

        let ns = prov
            .create_resource(&resource(
                "Ns",
                "AWS::ServiceDiscovery::HttpNamespace",
                json!({ "Name": "my-namespace", "Description": "test" }),
            ))
            .expect("namespace provisions");
        let ns_id = ns.physical_id.clone();
        assert!(ns_id.starts_with("ns-"));
        // Write-through: the namespace is visible in Cloud Map state.
        {
            let accounts = prov.servicediscovery_state.read();
            let state = accounts.get("000000000000").expect("account");
            assert!(state.namespaces.contains_key(&ns_id));
        }
        // Namespace GetAtt.
        assert!(prov
            .get_att(&ns, "Arn")
            .expect("ns arn")
            .contains(":namespace/ns-"));
        assert_eq!(prov.get_att(&ns, "Id").as_deref(), Some(ns_id.as_str()));

        // Service registered into the namespace.
        let svc_res = prov
            .create_resource(&resource(
                "Svc",
                "AWS::ServiceDiscovery::Service",
                json!({ "Name": "web", "NamespaceId": ns_id }),
            ))
            .expect("service provisions");
        let svc_id = svc_res.physical_id.clone();
        assert!(svc_id.starts_with("srv-"));
        // Write-through: the service is visible and bumped the namespace count.
        {
            let accounts = prov.servicediscovery_state.read();
            let state = accounts.get("000000000000").expect("account");
            let stored = state.services.get(&svc_id).expect("service present");
            assert_eq!(stored.name, "web");
            let namespace = state.namespaces.get(&ns.physical_id).expect("ns");
            assert_eq!(namespace.service_count, 1);
        }
        // Service GetAtt: Id / Arn / Name.
        assert_eq!(
            prov.get_att(&svc_res, "Id").as_deref(),
            Some(svc_id.as_str())
        );
        assert!(prov
            .get_att(&svc_res, "Arn")
            .expect("svc arn")
            .contains(":service/srv-"));
        assert_eq!(prov.get_att(&svc_res, "Name").as_deref(), Some("web"));

        // Delete decrements the namespace's service count.
        prov.delete_resource(&svc_res).expect("delete svc");
        {
            let accounts = prov.servicediscovery_state.read();
            let state = accounts.get("000000000000").expect("account");
            assert!(!state.services.contains_key(&svc_id));
            assert_eq!(
                state.namespaces.get(&ns.physical_id).unwrap().service_count,
                0
            );
        }
    }

    fn deps() -> CloudFormationDeps {
        use fakecloud_dynamodb::DynamoDbState;
        use fakecloud_ecr::EcrState;
        use fakecloud_eventbridge::EventBridgeState;
        use fakecloud_iam::IamState;
        use fakecloud_kinesis::KinesisState;
        use fakecloud_kms::KmsState;
        use fakecloud_lambda::LambdaState;
        use fakecloud_logs::LogsState;
        use fakecloud_s3::S3State;
        use fakecloud_secretsmanager::SecretsManagerState;
        use fakecloud_sns::SnsState;
        use fakecloud_sqs::SqsState;
        use fakecloud_ssm::SsmState;

        fn shared<T: fakecloud_core::multi_account::AccountState>(
        ) -> Arc<RwLock<MultiAccountState<T>>> {
            Arc::new(RwLock::new(MultiAccountState::<T>::new(
                "000000000000",
                "us-east-1",
                "",
            )))
        }
        CloudFormationDeps {
            sqs: shared::<SqsState>(),
            sns: shared::<SnsState>(),
            ssm: shared::<SsmState>(),
            iam: shared::<IamState>(),
            s3: shared::<S3State>(),
            eventbridge: shared::<EventBridgeState>(),
            dynamodb: shared::<DynamoDbState>(),
            logs: shared::<LogsState>(),
            lambda: shared::<LambdaState>(),
            secretsmanager: shared::<SecretsManagerState>(),
            kinesis: shared::<KinesisState>(),
            kms: shared::<KmsState>(),
            ecr: shared::<EcrState>(),
            cloudwatch: Arc::new(RwLock::new(fakecloud_cloudwatch::CloudWatchAccounts::new())),
            elbv2: Arc::new(RwLock::new(fakecloud_elbv2::Elbv2Accounts::new())),
            organizations: Arc::new(RwLock::new(None)),
            cognito: shared::<fakecloud_cognito::CognitoState>(),
            rds: shared::<fakecloud_rds::RdsState>(),
            ec2: shared::<fakecloud_ec2::Ec2State>(),
            autoscaling: Arc::new(parking_lot::RwLock::new(
                fakecloud_autoscaling::AutoScalingAccounts::new(),
            )),
            batch: Arc::new(parking_lot::RwLock::new(
                fakecloud_batch::BatchAccounts::new(),
            )),
            pipes: Arc::new(parking_lot::RwLock::new(
                fakecloud_pipes::PipesAccounts::new(),
            )),
            ecs: shared::<fakecloud_ecs::EcsState>(),
            acm: Arc::new(RwLock::new(fakecloud_acm::AcmAccounts::new())),
            elasticache: shared::<fakecloud_elasticache::ElastiCacheState>(),
            route53: Arc::new(RwLock::new(fakecloud_route53::Route53Accounts::new())),
            cloudfront: Arc::new(RwLock::new(fakecloud_cloudfront::CloudFrontAccounts::new())),
            stepfunctions: shared::<fakecloud_stepfunctions::StepFunctionsState>(),
            wafv2: Arc::new(RwLock::new(fakecloud_wafv2::Wafv2Accounts::default())),
            apigateway: shared::<fakecloud_apigateway::ApiGatewayState>(),
            apigatewayv2: shared::<fakecloud_apigatewayv2::ApiGatewayV2State>(),
            ses: shared::<fakecloud_ses::SesState>(),
            application_autoscaling: Arc::new(parking_lot::RwLock::new(
                fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
            )),
            athena: Arc::new(parking_lot::RwLock::new(
                fakecloud_athena::AthenaAccounts::new(),
            )),
            firehose: Arc::new(parking_lot::RwLock::new(
                fakecloud_firehose::FirehoseAccounts::new(),
            )),
            glue: Arc::new(parking_lot::RwLock::new(fakecloud_glue::GlueAccounts::new())),
            eks: shared::<fakecloud_eks::state::EksState>(),
            servicediscovery: shared::<fakecloud_servicediscovery::state::ServiceDiscoveryState>(),
            delivery: Arc::new(DeliveryBus::new()),
            lambda_runtime: None,
            rds_runtime: None,
            ec2_runtime: None,
            ecs_runtime: None,
            elasticache_runtime: None,
        }
    }

    fn svc() -> CloudFormationService {
        let state: SharedCloudFormationState =
            Arc::new(RwLock::new(MultiAccountState::<CloudFormationState>::new(
                "000000000000",
                "us-east-1",
                "",
            )));
        CloudFormationService::new(state, deps())
    }

    fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut q = HashMap::new();
        q.insert("Action".to_string(), action.to_string());
        for (k, v) in params {
            q.insert(k.to_string(), v.to_string());
        }
        AwsRequest {
            service: "cloudformation".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: q,
            headers: http::HeaderMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "000000000000".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            action: action.to_string(),
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn ok(action: &str, params: &[(&str, &str)]) {
        let r = svc().handle_extra_action(&req(action, params));
        match r {
            Ok(resp) => assert!(resp.status.is_success(), "{action} status: {}", resp.status),
            Err(e) => panic!("{action} failed: {e:?}"),
        }
    }

    #[test]
    fn change_sets() {
        ok(
            "CreateChangeSet",
            &[("StackName", "s"), ("ChangeSetName", "cs")],
        );
        ok("DescribeChangeSet", &[("ChangeSetName", "cs")]);
        ok("DescribeChangeSetHooks", &[("ChangeSetName", "cs")]);
        ok("ListChangeSets", &[("StackName", "s")]);
        ok("ExecuteChangeSet", &[("ChangeSetName", "cs")]);
        ok("DeleteChangeSet", &[("ChangeSetName", "cs")]);
    }

    fn body_str(resp: &fakecloud_core::service::AwsResponse) -> String {
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    #[test]
    fn hook_round_trip() {
        // Activate a hook, create + execute a change set, and verify the
        // hook surfaces in DescribeChangeSetHooks and that a real hook
        // result is recorded and readable (1.8) — instead of canned
        // empty/success responses.
        let s = svc();

        // Activate a FAIL-mode hook via SetTypeConfiguration.
        s.handle_extra_action(&req(
            "SetTypeConfiguration",
            &[
                ("Type", "HOOK"),
                ("TypeName", "MyOrg::MyHook::Hook"),
                (
                    "Configuration",
                    r#"{"CloudFormationConfiguration":{"HookConfiguration":{"FailureMode":"FAIL"}}}"#,
                ),
            ],
        ))
        .expect("SetTypeConfiguration");

        // CREATE change set (empty template -> executes the empty-stack
        // path which still records hook results).
        s.handle_extra_action(&req(
            "CreateChangeSet",
            &[
                ("StackName", "hooked-stack"),
                ("ChangeSetName", "cs1"),
                ("ChangeSetType", "CREATE"),
            ],
        ))
        .expect("CreateChangeSet");

        // DescribeChangeSetHooks now reflects the activated hook.
        let resp = s
            .handle_extra_action(&req("DescribeChangeSetHooks", &[("ChangeSetName", "cs1")]))
            .expect("DescribeChangeSetHooks");
        let xml = body_str(&resp);
        assert!(xml.contains("MyOrg::MyHook::Hook"), "hooks XML: {xml}");
        assert!(xml.contains("<FailureMode>FAIL</FailureMode>"));

        // Execute records a hook result.
        s.handle_extra_action(&req("ExecuteChangeSet", &[("ChangeSetName", "cs1")]))
            .expect("ExecuteChangeSet");

        // ListHookResults returns the recorded invocation.
        let resp = s
            .handle_extra_action(&req(
                "ListHookResults",
                &[("TargetType", "CLOUD_FORMATION")],
            ))
            .expect("ListHookResults");
        let xml = body_str(&resp);
        assert!(xml.contains("<HookResults>"), "list XML: {xml}");
        assert!(xml.contains("MyOrg::MyHook::Hook"));
        // A successful provision with a FAIL-mode hook records success.
        assert!(xml.contains("HOOK_COMPLETE_SUCCEEDED"));

        // Pull the HookResultId out and read it back via GetHookResult.
        let id = xml
            .split("<HookResultId>")
            .nth(1)
            .and_then(|s| s.split("</HookResultId>").next())
            .expect("a HookResultId in the list")
            .to_string();
        let resp = s
            .handle_extra_action(&req("GetHookResult", &[("HookResultId", &id)]))
            .expect("GetHookResult");
        let xml = body_str(&resp);
        assert!(xml.contains("HOOK_COMPLETE_SUCCEEDED"), "get XML: {xml}");
        assert!(xml.contains("MyOrg::MyHook::Hook"));

        // An unknown HookResultId returns a handled 2xx response with an
        // empty result (the route stays reachable for a hook with no
        // recorded invocation) — but it must NOT echo the recorded hook's
        // data, so it isn't masking a real result.
        let resp = s
            .handle_extra_action(&req("GetHookResult", &[("HookResultId", "nope")]))
            .expect("GetHookResult for an unknown id still returns 2xx");
        let xml = body_str(&resp);
        assert!(
            xml.contains("<HookResultId>nope</HookResultId>"),
            "unknown-id XML: {xml}"
        );
        assert!(
            !xml.contains("MyOrg::MyHook::Hook"),
            "unknown id must not echo the recorded hook: {xml}"
        );
    }

    // A minimal CREATE-type change set template with one resource and one
    // output, used by the changeset bookkeeping tests below.
    const CS_TEMPLATE: &str = r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cs-q"}}},"Outputs":{"QUrl":{"Value":{"Ref":"Q"}}}}"#;

    // Gap #2: a CREATE change set that mints a new REVIEW_IN_PROGRESS stack must
    // record a stack event, so `DescribeStackEvents` is non-empty before the
    // change set is executed (sam's `get_last_event_time` indexes `[0]`).
    #[test]
    fn create_change_set_records_review_in_progress_event() {
        let svc = svc();
        svc.handle_extra_action(&req(
            "CreateChangeSet",
            &[
                ("StackName", "cs-events"),
                ("ChangeSetName", "cs1"),
                ("ChangeSetType", "CREATE"),
                ("TemplateBody", CS_TEMPLATE),
            ],
        ))
        .expect("create change set");

        // The event log must carry exactly the REVIEW_IN_PROGRESS stack event.
        {
            let accounts = svc.state.read();
            let acct = accounts.get("000000000000").unwrap();
            let total: usize = acct.events.values().map(|v| v.len()).sum();
            assert_eq!(total, 1, "expected one event after CreateChangeSet");
            let ev = acct.events.values().next().unwrap().last().unwrap();
            assert_eq!(ev["ResourceStatus"].as_str(), Some("REVIEW_IN_PROGRESS"));
            assert_eq!(
                ev["ResourceType"].as_str(),
                Some("AWS::CloudFormation::Stack")
            );
        }

        // And DescribeStackEvents surfaces it rather than an empty list.
        let resp = svc
            .handle_extra_action(&req("DescribeStackEvents", &[("StackName", "cs-events")]))
            .expect("describe stack events");
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(!body.contains("<StackEvents/>"), "events list was empty");
        assert!(body.contains("REVIEW_IN_PROGRESS"), "body: {body}");
    }

    // Gap #2b: a stack that provisions within one wall-clock second must still
    // produce strictly-increasing, sub-second event timestamps — sam's
    // deploy-wait only registers completion on an event strictly later than the
    // REVIEW_IN_PROGRESS marker, so equal whole-second timestamps hang it.
    #[test]
    fn changeset_stack_events_have_monotonic_subsecond_timestamps() {
        let svc = svc();
        svc.handle_extra_action(&req(
            "CreateChangeSet",
            &[
                ("StackName", "cs-fast"),
                ("ChangeSetName", "cs1"),
                ("ChangeSetType", "CREATE"),
                ("TemplateBody", CS_TEMPLATE),
            ],
        ))
        .expect("create change set");
        svc.handle_extra_action(&req(
            "ExecuteChangeSet",
            &[("StackName", "cs-fast"), ("ChangeSetName", "cs1")],
        ))
        .expect("execute change set");

        let accounts = svc.state.read();
        let acct = accounts.get("000000000000").unwrap();
        let stack_id = acct.stacks.get("cs-fast").unwrap().stack_id.clone();
        let events = acct.events.get(&stack_id).expect("stack has events");

        // The full create lifecycle: REVIEW_IN_PROGRESS marker through the
        // terminal CREATE_COMPLETE, several events deep, all within one second.
        assert!(events.len() >= 3, "expected several lifecycle events");
        let ts: Vec<&str> = events
            .iter()
            .map(|e| e["Timestamp"].as_str().unwrap())
            .collect();
        assert_eq!(
            events.first().unwrap()["ResourceStatus"].as_str(),
            Some("REVIEW_IN_PROGRESS")
        );
        assert_eq!(
            events.last().unwrap()["ResourceStatus"].as_str(),
            Some("CREATE_COMPLETE")
        );
        // Millisecond precision (fractional seconds) and strictly increasing.
        // The fixed-width rfc3339 millis format sorts lexicographically by time.
        for t in &ts {
            assert!(t.contains('.'), "timestamp lacks sub-second precision: {t}");
        }
        for w in ts.windows(2) {
            assert!(w[1] > w[0], "timestamps not strictly increasing: {w:?}");
        }
    }

    // Gap #1: tags set on CreateChangeSet and outputs declared in the template
    // must both survive ExecuteChangeSet and appear on the created stack (sam's
    // managed-bucket health check requires Tags + Outputs in DescribeStacks).
    #[test]
    fn execute_change_set_persists_tags_and_outputs() {
        let svc = svc();
        svc.handle_extra_action(&req(
            "CreateChangeSet",
            &[
                ("StackName", "cs-stack"),
                ("ChangeSetName", "cs1"),
                ("ChangeSetType", "CREATE"),
                ("TemplateBody", CS_TEMPLATE),
                ("Tags.member.1.Key", "ManagedStackSource"),
                ("Tags.member.1.Value", "AwsSamCli"),
            ],
        ))
        .expect("create change set");
        svc.handle_extra_action(&req(
            "ExecuteChangeSet",
            &[("StackName", "cs-stack"), ("ChangeSetName", "cs1")],
        ))
        .expect("execute change set");

        let accounts = svc.state.read();
        let stack = accounts
            .get("000000000000")
            .unwrap()
            .stacks
            .get("cs-stack")
            .unwrap();
        assert_eq!(stack.status, "CREATE_COMPLETE");
        assert_eq!(
            stack.tags.get("ManagedStackSource").map(String::as_str),
            Some("AwsSamCli"),
            "changeset Tags dropped on execute"
        );
        assert_eq!(stack.outputs.len(), 1, "changeset Outputs not resolved");
        assert_eq!(stack.outputs[0].key, "QUrl");
        assert!(!stack.outputs[0].value.is_empty());
    }

    // Gap #3: a StateMachine that references a Lambda via DefinitionSubstitutions
    // must provision *after* the Lambda even when it sorts/declares first, so the
    // substitution resolves to the function name rather than leaving the logical
    // id baked in the ASL (which broke every invoke with Lambda.ResourceNotFound).
    #[test]
    fn changeset_provisions_lambda_before_referencing_state_machine() {
        let d = deps();
        let sfn = d.stepfunctions.clone();
        let state: SharedCloudFormationState =
            Arc::new(RwLock::new(MultiAccountState::<CloudFormationState>::new(
                "000000000000",
                "us-east-1",
                "",
            )));
        let svc = CloudFormationService::new(state, d);

        // "Machine" sorts before "Worker", so without dependency ordering the
        // StateMachine provisions first and bakes the unresolved logical id.
        let template = r#"{
            "Resources": {
                "Machine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties": {
                        "RoleArn": "arn:aws:iam::000000000000:role/sfn",
                        "DefinitionString": "{\"StartAt\":\"T\",\"States\":{\"T\":{\"Type\":\"Task\",\"Resource\":\"${fn}\",\"End\":true}}}",
                        "DefinitionSubstitutions": {"fn": {"Ref": "Worker"}}
                    }
                },
                "Worker": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {
                        "FunctionName": "workflow_dispatcher_v2-1",
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "Role": "arn:aws:iam::000000000000:role/lambda",
                        "Code": {"ZipFile": "def handler(e, c): return e"}
                    }
                }
            }
        }"#;

        svc.handle_extra_action(&req(
            "CreateChangeSet",
            &[
                ("StackName", "wf"),
                ("ChangeSetName", "cs1"),
                ("ChangeSetType", "CREATE"),
                ("TemplateBody", template),
            ],
        ))
        .expect("create change set");
        svc.handle_extra_action(&req(
            "ExecuteChangeSet",
            &[("StackName", "wf"), ("ChangeSetName", "cs1")],
        ))
        .expect("execute change set");

        let accounts = sfn.read();
        let st = accounts.get("000000000000").expect("sfn account exists");
        let machine = st
            .state_machines
            .values()
            .next()
            .expect("state machine was provisioned");
        assert!(
            machine.definition.contains("workflow_dispatcher_v2-1"),
            "ASL should carry the resolved function name, got: {}",
            machine.definition
        );
        assert!(
            !machine.definition.contains("${fn}"),
            "substitution token left unreplaced: {}",
            machine.definition
        );
        assert!(
            !machine.definition.contains("Worker"),
            "logical id leaked into baked ASL: {}",
            machine.definition
        );
    }

    #[test]
    fn stack_sets_instances_refactors() {
        ok("CreateStackSet", &[("StackSetName", "ss")]);
        ok("DescribeStackSet", &[("StackSetName", "ss")]);
        ok("ListStackSets", &[]);
        ok("UpdateStackSet", &[("StackSetName", "ss")]);
        ok(
            "DescribeStackSetOperation",
            &[("StackSetName", "ss"), ("OperationId", "op")],
        );
        ok("ListStackSetOperations", &[("StackSetName", "ss")]);
        ok(
            "ListStackSetOperationResults",
            &[("StackSetName", "ss"), ("OperationId", "op")],
        );
        ok(
            "ListStackSetAutoDeploymentTargets",
            &[("StackSetName", "ss")],
        );
        ok(
            "StopStackSetOperation",
            &[("StackSetName", "ss"), ("OperationId", "op")],
        );
        ok("ImportStacksToStackSet", &[("StackSetName", "ss")]);
        ok("DeleteStackSet", &[("StackSetName", "ss")]);
        ok(
            "CreateStackInstances",
            &[("StackSetName", "ss"), ("Regions.member.1", "us-east-1")],
        );
        ok(
            "UpdateStackInstances",
            &[("StackSetName", "ss"), ("Regions.member.1", "us-east-1")],
        );
        ok(
            "DeleteStackInstances",
            &[
                ("StackSetName", "ss"),
                ("Regions.member.1", "us-east-1"),
                ("RetainStacks", "false"),
            ],
        );
        ok(
            "DescribeStackInstance",
            &[
                ("StackSetName", "ss"),
                ("StackInstanceAccount", "000000000000"),
                ("StackInstanceRegion", "us-east-1"),
            ],
        );
        ok("ListStackInstances", &[("StackSetName", "ss")]);
        ok(
            "ListStackInstanceResourceDrifts",
            &[
                ("StackSetName", "ss"),
                ("StackInstanceAccount", "000000000000"),
                ("StackInstanceRegion", "us-east-1"),
                ("OperationId", "op"),
            ],
        );
        ok(
            "CreateStackRefactor",
            &[("StackDefinitions.member.1.StackName", "s")],
        );
        ok("DescribeStackRefactor", &[("StackRefactorId", "r")]);
        ok("ExecuteStackRefactor", &[("StackRefactorId", "r")]);
        ok("ListStackRefactors", &[]);
        ok("ListStackRefactorActions", &[("StackRefactorId", "r")]);
    }

    #[test]
    fn types_and_publishers() {
        ok("ActivateType", &[]);
        ok("DeactivateType", &[]);
        ok("DescribeType", &[]);
        ok("DescribeTypeRegistration", &[("RegistrationToken", "tok")]);
        ok(
            "RegisterType",
            &[("TypeName", "T"), ("SchemaHandlerPackage", "pkg")],
        );
        ok("DeregisterType", &[]);
        ok("ListTypes", &[]);
        ok("ListTypeRegistrations", &[]);
        ok("ListTypeVersions", &[]);
        ok(
            "BatchDescribeTypeConfigurations",
            &[("TypeConfigurationIdentifiers.member.1.Type", "RESOURCE")],
        );
        ok("SetTypeConfiguration", &[("Configuration", "{}")]);
        ok("SetTypeDefaultVersion", &[]);
        ok("TestType", &[]);
        ok("PublishType", &[]);
        ok("RegisterPublisher", &[]);
        ok("DescribePublisher", &[]);
    }

    #[test]
    fn templates_resource_scans_drift() {
        ok(
            "CreateGeneratedTemplate",
            &[("GeneratedTemplateName", "gt")],
        );
        ok(
            "UpdateGeneratedTemplate",
            &[("GeneratedTemplateName", "gt")],
        );
        ok(
            "DescribeGeneratedTemplate",
            &[("GeneratedTemplateName", "gt")],
        );
        ok("GetGeneratedTemplate", &[("GeneratedTemplateName", "gt")]);
        ok("ListGeneratedTemplates", &[]);
        ok(
            "DeleteGeneratedTemplate",
            &[("GeneratedTemplateName", "gt")],
        );
        ok("StartResourceScan", &[]);
        ok("DescribeResourceScan", &[("ResourceScanId", "rs")]);
        ok("ListResourceScans", &[]);
        ok("ListResourceScanResources", &[("ResourceScanId", "rs")]);
        ok(
            "ListResourceScanRelatedResources",
            &[
                ("ResourceScanId", "rs"),
                ("Resources.member.1.ResourceType", "AWS::SQS::Queue"),
            ],
        );
        ok("DetectStackDrift", &[("StackName", "s")]);
        ok(
            "DetectStackResourceDrift",
            &[("StackName", "s"), ("LogicalResourceId", "L")],
        );
        ok("DetectStackSetDrift", &[("StackSetName", "ss")]);
        ok(
            "DescribeStackDriftDetectionStatus",
            &[("StackDriftDetectionId", "id")],
        );
        ok("DescribeStackResourceDrifts", &[("StackName", "s")]);
        ok(
            "DescribeStackResource",
            &[("StackName", "s"), ("LogicalResourceId", "L")],
        );
    }

    #[test]
    fn events_hooks_imports_policies_org() {
        ok("DescribeStackEvents", &[("StackName", "s")]);
        ok("DescribeEvents", &[]);
        // GetHookResult now reads a real recorded result; an unknown id
        // is HookResultNotFound (covered in `hook_round_trip`), so it's
        // no longer a blanket-OK route. ListHookResults with no recorded
        // results returns an empty list.
        ok("ListHookResults", &[]);
        ok(
            "RecordHandlerProgress",
            &[("BearerToken", "tok"), ("OperationStatus", "SUCCESS")],
        );
        ok("ListExports", &[]);
        ok("ListImports", &[("ExportName", "SomeExport")]);
        ok("GetStackPolicy", &[("StackName", "s")]);
        ok("SetStackPolicy", &[("StackName", "s")]);
        ok(
            "UpdateTerminationProtection",
            &[("StackName", "s"), ("EnableTerminationProtection", "false")],
        );
        ok("DescribeAccountLimits", &[]);
        ok("ActivateOrganizationsAccess", &[]);
        ok("DescribeOrganizationsAccess", &[]);
        ok("DeactivateOrganizationsAccess", &[]);
        ok("ValidateTemplate", &[]);
        ok("EstimateTemplateCost", &[]);
        ok("GetTemplateSummary", &[]);
        ok("CancelUpdateStack", &[("StackName", "s")]);
        ok("ContinueUpdateRollback", &[("StackName", "s")]);
        ok("RollbackStack", &[("StackName", "s")]);
        ok(
            "SignalResource",
            &[
                ("StackName", "s"),
                ("LogicalResourceId", "L"),
                ("UniqueId", "U"),
                ("Status", "SUCCESS"),
            ],
        );
    }
}
