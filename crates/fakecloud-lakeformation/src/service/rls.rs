//! Row-level security (RLS) policies: Create/Get/Update/Delete/List
//! RLSPolicy, Attach/Detach RLSPolicy and ListRLSPolicyAttachments, plus the
//! `Resource.RLSPolicy` existence check used by the permission paths.

use super::*;
use crate::state::{LakeFormationState, RLSPolicyAttachmentRecord, RLSPolicyRecord};

/// The `(catalog id, policy name)` a `Resource.RLSPolicy` value names, with
/// the catalog defaulted to the caller's account.
fn rls_policy_ref(resource: &Value, account_id: &str) -> Option<(String, String)> {
    let r = resource.get("RLSPolicy")?;
    let catalog = r
        .get("CatalogId")
        .and_then(Value::as_str)
        .unwrap_or(account_id)
        .to_string();
    let name = r.get("Name").and_then(Value::as_str)?.to_string();
    Some((catalog, name))
}

/// For a `Resource` naming an RLS policy, the policy must exist in the
/// caller's catalog. Any other resource kind passes through.
pub(super) fn check_rls_policy_resource(
    st: &LakeFormationState,
    resource: &Value,
    account_id: &str,
) -> Result<(), AwsServiceError> {
    if resource.get("RLSPolicy").is_none() {
        return Ok(());
    }
    let (catalog, name) = rls_policy_ref(resource, account_id)
        .filter(|(_, name)| !name.is_empty())
        .ok_or_else(|| invalid_input("Resource.RLSPolicy.Name is required."))?;
    if catalog != account_id || !st.rls_policies.contains_key(&name) {
        return Err(entity_not_found(format!(
            "Row-level security policy {name} does not exist."
        )));
    }
    Ok(())
}

/// Signature arm for `Resource.RLSPolicy` in `resource_signature`.
pub(super) fn rls_policy_signature(resource: &Value, account_id: &str) -> Option<String> {
    rls_policy_ref(resource, account_id).map(|(cat, name)| format!("rls\u{1}{cat}\u{1}{name}"))
}

fn policy_value(p: &RLSPolicyRecord, account_id: &str) -> Value {
    json!({
        "CatalogId": account_id,
        "Name": p.name,
        "AttachmentColumns": p.attachment_columns,
        "FilterExpression": p.filter_expression,
    })
}

fn attachment_value(a: &RLSPolicyAttachmentRecord, account_id: &str) -> Value {
    let mut v = json!({
        "CatalogId": account_id,
        "PolicyName": a.policy_name,
        "Resource": a.resource,
        "DefaultRLSPolicy": a.default,
    });
    if let Some(p) = &a.principal {
        v["Principal"] = p.clone();
    }
    if let Some(c) = &a.condition {
        v["Condition"] = c.clone();
    }
    v
}

fn same_principal(a: &Option<Value>, b: &Option<Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => principal_matches(x, y),
        _ => false,
    }
}

/// Validate the `Column` list of CreateRLSPolicy: each entry needs a
/// non-empty `Name` within `NameString`'s 255 characters.
fn validate_attachment_columns(cols: &[Value]) -> Result<(), AwsServiceError> {
    for c in cols {
        let name = c.get("Name").and_then(Value::as_str).unwrap_or("");
        if name.is_empty() || name.chars().count() > 255 {
            return Err(invalid_input(
                "AttachmentColumns.Name is required and must be 1-255 characters.",
            ));
        }
        if c.get("Type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.chars().count() > 131_072)
        {
            return Err(invalid_input(
                "AttachmentColumns.Type exceeds the maximum length.",
            ));
        }
    }
    Ok(())
}

/// Validate the optional `Principal` and `Condition` of an attach/detach call.
fn principal_and_condition(
    body: &Value,
) -> Result<(Option<Value>, Option<Value>), AwsServiceError> {
    let principal = body.get("Principal").filter(|v| !v.is_null()).cloned();
    if let Some(p) = &principal {
        let id = p
            .get("DataLakePrincipalIdentifier")
            .and_then(Value::as_str)
            .unwrap_or("");
        if id.is_empty() || id.chars().count() > 255 {
            return Err(invalid_input(
                "Principal.DataLakePrincipalIdentifier must be 1-255 characters.",
            ));
        }
    }
    let condition = body.get("Condition").filter(|v| !v.is_null()).cloned();
    if condition
        .as_ref()
        .and_then(|c| c.get("Expression"))
        .and_then(Value::as_str)
        .is_some_and(|e| e.chars().count() > 3000)
    {
        return Err(invalid_input(
            "Condition.Expression exceeds the maximum length.",
        ));
    }
    Ok((principal, condition))
}

/// RLS policies attach to tables: the `Resource` must be a `Table` or a
/// `TableWithColumns` naming a database and a table.
fn table_of(resource: &Value) -> Result<&Value, AwsServiceError> {
    let t = resource
        .get("Table")
        .or_else(|| resource.get("TableWithColumns"))
        .ok_or_else(|| {
            invalid_input("A row-level security policy can only be attached to a table.")
        })?;
    let nonempty = |k: &str| {
        t.get(k)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
    };
    if !nonempty("DatabaseName") || !nonempty("Name") {
        return Err(invalid_input(
            "Resource must name the table's DatabaseName and Name.",
        ));
    }
    Ok(t)
}

impl LakeFormationService {
    pub(super) fn create_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let columns = body
            .get("AttachmentColumns")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| invalid_input("AttachmentColumns must be a list."))?;
        validate_attachment_columns(&columns)?;
        let filter_expression = str_field(body, "FilterExpression").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.rls_policies.contains_key(&name) {
            return Err(already_exists(format!(
                "Row-level security policy {name} already exists."
            )));
        }
        st.rls_policies.insert(
            name.clone(),
            RLSPolicyRecord {
                name,
                attachment_columns: columns,
                filter_expression,
            },
        );
        Ok(ok(json!({})))
    }

    pub(super) fn get_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let accounts = self.state.read();
        let policy = accounts
            .get(&req.account_id)
            .and_then(|st| st.rls_policies.get(&name))
            .ok_or_else(|| {
                entity_not_found(format!("Row-level security policy {name} does not exist."))
            })?;
        Ok(ok(
            json!({ "RLSPolicy": policy_value(policy, &req.account_id) }),
        ))
    }

    pub(super) fn update_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let policy = st.rls_policies.get_mut(&name).ok_or_else(|| {
            entity_not_found(format!("Row-level security policy {name} does not exist."))
        })?;
        if let Some(expr) = str_field(body, "FilterExpression") {
            policy.filter_expression = expr;
        }
        Ok(ok(json!({})))
    }

    pub(super) fn delete_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.rls_policies.remove(&name).is_none() {
            return Err(entity_not_found(format!(
                "Row-level security policy {name} does not exist."
            )));
        }
        // Deleting a policy removes its attachments and the permissions
        // granted on it.
        st.rls_policy_attachments.retain(|a| a.policy_name != name);
        let account = req.account_id.clone();
        st.grants.retain(|g| {
            rls_policy_ref(&g.resource, &account).is_none_or(|(cat, n)| cat != account || n != name)
        });
        Ok(ok(json!({})))
    }

    pub(super) fn list_rls_policies(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.rls_policies
                    .values()
                    .map(|p| policy_value(p, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "RLSPolicies", body, &[])
    }

    pub(super) fn attach_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let table = table_of(&resource)?;
        let (principal, condition) = principal_and_condition(body)?;
        let set_default = body.get("SetDefault").and_then(Value::as_bool) == Some(true);
        if principal.is_none() && !set_default {
            return Err(invalid_input(
                "Principal is required unless the policy is attached as the resource default.",
            ));
        }
        let account = req.account_id.clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        let policy = st.rls_policies.get(&name).ok_or_else(|| {
            entity_not_found(format!("Row-level security policy {name} does not exist."))
        })?;
        // A column-scoped resource must expose every column the policy's
        // filter expression references.
        if let Some(cols) = table.get("ColumnNames").and_then(Value::as_array) {
            let cols: Vec<&str> = cols.iter().filter_map(Value::as_str).collect();
            if let Some(missing) = policy
                .attachment_columns
                .iter()
                .filter_map(|c| c.get("Name").and_then(Value::as_str))
                .find(|c| !cols.contains(c))
            {
                return Err(invalid_input(format!(
                    "Resource does not include the policy's attachment column {missing}."
                )));
            }
        }
        let existing = st.rls_policy_attachments.iter().position(|a| {
            a.policy_name == name
                && resource_matches(&resource, &a.resource, &account)
                && same_principal(&principal, &a.principal)
                && a.condition == condition
        });
        if let Some(i) = existing {
            if !set_default || st.rls_policy_attachments[i].default {
                return Err(already_exists(format!(
                    "Row-level security policy {name} is already attached to this resource."
                )));
            }
        }
        if set_default {
            // A resource has at most one default RLS policy.
            for a in st.rls_policy_attachments.iter_mut() {
                if resource_matches(&resource, &a.resource, &account) {
                    a.default = false;
                }
            }
        }
        match existing {
            Some(i) => st.rls_policy_attachments[i].default = true,
            None => st.rls_policy_attachments.push(RLSPolicyAttachmentRecord {
                policy_name: name,
                resource,
                principal,
                condition,
                default: set_default,
            }),
        }
        Ok(ok(json!({})))
    }

    pub(super) fn detach_rls_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_field(body, "Name").unwrap_or_default();
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        table_of(&resource)?;
        let (principal, condition) = principal_and_condition(body)?;
        let remove_default = body.get("RemoveDefault").and_then(Value::as_bool) == Some(true);
        let account = req.account_id.clone();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&account);
        if !st.rls_policies.contains_key(&name) {
            return Err(entity_not_found(format!(
                "Row-level security policy {name} does not exist."
            )));
        }
        let i = st
            .rls_policy_attachments
            .iter()
            .position(|a| {
                a.policy_name == name
                    && resource_matches(&resource, &a.resource, &account)
                    && same_principal(&principal, &a.principal)
                    && a.condition == condition
                    && (!remove_default || a.default)
            })
            .ok_or_else(|| {
                entity_not_found(format!(
                    "Row-level security policy {name} is not attached to this resource."
                ))
            })?;
        // RemoveDefault only drops the default marking; a principal-scoped
        // attachment keeps applying to its principal, while a principal-less
        // default attachment has nothing left to apply to.
        if remove_default && st.rls_policy_attachments[i].principal.is_some() {
            st.rls_policy_attachments[i].default = false;
        } else {
            st.rls_policy_attachments.remove(i);
        }
        Ok(ok(json!({})))
    }

    pub(super) fn list_rls_policy_attachments(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = req.account_id.clone();
        let resource_filter = body.get("Resource").filter(|v| !v.is_null()).cloned();
        let principal_filter = body.get("Principal").filter(|v| !v.is_null()).cloned();
        let policy_filter = str_field(body, "PolicyName");
        let accounts = self.state.read();
        let st = accounts.get(&account);
        if let Some(p) = &policy_filter {
            if !st.is_some_and(|st| st.rls_policies.contains_key(p)) {
                return Err(entity_not_found(format!(
                    "Row-level security policy {p} does not exist."
                )));
            }
        }
        let items: Vec<Value> = st
            .map(|st| {
                st.rls_policy_attachments
                    .iter()
                    .filter(|a| policy_filter.as_ref().is_none_or(|p| &a.policy_name == p))
                    .filter(|a| {
                        resource_filter
                            .as_ref()
                            .is_none_or(|r| resource_matches(r, &a.resource, &account))
                    })
                    .filter(|a| {
                        principal_filter.as_ref().is_none_or(|p| {
                            a.principal
                                .as_ref()
                                .is_some_and(|ap| principal_matches(p, ap))
                        })
                    })
                    .map(|a| attachment_value(a, &account))
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "RLSPolicyAttachments", body, &[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    const ACCOUNT: &str = "123456789012";

    fn svc() -> LakeFormationService {
        LakeFormationService::new(Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "",
        ))))
    }

    fn call(
        svc: &LakeFormationService,
        action: &str,
        body: Value,
    ) -> Result<Value, AwsServiceError> {
        let req = AwsRequest {
            service: "lakeformation".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: Default::default(),
            query_params: Default::default(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: format!("/{action}"),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = svc.dispatch(action, &req)?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap_or(Value::Null))
    }

    fn table() -> Value {
        json!({"Table": {"DatabaseName": "sales", "Name": "orders"}})
    }

    fn analyst() -> Value {
        json!({"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/analyst"})
    }

    fn create(svc: &LakeFormationService, name: &str) {
        call(
            svc,
            "CreateRLSPolicy",
            json!({
                "Name": name,
                "AttachmentColumns": [{"Name": "region", "Type": "string"}],
                "FilterExpression": "region = 'us'",
            }),
        )
        .unwrap();
    }

    #[test]
    fn policy_crud_round_trips() {
        let svc = svc();
        create(&svc, "us-only");
        let dup = call(
            &svc,
            "CreateRLSPolicy",
            json!({"Name": "us-only", "AttachmentColumns": [], "FilterExpression": "1=1"}),
        )
        .unwrap_err();
        assert_eq!(dup.code(), "AlreadyExistsException");

        let got = call(&svc, "GetRLSPolicy", json!({"Name": "us-only"})).unwrap();
        assert_eq!(got["RLSPolicy"]["FilterExpression"], "region = 'us'");
        assert_eq!(got["RLSPolicy"]["CatalogId"], ACCOUNT);
        assert_eq!(got["RLSPolicy"]["AttachmentColumns"][0]["Name"], "region");

        call(
            &svc,
            "UpdateRLSPolicy",
            json!({"Name": "us-only", "FilterExpression": "region = 'eu'"}),
        )
        .unwrap();
        let listed = call(&svc, "ListRLSPolicies", json!({})).unwrap();
        assert_eq!(
            listed["RLSPolicies"][0]["FilterExpression"],
            "region = 'eu'"
        );

        call(&svc, "DeleteRLSPolicy", json!({"Name": "us-only"})).unwrap();
        let e = call(&svc, "GetRLSPolicy", json!({"Name": "us-only"})).unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");
        let e = call(&svc, "DeleteRLSPolicy", json!({"Name": "us-only"})).unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");
    }

    #[test]
    fn create_validates_input() {
        let svc = svc();
        for body in [
            json!({"AttachmentColumns": [], "FilterExpression": "x"}),
            json!({"Name": "p", "FilterExpression": "x"}),
            json!({"Name": "p", "AttachmentColumns": []}),
            json!({"Name": "p", "AttachmentColumns": [{}], "FilterExpression": "x"}),
            json!({"Name": "p", "AttachmentColumns": [], "FilterExpression": "x".repeat(2049)}),
        ] {
            let e = call(&svc, "CreateRLSPolicy", body).unwrap_err();
            assert_eq!(e.code(), "InvalidInputException");
        }
    }

    #[test]
    fn attach_detach_and_defaults() {
        let svc = svc();
        create(&svc, "p1");
        create(&svc, "p2");

        // Unknown policy, non-table resource, missing principal.
        let e = call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "nope", "Resource": table(), "Principal": analyst()}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");
        let e = call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": {"Database": {"Name": "sales"}}, "Principal": analyst()}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "InvalidInputException");
        let e = call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": table()}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "InvalidInputException");
        // A column-scoped resource missing the policy's column.
        let e = call(
            &svc,
            "AttachRLSPolicy",
            json!({
                "Name": "p1",
                "Resource": {"TableWithColumns": {"DatabaseName": "sales", "Name": "orders", "ColumnNames": ["id"]}},
                "Principal": analyst(),
            }),
        )
        .unwrap_err();
        assert_eq!(e.code(), "InvalidInputException");

        call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst()}),
        )
        .unwrap();
        let e = call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst()}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "AlreadyExistsException");
        call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p2", "Resource": table(), "SetDefault": true}),
        )
        .unwrap();
        // Promoting p1's attachment to default demotes p2's.
        call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst(), "SetDefault": true}),
        )
        .unwrap();
        let list = call(
            &svc,
            "ListRLSPolicyAttachments",
            json!({"Resource": table()}),
        )
        .unwrap();
        let items = list["RLSPolicyAttachments"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["PolicyName"], "p1");
        assert_eq!(items[0]["DefaultRLSPolicy"], true);
        assert_eq!(items[1]["PolicyName"], "p2");
        assert_eq!(items[1]["DefaultRLSPolicy"], false);

        let by_principal = call(
            &svc,
            "ListRLSPolicyAttachments",
            json!({"Principal": analyst()}),
        )
        .unwrap();
        assert_eq!(
            by_principal["RLSPolicyAttachments"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let e = call(
            &svc,
            "ListRLSPolicyAttachments",
            json!({"PolicyName": "nope"}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");

        // RemoveDefault keeps the principal-scoped attachment, un-defaulted.
        call(
            &svc,
            "DetachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst(), "RemoveDefault": true}),
        )
        .unwrap();
        let list = call(
            &svc,
            "ListRLSPolicyAttachments",
            json!({"PolicyName": "p1"}),
        )
        .unwrap();
        assert_eq!(list["RLSPolicyAttachments"][0]["DefaultRLSPolicy"], false);

        call(
            &svc,
            "DetachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst()}),
        )
        .unwrap();
        let e = call(
            &svc,
            "DetachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "Principal": analyst()}),
        )
        .unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");

        // Deleting a policy removes its attachments.
        call(&svc, "DeleteRLSPolicy", json!({"Name": "p2"})).unwrap();
        let list = call(&svc, "ListRLSPolicyAttachments", json!({})).unwrap();
        assert!(list["RLSPolicyAttachments"].as_array().unwrap().is_empty());
    }

    #[test]
    fn state_answers_default_policy_and_registered_locations() {
        let svc = svc();
        create(&svc, "p1");
        call(
            &svc,
            "AttachRLSPolicy",
            json!({"Name": "p1", "Resource": table(), "SetDefault": true}),
        )
        .unwrap();
        call(
            &svc,
            "RegisterResource",
            json!({"ResourceArn": "arn:aws:s3:::lake/udfs", "UseServiceLinkedRole": true}),
        )
        .unwrap();
        let accounts = svc.state.read();
        let st = accounts.get(ACCOUNT).unwrap();
        assert_eq!(
            st.default_rls_policy(ACCOUNT, ACCOUNT, "sales", "orders")
                .as_deref(),
            Some("p1")
        );
        assert!(st
            .default_rls_policy(ACCOUNT, ACCOUNT, "sales", "other")
            .is_none());
        assert!(st
            .default_rls_policy(ACCOUNT, "999999999999", "sales", "orders")
            .is_none());
        assert!(st.location_registered("s3://lake/udfs"));
        assert!(st.location_registered("s3://lake/udfs/v1/"));
        assert!(!st.location_registered("s3://lake/udfs-other"));
        assert!(!st.location_registered("s3://elsewhere/udfs"));
    }

    #[test]
    fn permissions_on_rls_policy_resources() {
        let svc = svc();
        let resource = json!({"RLSPolicy": {"Name": "p1"}});
        let grant =
            json!({"Principal": analyst(), "Resource": resource, "Permissions": ["DESCRIBE"]});
        let e = call(&svc, "GrantPermissions", grant.clone()).unwrap_err();
        assert_eq!(e.code(), "EntityNotFoundException");

        create(&svc, "p1");
        call(&svc, "GrantPermissions", grant.clone()).unwrap();
        // Explicit own-catalog id names the same resource.
        let listed = call(
            &svc,
            "ListPermissions",
            json!({"Resource": {"RLSPolicy": {"CatalogId": ACCOUNT, "Name": "p1"}}}),
        )
        .unwrap();
        let perms = listed["PrincipalResourcePermissions"].as_array().unwrap();
        assert_eq!(perms.len(), 1);
        assert_eq!(perms[0]["Resource"]["RLSPolicy"]["Name"], "p1");

        let batch = call(
            &svc,
            "BatchGrantPermissions",
            json!({"Entries": [
                {"Id": "1", "Principal": analyst(), "Resource": {"RLSPolicy": {"Name": "missing"}}, "Permissions": ["DESCRIBE"]},
            ]}),
        )
        .unwrap();
        assert_eq!(
            batch["Failures"][0]["Error"]["ErrorCode"],
            "EntityNotFoundException"
        );

        call(&svc, "RevokePermissions", grant.clone()).unwrap();
        let listed = call(&svc, "ListPermissions", json!({"Resource": resource})).unwrap();
        assert!(listed["PrincipalResourcePermissions"]
            .as_array()
            .unwrap()
            .is_empty());

        // Deleting the policy drops grants on it.
        call(&svc, "GrantPermissions", grant).unwrap();
        call(&svc, "DeleteRLSPolicy", json!({"Name": "p1"})).unwrap();
        let listed = call(&svc, "ListPermissions", json!({})).unwrap();
        assert!(listed["PrincipalResourcePermissions"]
            .as_array()
            .unwrap()
            .is_empty());
    }
}
