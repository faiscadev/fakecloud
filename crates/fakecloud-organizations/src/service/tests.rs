use super::*;
use bytes::Bytes;
use http::StatusCode;
use http::{HeaderMap, Method};
use serde_json::{json, Value};
use std::collections::HashMap;

fn req_with(account: &str, action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "organizations".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: account.to_string(),
        request_id: "test".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn body_json(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match r {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    }
}

#[tokio::test]
async fn create_organization_succeeds_once() {
    let (svc, state) = OrganizationsService::shared();
    let resp = svc
        .handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let v = body_json(&resp);
    assert_eq!(v["Organization"]["MasterAccountId"], "111111111111");
    assert!(state.read().is_some());
}

#[tokio::test]
async fn create_organization_twice_errors() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AlreadyInOrganizationException");
}

#[tokio::test]
async fn describe_without_org_errors() {
    let (svc, _state) = OrganizationsService::shared();
    let err = expect_err(
        svc.handle(req_with("111111111111", "DescribeOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
}

#[tokio::test]
async fn describe_round_trips_create() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let resp = svc
        .handle(req_with("111111111111", "DescribeOrganization", json!({})))
        .await
        .unwrap();
    let v = body_json(&resp);
    assert_eq!(v["Organization"]["MasterAccountId"], "111111111111");
    assert_eq!(v["Organization"]["FeatureSet"], "ALL");
}

#[tokio::test]
async fn non_member_describe_returns_not_in_use() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with("222222222222", "DescribeOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
}

#[tokio::test]
async fn non_member_delete_returns_not_in_use() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with("222222222222", "DeleteOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
}

#[tokio::test]
async fn member_non_management_delete_returns_access_denied() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    // Simulate Batch 2 membership by enrolling a second account
    // directly in state (auto-enrollment lands in Batch 2).
    {
        let mut guard = state.write();
        let org = guard.as_mut().unwrap();
        let account_id = "222222222222".to_string();
        let parent_id = org.root_id.clone();
        let org_id = org.org_id.clone();
        let arn = format!(
            "arn:aws:organizations::111111111111:account/{}/{}",
            org_id, account_id
        );
        org.accounts.insert(
            account_id.clone(),
            crate::state::MemberAccount {
                id: account_id.clone(),
                arn,
                email: "member@example.com".to_string(),
                name: "member".to_string(),
                status: "ACTIVE".to_string(),
                joined_method: "INVITED".to_string(),
                joined_timestamp: chrono::Utc::now(),
                parent_id,
            },
        );
    }
    let err = expect_err(
        svc.handle(req_with("222222222222", "DeleteOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");
}

#[tokio::test]
async fn delete_clears_state() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    svc.handle(req_with("111111111111", "DeleteOrganization", json!({})))
        .await
        .unwrap();
    assert!(state.read().is_none());
}

#[tokio::test]
async fn create_with_consolidated_billing_accepted() {
    let (svc, _state) = OrganizationsService::shared();
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganization",
            json!({"FeatureSet": "CONSOLIDATED_BILLING"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(&resp)["Organization"]["FeatureSet"],
        "CONSOLIDATED_BILLING"
    );
}

#[tokio::test]
async fn create_with_invalid_feature_set_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreateOrganization",
            json!({"FeatureSet": "NONSENSE"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

/// Helper: create org with ACCOUNT_A as management, return shared
/// state + root id for subsequent assertions.
async fn create_org_with_root(svc: &Arc<OrganizationsService>) -> String {
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let roots = svc
        .handle(req_with("111111111111", "ListRoots", json!({})))
        .await
        .unwrap();
    let v = body_json(&roots);
    v["Roots"][0]["Id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn list_roots_returns_single_root() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    assert!(root_id.starts_with("r-"));
}

#[tokio::test]
async fn list_roots_non_member_hidden() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with("999999999999", "ListRoots", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
}

#[tokio::test]
async fn create_ou_happy_path_and_describe() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "eng"}),
        ))
        .await
        .unwrap();
    let ou = body_json(&created);
    let ou_id = ou["OrganizationalUnit"]["Id"].as_str().unwrap().to_string();
    assert!(ou_id.starts_with("ou-"));

    let described = svc
        .handle(req_with(
            "111111111111",
            "DescribeOrganizationalUnit",
            json!({"OrganizationalUnitId": ou_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&described);
    assert_eq!(v["OrganizationalUnit"]["Name"], "eng");
}

#[tokio::test]
async fn create_ou_applies_create_time_tags() {
    // Create-time Tags were dropped (bug-audit 2026-06-20, 1.24); they must be
    // visible via ListTagsForResource without a follow-up TagResource.
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({
                "ParentId": root_id,
                "Name": "eng",
                "Tags": [{"Key": "team", "Value": "platform"}]
            }),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    let listed = svc
        .handle(req_with(
            "111111111111",
            "ListTagsForResource",
            json!({"ResourceId": ou_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&listed);
    let tags = v["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "team" && t["Value"] == "platform"),
        "create-time tag must be listed: {v}"
    );
}

#[tokio::test]
async fn create_policy_applies_create_time_tags() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({
                "Name": "p1",
                "Description": "d",
                "Type": "SERVICE_CONTROL_POLICY",
                "Content": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\"}]}",
                "Tags": [{"Key": "env", "Value": "prod"}]
            }),
        ))
        .await
        .unwrap();
    let policy_id = body_json(&created)["Policy"]["PolicySummary"]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    let listed = svc
        .handle(req_with(
            "111111111111",
            "ListTagsForResource",
            json!({"ResourceId": policy_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&listed);
    assert!(
        v["Tags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "prod"),
        "create-time tag must be listed: {v}"
    );
}

#[tokio::test]
async fn create_ou_missing_parent_id_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"Name": "eng"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn create_ou_duplicate_under_same_parent() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    svc.handle(req_with(
        "111111111111",
        "CreateOrganizationalUnit",
        json!({"ParentId": root_id, "Name": "eng"}),
    ))
    .await
    .unwrap();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "eng"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "DuplicateOrganizationalUnitException");
}

#[tokio::test]
async fn create_ou_unknown_parent_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": "ou-bogus", "Name": "eng"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ParentNotFoundException");
}

#[tokio::test]
async fn create_ou_non_management_rejected() {
    let (svc, state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    // Enroll a non-management member directly.
    {
        let mut guard = state.write();
        guard
            .as_mut()
            .unwrap()
            .enroll_account_if_missing("222222222222");
    }
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "eng"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");
}

#[tokio::test]
async fn create_ou_without_org_not_in_use() {
    let (svc, _state) = OrganizationsService::shared();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": "r-whatever", "Name": "eng"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
}

#[tokio::test]
async fn update_ou_renames_and_rejects_dup() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "eng"}),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    svc.handle(req_with(
        "111111111111",
        "CreateOrganizationalUnit",
        json!({"ParentId": root_id, "Name": "ops"}),
    ))
    .await
    .unwrap();

    let renamed = svc
        .handle(req_with(
            "111111111111",
            "UpdateOrganizationalUnit",
            json!({"OrganizationalUnitId": ou_id, "Name": "platform"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(&renamed)["OrganizationalUnit"]["Name"],
        "platform"
    );

    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "UpdateOrganizationalUnit",
            json!({"OrganizationalUnitId": ou_id, "Name": "ops"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "DuplicateOrganizationalUnitException");
}

#[tokio::test]
async fn update_ou_unknown_id_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "UpdateOrganizationalUnit",
            json!({"OrganizationalUnitId": "ou-unknown", "Name": "x"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "OrganizationalUnitNotFoundException");
}

#[tokio::test]
async fn delete_ou_rejects_when_not_empty() {
    let (svc, state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "eng"}),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let mut guard = state.write();
        let org = guard.as_mut().unwrap();
        org.enroll_account_if_missing("222222222222");
        let root = org.root_id.clone();
        org.move_account("222222222222", &root, &ou_id).unwrap();
    }
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DeleteOrganizationalUnit",
            json!({"OrganizationalUnitId": ou_id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "OrganizationalUnitNotEmptyException");
}

#[tokio::test]
async fn delete_ou_unknown_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DeleteOrganizationalUnit",
            json!({"OrganizationalUnitId": "ou-unknown"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "OrganizationalUnitNotFoundException");
}

#[tokio::test]
async fn describe_ou_unknown_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribeOrganizationalUnit",
            json!({"OrganizationalUnitId": "ou-unknown"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "OrganizationalUnitNotFoundException");
}

#[tokio::test]
async fn list_ous_for_parent_filters_by_parent() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "top"}),
        ))
        .await
        .unwrap();
    let top_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    svc.handle(req_with(
        "111111111111",
        "CreateOrganizationalUnit",
        json!({"ParentId": top_id, "Name": "child"}),
    ))
    .await
    .unwrap();

    let under_root = svc
        .handle(req_with(
            "111111111111",
            "ListOrganizationalUnitsForParent",
            json!({"ParentId": root_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&under_root);
    assert_eq!(v["OrganizationalUnits"].as_array().unwrap().len(), 1);
    assert_eq!(v["OrganizationalUnits"][0]["Id"], top_id);

    let under_top = svc
        .handle(req_with(
            "111111111111",
            "ListOrganizationalUnitsForParent",
            json!({"ParentId": top_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&under_top);
    assert_eq!(v["OrganizationalUnits"].as_array().unwrap().len(), 1);
    assert_eq!(v["OrganizationalUnits"][0]["Name"], "child");
}

#[tokio::test]
async fn list_ous_for_parent_unknown_parent() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListOrganizationalUnitsForParent",
            json!({"ParentId": "ou-unknown"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ParentNotFoundException");
}

#[tokio::test]
async fn list_accounts_returns_all_members() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    {
        let mut guard = state.write();
        guard
            .as_mut()
            .unwrap()
            .enroll_account_if_missing("222222222222");
    }
    let resp = svc
        .handle(req_with("111111111111", "ListAccounts", json!({})))
        .await
        .unwrap();
    let v = body_json(&resp);
    assert_eq!(v["Accounts"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_accounts_for_parent_scopes_to_parent() {
    let (svc, state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "team"}),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let mut guard = state.write();
        let org = guard.as_mut().unwrap();
        org.enroll_account_if_missing("222222222222");
        org.move_account("222222222222", &org.root_id.clone(), &ou_id)
            .unwrap();
    }
    let in_ou = svc
        .handle(req_with(
            "111111111111",
            "ListAccountsForParent",
            json!({"ParentId": ou_id}),
        ))
        .await
        .unwrap();
    let v = body_json(&in_ou);
    assert_eq!(v["Accounts"].as_array().unwrap().len(), 1);
    assert_eq!(v["Accounts"][0]["Id"], "222222222222");
}

#[tokio::test]
async fn list_accounts_for_parent_unknown_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListAccountsForParent",
            json!({"ParentId": "ou-unknown"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ParentNotFoundException");
}

#[tokio::test]
async fn describe_account_roundtrip_and_unknown() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "DescribeAccount",
            json!({"AccountId": "111111111111"}),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(&resp)["Account"]["Id"], "111111111111");

    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribeAccount",
            json!({"AccountId": "999999999999"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccountNotFoundException");
}

#[tokio::test]
async fn move_account_happy_path() {
    let (svc, state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "team"}),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let mut guard = state.write();
        guard
            .as_mut()
            .unwrap()
            .enroll_account_if_missing("222222222222");
    }
    svc.handle(req_with(
        "111111111111",
        "MoveAccount",
        json!({
            "AccountId": "222222222222",
            "SourceParentId": root_id,
            "DestinationParentId": ou_id,
        }),
    ))
    .await
    .unwrap();
    let guard = state.read();
    let org = guard.as_ref().unwrap();
    assert_eq!(org.accounts.get("222222222222").unwrap().parent_id, ou_id);
}

#[tokio::test]
async fn move_account_unknown_account() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "MoveAccount",
            json!({
                "AccountId": "777777777777",
                "SourceParentId": root_id,
                "DestinationParentId": root_id,
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccountNotFoundException");
}

#[tokio::test]
async fn move_account_wrong_source_parent() {
    let (svc, state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let created = svc
        .handle(req_with(
            "111111111111",
            "CreateOrganizationalUnit",
            json!({"ParentId": root_id, "Name": "team"}),
        ))
        .await
        .unwrap();
    let ou_id = body_json(&created)["OrganizationalUnit"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let mut guard = state.write();
        guard
            .as_mut()
            .unwrap()
            .enroll_account_if_missing("222222222222");
    }
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "MoveAccount",
            json!({
                "AccountId": "222222222222",
                "SourceParentId": ou_id,
                "DestinationParentId": root_id,
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "SourceParentNotFoundException");
}

#[tokio::test]
async fn move_account_unknown_destination() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "MoveAccount",
            json!({
                "AccountId": "111111111111",
                "SourceParentId": root_id,
                "DestinationParentId": "ou-bogus",
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "DestinationParentNotFoundException");
}

#[tokio::test]
async fn unknown_action_returns_not_implemented() {
    let (svc, _state) = OrganizationsService::shared();
    let err = expect_err(
        svc.handle(req_with("111111111111", "BogusAction", json!({})))
            .await,
    );
    // ActionNotImplemented carries NOT_IMPLEMENTED status.
    assert_eq!(err.status(), StatusCode::NOT_IMPLEMENTED);
}

const SCP_ALLOW_ALL: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

async fn create_scp(svc: &Arc<OrganizationsService>, name: &str) -> String {
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({
                "Name": name,
                "Description": "",
                "Type": "SERVICE_CONTROL_POLICY",
                "Content": SCP_ALLOW_ALL,
            }),
        ))
        .await
        .unwrap();
    body_json(&resp)["Policy"]["PolicySummary"]["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn create_policy_happy_path() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let id = create_scp(&svc, "Custom").await;
    assert!(id.starts_with("p-"));
}

#[tokio::test]
async fn create_policy_rejects_unrecognized_type() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({
                "Name": "T",
                "Description": "",
                "Type": "NONSENSE_POLICY",
                "Content": SCP_ALLOW_ALL,
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn create_policy_accepts_tag_policy_type() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({
                "Name": "MyTags",
                "Description": "",
                "Type": "TAG_POLICY",
                "Content": SCP_ALLOW_ALL,
            }),
        ))
        .await
        .unwrap();
    let v = body_json(&resp);
    assert_eq!(v["Policy"]["PolicySummary"]["Type"], "TAG_POLICY");
    assert!(v["Policy"]["PolicySummary"]["Arn"]
        .as_str()
        .unwrap()
        .contains("/tag_policy/"));
}

#[tokio::test]
async fn create_policy_malformed_content_rejected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({
                "Name": "X",
                "Description": "",
                "Type": "SERVICE_CONTROL_POLICY",
                "Content": "not json",
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "MalformedPolicyDocumentException");
}

#[tokio::test]
async fn create_policy_missing_required_fields() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CreatePolicy",
            json!({"Name": "X", "Type": "SERVICE_CONTROL_POLICY"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn create_policy_non_management_rejected() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    {
        let mut guard = state.write();
        guard
            .as_mut()
            .unwrap()
            .enroll_account_if_missing("222222222222");
    }
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "CreatePolicy",
            json!({
                "Name": "X",
                "Description": "",
                "Type": "SERVICE_CONTROL_POLICY",
                "Content": SCP_ALLOW_ALL,
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");
}

#[tokio::test]
async fn update_policy_roundtrip_and_blocks_aws_managed() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let id = create_scp(&svc, "Original").await;
    let renamed = svc
        .handle(req_with(
            "111111111111",
            "UpdatePolicy",
            json!({"PolicyId": id, "Name": "Renamed"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(&renamed)["Policy"]["PolicySummary"]["Name"],
        "Renamed"
    );
    // FullAWSAccess is AWS-managed -> blocked.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "UpdatePolicy",
            json!({"PolicyId": "p-FullAWSAccess", "Name": "Hacked"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyChangesNotAllowedException");
}

#[tokio::test]
async fn delete_policy_blocked_when_attached_and_aws_managed() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let id = create_scp(&svc, "InUse").await;
    svc.handle(req_with(
        "111111111111",
        "AttachPolicy",
        json!({"PolicyId": id, "TargetId": root_id}),
    ))
    .await
    .unwrap();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DeletePolicy",
            json!({"PolicyId": id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyInUseException");
    // AWS-managed cannot be deleted either.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DeletePolicy",
            json!({"PolicyId": "p-FullAWSAccess"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyChangesNotAllowedException");
}

#[tokio::test]
async fn describe_policy_unknown_and_known() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let id = create_scp(&svc, "X").await;
    let ok = svc
        .handle(req_with(
            "111111111111",
            "DescribePolicy",
            json!({"PolicyId": id}),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(&ok)["Policy"]["PolicySummary"]["Id"], id);
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribePolicy",
            json!({"PolicyId": "p-none"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyNotFoundException");
}

#[tokio::test]
async fn list_policies_rejects_unrecognized_filter() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListPolicies",
            json!({"Filter": "NONSENSE_POLICY"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn list_policies_includes_full_aws_access() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListPolicies",
            json!({"Filter": "SERVICE_CONTROL_POLICY"}),
        ))
        .await
        .unwrap();
    let v = body_json(&resp);
    assert!(v["Policies"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["Id"] == "p-FullAWSAccess"));
}

#[tokio::test]
async fn attach_detach_lifecycle_and_errors() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let id = create_scp(&svc, "X").await;

    // Attach then detach.
    svc.handle(req_with(
        "111111111111",
        "AttachPolicy",
        json!({"PolicyId": id, "TargetId": root_id}),
    ))
    .await
    .unwrap();
    // Re-attach is idempotent.
    svc.handle(req_with(
        "111111111111",
        "AttachPolicy",
        json!({"PolicyId": id, "TargetId": root_id}),
    ))
    .await
    .unwrap();

    // Unknown target.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "AttachPolicy",
            json!({"PolicyId": id, "TargetId": "ou-bogus"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "TargetNotFoundException");

    // Unknown policy.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "AttachPolicy",
            json!({"PolicyId": "p-none", "TargetId": root_id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyNotFoundException");

    // Detach unattached policy.
    let id2 = create_scp(&svc, "Y").await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DetachPolicy",
            json!({"PolicyId": id2, "TargetId": root_id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyNotAttachedException");

    // Happy-path detach of the first policy.
    svc.handle(req_with(
        "111111111111",
        "DetachPolicy",
        json!({"PolicyId": id, "TargetId": root_id}),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn list_policies_for_target_and_targets_for_policy() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let id = create_scp(&svc, "Custom").await;
    svc.handle(req_with(
        "111111111111",
        "AttachPolicy",
        json!({"PolicyId": id, "TargetId": root_id}),
    ))
    .await
    .unwrap();

    let list = svc
        .handle(req_with(
            "111111111111",
            "ListPoliciesForTarget",
            json!({"TargetId": root_id, "Filter": "SERVICE_CONTROL_POLICY"}),
        ))
        .await
        .unwrap();
    let v = body_json(&list);
    let names: Vec<_> = v["Policies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["Name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"Custom".to_string()));
    assert!(names.contains(&"FullAWSAccess".to_string()));

    let targets = svc
        .handle(req_with(
            "111111111111",
            "ListTargetsForPolicy",
            json!({"PolicyId": id}),
        ))
        .await
        .unwrap();
    let v = body_json(&targets);
    assert_eq!(v["Targets"].as_array().unwrap().len(), 1);
    assert_eq!(v["Targets"][0]["TargetId"], root_id);
    assert_eq!(v["Targets"][0]["Type"], "ROOT");
}

#[tokio::test]
async fn list_policies_for_target_rejects_bad_filter() {
    let (svc, _state) = OrganizationsService::shared();
    let root_id = create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListPoliciesForTarget",
            json!({"TargetId": root_id, "Filter": "NONSENSE_POLICY"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn list_targets_for_unknown_policy() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListTargetsForPolicy",
            json!({"PolicyId": "p-none"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "PolicyNotFoundException");
}

// ── account lifecycle (CreateAccount, Describe/ListCreateAccountStatus,
//    CloseAccount, RemoveAccountFromOrganization) ─────────────────────

fn body_value(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

/// Poll `DescribeCreateAccountStatus` until the request reaches a
/// terminal state, with a timeout. Mirrors how SDK callers observe
/// the async `CreateAccount` lifecycle in fakecloud.
async fn poll_until_terminal(svc: &Arc<OrganizationsService>, request_id: &str) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = svc
            .handle(req_with(
                "111111111111",
                "DescribeCreateAccountStatus",
                json!({"CreateAccountRequestId": request_id}),
            ))
            .await
            .unwrap();
        let body = body_value(resp);
        let state = body["CreateAccountStatus"]["State"]
            .as_str()
            .unwrap()
            .to_string();
        if state == "SUCCEEDED" || state == "FAILED" {
            return body;
        }
        if std::time::Instant::now() >= deadline {
            panic!("CreateAccount {request_id} did not terminate before deadline");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn create_account_starts_in_progress_then_describes_succeeded() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": "new@example.com", "AccountName": "New"}),
        ))
        .await
        .unwrap();
    let body = body_value(resp);
    let status = &body["CreateAccountStatus"];
    let request_id = status["Id"].as_str().unwrap().to_string();
    assert_eq!(status["State"].as_str().unwrap(), "IN_PROGRESS");
    assert_eq!(status["AccountName"].as_str().unwrap(), "New");
    let new_account_id = status["AccountId"].as_str().unwrap().to_string();
    assert_eq!(new_account_id.len(), 12);

    let body = poll_until_terminal(&svc, &request_id).await;
    assert_eq!(
        body["CreateAccountStatus"]["State"].as_str().unwrap(),
        "SUCCEEDED"
    );
    assert!(body["CreateAccountStatus"]["CompletedTimestamp"].is_number());
    assert_eq!(
        body["CreateAccountStatus"]["AccountId"].as_str().unwrap(),
        new_account_id
    );
}

#[tokio::test]
async fn create_account_applies_create_time_tags() {
    // Create-time Tags were dropped; they must be visible via
    // ListTagsForResource on the new account id without a follow-up
    // TagResource (bug-hunt). Tags are set at reserve-time, queryable
    // immediately even before the async enrollment completes.
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({
                "Email": "tagged@example.com",
                "AccountName": "Tagged",
                "Tags": [{"Key": "team", "Value": "platform"}]
            }),
        ))
        .await
        .unwrap();
    let acct_id = body_value(resp)["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();

    let listed = svc
        .handle(req_with(
            "111111111111",
            "ListTagsForResource",
            json!({"ResourceId": acct_id}),
        ))
        .await
        .unwrap();
    let v = body_value(listed);
    let tags = v["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "team" && t["Value"] == "platform"),
        "create-time tag must be listed: {v}"
    );
}

#[tokio::test]
async fn create_account_only_management_account_can_call() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    // Enroll a non-management account first and wait for it to succeed.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": "non-mgmt@example.com", "AccountName": "NonMgmt"}),
        ))
        .await
        .unwrap();
    let request_id = body_value(resp)["CreateAccountStatus"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let body = poll_until_terminal(&svc, &request_id).await;
    let new_id = body["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    let err = expect_err(
        svc.handle(req_with(
            &new_id,
            "CreateAccount",
            json!({"Email": "x@example.com", "AccountName": "X"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");
}

#[tokio::test]
async fn list_create_account_status_filters_by_state() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": "a@example.com", "AccountName": "A"}),
        ))
        .await
        .unwrap();
    let request_id = body_value(resp)["CreateAccountStatus"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    // Filter for IN_PROGRESS first — should include the new request.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"States": ["IN_PROGRESS"]}),
        ))
        .await
        .unwrap();
    let listed = body_value(resp);
    let arr = listed["CreateAccountStatuses"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["Id"].as_str().unwrap(), request_id);

    // Wait for the spawned completion task to flip the status, then
    // re-filter for IN_PROGRESS — the new request should drop out.
    poll_until_terminal(&svc, &request_id).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"States": ["IN_PROGRESS"]}),
        ))
        .await
        .unwrap();
    assert!(body_value(resp)["CreateAccountStatuses"]
        .as_array()
        .unwrap()
        .is_empty());
    // SUCCEEDED filter should now contain it.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"States": ["SUCCEEDED"]}),
        ))
        .await
        .unwrap();
    let arr = body_value(resp)["CreateAccountStatuses"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["Id"].as_str().unwrap(), request_id);
}

#[tokio::test]
async fn list_create_account_status_rejects_out_of_range_max_results() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    for bad in [json!(0), json!(21), json!(-1), json!("five")] {
        let err = expect_err(
            svc.handle(req_with(
                "111111111111",
                "ListCreateAccountStatus",
                json!({"MaxResults": bad}),
            ))
            .await,
        );
        assert_eq!(err.code(), "InvalidInputException");
    }
}

#[tokio::test]
async fn list_create_account_status_rejects_invalid_next_token() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"NextToken": "not-a-number"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
    // Numeric NextToken is accepted (round-trips a token we minted).
    svc.handle(req_with(
        "111111111111",
        "ListCreateAccountStatus",
        json!({"NextToken": "0"}),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn list_create_account_status_paginates_with_max_results() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    // Fire three CreateAccount requests so we have something to page over.
    let mut request_ids = Vec::new();
    for i in 0..3 {
        let resp = svc
            .handle(req_with(
                "111111111111",
                "CreateAccount",
                json!({"Email": format!("p{i}@example.com"), "AccountName": format!("P{i}")}),
            ))
            .await
            .unwrap();
        request_ids.push(
            body_value(resp)["CreateAccountStatus"]["Id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    // First page: MaxResults=2 -> 2 entries + NextToken.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"MaxResults": 2}),
        ))
        .await
        .unwrap();
    let body = body_value(resp);
    assert_eq!(body["CreateAccountStatuses"].as_array().unwrap().len(), 2);
    let next = body["NextToken"].as_str().unwrap().to_string();

    // Second page: same MaxResults + the token returns the remaining one
    // and no further token.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListCreateAccountStatus",
            json!({"MaxResults": 2, "NextToken": next}),
        ))
        .await
        .unwrap();
    let body = body_value(resp);
    assert_eq!(body["CreateAccountStatuses"].as_array().unwrap().len(), 1);
    assert!(body.get("NextToken").is_none());
}

#[tokio::test]
async fn close_account_marks_suspended_and_management_is_protected() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let new_resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": "a@example.com", "AccountName": "A"}),
        ))
        .await
        .unwrap();
    let request_id = body_value(new_resp)["CreateAccountStatus"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let body = poll_until_terminal(&svc, &request_id).await;
    let new_id = body["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    svc.handle(req_with(
        "111111111111",
        "CloseAccount",
        json!({"AccountId": new_id}),
    ))
    .await
    .unwrap();
    // Status should be SUSPENDED via DescribeAccount.
    let resp = svc
        .handle(req_with(
            "111111111111",
            "DescribeAccount",
            json!({"AccountId": new_id}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_value(resp)["Account"]["Status"].as_str().unwrap(),
        "SUSPENDED"
    );

    // Management account cannot be closed.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "CloseAccount",
            json!({"AccountId": "111111111111"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ConstraintViolationException");
}

#[tokio::test]
async fn remove_account_from_organization_drops_member() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let new_resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": "a@example.com", "AccountName": "A"}),
        ))
        .await
        .unwrap();
    let request_id = body_value(new_resp)["CreateAccountStatus"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let body = poll_until_terminal(&svc, &request_id).await;
    let new_id = body["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    svc.handle(req_with(
        "111111111111",
        "RemoveAccountFromOrganization",
        json!({"AccountId": new_id}),
    ))
    .await
    .unwrap();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribeAccount",
            json!({"AccountId": new_id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccountNotFoundException");
}

#[tokio::test]
async fn create_gov_cloud_account_returns_paired_id() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "CreateGovCloudAccount",
            json!({"Email": "gov@example.com", "AccountName": "Gov"}),
        ))
        .await
        .unwrap();
    let body = body_value(resp);
    let status = &body["CreateAccountStatus"];
    assert!(status["AccountId"].is_string());
    assert!(status["GovCloudAccountId"].is_string());
    assert_ne!(
        status["AccountId"].as_str().unwrap(),
        status["GovCloudAccountId"].as_str().unwrap()
    );
}

/// Create the org, then add one member account and return its id.
async fn add_member_account(
    svc: &std::sync::Arc<OrganizationsService>,
    email: &str,
    name: &str,
) -> String {
    let new_resp = svc
        .handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({"Email": email, "AccountName": name}),
        ))
        .await
        .unwrap();
    let request_id = body_value(new_resp)["CreateAccountStatus"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let body = poll_until_terminal(svc, &request_id).await;
    body["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn leave_organization_removes_calling_member() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let member = add_member_account(&svc, "leaver@example.com", "Leaver").await;
    svc.handle(req_with(&member, "LeaveOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribeAccount",
            json!({"AccountId": member}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccountNotFoundException");
}

#[tokio::test]
async fn leave_organization_management_cannot_leave() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with("111111111111", "LeaveOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "MasterCannotLeaveOrganizationException");
}

#[tokio::test]
async fn leave_organization_non_member_errors() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with("999999999999", "LeaveOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AccountNotFoundException");
}

#[tokio::test]
async fn list_accounts_with_invalid_effective_policy_is_empty() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListAccountsWithInvalidEffectivePolicy",
            json!({"PolicyType": "TAG_POLICY"}),
        ))
        .await
        .unwrap();
    let v = body_value(resp);
    assert_eq!(v["PolicyType"], "TAG_POLICY");
    assert_eq!(v["Accounts"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_accounts_with_invalid_effective_policy_requires_type() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "ListAccountsWithInvalidEffectivePolicy",
            json!({}),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}

#[tokio::test]
async fn list_effective_policy_validation_errors_is_empty() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "ListEffectivePolicyValidationErrors",
            json!({"AccountId": "111111111111", "PolicyType": "BACKUP_POLICY"}),
        ))
        .await
        .unwrap();
    let v = body_value(resp);
    assert_eq!(v["AccountId"], "111111111111");
    assert_eq!(v["PolicyType"], "BACKUP_POLICY");
    assert_eq!(
        v["EffectivePolicyValidationErrors"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn responsibility_transfer_lifecycle() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    // Invite an outbound BILLING transfer.
    let invite = svc
        .handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "my-billing-transfer",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap();
    let invite_body = body_value(invite);
    assert_eq!(
        invite_body["Handshake"]["Action"],
        "TRANSFER_RESPONSIBILITY"
    );

    // It must surface in the outbound list, not the inbound list.
    let out = svc
        .handle(req_with(
            "111111111111",
            "ListOutboundResponsibilityTransfers",
            json!({"Type": "BILLING"}),
        ))
        .await
        .unwrap();
    let out_body = body_value(out);
    let transfers = out_body["ResponsibilityTransfers"].as_array().unwrap();
    assert_eq!(transfers.len(), 1);
    let transfer_id = transfers[0]["Id"].as_str().unwrap().to_string();
    assert_eq!(transfers[0]["Status"], "REQUESTED");
    assert_eq!(transfers[0]["Type"], "BILLING");

    let inbound = svc
        .handle(req_with(
            "111111111111",
            "ListInboundResponsibilityTransfers",
            json!({"Type": "BILLING"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_value(inbound)["ResponsibilityTransfers"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Describe echoes the record.
    let desc = svc
        .handle(req_with(
            "111111111111",
            "DescribeResponsibilityTransfer",
            json!({"Id": transfer_id}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_value(desc)["ResponsibilityTransfer"]["Name"],
        "my-billing-transfer"
    );

    // Rename it.
    let upd = svc
        .handle(req_with(
            "111111111111",
            "UpdateResponsibilityTransfer",
            json!({"Id": transfer_id, "Name": "renamed-transfer"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_value(upd)["ResponsibilityTransfer"]["Name"],
        "renamed-transfer"
    );

    // Terminate it: status flips to WITHDRAWN with an EndTimestamp.
    let term = svc
        .handle(req_with(
            "111111111111",
            "TerminateResponsibilityTransfer",
            json!({"Id": transfer_id}),
        ))
        .await
        .unwrap();
    let term_body = body_value(term);
    assert_eq!(term_body["ResponsibilityTransfer"]["Status"], "WITHDRAWN");
    assert!(term_body["ResponsibilityTransfer"]["EndTimestamp"].is_number());

    // Terminating again is rejected (already withdrawn).
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "TerminateResponsibilityTransfer",
            json!({"Id": transfer_id}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ResponsibilityTransferAlreadyInStatusException");
}

#[tokio::test]
async fn describe_responsibility_transfer_unknown_id_errors() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DescribeResponsibilityTransfer",
            json!({"Id": "rt-doesnotexist"}),
        ))
        .await,
    );
    assert_eq!(err.code(), "ResponsibilityTransferNotFoundException");
}

#[tokio::test]
async fn invite_responsibility_transfer_rejects_bad_type() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "NOPE",
                "SourceName": "x",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidInputException");
}
