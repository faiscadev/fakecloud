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
    assert!(!state.read().is_empty());
}

#[tokio::test]
async fn create_organization_twice_from_the_same_account_errors() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AlreadyInOrganizationException");
}

/// #2543: organizations are independent. One account creating an
/// organization must not stop an unrelated account from creating its
/// own, and the two must not see each other.
#[tokio::test]
async fn a_second_account_can_create_its_own_organization() {
    let (svc, state) = OrganizationsService::shared();
    let first = body_json(
        &svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
            .await
            .unwrap(),
    );
    let second = body_json(
        &svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
            .await
            .unwrap(),
    );

    let first_id = first["Organization"]["Id"].as_str().unwrap();
    let second_id = second["Organization"]["Id"].as_str().unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(state.read().len(), 2);

    // Each management account describes only its own organization.
    let described = body_json(
        &svc.handle(req_with("222222222222", "DescribeOrganization", json!({})))
            .await
            .unwrap(),
    );
    assert_eq!(described["Organization"]["Id"], second_id);
    assert_eq!(described["Organization"]["MasterAccountId"], "222222222222");

    // ...and lists only its own accounts.
    let listed = body_json(
        &svc.handle(req_with("222222222222", "ListAccounts", json!({})))
            .await
            .unwrap(),
    );
    let ids: Vec<&str> = listed["Accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["Id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["222222222222"]);
}

/// An account already in an organization cannot create another one, and
/// the error is the same whichever organization it belongs to.
#[tokio::test]
async fn a_member_of_another_organization_cannot_create_one() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");

    let err = expect_err(
        svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AlreadyInOrganizationException");
}

/// An account can only ever be in one organization, so an invitation to
/// an account another organization already holds is rejected up front
/// rather than opening a handshake that could never be accepted.
#[tokio::test]
async fn inviting_an_account_from_another_organization_errors() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");
}

/// Reads that used to be satisfied by "an organization exists" now
/// resolve the caller's own organization. A bystander account must not
/// be able to read another organization's tree, tags or resource policy
/// by guessing ids.
#[tokio::test]
async fn a_bystander_cannot_read_another_organizations_state() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let root_id = state.read().sole().unwrap().root_id.clone();

    svc.handle(req_with(
        "111111111111",
        "PutResourcePolicy",
        json!({ "Content": "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }),
    ))
    .await
    .unwrap();

    for (action, body) in [
        ("ListParents", json!({ "ChildId": "111111111111" })),
        (
            "ListChildren",
            json!({ "ParentId": root_id, "ChildType": "ACCOUNT" }),
        ),
        ("ListTagsForResource", json!({ "ResourceId": root_id })),
        ("DescribeResourcePolicy", json!({})),
        (
            "DescribeEffectivePolicy",
            json!({ "PolicyType": "SERVICE_CONTROL_POLICY" }),
        ),
    ] {
        let err = expect_err(svc.handle(req_with("999999999999", action, body)).await);
        assert_eq!(
            err.code(),
            "AWSOrganizationsNotInUseException",
            "{action} leaked another organization's state to a non-member"
        );
    }
}

/// A handshake is readable only by its two parties. Otherwise a
/// bystander could enumerate handshake ids to learn another
/// organization's id and management account.
#[tokio::test]
async fn describe_handshake_is_limited_to_the_parties() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    // The target can read it...
    svc.handle(req_with(
        "222222222222",
        "DescribeHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .unwrap();

    // ...a bystander cannot.
    let err = expect_err(
        svc.handle(req_with(
            "999999999999",
            "DescribeHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeNotFoundException");
}

/// Membership is re-checked when the handshake is accepted, not only
/// when it is opened: the target may have joined another organization
/// while the invitation sat open.
#[tokio::test]
async fn accepting_an_invite_fails_once_the_target_joined_another_organization() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    // The target creates its own organization before answering.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "AcceptHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");
}

/// An EMAIL-target invite records the address, not the account id. The
/// account it names must still be able to read and accept it — matching
/// the raw field would compare an address against a 12-digit id and
/// never hit.
#[tokio::test]
async fn an_email_target_invite_is_readable_and_acceptable_by_the_account_it_names() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "EMAIL", "Id": "222222222222@example.com" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    svc.handle(req_with(
        "222222222222",
        "DescribeHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("the named account is a party to the invitation");

    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("the named account can accept");

    assert!(state
        .read()
        .org_of_account("222222222222")
        .is_some_and(|org| org.is_management("111111111111")));
}

/// The cross-organization guard applies to an EMAIL target too, once it
/// resolves — otherwise the invite opens a handshake nobody can accept.
#[tokio::test]
async fn an_email_invite_to_an_account_in_another_organization_errors() {
    let (svc, _state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "EMAIL", "Id": "222222222222@example.com" } }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");
}

/// `Target.Id` must match the declared `Target.Type`. Accepting a
/// mismatch opened a handshake keyed by a string no caller can ever
/// authenticate as, which then sat OPEN forever with no error anywhere
/// the caller could see it.
#[tokio::test]
async fn invite_rejects_a_target_id_that_does_not_match_its_type() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;

    for target in [
        json!({ "Type": "ACCOUNT", "Id": "bob@corp.com" }),
        json!({ "Type": "ACCOUNT", "Id": "12345" }),
        json!({ "Type": "EMAIL", "Id": "222222222222" }),
        json!({ "Type": "SOMETHING", "Id": "222222222222" }),
    ] {
        let err = expect_err(
            svc.handle(req_with(
                "111111111111",
                "InviteAccountToOrganization",
                json!({ "Target": target }),
            ))
            .await,
        );
        assert_eq!(
            err.code(),
            "InvalidInputException",
            "target {target} should have been rejected"
        );
    }
}

/// `TerminateResponsibilityTransfer` ends a transfer, which includes one
/// already ACCEPTED and running -- that is the case the operation exists
/// for. Syncing the transfer's status with its handshake must not lock
/// the accepted transfer out of ever being ended.
#[tokio::test]
async fn an_accepted_responsibility_transfer_can_still_be_terminated() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let invite = body_value(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invite["Handshake"]["Id"].as_str().unwrap().to_string();
    let listed = body_value(
        svc.handle(req_with(
            "111111111111",
            "ListOutboundResponsibilityTransfers",
            json!({ "Type": "BILLING" }),
        ))
        .await
        .unwrap(),
    );
    let transfer_id = listed["ResponsibilityTransfers"][0]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .unwrap();

    // An accepted transfer is starting, not over.
    let accepted = body_value(
        svc.handle(req_with(
            "111111111111",
            "DescribeResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(accepted["ResponsibilityTransfer"]["Status"], "ACCEPTED");
    assert!(
        accepted["ResponsibilityTransfer"]["EndTimestamp"].is_null(),
        "an accepted transfer has not ended"
    );

    // Once accepted the riding handshake is gone, so the TARGET -- which
    // is actively carrying the responsibility -- can end it too.
    let ended = body_value(
        svc.handle(req_with(
            "222222222222",
            "TerminateResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await
        .expect("an accepted transfer can be ended by either party"),
    );
    assert_eq!(ended["ResponsibilityTransfer"]["Status"], "WITHDRAWN");
    assert!(!ended["ResponsibilityTransfer"]["EndTimestamp"].is_null());
}

/// AWS's primary invite flow names the account owner's real address, so
/// an external address is accepted -- fakecloud just cannot resolve it
/// to an account, exactly as AWS cannot until the owner acts on the
/// emailed link.
#[tokio::test]
async fn invite_accepts_an_external_email_target() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "EMAIL", "Id": "owner@acme.com" } }),
        ))
        .await
        .expect("a real address is a valid invite target"),
    );
    assert_eq!(invited["Handshake"]["State"], "OPEN");
}

/// AWS documents `DescribeHandshake` as callable from any account in the
/// organization, not just the handshake's two parties -- while an
/// account in a DIFFERENT organization still sees nothing.
#[tokio::test]
async fn describe_handshake_is_readable_by_any_member_of_the_owning_org() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("333333333333");
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    // A plain member of the inviting organization can read it.
    svc.handle(req_with(
        "333333333333",
        "DescribeHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("any member of the owning organization may read it");

    // An account outside the organization still cannot.
    let err = expect_err(
        svc.handle(req_with(
            "999999999999",
            "DescribeHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeNotFoundException");
}

/// An EMAIL-targeted responsibility transfer must be answerable by the
/// account the address names. Recording the resolved id while still
/// labelling the target "EMAIL" made it unresolvable again, so the
/// target could neither accept nor describe its own handshake.
#[tokio::test]
async fn an_email_targeted_responsibility_transfer_is_answerable() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let invite = body_value(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222@example.com", "Type": "EMAIL"},
            }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invite["Handshake"]["Id"].as_str().unwrap().to_string();

    svc.handle(req_with(
        "222222222222",
        "DescribeHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("the account the address names is a party to it");

    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("and can accept it");
}

/// A responsibility transfer cannot name the caller's own organization,
/// nor a plain member of another one -- neither has billing
/// responsibility to hand over. An account in no organization IS a
/// valid target, the way AWS's invite addresses an owner it has not
/// seen yet. Every rejection reads the same, so the error says nothing
/// about what exists elsewhere.
#[tokio::test]
async fn a_responsibility_transfer_cannot_target_its_own_organization() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let own_email = state
        .read()
        .sole()
        .unwrap()
        .management_account_email
        .clone();

    // A plain member of the caller's own organization, by id and by both
    // spellings of its address, is just as much "itself".
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("333333333333");
    for target in [
        json!({"Id": "111111111111", "Type": "ACCOUNT"}),
        json!({"Id": own_email, "Type": "EMAIL"}),
        json!({"Id": "333333333333", "Type": "ACCOUNT"}),
        json!({"Id": "333333333333@example.com", "Type": "EMAIL"}),
    ] {
        let err = expect_err(
            svc.handle(req_with(
                "111111111111",
                "InviteOrganizationToTransferResponsibility",
                json!({
                    "Type": "BILLING",
                    "SourceName": "handover",
                    "StartTimestamp": 1893456000.0,
                    "Target": target,
                }),
            ))
            .await,
        );
        assert_eq!(err.code(), "HandshakeConstraintViolationException");
    }

    // Nor a member of ANOTHER organization that is not its management
    // account -- it cannot take over billing for one.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
    state
        .write()
        .org_of_account_mut("222222222222")
        .unwrap()
        .enroll_account_if_missing("222222220001");
    // Both spellings of that member are refused: resolving only the
    // ACCOUNT form left the EMAIL form as a way around the check.
    for target in [
        json!({"Id": "222222220001", "Type": "ACCOUNT"}),
        json!({"Id": "222222220001@example.com", "Type": "EMAIL"}),
    ] {
        let err = expect_err(
            svc.handle(req_with(
                "111111111111",
                "InviteOrganizationToTransferResponsibility",
                json!({
                    "Type": "BILLING",
                    "SourceName": "handover",
                    "StartTimestamp": 1893456000.0,
                    "Target": target,
                }),
            ))
            .await,
        );
        assert_eq!(err.code(), "HandshakeConstraintViolationException");
    }

    // An account in NO organization is allowed: AWS's invite addresses an
    // owner it has not seen yet, and the inbound reads are party-scoped
    // so the target can still find it.
    svc.handle(req_with(
        "111111111111",
        "InviteOrganizationToTransferResponsibility",
        json!({
            "Type": "BILLING",
            "SourceName": "handover",
            "StartTimestamp": 1893456000.0,
            "Target": {"Id": "ops@acme.com", "Type": "EMAIL"},
        }),
    ))
    .await
    .expect("an owner fakecloud has not seen is a valid target");

    // As is another organization's management account.
    svc.handle(req_with(
        "111111111111",
        "InviteOrganizationToTransferResponsibility",
        json!({
            "Type": "BILLING",
            "SourceName": "handover",
            "StartTimestamp": 1893456000.0,
            "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
        }),
    ))
    .await
    .expect("another organization's management account is a valid target");
}

/// Whoever can accept an invitation must also be able to find it and
/// act on what it created. Two matchers that disagreed let an account
/// accept a transfer it could then neither read nor end.
#[tokio::test]
async fn the_target_of_an_email_invite_can_find_accept_and_act_on_it() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let invite = body_value(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222@example.com", "Type": "EMAIL"},
            }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invite["Handshake"]["Id"].as_str().unwrap().to_string();

    // Findable...
    let listed = body_value(
        svc.handle(req_with(
            "222222222222",
            "ListHandshakesForAccount",
            json!({}),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(listed["Handshakes"][0]["Id"], handshake_id.as_str());

    // ...acceptable...
    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .expect("the named account can accept");

    // ...and the transfer it accepted is readable and endable by it.
    let inbound = body_value(
        svc.handle(req_with(
            "222222222222",
            "ListInboundResponsibilityTransfers",
            json!({ "Type": "BILLING" }),
        ))
        .await
        .unwrap(),
    );
    let transfer_id = inbound["ResponsibilityTransfers"][0]["Id"]
        .as_str()
        .expect("the accepted transfer is visible to its target")
        .to_string();
    svc.handle(req_with(
        "222222222222",
        "TerminateResponsibilityTransfer",
        json!({ "Id": transfer_id }),
    ))
    .await
    .expect("and can be ended by it");
}

/// `CreateAccount` hands back the new id immediately and enrolls it a
/// moment later. During that window the id is already spoken for: it
/// must not be able to create an organization of its own, be invited
/// elsewhere, or accept an invitation, or the completion tick would
/// leave it in two organizations at once.
#[tokio::test]
async fn an_account_id_reserved_by_create_account_is_already_claimed() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let created = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "dev@example.com", "AccountName": "dev" }),
        ))
        .await
        .unwrap(),
    );
    let reserved = created["CreateAccountStatus"]["AccountId"]
        .as_str()
        .expect("the id is handed back before enrollment")
        .to_string();
    // Still only the management account is enrolled.
    assert!(state.read().org_of_account(&reserved).is_none());

    // ...but it cannot start an organization of its own.
    let err = expect_err(
        svc.handle(req_with(&reserved, "CreateOrganization", json!({})))
            .await,
    );
    assert_eq!(err.code(), "AlreadyInOrganizationException");

    // ...nor be invited into another one.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": reserved } }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");
}

/// `DescribeEffectivePolicy` must not answer for a target in another
/// organization. Walking a hierarchy that does not contain the target
/// found no ancestors and returned an empty, successful "no effective
/// policy" -- the worst answer for a caller auditing one.
#[tokio::test]
async fn describe_effective_policy_rejects_a_target_in_another_organization() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let other_root = state
        .read()
        .org_of_account("222222222222")
        .unwrap()
        .root_id
        .clone();

    for target in [other_root.as_str(), "222222222222"] {
        let err = expect_err(
            svc.handle(req_with(
                "111111111111",
                "DescribeEffectivePolicy",
                json!({ "PolicyType": "SERVICE_CONTROL_POLICY", "TargetId": target }),
            ))
            .await,
        );
        assert_eq!(err.code(), "TargetNotFoundException");
    }

    // The caller's own account still resolves.
    svc.handle(req_with(
        "111111111111",
        "DescribeEffectivePolicy",
        json!({ "PolicyType": "SERVICE_CONTROL_POLICY" }),
    ))
    .await
    .expect("the caller's own account is a valid target");
}

/// An address names exactly one account. `CreateAccount` stores the
/// caller's own email, so an account can be registered with an address
/// that *looks* like the synthetic form of a different id -- and then
/// both resolutions were accepted, letting the account the address only
/// spells read and accept an invitation meant for its real owner.
#[tokio::test]
async fn a_registered_address_names_its_own_account_and_no_other() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    // A member registered with an address that spells another id.
    let created = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "222222222222@example.com", "AccountName": "decoy" }),
        ))
        .await
        .unwrap(),
    );
    let owner = created["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(owner, "222222222222");
    state
        .write()
        .org_of_account_mut("111111111111")
        .unwrap()
        .complete_create_account(created["CreateAccountStatus"]["Id"].as_str().unwrap());

    // Inviting that address now names a member already enrolled.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "EMAIL", "Id": "222222222222@example.com" } }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");

    // And the account the address merely spells is not its owner.
    assert!(state
        .read()
        .account_matches_target("EMAIL", "222222222222@example.com", &owner));
    assert!(!state.read().account_matches_target(
        "EMAIL",
        "222222222222@example.com",
        "222222222222"
    ));
}

/// AWS requires an account's address to be unused, and reports a
/// duplicate ASYNCHRONOUSLY: `CreateAccount` models no synchronous error
/// for it, so a polling client (Terraform's `aws_organizations_account`)
/// must still get a request id back. The request then lands in FAILED
/// with `EMAIL_ALREADY_EXISTS`.
#[tokio::test]
async fn create_account_fails_asynchronously_on_a_duplicate_address() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let first = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "ops@corp.com", "AccountName": "ops" }),
        ))
        .await
        .unwrap(),
    );
    state
        .write()
        .sole_mut()
        .unwrap()
        .complete_create_account(first["CreateAccountStatus"]["Id"].as_str().unwrap());

    // The duplicate is accepted, with a request id to poll.
    let second = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "ops@corp.com", "AccountName": "dupe" }),
        ))
        .await
        .expect("a duplicate address is not a synchronous error"),
    );
    assert_eq!(second["CreateAccountStatus"]["State"], "IN_PROGRESS");
    let request_id = second["CreateAccountStatus"]["Id"].as_str().unwrap();

    // ...and resolves to FAILED rather than a second account on one
    // address, which would make resolution depend on id ordering.
    let described = body_value(
        svc.handle(req_with(
            "111111111111",
            "DescribeCreateAccountStatus",
            json!({ "CreateAccountRequestId": request_id }),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(described["CreateAccountStatus"]["State"], "IN_PROGRESS");

    // Drive the tick the server would run in the background.
    let failed = state
        .write()
        .sole_mut()
        .unwrap()
        .fail_create_account(request_id, "EMAIL_ALREADY_EXISTS")
        .unwrap();
    assert_eq!(failed.state, "FAILED");
    assert_eq!(
        failed.failure_reason.as_deref(),
        Some("EMAIL_ALREADY_EXISTS")
    );
}

/// Accepting an invitation you have since satisfied another way is not a
/// silent no-op. `invite_account` rejects an existing member at invite
/// time; the account may have joined between invite and accept, and the
/// two gates must give the same answer.
#[tokio::test]
async fn accepting_after_joining_the_inviting_organization_errors() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    // It joins the same organization by another route while the
    // invitation sits open.
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");

    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "AcceptHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "HandshakeConstraintViolationException");
}

/// Closing an account releases its address everywhere. `CloseAccount`
/// only suspends, so a resolver that still matched the closed record
/// would let the address be re-used by `CreateAccount` while making it
/// permanently un-invitable.
#[tokio::test]
async fn a_closed_account_releases_its_address() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let created = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "alice@corp.com", "AccountName": "alice" }),
        ))
        .await
        .unwrap(),
    );
    let account_id = {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        org.complete_create_account(created["CreateAccountStatus"]["Id"].as_str().unwrap());
        let id = created["CreateAccountStatus"]["AccountId"]
            .as_str()
            .unwrap()
            .to_string();
        org.close_account(&id).unwrap();
        id
    };

    // The address is free again...
    assert!(!state.read().email_in_use("alice@corp.com"));
    // ...and no longer resolves to the closed account, so inviting it is
    // not refused as "already a member".
    assert!(!state
        .read()
        .account_matches_target("EMAIL", "alice@corp.com", &account_id));
    svc.handle(req_with(
        "111111111111",
        "InviteAccountToOrganization",
        json!({ "Target": { "Type": "EMAIL", "Id": "alice@corp.com" } }),
    ))
    .await
    .expect("a closed account's address can be invited again");
}

/// A `<account-id>@example.com` address belongs to the id it spells.
/// fakecloud mints those for the accounts it creates, so letting an
/// unrelated account register one put two live accounts on one address
/// -- and resolution by address decides who may accept an
/// EMAIL-targeted handshake.
#[tokio::test]
async fn a_synthetic_address_is_reserved_for_the_account_it_spells() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;

    // `CreateAccount` cannot take another id's synthetic address.
    let created = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({ "Email": "222222222222@example.com", "AccountName": "decoy" }),
        ))
        .await
        .expect("accepted, then failed asynchronously"),
    );
    let request_id = created["CreateAccountStatus"]["Id"].as_str().unwrap();
    let minted = created["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        crate::state::OrganizationsRegistry::email_reserved_for_other(
            "222222222222@example.com",
            &minted
        ),
        "the address spells an id other than the one being created"
    );
    state
        .write()
        .sole_mut()
        .unwrap()
        .fail_create_account(request_id, "EMAIL_ALREADY_EXISTS");

    // ...and the account it spells keeps it when it creates its own
    // organization.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .expect("its own synthetic address is free");
    assert!(state.read().account_matches_target(
        "EMAIL",
        "222222222222@example.com",
        "222222222222"
    ));
}

/// AWS lets the management account OR a delegated administrator read
/// the resource policy, unlike `Put`/`Delete`, which are
/// management-only. A plain member still cannot.
#[tokio::test]
async fn describe_resource_policy_allows_a_delegated_administrator() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");
    svc.handle(req_with(
        "111111111111",
        "PutResourcePolicy",
        json!({ "Content": "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }),
    ))
    .await
    .unwrap();

    // A plain member cannot read it.
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "DescribeResourcePolicy",
            json!({}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");

    // Registered as a delegated administrator, it can.
    svc.handle(req_with(
        "111111111111",
        "EnableAWSServiceAccess",
        json!({ "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();
    svc.handle(req_with(
        "111111111111",
        "RegisterDelegatedAdministrator",
        json!({ "AccountId": "222222222222", "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();
    svc.handle(req_with(
        "222222222222",
        "DescribeResourcePolicy",
        json!({}),
    ))
    .await
    .expect("a delegated administrator may read the resource policy");
}

/// Re-accepting an already-accepted handshake is a terminal-transition
/// error, not a membership one. The membership gates are necessarily
/// satisfied once the accept succeeded, so checking them first made a
/// client retrying after a timeout read a join that had worked as a
/// hard constraint failure -- and disagreed with Decline/Cancel, which
/// answered correctly.
#[tokio::test]
async fn re_accepting_a_handshake_reports_the_transition_error() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();
    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .unwrap();

    for action in ["AcceptHandshake", "DeclineHandshake"] {
        let err = expect_err(
            svc.handle(req_with(
                "222222222222",
                action,
                json!({ "HandshakeId": handshake_id }),
            ))
            .await,
        );
        assert_eq!(
            err.code(),
            "InvalidHandshakeTransitionException",
            "{action} on a terminal handshake"
        );
    }
}

/// A `CreateAccount` that ends FAILED leaves no tags behind on the id it
/// reserved: create-time tags are applied straight away, but that id
/// never becomes an account, and AWS answers `TargetNotFoundException`
/// for an id that is not a real resource.
#[tokio::test]
async fn a_failed_create_account_drops_the_tags_it_reserved() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let created = body_value(
        svc.handle(req_with(
            "111111111111",
            "CreateAccount",
            json!({
                "Email": "222222222222@example.com",
                "AccountName": "doomed",
                "Tags": [{ "Key": "env", "Value": "prod" }],
            }),
        ))
        .await
        .unwrap(),
    );
    let account_id = created["CreateAccountStatus"]["AccountId"]
        .as_str()
        .unwrap()
        .to_string();
    // Tagged synchronously, against the reserved id.
    assert!(!state
        .read()
        .sole()
        .unwrap()
        .list_resource_tags(&account_id)
        .is_empty());

    state.write().sole_mut().unwrap().fail_create_account(
        created["CreateAccountStatus"]["Id"].as_str().unwrap(),
        "EMAIL_ALREADY_EXISTS",
    );

    assert!(
        state
            .read()
            .sole()
            .unwrap()
            .list_resource_tags(&account_id)
            .is_empty(),
        "a failed request's reserved id keeps no tags"
    );
}

/// A non-party learns nothing from a handshake id it guessed -- not
/// even whether it exists or what state it is in. The terminal-state
/// answer is for the handshake's own parties.
#[tokio::test]
async fn a_bystander_cannot_read_handshake_state_off_an_error() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();
    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .unwrap();

    // Resolved, but a bystander is told only "not a party".
    let err = expect_err(
        svc.handle(req_with(
            "999999999999",
            "AcceptHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    // `InvalidHandshakeParty` renders as AccessDenied -- the point is
    // that it says nothing about the handshake's state.
    assert_eq!(err.code(), "AccessDeniedException");

    // ...while a party still gets the transition error it needs.
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "AcceptHandshake",
            json!({ "HandshakeId": handshake_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidHandshakeTransitionException");
}

/// `ListHandshakesForAccount` lists the handshakes associated with the
/// calling account, which includes the invitations it SENT -- not only
/// those addressed to it.
#[tokio::test]
async fn list_handshakes_for_account_includes_the_ones_it_sent() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invited = body_json(
        &svc.handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": { "Type": "ACCOUNT", "Id": "222222222222" } }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invited["Handshake"]["Id"].as_str().unwrap().to_string();

    for caller in ["111111111111", "222222222222"] {
        let listed = body_value(
            svc.handle(req_with(caller, "ListHandshakesForAccount", json!({})))
                .await
                .unwrap(),
        );
        assert_eq!(
            listed["Handshakes"][0]["Id"],
            handshake_id.as_str(),
            "{caller} should see the handshake it is a party to"
        );
    }

    // A bystander sees none of it.
    let listed = body_value(
        svc.handle(req_with(
            "999999999999",
            "ListHandshakesForAccount",
            json!({}),
        ))
        .await
        .unwrap(),
    );
    assert!(listed["Handshakes"].as_array().unwrap().is_empty());
}

/// Deleting one organization leaves every other one standing.
#[tokio::test]
async fn deleting_one_organization_leaves_the_others() {
    let (svc, state) = OrganizationsService::shared();
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let second = body_json(
        &svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
            .await
            .unwrap(),
    );
    let second_id = second["Organization"]["Id"].as_str().unwrap().to_string();

    svc.handle(req_with("111111111111", "DeleteOrganization", json!({})))
        .await
        .unwrap();

    let guard = state.read();
    assert_eq!(guard.len(), 1);
    assert!(guard.org_by_id(&second_id).is_some());
    assert!(guard.org_of_account("111111111111").is_none());
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
        let org = guard.sole_mut().unwrap();
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
    assert!(state.read().is_empty());
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
            .sole_mut()
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
        let org = guard.sole_mut().unwrap();
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
            .sole_mut()
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
        let org = guard.sole_mut().unwrap();
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
            .sole_mut()
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
    let org = guard.sole().unwrap();
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
            .sole_mut()
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
            .sole_mut()
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
    // `LeaveOrganization` takes no AccountId, so AWS's answer for a caller
    // that belongs to no organization is `AWSOrganizationsNotInUseException`,
    // not `AccountNotFoundException`. It is also the non-leaking answer: with
    // several organizations in the process, a non-member must not be able to
    // tell whether any exist.
    assert_eq!(err.code(), "AWSOrganizationsNotInUseException");
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
    // The op invites an ORGANIZATION, so the target must be another
    // organization's management account.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
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

/// The inbound side of a transfer: the target management account runs
/// its own organization, and must be able to see, accept, and correctly
/// read the direction of the transfer offered to it.
#[tokio::test]
async fn the_target_organization_sees_its_inbound_responsibility_transfer() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();

    let invite = body_value(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap(),
    );
    let handshake_id = invite["Handshake"]["Id"].as_str().unwrap().to_string();

    // The target lists it INBOUND -- the stored row is the source's, and
    // reads OUTBOUND there.
    let inbound = body_value(
        svc.handle(req_with(
            "222222222222",
            "ListInboundResponsibilityTransfers",
            json!({ "Type": "BILLING" }),
        ))
        .await
        .unwrap(),
    );
    let rows = inbound["ResponsibilityTransfers"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "target must see the transfer offered to it");
    let transfer_id = rows[0]["Id"].as_str().unwrap().to_string();

    // The same transfer is the source's OUTBOUND one. AWS's shape carries
    // no Direction member -- which list you call IS the direction.
    let outbound = body_value(
        svc.handle(req_with(
            "111111111111",
            "ListOutboundResponsibilityTransfers",
            json!({ "Type": "BILLING" }),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        outbound["ResponsibilityTransfers"][0]["Id"],
        transfer_id.as_str()
    );
    // ...and the target can describe it directly.
    let described = body_value(
        svc.handle(req_with(
            "222222222222",
            "DescribeResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        described["ResponsibilityTransfer"]["Id"],
        transfer_id.as_str()
    );

    // Accepting the riding handshake moves the transfer with it, rather
    // than leaving an ACCEPTED handshake beside a REQUESTED transfer.
    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": handshake_id }),
    ))
    .await
    .unwrap();
    let after = body_value(
        svc.handle(req_with(
            "111111111111",
            "DescribeResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await
        .unwrap(),
    );
    assert_eq!(after["ResponsibilityTransfer"]["Status"], "ACCEPTED");
    assert!(after["ResponsibilityTransfer"]["ActiveHandshakeId"].is_null());

    // Accepting a transfer must NOT enroll the other organization's
    // management account as a member.
    let listed = body_value(
        svc.handle(req_with("111111111111", "ListAccounts", json!({})))
            .await
            .unwrap(),
    );
    let ids: Vec<&str> = listed["Accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["Id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["111111111111"]);
}

/// Both parties can read a transfer and either can end the arrangement,
/// but only the source -- the one that named it -- can rename it. A
/// stranger gets "not found"; the target gets a clear AccessDenied on
/// the source-only operation rather than a not-found it cannot
/// distinguish from a bad id.
#[tokio::test]
async fn only_the_source_can_rename_a_responsibility_transfer() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let invite = body_value(
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap(),
    );
    let _ = invite;
    let listed = body_value(
        svc.handle(req_with(
            "111111111111",
            "ListOutboundResponsibilityTransfers",
            json!({ "Type": "BILLING" }),
        ))
        .await
        .unwrap(),
    );
    let transfer_id = listed["ResponsibilityTransfers"][0]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    // The target is a party, so not "not found" -- but renaming is the
    // source's alone.
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "UpdateResponsibilityTransfer",
            json!({ "Id": transfer_id, "Name": "renamed" }),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");

    // A stranger learns nothing beyond "no such transfer".
    let err = expect_err(
        svc.handle(req_with(
            "999999999999",
            "TerminateResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "ResponsibilityTransferNotFoundException");

    // While the offer is still open, withdrawing it is the source's --
    // the target's answer to an open offer is DeclineHandshake.
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "TerminateResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");

    svc.handle(req_with(
        "111111111111",
        "TerminateResponsibilityTransfer",
        json!({ "Id": transfer_id }),
    ))
    .await
    .expect("the source withdraws its own open offer");
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

#[tokio::test]
async fn a_mutation_that_changes_no_membership_notifies_nobody() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let state: SharedOrganizationsState =
        Arc::new(parking_lot::RwLock::new(OrganizationsRegistry::default()));
    let hooks = OrgChangeHooks::new();
    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        hooks.register(Arc::new(move || {
            let fired = fired.clone();
            Box::pin(async move {
                fired.fetch_add(1, Ordering::SeqCst);
            })
        }));
    }
    // No organization at all: nothing to tell anyone about.
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);

    state
        .write()
        .insert(OrganizationState::bootstrap("000000000000"));
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 1);

    // A tag is not a membership change.
    state
        .write()
        .sole_mut()
        .unwrap()
        .set_resource_tags("000000000000", &[("Env".to_string(), "dev".to_string())]);
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 1);

    // An account joining is.
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("111111111111");
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 2);

    // So is moving it, and so is an OU appearing for it to move into.
    let (root, ou) = {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "workloads").unwrap().id;
        (root, ou)
    };
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 3);
    state
        .write()
        .sole_mut()
        .unwrap()
        .move_account("111111111111", &root, &ou)
        .unwrap();
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn a_membership_change_is_announced_even_after_a_reversal_races_it() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let state: SharedOrganizationsState = Arc::new(parking_lot::RwLock::new(
        OrganizationState::bootstrap("000000000000").into(),
    ));
    let hooks = OrgChangeHooks::new();
    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        hooks.register(Arc::new(move || {
            let fired = fired.clone();
            Box::pin(async move {
                fired.fetch_add(1, Ordering::SeqCst);
            })
        }));
    }
    let (root, ou) = {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        org.enroll_account_if_missing("111111111111");
        let root = org.root_id.clone();
        let ou = org.create_ou(&root, "workloads").unwrap().id;
        (root, ou)
    };
    hooks.fire_if_membership_changed(&state).await;
    let base = fired.load(Ordering::SeqCst);

    // Move out and straight back: the organization ends where it started,
    // so what was recorded must describe that state, not the one in
    // between — otherwise the next real move looks like no change.
    {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        org.move_account("111111111111", &root, &ou).unwrap();
    }
    hooks.fire_if_membership_changed(&state).await;
    {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        org.move_account("111111111111", &ou, &root).unwrap();
    }
    hooks.fire_if_membership_changed(&state).await;
    let after_round_trip = fired.load(Ordering::SeqCst);
    assert_eq!(after_round_trip, base + 2);

    // The same move again is a real change and has to be announced.
    {
        let mut guard = state.write();
        let org = guard.sole_mut().unwrap();
        org.move_account("111111111111", &root, &ou).unwrap();
    }
    hooks.fire_if_membership_changed(&state).await;
    assert_eq!(fired.load(Ordering::SeqCst), after_round_trip + 1);
}

/// A delegated administrator runs a service's organization-wide
/// integration on the management account's behalf, which AWS documents
/// as including the organization's read operations. Gating them on the
/// management account alone left every delegated administrator unable
/// to read the organization it administers.
#[tokio::test]
async fn a_delegated_administrator_can_read_the_organization() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");

    let reads = [
        ("ListHandshakesForOrganization", json!({})),
        ("ListAWSServiceAccessForOrganization", json!({})),
        ("ListDelegatedAdministrators", json!({})),
        (
            "ListDelegatedServicesForAccount",
            json!({ "AccountId": "222222222222" }),
        ),
    ];

    // A plain member is refused.
    for (action, body) in &reads {
        let err = expect_err(
            svc.handle(req_with("222222222222", action, body.clone()))
                .await,
        );
        assert_eq!(err.code(), "AccessDeniedException", "{action}");
    }

    svc.handle(req_with(
        "111111111111",
        "EnableAWSServiceAccess",
        json!({ "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();
    svc.handle(req_with(
        "111111111111",
        "RegisterDelegatedAdministrator",
        json!({ "AccountId": "222222222222", "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();

    // Registered, it can run all four.
    for (action, body) in &reads {
        svc.handle(req_with("222222222222", action, body.clone()))
            .await
            .unwrap_or_else(|e| panic!("{action} refused a delegated administrator: {e:?}"));
    }
}

/// One live responsibility-transfer offer per target, the same rule
/// `InviteAccountToOrganization` enforces. Stacking OPEN transfers on
/// one target meant accepting any of them moved billing while the rest
/// stayed OPEN against an organization that no longer owned it.
#[tokio::test]
async fn a_second_open_transfer_to_the_same_target_errors() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let invite = |target: Value| {
        svc.handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": target,
            }),
        ))
    };

    invite(json!({"Id": "222222222222", "Type": "ACCOUNT"}))
        .await
        .unwrap();
    let err = expect_err(invite(json!({"Id": "222222222222", "Type": "ACCOUNT"})).await);
    assert_eq!(err.code(), "DuplicateHandshakeException");
    // The synthetic address names the same account, so it collides too.
    let err = expect_err(invite(json!({"Id": "222222222222@example.com", "Type": "EMAIL"})).await);
    assert_eq!(err.code(), "DuplicateHandshakeException");
    // A different target is unaffected.
    invite(json!({"Id": "333333333333", "Type": "ACCOUNT"}))
        .await
        .unwrap();
}

/// `HandshakeResourceType` models RESPONSIBILITY_TRANSFER, TRANSFER_TYPE,
/// TRANSFER_START_TIMESTAMP and MANAGEMENT_ACCOUNT for one purpose: an
/// SDK reading only the handshake has to learn what is being handed over
/// and by whom. Rendering just ORGANIZATION and the target left the
/// invited account unable to tell a billing transfer from a plain invite
/// without a second call.
#[tokio::test]
async fn a_transfer_handshake_carries_the_transfer_as_a_resource() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap();
    let handshake = body_json(&resp)["Handshake"].clone();
    let transfer = handshake["Resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["Type"] == "RESPONSIBILITY_TRANSFER")
        .expect("the handshake reports the transfer it carries")
        .clone();
    assert!(transfer["Value"].as_str().unwrap().starts_with("rt-"));
    let nested: HashMap<&str, &str> = transfer["Resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["Type"].as_str().unwrap(), r["Value"].as_str().unwrap()))
        .collect();
    assert_eq!(nested.get("TRANSFER_TYPE"), Some(&"BILLING"));
    assert_eq!(nested.get("MANAGEMENT_ACCOUNT"), Some(&"111111111111"));
    assert_eq!(
        nested.get("TRANSFER_START_TIMESTAMP"),
        Some(&"2030-01-01T00:00:00.000Z")
    );

    // The link survives resolution: the transfer clears its
    // `ActiveHandshakeId` on accept, so reading it from that side would
    // have made an ACCEPTED handshake report no transfer at all.
    let id = handshake["Id"].as_str().unwrap().to_string();
    svc.handle(req_with(
        "222222222222",
        "AcceptHandshake",
        json!({ "HandshakeId": id }),
    ))
    .await
    .unwrap();
    let described = svc
        .handle(req_with(
            "222222222222",
            "DescribeHandshake",
            json!({ "HandshakeId": id }),
        ))
        .await
        .unwrap();
    assert!(body_json(&described)["Handshake"]["Resources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["Type"] == "RESPONSIBILITY_TRANSFER"));
}

/// An in-flight `CreateAccount` whose address is the synthetic form of
/// an id OTHER than the one it reserved is already doomed -- the
/// completion tick fails it with `EMAIL_ALREADY_EXISTS`. Letting it hold
/// the address meanwhile let any caller park another account's address
/// for the length of the creation delay, blocking that account's own
/// `CreateOrganization`.
#[tokio::test]
async fn a_doomed_reservation_does_not_hold_another_accounts_address() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    svc.handle(req_with(
        "111111111111",
        "CreateAccount",
        json!({ "Email": "222222222222@example.com", "AccountName": "squatter" }),
    ))
    .await
    .unwrap();
    // The account that address actually names can still bootstrap while
    // the doomed request is in flight.
    svc.handle(req_with("222222222222", "CreateOrganization", json!({})))
        .await
        .expect("a doomed reservation must not hold the address it cannot keep");
}

/// A delegated-administrator registration is an organization's grant to
/// one of its own members, so it cannot outlive the membership. Leaving
/// it behind meant `ListDelegatedServicesForAccount` still answered for
/// an account the organization no longer contains, and an account that
/// left and was later re-invited came back holding authority nobody had
/// granted it.
///
/// (`ListDelegatedAdministrators` never showed the symptom: its handler
/// drops any registration whose account is not in `accounts`.)
#[tokio::test]
async fn leaving_the_organization_drops_the_delegated_administrator_grant() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");
    svc.handle(req_with(
        "111111111111",
        "EnableAWSServiceAccess",
        json!({ "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();
    svc.handle(req_with(
        "111111111111",
        "RegisterDelegatedAdministrator",
        json!({ "AccountId": "222222222222", "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();

    svc.handle(req_with("222222222222", "LeaveOrganization", json!({})))
        .await
        .unwrap();

    // The organization no longer answers for its delegated services.
    let listed = svc
        .handle(req_with(
            "111111111111",
            "ListDelegatedServicesForAccount",
            json!({ "AccountId": "222222222222" }),
        ))
        .await
        .unwrap();
    assert_eq!(
        body_json(&listed)["DelegatedServices"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // And rejoining does not restore the grant.
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "ListDelegatedAdministrators",
            json!({}),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccessDeniedException");
}

/// A `TRANSFER_RESPONSIBILITY` handshake written before the handshake
/// carried its transfer id deserializes without one, and nothing
/// backfills it. The transfer's own `ActiveHandshakeId` still points
/// back for every handshake this matters for -- it is cleared only on
/// resolution -- so the payload falls back to it rather than silently
/// dropping the resource for a restored organization.
#[tokio::test]
async fn a_restored_handshake_still_reports_its_transfer() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap();
    let id = body_json(&resp)["Handshake"]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    // Simulate the pre-upgrade snapshot: the link the handshake now
    // stores did not exist when it was written.
    state
        .write()
        .sole_mut()
        .unwrap()
        .handshakes
        .get_mut(&id)
        .unwrap()
        .responsibility_transfer_id = None;

    let described = svc
        .handle(req_with(
            "222222222222",
            "DescribeHandshake",
            json!({ "HandshakeId": id }),
        ))
        .await
        .unwrap();
    assert!(body_json(&described)["Handshake"]["Resources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["Type"] == "RESPONSIBILITY_TRANSFER"));
}

/// Tags are keyed by account id, so an account removed with tags still
/// attached came back wearing them if it was ever re-enrolled -- and
/// `ListTagsForResource` answered for the id meanwhile, though
/// `ListAccounts` no longer knew it.
#[tokio::test]
async fn removing_an_account_drops_the_tags_it_carried() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");
    svc.handle(req_with(
        "111111111111",
        "TagResource",
        json!({ "ResourceId": "222222222222", "Tags": [{"Key": "env", "Value": "prod"}] }),
    ))
    .await
    .unwrap();

    svc.handle(req_with(
        "111111111111",
        "RemoveAccountFromOrganization",
        json!({ "AccountId": "222222222222" }),
    ))
    .await
    .unwrap();

    let listed = svc
        .handle(req_with(
            "111111111111",
            "ListTagsForResource",
            json!({ "ResourceId": "222222222222" }),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(&listed)["Tags"].as_array().unwrap().len(), 0);
}

/// `AccountNotRegisteredException` names the ACCOUNT, in both the arm
/// where the service has other delegates and the arm where it has none.
/// Passing the service principal into the second rendered "Account
/// config.amazonaws.com is not registered as a delegated administrator."
#[tokio::test]
async fn deregistering_an_unregistered_administrator_names_the_account() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    state
        .write()
        .sole_mut()
        .unwrap()
        .enroll_account_if_missing("222222222222");
    svc.handle(req_with(
        "111111111111",
        "EnableAWSServiceAccess",
        json!({ "ServicePrincipal": "config.amazonaws.com" }),
    ))
    .await
    .unwrap();

    // No account is registered for the principal at all.
    let err = expect_err(
        svc.handle(req_with(
            "111111111111",
            "DeregisterDelegatedAdministrator",
            json!({ "AccountId": "222222222222", "ServicePrincipal": "config.amazonaws.com" }),
        ))
        .await,
    );
    assert_eq!(err.code(), "AccountNotRegisteredException");
    assert!(
        err.to_string().contains("222222222222"),
        "the error must name the account, got: {err}"
    );
    assert!(
        !err.to_string().contains("config.amazonaws.com"),
        "the error must not name the service principal, got: {err}"
    );
}

/// AWS gives a handshake 15 days and expires it on its own. Nothing
/// here ever did, so an overdue invitation stayed OPEN and acceptable
/// forever and the `ExpirationTimestamp` fakecloud reported was
/// decoration.
#[tokio::test]
async fn an_overdue_handshake_expires_and_cannot_be_accepted() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": {"Id": "222222222222", "Type": "ACCOUNT"} }),
        ))
        .await
        .unwrap();
    let id = body_json(&resp)["Handshake"]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    // Backdate it past its deadline, the way 15 days of wall clock would.
    state
        .write()
        .sole_mut()
        .unwrap()
        .handshakes
        .get_mut(&id)
        .unwrap()
        .expiration_timestamp = Utc::now() - chrono::Duration::days(1);

    let described = svc
        .handle(req_with(
            "222222222222",
            "DescribeHandshake",
            json!({ "HandshakeId": id }),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(&described)["Handshake"]["State"], "EXPIRED");

    // And the offer is gone: accepting it is a terminal-transition error,
    // not a join.
    let err = expect_err(
        svc.handle(req_with(
            "222222222222",
            "AcceptHandshake",
            json!({ "HandshakeId": id }),
        ))
        .await,
    );
    assert_eq!(err.code(), "InvalidHandshakeTransitionException");
    assert!(
        !state
            .read()
            .sole()
            .unwrap()
            .accounts
            .contains_key("222222222222"),
        "an expired invitation must not enroll its target"
    );
}

/// A responsibility transfer rides a handshake, so it ends when that
/// handshake lapses -- leaving it REQUESTED with a live
/// `ActiveHandshakeId` pointing at an EXPIRED handshake made the two
/// records disagree, the same way an unhandled accept once did.
#[tokio::test]
async fn an_expired_handshake_ends_the_transfer_riding_it() {
    let (svc, state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteOrganizationToTransferResponsibility",
            json!({
                "Type": "BILLING",
                "SourceName": "handover",
                "StartTimestamp": 1893456000.0,
                "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
            }),
        ))
        .await
        .unwrap();
    let handshake_id = body_json(&resp)["Handshake"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let transfer_id = body_json(&resp)["Handshake"]["Resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["Type"] == "RESPONSIBILITY_TRANSFER")
        .unwrap()["Value"]
        .as_str()
        .unwrap()
        .to_string();

    let deadline = Utc::now() - chrono::Duration::days(5);
    state
        .write()
        .sole_mut()
        .unwrap()
        .handshakes
        .get_mut(&handshake_id)
        .unwrap()
        .expiration_timestamp = deadline;

    let described = svc
        .handle(req_with(
            "111111111111",
            "DescribeResponsibilityTransfer",
            json!({ "Id": transfer_id }),
        ))
        .await
        .unwrap();
    let transfer = body_json(&described)["ResponsibilityTransfer"].clone();
    assert_eq!(transfer["Status"], "EXPIRED");
    assert!(
        transfer.get("ActiveHandshakeId").is_none(),
        "an expired transfer holds no live handshake, got: {transfer}"
    );
    // The transfer ended when its handshake lapsed, not when the sweep
    // happened to notice -- an idle process would otherwise report an
    // EndTimestamp days after the ExpirationTimestamp on the same record.
    assert_eq!(
        transfer["EndTimestamp"].as_f64().unwrap() as i64,
        deadline.timestamp()
    );

    // The introspection route reads the same object and must not
    // contradict the API: it answered REQUESTED with a live
    // activeHandshakeId until it swept too.
    let rows = crate::introspection::list_all_responsibility_transfers(&state);
    let row = rows.iter().find(|r| r.id == transfer_id).unwrap();
    assert_eq!(row.status, "EXPIRED");
    assert!(row.active_handshake_id.is_none());

    // The target is free again: the expired offer no longer collides.
    svc.handle(req_with(
        "111111111111",
        "InviteOrganizationToTransferResponsibility",
        json!({
            "Type": "BILLING",
            "SourceName": "handover",
            "StartTimestamp": 1893456000.0,
            "Target": {"Id": "222222222222", "Type": "ACCOUNT"},
        }),
    ))
    .await
    .expect("an expired offer must not block a fresh one");
}

/// A handshake still inside its 15 days is untouched by the sweep.
///
/// This guards against OVER-expiry only -- it passes without the sweep
/// too. Its job is to fail if the deadline comparison is ever inverted
/// or the `OPEN | REQUESTED` filter dropped.
#[tokio::test]
async fn a_live_handshake_is_left_alone() {
    let (svc, _state) = OrganizationsService::shared();
    create_org_with_root(&svc).await;
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": {"Id": "222222222222", "Type": "ACCOUNT"} }),
        ))
        .await
        .unwrap();
    let id = body_json(&resp)["Handshake"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let described = svc
        .handle(req_with(
            "222222222222",
            "DescribeHandshake",
            json!({ "HandshakeId": id }),
        ))
        .await
        .unwrap();
    assert_eq!(body_json(&described)["Handshake"]["State"], "OPEN");
}

/// Recording store: keeps the last bytes written so a test can assert
/// what was actually persisted.
#[derive(Default)]
struct RecordingStore(parking_lot::Mutex<Option<Vec<u8>>>);

impl fakecloud_persistence::SnapshotStore for RecordingStore {
    fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.0.lock().clone())
    }

    fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
        *self.0.lock() = Some(bytes.to_vec());
        Ok(())
    }
}

/// A sweep is a mutation even when it happens on the way into a READ,
/// so it has to be persisted there too. Without that, an expiry noticed
/// by a `DescribeHandshake` is lost on restart and the handshake comes
/// back OPEN -- past its deadline, and acceptable again.
#[tokio::test]
async fn an_expiry_noticed_by_a_read_is_persisted() {
    let state: SharedOrganizationsState =
        Arc::new(parking_lot::RwLock::new(OrganizationsRegistry::default()));
    let store = Arc::new(RecordingStore::default());
    let svc = Arc::new(OrganizationsService::new(state.clone()).with_snapshot_store(store.clone()));
    svc.handle(req_with("111111111111", "CreateOrganization", json!({})))
        .await
        .unwrap();
    let resp = svc
        .handle(req_with(
            "111111111111",
            "InviteAccountToOrganization",
            json!({ "Target": {"Id": "222222222222", "Type": "ACCOUNT"} }),
        ))
        .await
        .unwrap();
    let id = body_json(&resp)["Handshake"]["Id"]
        .as_str()
        .unwrap()
        .to_string();

    state
        .write()
        .sole_mut()
        .unwrap()
        .handshakes
        .get_mut(&id)
        .unwrap()
        .expiration_timestamp = Utc::now() - chrono::Duration::days(1);

    // A pure read triggers the sweep.
    svc.handle(req_with(
        "222222222222",
        "DescribeHandshake",
        json!({ "HandshakeId": id }),
    ))
    .await
    .unwrap();

    // What landed on disk must carry the expiry, not the OPEN state the
    // last mutating call wrote.
    let bytes = store
        .0
        .lock()
        .clone()
        .expect("the read persisted a snapshot");
    let snapshot: OrganizationsSnapshot = serde_json::from_slice(&bytes).unwrap();
    let restored = snapshot.into_registry();
    assert_eq!(
        restored.sole().unwrap().handshakes.get(&id).unwrap().state,
        "EXPIRED"
    );
}
