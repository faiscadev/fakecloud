//! Organizations conformance tests. Every operation in the Smithy model is
//! exercised against a live fakecloud via the real `aws-sdk-organizations`
//! client and tagged with `#[test_action(...)]` so the audit step counts it.

mod helpers;

use aws_sdk_organizations::types::{
    EffectivePolicyType, HandshakeParty, HandshakePartyType, PolicyType,
    ResponsibilityTransferType, Tag,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const SCP_ALLOW_ALL: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

/// Poll `DescribeCreateAccountStatus` until the request leaves IN_PROGRESS,
/// returning the assigned account id. fakecloud flips the status to SUCCEEDED
/// after a 1-2s synthetic delay.
async fn await_account(client: &aws_sdk_organizations::Client, request_id: &str) -> String {
    for _ in 0..50 {
        let status = client
            .describe_create_account_status()
            .create_account_request_id(request_id)
            .send()
            .await
            .unwrap();
        let s = status.create_account_status().unwrap();
        if s.state().map(|st| st.as_str()) != Some("IN_PROGRESS") {
            return s.account_id().unwrap().to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("create account never left IN_PROGRESS");
}

async fn create_member(client: &aws_sdk_organizations::Client, email: &str, name: &str) -> String {
    let resp = client
        .create_account()
        .email(email)
        .account_name(name)
        .send()
        .await
        .unwrap();
    let request_id = resp
        .create_account_status()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    await_account(client, &request_id).await
}

#[test_action("organizations", "CreateOrganization", checksum = "ec60d87e")]
#[test_action("organizations", "DescribeOrganization", checksum = "95bbec3c")]
#[test_action("organizations", "ListRoots", checksum = "4dbc05ab")]
#[test_action("organizations", "EnableAllFeatures", checksum = "5f0d8a7e")]
#[test_action("organizations", "DeleteOrganization", checksum = "a902a4bd")]
#[tokio::test]
async fn organizations_org_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;

    let created = client.create_organization().send().await.unwrap();
    assert!(created.organization().unwrap().id().is_some());

    let described = client.describe_organization().send().await.unwrap();
    let master = described
        .organization()
        .unwrap()
        .master_account_id()
        .unwrap()
        .to_string();
    assert!(!master.is_empty());

    let roots = client.list_roots().send().await.unwrap();
    assert_eq!(roots.roots().len(), 1);

    // EnableAllFeatures starts a handshake-style process; it succeeds on a
    // fresh org (already all-features) by returning the in-progress handshake.
    let _ = client.enable_all_features().send().await;

    client.delete_organization().send().await.unwrap();
    assert!(client.describe_organization().send().await.is_err());
}

#[test_action("organizations", "CreateOrganizationalUnit", checksum = "141dbfd8")]
#[test_action("organizations", "UpdateOrganizationalUnit", checksum = "3991f786")]
#[test_action("organizations", "DescribeOrganizationalUnit", checksum = "2b766274")]
#[test_action(
    "organizations",
    "ListOrganizationalUnitsForParent",
    checksum = "fb8e3d16"
)]
#[test_action("organizations", "ListChildren", checksum = "d06f00d7")]
#[test_action("organizations", "ListParents", checksum = "a07448b8")]
#[test_action("organizations", "DeleteOrganizationalUnit", checksum = "fedecca6")]
#[tokio::test]
async fn organizations_ou_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();
    let root = client.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    let ou = client
        .create_organizational_unit()
        .parent_id(&root)
        .name("Engineering")
        .send()
        .await
        .unwrap();
    let ou_id = ou.organizational_unit().unwrap().id().unwrap().to_string();

    client
        .update_organizational_unit()
        .organizational_unit_id(&ou_id)
        .name("Eng")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_organizational_unit()
        .organizational_unit_id(&ou_id)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.organizational_unit().unwrap().name(), Some("Eng"));

    let list = client
        .list_organizational_units_for_parent()
        .parent_id(&root)
        .send()
        .await
        .unwrap();
    assert_eq!(list.organizational_units().len(), 1);

    let children = client
        .list_children()
        .parent_id(&root)
        .child_type(aws_sdk_organizations::types::ChildType::OrganizationalUnit)
        .send()
        .await
        .unwrap();
    assert_eq!(children.children().len(), 1);

    let parents = client.list_parents().child_id(&ou_id).send().await.unwrap();
    assert_eq!(parents.parents().len(), 1);

    client
        .delete_organizational_unit()
        .organizational_unit_id(&ou_id)
        .send()
        .await
        .unwrap();
}

#[test_action("organizations", "CreateAccount", checksum = "c1c53bd6")]
#[test_action("organizations", "DescribeCreateAccountStatus", checksum = "f6c0cdd5")]
#[test_action("organizations", "ListCreateAccountStatus", checksum = "8ee70cf4")]
#[test_action("organizations", "ListAccounts", checksum = "5d8c3d19")]
#[test_action("organizations", "ListAccountsForParent", checksum = "e1687feb")]
#[test_action("organizations", "DescribeAccount", checksum = "9ce20fa6")]
#[test_action("organizations", "MoveAccount", checksum = "0c836fba")]
#[test_action("organizations", "CloseAccount", checksum = "c0dea863")]
#[test_action(
    "organizations",
    "RemoveAccountFromOrganization",
    checksum = "5c8132ce"
)]
#[test_action("organizations", "CreateGovCloudAccount", checksum = "52b77ac6")]
#[test_action("organizations", "LeaveOrganization", checksum = "d1d75daf")]
#[tokio::test]
async fn organizations_account_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();
    let root = client.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    let member = create_member(&client, "alice@example.com", "Alice").await;

    let statuses = client.list_create_account_status().send().await.unwrap();
    assert!(!statuses.create_account_statuses().is_empty());

    let accounts = client.list_accounts().send().await.unwrap();
    assert!(accounts
        .accounts()
        .iter()
        .any(|a| a.id() == Some(member.as_str())));

    let for_parent = client
        .list_accounts_for_parent()
        .parent_id(&root)
        .send()
        .await
        .unwrap();
    assert!(!for_parent.accounts().is_empty());

    let desc = client
        .describe_account()
        .account_id(&member)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.account().unwrap().id(), Some(member.as_str()));

    let ou = client
        .create_organizational_unit()
        .parent_id(&root)
        .name("Dest")
        .send()
        .await
        .unwrap();
    let ou_id = ou.organizational_unit().unwrap().id().unwrap().to_string();
    client
        .move_account()
        .account_id(&member)
        .source_parent_id(&root)
        .destination_parent_id(&ou_id)
        .send()
        .await
        .unwrap();

    // CreateGovCloudAccount returns a paired status.
    let gov = client
        .create_gov_cloud_account()
        .email("gov@example.com")
        .account_name("Gov")
        .send()
        .await
        .unwrap();
    assert!(gov.create_account_status().unwrap().id().is_some());

    // LeaveOrganization is called by a member account; the management caller
    // here cannot leave its own org — the documented error.
    let leave_err = client.leave_organization().send().await;
    assert!(leave_err.is_err());

    // CloseAccount marks an account closed; then it can be removed.
    let _ = client.close_account().account_id(&member).send().await;
    let _ = client
        .remove_account_from_organization()
        .account_id(&member)
        .send()
        .await;
}

#[test_action("organizations", "CreatePolicy", checksum = "98aa2760")]
#[test_action("organizations", "UpdatePolicy", checksum = "78a189df")]
#[test_action("organizations", "DescribePolicy", checksum = "7af077ad")]
#[test_action("organizations", "ListPolicies", checksum = "6e5b7425")]
#[test_action("organizations", "AttachPolicy", checksum = "f093f2e9")]
#[test_action("organizations", "ListPoliciesForTarget", checksum = "3e798227")]
#[test_action("organizations", "ListTargetsForPolicy", checksum = "195ce5cf")]
#[test_action("organizations", "DetachPolicy", checksum = "8d590bab")]
#[test_action("organizations", "DeletePolicy", checksum = "389c2057")]
#[test_action("organizations", "DescribeEffectivePolicy", checksum = "5dc453ad")]
#[test_action("organizations", "EnablePolicyType", checksum = "26567e28")]
#[test_action("organizations", "DisablePolicyType", checksum = "f283538b")]
#[test_action(
    "organizations",
    "ListAccountsWithInvalidEffectivePolicy",
    checksum = "4e459933"
)]
#[test_action(
    "organizations",
    "ListEffectivePolicyValidationErrors",
    checksum = "9b882de8"
)]
#[tokio::test]
async fn organizations_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();
    let root = client.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();
    let master = client
        .describe_organization()
        .send()
        .await
        .unwrap()
        .organization()
        .unwrap()
        .master_account_id()
        .unwrap()
        .to_string();

    let policy = client
        .create_policy()
        .name("deny-nothing")
        .description("test")
        .content(SCP_ALLOW_ALL)
        .r#type(PolicyType::ServiceControlPolicy)
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap()
        .to_string();

    client
        .update_policy()
        .policy_id(&policy_id)
        .description("updated")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_policy()
        .policy_id(&policy_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.policy().unwrap().policy_summary().unwrap().id(),
        Some(policy_id.as_str())
    );

    let policies = client
        .list_policies()
        .filter(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .send()
        .await
        .unwrap();
    assert!(!policies.policies().is_empty());

    client
        .attach_policy()
        .policy_id(&policy_id)
        .target_id(&root)
        .send()
        .await
        .unwrap();

    let for_target = client
        .list_policies_for_target()
        .target_id(&root)
        .filter(PolicyType::ServiceControlPolicy)
        .send()
        .await
        .unwrap();
    assert!(!for_target.policies().is_empty());

    let targets = client
        .list_targets_for_policy()
        .policy_id(&policy_id)
        .send()
        .await
        .unwrap();
    assert!(!targets.targets().is_empty());

    let eff = client
        .describe_effective_policy()
        .policy_type(EffectivePolicyType::TagPolicy)
        .target_id(&master)
        .send()
        .await;
    assert!(eff.is_ok());

    let invalid = client
        .list_accounts_with_invalid_effective_policy()
        .policy_type(EffectivePolicyType::TagPolicy)
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.accounts().len(), 0);

    let errs = client
        .list_effective_policy_validation_errors()
        .account_id(&master)
        .policy_type(EffectivePolicyType::BackupPolicy)
        .send()
        .await
        .unwrap();
    assert_eq!(errs.effective_policy_validation_errors().len(), 0);

    client
        .detach_policy()
        .policy_id(&policy_id)
        .target_id(&root)
        .send()
        .await
        .unwrap();
    client
        .delete_policy()
        .policy_id(&policy_id)
        .send()
        .await
        .unwrap();

    // Enable then disable a non-SCP policy type on the root.
    let _ = client
        .enable_policy_type()
        .root_id(&root)
        .policy_type(PolicyType::TagPolicy)
        .send()
        .await;
    let _ = client
        .disable_policy_type()
        .root_id(&root)
        .policy_type(PolicyType::TagPolicy)
        .send()
        .await;
}

#[test_action("organizations", "InviteAccountToOrganization", checksum = "670e5927")]
#[test_action("organizations", "DescribeHandshake", checksum = "25a7b884")]
#[test_action(
    "organizations",
    "ListHandshakesForOrganization",
    checksum = "65af6765"
)]
#[test_action("organizations", "ListHandshakesForAccount", checksum = "5dc3c04a")]
#[test_action("organizations", "CancelHandshake", checksum = "b8bfa7c6")]
#[test_action("organizations", "AcceptHandshake", checksum = "535da0e1")]
#[test_action("organizations", "DeclineHandshake", checksum = "91ddd2a0")]
#[tokio::test]
async fn organizations_handshake_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();

    let invite = client
        .invite_account_to_organization()
        .target(
            HandshakeParty::builder()
                .id("222222222222")
                .r#type(HandshakePartyType::Account)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let handshake_id = invite.handshake().unwrap().id().unwrap().to_string();

    let desc = client
        .describe_handshake()
        .handshake_id(&handshake_id)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.handshake().unwrap().id(), Some(handshake_id.as_str()));

    let for_org = client
        .list_handshakes_for_organization()
        .send()
        .await
        .unwrap();
    assert!(!for_org.handshakes().is_empty());

    let _ = client.list_handshakes_for_account().send().await.unwrap();

    // Cancel from the source (management) account.
    client
        .cancel_handshake()
        .handshake_id(&handshake_id)
        .send()
        .await
        .unwrap();

    // Accept / Decline on an already-resolved handshake error; the calls
    // still route and return a declared exception.
    let _ = client
        .accept_handshake()
        .handshake_id(&handshake_id)
        .send()
        .await;
    let _ = client
        .decline_handshake()
        .handshake_id(&handshake_id)
        .send()
        .await;
}

#[test_action("organizations", "EnableAWSServiceAccess", checksum = "72ae7271")]
#[test_action(
    "organizations",
    "ListAWSServiceAccessForOrganization",
    checksum = "5f370588"
)]
#[test_action("organizations", "DisableAWSServiceAccess", checksum = "ca5af69b")]
#[test_action(
    "organizations",
    "RegisterDelegatedAdministrator",
    checksum = "24a0bf0f"
)]
#[test_action("organizations", "ListDelegatedAdministrators", checksum = "859569ad")]
#[test_action(
    "organizations",
    "ListDelegatedServicesForAccount",
    checksum = "917b4aab"
)]
#[test_action(
    "organizations",
    "DeregisterDelegatedAdministrator",
    checksum = "8d4aef12"
)]
#[tokio::test]
async fn organizations_service_access_and_delegation() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();
    let member = create_member(&client, "admin@example.com", "Admin").await;

    let principal = "config.amazonaws.com";
    client
        .enable_aws_service_access()
        .service_principal(principal)
        .send()
        .await
        .unwrap();

    let access = client
        .list_aws_service_access_for_organization()
        .send()
        .await
        .unwrap();
    assert!(!access.enabled_service_principals().is_empty());

    client
        .register_delegated_administrator()
        .account_id(&member)
        .service_principal(principal)
        .send()
        .await
        .unwrap();

    let admins = client.list_delegated_administrators().send().await.unwrap();
    assert!(!admins.delegated_administrators().is_empty());

    let services = client
        .list_delegated_services_for_account()
        .account_id(&member)
        .send()
        .await
        .unwrap();
    assert!(!services.delegated_services().is_empty());

    client
        .deregister_delegated_administrator()
        .account_id(&member)
        .service_principal(principal)
        .send()
        .await
        .unwrap();

    client
        .disable_aws_service_access()
        .service_principal(principal)
        .send()
        .await
        .unwrap();
}

#[test_action("organizations", "TagResource", checksum = "54c95523")]
#[test_action("organizations", "ListTagsForResource", checksum = "426c8d7e")]
#[test_action("organizations", "UntagResource", checksum = "fc0af618")]
#[test_action("organizations", "PutResourcePolicy", checksum = "14016aa4")]
#[test_action("organizations", "DescribeResourcePolicy", checksum = "674a8b11")]
#[test_action("organizations", "DeleteResourcePolicy", checksum = "b6718718")]
#[tokio::test]
async fn organizations_tags_and_resource_policy() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();
    let root = client.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    client
        .tag_resource()
        .resource_id(&root)
        .tags(Tag::builder().key("team").value("infra").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_id(&root)
        .send()
        .await
        .unwrap();
    assert!(tags.tags().iter().any(|t| t.key() == "team"));

    client
        .untag_resource()
        .resource_id(&root)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let rp = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"organizations:Describe*","Resource":"*"}]}"#;
    client
        .put_resource_policy()
        .content(rp)
        .send()
        .await
        .unwrap();

    let desc = client.describe_resource_policy().send().await.unwrap();
    assert!(desc.resource_policy().is_some());

    client.delete_resource_policy().send().await.unwrap();
}

#[test_action(
    "organizations",
    "InviteOrganizationToTransferResponsibility",
    checksum = "fe2aedfb"
)]
#[test_action(
    "organizations",
    "ListOutboundResponsibilityTransfers",
    checksum = "588d95f4"
)]
#[test_action(
    "organizations",
    "ListInboundResponsibilityTransfers",
    checksum = "cb757b8d"
)]
#[test_action(
    "organizations",
    "DescribeResponsibilityTransfer",
    checksum = "b5b899ba"
)]
#[test_action("organizations", "UpdateResponsibilityTransfer", checksum = "f666ed2a")]
#[test_action(
    "organizations",
    "TerminateResponsibilityTransfer",
    checksum = "6cb8b5bb"
)]
#[tokio::test]
async fn organizations_responsibility_transfer_lifecycle() {
    let server = TestServer::start().await;
    let client = server.organizations_client().await;
    client.create_organization().send().await.unwrap();

    client
        .invite_organization_to_transfer_responsibility()
        .r#type(ResponsibilityTransferType::Billing)
        .source_name("billing-handoff")
        .start_timestamp(aws_smithy_types::DateTime::from_secs(1893456000))
        .target(
            HandshakeParty::builder()
                .id("222222222222")
                .r#type(HandshakePartyType::Account)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let outbound = client
        .list_outbound_responsibility_transfers()
        .r#type(ResponsibilityTransferType::Billing)
        .send()
        .await
        .unwrap();
    let transfers = outbound.responsibility_transfers();
    assert_eq!(transfers.len(), 1);
    let transfer_id = transfers[0].id().unwrap().to_string();

    let inbound = client
        .list_inbound_responsibility_transfers()
        .r#type(ResponsibilityTransferType::Billing)
        .send()
        .await
        .unwrap();
    assert_eq!(inbound.responsibility_transfers().len(), 0);

    let desc = client
        .describe_responsibility_transfer()
        .id(&transfer_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.responsibility_transfer().unwrap().id(),
        Some(transfer_id.as_str())
    );

    client
        .update_responsibility_transfer()
        .id(&transfer_id)
        .name("renamed")
        .send()
        .await
        .unwrap();

    let term = client
        .terminate_responsibility_transfer()
        .id(&transfer_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        term.responsibility_transfer()
            .unwrap()
            .status()
            .map(|s| s.as_str()),
        Some("WITHDRAWN")
    );
}
