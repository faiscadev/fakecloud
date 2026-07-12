//! End-to-end tests for Amazon Managed Blockchain, driven through the real
//! `aws-sdk-managedblockchain` client against a live fakecloud server.
//!
//! Exercises the control plane end to end: create a Hyperledger Fabric network
//! (which atomically creates the first member) -> get the network + member ->
//! create a node -> create a proposal inviting an account -> vote -> the
//! proposal reaches `APPROVED` and materialises an invitation -> list
//! invitations -> create a token accessor.

use aws_sdk_managedblockchain::types::{
    AccessorType, ApprovalThresholdPolicy, Framework, InviteAction, MemberConfiguration,
    MemberFabricConfiguration, MemberFrameworkConfiguration, NodeConfiguration, ProposalActions,
    ThresholdComparator, VoteValue, VotingPolicy,
};
use fakecloud_testkit::TestServer;

async fn mbc_client(server: &TestServer) -> aws_sdk_managedblockchain::Client {
    let conf = aws_sdk_managedblockchain::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_managedblockchain::Client::from_conf(conf)
}

#[tokio::test]
async fn managedblockchain_full_lifecycle() {
    let server = TestServer::start().await;
    let client = mbc_client(&server).await;

    // --- Create a Hyperledger Fabric network with its first member ---
    let member_config = MemberConfiguration::builder()
        .name("member1")
        .description("founding member")
        .framework_configuration(
            MemberFrameworkConfiguration::builder()
                .fabric(
                    MemberFabricConfiguration::builder()
                        .admin_username("admin")
                        .admin_password("Password123!")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();

    let voting_policy = VotingPolicy::builder()
        .approval_threshold_policy(
            ApprovalThresholdPolicy::builder()
                .threshold_percentage(50)
                .proposal_duration_in_hours(24)
                .threshold_comparator(ThresholdComparator::GreaterThan)
                .build(),
        )
        .build();

    let created = client
        .create_network()
        .client_request_token("token-net-1")
        .name("e2e-network")
        .description("end-to-end network")
        .framework(Framework::HyperledgerFabric)
        .framework_version("2.2")
        .voting_policy(voting_policy)
        .member_configuration(member_config)
        .send()
        .await
        .expect("create_network");
    let network_id = created.network_id().expect("network id").to_string();
    let member_id = created.member_id().expect("member id").to_string();
    assert!(network_id.starts_with("n-"));
    assert!(member_id.starts_with("m-"));

    // --- Get the network ---
    let net = client
        .get_network()
        .network_id(&network_id)
        .send()
        .await
        .expect("get_network")
        .network
        .expect("network present");
    assert_eq!(net.name(), Some("e2e-network"));
    assert_eq!(net.description(), Some("end-to-end network"));
    assert_eq!(net.framework(), Some(&Framework::HyperledgerFabric));

    // --- Get the member (settles CREATING -> AVAILABLE on read) ---
    let member = client
        .get_member()
        .network_id(&network_id)
        .member_id(&member_id)
        .send()
        .await
        .expect("get_member")
        .member
        .expect("member present");
    assert_eq!(member.name(), Some("member1"));
    let ca_endpoint = member
        .framework_attributes()
        .and_then(|fa| fa.fabric())
        .and_then(|f| f.ca_endpoint())
        .unwrap_or_default();
    assert!(!ca_endpoint.is_empty(), "member should have a CA endpoint");

    // --- Create a node under the member ---
    let node = client
        .create_node()
        .client_request_token("token-node-1")
        .network_id(&network_id)
        .member_id(&member_id)
        .node_configuration(
            NodeConfiguration::builder()
                .instance_type("bc.t3.small")
                .availability_zone("us-east-1a")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_node");
    let node_id = node.node_id().expect("node id").to_string();
    assert!(node_id.starts_with("nd-"));

    let got_node = client
        .get_node()
        .network_id(&network_id)
        .node_id(&node_id)
        .send()
        .await
        .expect("get_node")
        .node
        .expect("node present");
    let peer_endpoint = got_node
        .framework_attributes()
        .and_then(|fa| fa.fabric())
        .and_then(|f| f.peer_endpoint())
        .unwrap_or_default();
    assert!(
        !peer_endpoint.is_empty(),
        "node should have a peer endpoint"
    );

    // --- Create a proposal inviting our own account ---
    let proposal = client
        .create_proposal()
        .client_request_token("token-prop-1")
        .network_id(&network_id)
        .member_id(&member_id)
        .actions(
            ProposalActions::builder()
                .invitations(
                    InviteAction::builder()
                        .principal("123456789012")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create_proposal");
    let proposal_id = proposal.proposal_id().expect("proposal id").to_string();
    assert!(proposal_id.starts_with("p-"));

    // --- Vote YES; a single member at 50% GREATER_THAN approves ---
    client
        .vote_on_proposal()
        .network_id(&network_id)
        .proposal_id(&proposal_id)
        .voter_member_id(&member_id)
        .vote(VoteValue::Yes)
        .send()
        .await
        .expect("vote_on_proposal");

    let decided = client
        .get_proposal()
        .network_id(&network_id)
        .proposal_id(&proposal_id)
        .send()
        .await
        .expect("get_proposal")
        .proposal
        .expect("proposal present");
    assert_eq!(
        decided.status(),
        Some(&aws_sdk_managedblockchain::types::ProposalStatus::Approved)
    );
    assert_eq!(decided.yes_vote_count(), Some(1));

    // --- The vote is recorded ---
    let votes = client
        .list_proposal_votes()
        .network_id(&network_id)
        .proposal_id(&proposal_id)
        .send()
        .await
        .expect("list_proposal_votes")
        .proposal_votes
        .unwrap_or_default();
    assert_eq!(votes.len(), 1);
    assert_eq!(votes[0].vote(), Some(&VoteValue::Yes));

    // --- The approved proposal materialised an invitation ---
    let invitations = client
        .list_invitations()
        .send()
        .await
        .expect("list_invitations")
        .invitations
        .unwrap_or_default();
    assert_eq!(invitations.len(), 1);
    assert_eq!(
        invitations[0].status(),
        Some(&aws_sdk_managedblockchain::types::InvitationStatus::Pending)
    );

    // --- Create a token accessor ---
    let accessor = client
        .create_accessor()
        .client_request_token("token-acc-1")
        .accessor_type(AccessorType::BillingToken)
        .send()
        .await
        .expect("create_accessor");
    let accessor_id = accessor.accessor_id().expect("accessor id").to_string();
    assert!(!accessor.billing_token().unwrap_or_default().is_empty());

    let got_accessor = client
        .get_accessor()
        .accessor_id(&accessor_id)
        .send()
        .await
        .expect("get_accessor")
        .accessor
        .expect("accessor present");
    assert_eq!(
        got_accessor.status(),
        Some(&aws_sdk_managedblockchain::types::AccessorStatus::Available)
    );
    assert_eq!(got_accessor.r#type(), Some(&AccessorType::BillingToken));
}
