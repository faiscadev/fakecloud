mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ram", "AcceptResourceShareInvitation", checksum = "7b14999f")]
#[test_action("ram", "AssociateResourceShare", checksum = "117e6cca")]
#[test_action("ram", "AssociateResourceSharePermission", checksum = "46d0fd78")]
#[test_action("ram", "CreatePermission", checksum = "9f615b56")]
#[test_action("ram", "CreatePermissionVersion", checksum = "01e9d8fe")]
#[test_action("ram", "CreateResourceShare", checksum = "0912f0f2")]
#[test_action("ram", "DeletePermission", checksum = "5b03f6d1")]
#[test_action("ram", "DeletePermissionVersion", checksum = "bc3be5b1")]
#[test_action("ram", "DeleteResourceShare", checksum = "ef35843b")]
#[test_action("ram", "DisassociateResourceShare", checksum = "86b713ca")]
#[test_action("ram", "DisassociateResourceSharePermission", checksum = "cb575463")]
#[test_action("ram", "EnableSharingWithAwsOrganization", checksum = "2305020d")]
#[test_action("ram", "GetPermission", checksum = "0df84914")]
#[test_action("ram", "GetResourcePolicies", checksum = "d748d193")]
#[test_action("ram", "GetResourceShareAssociations", checksum = "815ccde3")]
#[test_action("ram", "GetResourceShareInvitations", checksum = "b8b1a970")]
#[test_action("ram", "GetResourceShares", checksum = "178ea1fe")]
#[test_action("ram", "ListPendingInvitationResources", checksum = "1c4b4de1")]
#[test_action("ram", "ListPermissionAssociations", checksum = "fc3434d2")]
#[test_action("ram", "ListPermissionVersions", checksum = "94f5e5f0")]
#[test_action("ram", "ListPermissions", checksum = "7b229cab")]
#[test_action("ram", "ListPrincipals", checksum = "5ac27c29")]
#[test_action("ram", "ListReplacePermissionAssociationsWork", checksum = "9c84ef2e")]
#[test_action("ram", "ListResourceSharePermissions", checksum = "d7784812")]
#[test_action("ram", "ListResourceTypes", checksum = "4a4cdc4d")]
#[test_action("ram", "ListResources", checksum = "e387553b")]
#[test_action("ram", "ListSourceAssociations", checksum = "f7a591bd")]
#[test_action("ram", "PromotePermissionCreatedFromPolicy", checksum = "3eaccaf0")]
#[test_action("ram", "PromoteResourceShareCreatedFromPolicy", checksum = "ba4723c6")]
#[test_action("ram", "RejectResourceShareInvitation", checksum = "d5a5d899")]
#[test_action("ram", "ReplacePermissionAssociations", checksum = "da5ca237")]
#[test_action("ram", "SetDefaultPermissionVersion", checksum = "f586612c")]
#[test_action("ram", "TagResource", checksum = "29c2a917")]
#[test_action("ram", "UntagResource", checksum = "c0d920be")]
#[test_action("ram", "UpdateResourceShare", checksum = "c0280e19")]
#[tokio::test]
async fn ram_probe() {
    let _server = TestServer::start().await;
}
