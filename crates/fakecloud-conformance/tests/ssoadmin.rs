mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("sso", "AddRegion", checksum = "0dbd0fed")]
#[test_action(
    "sso",
    "AttachCustomerManagedPolicyReferenceToPermissionSet",
    checksum = "fcc73818"
)]
#[test_action("sso", "AttachManagedPolicyToPermissionSet", checksum = "9b1e0a81")]
#[test_action("sso", "CreateAccountAssignment", checksum = "6f844c02")]
#[test_action("sso", "CreateApplication", checksum = "187db9fe")]
#[test_action("sso", "CreateApplicationAssignment", checksum = "f08253d4")]
#[test_action("sso", "CreateInstance", checksum = "1788beb3")]
#[test_action(
    "sso",
    "CreateInstanceAccessControlAttributeConfiguration",
    checksum = "c06f362a"
)]
#[test_action("sso", "CreatePermissionSet", checksum = "1d558b80")]
#[test_action("sso", "CreateTrustedTokenIssuer", checksum = "00e8267b")]
#[test_action("sso", "DeleteAccountAssignment", checksum = "c9d1d193")]
#[test_action("sso", "DeleteApplication", checksum = "c6f31f0c")]
#[test_action("sso", "DeleteApplicationAccessScope", checksum = "351f731c")]
#[test_action("sso", "DeleteApplicationAssignment", checksum = "51590018")]
#[test_action("sso", "DeleteApplicationAuthenticationMethod", checksum = "d9477f3a")]
#[test_action("sso", "DeleteApplicationGrant", checksum = "9407214d")]
#[test_action("sso", "DeleteInlinePolicyFromPermissionSet", checksum = "3b1053d5")]
#[test_action("sso", "DeleteInstance", checksum = "27155b62")]
#[test_action(
    "sso",
    "DeleteInstanceAccessControlAttributeConfiguration",
    checksum = "1fc92c21"
)]
#[test_action(
    "sso",
    "DeletePermissionsBoundaryFromPermissionSet",
    checksum = "053d9008"
)]
#[test_action("sso", "DeletePermissionSet", checksum = "746e7962")]
#[test_action("sso", "DeleteTrustedTokenIssuer", checksum = "b716079b")]
#[test_action(
    "sso",
    "DescribeAccountAssignmentCreationStatus",
    checksum = "0871ffaa"
)]
#[test_action(
    "sso",
    "DescribeAccountAssignmentDeletionStatus",
    checksum = "3f671f39"
)]
#[test_action("sso", "DescribeApplication", checksum = "04bfbb96")]
#[test_action("sso", "DescribeApplicationAssignment", checksum = "6da00b39")]
#[test_action("sso", "DescribeApplicationProvider", checksum = "bc843c7f")]
#[test_action("sso", "DescribeInstance", checksum = "fc5c02ab")]
#[test_action(
    "sso",
    "DescribeInstanceAccessControlAttributeConfiguration",
    checksum = "8da0a3d8"
)]
#[test_action("sso", "DescribePermissionSet", checksum = "f5567ad8")]
#[test_action(
    "sso",
    "DescribePermissionSetProvisioningStatus",
    checksum = "5361e67d"
)]
#[test_action("sso", "DescribeRegion", checksum = "9598a8da")]
#[test_action("sso", "DescribeTrustedTokenIssuer", checksum = "50b704a9")]
#[test_action(
    "sso",
    "DetachCustomerManagedPolicyReferenceFromPermissionSet",
    checksum = "996571ee"
)]
#[test_action("sso", "DetachManagedPolicyFromPermissionSet", checksum = "f9485c5f")]
#[test_action("sso", "GetApplicationAccessScope", checksum = "528704ac")]
#[test_action("sso", "GetApplicationAssignmentConfiguration", checksum = "12d37513")]
#[test_action("sso", "GetApplicationAuthenticationMethod", checksum = "c0908f3a")]
#[test_action("sso", "GetApplicationGrant", checksum = "828cf766")]
#[test_action("sso", "GetApplicationSessionConfiguration", checksum = "4625209c")]
#[test_action("sso", "GetInlinePolicyForPermissionSet", checksum = "87a50e19")]
#[test_action("sso", "GetPermissionsBoundaryForPermissionSet", checksum = "5877c4fb")]
#[test_action("sso", "ListAccountAssignmentCreationStatus", checksum = "0908818a")]
#[test_action("sso", "ListAccountAssignmentDeletionStatus", checksum = "06de4985")]
#[test_action("sso", "ListAccountAssignments", checksum = "51ca7ebb")]
#[test_action("sso", "ListAccountAssignmentsForPrincipal", checksum = "c9f68d0a")]
#[test_action(
    "sso",
    "ListAccountsForProvisionedPermissionSet",
    checksum = "05ca66bb"
)]
#[test_action("sso", "ListApplicationAccessScopes", checksum = "984a309f")]
#[test_action("sso", "ListApplicationAssignments", checksum = "b5c383b3")]
#[test_action("sso", "ListApplicationAssignmentsForPrincipal", checksum = "c6018fcd")]
#[test_action("sso", "ListApplicationAuthenticationMethods", checksum = "9f526ce9")]
#[test_action("sso", "ListApplicationGrants", checksum = "13580835")]
#[test_action("sso", "ListApplicationProviders", checksum = "53421c3c")]
#[test_action("sso", "ListApplications", checksum = "3591ec21")]
#[test_action(
    "sso",
    "ListCustomerManagedPolicyReferencesInPermissionSet",
    checksum = "93ff10bc"
)]
#[test_action("sso", "ListInstances", checksum = "cd19bc86")]
#[test_action("sso", "ListManagedPoliciesInPermissionSet", checksum = "5ff82f40")]
#[test_action("sso", "ListPermissionSetProvisioningStatus", checksum = "9a229c19")]
#[test_action("sso", "ListPermissionSets", checksum = "f7c54322")]
#[test_action("sso", "ListPermissionSetsProvisionedToAccount", checksum = "ea56a923")]
#[test_action("sso", "ListRegions", checksum = "5539d9df")]
#[test_action("sso", "ListTagsForResource", checksum = "58438c8a")]
#[test_action("sso", "ListTrustedTokenIssuers", checksum = "7f98ad71")]
#[test_action("sso", "ProvisionPermissionSet", checksum = "02e55de4")]
#[test_action("sso", "PutApplicationAccessScope", checksum = "35a26a49")]
#[test_action("sso", "PutApplicationAssignmentConfiguration", checksum = "1f306171")]
#[test_action("sso", "PutApplicationAuthenticationMethod", checksum = "5303b35a")]
#[test_action("sso", "PutApplicationGrant", checksum = "dd12b636")]
#[test_action("sso", "PutApplicationSessionConfiguration", checksum = "54f0bf55")]
#[test_action("sso", "PutInlinePolicyToPermissionSet", checksum = "a17b9a2e")]
#[test_action("sso", "PutPermissionsBoundaryToPermissionSet", checksum = "d33db2f7")]
#[test_action("sso", "RemoveRegion", checksum = "1ecbc260")]
#[test_action("sso", "TagResource", checksum = "92207645")]
#[test_action("sso", "UntagResource", checksum = "0e0b6802")]
#[test_action("sso", "UpdateApplication", checksum = "08b88059")]
#[test_action("sso", "UpdateInstance", checksum = "965285db")]
#[test_action(
    "sso",
    "UpdateInstanceAccessControlAttributeConfiguration",
    checksum = "7c840f27"
)]
#[test_action("sso", "UpdatePermissionSet", checksum = "5920bcbc")]
#[test_action("sso", "UpdateTrustedTokenIssuer", checksum = "c8a3080e")]
#[tokio::test]
async fn ssoadmin_probe() {
    let _server = TestServer::start().await;
}
