mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ram", "AcceptResourceShareInvitation", checksum = "7b14999f")]
#[test_action("ram", "AssociateResourceShare", checksum = "117e6cca")]
#[test_action("ram", "AssociateResourceSharePermission", checksum = "09670926")]
#[test_action("ram", "CreatePermission", checksum = "8b09161f")]
#[test_action("ram", "CreatePermissionVersion", checksum = "6cd426a1")]
#[test_action("ram", "CreateResourceShare", checksum = "57b4a107")]
#[test_action("ram", "DeletePermission", checksum = "34868c4f")]
#[test_action("ram", "DeletePermissionVersion", checksum = "e2b844f1")]
#[test_action("ram", "DeleteResourceShare", checksum = "ef35843b")]
#[test_action("ram", "DisassociateResourceShare", checksum = "86b713ca")]
#[test_action("ram", "DisassociateResourceSharePermission", checksum = "cb575463")]
#[test_action("ram", "EnableSharingWithAwsOrganization", checksum = "2305020d")]
#[test_action("ram", "GetPermission", checksum = "89455ab2")]
#[test_action("ram", "GetResourcePolicies", checksum = "d748d193")]
#[test_action("ram", "GetResourceShareAssociations", checksum = "815ccde3")]
#[test_action("ram", "GetResourceShareInvitations", checksum = "b8b1a970")]
#[test_action("ram", "GetResourceShares", checksum = "178ea1fe")]
#[test_action("ram", "ListPendingInvitationResources", checksum = "1c4b4de1")]
#[test_action("ram", "ListPermissionAssociations", checksum = "54bdafae")]
#[test_action("ram", "ListPermissionVersions", checksum = "a3a2bdaf")]
#[test_action("ram", "ListPermissions", checksum = "bfed21da")]
#[test_action("ram", "ListPrincipals", checksum = "5ac27c29")]
#[test_action("ram", "ListReplacePermissionAssociationsWork", checksum = "f7494e9c")]
#[test_action("ram", "ListResourceSharePermissions", checksum = "20749872")]
#[test_action("ram", "ListResourceTypes", checksum = "4a4cdc4d")]
#[test_action("ram", "ListResources", checksum = "e387553b")]
#[test_action("ram", "ListSourceAssociations", checksum = "f7a591bd")]
#[test_action("ram", "PromotePermissionCreatedFromPolicy", checksum = "6f69e0bf")]
#[test_action("ram", "PromoteResourceShareCreatedFromPolicy", checksum = "ba4723c6")]
#[test_action("ram", "RejectResourceShareInvitation", checksum = "d5a5d899")]
#[test_action("ram", "ReplacePermissionAssociations", checksum = "8a582267")]
#[test_action("ram", "SetDefaultPermissionVersion", checksum = "f586612c")]
#[test_action("ram", "TagResource", checksum = "29c2a917")]
#[test_action("ram", "UntagResource", checksum = "c0d920be")]
#[test_action("ram", "UpdateResourceShare", checksum = "c0280e19")]
#[tokio::test]
async fn ram_probe() {
    let _server = TestServer::start().await;
}

/// Drive a RAM operation over raw restJson1 (`POST /<op lowercased>`): the
/// pinned SDK predates UpdatePermission.
async fn ram_raw(
    server: &TestServer,
    op: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/{}", server.endpoint(), op.to_ascii_lowercase()))
        .header("content-type", "application/json")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/ram/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let json = resp
        .json::<serde_json::Value>()
        .await
        .unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[test_action("ram", "UpdatePermission", checksum = "fc69a92f")]
#[tokio::test]
async fn ram_update_permission() {
    let server = TestServer::start().await;
    let (s, created) = ram_raw(
        &server,
        "CreatePermission",
        serde_json::json!({
            "name": "conf-perm",
            "resourceType": "ec2:Subnet",
            "policyTemplate": "{\"Effect\":\"Allow\",\"Action\":[\"ec2:DescribeSubnets\"]}",
        }),
    )
    .await;
    assert_eq!(s, 200, "{created}");
    let arn = created["permission"]["arn"].as_str().unwrap().to_string();

    let (s, b) = ram_raw(
        &server,
        "UpdatePermission",
        serde_json::json!({"permissionArn": arn, "status": "DEPRECATED"}),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["permissionStatus"], "DEPRECATED");

    // A deprecated permission is listed by its status and takes no new
    // resource share associations.
    let (_, listed) = ram_raw(
        &server,
        "ListPermissions",
        serde_json::json!({"permissionStatus": "DEPRECATED", "resourceOwner": "SELF"}),
    )
    .await;
    assert_eq!(listed["permissions"][0]["arn"], arn.as_str());
    let (s, _) = ram_raw(
        &server,
        "CreateResourceShare",
        serde_json::json!({"name": "s", "permissionArns": [arn]}),
    )
    .await;
    assert_eq!(s, 400);

    let (s, b) = ram_raw(
        &server,
        "UpdatePermission",
        serde_json::json!({"permissionArn": arn, "status": "ATTACHABLE"}),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["permissionStatus"], "ATTACHABLE");
}
