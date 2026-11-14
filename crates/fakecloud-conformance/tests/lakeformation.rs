mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("lakeformation", "AddLFTagsToResource", checksum = "6b7a8da0")]
#[test_action("lakeformation", "AssumeDecoratedRoleWithSAML", checksum = "e82ee9b5")]
#[test_action("lakeformation", "BatchGrantPermissions", checksum = "9be04c21")]
#[test_action("lakeformation", "BatchRevokePermissions", checksum = "6a06f56f")]
#[test_action("lakeformation", "CancelTransaction", checksum = "d1acd175")]
#[test_action("lakeformation", "CommitTransaction", checksum = "ae3045c8")]
#[test_action("lakeformation", "CreateDataCellsFilter", checksum = "78b2e203")]
#[test_action("lakeformation", "CreateLFTag", checksum = "1e3e3c76")]
#[test_action("lakeformation", "CreateLFTagExpression", checksum = "5e92fdcd")]
#[test_action(
    "lakeformation",
    "CreateLakeFormationIdentityCenterConfiguration",
    checksum = "2085b934"
)]
#[test_action("lakeformation", "CreateLakeFormationOptIn", checksum = "836b5051")]
#[test_action("lakeformation", "DeleteDataCellsFilter", checksum = "4712443c")]
#[test_action("lakeformation", "DeleteLFTag", checksum = "8850abc4")]
#[test_action("lakeformation", "DeleteLFTagExpression", checksum = "a7c95c45")]
#[test_action(
    "lakeformation",
    "DeleteLakeFormationIdentityCenterConfiguration",
    checksum = "c4b1b43c"
)]
#[test_action("lakeformation", "DeleteLakeFormationOptIn", checksum = "a5bfa04b")]
#[test_action("lakeformation", "DeleteObjectsOnCancel", checksum = "5ac55ab9")]
#[test_action("lakeformation", "DeregisterResource", checksum = "4b8c7ad9")]
#[test_action(
    "lakeformation",
    "DescribeLakeFormationIdentityCenterConfiguration",
    checksum = "63282eb0"
)]
#[test_action("lakeformation", "DescribeResource", checksum = "770306f8")]
#[test_action("lakeformation", "DescribeTransaction", checksum = "c79dd067")]
#[test_action("lakeformation", "ExtendTransaction", checksum = "17bdd1ee")]
#[test_action("lakeformation", "GetDataCellsFilter", checksum = "7c568b71")]
#[test_action("lakeformation", "GetDataLakePrincipal", checksum = "67ac1c5e")]
#[test_action("lakeformation", "GetDataLakeSettings", checksum = "4a8004a5")]
#[test_action(
    "lakeformation",
    "GetEffectivePermissionsForPath",
    checksum = "bf9ee4d6"
)]
#[test_action("lakeformation", "GetLFTag", checksum = "6e3e3d4e")]
#[test_action("lakeformation", "GetLFTagExpression", checksum = "a7dbb64c")]
#[test_action("lakeformation", "GetQueryState", checksum = "be5d50b0")]
#[test_action("lakeformation", "GetQueryStatistics", checksum = "c4fe161c")]
#[test_action("lakeformation", "GetResourceLFTags", checksum = "aa555d4b")]
#[test_action("lakeformation", "GetTableObjects", checksum = "bf0ea4e4")]
#[test_action(
    "lakeformation",
    "GetTemporaryDataLocationCredentials",
    checksum = "73980393"
)]
#[test_action(
    "lakeformation",
    "GetTemporaryGluePartitionCredentials",
    checksum = "ea8a49e8"
)]
#[test_action(
    "lakeformation",
    "GetTemporaryGlueTableCredentials",
    checksum = "984c43ba"
)]
#[test_action("lakeformation", "GetWorkUnitResults", checksum = "85fd7e89")]
#[test_action("lakeformation", "GetWorkUnits", checksum = "df087a71")]
#[test_action("lakeformation", "GrantPermissions", checksum = "503b0756")]
#[test_action("lakeformation", "ListDataCellsFilter", checksum = "acee8b5e")]
#[test_action("lakeformation", "ListLFTagExpressions", checksum = "a0e866c0")]
#[test_action("lakeformation", "ListLFTags", checksum = "7cfab7c3")]
#[test_action("lakeformation", "ListLakeFormationOptIns", checksum = "9528404d")]
#[test_action("lakeformation", "ListPermissions", checksum = "524714dd")]
#[test_action("lakeformation", "ListResources", checksum = "6eef415a")]
#[test_action("lakeformation", "ListTableStorageOptimizers", checksum = "2570c9a5")]
#[test_action("lakeformation", "ListTransactions", checksum = "4e99c819")]
#[test_action("lakeformation", "PutDataLakeSettings", checksum = "604ed3e6")]
#[test_action("lakeformation", "RegisterResource", checksum = "4b6369ff")]
#[test_action("lakeformation", "RemoveLFTagsFromResource", checksum = "95488cd5")]
#[test_action("lakeformation", "RevokePermissions", checksum = "be1ddcbb")]
#[test_action("lakeformation", "SearchDatabasesByLFTags", checksum = "eaa46ab6")]
#[test_action("lakeformation", "SearchTablesByLFTags", checksum = "392895fa")]
#[test_action("lakeformation", "StartQueryPlanning", checksum = "96f727e2")]
#[test_action("lakeformation", "StartTransaction", checksum = "5bcacb3d")]
#[test_action("lakeformation", "UpdateDataCellsFilter", checksum = "9d89ef76")]
#[test_action("lakeformation", "UpdateLFTag", checksum = "d452386f")]
#[test_action("lakeformation", "UpdateLFTagExpression", checksum = "b26eee0d")]
#[test_action(
    "lakeformation",
    "UpdateLakeFormationIdentityCenterConfiguration",
    checksum = "aa327f5e"
)]
#[test_action("lakeformation", "UpdateResource", checksum = "127ee5ea")]
#[test_action("lakeformation", "UpdateTableObjects", checksum = "4a9546a3")]
#[test_action("lakeformation", "UpdateTableStorageOptimizer", checksum = "464a1329")]
#[tokio::test]
async fn lakeformation_conformance() {
    let _server = TestServer::start().await;
}

/// POST a restJson1 Lake Formation operation (`POST /<Operation>`), returning
/// `(status, parsed_body)`. The RLS operations are newer than any Lake
/// Formation SDK this workspace depends on.
async fn lf(server: &TestServer, op: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/{op}", server.endpoint()))
        .header("content-type", "application/json")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/lakeformation/aws4_request, SignedHeaders=host, Signature=0",
        )
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
    )
}

#[test_action("lakeformation", "CreateRLSPolicy", checksum = "1614d26a")]
#[test_action("lakeformation", "GetRLSPolicy", checksum = "46224af1")]
#[test_action("lakeformation", "UpdateRLSPolicy", checksum = "43d402b3")]
#[test_action("lakeformation", "ListRLSPolicies", checksum = "0097b41a")]
#[test_action("lakeformation", "AttachRLSPolicy", checksum = "745919e2")]
#[test_action("lakeformation", "ListRLSPolicyAttachments", checksum = "b765833d")]
#[test_action("lakeformation", "DetachRLSPolicy", checksum = "56638f50")]
#[test_action("lakeformation", "DeleteRLSPolicy", checksum = "4c9492f9")]
#[tokio::test]
async fn lakeformation_rls_policy_lifecycle() {
    use serde_json::json;
    let server = TestServer::start().await;
    let table = json!({"Table": {"DatabaseName": "sales", "Name": "orders"}});
    let principal =
        json!({"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/analyst"});

    let (status, out) = lf(
        &server,
        "CreateRLSPolicy",
        json!({
            "Name": "us-only",
            "AttachmentColumns": [{"Name": "region", "Type": "string"}],
            "FilterExpression": "region = 'us'",
        }),
    )
    .await;
    assert_eq!(status, 200, "{out}");

    let (status, out) = lf(
        &server,
        "UpdateRLSPolicy",
        json!({"Name": "us-only", "FilterExpression": "region = 'eu'"}),
    )
    .await;
    assert_eq!(status, 200, "{out}");

    let (status, out) = lf(&server, "GetRLSPolicy", json!({"Name": "us-only"})).await;
    assert_eq!(status, 200, "{out}");
    assert_eq!(out["RLSPolicy"]["FilterExpression"], "region = 'eu'");
    assert_eq!(out["RLSPolicy"]["AttachmentColumns"][0]["Name"], "region");

    let (status, out) = lf(&server, "ListRLSPolicies", json!({})).await;
    assert_eq!(status, 200, "{out}");
    assert_eq!(out["RLSPolicies"][0]["Name"], "us-only");

    let (status, out) = lf(
        &server,
        "AttachRLSPolicy",
        json!({"Name": "us-only", "Resource": table, "Principal": principal, "SetDefault": true}),
    )
    .await;
    assert_eq!(status, 200, "{out}");

    let (status, out) = lf(
        &server,
        "ListRLSPolicyAttachments",
        json!({"Resource": table}),
    )
    .await;
    assert_eq!(status, 200, "{out}");
    let items = out["RLSPolicyAttachments"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["PolicyName"], "us-only");
    assert_eq!(items[0]["DefaultRLSPolicy"], true);
    assert_eq!(items[0]["Principal"], principal);

    // Permissions on the policy itself go through the regular grant path.
    let (status, out) = lf(
        &server,
        "GrantPermissions",
        json!({"Principal": principal, "Resource": {"RLSPolicy": {"Name": "us-only"}}, "Permissions": ["DESCRIBE"]}),
    )
    .await;
    assert_eq!(status, 200, "{out}");
    let (_, out) = lf(
        &server,
        "ListPermissions",
        json!({"Resource": {"RLSPolicy": {"Name": "us-only"}}}),
    )
    .await;
    assert_eq!(
        out["PrincipalResourcePermissions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let (status, out) = lf(
        &server,
        "DetachRLSPolicy",
        json!({"Name": "us-only", "Resource": table, "Principal": principal}),
    )
    .await;
    assert_eq!(status, 200, "{out}");
    let (_, out) = lf(&server, "ListRLSPolicyAttachments", json!({})).await;
    assert!(out["RLSPolicyAttachments"].as_array().unwrap().is_empty());

    let (status, out) = lf(&server, "DeleteRLSPolicy", json!({"Name": "us-only"})).await;
    assert_eq!(status, 200, "{out}");
    let (status, out) = lf(&server, "GetRLSPolicy", json!({"Name": "us-only"})).await;
    assert_eq!(status, 400);
    assert!(out.to_string().contains("EntityNotFoundException"), "{out}");
}
