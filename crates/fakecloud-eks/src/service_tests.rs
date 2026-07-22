//! Unit tests for the EKS service (extracted from `service.rs`).

use super::*;

use bytes::Bytes;
use http::HeaderMap;
use parking_lot::RwLock;
use std::collections::HashMap;

fn make_state() -> SharedEksState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("111122223333", "us-east-1", ""),
    ))
}

fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
    let (p, q) = match path.find('?') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (path, ""),
    };
    let path_segments: Vec<String> = p
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let query_params: HashMap<String, String> = q
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();
    AwsRequest {
        service: "eks".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "111122223333".to_string(),
        request_id: "test".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments,
        raw_path: p.to_string(),
        raw_query: q.to_string(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn create_body(name: &str) -> String {
    json!({
        "name": name,
        "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
        "resourcesVpcConfig": {
            "subnetIds": ["subnet-1", "subnet-2"],
            "endpointPublicAccess": true
        }
    })
    .to_string()
}

#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = EksService::new(make_state());
    assert!(svc.snapshot_hook().is_none());
}

#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn SnapshotStore> = Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = EksService::new(make_state()).with_snapshot_store(store);
    let hook = svc.snapshot_hook().expect("hook present when store set");
    hook().await;
}

#[tokio::test]
async fn create_describe_round_trip_settles_active() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters",
            &create_body("demo"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["name"], "demo");
    assert_eq!(v["cluster"]["status"], "CREATING");
    assert_eq!(
        v["cluster"]["arn"],
        "arn:aws:eks:us-east-1:111122223333:cluster/demo"
    );
    assert_eq!(v["cluster"]["version"], DEFAULT_K8S_VERSION);

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/demo", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["status"], "ACTIVE");
    assert_eq!(
        v["cluster"]["resourcesVpcConfig"]["endpointPublicAccess"],
        true
    );
}

fn make_request_in_region(method: Method, path: &str, body: &str, region: &str) -> AwsRequest {
    let mut req = make_request(method, path, body);
    req.region = region.to_string();
    req
}

#[tokio::test]
async fn describe_cluster_oidc_issuer_includes_region() {
    // Real AWS scopes the OIDC issuer host to the cluster's region:
    // `https://oidc.eks.<region>.amazonaws.com/id/<ID>`. Tools (eksctl IRSA,
    // Terraform aws_iam_openid_connect_provider) parse the region from the
    // host and break on a region-less issuer. The region flows from the
    // request (which is what the cluster's ARN records).
    for region in ["us-east-1", "eu-west-1", "ap-southeast-2"] {
        let svc = EksService::new(make_state());
        svc.handle(make_request_in_region(
            Method::POST,
            "/clusters",
            &create_body("oidc"),
            region,
        ))
        .await
        .unwrap();
        let resp = svc
            .handle(make_request_in_region(
                Method::GET,
                "/clusters/oidc",
                "",
                region,
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let issuer = v["cluster"]["identity"]["oidc"]["issuer"].as_str().unwrap();
        let expected_prefix = format!("https://oidc.eks.{region}.amazonaws.com/id/");
        assert!(
            issuer.starts_with(&expected_prefix),
            "issuer {issuer} should start with {expected_prefix}"
        );
        // The trailing segment is the uppercased cluster id.
        let id = issuer.strip_prefix(&expected_prefix).unwrap();
        assert!(!id.is_empty());
        assert_eq!(id, id.to_uppercase());
    }
}

#[tokio::test]
async fn create_rejects_duplicate() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("dup")))
        .await
        .unwrap();
    let err = svc
        .handle(make_request(Method::POST, "/clusters", &create_body("dup")))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn describe_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(Method::GET, "/clusters/ghost", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn list_then_delete_cluster() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("c1")))
        .await
        .unwrap();
    let resp = svc
        .handle(make_request(Method::GET, "/clusters", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["clusters"], json!(["c1"]));

    let resp = svc
        .handle(make_request(Method::DELETE, "/clusters/c1", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["status"], "DELETING");

    let err = svc
        .handle(make_request(Method::GET, "/clusters/c1", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_reports_access_config_upgrade_policy_and_elb_defaults() {
    let svc = EksService::new(make_state());
    // Create with no accessConfig/upgradePolicy/kubernetesNetworkConfig: the
    // response defaults them the way the real control plane does.
    let resp = svc
        .handle(make_request(Method::POST, "/clusters", &create_body("c1")))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let c = &v["cluster"];
    assert_eq!(c["accessConfig"]["authenticationMode"], "CONFIG_MAP");
    assert_eq!(c["upgradePolicy"]["supportType"], "EXTENDED");
    assert_eq!(
        c["kubernetesNetworkConfig"]["elasticLoadBalancing"]["enabled"],
        false
    );
    assert_eq!(
        c["kubernetesNetworkConfig"]["serviceIpv4Cidr"],
        "172.20.0.0/16"
    );
    assert_eq!(c["kubernetesNetworkConfig"]["ipFamily"], "ipv4");

    // Describe echoes the same shape (drift-free round-trip).
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["cluster"]["accessConfig"]["authenticationMode"],
        "CONFIG_MAP"
    );
    assert_eq!(v["cluster"]["upgradePolicy"]["supportType"], "EXTENDED");
    assert_eq!(
        v["cluster"]["kubernetesNetworkConfig"]["elasticLoadBalancing"]["enabled"],
        false
    );
}

#[tokio::test]
async fn cluster_echoes_supplied_access_config() {
    let svc = EksService::new(make_state());
    let body = json!({
        "name": "c1",
        "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
        "resourcesVpcConfig": { "subnetIds": ["subnet-1", "subnet-2"] },
        "accessConfig": { "authenticationMode": "API" },
    })
    .to_string();
    let resp = svc
        .handle(make_request(Method::POST, "/clusters", &body))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["accessConfig"]["authenticationMode"], "API");
}

#[tokio::test]
async fn update_version_and_describe_update() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("up")))
        .await
        .unwrap();
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/up/updates",
            &json!({ "version": "1.32" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "VersionUpdate");
    assert_eq!(v["update"]["status"], "InProgress");
    let update_id = v["update"]["id"].as_str().unwrap().to_string();

    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/up/updates/{update_id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "Successful");

    // The cluster's version reflects the update.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/up", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["version"], "1.32");

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/up/updates", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["updateIds"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn tag_untag_round_trip() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("tg")))
        .await
        .unwrap();
    let arn = "arn:aws:eks:us-east-1:111122223333:cluster/tg";
    let encoded = arn.replace('/', "%2F");
    svc.handle(make_request(
        Method::POST,
        &format!("/tags/{encoded}"),
        &json!({ "tags": { "env": "prod" } }).to_string(),
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["tags"]["env"], "prod");

    svc.handle(make_request(
        Method::DELETE,
        &format!("/tags/{encoded}?tagKeys=env"),
        "",
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["tags"], json!({}));
}

#[tokio::test]
async fn update_cluster_config_persists_access_and_upgrade() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("uc")))
        .await
        .unwrap();
    // Settle to ACTIVE.
    svc.handle(make_request(Method::GET, "/clusters/uc", ""))
        .await
        .unwrap();
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/uc/update-config",
            &json!({
                "accessConfig": { "authenticationMode": "API_AND_CONFIG_MAP" },
                "upgradePolicy": { "supportType": "STANDARD" },
                "computeConfig": { "enabled": true }
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "InProgress");

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/uc", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["cluster"]["accessConfig"]["authenticationMode"],
        "API_AND_CONFIG_MAP"
    );
    assert_eq!(v["cluster"]["upgradePolicy"]["supportType"], "STANDARD");
    assert_eq!(v["cluster"]["computeConfig"]["enabled"], true);
}

#[tokio::test]
async fn tag_nodegroup_sub_resource_arn() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "tc").await;
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/tc/node-groups",
            &nodegroup_body("tng"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ng_arn = v["nodegroup"]["nodegroupArn"].as_str().unwrap().to_string();
    let encoded = ng_arn.replace('/', "%2F").replace(':', "%3A");

    // Tagging a node-group ARN must succeed (previously rejected with 400).
    svc.handle(make_request(
        Method::POST,
        &format!("/tags/{encoded}"),
        &json!({ "tags": { "team": "core" } }).to_string(),
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["tags"]["team"], "core");

    // And it is reflected on the node group itself.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/tc/node-groups/tng",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["tags"]["team"], "core");
}

#[tokio::test]
async fn delete_cluster_blocked_by_live_nodegroup() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "bc").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/bc/node-groups",
        &nodegroup_body("ng"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(Method::DELETE, "/clusters/bc", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn delete_cluster_cascades_subresources() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "cc").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/cc/node-groups",
        &nodegroup_body("ng"),
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        Method::DELETE,
        "/clusters/cc/node-groups/ng",
        "",
    ))
    .await
    .unwrap();
    svc.handle(make_request(Method::DELETE, "/clusters/cc", ""))
        .await
        .unwrap();
    // Recreate the same name: no orphaned node groups may leak through.
    create_cluster(&svc, "cc").await;
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/cc/node-groups", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroups"], json!([]));
}

#[tokio::test]
async fn describe_cluster_versions_filters_by_version_status() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/cluster-versions?versionStatus=EXTENDED_SUPPORT",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let rows = v["clusterVersions"].as_array().unwrap();
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|r| r["versionStatus"] == "EXTENDED_SUPPORT"));
}

// -----------------------------------------------------------------------
// Node groups
// -----------------------------------------------------------------------

fn nodegroup_body(name: &str) -> String {
    json!({
        "nodegroupName": name,
        "nodeRole": "arn:aws:iam::111122223333:role/eks-node",
        "subnets": ["subnet-1", "subnet-2"],
        "scalingConfig": { "minSize": 1, "maxSize": 3, "desiredSize": 2 },
        "instanceTypes": ["t3.large"],
        "labels": { "team": "core" }
    })
    .to_string()
}

async fn create_cluster(svc: &EksService, name: &str) {
    svc.handle(make_request(Method::POST, "/clusters", &create_body(name)))
        .await
        .unwrap();
}

#[tokio::test]
async fn nodegroup_create_describe_list_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["nodegroupName"], "ng1");
    assert_eq!(v["nodegroup"]["status"], "CREATING");
    assert_eq!(v["nodegroup"]["clusterName"], "c1");
    assert_eq!(v["nodegroup"]["scalingConfig"]["maxSize"], 3);
    assert!(v["nodegroup"]["nodegroupArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:nodegroup/c1/ng1/"));

    // Describe settles CREATING -> ACTIVE.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["status"], "ACTIVE");
    assert_eq!(v["nodegroup"]["labels"]["team"], "core");

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/node-groups", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroups"], json!(["ng1"]));

    let resp = svc
        .handle(make_request(
            Method::DELETE,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["status"], "DELETING");

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn nodegroup_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn nodegroup_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups",
        &nodegroup_body("ng1"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn nodegroup_cross_cluster_isolation() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    create_cluster(&svc, "c2").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups",
        &nodegroup_body("ng1"),
    ))
    .await
    .unwrap();

    // c2 sees no node groups, and can't describe c1's node group.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c2/node-groups", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroups"], json!([]));

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c2/node-groups/ng1",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn nodegroup_update_config_and_version_have_own_updates() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups",
        &nodegroup_body("ng1"),
    ))
    .await
    .unwrap();

    // UpdateNodegroupVersion returns an Update.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups/ng1/update-version",
            &json!({ "version": "1.30" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "VersionUpdate");
    let update_id = v["update"]["id"].as_str().unwrap().to_string();

    // UpdateNodegroupConfig returns another Update.
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups/ng1/update-config",
        &json!({ "scalingConfig": { "minSize": 2, "maxSize": 5, "desiredSize": 3 } }).to_string(),
    ))
    .await
    .unwrap();

    // ListUpdates with nodegroupName returns the node group's updates.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/updates?nodegroupName=ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["updateIds"].as_array().unwrap().len(), 2);

    // Cluster-scoped ListUpdates (no nodegroupName) is empty.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/updates", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["updateIds"].as_array().unwrap().len(), 0);

    // DescribeUpdate with nodegroupName settles the node group update.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/updates/{update_id}?nodegroupName=ng1"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "Successful");

    // The node group reflects the new version and scaling config.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["version"], "1.30");
    assert_eq!(v["nodegroup"]["scalingConfig"]["maxSize"], 5);
}

// -----------------------------------------------------------------------
// Fargate profiles
// -----------------------------------------------------------------------

fn fargate_body(name: &str) -> String {
    json!({
        "fargateProfileName": name,
        "podExecutionRoleArn": "arn:aws:iam::111122223333:role/eks-fargate",
        "subnets": ["subnet-1"],
        "selectors": [{ "namespace": "default", "labels": { "app": "web" } }]
    })
    .to_string()
}

#[tokio::test]
async fn fargate_create_describe_list_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/fargate-profiles",
            &fargate_body("fp1"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["fargateProfile"]["fargateProfileName"], "fp1");
    assert_eq!(v["fargateProfile"]["status"], "CREATING");
    assert!(v["fargateProfile"]["fargateProfileArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:fargateprofile/c1/fp1/"));

    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/fargate-profiles/fp1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["fargateProfile"]["status"], "ACTIVE");
    assert_eq!(v["fargateProfile"]["selectors"][0]["namespace"], "default");

    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/fargate-profiles",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["fargateProfileNames"], json!(["fp1"]));

    let resp = svc
        .handle(make_request(
            Method::DELETE,
            "/clusters/c1/fargate-profiles/fp1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["fargateProfile"]["status"], "DELETING");

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/fargate-profiles/fp1",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn fargate_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/fargate-profiles",
            &fargate_body("fp1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn fargate_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/fargate-profiles",
        &fargate_body("fp1"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/fargate-profiles",
            &fargate_body("fp1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn unimplemented_subresource_falls_through() {
    // An unknown sub-resource collection must not accidentally match via
    // the node-group/fargate-profile/addon/access-entry/idp/pod-identity
    // arms; the router falls through to UnknownOperationException.
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let err = svc
        .handle(make_request(Method::GET, "/clusters/c1/insights", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "UnknownOperationException");
}

// -----------------------------------------------------------------------
// Add-ons
// -----------------------------------------------------------------------

fn addon_body(name: &str) -> String {
    json!({
        "addonName": name,
        "addonVersion": "v1.18.3-eksbuild.2",
        "serviceAccountRoleArn": "arn:aws:iam::111122223333:role/eks-addon",
        "configurationValues": "{\"replicaCount\":2}",
        "tags": { "team": "core" }
    })
    .to_string()
}

#[tokio::test]
async fn addon_create_describe_list_update_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/addons",
            &addon_body("vpc-cni"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["addonName"], "vpc-cni");
    assert_eq!(v["addon"]["status"], "CREATING");
    assert_eq!(v["addon"]["clusterName"], "c1");
    assert_eq!(v["addon"]["addonVersion"], "v1.18.3-eksbuild.2");
    assert_eq!(v["addon"]["configurationValues"], "{\"replicaCount\":2}");
    assert!(v["addon"]["addonArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:addon/c1/vpc-cni/"));

    // Describe settles CREATING -> ACTIVE.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["status"], "ACTIVE");
    assert_eq!(v["addon"]["tags"]["team"], "core");

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/addons", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addons"], json!(["vpc-cni"]));

    // UpdateAddon mints a tracked Update, discoverable via addonName.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/addons/vpc-cni/update",
            &json!({ "addonVersion": "v1.18.5-eksbuild.1" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "AddonUpdate");
    assert_eq!(v["update"]["status"], "InProgress");
    let update_id = v["update"]["id"].as_str().unwrap().to_string();

    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/updates/{update_id}?addonName=vpc-cni"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "Successful");

    // Addon reflects the new version.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["addonVersion"], "v1.18.5-eksbuild.1");

    // ListUpdates with addonName sees the update.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/updates?addonName=vpc-cni",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["updateIds"].as_array().unwrap().len(), 1);

    // Delete.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            "/clusters/c1/addons/vpc-cni",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["status"], "DELETING");

    let err = svc
        .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn addon_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/addons",
            &addon_body("coredns"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn addon_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/addons",
        &addon_body("coredns"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/addons",
            &addon_body("coredns"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn describe_addon_versions_catalog_is_non_empty() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(Method::GET, "/addons/supported-versions", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let addons = v["addons"].as_array().unwrap();
    assert!(addons.len() >= 5);
    let names: Vec<&str> = addons
        .iter()
        .map(|a| a["addonName"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"vpc-cni"));
    assert!(names.contains(&"coredns"));
    assert!(!addons[0]["addonVersions"].as_array().unwrap().is_empty());

    // Filtered by addonName.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/addons/supported-versions?addonName=coredns",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let addons = v["addons"].as_array().unwrap();
    assert_eq!(addons.len(), 1);
    assert_eq!(addons[0]["addonName"], "coredns");
}

#[tokio::test]
async fn describe_addon_configuration_returns_schema() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/addons/configuration-schemas?addonName=vpc-cni&addonVersion=v1.18.3-eksbuild.2",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addonName"], "vpc-cni");
    assert_eq!(v["addonVersion"], "v1.18.3-eksbuild.2");
    assert!(v["configurationSchema"]
        .as_str()
        .unwrap()
        .contains("$schema"));
    assert_eq!(
        v["podIdentityConfiguration"][0]["serviceAccount"],
        "aws-node"
    );
}

// -----------------------------------------------------------------------
// Access entries / access policies
// -----------------------------------------------------------------------

const PRINCIPAL: &str = "arn:aws:iam::111122223333:role/dev";

fn access_entry_body() -> String {
    json!({
        "principalArn": PRINCIPAL,
        "kubernetesGroups": ["viewers"],
        "tags": { "team": "core" }
    })
    .to_string()
}

fn url_encode(s: &str) -> String {
    s.replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
}

fn encoded_principal() -> String {
    url_encode(PRINCIPAL)
}

#[tokio::test]
async fn access_entry_full_lifecycle() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    // Create.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/access-entries",
            &access_entry_body(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["accessEntry"]["principalArn"], PRINCIPAL);
    assert_eq!(v["accessEntry"]["clusterName"], "c1");
    assert_eq!(v["accessEntry"]["type"], "STANDARD");
    assert_eq!(v["accessEntry"]["kubernetesGroups"], json!(["viewers"]));
    assert_eq!(v["accessEntry"]["tags"]["team"], "core");
    assert!(v["accessEntry"]["accessEntryArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:access-entry/c1/role/"));
    assert!(v["accessEntry"]["username"]
        .as_str()
        .unwrap()
        .contains("dev"));

    let enc = encoded_principal();

    // Describe.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries/{enc}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["accessEntry"]["principalArn"], PRINCIPAL);

    // List.
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/access-entries", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["accessEntries"], json!([PRINCIPAL]));

    // Update.
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/access-entries/{enc}"),
            &json!({ "kubernetesGroups": ["admins"], "username": "custom" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["accessEntry"]["kubernetesGroups"], json!(["admins"]));
    assert_eq!(v["accessEntry"]["username"], "custom");

    // Associate an access policy.
    let policy = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy";
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/access-entries/{enc}/access-policies"),
            &json!({
                "policyArn": policy,
                "accessScope": { "type": "namespace", "namespaces": ["default"] }
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["clusterName"], "c1");
    assert_eq!(v["principalArn"], PRINCIPAL);
    assert_eq!(v["associatedAccessPolicy"]["policyArn"], policy);
    assert_eq!(
        v["associatedAccessPolicy"]["accessScope"]["type"],
        "namespace"
    );

    // List associated policies.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries/{enc}/access-policies"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["clusterName"], "c1");
    assert_eq!(v["associatedAccessPolicies"].as_array().unwrap().len(), 1);
    assert_eq!(v["associatedAccessPolicies"][0]["policyArn"], policy);

    // ListAccessEntries with associatedPolicyArn filter finds the entry.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries?associatedPolicyArn={policy}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["accessEntries"], json!([PRINCIPAL]));

    // Disassociate.
    let enc_policy = encoded_principal_of(policy);
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/clusters/c1/access-entries/{enc}/access-policies/{enc_policy}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v, json!({}));

    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries/{enc}/access-policies"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["associatedAccessPolicies"].as_array().unwrap().len(), 0);

    // Delete the entry.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/clusters/c1/access-entries/{enc}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v, json!({}));

    let err = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries/{enc}"),
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

fn encoded_principal_of(s: &str) -> String {
    url_encode(s)
}

#[tokio::test]
async fn access_entry_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/access-entries",
            &access_entry_body(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn access_entry_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/access-entries",
        &access_entry_body(),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/access-entries",
            &access_entry_body(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn describe_access_entry_missing_is_not_found() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let err = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/access-entries/{}", encoded_principal()),
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn list_access_policies_catalog_is_non_empty() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(Method::GET, "/access-policies", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policies = v["accessPolicies"].as_array().unwrap();
    assert!(policies.len() >= 10);
    let names: Vec<&str> = policies
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"AmazonEKSClusterAdminPolicy"));
    assert!(names.contains(&"AmazonEKSViewPolicy"));
    assert!(policies[0]["arn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks::aws:cluster-access-policy/"));
}

// -----------------------------------------------------------------------
// Identity-provider configs
// -----------------------------------------------------------------------

fn idp_body(name: &str) -> String {
    json!({
        "oidc": {
            "identityProviderConfigName": name,
            "issuerUrl": "https://example.com",
            "clientId": "kubernetes",
            "usernameClaim": "email",
            "groupsClaim": "groups"
        },
        "tags": { "team": "core" }
    })
    .to_string()
}

#[tokio::test]
async fn idp_associate_describe_list_disassociate() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    // Associate mints a tracked cluster-scoped Update and echoes tags.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/associate",
            &idp_body("oidc1"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "AssociateIdentityProviderConfig");
    assert_eq!(v["update"]["status"], "InProgress");
    assert_eq!(v["tags"]["team"], "core");
    let update_id = v["update"]["id"].as_str().unwrap().to_string();

    // The Update is cluster-scoped and settles on DescribeUpdate.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/updates/{update_id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "Successful");

    // Describe returns the OIDC config and settles CREATING -> ACTIVE.
    let describe =
        json!({ "identityProviderConfig": { "type": "oidc", "name": "oidc1" } }).to_string();
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/describe",
            &describe,
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let oidc = &v["identityProviderConfig"]["oidc"];
    assert_eq!(oidc["identityProviderConfigName"], "oidc1");
    assert_eq!(oidc["issuerUrl"], "https://example.com");
    assert_eq!(oidc["clientId"], "kubernetes");
    assert_eq!(oidc["usernameClaim"], "email");
    assert_eq!(oidc["status"], "ACTIVE");
    assert!(oidc["identityProviderConfigArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:identityproviderconfig/c1/oidc/oidc1/"));

    // List returns the {type, name} summary.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/identity-provider-configs",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["identityProviderConfigs"],
        json!([{ "type": "oidc", "name": "oidc1" }])
    );

    // Disassociate mints another tracked Update and removes the config.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/disassociate",
            &describe,
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "DisassociateIdentityProviderConfig");

    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/identity-provider-configs",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["identityProviderConfigs"], json!([]));

    // Describe of the removed config is not found.
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/describe",
            &describe,
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn idp_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/identity-provider-configs/associate",
            &idp_body("oidc1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn idp_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/identity-provider-configs/associate",
        &idp_body("oidc1"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/associate",
            &idp_body("oidc1"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

// -----------------------------------------------------------------------
// Pod identity associations
// -----------------------------------------------------------------------

fn pod_identity_body(namespace: &str, sa: &str) -> String {
    json!({
        "namespace": namespace,
        "serviceAccount": sa,
        "roleArn": "arn:aws:iam::111122223333:role/pod-role",
        "tags": { "team": "core" }
    })
    .to_string()
}

#[tokio::test]
async fn pod_identity_create_describe_list_update_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/pod-identity-associations",
            &pod_identity_body("default", "app"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let assoc = &v["association"];
    assert_eq!(assoc["clusterName"], "c1");
    assert_eq!(assoc["namespace"], "default");
    assert_eq!(assoc["serviceAccount"], "app");
    assert_eq!(assoc["roleArn"], "arn:aws:iam::111122223333:role/pod-role");
    assert_eq!(assoc["disableSessionTags"], false);
    assert_eq!(assoc["tags"]["team"], "core");
    // No targetRoleArn was supplied, so no externalId is returned.
    assert!(assoc.get("externalId").is_none());
    let association_id = assoc["associationId"].as_str().unwrap().to_string();
    assert!(association_id.starts_with("a-"));
    assert!(assoc["associationArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:podidentityassociation/c1/a-"));

    // Describe round-trips.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/pod-identity-associations/{association_id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["association"]["serviceAccount"], "app");

    // List returns a summary, filterable by namespace.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/pod-identity-associations?namespace=default",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["associations"].as_array().unwrap().len(), 1);
    assert_eq!(v["associations"][0]["associationId"], association_id);

    // A non-matching namespace filter returns nothing.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/pod-identity-associations?namespace=other",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["associations"], json!([]));

    // Update upserts the role and cross-account target (minting externalId).
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/pod-identity-associations/{association_id}"),
            &json!({
                "roleArn": "arn:aws:iam::111122223333:role/new-role",
                "targetRoleArn": "arn:aws:iam::444455556666:role/target",
                "disableSessionTags": true
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["association"]["roleArn"],
        "arn:aws:iam::111122223333:role/new-role"
    );
    assert_eq!(
        v["association"]["targetRoleArn"],
        "arn:aws:iam::444455556666:role/target"
    );
    assert_eq!(v["association"]["disableSessionTags"], true);
    assert!(v["association"]["externalId"].is_string());

    // Delete returns the association and then it's gone.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/clusters/c1/pod-identity-associations/{association_id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["association"]["associationId"], association_id);

    let err = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/pod-identity-associations/{association_id}"),
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn pod_identity_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/pod-identity-associations",
            &pod_identity_body("default", "app"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn pod_identity_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/pod-identity-associations",
        &pod_identity_body("default", "app"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/pod-identity-associations",
            &pod_identity_body("default", "app"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

// -----------------------------------------------------------------------
// Insights
// -----------------------------------------------------------------------

#[tokio::test]
async fn insights_list_describe_and_refresh() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    // ListInsights seeds and returns a non-empty, real catalogue.
    let resp = svc
        .handle(make_request(Method::POST, "/clusters/c1/insights", "{}"))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let insights = v["insights"].as_array().unwrap();
    assert!(!insights.is_empty());
    assert_eq!(insights[0]["insightStatus"]["status"], "PASSING");
    let id = insights[0]["id"].as_str().unwrap().to_string();

    // DescribeInsight round-trips by id.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/insights/{id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["insight"]["id"], id);
    assert_eq!(v["insight"]["category"], "UPGRADE_READINESS");

    // StartInsightsRefresh returns only message + status.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/insights-refresh",
            "{}",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["status"], "IN_PROGRESS");
    assert!(v.get("startedAt").is_none());

    // DescribeInsightsRefresh settles IN_PROGRESS -> COMPLETED.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/insights-refresh",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["status"], "COMPLETED");
    assert!(v.get("endedAt").is_some());
}

#[tokio::test]
async fn insights_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(Method::POST, "/clusters/ghost/insights", "{}"))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn describe_insight_missing_is_not_found() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let err = svc
        .handle(make_request(Method::GET, "/clusters/c1/insights/ghost", ""))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

// -----------------------------------------------------------------------
// Encryption config / cancel update / register / cluster versions
// -----------------------------------------------------------------------

#[tokio::test]
async fn associate_encryption_config_mints_update() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let body = json!({
        "encryptionConfig": [{
            "resources": ["secrets"],
            "provider": { "keyArn": "arn:aws:kms:us-east-1:111122223333:key/abc" }
        }]
    })
    .to_string();
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/encryption-config/associate",
            &body,
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "AssociateEncryptionConfig");
    assert_eq!(v["update"]["status"], "InProgress");
    let update_id = v["update"]["id"].as_str().unwrap().to_string();

    // The update is a cluster update discoverable via DescribeUpdate, and
    // CancelUpdate marks it Cancelled.
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/updates/{update_id}/cancel-update"),
            "{}",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["status"], "Cancelled");
}

#[tokio::test]
async fn encryption_config_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/encryption-config/associate",
            &json!({ "encryptionConfig": [] }).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn register_and_deregister_cluster() {
    let svc = EksService::new(make_state());
    let body = json!({
        "name": "connected1",
        "connectorConfig": {
            "roleArn": "arn:aws:iam::111122223333:role/eks-connector",
            "provider": "EKS_ANYWHERE"
        }
    })
    .to_string();
    let resp = svc
        .handle(make_request(Method::POST, "/cluster-registrations", &body))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["name"], "connected1");
    assert_eq!(v["cluster"]["status"], "PENDING");
    assert_eq!(v["cluster"]["connectorConfig"]["provider"], "EKS_ANYWHERE");
    assert!(v["cluster"]["connectorConfig"]["activationId"].is_string());

    // Duplicate registration conflicts.
    let err = svc
        .handle(make_request(Method::POST, "/cluster-registrations", &body))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceInUseException");

    // Deregister removes the connected cluster.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            "/cluster-registrations/connected1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["cluster"]["status"], "DELETING");

    let err = svc
        .handle(make_request(
            Method::DELETE,
            "/cluster-registrations/connected1",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn deregister_non_connected_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "regular").await;
    // A regular (non-registered) cluster can't be deregistered.
    let err = svc
        .handle(make_request(
            Method::DELETE,
            "/cluster-registrations/regular",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn describe_cluster_versions_catalog_is_non_empty() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(Method::GET, "/cluster-versions", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let versions = v["clusterVersions"].as_array().unwrap();
    assert!(versions.len() >= 5);
    let names: Vec<&str> = versions
        .iter()
        .map(|x| x["clusterVersion"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"1.31"));
    // Exactly one default version.
    assert_eq!(
        versions
            .iter()
            .filter(|x| x["defaultVersion"] == true)
            .count(),
        1
    );
    // AWS reports the default version first (index 0).
    assert_eq!(versions[0]["defaultVersion"], true);

    // defaultOnly filter narrows to the single default.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/cluster-versions?defaultOnly=true",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["clusterVersions"].as_array().unwrap().len(), 1);
    assert_eq!(v["clusterVersions"][0]["clusterVersion"], "1.31");
}

#[tokio::test]
async fn describe_cluster_versions_rejects_bad_maxresults() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::GET,
            "/cluster-versions?maxResults=0",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "InvalidParameterException");
}

// -----------------------------------------------------------------------
// Capabilities
// -----------------------------------------------------------------------

fn capability_body(name: &str) -> String {
    json!({
        "capabilityName": name,
        "type": "ACK",
        "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
        "deletePropagationPolicy": "RETAIN",
        "tags": { "team": "core" }
    })
    .to_string()
}

#[tokio::test]
async fn capability_create_describe_list_update_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities",
            &capability_body("ack"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["capability"]["capabilityName"], "ack");
    assert_eq!(v["capability"]["status"], "CREATING");
    assert_eq!(v["capability"]["type"], "ACK");
    assert!(v["capability"]["arn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:capability/c1/ack/"));

    // Describe settles CREATING -> ACTIVE.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/capabilities/ack",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["capability"]["status"], "ACTIVE");
    assert_eq!(v["capability"]["tags"]["team"], "core");

    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c1/capabilities", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["capabilities"].as_array().unwrap().len(), 1);
    assert_eq!(v["capabilities"][0]["capabilityName"], "ack");

    // UpdateCapability mints a tracked cluster Update.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities/ack",
            &json!({ "roleArn": "arn:aws:iam::111122223333:role/new" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "CapabilityUpdate");

    // Delete.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            "/clusters/c1/capabilities/ack",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["capability"]["status"], "DELETING");

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/capabilities/ack",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn capability_on_missing_cluster_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ghost/capabilities",
            &capability_body("ack"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn capability_duplicate_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/capabilities",
        &capability_body("ack"),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities",
            &capability_body("ack"),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

// -----------------------------------------------------------------------
// EKS Anywhere subscriptions
// -----------------------------------------------------------------------

fn subscription_body(name: &str) -> String {
    json!({
        "name": name,
        "term": { "duration": 12, "unit": "MONTHS" },
        "licenseQuantity": 5,
        "licenseType": "Cluster",
        "autoRenew": false,
        "tags": { "team": "core" }
    })
    .to_string()
}

#[tokio::test]
async fn subscription_create_describe_list_update_delete() {
    let svc = EksService::new(make_state());

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/eks-anywhere-subscriptions",
            &subscription_body("sub1"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let sub = &v["subscription"];
    assert_eq!(sub["status"], "ACTIVE");
    assert_eq!(sub["licenseQuantity"], 5);
    assert_eq!(sub["term"]["duration"], 12);
    assert_eq!(sub["autoRenew"], false);
    let id = sub["id"].as_str().unwrap().to_string();
    assert!(sub["arn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:eks:us-east-1:111122223333:eks-anywhere-subscription/"));

    // Describe round-trips.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/eks-anywhere-subscriptions/{id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["subscription"]["id"], id);

    // List returns the subscription.
    let resp = svc
        .handle(make_request(Method::GET, "/eks-anywhere-subscriptions", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["subscriptions"].as_array().unwrap().len(), 1);

    // Update toggles autoRenew.
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/eks-anywhere-subscriptions/{id}"),
            &json!({ "autoRenew": true }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["subscription"]["autoRenew"], true);

    // Delete.
    let resp = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/eks-anywhere-subscriptions/{id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["subscription"]["status"], "DELETING");

    let err = svc
        .handle(make_request(
            Method::GET,
            &format!("/eks-anywhere-subscriptions/{id}"),
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn subscription_describe_missing_is_not_found() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::GET,
            "/eks-anywhere-subscriptions/ghost",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn subscription_rejects_bad_name() {
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/eks-anywhere-subscriptions",
            &json!({ "name": "-bad", "term": { "duration": 1, "unit": "MONTHS" } }).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidParameterException");
}

#[tokio::test]
async fn subscription_rejects_overflow_duration() {
    // Regression: a hostile `term.duration` used to overflow the
    // `DateTime + Duration` expiration arithmetic and panic the handler.
    let svc = EksService::new(make_state());
    let err = svc
        .handle(make_request(
            Method::POST,
            "/eks-anywhere-subscriptions",
            &json!({ "name": "sub", "term": { "duration": 100_000_000_i64, "unit": "MONTHS" } })
                .to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "InvalidParameterException");
}

#[tokio::test]
async fn subscription_accepts_valid_duration() {
    let svc = EksService::new(make_state());
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/eks-anywhere-subscriptions",
            &json!({ "name": "sub-ok", "term": { "duration": 36, "unit": "MONTHS" } }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["subscription"]["term"]["duration"], 36);
    assert_eq!(v["subscription"]["status"], "ACTIVE");
}

#[tokio::test]
async fn update_nodegroup_config_applies_taints_and_removes_labels() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(Method::POST, "/clusters", &create_body("ngc")))
        .await
        .unwrap();
    svc.handle(make_request(
        Method::POST,
        "/clusters/ngc/node-groups",
        &json!({
            "nodegroupName": "ng1",
            "nodeRole": "arn:aws:iam::111122223333:role/eks-node",
            "subnets": ["subnet-1"],
            "labels": { "team": "core", "tier": "gold" },
            "taints": [ { "key": "dedicated", "value": "gpu", "effect": "NO_SCHEDULE" } ]
        })
        .to_string(),
    ))
    .await
    .unwrap();

    // Update: add/update a taint, remove a taint that doesn't exist yet is a
    // no-op; also add a label and remove one.
    svc.handle(make_request(
        Method::POST,
        "/clusters/ngc/node-groups/ng1/update-config",
        &json!({
            "labels": {
                "addOrUpdateLabels": { "env": "prod" },
                "removeLabels": ["tier"]
            },
            "taints": {
                "addOrUpdateTaints": [
                    { "key": "dedicated", "value": "tpu", "effect": "NO_SCHEDULE" },
                    { "key": "spot", "value": "true", "effect": "PREFER_NO_SCHEDULE" }
                ],
                "removeTaints": []
            }
        })
        .to_string(),
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/ngc/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // Labels: env added, tier removed, team + core unchanged.
    assert_eq!(v["nodegroup"]["labels"]["env"], "prod");
    assert_eq!(v["nodegroup"]["labels"]["team"], "core");
    assert!(v["nodegroup"]["labels"].get("tier").is_none());
    // Taints: dedicated updated in place to tpu, spot added.
    let taints = v["nodegroup"]["taints"].as_array().unwrap();
    assert_eq!(taints.len(), 2);
    let dedicated = taints
        .iter()
        .find(|t| t["key"] == "dedicated")
        .expect("dedicated taint");
    assert_eq!(dedicated["value"], "tpu");
    assert!(taints.iter().any(|t| t["key"] == "spot"));

    // Now remove the dedicated taint.
    svc.handle(make_request(
        Method::POST,
        "/clusters/ngc/node-groups/ng1/update-config",
        &json!({
            "taints": { "removeTaints": [ { "key": "dedicated", "effect": "NO_SCHEDULE" } ] }
        })
        .to_string(),
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/ngc/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let taints = v["nodegroup"]["taints"].as_array().unwrap();
    assert_eq!(taints.len(), 1);
    assert_eq!(taints[0]["key"], "spot");
}

#[tokio::test]
async fn create_cluster_stores_encryption_config() {
    let svc = EksService::new(make_state());
    let body = json!({
        "name": "enc",
        "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
        "resourcesVpcConfig": { "subnetIds": ["subnet-1"] },
        "encryptionConfig": [
            {
                "resources": ["secrets"],
                "provider": { "keyArn": "arn:aws:kms:us-east-1:111122223333:key/abc" }
            }
        ]
    })
    .to_string();
    svc.handle(make_request(Method::POST, "/clusters", &body))
        .await
        .unwrap();
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/enc", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ec = &v["cluster"]["encryptionConfig"][0];
    assert_eq!(ec["resources"][0], "secrets");
    assert_eq!(
        ec["provider"]["keyArn"],
        "arn:aws:kms:us-east-1:111122223333:key/abc"
    );
}

#[tokio::test]
async fn nodegroup_round_trips_node_repair_and_warm_pool_config() {
    let svc = EksService::new(make_state());
    svc.handle(make_request(
        Method::POST,
        "/clusters",
        &create_body("ng-cl"),
    ))
    .await
    .unwrap();

    let create = json!({
        "nodegroupName": "ng1",
        "nodeRole": "arn:aws:iam::111122223333:role/eks-node",
        "subnets": ["subnet-1", "subnet-2"],
        "nodeRepairConfig": { "enabled": true },
        "warmPoolConfig": { "enabled": true, "minSize": 1, "maxGroupPreparedCapacity": 3 },
    })
    .to_string();
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/ng-cl/node-groups",
            &create,
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["nodeRepairConfig"]["enabled"], true);
    assert_eq!(v["nodegroup"]["warmPoolConfig"]["minSize"], 1);

    // Describe echoes both configs back.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/ng-cl/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["nodeRepairConfig"]["enabled"], true);
    assert_eq!(
        v["nodegroup"]["warmPoolConfig"]["maxGroupPreparedCapacity"],
        3
    );

    // UpdateNodegroupConfig toggling nodeRepairConfig is reflected on Describe.
    svc.handle(make_request(
        Method::POST,
        "/clusters/ng-cl/node-groups/ng1/update-config",
        &json!({ "nodeRepairConfig": { "enabled": false } }).to_string(),
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/ng-cl/node-groups/ng1",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["nodegroup"]["nodeRepairConfig"]["enabled"], false);
}
