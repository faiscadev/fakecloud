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
    // A real build of each add-on offered on the default (1.31) cluster.
    let version = match name {
        "coredns" => "v1.11.4-eksbuild.24",
        _ => "v1.18.3-eksbuild.2",
    };
    json!({
        "addonName": name,
        "addonVersion": version,
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

async fn addon_versions(svc: &EksService, query: &str) -> Vec<Value> {
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/addons/supported-versions?{query}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    v["addons"].as_array().unwrap().clone()
}

/// The version flagged `defaultVersion` for `cluster_version`, the way the
/// Terraform `aws_eks_addon_version` data source picks it.
fn default_version_of(addon: &Value, cluster_version: &str) -> Option<String> {
    addon["addonVersions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ver| {
            ver["compatibilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["clusterVersion"] == cluster_version && c["defaultVersion"] == true)
        })
        .map(|ver| ver["addonVersion"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn describe_addon_versions_lists_pod_identity_agent_for_cluster_version() {
    let svc = EksService::new(make_state());
    let addons = addon_versions(
        &svc,
        "addonName=eks-pod-identity-agent&kubernetesVersion=1.32",
    )
    .await;
    assert_eq!(addons.len(), 1);
    let a = &addons[0];
    assert_eq!(a["type"], "security");
    assert_eq!(a["owner"], "aws");
    assert_eq!(a["publisher"], "eks");
    assert_eq!(a["defaultNamespace"], "kube-system");
    assert!(!a["addonVersions"].as_array().unwrap().is_empty());
    // Every compatibility is scoped to the requested cluster version.
    for ver in a["addonVersions"].as_array().unwrap() {
        for c in ver["compatibilities"].as_array().unwrap() {
            assert_eq!(c["clusterVersion"], "1.32");
        }
    }
    assert_eq!(
        default_version_of(a, "1.32").as_deref(),
        Some("v1.3.10-eksbuild.1")
    );
}

#[tokio::test]
async fn describe_addon_versions_catalog_covers_aws_and_community_addons() {
    let svc = EksService::new(make_state());
    let addons = addon_versions(&svc, "kubernetesVersion=1.31").await;
    let names: Vec<&str> = addons
        .iter()
        .map(|a| a["addonName"].as_str().unwrap())
        .collect();
    for want in [
        "eks-pod-identity-agent",
        "metrics-server",
        "snapshot-controller",
        "amazon-cloudwatch-observability",
        "aws-guardduty-agent",
        "aws-mountpoint-s3-csi-driver",
        "adot",
        "eks-node-monitoring-agent",
        "aws-network-flow-monitoring-agent",
    ] {
        assert!(names.contains(&want), "missing {want}");
    }
    // Every add-on has exactly one default version for the cluster version.
    for a in &addons {
        let defaults = a["addonVersions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|ver| ver["compatibilities"][0]["defaultVersion"] == true)
            .count();
        assert_eq!(defaults, 1, "{}", a["addonName"]);
    }

    // `owners` / `types` filters.
    let community = addon_versions(&svc, "kubernetesVersion=1.31&owners=community").await;
    assert!(!community.is_empty());
    assert!(community.iter().all(|a| a["owner"] == "community"));
    let security = addon_versions(&svc, "types=security").await;
    assert!(security
        .iter()
        .any(|a| a["addonName"] == "eks-pod-identity-agent"));
    assert!(security.iter().all(|a| a["type"] == "security"));
    // Repeated list keys OR together.
    let mixed = addon_versions(&svc, "types=security&types=storage").await;
    assert!(mixed.iter().any(|a| a["type"] == "security"));
    assert!(mixed.iter().any(|a| a["type"] == "storage"));
    assert!(mixed
        .iter()
        .all(|a| a["type"] == "security" || a["type"] == "storage"));

    // A Kubernetes version EKS does not offer has no add-ons.
    assert!(addon_versions(&svc, "kubernetesVersion=1.99")
        .await
        .is_empty());
}

#[tokio::test]
async fn describe_addon_versions_tracks_kubernetes_minor() {
    let svc = EksService::new(make_state());
    let kp = addon_versions(&svc, "addonName=kube-proxy&kubernetesVersion=1.33").await;
    assert_eq!(
        default_version_of(&kp[0], "1.33").as_deref(),
        Some("v1.33.3-eksbuild.4")
    );
    // Without a kubernetesVersion every catalog cluster version is covered.
    let all = addon_versions(&svc, "addonName=coredns").await;
    let compat_versions: Vec<String> = all[0]["addonVersions"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|ver| ver["compatibilities"].as_array().unwrap().clone())
        .map(|c| c["clusterVersion"].as_str().unwrap().to_string())
        .collect();
    for cv in ["1.28", "1.31", "1.34"] {
        assert!(compat_versions.iter().any(|v| v == cv), "missing {cv}");
    }
    assert_eq!(
        default_version_of(&all[0], "1.34").as_deref(),
        Some("v1.12.3-eksbuild.1")
    );
}

#[tokio::test]
async fn create_addon_defaults_to_catalog_default_version() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/addons",
            &json!({ "addonName": "eks-pod-identity-agent" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["addonVersion"], "v1.3.10-eksbuild.1");
}

async fn create_cluster_on(svc: &EksService, name: &str, version: &str) {
    let mut body: Value = serde_json::from_str(&create_body(name)).unwrap();
    body["version"] = json!(version);
    svc.handle(make_request(Method::POST, "/clusters", &body.to_string()))
        .await
        .unwrap();
}

#[tokio::test]
async fn create_addon_refuses_addon_without_a_build_for_the_cluster_version() {
    let svc = EksService::new(make_state());
    create_cluster_on(&svc, "c134", "1.34").await;
    // adot has no 1.34 build: no fallback to another minor's default.
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c134/addons",
            &json!({ "addonName": "adot" }).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "InvalidParameterException");
    assert_eq!(
        err.message(),
        "Addon adot specified is not supported in 1.34 kubernetes version"
    );
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/c134/addons", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addons"], json!([]));

    // The default for a non-default minor is that minor's own default.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c134/addons",
            &json!({ "addonName": "kube-proxy" }).to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["addonVersion"], "v1.34.0-eksbuild.2");
}

#[tokio::test]
async fn create_and_update_addon_refuse_versions_for_another_minor() {
    // kube-proxy builds run on their own minor and newer ones, never older.
    let svc = EksService::new(make_state());
    create_cluster_on(&svc, "c133", "1.33").await;
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c133/addons",
            &json!({ "addonName": "kube-proxy", "addonVersion": "v1.34.1-eksbuild.2" }).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidParameterException");
    assert_eq!(err.message(), "Addon version specified is not supported");

    svc.handle(make_request(
        Method::POST,
        "/clusters/c133/addons",
        &json!({ "addonName": "kube-proxy", "addonVersion": "v1.33.3-eksbuild.10" }).to_string(),
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c133/addons/kube-proxy/update",
            &json!({ "addonVersion": "v1.34.1-eksbuild.2" }).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidParameterException");
    assert_eq!(err.message(), "Addon version specified is not supported");
    // The refused update changed nothing.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c133/updates?addonName=kube-proxy",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["updateIds"], json!([]));
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c133/addons/kube-proxy",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["addon"]["addonVersion"], "v1.33.3-eksbuild.10");

    // A compatible version goes through.
    svc.handle(make_request(
        Method::POST,
        "/clusters/c133/addons/kube-proxy/update",
        &json!({ "addonVersion": "v1.33.5-eksbuild.2" }).to_string(),
    ))
    .await
    .unwrap();
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
    // AWS lists newest first and leaves the default version in place; the
    // flagged default is the version CreateCluster falls back to.
    assert_eq!(
        names,
        vec!["1.34", "1.33", "1.32", "1.31", "1.30", "1.29", "1.28"]
    );
    let flagged: Vec<&str> = versions
        .iter()
        .filter(|x| x["defaultVersion"] == true)
        .map(|x| x["clusterVersion"].as_str().unwrap())
        .collect();
    assert_eq!(flagged, vec![DEFAULT_K8S_VERSION]);

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
    assert_eq!(
        v["clusterVersions"][0]["clusterVersion"],
        DEFAULT_K8S_VERSION
    );
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

async fn cluster_versions(svc: &EksService, query: &str) -> Vec<Value> {
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/cluster-versions?{query}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    v["clusterVersions"].as_array().unwrap().clone()
}

fn row<'a>(rows: &'a [Value], version: &str) -> &'a Value {
    rows.iter()
        .find(|r| r["clusterVersion"] == version)
        .unwrap_or_else(|| panic!("missing {version}"))
}

#[tokio::test]
async fn describe_cluster_versions_reports_aws_dates_and_status() {
    let svc = EksService::new(make_state());
    let rows = cluster_versions(&svc, "").await;
    // AWS sends the `VersionStatus` value in the deprecated `status` member
    // too, not the model's hyphenated `ClusterVersionStatus` values.
    for r in &rows {
        assert_eq!(r["status"], r["versionStatus"], "{}", r["clusterVersion"]);
    }
    // 2025-10-02 / 2026-12-02 / 2027-12-02 at midnight UTC.
    let v134 = row(&rows, "1.34");
    assert_eq!(v134["releaseDate"], json!(1759363200.0));
    assert_eq!(v134["endOfStandardSupportDate"], json!(1796169600.0));
    assert_eq!(v134["endOfExtendedSupportDate"], json!(1827705600.0));
    assert_eq!(v134["kubernetesPatchVersion"], "1.34.1");
    // 2025-05-29 / 2026-07-29 / 2027-07-29.
    let v133 = row(&rows, "1.33");
    assert_eq!(v133["releaseDate"], json!(1748476800.0));
    assert_eq!(v133["endOfStandardSupportDate"], json!(1785283200.0));
    assert_eq!(v133["endOfExtendedSupportDate"], json!(1816819200.0));
    // 1.30 left standard support on 2025-07-23.
    assert_eq!(row(&rows, "1.30")["status"], "EXTENDED_SUPPORT");
    assert_eq!(row(&rows, "1.31")["status"], "STANDARD_SUPPORT");
}

async fn cluster_versions_err(svc: &EksService, query: &str) -> AwsServiceError {
    svc.handle(make_request(
        Method::GET,
        &format!("/cluster-versions?{query}"),
        "",
    ))
    .await
    .err()
    .unwrap()
}

#[tokio::test]
async fn describe_cluster_versions_status_filter_takes_version_status_values() {
    let svc = EksService::new(make_state());
    let names = |rows: &[Value]| -> Vec<String> {
        rows.iter()
            .map(|r| r["clusterVersion"].as_str().unwrap().to_string())
            .collect()
    };
    let extended = cluster_versions(&svc, "status=EXTENDED_SUPPORT").await;
    assert_eq!(names(&extended), vec!["1.30", "1.29", "1.28"]);
    let standard = cluster_versions(&svc, "status=STANDARD_SUPPORT").await;
    assert_eq!(names(&standard), vec!["1.34", "1.33", "1.32", "1.31"]);
    // `versionStatus` wins when both are given.
    let both = cluster_versions(
        &svc,
        "status=EXTENDED_SUPPORT&versionStatus=STANDARD_SUPPORT",
    )
    .await;
    assert_eq!(names(&both), vec!["1.34", "1.33", "1.32", "1.31"]);
    // The model's hyphenated enum values and the member names are refused,
    // with the same wording for either parameter.
    for q in [
        "status=extended-support",
        "status=standard_support",
        "versionStatus=extended-support",
    ] {
        let err = cluster_versions_err(&svc, q).await;
        assert_eq!(err.code(), "InvalidParameterException", "{q}");
        let value = q.split('=').nth(1).unwrap();
        assert_eq!(
            err.message(),
            format!("Provided Cluster Status is invalid: {value}"),
            "{q}"
        );
    }
}

#[tokio::test]
async fn describe_cluster_versions_named_versions_are_ascending() {
    let svc = EksService::new(make_state());
    // Explicit versions come back ascending regardless of request order.
    let rows = cluster_versions(&svc, "clusterVersions=1.34&clusterVersions=1.31").await;
    let names: Vec<&str> = rows
        .iter()
        .map(|r| r["clusterVersion"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["1.31", "1.34"]);
}

#[tokio::test]
async fn describe_cluster_versions_rejects_unknown_cluster_type() {
    let svc = EksService::new(make_state());
    for q in ["clusterType=foo", "clusterType=eks-local-outposts"] {
        let err = cluster_versions_err(&svc, q).await;
        assert_eq!(err.code(), "InvalidParameterException");
        assert_eq!(err.message(), "Provided Cluster Type is invalid");
    }
    let err = cluster_versions_err(&svc, "maxResults=101").await;
    assert_eq!(err.message(), "MaxResults must be between 0 to 100");
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
async fn argocd_endpoint_prefix_round_trips_and_survives_update() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let viewer = json!({"id": "u-1", "type": "SSO_USER"});
    let editor = json!({"id": "g-1", "type": "SSO_GROUP"});
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities",
            &json!({
                "capabilityName": "argo",
                "type": "ARGOCD",
                "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
                "deletePropagationPolicy": "RETAIN",
                "configuration": {"argoCd": {
                    "namespace": "argocd",
                    "endpointPrefix": "my-argo",
                    "rbacRoleMappings": [{"role": "VIEWER", "identities": [viewer]}]
                }}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["capability"]["configuration"]["argoCd"]["endpointPrefix"],
        "my-argo"
    );

    // UpdateArgoCdConfig carries only rbac deltas + networkAccess; the
    // create-time endpointPrefix and namespace must survive it.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities/argo",
            &json!({"configuration": {"argoCd": {
                "networkAccess": {"vpceIds": ["vpce-0123456789abcdef0"]},
                "rbacRoleMappings": {
                    "addOrUpdateRoleMappings": [{"role": "EDITOR", "identities": [editor]}],
                    "removeRoleMappings": [{"role": "VIEWER", "identities": [viewer]}]
                }
            }}})
            .to_string(),
        ))
        .await
        .unwrap();
    // Each ArgoCD field reports its own UpdateParamType.
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let params: Vec<&str> = v["update"]["params"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        params,
        [
            "RoleMappingsToAddOrUpdate",
            "RoleMappingsToRemove",
            "NetworkAccess"
        ]
    );
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/capabilities/argo",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let argo = &v["capability"]["configuration"]["argoCd"];
    assert_eq!(argo["endpointPrefix"], "my-argo");
    assert_eq!(argo["namespace"], "argocd");
    assert_eq!(
        argo["networkAccess"]["vpceIds"][0],
        "vpce-0123456789abcdef0"
    );
    assert_eq!(
        argo["rbacRoleMappings"],
        json!([{"role": "EDITOR", "identities": [editor]}])
    );
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

// -----------------------------------------------------------------------
// Certificate authorities
// -----------------------------------------------------------------------

async fn list_cas(svc: &EksService, cluster: &str) -> Value {
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/{cluster}/certificate-authorities"),
            "",
        ))
        .await
        .unwrap();
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[tokio::test]
async fn cluster_create_seeds_an_eks_signed_certificate_authority() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let v = list_cas(&svc, "c1").await;
    let cas = v["certificateAuthorities"].as_array().unwrap();
    assert_eq!(cas.len(), 1);
    assert_eq!(cas[0]["createdBy"], "EKS");
    assert_eq!(cas[0]["activatedBy"], "EKS");
    assert_eq!(cas[0]["signingStatus"], "IN_USE");
    assert_eq!(cas[0]["distributionStatus"], "COMPLETE");
}

#[tokio::test]
async fn certificate_authority_create_describe_activate_delete() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let seeded = list_cas(&svc, "c1").await["certificateAuthorities"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/certificate-authorities",
            "{}",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["update"]["type"], "CertificateAuthorityUpdate");
    assert_eq!(v["certificateAuthority"]["createdBy"], "CUSTOMER");
    assert_eq!(v["certificateAuthority"]["signingStatus"], "NOT_USED");
    assert_eq!(
        v["certificateAuthority"]["distributionStatus"],
        "IN_PROGRESS"
    );
    let new_id = v["certificateAuthority"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Describe settles IN_PROGRESS -> COMPLETE and carries the full shape.
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/certificate-authorities/{new_id}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ca = &v["certificateAuthority"];
    assert_eq!(ca["distributionStatus"], "COMPLETE");
    assert_eq!(ca["rollbackAvailable"], false);
    assert!(!ca["data"].as_str().unwrap().is_empty());
    assert!(
        ca["validity"]["notAfter"].as_f64().unwrap()
            > ca["validity"]["notBefore"].as_f64().unwrap()
    );

    // Activating the new CA demotes the seeded one and leaves it rollbackable.
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/certificate-authorities/{new_id}/activate"),
            "{}",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["certificateAuthority"]["signingStatus"], "IN_USE");
    assert_eq!(v["certificateAuthority"]["activatedBy"], "CUSTOMER");
    assert_eq!(v["update"]["type"], "CertificateAuthorityUpdate");

    let v = list_cas(&svc, "c1").await;
    let cas = v["certificateAuthorities"].as_array().unwrap();
    assert_eq!(cas.len(), 2);
    let old = cas
        .iter()
        .find(|c| c["id"] == seeded.as_str())
        .expect("seeded CA still listed");
    assert_eq!(old["signingStatus"], "NOT_USED");

    // The demoted CA is deletable; the signing one is not.
    let err = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/clusters/c1/certificate-authorities/{new_id}"),
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");

    let resp = svc
        .handle(make_request(
            Method::DELETE,
            &format!("/clusters/c1/certificate-authorities/{seeded}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(v["certificateAuthority"]["distributionStatus"], "DELETING");
    assert_eq!(
        list_cas(&svc, "c1").await["certificateAuthorities"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn certificate_authority_create_while_one_distributes_is_in_use() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/certificate-authorities",
        "{}",
    ))
    .await
    .unwrap();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/certificate-authorities",
            "{}",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::CONFLICT);
    assert_eq!(err.code(), "ResourceInUseException");
}

#[tokio::test]
async fn certificate_authority_on_missing_cluster_or_id_is_not_found() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/ghost/certificate-authorities",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");

    let err = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/certificate-authorities/ca-missing",
            "",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");

    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/certificate-authorities/ca-missing/activate",
            "{}",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn delete_cluster_cascades_certificate_authorities() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    svc.handle(make_request(Method::DELETE, "/clusters/c1", ""))
        .await
        .unwrap();
    create_cluster(&svc, "c1").await;
    // The recreated cluster gets exactly one fresh EKS CA, not the old one too.
    assert_eq!(
        list_cas(&svc, "c1").await["certificateAuthorities"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn only_the_most_recently_demoted_ca_is_the_rollback_target() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;

    async fn add_and_activate(svc: &EksService) -> String {
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/certificate-authorities",
                "{}",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let id = v["certificateAuthority"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        svc.handle(make_request(
            Method::POST,
            &format!("/clusters/c1/certificate-authorities/{id}/activate"),
            "{}",
        ))
        .await
        .unwrap();
        id
    }

    let b = add_and_activate(&svc).await;
    let c = add_and_activate(&svc).await;

    // After A -> B -> C, only B (the CA C superseded) is rollbackable.
    let mut rollbackable = Vec::new();
    for id in [&b, &c] {
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/certificate-authorities/{id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        if v["certificateAuthority"]["rollbackAvailable"] == true {
            rollbackable.push(id.clone());
        }
    }
    assert_eq!(rollbackable, vec![b.clone()]);

    let v = list_cas(&svc, "c1").await;
    let in_use: Vec<&str> = v["certificateAuthorities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|ca| ca["signingStatus"] == "IN_USE")
        .map(|ca| ca["id"].as_str().unwrap())
        .collect();
    assert_eq!(in_use, vec![c.as_str()]);
}

#[tokio::test]
async fn china_region_arns_use_the_aws_cn_partition() {
    let svc = EksService::new(make_state());
    let in_china = |method: Method, path: &str, body: &str| {
        let mut r = make_request(method, path, body);
        r.region = "cn-north-1".to_string();
        r
    };
    let body_of =
        |resp: AwsResponse| -> Value { serde_json::from_slice(resp.body.expect_bytes()).unwrap() };

    let v = body_of(
        svc.handle(in_china(Method::POST, "/clusters", &create_body("cn")))
            .await
            .unwrap(),
    );
    let arn = v["cluster"]["arn"].as_str().unwrap().to_string();
    assert_eq!(arn, "arn:aws-cn:eks:cn-north-1:111122223333:cluster/cn");

    let encoded = url_encode(&arn);
    svc.handle(in_china(
        Method::POST,
        &format!("/tags/{encoded}"),
        &json!({ "tags": { "env": "cn" } }).to_string(),
    ))
    .await
    .unwrap();
    let v = body_of(
        svc.handle(in_china(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap(),
    );
    assert_eq!(v["tags"]["env"], "cn");

    let v = body_of(
        svc.handle(in_china(
            Method::POST,
            "/clusters/cn/access-entries",
            &json!({ "principalArn": "arn:aws-cn:iam::111122223333:role/dev" }).to_string(),
        ))
        .await
        .unwrap(),
    );
    assert!(v["accessEntry"]["accessEntryArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws-cn:eks:cn-north-1:111122223333:access-entry/cn/role/"));
    assert_eq!(
        v["accessEntry"]["username"],
        "arn:aws-cn:sts::{{AccountID}}:assumed-role/dev/{{SessionName}}"
    );

    let v = body_of(
        svc.handle(in_china(Method::GET, "/access-policies", ""))
            .await
            .unwrap(),
    );
    assert!(v["accessPolicies"]
        .as_array()
        .unwrap()
        .iter()
        .all(|p| p["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws-cn:eks::aws:cluster-access-policy/")));
}

// -----------------------------------------------------------------------
// Cluster security group (EC2-backed)
// -----------------------------------------------------------------------

fn make_ec2_state() -> fakecloud_ec2::SharedEc2State {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("111122223333", "us-east-1", ""),
    ))
}

/// A subnet EC2 knows about (from the seeded default VPC) and its VPC.
fn ec2_subnet(ec2: &fakecloud_ec2::SharedEc2State) -> (String, String) {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut("111122223333", "us-east-1");
    let subnet = state.subnets.values().next().expect("default subnet");
    (subnet.subnet_id.clone(), subnet.vpc_id.clone())
}

#[tokio::test]
async fn create_cluster_creates_cluster_security_group_in_ec2() {
    let ec2 = make_ec2_state();
    let (subnet_id, vpc_id) = ec2_subnet(&ec2);
    let svc = EksService::new(make_state()).with_ec2_state(ec2.clone());
    let body = json!({
        "name": "demo",
        "roleArn": "arn:aws:iam::111122223333:role/eks",
        "resourcesVpcConfig": { "subnetIds": [subnet_id] },
    });
    let resp = svc
        .handle(make_request(Method::POST, "/clusters", &body.to_string()))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["cluster"]["resourcesVpcConfig"];
    assert_eq!(cfg["vpcId"], vpc_id.as_str());
    let sg_id = cfg["clusterSecurityGroupId"].as_str().unwrap().to_string();

    {
        let accounts = ec2.read();
        let state = accounts.regional("111122223333", "us-east-1").unwrap();
        let sg = state.security_groups.get(&sg_id).expect("SG exists in EC2");
        assert!(sg.group_name.starts_with("eks-cluster-sg-demo-"));
        assert_eq!(sg.vpc_id, vpc_id);
        assert_eq!(
            sg.description,
            crate::cluster_sg::CLUSTER_SECURITY_GROUP_DESCRIPTION
        );
        let ingress: Vec<_> = sg.rules.iter().filter(|r| !r.is_egress).collect();
        assert_eq!(ingress.len(), 1);
        assert_eq!(ingress[0].ip_protocol, "-1");
        assert_eq!(
            ingress[0].referenced_group_id.as_deref(),
            Some(sg_id.as_str())
        );
        let egress: Vec<_> = sg.rules.iter().filter(|r| r.is_egress).collect();
        assert_eq!(egress.len(), 1);
        assert_eq!(egress[0].cidr_ipv4.as_deref(), Some("0.0.0.0/0"));
        let tags: Vec<(String, String)> = state
            .tags_for(&sg_id)
            .iter()
            .map(|t| (t.key.clone(), t.value.clone()))
            .collect();
        assert!(tags.contains(&("aws:eks:cluster-name".into(), "demo".into())));
        assert!(tags.contains(&("kubernetes.io/cluster/demo".into(), "owned".into())));
        assert!(tags.contains(&("Name".into(), sg.group_name.clone())));
    }

    // A VPC-config update keeps the cluster's VPC and security group and
    // only changes the members it carries.
    svc.handle(make_request(
        Method::POST,
        "/clusters/demo/update-config",
        &json!({ "resourcesVpcConfig": { "endpointPrivateAccess": true } }).to_string(),
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(make_request(Method::GET, "/clusters/demo", ""))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["cluster"]["resourcesVpcConfig"];
    assert_eq!(cfg["clusterSecurityGroupId"], sg_id.as_str());
    assert_eq!(cfg["vpcId"], vpc_id.as_str());
    assert_eq!(cfg["endpointPrivateAccess"], true);
    assert_eq!(cfg["subnetIds"][0], subnet_id.as_str());

    // Deleting the cluster deletes its security group.
    svc.handle(make_request(Method::DELETE, "/clusters/demo", ""))
        .await
        .unwrap();
    let accounts = ec2.read();
    let state = accounts.regional("111122223333", "us-east-1").unwrap();
    assert!(!state.security_groups.contains_key(&sg_id));
    assert!(state.tags_for(&sg_id).is_empty());
}

#[tokio::test]
async fn create_cluster_with_unknown_subnets_still_creates_security_group() {
    let ec2 = make_ec2_state();
    let svc = EksService::new(make_state()).with_ec2_state(ec2.clone());
    let resp = svc
        .handle(make_request(Method::POST, "/clusters", &create_body("c1")))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["cluster"]["resourcesVpcConfig"];
    let sg_id = cfg["clusterSecurityGroupId"].as_str().unwrap();
    let accounts = ec2.read();
    let sg = &accounts
        .regional("111122223333", "us-east-1")
        .unwrap()
        .security_groups[sg_id];
    assert_eq!(cfg["vpcId"], sg.vpc_id.as_str());
}

/// A restored cluster whose ARN names no region gets its group in the
/// server's configured region, not a hardcoded one.
#[tokio::test]
async fn restore_places_region_less_cluster_group_in_server_region() {
    let eks = make_state();
    let svc = EksService::new(eks.clone());
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters",
            &create_body("noregion"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let sg_id = v["cluster"]["resourcesVpcConfig"]["clusterSecurityGroupId"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let mut accounts = eks.write();
        let st = accounts.get_mut("111122223333").unwrap();
        let cluster = st.clusters.values_mut().next().unwrap();
        cluster.arn = "arn:aws:eks::111122223333:cluster/noregion".to_string();
    }
    let ec2: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("111122223333", "eu-west-1", ""),
    ));
    assert_eq!(crate::restore_cluster_security_groups(&eks, &ec2), 1);
    let accounts = ec2.read();
    assert!(accounts
        .regional("111122223333", "eu-west-1")
        .unwrap()
        .security_groups
        .contains_key(&sg_id));
    assert!(accounts.regional("111122223333", "us-east-1").is_none());
}

#[tokio::test]
async fn restore_recreates_security_group_for_clusters_saved_without_one() {
    // A cluster created without EC2 wiring carries a group id with no EC2
    // record behind it, like clusters persisted before EKS created the group.
    let eks = make_state();
    let svc = EksService::new(eks.clone());
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters",
            &create_body("legacy"),
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cfg = &v["cluster"]["resourcesVpcConfig"];
    let sg_id = cfg["clusterSecurityGroupId"].as_str().unwrap().to_string();
    let vpc_id = cfg["vpcId"].as_str().unwrap().to_string();

    let ec2 = make_ec2_state();
    assert_eq!(crate::restore_cluster_security_groups(&eks, &ec2), 1);
    {
        let accounts = ec2.read();
        let state = accounts.regional("111122223333", "us-east-1").unwrap();
        let sg = state.security_groups.get(&sg_id).expect("group recreated");
        assert_eq!(sg.vpc_id, vpc_id);
        assert!(sg.group_name.starts_with("eks-cluster-sg-legacy-"));
        let tags: Vec<(String, String)> = state
            .tags_for(&sg_id)
            .iter()
            .map(|t| (t.key.clone(), t.value.clone()))
            .collect();
        assert!(tags.contains(&("aws:eks:cluster-name".into(), "legacy".into())));
        assert!(tags.contains(&("kubernetes.io/cluster/legacy".into(), "owned".into())));
    }

    // Already-backed groups are left alone.
    assert_eq!(crate::restore_cluster_security_groups(&eks, &ec2), 0);

    // The recreated group is the cluster's, so DeleteCluster removes it.
    let svc = EksService::new(eks.clone()).with_ec2_state(ec2.clone());
    svc.handle(make_request(Method::DELETE, "/clusters/legacy", ""))
        .await
        .unwrap();
    let accounts = ec2.read();
    let state = accounts.regional("111122223333", "us-east-1").unwrap();
    assert!(!state.security_groups.contains_key(&sg_id));
}

// -----------------------------------------------------------------------
// Node group Auto Scaling groups, add-on pod identities, version/config merges
// -----------------------------------------------------------------------

fn body_of(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[tokio::test]
async fn nodegroup_creates_resizes_and_deletes_its_auto_scaling_group() {
    let ec2 = make_ec2_state();
    let (subnet_id, _) = ec2_subnet(&ec2);
    let asg: fakecloud_autoscaling::SharedAutoScalingState = Arc::new(RwLock::new(
        fakecloud_autoscaling::AutoScalingAccounts::new(),
    ));
    let svc = EksService::new(make_state())
        .with_ec2_state(ec2.clone())
        .with_autoscaling_state(asg.clone());
    create_cluster(&svc, "c1").await;
    let mut body: Value = serde_json::from_str(&nodegroup_body("ng1")).unwrap();
    body["subnets"] = json!([subnet_id]);
    let v = body_of(
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &body.to_string(),
        ))
        .await
        .unwrap(),
    );
    let asg_name = v["nodegroup"]["resources"]["autoScalingGroups"][0]["name"]
        .as_str()
        .unwrap()
        .to_string();
    {
        let g = asg.read();
        let group = &g.accounts["111122223333"].groups[&asg_name];
        assert_eq!(
            (group.min_size, group.max_size, group.desired_capacity),
            (1, 3, 2)
        );
        assert_eq!(
            group.vpc_zone_identifier.as_deref(),
            Some(subnet_id.as_str())
        );
        assert_eq!(group.availability_zones.len(), 1);
        assert!(group
            .tags
            .iter()
            .any(|t| t.key == "eks:nodegroup-name" && t.value == "ng1"));
        assert!(group
            .tags
            .iter()
            .any(|t| t.key == "k8s.io/cluster-autoscaler/c1" && t.value == "owned"));
    }

    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups/ng1/update-config",
        &json!({ "scalingConfig": { "minSize": 2, "maxSize": 5, "desiredSize": 4 } }).to_string(),
    ))
    .await
    .unwrap();
    {
        let g = asg.read();
        let group = &g.accounts["111122223333"].groups[&asg_name];
        assert_eq!(
            (group.min_size, group.max_size, group.desired_capacity),
            (2, 5, 4)
        );
    }

    svc.handle(make_request(
        Method::DELETE,
        "/clusters/c1/node-groups/ng1",
        "",
    ))
    .await
    .unwrap();
    assert!(!asg.read().accounts["111122223333"]
        .groups
        .contains_key(&asg_name));
}

#[tokio::test]
async fn addon_pod_identity_associations_are_real_and_stable_on_update() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let v = body_of(
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/addons",
            &json!({
                "addonName": "vpc-cni",
                "podIdentityAssociations": [
                    { "serviceAccount": "aws-node", "roleArn": "arn:aws:iam::111122223333:role/cni" }
                ]
            })
            .to_string(),
        ))
        .await
        .unwrap(),
    );
    let addon_arn = v["addon"]["addonArn"].as_str().unwrap().to_string();
    let assoc_arn = v["addon"]["podIdentityAssociations"][0]
        .as_str()
        .unwrap()
        .to_string();

    // The association exists and is owned by the add-on.
    let list = body_of(
        svc.handle(make_request(
            Method::GET,
            "/clusters/c1/pod-identity-associations",
            "",
        ))
        .await
        .unwrap(),
    );
    let summary = &list["associations"][0];
    assert_eq!(summary["associationArn"], assoc_arn.as_str());
    assert_eq!(summary["namespace"], "kube-system");
    assert_eq!(summary["serviceAccount"], "aws-node");
    assert_eq!(summary["ownerArn"], addon_arn.as_str());
    let assoc_id = summary["associationId"].as_str().unwrap().to_string();
    let desc = body_of(
        svc.handle(make_request(
            Method::GET,
            &format!("/clusters/c1/pod-identity-associations/{assoc_id}"),
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        desc["association"]["roleArn"],
        "arn:aws:iam::111122223333:role/cni"
    );

    // Updating the role keeps the association (and its ARN).
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/addons/vpc-cni/update",
        &json!({
            "podIdentityAssociations": [
                { "serviceAccount": "aws-node", "roleArn": "arn:aws:iam::111122223333:role/cni2" }
            ]
        })
        .to_string(),
    ))
    .await
    .unwrap();
    let addon = body_of(
        svc.handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
            .await
            .unwrap(),
    );
    assert_eq!(
        addon["addon"]["podIdentityAssociations"][0],
        assoc_arn.as_str()
    );
    let desc = body_of(
        svc.handle(make_request(
            Method::GET,
            &format!("/clusters/c1/pod-identity-associations/{assoc_id}"),
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        desc["association"]["roleArn"],
        "arn:aws:iam::111122223333:role/cni2"
    );

    // The association belongs to the add-on: it cannot be changed or deleted
    // directly.
    for (method, body) in [
        (
            Method::POST,
            json!({ "roleArn": "arn:aws:iam::111122223333:role/other" }).to_string(),
        ),
        (Method::DELETE, String::new()),
    ] {
        let err = svc
            .handle(make_request(
                method,
                &format!("/clusters/c1/pod-identity-associations/{assoc_id}"),
                &body,
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidRequestException");
    }
    let desc = body_of(
        svc.handle(make_request(
            Method::GET,
            &format!("/clusters/c1/pod-identity-associations/{assoc_id}"),
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        desc["association"]["roleArn"],
        "arn:aws:iam::111122223333:role/cni2"
    );

    // Deleting the add-on deletes its associations.
    svc.handle(make_request(
        Method::DELETE,
        "/clusters/c1/addons/vpc-cni",
        "",
    ))
    .await
    .unwrap();
    let list = body_of(
        svc.handle(make_request(
            Method::GET,
            "/clusters/c1/pod-identity-associations",
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(list["associations"], json!([]));
}

#[tokio::test]
async fn update_nodegroup_version_applies_launch_template_version() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let mut body: Value = serde_json::from_str(&nodegroup_body("ng1")).unwrap();
    body["launchTemplate"] = json!({ "id": "lt-0123456789abcdef0", "version": "1" });
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups",
        &body.to_string(),
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/node-groups/ng1/update-version",
        &json!({ "launchTemplate": { "id": "lt-0123456789abcdef0", "version": "3" } }).to_string(),
    ))
    .await
    .unwrap();
    let ng = body_of(
        svc.handle(make_request(
            Method::GET,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(ng["nodegroup"]["launchTemplate"]["version"], "3");
    assert_eq!(
        ng["nodegroup"]["launchTemplate"]["id"],
        "lt-0123456789abcdef0"
    );

    // A different launch template is rejected, and the rejected request
    // changes nothing (not even the Kubernetes version it carried).
    let before = ng["nodegroup"]["version"].clone();
    let err = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups/ng1/update-version",
            &json!({
                "version": "1.99",
                "releaseVersion": "1.99-x",
                "launchTemplate": { "id": "lt-0000000000000000f", "version": "4" }
            })
            .to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "InvalidParameterException");
    let ng = body_of(
        svc.handle(make_request(
            Method::GET,
            "/clusters/c1/node-groups/ng1",
            "",
        ))
        .await
        .unwrap(),
    );
    assert_eq!(ng["nodegroup"]["version"], before);
    assert_ne!(ng["nodegroup"]["releaseVersion"], "1.99-x");
    assert_eq!(ng["nodegroup"]["launchTemplate"]["version"], "3");
}

#[tokio::test]
async fn update_cluster_config_merges_network_config() {
    let svc = EksService::new(make_state());
    let mut body: Value = serde_json::from_str(&create_body("c1")).unwrap();
    body["kubernetesNetworkConfig"] = json!({ "serviceIpv4Cidr": "10.100.0.0/16" });
    svc.handle(make_request(Method::POST, "/clusters", &body.to_string()))
        .await
        .unwrap();
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/update-config",
        &json!({ "kubernetesNetworkConfig": { "elasticLoadBalancing": { "enabled": true } } })
            .to_string(),
    ))
    .await
    .unwrap();
    // An endpoint toggle keeps the subnets.
    svc.handle(make_request(
        Method::POST,
        "/clusters/c1/update-config",
        &json!({ "resourcesVpcConfig": { "endpointPrivateAccess": true } }).to_string(),
    ))
    .await
    .unwrap();
    let c = body_of(
        svc.handle(make_request(Method::GET, "/clusters/c1", ""))
            .await
            .unwrap(),
    );
    let kn = &c["cluster"]["kubernetesNetworkConfig"];
    assert_eq!(kn["serviceIpv4Cidr"], "10.100.0.0/16");
    assert_eq!(kn["elasticLoadBalancing"]["enabled"], true);
    let vpc = &c["cluster"]["resourcesVpcConfig"];
    assert_eq!(vpc["subnetIds"], json!(["subnet-1", "subnet-2"]));
    assert_eq!(vpc["endpointPrivateAccess"], true);
}

#[tokio::test]
async fn addon_errors_match_a_live_eks_cluster() {
    let svc = EksService::new(make_state());
    create_cluster_on(&svc, "live", "1.31").await;
    let call =
        |method: Method, path: &str, body: Value| make_request(method, path, &body.to_string());
    // Unknown add-on names are refused, with or without a version.
    for body in [
        json!({ "addonName": "not-an-addon" }),
        json!({ "addonName": "not-an-addon", "addonVersion": "v1.0.0-eksbuild.1" }),
    ] {
        let err = svc
            .handle(call(Method::POST, "/clusters/live/addons", body))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        assert_eq!(
            err.message(),
            "Addon not-an-addon specified is not supported in 1.31 kubernetes version"
        );
    }
    // UpdateAddon on a missing add-on is a not-found before any version check.
    let err = svc
        .handle(call(
            Method::POST,
            "/clusters/live/addons/kube-proxy/update",
            json!({ "addonVersion": "v9.9.9-eksbuild.1" }),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.status(), StatusCode::NOT_FOUND);
    assert_eq!(err.code(), "ResourceNotFoundException");
    assert_eq!(err.message(), "The requested resource does not exist.");
}

async fn create_capability_with(svc: &EksService, body: Value) -> Result<Value, AwsServiceError> {
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities",
            &body.to_string(),
        ))
        .await?;
    Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
}

async fn update_capability_with(
    svc: &EksService,
    name: &str,
    body: Value,
) -> Result<Value, AwsServiceError> {
    let resp = svc
        .handle(make_request(
            Method::POST,
            &format!("/clusters/c1/capabilities/{name}"),
            &body.to_string(),
        ))
        .await?;
    Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
}

async fn describe_capability_config(svc: &EksService, name: &str) -> Value {
    let resp = svc
        .handle(make_request(
            Method::GET,
            &format!("/clusters/c1/capabilities/{name}"),
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    v["capability"]["configuration"].clone()
}

#[tokio::test]
async fn ack_configuration_round_trips_and_merges_on_update() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let v = create_capability_with(
        &svc,
        json!({
            "capabilityName": "ack",
            "type": "ACK",
            "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
            "configuration": {"ack": {
                "enableCrossNamespace": true,
                "disabledServices": ["s3", "ec2"]
            }}
        }),
    )
    .await
    .unwrap();
    let expected =
        json!({"ack": {"enableCrossNamespace": true, "disabledServices": ["s3", "ec2"]}});
    assert_eq!(v["capability"]["configuration"], expected);
    assert_eq!(describe_capability_config(&svc, "ack").await, expected);

    // Only disabledServices: replaces the list, flag unchanged, one param.
    let v = update_capability_with(
        &svc,
        "ack",
        json!({"configuration": {"ack": {"disabledServices": ["iam"]}}}),
    )
    .await
    .unwrap();
    assert_eq!(v["update"]["type"], "CapabilityUpdate");
    assert_eq!(
        v["update"]["params"],
        json!([{"type": "DisabledServices", "value": "[\"iam\"]"}])
    );
    assert_eq!(
        describe_capability_config(&svc, "ack").await,
        json!({"ack": {"enableCrossNamespace": true, "disabledServices": ["iam"]}})
    );

    // Only enableCrossNamespace: list unchanged.
    let v = update_capability_with(
        &svc,
        "ack",
        json!({"configuration": {"ack": {"enableCrossNamespace": false}}}),
    )
    .await
    .unwrap();
    assert_eq!(
        v["update"]["params"],
        json!([{"type": "EnableCrossNamespace", "value": "false"}])
    );
    assert_eq!(
        describe_capability_config(&svc, "ack").await,
        json!({"ack": {"enableCrossNamespace": false, "disabledServices": ["iam"]}})
    );

    // Empty list turns every service back on; both params reported.
    let v = update_capability_with(
        &svc,
        "ack",
        json!({"configuration": {"ack": {"enableCrossNamespace": true, "disabledServices": []}}}),
    )
    .await
    .unwrap();
    assert_eq!(
        v["update"]["params"],
        json!([
            {"type": "EnableCrossNamespace", "value": "true"},
            {"type": "DisabledServices", "value": "[]"}
        ])
    );
    assert_eq!(
        describe_capability_config(&svc, "ack").await,
        json!({"ack": {"enableCrossNamespace": true, "disabledServices": []}})
    );
}

#[tokio::test]
async fn ack_capability_defaults_when_ack_config_omitted() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let v = create_capability_with(
        &svc,
        json!({
            "capabilityName": "ack",
            "type": "ACK",
            "roleArn": "arn:aws:iam::111122223333:role/eks-capability"
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        v["capability"]["configuration"],
        json!({"ack": {"enableCrossNamespace": false, "disabledServices": []}})
    );
}

#[tokio::test]
async fn ack_configuration_ignored_for_other_capability_types() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let v = create_capability_with(
        &svc,
        json!({
            "capabilityName": "kro",
            "type": "KRO",
            "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
            "configuration": {"ack": {"enableCrossNamespace": true}}
        }),
    )
    .await
    .unwrap();
    assert!(v["capability"].get("configuration").is_none());
    let v = update_capability_with(
        &svc,
        "kro",
        json!({"configuration": {"ack": {"disabledServices": ["s3"]}}}),
    )
    .await
    .unwrap();
    assert_eq!(v["update"]["params"], json!([]));
    assert!(describe_capability_config(&svc, "kro").await.is_null());
}

#[tokio::test]
async fn ack_configuration_rejects_invalid_service_names() {
    let svc = EksService::new(make_state());
    create_cluster(&svc, "c1").await;
    let base = |ack: Value| {
        json!({
            "capabilityName": "ack",
            "type": "ACK",
            "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
            "configuration": {"ack": ack}
        })
    };
    let too_long = "a".repeat(64);
    let too_many: Vec<String> = (0..201).map(|i| format!("svc{i}")).collect();
    for bad in [
        json!({"disabledServices": ["S3"]}),
        json!({"disabledServices": ["1ec2"]}),
        json!({"disabledServices": ["ec-2"]}),
        json!({"disabledServices": [""]}),
        json!({"disabledServices": [too_long]}),
        json!({"disabledServices": too_many}),
        json!({"enableCrossNamespace": "yes"}),
    ] {
        let err = create_capability_with(&svc, base(bad.clone()))
            .await
            .err()
            .unwrap_or_else(|| panic!("expected rejection for {bad}"));
        assert_eq!(err.code(), "InvalidParameterException", "{bad}");
    }
    // The 63-char maximum and 200-entry maximum are accepted.
    let max_ok: Vec<String> = (0..200).map(|i| format!("svc{i}")).collect();
    create_capability_with(
        &svc,
        base(json!({"disabledServices": max_ok, "enableCrossNamespace": true})),
    )
    .await
    .unwrap();

    // A rejected update leaves the capability (incl. roleArn) untouched.
    let err = update_capability_with(
        &svc,
        "ack",
        json!({
            "roleArn": "arn:aws:iam::111122223333:role/other",
            "configuration": {"ack": {"disabledServices": ["Bad"]}}
        }),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.code(), "InvalidParameterException");
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/clusters/c1/capabilities/ack",
            "",
        ))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        v["capability"]["roleArn"],
        "arn:aws:iam::111122223333:role/eks-capability"
    );
    assert_eq!(
        v["capability"]["configuration"]["ack"]["disabledServices"]
            .as_array()
            .unwrap()
            .len(),
        200
    );
}
