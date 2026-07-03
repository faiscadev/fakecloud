//! End-to-end handler tests for the shared OpenSearch / Elasticsearch service.
//!
//! Every test drives [`OpenSearchService::handle`] with a hand-built
//! `AwsRequest` at the appropriate API path version, proving real round-trip
//! behaviour: a domain created through either API is one shared entity visible
//! through both, config updates persist, deletes remove, tags round-trip, and
//! the documented error codes fire.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService};
use fakecloud_opensearch::{OpenSearchService, SharedOpenSearchState};

const ES: &str = "/2015-01-01";
const OS: &str = "/2021-01-01";

fn service() -> OpenSearchService {
    let state: SharedOpenSearchState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    OpenSearchService::new(state)
}

fn req(method: Method, path: &str, body: Value) -> AwsRequest {
    let raw_path = path.to_string();
    let path_segments = raw_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    AwsRequest {
        service: "es".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments,
        raw_path,
        raw_query: String::new(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn with_query(mut r: AwsRequest, k: &str, v: &str) -> AwsRequest {
    r.query_params.insert(k.to_string(), v.to_string());
    r.raw_query = format!("{k}={v}");
    r
}

async fn call(svc: &OpenSearchService, r: AwsRequest) -> AwsResponse {
    svc.handle(r).await.expect("handler returned an error")
}

async fn call_err(svc: &OpenSearchService, r: AwsRequest) -> (u16, String) {
    match svc.handle(r).await {
        Ok(_) => panic!("expected an error, got success"),
        Err(e) => (e.status().as_u16(), e.code().to_string()),
    }
}

fn json_of(resp: &AwsResponse) -> Value {
    let bytes = match &resp.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("non-bytes body"),
    };
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Domain lifecycle (shared store across both APIs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_domain_via_opensearch_roundtrips() {
    let svc = service();
    let resp = call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "mydomain", "EngineVersion": "OpenSearch_2.11"}),
        ),
    )
    .await;
    let v = json_of(&resp);
    assert_eq!(v["DomainStatus"]["DomainName"], "mydomain");
    assert_eq!(v["DomainStatus"]["EngineVersion"], "OpenSearch_2.11");
    assert!(v["DomainStatus"]["ARN"].as_str().unwrap().contains(":es:"));
}

#[tokio::test]
async fn create_domain_via_elasticsearch_roundtrips() {
    let svc = service();
    let resp = call(
        &svc,
        req(
            Method::POST,
            &format!("{ES}/es/domain"),
            json!({"DomainName": "legacy", "ElasticsearchVersion": "7.10"}),
        ),
    )
    .await;
    let v = json_of(&resp);
    assert_eq!(v["DomainStatus"]["DomainName"], "legacy");
    // The 2015 API exposes the version under `ElasticsearchVersion`.
    assert_eq!(v["DomainStatus"]["ElasticsearchVersion"], "7.10");
    assert!(v["DomainStatus"]
        .get("ElasticsearchClusterConfig")
        .is_some());
}

#[tokio::test]
async fn domain_created_via_opensearch_is_visible_via_elasticsearch() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "shared", "EngineVersion": "OpenSearch_2.11"}),
        ),
    )
    .await;
    // Describe the SAME domain through the legacy 2015 Elasticsearch API.
    let resp = call(
        &svc,
        req(Method::GET, &format!("{ES}/es/domain/shared"), json!({})),
    )
    .await;
    let v = json_of(&resp);
    assert_eq!(v["DomainStatus"]["DomainName"], "shared");
    // Exposed via the ES shape (ElasticsearchVersion), same underlying entity.
    assert_eq!(v["DomainStatus"]["ElasticsearchVersion"], "2.11");
}

#[tokio::test]
async fn domain_created_via_elasticsearch_is_visible_via_opensearch() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{ES}/es/domain"),
            json!({"DomainName": "legacy2", "ElasticsearchVersion": "7.10"}),
        ),
    )
    .await;
    let resp = call(
        &svc,
        req(
            Method::GET,
            &format!("{OS}/opensearch/domain/legacy2"),
            json!({}),
        ),
    )
    .await;
    let v = json_of(&resp);
    assert_eq!(v["DomainStatus"]["DomainName"], "legacy2");
    assert_eq!(v["DomainStatus"]["EngineVersion"], "Elasticsearch_7.10");
}

#[tokio::test]
async fn describe_settles_created_flag() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/domain"),
                json!({"DomainName": "settle"}),
            ),
        )
        .await,
    );
    assert_eq!(created["DomainStatus"]["Created"], false);
    let described = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/domain/settle"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(described["DomainStatus"]["Created"], true);
    assert_eq!(described["DomainStatus"]["Processing"], false);
}

#[tokio::test]
async fn duplicate_domain_is_resource_already_exists() {
    let svc = service();
    let make = || {
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "dup"}),
        )
    };
    call(&svc, make()).await;
    let (status, code) = call_err(&svc, make()).await;
    assert_eq!(status, 409);
    assert_eq!(code, "ResourceAlreadyExistsException");
}

#[tokio::test]
async fn describe_missing_domain_is_resource_not_found() {
    let svc = service();
    let (status, code) = call_err(
        &svc,
        req(
            Method::GET,
            &format!("{OS}/opensearch/domain/ghost"),
            json!({}),
        ),
    )
    .await;
    // ResourceNotFoundException carries HTTP 409 in both Smithy models.
    assert_eq!(status, 409);
    assert_eq!(code, "ResourceNotFoundException");
}

#[tokio::test]
async fn invalid_domain_name_is_validation_exception() {
    let svc = service();
    let (status, code) = call_err(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "AB"}), // too short + uppercase
        ),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code, "ValidationException");
}

#[tokio::test]
async fn update_domain_config_persists() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "cfg", "EngineVersion": "OpenSearch_2.9"}),
        ),
    )
    .await;
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain/cfg/config"),
            json!({"EngineVersion": "OpenSearch_2.11"}),
        ),
    )
    .await;
    let described = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/domain/cfg"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        described["DomainStatus"]["EngineVersion"],
        "OpenSearch_2.11"
    );
}

#[tokio::test]
async fn dry_run_config_update_does_not_persist() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "dry", "EngineVersion": "OpenSearch_2.9"}),
        ),
    )
    .await;
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain/dry/config"),
            json!({"EngineVersion": "OpenSearch_2.11", "DryRun": true}),
        ),
    )
    .await;
    let described = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/domain/dry"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(described["DomainStatus"]["EngineVersion"], "OpenSearch_2.9");
}

#[tokio::test]
async fn delete_domain_removes_it() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "gone"}),
        ),
    )
    .await;
    let deleted = json_of(
        &call(
            &svc,
            req(
                Method::DELETE,
                &format!("{OS}/opensearch/domain/gone"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(deleted["DomainStatus"]["Deleted"], true);
    let (status, _) = call_err(
        &svc,
        req(
            Method::GET,
            &format!("{OS}/opensearch/domain/gone"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(status, 409);
}

#[tokio::test]
async fn list_domain_names_reflects_both_apis() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "osdomain", "EngineVersion": "OpenSearch_2.11"}),
        ),
    )
    .await;
    call(
        &svc,
        req(
            Method::POST,
            &format!("{ES}/es/domain"),
            json!({"DomainName": "esdomain", "ElasticsearchVersion": "7.10"}),
        ),
    )
    .await;
    let listed = json_of(&call(&svc, req(Method::GET, &format!("{ES}/domain"), json!({}))).await);
    let names = listed["DomainNames"].as_array().unwrap();
    assert_eq!(names.len(), 2);
    let engine_types: Vec<&str> = names
        .iter()
        .map(|n| n["EngineType"].as_str().unwrap())
        .collect();
    assert!(engine_types.contains(&"OpenSearch"));
    assert!(engine_types.contains(&"Elasticsearch"));
}

#[tokio::test]
async fn describe_domains_batch_returns_requested() {
    let svc = service();
    for d in ["dom1", "dom2"] {
        call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/domain"),
                json!({"DomainName": d}),
            ),
        )
        .await;
    }
    let v = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/domain-info"),
                json!({"DomainNames": ["dom1", "dom2"]}),
            ),
        )
        .await,
    );
    assert_eq!(v["DomainStatusList"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn describe_domain_config_wraps_options() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "wrapcfg", "EngineVersion": "OpenSearch_2.11"}),
        ),
    )
    .await;
    let v = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/domain/wrapcfg/config"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        v["DomainConfig"]["EngineVersion"]["Options"],
        "OpenSearch_2.11"
    );
    assert!(v["DomainConfig"]["EngineVersion"]["Status"].is_object());
}

// ---------------------------------------------------------------------------
// Tags (shared ARN space)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tags_added_at_create_are_listed() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/domain"),
                json!({"DomainName": "tagged", "TagList": [{"Key": "env", "Value": "prod"}]}),
            ),
        )
        .await,
    );
    let arn = created["DomainStatus"]["ARN"].as_str().unwrap().to_string();
    let listed = json_of(
        &call(
            &svc,
            with_query(
                req(Method::GET, &format!("{OS}/tags"), json!({})),
                "arn",
                &arn,
            ),
        )
        .await,
    );
    let tags = listed["TagList"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "env");
}

#[tokio::test]
async fn add_and_remove_tags_roundtrip() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/domain"),
                json!({"DomainName": "tagrt"}),
            ),
        )
        .await,
    );
    let arn = created["DomainStatus"]["ARN"].as_str().unwrap().to_string();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/tags"),
            json!({"ARN": arn, "TagList": [{"Key": "team", "Value": "search"}]}),
        ),
    )
    .await;
    // Tags added via ES AddTags are visible via OpenSearch ListTags (shared ARN).
    let listed = json_of(
        &call(
            &svc,
            with_query(
                req(Method::GET, &format!("{ES}/tags"), json!({})),
                "arn",
                &arn,
            ),
        )
        .await,
    );
    assert_eq!(listed["TagList"].as_array().unwrap().len(), 1);
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/tags-removal"),
            json!({"ARN": arn, "TagKeys": ["team"]}),
        ),
    )
    .await;
    let after = json_of(
        &call(
            &svc,
            with_query(
                req(Method::GET, &format!("{OS}/tags"), json!({})),
                "arn",
                &arn,
            ),
        )
        .await,
    );
    assert!(after["TagList"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Packages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn package_create_describe_delete_roundtrip() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/packages"),
                json!({"PackageName": "synonyms", "PackageType": "TXT-DICTIONARY", "PackageSource": {"S3BucketName": "b", "S3Key": "k"}}),
            ),
        )
        .await,
    );
    let pid = created["PackageDetails"]["PackageID"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(created["PackageDetails"]["PackageName"], "synonyms");

    let listed = json_of(
        &call(
            &svc,
            req(Method::POST, &format!("{OS}/packages/describe"), json!({})),
        )
        .await,
    );
    assert_eq!(listed["PackageDetailsList"].as_array().unwrap().len(), 1);

    call(
        &svc,
        req(Method::DELETE, &format!("{OS}/packages/{pid}"), json!({})),
    )
    .await;
    let after = json_of(
        &call(
            &svc,
            req(Method::POST, &format!("{OS}/packages/describe"), json!({})),
        )
        .await,
    );
    assert!(after["PackageDetailsList"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn associate_package_requires_existing_domain() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/packages"),
                json!({"PackageName": "pkg", "PackageType": "TXT-DICTIONARY", "PackageSource": {"S3BucketName": "b", "S3Key": "k"}}),
            ),
        )
        .await,
    );
    let pid = created["PackageDetails"]["PackageID"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, code) = call_err(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/packages/associate/{pid}/nodomain"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(code, "ResourceNotFoundException");
}

// ---------------------------------------------------------------------------
// VPC endpoints + connections
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vpc_endpoint_create_and_list() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/vpcEndpoints"),
                json!({"DomainArn": "arn:aws:es:us-east-1:000000000000:domain/d",
                       "VpcOptions": {"SubnetIds": ["subnet-1"]}}),
            ),
        )
        .await,
    );
    assert_eq!(created["VpcEndpoint"]["Status"], "ACTIVE");
    let listed = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/vpcEndpoints"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        listed["VpcEndpointSummaryList"].as_array().unwrap().len(),
        1
    );
    // NextToken is a required output member on the list shape.
    assert!(listed.get("NextToken").is_some());
}

#[tokio::test]
async fn outbound_connection_create_and_delete() {
    let svc = service();
    let created = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/cc/outboundConnection"),
                json!({
                    "LocalDomainInfo": {"AWSDomainInformation": {"DomainName": "local"}},
                    "RemoteDomainInfo": {"AWSDomainInformation": {"DomainName": "remote"}},
                    "ConnectionAlias": "link"
                }),
            ),
        )
        .await,
    );
    let cid = created["ConnectionId"].as_str().unwrap().to_string();
    assert_eq!(
        created["ConnectionStatus"]["StatusCode"],
        "PENDING_ACCEPTANCE"
    );
    let deleted = json_of(
        &call(
            &svc,
            req(
                Method::DELETE,
                &format!("{OS}/opensearch/cc/outboundConnection/{cid}"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        deleted["CrossClusterSearchConnection"]["ConnectionStatus"]["StatusCode"],
        "DELETING"
    );
}

// ---------------------------------------------------------------------------
// Applications + reserved instances + versions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_application_returns_create_shape() {
    let svc = service();
    let v = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/application"),
                json!({"name": "myapp"}),
            ),
        )
        .await,
    );
    assert_eq!(v["name"], "myapp");
    assert!(v["arn"].as_str().unwrap().contains(":application/"));
    // CreateApplicationResponse omits status/endpoint (unlike GetApplication).
    assert!(v.get("status").is_none());
}

#[tokio::test]
async fn purchase_reserved_instance_uses_api_specific_field() {
    let svc = service();
    let os = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{OS}/opensearch/purchaseReservedInstanceOffering"),
                json!({"ReservedInstanceOfferingId": "o-1", "ReservationName": "reserve"}),
            ),
        )
        .await,
    );
    assert!(os.get("ReservedInstanceId").is_some());
    let es = json_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("{ES}/es/purchaseReservedInstanceOffering"),
                json!({"ReservedElasticsearchInstanceOfferingId": "o-1", "ReservationName": "reserve"}),
            ),
        )
        .await,
    );
    assert!(es.get("ReservedElasticsearchInstanceId").is_some());
}

#[tokio::test]
async fn list_versions_uses_api_specific_key() {
    let svc = service();
    let os = json_of(
        &call(
            &svc,
            req(Method::GET, &format!("{OS}/opensearch/versions"), json!({})),
        )
        .await,
    );
    assert!(os.get("Versions").is_some());
    let es = json_of(
        &call(
            &svc,
            req(Method::GET, &format!("{ES}/es/versions"), json!({})),
        )
        .await,
    );
    assert!(es.get("ElasticsearchVersions").is_some());
}

#[tokio::test]
async fn omitted_required_body_field_is_validation_error() {
    let svc = service();
    // AddTags requires ARN.
    let (status, code) = call_err(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/tags"),
            json!({"TagList": [{"Key": "k", "Value": "v"}]}),
        ),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code, "ValidationException");
}

#[tokio::test]
async fn per_account_isolation() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "acctdomain"}),
        ),
    )
    .await;
    // A different account must not see it.
    let mut other = req(
        Method::GET,
        &format!("{OS}/opensearch/domain/acctdomain"),
        json!({}),
    );
    other.account_id = "111122223333".to_string();
    let (status, _) = call_err(&svc, other).await;
    assert_eq!(status, 409);
}

#[tokio::test]
async fn data_source_roundtrip_on_domain() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain"),
            json!({"DomainName": "dsdomain"}),
        ),
    )
    .await;
    call(
        &svc,
        req(
            Method::POST,
            &format!("{OS}/opensearch/domain/dsdomain/dataSource"),
            json!({"Name": "s3ds", "DataSourceType": {"S3GlueDataCatalog": {}}}),
        ),
    )
    .await;
    let listed = json_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("{OS}/opensearch/domain/dsdomain/dataSource"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(listed["DataSources"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_route_is_validation_not_routing_gap() {
    let svc = service();
    // A path with no version prefix has no route.
    let (status, code) = call_err(&svc, req(Method::GET, "/nope", json!({}))).await;
    assert_eq!(status, 400);
    assert_eq!(code, "ValidationException");
}
