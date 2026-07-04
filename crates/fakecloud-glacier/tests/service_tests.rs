//! End-to-end handler tests for Amazon S3 Glacier.
//!
//! Every test drives [`GlacierService::handle`] with a hand-built restJson1
//! `AwsRequest`, proving real round-trip behaviour: vault CRUD, an
//! upload-archive -> initiate-job -> get-job-output cycle that returns the
//! exact bytes uploaded, inventory jobs, multipart assembly, the vault-lock
//! state machine, tags, pagination, and the documented error codes.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, ResponseBody};
use fakecloud_glacier::{GlacierService, SharedGlacierState};

const ACCT: &str = "000000000000";

fn service() -> GlacierService {
    let state: SharedGlacierState =
        Arc::new(RwLock::new(MultiAccountState::new(ACCT, "us-east-1", "")));
    GlacierService::new(state)
}

/// Build a request with a JSON body.
fn req(method: Method, path: &str, body: Value) -> AwsRequest {
    let bytes = if body.is_null() {
        Bytes::new()
    } else {
        Bytes::from(serde_json::to_vec(&body).unwrap())
    };
    req_raw(method, path, bytes, &[])
}

/// Build a request with a raw byte body and explicit headers.
fn req_raw(method: Method, path: &str, body: Bytes, headers: &[(&str, &str)]) -> AwsRequest {
    let raw_path = path.split('?').next().unwrap_or(path).to_string();
    let raw_query = path
        .split_once('?')
        .map(|(_, q)| q.to_string())
        .unwrap_or_default();
    let mut query_params = HashMap::new();
    for pair in raw_query.split('&').filter(|s| !s.is_empty()) {
        if let Some((k, v)) = pair.split_once('=') {
            query_params.insert(k.to_string(), v.to_string());
        }
    }
    let path_segments = raw_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut hmap = HeaderMap::new();
    for (k, v) in headers {
        hmap.insert(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    AwsRequest {
        service: "glacier".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: ACCT.to_string(),
        request_id: "test".to_string(),
        headers: hmap,
        query_params,
        body,
        body_stream: Mutex::new(None),
        path_segments,
        raw_path,
        raw_query,
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

async fn call(svc: &GlacierService, r: AwsRequest) -> AwsResponse {
    svc.handle(r).await.expect("handler must not return Err")
}

fn raw_body(resp: &AwsResponse) -> Bytes {
    match &resp.body {
        ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("non-bytes body"),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    let bytes = raw_body(resp);
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes).unwrap()
}

fn header_of(resp: &AwsResponse, name: &str) -> Option<String> {
    resp.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// `(status, error-code)` for a response that carries the Glacier error
/// envelope. Errors are returned as an `Ok(AwsResponse)` with a 4xx status.
fn err_of(resp: &AwsResponse) -> (u16, String) {
    let code = body_of(resp)["code"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (resp.status.as_u16(), code)
}

async fn make_vault(svc: &GlacierService, name: &str) {
    let resp = call(
        svc,
        req(Method::PUT, &format!("/-/vaults/{name}"), Value::Null),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
}

// ---------------------------------------------------------------------------
// Vault CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vault_create_describe_list_delete_roundtrip() {
    let svc = service();
    let resp = call(&svc, req(Method::PUT, "/-/vaults/myvault", Value::Null)).await;
    assert_eq!(resp.status.as_u16(), 201);
    assert_eq!(
        header_of(&resp, "Location").unwrap(),
        "/000000000000/vaults/myvault"
    );

    let d = body_of(&call(&svc, req(Method::GET, "/-/vaults/myvault", Value::Null)).await);
    assert_eq!(d["VaultName"], "myvault");
    assert_eq!(d["NumberOfArchives"], 0);
    assert_eq!(d["SizeInBytes"], 0);
    assert!(d["VaultARN"].as_str().unwrap().contains(":vaults/myvault"));

    let l = body_of(&call(&svc, req(Method::GET, "/-/vaults", Value::Null)).await);
    assert_eq!(l["VaultList"].as_array().unwrap().len(), 1);

    let del = call(&svc, req(Method::DELETE, "/-/vaults/myvault", Value::Null)).await;
    assert_eq!(del.status.as_u16(), 204);

    let (status, code) =
        err_of(&call(&svc, req(Method::GET, "/-/vaults/myvault", Value::Null)).await);
    assert_eq!(status, 404);
    assert_eq!(code, "ResourceNotFoundException");
}

#[tokio::test]
async fn create_vault_is_idempotent() {
    let svc = service();
    make_vault(&svc, "v").await;
    make_vault(&svc, "v").await;
    let l = body_of(&call(&svc, req(Method::GET, "/-/vaults", Value::Null)).await);
    assert_eq!(l["VaultList"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn describe_missing_vault_is_not_found() {
    let svc = service();
    let (status, code) = err_of(&call(&svc, req(Method::GET, "/-/vaults/nope", Value::Null)).await);
    assert_eq!(status, 404);
    assert_eq!(code, "ResourceNotFoundException");
}

#[tokio::test]
async fn invalid_vault_name_rejected() {
    let svc = service();
    let (status, code) = err_of(
        &call(
            &svc,
            req(Method::PUT, "/-/vaults/bad%20name%21", Value::Null),
        )
        .await,
    );
    assert_eq!(status, 400);
    assert_eq!(code, "InvalidParameterValueException");
}

#[tokio::test]
async fn any_account_id_maps_to_caller() {
    // The literal `-` and a real account id both resolve to the caller.
    let svc = service();
    call(
        &svc,
        req(Method::PUT, "/999988887777/vaults/v", Value::Null),
    )
    .await;
    let d = body_of(&call(&svc, req(Method::GET, "/-/vaults/v", Value::Null)).await);
    assert_eq!(d["VaultName"], "v");
}

// ---------------------------------------------------------------------------
// Archives + retrieval round-trip
// ---------------------------------------------------------------------------

async fn upload(svc: &GlacierService, vault: &str, data: &[u8]) -> String {
    let resp = call(
        svc,
        req_raw(
            Method::POST,
            &format!("/-/vaults/{vault}/archives"),
            Bytes::from(data.to_vec()),
            &[("x-amz-archive-description", "hello")],
        ),
    )
    .await;
    assert_eq!(resp.status.as_u16(), 201);
    header_of(&resp, "x-amz-archive-id").unwrap()
}

#[tokio::test]
async fn upload_retrieve_get_output_returns_same_bytes() {
    let svc = service();
    make_vault(&svc, "v").await;
    let data = b"the quick brown fox jumps over the lazy dog";
    let archive_id = upload(&svc, "v", data).await;

    // NumberOfArchives + SizeInBytes reflect the upload.
    let d = body_of(&call(&svc, req(Method::GET, "/-/vaults/v", Value::Null)).await);
    assert_eq!(d["NumberOfArchives"], 1);
    assert_eq!(d["SizeInBytes"], data.len() as u64);

    // Initiate an archive-retrieval job.
    let job_resp = call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/jobs",
            json!({ "Type": "archive-retrieval", "ArchiveId": archive_id }),
        ),
    )
    .await;
    assert_eq!(job_resp.status.as_u16(), 202);
    let job_id = header_of(&job_resp, "x-amz-job-id").unwrap();

    // GetJobOutput returns exactly the uploaded bytes.
    let out = call(
        &svc,
        req(
            Method::GET,
            &format!("/-/vaults/v/jobs/{job_id}/output"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(out.status.as_u16(), 200);
    assert_eq!(raw_body(&out).as_ref(), data);
    assert!(header_of(&out, "x-amz-sha256-tree-hash").is_some());
}

#[tokio::test]
async fn get_job_output_honours_range() {
    let svc = service();
    make_vault(&svc, "v").await;
    let data = b"0123456789";
    let archive_id = upload(&svc, "v", data).await;
    let job_resp = call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/jobs",
            json!({ "Type": "archive-retrieval", "ArchiveId": archive_id }),
        ),
    )
    .await;
    let job_id = header_of(&job_resp, "x-amz-job-id").unwrap();
    let out = call(
        &svc,
        req_raw(
            Method::GET,
            &format!("/-/vaults/v/jobs/{job_id}/output"),
            Bytes::new(),
            &[("Range", "bytes=2-5")],
        ),
    )
    .await;
    assert_eq!(out.status.as_u16(), 206);
    assert_eq!(raw_body(&out).as_ref(), b"2345");
    assert_eq!(header_of(&out, "Content-Range").unwrap(), "bytes 2-5/10");
}

#[tokio::test]
async fn tree_hash_mismatch_rejected() {
    let svc = service();
    make_vault(&svc, "v").await;
    let (status, code) = err_of(
        &call(
            &svc,
            req_raw(
                Method::POST,
                "/-/vaults/v/archives",
                Bytes::from_static(b"payload"),
                &[("x-amz-sha256-tree-hash", "deadbeef")],
            ),
        )
        .await,
    );
    assert_eq!(status, 400);
    assert_eq!(code, "InvalidParameterValueException");
}

#[tokio::test]
async fn upload_to_missing_vault_is_not_found() {
    let svc = service();
    let (status, code) = err_of(
        &call(
            &svc,
            req_raw(
                Method::POST,
                "/-/vaults/ghost/archives",
                Bytes::from_static(b"x"),
                &[],
            ),
        )
        .await,
    );
    assert_eq!(status, 404);
    assert_eq!(code, "ResourceNotFoundException");
}

#[tokio::test]
async fn delete_archive_removes_it() {
    let svc = service();
    make_vault(&svc, "v").await;
    let id = upload(&svc, "v", b"data").await;
    let del = call(
        &svc,
        req(
            Method::DELETE,
            &format!("/-/vaults/v/archives/{id}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(del.status.as_u16(), 204);
    let d = body_of(&call(&svc, req(Method::GET, "/-/vaults/v", Value::Null)).await);
    assert_eq!(d["NumberOfArchives"], 0);
}

// ---------------------------------------------------------------------------
// Inventory job
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inventory_job_lists_archives() {
    let svc = service();
    make_vault(&svc, "v").await;
    let id = upload(&svc, "v", b"abc").await;
    let job_resp = call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/jobs",
            json!({ "Type": "inventory-retrieval" }),
        ),
    )
    .await;
    assert_eq!(job_resp.status.as_u16(), 202);
    let job_id = header_of(&job_resp, "x-amz-job-id").unwrap();

    // DescribeJob settles it to Succeeded.
    let desc = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/-/vaults/v/jobs/{job_id}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(desc["Action"], "InventoryRetrieval");
    assert_eq!(desc["StatusCode"], "Succeeded");
    assert_eq!(desc["Completed"], true);

    let out = call(
        &svc,
        req(
            Method::GET,
            &format!("/-/vaults/v/jobs/{job_id}/output"),
            Value::Null,
        ),
    )
    .await;
    let inv: Value = serde_json::from_slice(&raw_body(&out)).unwrap();
    let list = inv["ArchiveList"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["ArchiveId"], id);
}

#[tokio::test]
async fn list_jobs_filters_by_completed() {
    let svc = service();
    make_vault(&svc, "v").await;
    call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/jobs",
            json!({ "Type": "inventory-retrieval" }),
        ),
    )
    .await;
    let l = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/jobs?completed=true", Value::Null),
        )
        .await,
    );
    // Jobs settle to Succeeded on the list read, so completed=true matches.
    assert_eq!(l["JobList"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn retrieval_of_missing_archive_is_not_found() {
    let svc = service();
    make_vault(&svc, "v").await;
    let (status, code) = err_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/-/vaults/v/jobs",
                json!({ "Type": "archive-retrieval", "ArchiveId": "nope" }),
            ),
        )
        .await,
    );
    assert_eq!(status, 404);
    assert_eq!(code, "ResourceNotFoundException");
}

// ---------------------------------------------------------------------------
// Multipart upload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multipart_assembles_into_archive() {
    let svc = service();
    make_vault(&svc, "v").await;
    let one_mib = 1024 * 1024;

    let init = call(
        &svc,
        req_raw(
            Method::POST,
            "/-/vaults/v/multipart-uploads",
            Bytes::new(),
            &[("x-amz-part-size", &one_mib.to_string())],
        ),
    )
    .await;
    assert_eq!(init.status.as_u16(), 201);
    let upload_id = header_of(&init, "x-amz-multipart-upload-id").unwrap();

    // Two parts: first a full 1 MiB, then a short tail.
    let part0 = vec![0xAAu8; one_mib];
    let part1 = vec![0xBBu8; 100];
    let up0 = call(
        &svc,
        req_raw(
            Method::PUT,
            &format!("/-/vaults/v/multipart-uploads/{upload_id}"),
            Bytes::from(part0.clone()),
            &[("Content-Range", &format!("bytes 0-{}/*", one_mib - 1))],
        ),
    )
    .await;
    assert_eq!(up0.status.as_u16(), 204);
    call(
        &svc,
        req_raw(
            Method::PUT,
            &format!("/-/vaults/v/multipart-uploads/{upload_id}"),
            Bytes::from(part1.clone()),
            &[(
                "Content-Range",
                &format!("bytes {}-{}/*", one_mib, one_mib + 99),
            )],
        ),
    )
    .await;

    // ListParts shows two parts.
    let parts = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/-/vaults/v/multipart-uploads/{upload_id}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(parts["Parts"].as_array().unwrap().len(), 2);

    // ListMultipartUploads shows the in-flight upload.
    let uploads = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/multipart-uploads", Value::Null),
        )
        .await,
    );
    assert_eq!(uploads["UploadsList"].as_array().unwrap().len(), 1);

    let total = one_mib + 100;
    let complete = call(
        &svc,
        req_raw(
            Method::POST,
            &format!("/-/vaults/v/multipart-uploads/{upload_id}"),
            Bytes::new(),
            &[("x-amz-archive-size", &total.to_string())],
        ),
    )
    .await;
    assert_eq!(complete.status.as_u16(), 201);
    assert!(header_of(&complete, "x-amz-archive-id").is_some());

    // Archive now exists; the upload is gone.
    let d = body_of(&call(&svc, req(Method::GET, "/-/vaults/v", Value::Null)).await);
    assert_eq!(d["NumberOfArchives"], 1);
    assert_eq!(d["SizeInBytes"], total as u64);
    let uploads = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/multipart-uploads", Value::Null),
        )
        .await,
    );
    assert_eq!(uploads["UploadsList"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn multipart_bad_part_size_rejected() {
    let svc = service();
    make_vault(&svc, "v").await;
    let (status, code) = err_of(
        &call(
            &svc,
            req_raw(
                Method::POST,
                "/-/vaults/v/multipart-uploads",
                Bytes::new(),
                &[("x-amz-part-size", "3000000")],
            ),
        )
        .await,
    );
    assert_eq!(status, 400);
    assert_eq!(code, "InvalidParameterValueException");
}

#[tokio::test]
async fn abort_multipart_removes_upload() {
    let svc = service();
    make_vault(&svc, "v").await;
    let init = call(
        &svc,
        req_raw(
            Method::POST,
            "/-/vaults/v/multipart-uploads",
            Bytes::new(),
            &[("x-amz-part-size", "1048576")],
        ),
    )
    .await;
    let upload_id = header_of(&init, "x-amz-multipart-upload-id").unwrap();
    let abort = call(
        &svc,
        req(
            Method::DELETE,
            &format!("/-/vaults/v/multipart-uploads/{upload_id}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(abort.status.as_u16(), 204);
    let uploads = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/multipart-uploads", Value::Null),
        )
        .await,
    );
    assert_eq!(uploads["UploadsList"].as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// Vault lock state machine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vault_lock_state_machine() {
    let svc = service();
    make_vault(&svc, "v").await;

    // Initiate -> InProgress, returns a lock id.
    let init = call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/lock-policy",
            json!({ "Policy": "{}" }),
        ),
    )
    .await;
    assert_eq!(init.status.as_u16(), 201);
    let lock_id = header_of(&init, "x-amz-lock-id").unwrap();

    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/lock-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(g["State"], "InProgress");
    assert!(g["ExpirationDate"].is_string());

    // Initiating again while in progress is rejected.
    let (status, _) = err_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/-/vaults/v/lock-policy",
                json!({ "Policy": "{}" }),
            ),
        )
        .await,
    );
    assert_eq!(status, 409);

    // Complete with the wrong id -> error.
    let (bad, _) = err_of(
        &call(
            &svc,
            req(Method::POST, "/-/vaults/v/lock-policy/wrong", Value::Null),
        )
        .await,
    );
    assert_eq!(bad, 400);

    // Complete with the right id -> Locked.
    let done = call(
        &svc,
        req(
            Method::POST,
            &format!("/-/vaults/v/lock-policy/{lock_id}"),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(done.status.as_u16(), 204);
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/lock-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(g["State"], "Locked");

    // A locked vault cannot be aborted.
    let (abort_status, _) = err_of(
        &call(
            &svc,
            req(Method::DELETE, "/-/vaults/v/lock-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(abort_status, 400);
}

#[tokio::test]
async fn get_vault_lock_without_lock_is_not_found() {
    let svc = service();
    make_vault(&svc, "v").await;
    let (status, code) = err_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/lock-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(status, 404);
    assert_eq!(code, "ResourceNotFoundException");
}

// ---------------------------------------------------------------------------
// Notifications + access policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn notifications_set_get_delete() {
    let svc = service();
    make_vault(&svc, "v").await;
    let cfg = json!({ "SNSTopic": "arn:aws:sns:us-east-1:000000000000:t", "Events": ["ArchiveRetrievalCompleted"] });
    let set = call(
        &svc,
        req(Method::PUT, "/-/vaults/v/notification-configuration", cfg),
    )
    .await;
    assert_eq!(set.status.as_u16(), 204);
    let g = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/-/vaults/v/notification-configuration",
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(g["SNSTopic"], "arn:aws:sns:us-east-1:000000000000:t");
    assert_eq!(g["Events"].as_array().unwrap().len(), 1);
    call(
        &svc,
        req(
            Method::DELETE,
            "/-/vaults/v/notification-configuration",
            Value::Null,
        ),
    )
    .await;
    let (status, _) = err_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/-/vaults/v/notification-configuration",
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(status, 404);
}

#[tokio::test]
async fn access_policy_set_get_delete() {
    let svc = service();
    make_vault(&svc, "v").await;
    let set = call(
        &svc,
        req(
            Method::PUT,
            "/-/vaults/v/access-policy",
            json!({ "Policy": "{\"Version\":\"2012-10-17\"}" }),
        ),
    )
    .await;
    assert_eq!(set.status.as_u16(), 204);
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/access-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(g["Policy"], "{\"Version\":\"2012-10-17\"}");
    call(
        &svc,
        req(Method::DELETE, "/-/vaults/v/access-policy", Value::Null),
    )
    .await;
    let (status, _) = err_of(
        &call(
            &svc,
            req(Method::GET, "/-/vaults/v/access-policy", Value::Null),
        )
        .await,
    );
    assert_eq!(status, 404);
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tags_add_list_remove() {
    let svc = service();
    make_vault(&svc, "v").await;
    call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/tags?operation=add",
            json!({ "Tags": { "env": "prod", "team": "core" } }),
        ),
    )
    .await;
    let l = body_of(&call(&svc, req(Method::GET, "/-/vaults/v/tags", Value::Null)).await);
    assert_eq!(l["Tags"]["env"], "prod");
    assert_eq!(l["Tags"]["team"], "core");

    call(
        &svc,
        req(
            Method::POST,
            "/-/vaults/v/tags?operation=remove",
            json!({ "TagKeys": ["env"] }),
        ),
    )
    .await;
    let l = body_of(&call(&svc, req(Method::GET, "/-/vaults/v/tags", Value::Null)).await);
    assert!(l["Tags"]["env"].is_null());
    assert_eq!(l["Tags"]["team"], "core");
}

// ---------------------------------------------------------------------------
// Data-retrieval policy + provisioned capacity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn data_retrieval_policy_default_and_set() {
    let svc = service();
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/policies/data-retrieval", Value::Null),
        )
        .await,
    );
    assert_eq!(g["Policy"]["Rules"][0]["Strategy"], "BytesPerHour");

    let set = call(
        &svc,
        req(
            Method::PUT,
            "/-/policies/data-retrieval",
            json!({ "Policy": { "Rules": [{ "Strategy": "BytesPerHour", "BytesPerHour": 10485760 }] } }),
        ),
    )
    .await;
    assert_eq!(set.status.as_u16(), 204);
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/policies/data-retrieval", Value::Null),
        )
        .await,
    );
    assert_eq!(g["Policy"]["Rules"][0]["Strategy"], "BytesPerHour");
}

#[tokio::test]
async fn provisioned_capacity_purchase_and_list() {
    let svc = service();
    let purchase = call(
        &svc,
        req(Method::POST, "/-/provisioned-capacity", Value::Null),
    )
    .await;
    assert_eq!(purchase.status.as_u16(), 201);
    assert!(header_of(&purchase, "x-amz-capacity-id").is_some());
    let l = body_of(
        &call(
            &svc,
            req(Method::GET, "/-/provisioned-capacity", Value::Null),
        )
        .await,
    );
    assert_eq!(l["ProvisionedCapacityList"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_vaults_paginates() {
    let svc = service();
    for i in 0..5 {
        make_vault(&svc, &format!("v{i}")).await;
    }
    let page1 = body_of(&call(&svc, req(Method::GET, "/-/vaults?limit=2", Value::Null)).await);
    assert_eq!(page1["VaultList"].as_array().unwrap().len(), 2);
    let marker = page1["Marker"].as_str().unwrap().to_string();
    let page2 = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/-/vaults?limit=2&marker={marker}"),
                Value::Null,
            ),
        )
        .await,
    );
    assert_eq!(page2["VaultList"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn unroutable_path_returns_aws_error() {
    let svc = service();
    let (status, code) = err_of(&call(&svc, req(Method::GET, "/-/bogus/thing", Value::Null)).await);
    assert_eq!(status, 400);
    assert_eq!(code, "InvalidParameterValueException");
}
