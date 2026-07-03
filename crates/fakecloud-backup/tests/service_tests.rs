//! End-to-end handler tests for the AWS Backup service.
//!
//! Every test drives [`BackupService::handle`] with a hand-built restJson1
//! `AwsRequest`, proving real round-trip behaviour: create -> get/describe/list
//! reflect persisted state, update persists, delete removes, jobs progress to a
//! terminal state and their recovery point resolves, list filters apply, and
//! the documented error codes fire.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};

use fakecloud_backup::{BackupService, SharedBackupState};
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService};

fn service() -> BackupService {
    let state: SharedBackupState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    BackupService::new(state)
}

fn req(method: Method, path: &str, body: Value) -> AwsRequest {
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
    AwsRequest {
        service: "backup".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "test".to_string(),
        headers: http::HeaderMap::new(),
        query_params,
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
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

async fn call(svc: &BackupService, r: AwsRequest) -> AwsResponse {
    svc.handle(r).await.expect("handler returned an error")
}

async fn call_err(svc: &BackupService, r: AwsRequest) -> (u16, String) {
    match svc.handle(r).await {
        Ok(_) => panic!("expected an error, got success"),
        Err(e) => (e.status().as_u16(), e.code().to_string()),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    let bytes = match &resp.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => b.clone(),
        _ => panic!("non-bytes body"),
    };
    if bytes.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&bytes).unwrap()
}

// helper: create a vault named `name`, return its ARN
async fn make_vault(svc: &BackupService, name: &str) -> String {
    let resp = call(
        svc,
        req(Method::PUT, &format!("/backup-vaults/{name}"), json!({})),
    )
    .await;
    body_of(&resp)["BackupVaultArn"]
        .as_str()
        .unwrap()
        .to_string()
}

// ---------------------------------------------------------------------------
// Backup vaults
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vault_create_describe_list_delete_roundtrip() {
    let svc = service();
    let arn = make_vault(&svc, "myvault").await;
    assert!(arn.contains(":backup-vault:myvault"));

    let d = body_of(&call(&svc, req(Method::GET, "/backup-vaults/myvault", json!({}))).await);
    assert_eq!(d["BackupVaultName"], "myvault");
    assert_eq!(d["VaultState"], "AVAILABLE");
    assert_eq!(d["NumberOfRecoveryPoints"], 0);

    let l = body_of(&call(&svc, req(Method::GET, "/backup-vaults", json!({}))).await);
    assert_eq!(l["BackupVaultList"].as_array().unwrap().len(), 1);

    call(
        &svc,
        req(Method::DELETE, "/backup-vaults/myvault", json!({})),
    )
    .await;
    let (code, err) = call_err(&svc, req(Method::GET, "/backup-vaults/myvault", json!({}))).await;
    assert_eq!(code, 400);
    assert_eq!(err, "ResourceNotFoundException");
}

#[tokio::test]
async fn vault_duplicate_is_already_exists() {
    let svc = service();
    make_vault(&svc, "dup").await;
    let (code, err) = call_err(&svc, req(Method::PUT, "/backup-vaults/dup", json!({}))).await;
    assert_eq!(code, 400);
    assert_eq!(err, "AlreadyExistsException");
}

#[tokio::test]
async fn vault_invalid_name_rejected() {
    let svc = service();
    let (code, err) = call_err(&svc, req(Method::PUT, "/backup-vaults/a", json!({}))).await;
    assert_eq!(code, 400);
    assert_eq!(err, "InvalidParameterValueException");
}

#[tokio::test]
async fn vault_encryption_key_echoes_on_describe() {
    let svc = service();
    call(
        &svc,
        req(
            Method::PUT,
            "/backup-vaults/enc",
            json!({ "EncryptionKeyArn": "arn:aws:kms:us-east-1:000000000000:key/abc" }),
        ),
    )
    .await;
    let d = body_of(&call(&svc, req(Method::GET, "/backup-vaults/enc", json!({}))).await);
    assert_eq!(
        d["EncryptionKeyArn"],
        "arn:aws:kms:us-east-1:000000000000:key/abc"
    );
}

#[tokio::test]
async fn lag_vault_reports_retention() {
    let svc = service();
    let r = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                "/logically-air-gapped-backup-vaults/lag1",
                json!({ "MinRetentionDays": 7, "MaxRetentionDays": 30 }),
            ),
        )
        .await,
    );
    assert_eq!(r["VaultState"], "CREATING");
    let d = body_of(&call(&svc, req(Method::GET, "/backup-vaults/lag1", json!({}))).await);
    assert_eq!(d["VaultType"], "LOGICALLY_AIR_GAPPED_BACKUP_VAULT");
    assert_eq!(d["MinRetentionDays"], 7);
    assert_eq!(d["MaxRetentionDays"], 30);
}

// ---------------------------------------------------------------------------
// Backup plans + selections + versions
// ---------------------------------------------------------------------------

async fn make_plan(svc: &BackupService, name: &str) -> String {
    let resp = call(
        svc,
        req(
            Method::PUT,
            "/backup/plans",
            json!({ "BackupPlan": {
                "BackupPlanName": name,
                "Rules": [{ "RuleName": "daily", "TargetBackupVaultName": "v" }],
            }}),
        ),
    )
    .await;
    body_of(&resp)["BackupPlanId"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn plan_create_get_update_delete_roundtrip() {
    let svc = service();
    let id = make_plan(&svc, "plan-a").await;

    let g = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/backup/plans/{id}"), json!({})),
        )
        .await,
    );
    assert_eq!(g["BackupPlan"]["BackupPlanName"], "plan-a");
    assert_eq!(g["BackupPlanId"], id);

    // update -> new version id
    let orig_version = g["VersionId"].as_str().unwrap().to_string();
    let u = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                &format!("/backup/plans/{id}"),
                json!({ "BackupPlan": { "BackupPlanName": "plan-a2",
                    "Rules": [{ "RuleName": "daily", "TargetBackupVaultName": "v" }] } }),
            ),
        )
        .await,
    );
    assert_ne!(u["VersionId"].as_str().unwrap(), orig_version);

    let versions = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/backup/plans/{id}/versions"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        versions["BackupPlanVersionsList"].as_array().unwrap().len(),
        2
    );

    let del = body_of(
        &call(
            &svc,
            req(Method::DELETE, &format!("/backup/plans/{id}"), json!({})),
        )
        .await,
    );
    assert!(del["DeletionDate"].is_number());
}

#[tokio::test]
async fn plan_missing_is_not_found() {
    let svc = service();
    let (code, err) = call_err(&svc, req(Method::GET, "/backup/plans/nope", json!({}))).await;
    assert_eq!(code, 400);
    assert_eq!(err, "ResourceNotFoundException");
}

#[tokio::test]
async fn backup_selection_roundtrip() {
    let svc = service();
    let id = make_plan(&svc, "plan-sel").await;
    let created = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                &format!("/backup/plans/{id}/selections"),
                json!({ "BackupSelection": { "SelectionName": "sel1", "IamRoleArn": "arn:aws:iam::000000000000:role/r" },
                        "CreatorRequestId": "tok-1" }),
            ),
        )
        .await,
    );
    let sid = created["SelectionId"].as_str().unwrap().to_string();

    let g = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/backup/plans/{id}/selections/{sid}"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(g["BackupSelection"]["SelectionName"], "sel1");
    assert_eq!(g["CreatorRequestId"], "tok-1");

    let l = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/backup/plans/{id}/selections"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(l["BackupSelectionsList"].as_array().unwrap().len(), 1);

    call(
        &svc,
        req(
            Method::DELETE,
            &format!("/backup/plans/{id}/selections/{sid}"),
            json!({}),
        ),
    )
    .await;
    let l2 = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/backup/plans/{id}/selections"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(l2["BackupSelectionsList"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn backup_selection_on_missing_plan_is_invalid_param() {
    let svc = service();
    let (code, err) = call_err(
        &svc,
        req(
            Method::PUT,
            "/backup/plans/ghost/selections",
            json!({ "BackupSelection": { "SelectionName": "s", "IamRoleArn": "arn:..." } }),
        ),
    )
    .await;
    assert_eq!(code, 400);
    assert_eq!(err, "InvalidParameterValueException");
}

// ---------------------------------------------------------------------------
// Backup jobs -> recovery points -> describe/restore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_backup_job_creates_recovery_point_and_terminal_state() {
    let svc = service();
    make_vault(&svc, "jobvault").await;
    let started = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                "/backup-jobs",
                json!({
                    "BackupVaultName": "jobvault",
                    "ResourceArn": "arn:aws:ec2:us-east-1:000000000000:volume/vol-123",
                    "IamRoleArn": "arn:aws:iam::000000000000:role/backup",
                }),
            ),
        )
        .await,
    );
    let job_id = started["BackupJobId"].as_str().unwrap().to_string();
    let rp_arn = started["RecoveryPointArn"].as_str().unwrap().to_string();
    assert!(rp_arn.contains(":recovery-point:"));

    // DescribeBackupJob settles to COMPLETED
    let d = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/backup-jobs/{job_id}"), json!({})),
        )
        .await,
    );
    assert_eq!(d["State"], "COMPLETED");
    assert_eq!(d["PercentDone"], "100.0");

    // The synthetic recovery point resolves via DescribeRecoveryPoint
    let enc = percent(&rp_arn);
    let rp = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                &format!("/backup-vaults/jobvault/recovery-points/{enc}"),
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(rp["Status"], "COMPLETED");
    assert_eq!(rp["ResourceType"], "EBS");

    // Protected resource is now visible
    let arn_enc = percent("arn:aws:ec2:us-east-1:000000000000:volume/vol-123");
    let pr = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/resources/{arn_enc}"), json!({})),
        )
        .await,
    );
    assert_eq!(pr["ResourceType"], "EBS");
}

#[tokio::test]
async fn start_backup_job_missing_vault_is_not_found() {
    let svc = service();
    let (code, err) = call_err(
        &svc,
        req(
            Method::PUT,
            "/backup-jobs",
            json!({ "BackupVaultName": "ghost", "ResourceArn": "arn:aws:ec2:...:volume/v", "IamRoleArn": "arn:..." }),
        ),
    )
    .await;
    assert_eq!(code, 400);
    assert_eq!(err, "ResourceNotFoundException");
}

#[tokio::test]
async fn restore_job_resolves_recovery_point_and_completes() {
    let svc = service();
    make_vault(&svc, "rv").await;
    let started = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                "/backup-jobs",
                json!({ "BackupVaultName": "rv",
                        "ResourceArn": "arn:aws:ec2:us-east-1:000000000000:volume/vol-9",
                        "IamRoleArn": "arn:aws:iam::000000000000:role/b" }),
            ),
        )
        .await,
    );
    let rp = started["RecoveryPointArn"].as_str().unwrap().to_string();
    let rj = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                "/restore-jobs",
                json!({ "RecoveryPointArn": rp, "Metadata": { "k": "v" } }),
            ),
        )
        .await,
    );
    let rj_id = rj["RestoreJobId"].as_str().unwrap().to_string();
    let d = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/restore-jobs/{rj_id}"), json!({})),
        )
        .await,
    );
    assert_eq!(d["Status"], "COMPLETED");
    assert!(d["CreatedResourceArn"].is_string());
}

#[tokio::test]
async fn list_backup_jobs_filter_by_state() {
    let svc = service();
    make_vault(&svc, "fv").await;
    for i in 0..2 {
        call(
            &svc,
            req(
                Method::PUT,
                "/backup-jobs",
                json!({ "BackupVaultName": "fv",
                        "ResourceArn": format!("arn:aws:ec2:us-east-1:000000000000:volume/vol-{i}"),
                        "IamRoleArn": "arn:aws:iam::000000000000:role/b" }),
            ),
        )
        .await;
    }
    let completed = body_of(
        &call(
            &svc,
            req(Method::GET, "/backup-jobs?state=COMPLETED", json!({})),
        )
        .await,
    );
    assert_eq!(completed["BackupJobs"].as_array().unwrap().len(), 2);
    let running = body_of(
        &call(
            &svc,
            req(Method::GET, "/backup-jobs?state=RUNNING", json!({})),
        )
        .await,
    );
    assert_eq!(running["BackupJobs"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_backup_jobs_invalid_enum_rejected() {
    let svc = service();
    let (code, err) = call_err(
        &svc,
        req(Method::GET, "/backup-jobs?state=BOGUS", json!({})),
    )
    .await;
    assert_eq!(code, 400);
    assert_eq!(err, "InvalidParameterValueException");
}

#[tokio::test]
async fn list_max_results_out_of_range_rejected() {
    let svc = service();
    let (code, _) = call_err(
        &svc,
        req(Method::GET, "/backup-vaults?maxResults=0", json!({})),
    )
    .await;
    assert_eq!(code, 400);
    let (code2, _) = call_err(
        &svc,
        req(Method::GET, "/backup-vaults?maxResults=5000", json!({})),
    )
    .await;
    assert_eq!(code2, 400);
}

// ---------------------------------------------------------------------------
// Frameworks / report plans / legal holds / restore testing / tiering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn framework_roundtrip_and_name_validation() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            "/audit/frameworks",
            json!({ "FrameworkName": "MyFramework", "FrameworkDescription": "desc",
                    "FrameworkControls": [{ "ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN" }] }),
        ),
    )
    .await;
    let d = body_of(
        &call(
            &svc,
            req(Method::GET, "/audit/frameworks/MyFramework", json!({})),
        )
        .await,
    );
    assert_eq!(d["FrameworkName"], "MyFramework");
    assert_eq!(d["FrameworkDescription"], "desc");

    // Invalid name (starts with a digit) rejected.
    let (code, err) = call_err(
        &svc,
        req(
            Method::POST,
            "/audit/frameworks",
            json!({ "FrameworkName": "9bad", "FrameworkControls": [] }),
        ),
    )
    .await;
    assert_eq!(code, 400);
    assert_eq!(err, "InvalidParameterValueException");
}

#[tokio::test]
async fn report_plan_roundtrip() {
    let svc = service();
    call(
        &svc,
        req(
            Method::POST,
            "/audit/report-plans",
            json!({ "ReportPlanName": "MyReport",
                    "ReportDeliveryChannel": { "S3BucketName": "bkt" },
                    "ReportSetting": { "ReportTemplate": "BACKUP_JOB_REPORT" } }),
        ),
    )
    .await;
    let d = body_of(
        &call(
            &svc,
            req(Method::GET, "/audit/report-plans/MyReport", json!({})),
        )
        .await,
    );
    assert_eq!(d["ReportPlan"]["ReportPlanName"], "MyReport");
    // StartReportJob returns a report job id
    let j = body_of(
        &call(
            &svc,
            req(Method::POST, "/audit/report-jobs/MyReport", json!({})),
        )
        .await,
    );
    assert!(j["ReportJobId"].is_string());
}

#[tokio::test]
async fn legal_hold_create_get_cancel() {
    let svc = service();
    let created = body_of(
        &call(
            &svc,
            req(
                Method::POST,
                "/legal-holds",
                json!({ "Title": "Investigation", "Description": "hold everything" }),
            ),
        )
        .await,
    );
    let id = created["LegalHoldId"].as_str().unwrap().to_string();
    assert_eq!(created["Status"], "ACTIVE");

    let g = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/legal-holds/{id}"), json!({})),
        )
        .await,
    );
    assert_eq!(g["Title"], "Investigation");

    call(
        &svc,
        req(
            Method::DELETE,
            &format!("/legal-holds/{id}"),
            json!({ "CancelDescription": "done" }),
        ),
    )
    .await;
    let g2 = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/legal-holds/{id}"), json!({})),
        )
        .await,
    );
    assert_eq!(g2["Status"], "CANCELED");
}

#[tokio::test]
async fn restore_testing_plan_and_selection_roundtrip() {
    let svc = service();
    call(
        &svc,
        req(
            Method::PUT,
            "/restore-testing/plans",
            json!({ "RestoreTestingPlan": {
                "RestoreTestingPlanName": "rtp1",
                "ScheduleExpression": "cron(0 5 ? * * *)",
                "RecoveryPointSelection": { "Algorithm": "LATEST_WITHIN_WINDOW", "SelectionWindowDays": 30 },
            }}),
        ),
    )
    .await;
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/restore-testing/plans/rtp1", json!({})),
        )
        .await,
    );
    assert_eq!(g["RestoreTestingPlan"]["RestoreTestingPlanName"], "rtp1");
    assert!(g["RestoreTestingPlan"]["CreationTime"].is_number());

    call(
        &svc,
        req(
            Method::PUT,
            "/restore-testing/plans/rtp1/selections",
            json!({ "RestoreTestingSelection": {
                "RestoreTestingSelectionName": "sel1",
                "ProtectedResourceType": "EBS",
                "IamRoleArn": "arn:aws:iam::000000000000:role/r",
            }}),
        ),
    )
    .await;
    let gs = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/restore-testing/plans/rtp1/selections/sel1",
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(
        gs["RestoreTestingSelection"]["RestoreTestingSelectionName"],
        "sel1"
    );

    let ls = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/restore-testing/plans/rtp1/selections",
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(ls["RestoreTestingSelections"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn tiering_configuration_roundtrip() {
    let svc = service();
    call(
        &svc,
        req(
            Method::PUT,
            "/tiering-configurations",
            json!({ "TieringConfiguration": {
                "TieringConfigurationName": "tier1",
                "ResourceType": "EBS",
            }}),
        ),
    )
    .await;
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/tiering-configurations/tier1", json!({})),
        )
        .await,
    );
    assert_eq!(
        g["TieringConfiguration"]["TieringConfigurationName"],
        "tier1"
    );
    let l = body_of(&call(&svc, req(Method::GET, "/tiering-configurations", json!({}))).await);
    assert_eq!(l["TieringConfigurations"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Tags, notifications, access policy, settings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tags_roundtrip() {
    let svc = service();
    let arn = make_vault(&svc, "tagvault").await;
    let enc = percent(&arn);
    call(
        &svc,
        req(
            Method::POST,
            &format!("/tags/{enc}"),
            json!({ "Tags": { "env": "prod", "team": "core" } }),
        ),
    )
    .await;
    let l = body_of(&call(&svc, req(Method::GET, &format!("/tags/{enc}"), json!({}))).await);
    assert_eq!(l["Tags"]["env"], "prod");
    assert_eq!(l["Tags"]["team"], "core");

    call(
        &svc,
        req(
            Method::POST,
            &format!("/untag/{enc}"),
            json!({ "TagKeyList": ["env"] }),
        ),
    )
    .await;
    let l2 = body_of(&call(&svc, req(Method::GET, &format!("/tags/{enc}"), json!({}))).await);
    assert!(l2["Tags"].get("env").is_none());
    assert_eq!(l2["Tags"]["team"], "core");
}

#[tokio::test]
async fn vault_notifications_roundtrip() {
    let svc = service();
    make_vault(&svc, "nv").await;
    call(
        &svc,
        req(
            Method::PUT,
            "/backup-vaults/nv/notification-configuration",
            json!({ "SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:t", "BackupVaultEvents": ["BACKUP_JOB_COMPLETED"] }),
        ),
    )
    .await;
    let g = body_of(
        &call(
            &svc,
            req(
                Method::GET,
                "/backup-vaults/nv/notification-configuration",
                json!({}),
            ),
        )
        .await,
    );
    assert_eq!(g["SNSTopicArn"], "arn:aws:sns:us-east-1:000000000000:t");
    assert_eq!(g["BackupVaultEvents"][0], "BACKUP_JOB_COMPLETED");
}

#[tokio::test]
async fn vault_access_policy_roundtrip() {
    let svc = service();
    make_vault(&svc, "pv").await;
    call(
        &svc,
        req(
            Method::PUT,
            "/backup-vaults/pv/access-policy",
            json!({ "Policy": "{\"Version\":\"2012-10-17\"}" }),
        ),
    )
    .await;
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/backup-vaults/pv/access-policy", json!({})),
        )
        .await,
    );
    assert_eq!(g["Policy"], "{\"Version\":\"2012-10-17\"}");
}

#[tokio::test]
async fn region_settings_roundtrip() {
    let svc = service();
    call(
        &svc,
        req(
            Method::PUT,
            "/account-settings",
            json!({ "ResourceTypeOptInPreference": { "EBS": false, "RDS": true } }),
        ),
    )
    .await;
    let g = body_of(&call(&svc, req(Method::GET, "/account-settings", json!({}))).await);
    assert_eq!(g["ResourceTypeOptInPreference"]["EBS"], false);
    assert_eq!(g["ResourceTypeOptInPreference"]["RDS"], true);
}

#[tokio::test]
async fn global_settings_roundtrip() {
    let svc = service();
    call(
        &svc,
        req(
            Method::PUT,
            "/global-settings",
            json!({ "GlobalSettings": { "isCrossAccountBackupEnabled": "true" } }),
        ),
    )
    .await;
    let g = body_of(&call(&svc, req(Method::GET, "/global-settings", json!({}))).await);
    assert_eq!(g["GlobalSettings"]["isCrossAccountBackupEnabled"], "true");
}

#[tokio::test]
async fn supported_resource_types_listed() {
    let svc = service();
    let g = body_of(
        &call(
            &svc,
            req(Method::GET, "/supported-resource-types", json!({})),
        )
        .await,
    );
    let types = g["ResourceTypes"].as_array().unwrap();
    assert!(types.iter().any(|t| t == "EBS"));
    assert!(types.iter().any(|t| t == "DynamoDB"));
}

#[tokio::test]
async fn copy_job_roundtrip() {
    let svc = service();
    make_vault(&svc, "src").await;
    let started = body_of(
        &call(
            &svc,
            req(
                Method::PUT,
                "/copy-jobs",
                json!({ "RecoveryPointArn": "arn:aws:backup:us-east-1:000000000000:recovery-point:x",
                        "SourceBackupVaultName": "src",
                        "DestinationBackupVaultArn": "arn:aws:backup:us-west-2:000000000000:backup-vault:dst",
                        "IamRoleArn": "arn:aws:iam::000000000000:role/b" }),
            ),
        )
        .await,
    );
    let id = started["CopyJobId"].as_str().unwrap().to_string();
    let d = body_of(
        &call(
            &svc,
            req(Method::GET, &format!("/copy-jobs/{id}"), json!({})),
        )
        .await,
    );
    assert_eq!(d["CopyJob"]["State"], "COMPLETED");
}

#[tokio::test]
async fn unknown_route_is_unknown_operation() {
    let svc = service();
    let (code, err) = call_err(&svc, req(Method::GET, "/does-not-exist", json!({}))).await;
    assert_eq!(code, 404);
    assert_eq!(err, "UnknownOperationException");
}

/// Percent-encode an ARN the way an AWS SDK encodes a path label (slashes and
/// colons escaped), so it stays a single URI segment.
fn percent(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
