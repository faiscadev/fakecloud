// Handler bodies for `BackupService`, `include!`d into `service.rs` so the
// free helpers below share its module scope. Split out only to keep the
// routing table and the per-operation logic in separate files.

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// A 200 OK JSON response.
fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

/// A JSON response with an explicit success status.
fn resp(code: u16, v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::from_u16(code).unwrap_or(StatusCode::OK), v)
}

/// An empty-body response with an explicit status (for `Unit`-output ops).
fn empty(code: u16) -> AwsResponse {
    AwsResponse::json(
        StatusCode::from_u16(code).unwrap_or(StatusCode::OK),
        Vec::new(),
    )
}

/// Epoch-seconds timestamp, the restJson1 default wire form.
fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn aws_err(code: &str, status: StatusCode, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, msg)
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ResourceNotFoundException", StatusCode::BAD_REQUEST, msg)
}
fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    aws_err(
        "InvalidParameterValueException",
        StatusCode::BAD_REQUEST,
        msg,
    )
}
fn missing_param(msg: impl Into<String>) -> AwsServiceError {
    aws_err(
        "MissingParameterValueException",
        StatusCode::BAD_REQUEST,
        msg,
    )
}
fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    aws_err("AlreadyExistsException", StatusCode::BAD_REQUEST, msg)
}
fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    aws_err("InvalidRequestException", StatusCode::BAD_REQUEST, msg)
}

fn req_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn req_str(body: &Value, key: &str) -> Result<String, AwsServiceError> {
    str_field(body, key).ok_or_else(|| missing_param(format!("{key} is required")))
}

fn parse_tags(v: Option<&Value>) -> TagMap {
    let mut out = TagMap::new();
    if let Some(Value::Object(m)) = v {
        for (k, val) in m {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn tag_map_to_value(t: &TagMap) -> Value {
    Value::Object(
        t.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

fn max_results(req: &AwsRequest) -> usize {
    req.query_params
        .get("maxResults")
        .or_else(|| req.query_params.get("MaxResults"))
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1000)
}

fn next_token(req: &AwsRequest) -> Option<String> {
    req.query_params
        .get("nextToken")
        .or_else(|| req.query_params.get("NextToken"))
        .cloned()
}

/// Paginate a list of JSON items, appending `nextToken` under the given key
/// only when there is another page. `list_key` names the output list member.
fn page_response(
    items: Vec<Value>,
    list_key: &str,
    req: &AwsRequest,
    extra: &[(&str, Value)],
) -> Result<AwsResponse, AwsServiceError> {
    let (page, token) = paginate_checked(&items, next_token(req).as_deref(), max_results(req))
        .map_err(|_| invalid_param("Invalid nextToken"))?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(t) = token {
        out.insert("NextToken".to_string(), Value::String(t));
    }
    for (k, v) in extra {
        out.insert((*k).to_string(), v.clone());
    }
    Ok(ok(Value::Object(out)))
}

fn gen_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---- validation helpers -------------------------------------------------

/// Enum-valued query filters per list op: `(query-param, allowed-values)`.
/// Derived from the Smithy model's `@enum` targets on `@httpQuery` members.
fn enum_query_params(action: &str) -> &'static [(&'static str, &'static [&'static str])] {
    const BACKUP_JOB_STATE: &[&str] = &[
        "CREATED",
        "PENDING",
        "RUNNING",
        "ABORTING",
        "ABORTED",
        "COMPLETED",
        "FAILED",
        "EXPIRED",
        "PARTIAL",
    ];
    const BACKUP_JOB_STATUS: &[&str] = &[
        "CREATED",
        "PENDING",
        "RUNNING",
        "ABORTING",
        "ABORTED",
        "COMPLETED",
        "FAILED",
        "EXPIRED",
        "PARTIAL",
        "AGGREGATE_ALL",
        "ANY",
    ];
    const COPY_JOB_STATE: &[&str] = &["CREATED", "RUNNING", "COMPLETED", "FAILED", "PARTIAL"];
    const COPY_JOB_SUMMARY_STATE: &[&str] = &[
        "CREATED",
        "RUNNING",
        "ABORTING",
        "ABORTED",
        "COMPLETING",
        "COMPLETED",
        "FAILING",
        "FAILED",
        "PARTIAL",
        "AGGREGATE_ALL",
        "ANY",
    ];
    const RESTORE_JOB_STATUS: &[&str] = &["PENDING", "RUNNING", "COMPLETED", "ABORTED", "FAILED"];
    const RESTORE_JOB_SUMMARY_STATE: &[&str] = &[
        "CREATED",
        "PENDING",
        "RUNNING",
        "ABORTED",
        "COMPLETED",
        "FAILED",
        "AGGREGATE_ALL",
        "ANY",
    ];
    const SCAN_JOB_SUMMARY_STATE: &[&str] = &[
        "CREATED",
        "COMPLETED",
        "COMPLETED_WITH_ISSUES",
        "RUNNING",
        "FAILED",
        "CANCELED",
        "AGGREGATE_ALL",
        "ANY",
    ];
    const SCAN_JOB_STATE: &[&str] = &[
        "CANCELED",
        "COMPLETED",
        "COMPLETED_WITH_ISSUES",
        "CREATED",
        "FAILED",
        "RUNNING",
    ];
    const AGG_PERIOD: &[&str] = &["ONE_DAY", "SEVEN_DAYS", "FOURTEEN_DAYS"];
    const VAULT_TYPE: &[&str] = &[
        "BACKUP_VAULT",
        "LOGICALLY_AIR_GAPPED_BACKUP_VAULT",
        "RESTORE_ACCESS_BACKUP_VAULT",
    ];
    const INDEX_STATUS: &[&str] = &["PENDING", "ACTIVE", "FAILED", "DELETING"];
    const MALWARE_SCANNER: &[&str] = &["GUARDDUTY"];
    const SCAN_RESULT_STATUS: &[&str] = &["NO_THREATS_FOUND", "THREATS_FOUND", "UNKNOWN"];
    const SCAN_RESOURCE_TYPE: &[&str] = &["EBS", "EC2", "S3"];
    match action {
        "ListBackupJobs" => &[("state", BACKUP_JOB_STATE)],
        "ListBackupJobSummaries" => &[
            ("State", BACKUP_JOB_STATUS),
            ("AggregationPeriod", AGG_PERIOD),
        ],
        "ListCopyJobs" => &[("state", COPY_JOB_STATE)],
        "ListCopyJobSummaries" => &[
            ("State", COPY_JOB_SUMMARY_STATE),
            ("AggregationPeriod", AGG_PERIOD),
        ],
        "ListBackupVaults" => &[("vaultType", VAULT_TYPE)],
        "ListIndexedRecoveryPoints" => &[("indexStatus", INDEX_STATUS)],
        "ListRestoreJobs" | "ListRestoreJobsByProtectedResource" => {
            &[("status", RESTORE_JOB_STATUS)]
        }
        "ListRestoreJobSummaries" => &[
            ("State", RESTORE_JOB_SUMMARY_STATE),
            ("AggregationPeriod", AGG_PERIOD),
        ],
        "ListScanJobSummaries" => &[
            ("MalwareScanner", MALWARE_SCANNER),
            ("ScanResultStatus", SCAN_RESULT_STATUS),
            ("State", SCAN_JOB_SUMMARY_STATE),
            ("AggregationPeriod", AGG_PERIOD),
        ],
        "ListScanJobs" => &[
            ("ByMalwareScanner", MALWARE_SCANNER),
            ("ByResourceType", SCAN_RESOURCE_TYPE),
            ("ByScanResultStatus", SCAN_RESULT_STATUS),
            ("ByState", SCAN_JOB_STATE),
        ],
        "GetPITRMalwareScanResults" => &[("MalwareScanner", MALWARE_SCANNER)],
        _ => &[],
    }
}

/// Validate the shared `MaxResults` range (1..=1000) and enum-valued query
/// filters up front. All affected ops declare `InvalidParameterValueException`.
fn validate_query_constraints(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    for key in ["maxResults", "MaxResults"] {
        if let Some(raw) = req.query_params.get(key) {
            let n: i64 = raw
                .parse()
                .map_err(|_| invalid_param(format!("{key} must be an integer")))?;
            if !(1..=1000).contains(&n) {
                return Err(invalid_param(format!("{key} must be between 1 and 1000")));
            }
        }
    }
    for (param, allowed) in enum_query_params(action) {
        if let Some(val) = req.query_params.get(*param) {
            if !allowed.contains(&val.as_str()) {
                return Err(invalid_param(format!("Invalid value '{val}' for {param}")));
            }
        }
    }
    Ok(())
}

/// AWS `BackupVaultName` pattern: `^[a-zA-Z0-9\-\_]{2,50}$`.
fn valid_vault_name(n: &str) -> bool {
    (2..=50).contains(&n.len())
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// AWS framework/report-plan name: `^[a-zA-Z][_a-zA-Z0-9]*$`, length 1..=256.
fn valid_ident_name(n: &str) -> bool {
    (1..=256).contains(&n.len())
        && n.starts_with(|c: char| c.is_ascii_alphabetic())
        && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn check_vault_name(n: &str) -> Result<(), AwsServiceError> {
    if valid_vault_name(n) {
        Ok(())
    } else {
        Err(invalid_param(format!("Invalid backup vault name: {n}")))
    }
}

fn check_ident_name(n: &str, field: &str) -> Result<(), AwsServiceError> {
    if valid_ident_name(n) {
        Ok(())
    } else {
        Err(invalid_param(format!("Invalid {field}: {n}")))
    }
}

/// Reject an empty or probe-placeholder (`{Name}`) path label. Used by ops
/// whose identifier has no format constraint but still must be present.
fn check_label(v: &str, field: &str) -> Result<(), AwsServiceError> {
    if v.is_empty() || v.contains(['{', '}']) {
        return Err(invalid_param(format!("Invalid {field}: {v}")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl BackupService {
    // ===================== Backup plans =====================

    fn create_backup_plan(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let plan = body
            .get("BackupPlan")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("BackupPlan is required"))?;
        let id = gen_id();
        let version = gen_id().replace('-', "");
        let arn = plan_arn(&req.region, &req.account_id, &id);
        let now = Utc::now();
        let plan_name = plan
            .get("BackupPlanName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let advanced = plan
            .get("AdvancedBackupSettings")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let creator = str_field(&body, "CreatorRequestId");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let record = PlanRecord {
            id: id.clone(),
            arn: arn.clone(),
            version_id: version.clone(),
            creation_date: now,
            deletion_date: None,
            last_execution_date: None,
            creator_request_id: creator,
            plan,
            advanced_backup_settings: advanced.clone(),
            selections: Default::default(),
            versions: vec![PlanVersion {
                version_id: version.clone(),
                creation_date: now,
                deletion_date: None,
                plan_name,
            }],
        };
        st.plans.insert(id.clone(), record);
        if let Some(tags) = body.get("BackupPlanTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({
            "BackupPlanId": id,
            "BackupPlanArn": arn,
            "CreationDate": ts(now),
            "VersionId": version,
            "AdvancedBackupSettings": advanced,
        })))
    }

    fn get_backup_plan(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let plan = st
            .and_then(|s| s.plans.get(id))
            .ok_or_else(|| not_found(format!("Backup plan not found: {id}")))?;
        Ok(ok(json!({
            "BackupPlan": plan.plan,
            "BackupPlanId": plan.id,
            "BackupPlanArn": plan.arn,
            "VersionId": plan.version_id,
            "CreatorRequestId": plan.creator_request_id,
            "CreationDate": ts(plan.creation_date),
            "AdvancedBackupSettings": plan.advanced_backup_settings,
        })))
    }

    fn update_backup_plan(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let new_plan = body
            .get("BackupPlan")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("BackupPlan is required"))?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let plan = st
            .plans
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Backup plan not found: {id}")))?;
        let version = gen_id().replace('-', "");
        let plan_name = new_plan
            .get("BackupPlanName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let advanced = new_plan
            .get("AdvancedBackupSettings")
            .cloned()
            .unwrap_or_else(|| json!([]));
        plan.plan = new_plan;
        plan.version_id = version.clone();
        plan.advanced_backup_settings = advanced.clone();
        plan.versions.push(PlanVersion {
            version_id: version.clone(),
            creation_date: now,
            deletion_date: None,
            plan_name,
        });
        Ok(ok(json!({
            "BackupPlanId": plan.id,
            "BackupPlanArn": plan.arn,
            "CreationDate": ts(plan.creation_date),
            "VersionId": version,
            "AdvancedBackupSettings": advanced,
        })))
    }

    fn delete_backup_plan(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let plan = st
            .plans
            .remove(id)
            .ok_or_else(|| not_found(format!("Backup plan not found: {id}")))?;
        Ok(ok(json!({
            "BackupPlanId": plan.id,
            "BackupPlanArn": plan.arn,
            "DeletionDate": ts(now),
            "VersionId": plan.version_id,
        })))
    }

    fn list_backup_plans(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.plans
                    .values()
                    .map(|p| {
                        json!({
                            "BackupPlanArn": p.arn,
                            "BackupPlanId": p.id,
                            "CreationDate": ts(p.creation_date),
                            "VersionId": p.version_id,
                            "BackupPlanName": p.plan.get("BackupPlanName"),
                            "CreatorRequestId": p.creator_request_id,
                            "AdvancedBackupSettings": p.advanced_backup_settings,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "BackupPlansList", req, &[])
    }

    fn list_backup_plan_versions(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let plan = accounts
            .get(&req.account_id)
            .and_then(|s| s.plans.get(id))
            .ok_or_else(|| not_found(format!("Backup plan not found: {id}")))?;
        let items: Vec<Value> = plan
            .versions
            .iter()
            .map(|v| {
                json!({
                    "BackupPlanArn": plan.arn,
                    "BackupPlanId": plan.id,
                    "CreationDate": ts(v.creation_date),
                    "VersionId": v.version_id,
                    "BackupPlanName": v.plan_name,
                    "DeletionDate": v.deletion_date.map(ts),
                })
            })
            .collect();
        page_response(items, "BackupPlanVersionsList", req, &[])
    }

    fn list_backup_plan_templates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let items = vec![json!({
            "BackupPlanTemplateId": "default-template",
            "BackupPlanTemplateName": "Default (Daily, 35-day retention)",
        })];
        page_response(items, "BackupPlanTemplatesList", req, &[])
    }

    fn get_backup_plan_from_json(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let raw = req_str(&body, "BackupPlanTemplateJson")?;
        let plan: Value = serde_json::from_str(&raw)
            .map_err(|e| invalid_request(format!("Invalid BackupPlanTemplateJson: {e}")))?;
        Ok(ok(json!({ "BackupPlan": plan })))
    }

    fn get_backup_plan_from_template(
        &self,
        _req: &AwsRequest,
        tid: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if tid != "default-template" {
            return Err(not_found(format!("Backup plan template not found: {tid}")));
        }
        Ok(ok(json!({
            "BackupPlanDocument": {
                "BackupPlanName": "Default",
                "Rules": [{
                    "RuleName": "DailyBackups",
                    "TargetBackupVaultName": "Default",
                    "ScheduleExpression": "cron(0 5 ? * * *)",
                    "StartWindowMinutes": 480,
                    "CompletionWindowMinutes": 10080,
                    "Lifecycle": { "DeleteAfterDays": 35 },
                }],
            }
        })))
    }

    fn export_backup_plan_template(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let plan = accounts
            .get(&req.account_id)
            .and_then(|s| s.plans.get(id))
            .ok_or_else(|| not_found(format!("Backup plan not found: {id}")))?;
        let doc = json!({ "BackupPlan": plan.plan });
        Ok(ok(json!({
            "BackupPlanTemplateJson": doc.to_string(),
        })))
    }

    // ===================== Backup selections =====================

    fn create_backup_selection(
        &self,
        req: &AwsRequest,
        plan_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let selection = body
            .get("BackupSelection")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("BackupSelection is required"))?;
        let creator = str_field(&body, "CreatorRequestId");
        let now = Utc::now();
        let sel_id = gen_id();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let plan = st
            .plans
            .get_mut(plan_id)
            // CreateBackupSelection does not declare ResourceNotFoundException.
            .ok_or_else(|| invalid_param(format!("Backup plan not found: {plan_id}")))?;
        plan.selections.insert(
            sel_id.clone(),
            SelectionRecord {
                selection_id: sel_id.clone(),
                creation_date: now,
                creator_request_id: creator,
                selection,
            },
        );
        Ok(ok(json!({
            "SelectionId": sel_id,
            "BackupPlanId": plan_id,
            "CreationDate": ts(now),
        })))
    }

    fn get_backup_selection(
        &self,
        req: &AwsRequest,
        plan_id: &str,
        sel_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let sel = accounts
            .get(&req.account_id)
            .and_then(|s| s.plans.get(plan_id))
            .and_then(|p| p.selections.get(sel_id))
            .ok_or_else(|| not_found(format!("Backup selection not found: {sel_id}")))?;
        Ok(ok(json!({
            "BackupSelection": sel.selection,
            "SelectionId": sel.selection_id,
            "BackupPlanId": plan_id,
            "CreationDate": ts(sel.creation_date),
            "CreatorRequestId": sel.creator_request_id,
        })))
    }

    fn delete_backup_selection(
        &self,
        req: &AwsRequest,
        plan_id: &str,
        sel_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let plan = st
            .plans
            .get_mut(plan_id)
            .ok_or_else(|| not_found(format!("Backup plan not found: {plan_id}")))?;
        plan.selections
            .remove(sel_id)
            .ok_or_else(|| not_found(format!("Backup selection not found: {sel_id}")))?;
        Ok(empty(200))
    }

    fn list_backup_selections(
        &self,
        req: &AwsRequest,
        plan_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let plan = accounts
            .get(&req.account_id)
            .and_then(|s| s.plans.get(plan_id))
            .ok_or_else(|| not_found(format!("Backup plan not found: {plan_id}")))?;
        let items: Vec<Value> = plan
            .selections
            .values()
            .map(|s| {
                json!({
                    "SelectionId": s.selection_id,
                    "SelectionName": s.selection.get("SelectionName"),
                    "BackupPlanId": plan_id,
                    "CreationDate": ts(s.creation_date),
                    "CreatorRequestId": s.creator_request_id,
                    "IamRoleArn": s.selection.get("IamRoleArn"),
                })
            })
            .collect();
        page_response(items, "BackupSelectionsList", req, &[])
    }

    // ===================== Backup vaults =====================

    fn create_backup_vault(
        &self,
        req: &AwsRequest,
        name: &str,
        vault_type: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_vault_name(name)?;
        let body = req_body(req);
        let now = Utc::now();
        let arn = vault_arn(&req.region, &req.account_id, name);
        let encryption = str_field(&body, "EncryptionKeyArn");
        let creator = str_field(&body, "CreatorRequestId");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.vaults.contains_key(name) {
            return Err(already_exists(format!(
                "Backup vault already exists: {name}"
            )));
        }
        st.vaults.insert(
            name.to_string(),
            VaultRecord {
                name: name.to_string(),
                arn: arn.clone(),
                vault_type: vault_type.to_string(),
                vault_state: "AVAILABLE".to_string(),
                encryption_key_arn: encryption.clone(),
                creation_date: now,
                creator_request_id: creator,
                min_retention_days: None,
                max_retention_days: None,
                locked: false,
                lock_date: None,
                changeable_for_days: None,
                source_backup_vault_arn: None,
                access_policy: None,
                notifications: None,
                recovery_points: Default::default(),
            },
        );
        if let Some(tags) = body.get("BackupVaultTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({
            "BackupVaultName": name,
            "BackupVaultArn": arn,
            "CreationDate": ts(now),
        })))
    }

    fn create_lag_vault(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_vault_name(name)?;
        let body = req_body(req);
        let now = Utc::now();
        let arn = vault_arn(&req.region, &req.account_id, name);
        let min = body.get("MinRetentionDays").and_then(|v| v.as_i64());
        let max = body.get("MaxRetentionDays").and_then(|v| v.as_i64());
        let encryption = str_field(&body, "EncryptionKeyArn");
        let creator = str_field(&body, "CreatorRequestId");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.vaults.contains_key(name) {
            return Err(already_exists(format!(
                "Backup vault already exists: {name}"
            )));
        }
        st.vaults.insert(
            name.to_string(),
            VaultRecord {
                name: name.to_string(),
                arn: arn.clone(),
                vault_type: "LOGICALLY_AIR_GAPPED_BACKUP_VAULT".to_string(),
                vault_state: "CREATING".to_string(),
                encryption_key_arn: encryption,
                creation_date: now,
                creator_request_id: creator,
                min_retention_days: min,
                max_retention_days: max,
                locked: true,
                lock_date: None,
                changeable_for_days: None,
                source_backup_vault_arn: None,
                access_policy: None,
                notifications: None,
                recovery_points: Default::default(),
            },
        );
        if let Some(tags) = body.get("BackupVaultTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({
            "BackupVaultName": name,
            "BackupVaultArn": arn,
            "CreationDate": ts(now),
            "VaultState": "CREATING",
        })))
    }

    fn create_restore_access_vault(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let source = req_str(&body, "SourceBackupVaultArn")?;
        let now = Utc::now();
        let name = str_field(&body, "BackupVaultName")
            .unwrap_or_else(|| format!("restore-access-{}", gen_id()));
        let arn = vault_arn(&req.region, &req.account_id, &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.vaults.insert(
            name.clone(),
            VaultRecord {
                name: name.clone(),
                arn: arn.clone(),
                vault_type: "RESTORE_ACCESS_BACKUP_VAULT".to_string(),
                vault_state: "CREATING".to_string(),
                encryption_key_arn: None,
                creation_date: now,
                creator_request_id: str_field(&body, "CreatorRequestId"),
                min_retention_days: None,
                max_retention_days: None,
                locked: false,
                lock_date: None,
                changeable_for_days: None,
                source_backup_vault_arn: Some(source),
                access_policy: None,
                notifications: None,
                recovery_points: Default::default(),
            },
        );
        Ok(ok(json!({
            "RestoreAccessBackupVaultArn": arn,
            "VaultState": "CREATING",
            "RestoreAccessBackupVaultName": name,
            "CreationDate": ts(now),
        })))
    }

    fn describe_backup_vault(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        // Settle CREATING -> AVAILABLE on first describe.
        if v.vault_state == "CREATING" {
            v.vault_state = "AVAILABLE".to_string();
        }
        Ok(ok(vault_describe_json(v)))
    }

    fn delete_backup_vault(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        if !v.recovery_points.is_empty() {
            return Err(invalid_request(
                "Backup vault cannot be deleted while it contains recovery points",
            ));
        }
        st.vaults.remove(name);
        Ok(empty(200))
    }

    fn list_backup_vaults(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let by_type = req.query_params.get("vaultType").cloned();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.vaults
                    .values()
                    .filter(|v| by_type.as_deref().is_none_or(|t| t == v.vault_type))
                    .map(vault_list_json)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "BackupVaultList", req, &[])
    }

    fn list_restore_access_vaults(
        &self,
        req: &AwsRequest,
        source_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_label(source_name, "BackupVaultName")?;
        let src_arn = vault_arn(&req.region, &req.account_id, source_name);
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.vaults
                    .values()
                    .filter(|v| v.source_backup_vault_arn.as_deref() == Some(src_arn.as_str()))
                    .map(|v| {
                        json!({
                            "RestoreAccessBackupVaultArn": v.arn,
                            "CreationDate": ts(v.creation_date),
                            "ApprovalDate": ts(v.creation_date),
                            "VaultState": v.vault_state,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "RestoreAccessBackupVaults", req, &[])
    }

    fn revoke_restore_access_vault(
        &self,
        req: &AwsRequest,
        _source_name: &str,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let name = st
            .vaults
            .iter()
            .find(|(_, v)| v.arn == arn)
            .map(|(k, _)| k.clone());
        match name {
            Some(n) => {
                st.vaults.remove(&n);
                Ok(empty(200))
            }
            None => Err(not_found(format!("Restore access vault not found: {arn}"))),
        }
    }

    fn put_vault_access_policy(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let policy = str_field(&body, "Policy");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        v.access_policy = policy;
        Ok(empty(200))
    }

    fn get_vault_access_policy(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(name))
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        let policy = v
            .access_policy
            .clone()
            .ok_or_else(|| not_found(format!("No access policy on vault: {name}")))?;
        Ok(ok(json!({
            "BackupVaultName": v.name,
            "BackupVaultArn": v.arn,
            "Policy": policy,
        })))
    }

    fn delete_vault_access_policy(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        v.access_policy = None;
        Ok(empty(200))
    }

    fn put_vault_lock(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        v.min_retention_days = body.get("MinRetentionDays").and_then(|x| x.as_i64());
        v.max_retention_days = body.get("MaxRetentionDays").and_then(|x| x.as_i64());
        v.changeable_for_days = body.get("ChangeableForDays").and_then(|x| x.as_i64());
        v.locked = v.changeable_for_days.unwrap_or(0) == 0;
        v.lock_date = v
            .changeable_for_days
            .map(|d| Utc::now() + chrono::Duration::days(d));
        Ok(empty(200))
    }

    fn delete_vault_lock(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        if v.locked {
            return Err(invalid_request(
                "Vault lock is in compliance mode and cannot be removed",
            ));
        }
        v.min_retention_days = None;
        v.max_retention_days = None;
        v.changeable_for_days = None;
        v.lock_date = None;
        Ok(empty(200))
    }

    fn put_vault_notifications(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let topic = req_str(&body, "SNSTopicArn")?;
        let events = body
            .get("BackupVaultEvents")
            .cloned()
            .ok_or_else(|| missing_param("BackupVaultEvents is required"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        v.notifications = Some(json!({ "SNSTopicArn": topic, "BackupVaultEvents": events }));
        Ok(empty(200))
    }

    fn get_vault_notifications(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(name))
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        let n = v
            .notifications
            .clone()
            .ok_or_else(|| not_found(format!("No notifications on vault: {name}")))?;
        Ok(ok(json!({
            "BackupVaultName": v.name,
            "BackupVaultArn": v.arn,
            "SNSTopicArn": n.get("SNSTopicArn"),
            "BackupVaultEvents": n.get("BackupVaultEvents"),
        })))
    }

    fn delete_vault_notifications(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        v.notifications = None;
        Ok(empty(200))
    }

    fn associate_mpa(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(name))
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        Ok(empty(204))
    }

    fn disassociate_mpa(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(name))
            .ok_or_else(|| not_found(format!("Backup vault not found: {name}")))?;
        Ok(empty(204))
    }

    // ===================== Recovery points =====================

    fn describe_recovery_point(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let point = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .and_then(|v| v.recovery_points.get(rp))
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        Ok(ok(point.clone()))
    }

    fn delete_recovery_point(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        v.recovery_points
            .remove(rp)
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        for arns in st.resource_recovery_points.values_mut() {
            arns.retain(|a| a != rp);
        }
        Ok(empty(200))
    }

    fn update_recovery_point_lifecycle(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let lifecycle = body.get("Lifecycle").cloned().unwrap_or(Value::Null);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let point = v
            .recovery_points
            .get_mut(rp)
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        if let Value::Object(obj) = point {
            obj.insert("Lifecycle".to_string(), lifecycle.clone());
        }
        let arn = v.arn.clone();
        Ok(ok(json!({
            "BackupVaultArn": arn,
            "RecoveryPointArn": rp,
            "Lifecycle": lifecycle,
            "CalculatedLifecycle": { "MoveToColdStorageAt": ts(Utc::now()), "DeleteAt": ts(Utc::now()) },
        })))
    }

    fn get_rp_index_details(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        if !v.recovery_points.contains_key(rp) {
            return Err(not_found(format!("Recovery point not found: {rp}")));
        }
        Ok(ok(json!({
            "RecoveryPointArn": rp,
            "BackupVaultArn": v.arn,
            "IndexStatus": "ACTIVE",
            "TotalItemsIndexed": 0,
        })))
    }

    fn update_rp_index_settings(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let index = req_str(&body, "Index")?;
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        if !v.recovery_points.contains_key(rp) {
            return Err(not_found(format!("Recovery point not found: {rp}")));
        }
        Ok(ok(json!({
            "BackupVaultName": vault,
            "RecoveryPointArn": rp,
            "IndexStatus": if index == "ENABLED" { "ACTIVE" } else { "DELETING" },
            "Index": index,
        })))
    }

    fn get_rp_restore_metadata(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let point = v
            .recovery_points
            .get(rp)
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        Ok(ok(json!({
            "BackupVaultArn": v.arn,
            "RecoveryPointArn": rp,
            "RestoreMetadata": {},
            "ResourceType": point.get("ResourceType"),
        })))
    }

    fn disassociate_recovery_point(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        v.recovery_points
            .remove(rp)
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        Ok(empty(200))
    }

    fn disassociate_rp_parent(
        &self,
        req: &AwsRequest,
        vault: &str,
        rp: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        if !v.recovery_points.contains_key(rp) {
            return Err(not_found(format!("Recovery point not found: {rp}")));
        }
        Ok(empty(204))
    }

    fn list_rp_by_vault(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let by_resource_type = req.query_params.get("resourceType").cloned();
        let by_resource_arn = req.query_params.get("resourceArn").cloned();
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let items: Vec<Value> = v
            .recovery_points
            .values()
            .filter(|p| {
                by_resource_type
                    .as_deref()
                    .is_none_or(|t| p.get("ResourceType").and_then(|x| x.as_str()) == Some(t))
                    && by_resource_arn
                        .as_deref()
                        .is_none_or(|a| p.get("ResourceArn").and_then(|x| x.as_str()) == Some(a))
            })
            .cloned()
            .collect();
        page_response(items, "RecoveryPoints", req, &[])
    }

    fn list_rp_by_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_label(resource_arn, "ResourceArn")?;
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let arns = st
            .and_then(|s| s.resource_recovery_points.get(resource_arn))
            .cloned()
            .unwrap_or_default();
        let items: Vec<Value> = arns
            .iter()
            .filter_map(|rp| st.and_then(|s| find_recovery_point(s, rp)))
            .map(|p| {
                json!({
                    "RecoveryPointArn": p.get("RecoveryPointArn"),
                    "CreationDate": p.get("CreationDate"),
                    "Status": p.get("Status"),
                    "BackupSizeBytes": p.get("BackupSizeInBytes"),
                    "BackupVaultName": p.get("BackupVaultName"),
                    "ResourceType": p.get("ResourceType"),
                    "EncryptionKeyArn": p.get("EncryptionKeyArn"),
                    "IsParent": p.get("IsParent"),
                })
            })
            .collect();
        page_response(items, "RecoveryPoints", req, &[])
    }

    fn list_rp_by_legal_hold(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_label(id, "LegalHoldId")?;
        // ListRecoveryPointsByLegalHold does not declare
        // ResourceNotFoundException, so a known-but-empty hold yields a page.
        page_response(Vec::new(), "RecoveryPoints", req, &[])
    }

    fn list_indexed_recovery_points(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        page_response(Vec::new(), "IndexedRecoveryPoints", req, &[])
    }

    // ===================== Protected resources =====================

    fn list_protected_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.resource_recovery_points
                    .iter()
                    .filter_map(|(arn, rps)| {
                        rps.last()
                            .and_then(|rp| find_recovery_point(s, rp))
                            .map(|p| protected_resource_json(arn, &p))
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "Results", req, &[])
    }

    fn list_protected_by_vault(
        &self,
        req: &AwsRequest,
        vault: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|s| s.vaults.get(vault))
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let mut seen = std::collections::BTreeSet::new();
        let items: Vec<Value> = v
            .recovery_points
            .values()
            .filter_map(|p| {
                let arn = p.get("ResourceArn").and_then(|x| x.as_str())?.to_string();
                if !seen.insert(arn.clone()) {
                    return None;
                }
                Some(protected_resource_json(&arn, p))
            })
            .collect();
        page_response(items, "Results", req, &[])
    }

    fn describe_protected_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let rps = st
            .and_then(|s| s.resource_recovery_points.get(resource_arn))
            .filter(|v| !v.is_empty())
            .ok_or_else(|| not_found(format!("Protected resource not found: {resource_arn}")))?;
        let last = rps
            .last()
            .and_then(|rp| st.and_then(|s| find_recovery_point(s, rp)))
            .ok_or_else(|| not_found(format!("Protected resource not found: {resource_arn}")))?;
        Ok(ok(json!({
            "ResourceArn": resource_arn,
            "ResourceType": last.get("ResourceType"),
            "LastBackupTime": last.get("CreationDate"),
            "LastBackupVaultArn": last.get("BackupVaultArn"),
            "LastRecoveryPointArn": last.get("RecoveryPointArn"),
            "ResourceName": last.get("ResourceName"),
        })))
    }

    // ===================== Backup / copy / restore / scan jobs =====================

    fn start_backup_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let vault = req_str(&body, "BackupVaultName")?;
        let resource_arn = req_str(&body, "ResourceArn")?;
        let iam_role = req_str(&body, "IamRoleArn")?;
        let now = Utc::now();
        let job_id = gen_id();
        let rp_id = gen_id();
        let rp_arn = recovery_point_arn(&req.region, &req.account_id, &rp_id);
        let resource_type = infer_resource_type(&resource_arn);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get_mut(&vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let vault_arn_s = v.arn.clone();
        let rp = json!({
            "RecoveryPointArn": rp_arn,
            "BackupVaultName": vault,
            "BackupVaultArn": vault_arn_s,
            "ResourceArn": resource_arn,
            "ResourceType": resource_type,
            "IamRoleArn": iam_role,
            "Status": "COMPLETED",
            "CreationDate": ts(now),
            "CompletionDate": ts(now),
            "BackupSizeInBytes": 0,
            "IsEncrypted": false,
            "IsParent": false,
            "EncryptionKeyArn": v.encryption_key_arn,
            "CreatedBy": { "BackupPlanId": "", "BackupPlanArn": "", "BackupPlanVersion": "", "BackupRuleId": "" },
        });
        v.recovery_points.insert(rp_arn.clone(), rp);
        st.resource_recovery_points
            .entry(resource_arn.clone())
            .or_default()
            .push(rp_arn.clone());
        let job = json!({
            "AccountId": req.account_id,
            "BackupJobId": job_id,
            "BackupVaultName": vault,
            "BackupVaultArn": vault_arn_s,
            "RecoveryPointArn": rp_arn,
            "ResourceArn": resource_arn,
            "ResourceType": resource_type,
            "CreationDate": ts(now),
            "State": "RUNNING",
            "PercentDone": "0.0",
            "BackupSizeInBytes": 0,
            "IamRoleArn": iam_role,
            "IsParent": false,
        });
        st.backup_jobs.insert(job_id.clone(), job);
        Ok(ok(json!({
            "BackupJobId": job_id,
            "RecoveryPointArn": rp_arn,
            "CreationDate": ts(now),
            "IsParent": false,
        })))
    }

    fn describe_backup_job(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .backup_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Backup job not found: {id}")))?;
        settle_job(job, "State");
        Ok(ok(job.clone()))
    }

    fn stop_backup_job(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .backup_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Backup job not found: {id}")))?;
        if let Value::Object(obj) = job {
            obj.insert("State".to_string(), json!("ABORTED"));
            obj.insert("StatusMessage".to_string(), json!("Stopped by user"));
        }
        Ok(empty(200))
    }

    fn list_backup_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let by_state = req.query_params.get("state").cloned();
        let by_vault = req.query_params.get("backupVaultName").cloned();
        let by_type = req.query_params.get("resourceType").cloned();
        let by_arn = req.query_params.get("resourceArn").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for job in st.backup_jobs.values_mut() {
            settle_job(job, "State");
        }
        let items: Vec<Value> = st
            .backup_jobs
            .values()
            .filter(|j| {
                filter_eq(j, "State", by_state.as_deref())
                    && filter_eq(j, "BackupVaultName", by_vault.as_deref())
                    && filter_eq(j, "ResourceType", by_type.as_deref())
                    && filter_eq(j, "ResourceArn", by_arn.as_deref())
            })
            .cloned()
            .collect();
        page_response(items, "BackupJobs", req, &[])
    }

    fn start_copy_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let rp_arn = req_str(&body, "RecoveryPointArn")?;
        let source_vault = req_str(&body, "SourceBackupVaultName")?;
        let dest_arn = req_str(&body, "DestinationBackupVaultArn")?;
        let iam_role = req_str(&body, "IamRoleArn")?;
        let now = Utc::now();
        let job_id = gen_id();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.vaults
            .get(&source_vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {source_vault}")))?;
        let job = json!({
            "AccountId": req.account_id,
            "CopyJobId": job_id,
            "SourceRecoveryPointArn": rp_arn,
            "SourceBackupVaultArn": vault_arn(&req.region, &req.account_id, &source_vault),
            "DestinationBackupVaultArn": dest_arn,
            "State": "RUNNING",
            "CreationDate": ts(now),
            "IamRoleArn": iam_role,
            "IsParent": false,
            "BackupSizeInBytes": 0,
        });
        st.copy_jobs.insert(job_id.clone(), job);
        Ok(ok(json!({
            "CopyJobId": job_id,
            "CreationDate": ts(now),
            "IsParent": false,
        })))
    }

    fn describe_copy_job(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .copy_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Copy job not found: {id}")))?;
        settle_job(job, "State");
        Ok(ok(json!({ "CopyJob": job })))
    }

    fn list_copy_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let by_state = req.query_params.get("state").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for job in st.copy_jobs.values_mut() {
            settle_job(job, "State");
        }
        let items: Vec<Value> = st
            .copy_jobs
            .values()
            .filter(|j| filter_eq(j, "State", by_state.as_deref()))
            .cloned()
            .collect();
        page_response(items, "CopyJobs", req, &[])
    }

    fn start_restore_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let rp_arn = req_str(&body, "RecoveryPointArn")?;
        if body.get("Metadata").filter(|v| v.is_object()).is_none() {
            return Err(missing_param("Metadata is required"));
        }
        let iam_role = str_field(&body, "IamRoleArn");
        let now = Utc::now();
        let job_id = gen_id();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let point = find_recovery_point(st, &rp_arn)
            .ok_or_else(|| not_found(format!("Recovery point not found: {rp_arn}")))?;
        let resource_type = point
            .get("ResourceType")
            .and_then(|v| v.as_str())
            .unwrap_or("EBS")
            .to_string();
        let source_resource_arn = point
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let job = json!({
            "AccountId": req.account_id,
            "RestoreJobId": job_id,
            "RecoveryPointArn": rp_arn,
            "SourceResourceArn": source_resource_arn,
            "CreationDate": ts(now),
            "Status": "RUNNING",
            "PercentDone": "0.0",
            "IamRoleArn": iam_role,
            "ResourceType": resource_type,
            "IsParent": false,
        });
        st.restore_jobs.insert(job_id.clone(), job);
        Ok(ok(json!({ "RestoreJobId": job_id })))
    }

    fn describe_restore_job(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .restore_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Restore job not found: {id}")))?;
        settle_job(job, "Status");
        if let Value::Object(obj) = job {
            if obj.get("Status").and_then(|v| v.as_str()) == Some("COMPLETED")
                && !obj.contains_key("CreatedResourceArn")
            {
                obj.insert(
                    "CreatedResourceArn".to_string(),
                    json!(format!(
                        "arn:aws:ec2:{}:{}:volume/vol-{}",
                        req.region,
                        req.account_id,
                        gen_id().replace('-', "").get(0..17).unwrap_or("0")
                    )),
                );
            }
        }
        Ok(ok(job.clone()))
    }

    fn list_restore_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let by_status = req.query_params.get("status").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for job in st.restore_jobs.values_mut() {
            settle_job(job, "Status");
        }
        let items: Vec<Value> = st
            .restore_jobs
            .values()
            .filter(|j| filter_eq(j, "Status", by_status.as_deref()))
            .cloned()
            .collect();
        page_response(items, "RestoreJobs", req, &[])
    }

    fn list_restore_jobs_by_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_label(resource_arn, "ResourceArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for job in st.restore_jobs.values_mut() {
            settle_job(job, "Status");
        }
        let items: Vec<Value> = st
            .restore_jobs
            .values()
            .filter(|j| j.get("SourceResourceArn").and_then(|v| v.as_str()) == Some(resource_arn))
            .cloned()
            .collect();
        page_response(items, "RestoreJobs", req, &[])
    }

    fn get_restore_job_metadata(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.restore_jobs.get(id))
            .ok_or_else(|| not_found(format!("Restore job not found: {id}")))?;
        Ok(ok(json!({ "RestoreJobId": id, "Metadata": {} })))
    }

    fn put_restore_validation(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let status = req_str(&body, "ValidationStatus")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .restore_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Restore job not found: {id}")))?;
        if let Value::Object(obj) = job {
            obj.insert("ValidationStatus".to_string(), json!(status));
            if let Some(m) = str_field(&body, "ValidationStatusMessage") {
                obj.insert("ValidationStatusMessage".to_string(), json!(m));
            }
        }
        Ok(empty(204))
    }

    fn start_scan_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let vault = req_str(&body, "BackupVaultName")?;
        let iam_role = req_str(&body, "IamRoleArn")?;
        let scanner = req_str(&body, "MalwareScanner")?;
        let rp_arn = req_str(&body, "RecoveryPointArn")?;
        let scan_mode = req_str(&body, "ScanMode")?;
        let scanner_role = req_str(&body, "ScannerRoleArn")?;
        let now = Utc::now();
        let job_id = gen_id();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let v = st
            .vaults
            .get(&vault)
            .ok_or_else(|| not_found(format!("Backup vault not found: {vault}")))?;
        let point = v.recovery_points.get(&rp_arn);
        let resource_arn = point
            .and_then(|p| p.get("ResourceArn").and_then(|x| x.as_str()))
            .unwrap_or("")
            .to_string();
        let resource_type = point
            .and_then(|p| p.get("ResourceType").and_then(|x| x.as_str()))
            .unwrap_or("EBS")
            .to_string();
        let job = json!({
            "AccountId": req.account_id,
            "BackupVaultArn": v.arn,
            "BackupVaultName": vault,
            "CreatedBy": { "BackupPlanId": "", "BackupPlanArn": "", "BackupPlanVersion": "", "BackupRuleId": "" },
            "CreationDate": ts(now),
            "IamRoleArn": iam_role,
            "MalwareScanner": scanner,
            "RecoveryPointArn": rp_arn,
            "ResourceArn": resource_arn,
            "ResourceName": "",
            "ResourceType": resource_type,
            "ScanJobId": job_id,
            "ScanMode": scan_mode,
            "ScannerRoleArn": scanner_role,
            "State": "RUNNING",
            "ScanResult": { "ScanResultStatus": "IN_PROGRESS" },
        });
        st.scan_jobs.insert(job_id.clone(), job);
        Ok(resp(
            201,
            json!({ "CreationDate": ts(now), "ScanJobId": job_id }),
        ))
    }

    fn describe_scan_job(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let job = st
            .scan_jobs
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Scan job not found: {id}")))?;
        if let Value::Object(obj) = job {
            obj.insert("State".to_string(), json!("COMPLETED"));
            obj.insert("CompletionDate".to_string(), ts(Utc::now()));
            obj.insert(
                "ScanResult".to_string(),
                json!({ "ScanResultStatus": "NO_THREATS_FOUND" }),
            );
        }
        Ok(ok(job.clone()))
    }

    fn list_scan_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.scan_jobs.values().cloned().collect())
            .unwrap_or_default();
        page_response(items, "ScanJobs", req, &[])
    }

    fn get_pitr_malware_scan_results(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let rp = req
            .query_params
            .get("RecoveryPointArn")
            .ok_or_else(|| missing_param("RecoveryPointArn is required"))?;
        let accounts = self.state.read();
        find_recovery_point(
            accounts
                .get(&req.account_id)
                .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?,
            rp,
        )
        .ok_or_else(|| not_found(format!("Recovery point not found: {rp}")))?;
        Ok(ok(json!({
            "ScanEndTime": ts(Utc::now()),
            "ScanResult": { "ScanResultStatus": "NO_THREATS_FOUND" },
        })))
    }

    fn list_job_summaries(
        &self,
        req: &AwsRequest,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let period = req
            .query_params
            .get("AggregationPeriod")
            .cloned()
            .unwrap_or_else(|| "ONE_DAY".to_string());
        page_response(
            Vec::new(),
            key,
            req,
            &[("AggregationPeriod", json!(period))],
        )
    }

    // ===================== Frameworks =====================

    fn create_framework(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "FrameworkName")?;
        check_ident_name(&name, "FrameworkName")?;
        let controls = body
            .get("FrameworkControls")
            .cloned()
            .ok_or_else(|| missing_param("FrameworkControls is required"))?;
        let now = Utc::now();
        let arn = framework_arn(&req.region, &req.account_id, &name, &gen_id());
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.frameworks.contains_key(&name) {
            return Err(already_exists(format!("Framework already exists: {name}")));
        }
        let fw = json!({
            "FrameworkName": name,
            "FrameworkArn": arn,
            "FrameworkDescription": body.get("FrameworkDescription"),
            "FrameworkControls": controls,
            "CreationTime": ts(now),
            "DeploymentStatus": "COMPLETED",
            "FrameworkStatus": "ACTIVE",
            "IdempotencyToken": body.get("IdempotencyToken"),
        });
        st.frameworks.insert(name.clone(), fw);
        if let Some(tags) = body.get("FrameworkTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({ "FrameworkName": name, "FrameworkArn": arn })))
    }

    fn describe_framework(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "FrameworkName")?;
        let accounts = self.state.read();
        let fw = accounts
            .get(&req.account_id)
            .and_then(|s| s.frameworks.get(name))
            .ok_or_else(|| not_found(format!("Framework not found: {name}")))?;
        Ok(ok(fw.clone()))
    }

    fn update_framework(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "FrameworkName")?;
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let fw = st
            .frameworks
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Framework not found: {name}")))?;
        if let Value::Object(obj) = fw {
            if let Some(d) = body.get("FrameworkDescription") {
                obj.insert("FrameworkDescription".to_string(), d.clone());
            }
            if let Some(c) = body.get("FrameworkControls") {
                obj.insert("FrameworkControls".to_string(), c.clone());
            }
        }
        let arn = fw.get("FrameworkArn").cloned();
        let created = fw.get("CreationTime").cloned();
        Ok(ok(json!({
            "FrameworkName": name,
            "FrameworkArn": arn,
            "CreationTime": created,
        })))
    }

    fn delete_framework(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "FrameworkName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.frameworks
            .remove(name)
            .ok_or_else(|| not_found(format!("Framework not found: {name}")))?;
        Ok(empty(200))
    }

    fn list_frameworks(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.frameworks
                    .values()
                    .map(|f| {
                        json!({
                            "FrameworkName": f.get("FrameworkName"),
                            "FrameworkArn": f.get("FrameworkArn"),
                            "FrameworkDescription": f.get("FrameworkDescription"),
                            "NumberOfControls": f.get("FrameworkControls").and_then(|c| c.as_array()).map_or(0, Vec::len),
                            "CreationTime": f.get("CreationTime"),
                            "DeploymentStatus": f.get("DeploymentStatus"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "Frameworks", req, &[])
    }

    // ===================== Report plans / jobs =====================

    fn create_report_plan(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "ReportPlanName")?;
        check_ident_name(&name, "ReportPlanName")?;
        let delivery = body
            .get("ReportDeliveryChannel")
            .cloned()
            .ok_or_else(|| missing_param("ReportDeliveryChannel is required"))?;
        let setting = body
            .get("ReportSetting")
            .cloned()
            .ok_or_else(|| missing_param("ReportSetting is required"))?;
        let now = Utc::now();
        let arn = report_plan_arn(&req.region, &req.account_id, &name, &gen_id());
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.report_plans.contains_key(&name) {
            return Err(already_exists(format!(
                "Report plan already exists: {name}"
            )));
        }
        let rp = json!({
            "ReportPlanName": name,
            "ReportPlanArn": arn,
            "ReportPlanDescription": body.get("ReportPlanDescription"),
            "ReportDeliveryChannel": delivery,
            "ReportSetting": setting,
            "DeploymentStatus": "COMPLETED",
            "CreationTime": ts(now),
        });
        st.report_plans.insert(name.clone(), rp);
        if let Some(tags) = body.get("ReportPlanTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({
            "ReportPlanName": name,
            "ReportPlanArn": arn,
            "CreationTime": ts(now),
        })))
    }

    fn describe_report_plan(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "ReportPlanName")?;
        let accounts = self.state.read();
        let rp = accounts
            .get(&req.account_id)
            .and_then(|s| s.report_plans.get(name))
            .ok_or_else(|| not_found(format!("Report plan not found: {name}")))?;
        Ok(ok(json!({ "ReportPlan": rp })))
    }

    fn update_report_plan(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "ReportPlanName")?;
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let rp = st
            .report_plans
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Report plan not found: {name}")))?;
        if let Value::Object(obj) = rp {
            for key in [
                "ReportPlanDescription",
                "ReportDeliveryChannel",
                "ReportSetting",
            ] {
                if let Some(val) = body.get(key) {
                    obj.insert(key.to_string(), val.clone());
                }
            }
        }
        let arn = rp.get("ReportPlanArn").cloned();
        let created = rp.get("CreationTime").cloned();
        Ok(ok(json!({
            "ReportPlanName": name,
            "ReportPlanArn": arn,
            "CreationTime": created,
        })))
    }

    fn delete_report_plan(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "ReportPlanName")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.report_plans
            .remove(name)
            .ok_or_else(|| not_found(format!("Report plan not found: {name}")))?;
        Ok(empty(200))
    }

    fn list_report_plans(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.report_plans.values().cloned().collect())
            .unwrap_or_default();
        page_response(items, "ReportPlans", req, &[])
    }

    fn start_report_job(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_ident_name(name, "ReportPlanName")?;
        let job_id = gen_id();
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let plan = st
            .report_plans
            .get(name)
            .ok_or_else(|| not_found(format!("Report plan not found: {name}")))?;
        let arn = plan.get("ReportPlanArn").cloned();
        let job = json!({
            "ReportJobId": job_id,
            "ReportPlanArn": arn,
            "Status": "COMPLETED",
            "CreationTime": ts(now),
            "CompletionTime": ts(now),
        });
        st.report_jobs.insert(job_id.clone(), job);
        Ok(ok(json!({ "ReportJobId": job_id })))
    }

    fn describe_report_job(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let job = accounts
            .get(&req.account_id)
            .and_then(|s| s.report_jobs.get(id))
            .ok_or_else(|| not_found(format!("Report job not found: {id}")))?;
        Ok(ok(json!({ "ReportJob": job })))
    }

    fn list_report_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        if let Some(n) = req.query_params.get("ReportPlanName") {
            check_ident_name(n, "ByReportPlanName")?;
        }
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.report_jobs.values().cloned().collect())
            .unwrap_or_default();
        page_response(items, "ReportJobs", req, &[])
    }

    // ===================== Legal holds =====================

    fn create_legal_hold(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let title = req_str(&body, "Title")?;
        let description = req_str(&body, "Description")?;
        let now = Utc::now();
        let id = gen_id();
        let arn = legal_hold_arn(&req.region, &req.account_id, &id);
        let selection = body
            .get("RecoveryPointSelection")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let hold = json!({
            "Title": title,
            "Status": "ACTIVE",
            "Description": description,
            "LegalHoldId": id,
            "LegalHoldArn": arn,
            "CreationDate": ts(now),
            "RecoveryPointSelection": selection,
        });
        st.legal_holds.insert(id.clone(), hold.clone());
        if let Some(tags) = body.get("Tags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(hold))
    }

    fn get_legal_hold(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let hold = accounts
            .get(&req.account_id)
            .and_then(|s| s.legal_holds.get(id))
            .ok_or_else(|| not_found(format!("Legal hold not found: {id}")))?;
        Ok(ok(hold.clone()))
    }

    fn cancel_legal_hold(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let cancel_desc = str_field(&body, "CancelDescription")
            .or_else(|| req.query_params.get("cancelDescription").cloned());
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let hold = st
            .legal_holds
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Legal hold not found: {id}")))?;
        if let Value::Object(obj) = hold {
            obj.insert("Status".to_string(), json!("CANCELED"));
            obj.insert("CancellationDate".to_string(), ts(Utc::now()));
            if let Some(cd) = cancel_desc {
                obj.insert("CancelDescription".to_string(), json!(cd));
            }
        }
        Ok(resp(201, json!({})))
    }

    fn list_legal_holds(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.legal_holds
                    .values()
                    .map(|h| {
                        json!({
                            "Title": h.get("Title"),
                            "Status": h.get("Status"),
                            "Description": h.get("Description"),
                            "LegalHoldId": h.get("LegalHoldId"),
                            "LegalHoldArn": h.get("LegalHoldArn"),
                            "CreationDate": h.get("CreationDate"),
                            "CancellationDate": h.get("CancellationDate"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "LegalHolds", req, &[])
    }

    // ===================== Restore testing =====================

    fn create_rtp(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let plan = body
            .get("RestoreTestingPlan")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("RestoreTestingPlan is required"))?;
        let name = plan
            .get("RestoreTestingPlanName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| missing_param("RestoreTestingPlanName is required"))?
            .to_string();
        let now = Utc::now();
        let arn = restore_testing_plan_arn(&req.region, &req.account_id, &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.restore_testing_plans.contains_key(&name) {
            return Err(already_exists(format!(
                "Restore testing plan exists: {name}"
            )));
        }
        st.restore_testing_plans.insert(
            name.clone(),
            RestoreTestingPlanRecord {
                name: name.clone(),
                arn: arn.clone(),
                creation_time: now,
                update_time: None,
                last_execution_time: None,
                creator_request_id: str_field(&body, "CreatorRequestId"),
                plan,
                selections: Default::default(),
            },
        );
        if let Some(tags) = body.get("Tags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(resp(
            201,
            json!({
                "CreationTime": ts(now),
                "RestoreTestingPlanArn": arn,
                "RestoreTestingPlanName": name,
            }),
        ))
    }

    fn get_rtp(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|s| s.restore_testing_plans.get(name))
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {name}")))?;
        Ok(ok(json!({ "RestoreTestingPlan": rtp_for_get(p) })))
    }

    fn update_rtp(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let patch = body
            .get("RestoreTestingPlan")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("RestoreTestingPlan is required"))?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .restore_testing_plans
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {name}")))?;
        if let (Value::Object(existing), Value::Object(upd)) = (&mut p.plan, &patch) {
            for (k, v) in upd {
                existing.insert(k.clone(), v.clone());
            }
        }
        p.update_time = Some(now);
        let arn = p.arn.clone();
        Ok(ok(json!({
            "CreationTime": ts(p.creation_time),
            "RestoreTestingPlanArn": arn,
            "RestoreTestingPlanName": name,
            "UpdateTime": ts(now),
        })))
    }

    fn delete_rtp(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        // DeleteRestoreTestingPlan declares only InvalidRequestException as a
        // client error, so reject an empty/malformed name with that code.
        if name.is_empty() || name.contains(['{', '}']) {
            return Err(invalid_request(format!(
                "Invalid RestoreTestingPlanName: {name}"
            )));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.restore_testing_plans.remove(name);
        Ok(empty(204))
    }

    fn list_rtp(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.restore_testing_plans.values().map(rtp_for_list).collect())
            .unwrap_or_default();
        page_response(items, "RestoreTestingPlans", req, &[])
    }

    fn create_rts(
        &self,
        req: &AwsRequest,
        plan_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let selection = body
            .get("RestoreTestingSelection")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("RestoreTestingSelection is required"))?;
        let sel_name = selection
            .get("RestoreTestingSelectionName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| missing_param("RestoreTestingSelectionName is required"))?
            .to_string();
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .restore_testing_plans
            .get_mut(plan_name)
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {plan_name}")))?;
        if p.selections.contains_key(&sel_name) {
            return Err(already_exists(format!("Selection exists: {sel_name}")));
        }
        let arn = p.arn.clone();
        p.selections.insert(
            sel_name.clone(),
            RestoreTestingSelectionRecord {
                name: sel_name.clone(),
                creation_time: now,
                update_time: None,
                creator_request_id: str_field(&body, "CreatorRequestId"),
                selection,
            },
        );
        Ok(resp(
            201,
            json!({
                "CreationTime": ts(now),
                "RestoreTestingPlanArn": arn,
                "RestoreTestingPlanName": plan_name,
                "RestoreTestingSelectionName": sel_name,
            }),
        ))
    }

    fn get_rts(
        &self,
        req: &AwsRequest,
        plan_name: &str,
        sel_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let sel = accounts
            .get(&req.account_id)
            .and_then(|s| s.restore_testing_plans.get(plan_name))
            .and_then(|p| p.selections.get(sel_name))
            .ok_or_else(|| not_found(format!("Restore testing selection not found: {sel_name}")))?;
        Ok(ok(json!({
            "RestoreTestingSelection": rts_for_get(sel, plan_name),
        })))
    }

    fn update_rts(
        &self,
        req: &AwsRequest,
        plan_name: &str,
        sel_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let patch = body
            .get("RestoreTestingSelection")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("RestoreTestingSelection is required"))?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .restore_testing_plans
            .get_mut(plan_name)
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {plan_name}")))?;
        let arn = p.arn.clone();
        let sel = p
            .selections
            .get_mut(sel_name)
            .ok_or_else(|| not_found(format!("Restore testing selection not found: {sel_name}")))?;
        if let (Value::Object(existing), Value::Object(upd)) = (&mut sel.selection, &patch) {
            for (k, v) in upd {
                existing.insert(k.clone(), v.clone());
            }
        }
        sel.update_time = Some(now);
        Ok(ok(json!({
            "CreationTime": ts(sel.creation_time),
            "RestoreTestingPlanArn": arn,
            "RestoreTestingPlanName": plan_name,
            "RestoreTestingSelectionName": sel_name,
            "UpdateTime": ts(now),
        })))
    }

    fn delete_rts(
        &self,
        req: &AwsRequest,
        plan_name: &str,
        sel_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .restore_testing_plans
            .get_mut(plan_name)
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {plan_name}")))?;
        p.selections.remove(sel_name);
        Ok(empty(204))
    }

    fn list_rts(&self, req: &AwsRequest, plan_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|s| s.restore_testing_plans.get(plan_name))
            .ok_or_else(|| not_found(format!("Restore testing plan not found: {plan_name}")))?;
        let items: Vec<Value> = p
            .selections
            .values()
            .map(|s| rts_for_list(s, plan_name))
            .collect();
        page_response(items, "RestoreTestingSelections", req, &[])
    }

    fn get_rt_inferred_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        for p in ["BackupVaultName", "RecoveryPointArn"] {
            if !req.query_params.contains_key(p) {
                return Err(missing_param(format!("{p} is required")));
            }
        }
        Ok(ok(json!({ "InferredMetadata": {} })))
    }

    // ===================== Tiering configurations =====================

    fn create_tiering(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let config = body
            .get("TieringConfiguration")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("TieringConfiguration is required"))?;
        let name = config
            .get("TieringConfigurationName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| missing_param("TieringConfigurationName is required"))?
            .to_string();
        let now = Utc::now();
        let arn = tiering_configuration_arn(&req.region, &req.account_id, &name);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.tiering_configs.contains_key(&name) {
            return Err(already_exists(format!(
                "Tiering configuration exists: {name}"
            )));
        }
        let mut stored = config;
        if let Value::Object(obj) = &mut stored {
            obj.insert("TieringConfigurationArn".to_string(), json!(arn));
            obj.insert("CreationTime".to_string(), ts(now));
            obj.insert("LastUpdatedTime".to_string(), ts(now));
        }
        st.tiering_configs.insert(name.clone(), stored);
        if let Some(tags) = body.get("TieringConfigurationTags") {
            let t = parse_tags(Some(tags));
            if !t.is_empty() {
                st.tags.insert(arn.clone(), t);
            }
        }
        Ok(ok(json!({
            "TieringConfigurationArn": arn,
            "TieringConfigurationName": name,
            "CreationTime": ts(now),
        })))
    }

    fn get_tiering(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.tiering_configs.get(name))
            .ok_or_else(|| not_found(format!("Tiering configuration not found: {name}")))?;
        Ok(ok(json!({ "TieringConfiguration": c })))
    }

    fn update_tiering(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let patch = body
            .get("TieringConfiguration")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing_param("TieringConfiguration is required"))?;
        let now = Utc::now();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let c = st
            .tiering_configs
            .get_mut(name)
            .ok_or_else(|| not_found(format!("Tiering configuration not found: {name}")))?;
        let arn = c.get("TieringConfigurationArn").cloned();
        let created = c.get("CreationTime").cloned();
        if let (Value::Object(existing), Value::Object(upd)) = (&mut *c, &patch) {
            for (k, v) in upd {
                existing.insert(k.clone(), v.clone());
            }
            existing.insert("LastUpdatedTime".to_string(), ts(now));
        }
        Ok(ok(json!({
            "TieringConfigurationArn": arn,
            "TieringConfigurationName": name,
            "CreationTime": created,
            "LastUpdatedTime": ts(now),
        })))
    }

    fn delete_tiering(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.tiering_configs
            .remove(name)
            .ok_or_else(|| not_found(format!("Tiering configuration not found: {name}")))?;
        Ok(ok(json!({})))
    }

    fn list_tiering(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.tiering_configs
                    .values()
                    .map(|c| {
                        json!({
                            "TieringConfigurationArn": c.get("TieringConfigurationArn"),
                            "TieringConfigurationName": c.get("TieringConfigurationName"),
                            "CreationTime": c.get("CreationTime"),
                            "LastUpdatedTime": c.get("LastUpdatedTime"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "TieringConfigurations", req, &[])
    }

    // ===================== Settings + tags =====================

    fn describe_global_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let settings = accounts
            .get(&req.account_id)
            .map(|s| s.global_settings.clone())
            .unwrap_or_default();
        Ok(ok(json!({
            "GlobalSettings": settings,
            "LastUpdateTime": ts(Utc::now()),
        })))
    }

    fn update_global_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(Value::Object(gs)) = body.get("GlobalSettings") {
            for (k, v) in gs {
                if let Some(s) = v.as_str() {
                    st.global_settings.insert(k.clone(), s.to_string());
                }
            }
        }
        Ok(empty(200))
    }

    fn describe_region_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        let mut optin = Map::new();
        let mut mgmt = Map::new();
        for rt in SUPPORTED_RESOURCE_TYPES {
            let enabled = st
                .and_then(|s| s.region_optin.get(*rt))
                .copied()
                .unwrap_or(true);
            optin.insert((*rt).to_string(), json!(enabled));
        }
        if let Some(s) = st {
            for (k, v) in &s.region_optin {
                optin.insert(k.clone(), json!(*v));
            }
            for (k, v) in &s.region_mgmt {
                mgmt.insert(k.clone(), json!(*v));
            }
        }
        Ok(ok(json!({
            "ResourceTypeOptInPreference": optin,
            "ResourceTypeManagementPreference": mgmt,
        })))
    }

    fn update_region_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(Value::Object(m)) = body.get("ResourceTypeOptInPreference") {
            for (k, v) in m {
                if let Some(b) = v.as_bool() {
                    st.region_optin.insert(k.clone(), b);
                }
            }
        }
        if let Some(Value::Object(m)) = body.get("ResourceTypeManagementPreference") {
            for (k, v) in m {
                if let Some(b) = v.as_bool() {
                    st.region_mgmt.insert(k.clone(), b);
                }
            }
        }
        Ok(empty(200))
    }

    fn list_tags(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        check_label(arn, "ResourceArn")?;
        let accounts = self.state.read();
        let tags = accounts
            .get(&req.account_id)
            .and_then(|s| s.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        Ok(ok(json!({ "Tags": tag_map_to_value(&tags) })))
    }

    fn tag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        check_label(arn, "ResourceArn")?;
        let body = req_body(req);
        let tags = body
            .get("Tags")
            .ok_or_else(|| missing_param("Tags is required"))?;
        let parsed = parse_tags(Some(tags));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let entry = st.tags.entry(arn.to_string()).or_default();
        for (k, v) in parsed {
            entry.insert(k, v);
        }
        Ok(empty(200))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        check_label(arn, "ResourceArn")?;
        let body = req_body(req);
        let keys = body
            .get("TagKeyList")
            .and_then(|v| v.as_array())
            .ok_or_else(|| missing_param("TagKeyList is required"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(entry) = st.tags.get_mut(arn) {
            for k in keys {
                if let Some(s) = k.as_str() {
                    entry.remove(s);
                }
            }
        }
        Ok(empty(200))
    }
}

// ---------------------------------------------------------------------------
// Response-shaping helpers
// ---------------------------------------------------------------------------

fn vault_describe_json(v: &VaultRecord) -> Value {
    json!({
        "BackupVaultName": v.name,
        "BackupVaultArn": v.arn,
        "VaultType": v.vault_type,
        "VaultState": v.vault_state,
        "EncryptionKeyArn": v.encryption_key_arn,
        "CreationDate": ts(v.creation_date),
        "CreatorRequestId": v.creator_request_id,
        "NumberOfRecoveryPoints": v.recovery_points.len() as i64,
        "Locked": v.locked,
        "MinRetentionDays": v.min_retention_days,
        "MaxRetentionDays": v.max_retention_days,
        "LockDate": v.lock_date.map(ts),
        "SourceBackupVaultArn": v.source_backup_vault_arn,
    })
}

fn vault_list_json(v: &VaultRecord) -> Value {
    json!({
        "BackupVaultName": v.name,
        "BackupVaultArn": v.arn,
        "VaultType": v.vault_type,
        "VaultState": v.vault_state,
        "CreationDate": ts(v.creation_date),
        "EncryptionKeyArn": v.encryption_key_arn,
        "CreatorRequestId": v.creator_request_id,
        "NumberOfRecoveryPoints": v.recovery_points.len() as i64,
        "Locked": v.locked,
        "MinRetentionDays": v.min_retention_days,
        "MaxRetentionDays": v.max_retention_days,
        "LockDate": v.lock_date.map(ts),
    })
}

fn protected_resource_json(arn: &str, rp: &Value) -> Value {
    json!({
        "ResourceArn": arn,
        "ResourceType": rp.get("ResourceType"),
        "LastBackupTime": rp.get("CreationDate"),
        "LastBackupVaultArn": rp.get("BackupVaultArn"),
        "LastRecoveryPointArn": rp.get("RecoveryPointArn"),
        "ResourceName": rp.get("ResourceName"),
    })
}

/// Find a recovery point by ARN across every vault in the account.
fn find_recovery_point(st: &crate::state::BackupState, rp_arn: &str) -> Option<Value> {
    st.vaults
        .values()
        .find_map(|v| v.recovery_points.get(rp_arn).cloned())
}

/// Advance a synthetic job to its terminal state on read.
fn settle_job(job: &mut Value, status_key: &str) {
    if let Value::Object(obj) = job {
        let cur = obj.get(status_key).and_then(|v| v.as_str()).unwrap_or("");
        if cur == "RUNNING" || cur == "CREATED" || cur == "PENDING" {
            obj.insert(status_key.to_string(), json!("COMPLETED"));
            obj.insert("PercentDone".to_string(), json!("100.0"));
            obj.insert("CompletionDate".to_string(), ts(Utc::now()));
        }
    }
}

fn filter_eq(v: &Value, key: &str, want: Option<&str>) -> bool {
    match want {
        None => true,
        Some(w) => v.get(key).and_then(|x| x.as_str()) == Some(w),
    }
}

fn infer_resource_type(arn: &str) -> &'static str {
    if arn.contains(":ec2:") && arn.contains(":volume/") {
        "EBS"
    } else if arn.contains(":ec2:") {
        "EC2"
    } else if arn.contains(":rds:") {
        "RDS"
    } else if arn.contains(":dynamodb:") {
        "DynamoDB"
    } else if arn.contains(":elasticfilesystem:") {
        "EFS"
    } else if arn.contains(":s3:") {
        "S3"
    } else {
        "EBS"
    }
}

fn rtp_for_get(p: &RestoreTestingPlanRecord) -> Value {
    let mut out = merge_plan_defaults(&p.plan);
    if let Value::Object(obj) = &mut out {
        obj.insert("CreationTime".to_string(), ts(p.creation_time));
        obj.insert("RestoreTestingPlanArn".to_string(), json!(p.arn));
        obj.insert("RestoreTestingPlanName".to_string(), json!(p.name));
        obj.insert("CreatorRequestId".to_string(), json!(p.creator_request_id));
        if let Some(u) = p.update_time {
            obj.insert("LastUpdateTime".to_string(), ts(u));
        }
        if let Some(e) = p.last_execution_time {
            obj.insert("LastExecutionTime".to_string(), ts(e));
        }
    }
    out
}

fn rtp_for_list(p: &RestoreTestingPlanRecord) -> Value {
    json!({
        "CreationTime": ts(p.creation_time),
        "RestoreTestingPlanArn": p.arn,
        "RestoreTestingPlanName": p.name,
        "ScheduleExpression": p.plan.get("ScheduleExpression").cloned().unwrap_or_else(|| json!("cron(0 5 ? * * *)")),
        "ScheduleExpressionTimezone": p.plan.get("ScheduleExpressionTimezone"),
        "StartWindowHours": p.plan.get("StartWindowHours"),
        "LastUpdateTime": p.update_time.map(ts),
        "LastExecutionTime": p.last_execution_time.map(ts),
    })
}

/// Ensure the required `RestoreTestingPlanForGet` members exist even if the
/// caller's create body omitted the ones with server defaults.
fn merge_plan_defaults(plan: &Value) -> Value {
    let mut obj = plan.as_object().cloned().unwrap_or_default();
    obj.entry("ScheduleExpression".to_string())
        .or_insert_with(|| json!("cron(0 5 ? * * *)"));
    obj.entry("RecoveryPointSelection".to_string())
        .or_insert_with(
            || json!({ "Algorithm": "LATEST_WITHIN_WINDOW", "SelectionWindowDays": 30 }),
        );
    Value::Object(obj)
}

fn rts_for_get(s: &RestoreTestingSelectionRecord, plan_name: &str) -> Value {
    let mut obj = s.selection.as_object().cloned().unwrap_or_default();
    obj.insert("CreationTime".to_string(), ts(s.creation_time));
    obj.insert("RestoreTestingPlanName".to_string(), json!(plan_name));
    obj.insert("RestoreTestingSelectionName".to_string(), json!(s.name));
    obj.entry("IamRoleArn".to_string())
        .or_insert_with(|| json!(""));
    obj.entry("ProtectedResourceType".to_string())
        .or_insert_with(|| json!("EBS"));
    obj.insert("CreatorRequestId".to_string(), json!(s.creator_request_id));
    Value::Object(obj)
}

fn rts_for_list(s: &RestoreTestingSelectionRecord, plan_name: &str) -> Value {
    json!({
        "CreationTime": ts(s.creation_time),
        "IamRoleArn": s.selection.get("IamRoleArn").cloned().unwrap_or_else(|| json!("")),
        "ProtectedResourceType": s.selection.get("ProtectedResourceType").cloned().unwrap_or_else(|| json!("EBS")),
        "RestoreTestingPlanName": plan_name,
        "RestoreTestingSelectionName": s.name,
        "ValidationWindowHours": s.selection.get("ValidationWindowHours"),
    })
}
