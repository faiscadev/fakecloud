// Handler bodies for `AppConfigService`, `include!`d into `service.rs` so the
// free helpers below share its module scope. Split out only to keep the
// routing table and the per-operation logic in separate files.

use chrono::SecondsFormat;

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// A JSON response with an explicit success status.
fn resp(code: u16, v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::from_u16(code).unwrap_or(StatusCode::OK), v)
}

fn ok(v: Value) -> AwsResponse {
    resp(200, v)
}

/// An empty-body response with an explicit status (204 for delete-style ops).
fn empty(code: u16) -> AwsResponse {
    AwsResponse::json(
        StatusCode::from_u16(code).unwrap_or(StatusCode::OK),
        Vec::new(),
    )
}

fn set_header(resp: &mut AwsResponse, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        resp.headers.insert(n, v);
    }
}

fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn aws_err(code: &str, status: StatusCode, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, msg)
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    aws_err("ResourceNotFoundException", StatusCode::NOT_FOUND, msg)
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    aws_err("BadRequestException", StatusCode::BAD_REQUEST, msg)
}

fn req_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn req_str(body: &Value, key: &str) -> Result<String, AwsServiceError> {
    str_field(body, key).ok_or_else(|| bad_request(format!("{key} is required")))
}

fn header_str(req: &AwsRequest, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
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

/// Insert `key: value` into `map` only when `value` is `Some`.
fn insert_opt(map: &mut Map<String, Value>, key: &str, value: &Option<String>) {
    if let Some(v) = value {
        map.insert(key.to_string(), Value::String(v.clone()));
    }
}

fn max_results(req: &AwsRequest) -> usize {
    req.query_params
        .get("max_results")
        .or_else(|| req.query_params.get("MaxResults"))
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1000)
}

fn next_token(req: &AwsRequest) -> Option<String> {
    req.query_params
        .get("next_token")
        .or_else(|| req.query_params.get("NextToken"))
        .cloned()
}

/// All values for a repeated query key, parsed from the raw query string.
fn query_all(req: &AwsRequest, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for pair in req.raw_query.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        if k == key {
            let v = it.next().unwrap_or("");
            out.push(decode(v));
        }
    }
    out
}

/// Paginate a list of JSON items into an `{Items, NextToken}` response.
fn page_response(items: Vec<Value>, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    let (page, token) = paginate_checked(&items, next_token(req).as_deref(), max_results(req))
        .map_err(|_| bad_request("Invalid NextToken"))?;
    let mut out = Map::new();
    out.insert("Items".to_string(), Value::Array(page));
    if let Some(t) = token {
        out.insert("NextToken".to_string(), Value::String(t));
    }
    Ok(ok(Value::Object(out)))
}

/// A 7-character lowercase alphanumeric id, AppConfig's resource-id shape.
fn gen_id7() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..7].to_string()
}

fn gen_token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

// ---------------------------------------------------------------------------
// Record -> JSON
// ---------------------------------------------------------------------------

fn application_json(app: &ApplicationRecord) -> Value {
    let mut m = Map::new();
    m.insert("Id".to_string(), Value::String(app.id.clone()));
    m.insert("Name".to_string(), Value::String(app.name.clone()));
    insert_opt(&mut m, "Description", &app.description);
    Value::Object(m)
}

fn environment_json(env: &EnvironmentRecord) -> Value {
    let mut m = Map::new();
    m.insert(
        "ApplicationId".to_string(),
        Value::String(env.application_id.clone()),
    );
    m.insert("Id".to_string(), Value::String(env.id.clone()));
    m.insert("Name".to_string(), Value::String(env.name.clone()));
    insert_opt(&mut m, "Description", &env.description);
    m.insert("State".to_string(), Value::String(env.state.clone()));
    if !env.monitors.is_null() {
        m.insert("Monitors".to_string(), env.monitors.clone());
    }
    Value::Object(m)
}

fn profile_json(p: &ProfileRecord) -> Value {
    let mut m = Map::new();
    m.insert(
        "ApplicationId".to_string(),
        Value::String(p.application_id.clone()),
    );
    m.insert("Id".to_string(), Value::String(p.id.clone()));
    m.insert("Name".to_string(), Value::String(p.name.clone()));
    insert_opt(&mut m, "Description", &p.description);
    m.insert(
        "LocationUri".to_string(),
        Value::String(p.location_uri.clone()),
    );
    insert_opt(&mut m, "RetrievalRoleArn", &p.retrieval_role_arn);
    if !p.validators.is_null() {
        m.insert("Validators".to_string(), p.validators.clone());
    }
    m.insert("Type".to_string(), Value::String(p.profile_type.clone()));
    insert_opt(&mut m, "KmsKeyArn", &p.kms_key_arn);
    insert_opt(&mut m, "KmsKeyIdentifier", &p.kms_key_identifier);
    Value::Object(m)
}

fn profile_summary_json(p: &ProfileRecord) -> Value {
    json!({
        "ApplicationId": p.application_id,
        "Id": p.id,
        "Name": p.name,
        "LocationUri": p.location_uri,
        "Type": p.profile_type,
        "ValidatorTypes": p.validators.as_array().map(|a| {
            a.iter().filter_map(|v| v.get("Type").cloned()).collect::<Vec<_>>()
        }).unwrap_or_default(),
    })
}

/// Serve the hosted-version raw bytes plus AppConfig's metadata headers.
fn hosted_version_response(status: u16, v: &HostedVersionRecord) -> AwsResponse {
    let mut resp = AwsResponse::json(
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        v.content.clone(),
    );
    resp.content_type = v.content_type.clone();
    set_header(&mut resp, "Content-Type", &v.content_type);
    set_header(&mut resp, "Version-Number", &v.version_number.to_string());
    if let Some(d) = &v.description {
        set_header(&mut resp, "Description", d);
    }
    if let Some(l) = &v.version_label {
        set_header(&mut resp, "VersionLabel", l);
    }
    resp
}

// ---------------------------------------------------------------------------
// Predefined deployment strategies
// ---------------------------------------------------------------------------

/// AWS ships four predefined deployment strategies whose `Id == Name`.
fn predefined_strategy(id: &str) -> Option<Value> {
    let (name, dur, growth, gtype, bake, desc) = match id {
        "AppConfig.AllAtOnce" => (
            "AppConfig.AllAtOnce",
            0,
            100.0,
            "EXPONENTIAL",
            10,
            "Quick",
        ),
        "AppConfig.Linear50PercentEvery30Seconds" => (
            "AppConfig.Linear50PercentEvery30Seconds",
            1,
            50.0,
            "LINEAR",
            1,
            "AppConfig predefined deployment strategy: Test/Demo",
        ),
        "AppConfig.Canary10Percent20Minutes" => (
            "AppConfig.Canary10Percent20Minutes",
            20,
            10.0,
            "EXPONENTIAL",
            10,
            "AWS Recommended",
        ),
        "AppConfig.Linear20PercentEvery6Minutes" => (
            "AppConfig.Linear20PercentEvery6Minutes",
            30,
            20.0,
            "LINEAR",
            30,
            "AWS Recommended",
        ),
        _ => return None,
    };
    Some(json!({
        "Id": id,
        "Name": name,
        "Description": desc,
        "DeploymentDurationInMinutes": dur,
        "GrowthType": gtype,
        "GrowthFactor": growth,
        "FinalBakeTimeInMinutes": bake,
        "ReplicateTo": "NONE",
    }))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

impl AppConfigService {
    // ---- applications ----------------------------------------------------

    fn create_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let description = str_field(&body, "Description");
        let tags = parse_tags(body.get("Tags"));
        let id = gen_id7();
        let arn = application_arn(&req.region, &req.account_id, &id);
        let record = ApplicationRecord {
            id: id.clone(),
            name,
            description,
            environments: Default::default(),
            profiles: Default::default(),
            experiments: Default::default(),
        };
        let out = application_json(&record);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.applications.insert(id, record);
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_application(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(id))
            .ok_or_else(|| not_found(format!("Application not found: {id}")))?;
        Ok(ok(application_json(app)))
    }

    fn update_application(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Application not found: {id}")))?;
        if let Some(n) = str_field(&body, "Name") {
            app.name = n;
        }
        if body.get("Description").is_some() {
            app.description = str_field(&body, "Description");
        }
        Ok(ok(application_json(app)))
    }

    fn delete_application(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Cascade: removing the application drops its environments, profiles
        // (and their hosted versions) and experiments with it.
        if st.applications.remove(id).is_none() {
            return Err(not_found(format!("Application not found: {id}")));
        }
        let arn = application_arn(&req.region, &req.account_id, id);
        st.tags.remove(&arn);
        Ok(empty(204))
    }

    fn list_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.applications
                    .values()
                    .map(|app| {
                        // AppConfig's ListApplications item always carries a
                        // `Description` string (empty when unset), matching the
                        // wire shape AWS's own example documents.
                        json!({
                            "Id": app.id,
                            "Name": app.name,
                            "Description": app.description.clone().unwrap_or_default(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, req)
    }

    // ---- configuration profiles -----------------------------------------

    fn create_profile(
        &self,
        req: &AwsRequest,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let location_uri = req_str(&body, "LocationUri")?;
        let description = str_field(&body, "Description");
        let retrieval_role_arn = str_field(&body, "RetrievalRoleArn");
        let validators = body.get("Validators").cloned().unwrap_or(Value::Null);
        let profile_type =
            str_field(&body, "Type").unwrap_or_else(|| "AWS.Freeform".to_string());
        let kms_key_identifier = str_field(&body, "KmsKeyIdentifier");
        let tags = parse_tags(body.get("Tags"));
        let id = gen_id7();
        let arn = profile_arn(&req.region, &req.account_id, app_id, &id);
        let record = ProfileRecord {
            id: id.clone(),
            application_id: app_id.to_string(),
            name,
            description,
            location_uri,
            retrieval_role_arn,
            validators,
            profile_type,
            kms_key_arn: None,
            kms_key_identifier,
            hosted_versions: Default::default(),
            next_version_number: 1,
        };
        let out = profile_json(&record);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        app.profiles.insert(id, record);
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_profile(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.profiles.get(id))
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {id}")))?;
        Ok(ok(profile_json(p)))
    }

    fn update_profile(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        let p = app
            .profiles
            .get_mut(id)
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {id}")))?;
        if let Some(n) = str_field(&body, "Name") {
            p.name = n;
        }
        if body.get("Description").is_some() {
            p.description = str_field(&body, "Description");
        }
        if body.get("RetrievalRoleArn").is_some() {
            p.retrieval_role_arn = str_field(&body, "RetrievalRoleArn");
        }
        if let Some(v) = body.get("Validators") {
            p.validators = v.clone();
        }
        if body.get("KmsKeyIdentifier").is_some() {
            p.kms_key_identifier = str_field(&body, "KmsKeyIdentifier");
        }
        Ok(ok(profile_json(p)))
    }

    fn delete_profile(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        // Cascade: removing the profile drops its hosted configuration versions.
        if app.profiles.remove(id).is_none() {
            return Err(not_found(format!("ConfigurationProfile not found: {id}")));
        }
        let arn = profile_arn(&req.region, &req.account_id, app_id, id);
        st.tags.remove(&arn);
        Ok(empty(204))
    }

    fn list_profiles(
        &self,
        req: &AwsRequest,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        let filter = req.query_params.get("type").cloned();
        let items: Vec<Value> = app
            .profiles
            .values()
            .filter(|p| filter.as_ref().is_none_or(|t| &p.profile_type == t))
            .map(profile_summary_json)
            .collect();
        page_response(items, req)
    }

    fn validate_configuration(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        if !app.profiles.contains_key(id) {
            return Err(not_found(format!("ConfigurationProfile not found: {id}")));
        }
        Ok(empty(204))
    }

    // ---- hosted configuration versions ----------------------------------

    fn create_hosted_version(
        &self,
        req: &AwsRequest,
        app_id: &str,
        profile_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let content = req.body.to_vec();
        let content_type = header_str(req, "Content-Type")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let description = header_str(req, "Description");
        let version_label = header_str(req, "VersionLabel");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        let p = app
            .profiles
            .get_mut(profile_id)
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {profile_id}")))?;
        let version_number = p.next_version_number.max(1);
        p.next_version_number = version_number + 1;
        let record = HostedVersionRecord {
            version_number,
            description,
            content,
            content_type,
            version_label,
        };
        let out = hosted_version_response(201, &record);
        p.hosted_versions.insert(version_number, record);
        Ok(out)
    }

    fn get_hosted_version(
        &self,
        req: &AwsRequest,
        app_id: &str,
        profile_id: &str,
        version: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_number: i64 = version
            .parse()
            .map_err(|_| bad_request("VersionNumber must be an integer"))?;
        let accounts = self.state.read();
        let v = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.profiles.get(profile_id))
            .and_then(|p| p.hosted_versions.get(&version_number))
            .ok_or_else(|| not_found(format!("HostedConfigurationVersion not found: {version}")))?;
        Ok(hosted_version_response(200, v))
    }

    fn delete_hosted_version(
        &self,
        req: &AwsRequest,
        app_id: &str,
        profile_id: &str,
        version: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_number: i64 = version
            .parse()
            .map_err(|_| bad_request("VersionNumber must be an integer"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let p = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.profiles.get_mut(profile_id))
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {profile_id}")))?;
        if p.hosted_versions.remove(&version_number).is_none() {
            return Err(not_found(format!(
                "HostedConfigurationVersion not found: {version}"
            )));
        }
        Ok(empty(204))
    }

    fn list_hosted_versions(
        &self,
        req: &AwsRequest,
        app_id: &str,
        profile_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let p = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.profiles.get(profile_id))
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {profile_id}")))?;
        let items: Vec<Value> = p
            .hosted_versions
            .values()
            .rev()
            .map(|v| {
                let mut m = Map::new();
                m.insert(
                    "ApplicationId".to_string(),
                    Value::String(app_id.to_string()),
                );
                m.insert(
                    "ConfigurationProfileId".to_string(),
                    Value::String(profile_id.to_string()),
                );
                m.insert(
                    "VersionNumber".to_string(),
                    Value::Number(v.version_number.into()),
                );
                insert_opt(&mut m, "Description", &v.description);
                m.insert(
                    "ContentType".to_string(),
                    Value::String(v.content_type.clone()),
                );
                insert_opt(&mut m, "VersionLabel", &v.version_label);
                Value::Object(m)
            })
            .collect();
        page_response(items, req)
    }

    // ---- environments ----------------------------------------------------

    fn create_environment(
        &self,
        req: &AwsRequest,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let description = str_field(&body, "Description");
        let monitors = body.get("Monitors").cloned().unwrap_or(Value::Null);
        let tags = parse_tags(body.get("Tags"));
        let id = gen_id7();
        let arn = environment_arn(&req.region, &req.account_id, app_id, &id);
        let record = EnvironmentRecord {
            id: id.clone(),
            application_id: app_id.to_string(),
            name,
            description,
            state: "READY_FOR_DEPLOYMENT".to_string(),
            monitors,
            deployments: Default::default(),
            next_deployment_number: 1,
        };
        let out = environment_json(&record);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        app.environments.insert(id, record);
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_environment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let env = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.environments.get(id))
            .ok_or_else(|| not_found(format!("Environment not found: {id}")))?;
        Ok(ok(environment_json(env)))
    }

    fn update_environment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let env = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.environments.get_mut(id))
            .ok_or_else(|| not_found(format!("Environment not found: {id}")))?;
        if let Some(n) = str_field(&body, "Name") {
            env.name = n;
        }
        if body.get("Description").is_some() {
            env.description = str_field(&body, "Description");
        }
        if let Some(v) = body.get("Monitors") {
            env.monitors = v.clone();
        }
        Ok(ok(environment_json(env)))
    }

    fn delete_environment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        if app.environments.remove(id).is_none() {
            return Err(not_found(format!("Environment not found: {id}")));
        }
        let arn = environment_arn(&req.region, &req.account_id, app_id, id);
        st.tags.remove(&arn);
        Ok(empty(204))
    }

    fn list_environments(
        &self,
        req: &AwsRequest,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        let items: Vec<Value> = app.environments.values().map(environment_json).collect();
        page_response(items, req)
    }

    // ---- deployments -----------------------------------------------------

    #[allow(clippy::too_many_lines)]
    fn start_deployment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        env_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let strategy_id = req_str(&body, "DeploymentStrategyId")?;
        let profile_id = req_str(&body, "ConfigurationProfileId")?;
        let configuration_version = req_str(&body, "ConfigurationVersion")?;
        let description = str_field(&body, "Description");
        let kms_key_identifier = str_field(&body, "KmsKeyIdentifier");
        let now = Utc::now();

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);

        // Resolve the deployment strategy (custom or predefined).
        let strategy = st
            .deployment_strategies
            .get(&strategy_id)
            .cloned()
            .or_else(|| predefined_strategy(&strategy_id))
            .ok_or_else(|| not_found(format!("DeploymentStrategy not found: {strategy_id}")))?;
        let duration = strategy
            .get("DeploymentDurationInMinutes")
            .cloned()
            .unwrap_or(json!(0));
        let growth_type = strategy
            .get("GrowthType")
            .cloned()
            .unwrap_or(json!("LINEAR"));
        let growth_factor = strategy.get("GrowthFactor").cloned().unwrap_or(json!(100.0));
        let final_bake = strategy
            .get("FinalBakeTimeInMinutes")
            .cloned()
            .unwrap_or(json!(0));

        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        // Copy profile summary before taking a mutable borrow of the environment.
        let (config_name, config_location) = {
            let p = app
                .profiles
                .get(&profile_id)
                .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {profile_id}")))?;
            (p.name.clone(), p.location_uri.clone())
        };
        let env = app
            .environments
            .get_mut(env_id)
            .ok_or_else(|| not_found(format!("Environment not found: {env_id}")))?;
        let deployment_number = env.next_deployment_number.max(1);
        env.next_deployment_number = deployment_number + 1;

        let event_log = json!([
            {
                "EventType": "DEPLOYMENT_COMPLETED",
                "TriggeredBy": "APPCONFIG",
                "Description": "Deployment completed",
                "ActionInvocations": [],
                "OccurredAt": iso(now),
            },
            {
                "EventType": "DEPLOYMENT_STARTED",
                "TriggeredBy": "USER",
                "Description": "Deployment started",
                "ActionInvocations": [],
                "OccurredAt": iso(now),
            }
        ]);

        let mut deployment = Map::new();
        deployment.insert("ApplicationId".to_string(), json!(app_id));
        deployment.insert("EnvironmentId".to_string(), json!(env_id));
        deployment.insert("DeploymentStrategyId".to_string(), json!(strategy_id));
        deployment.insert("ConfigurationProfileId".to_string(), json!(profile_id));
        deployment.insert("DeploymentNumber".to_string(), json!(deployment_number));
        deployment.insert("ConfigurationName".to_string(), json!(config_name));
        deployment.insert("ConfigurationLocationUri".to_string(), json!(config_location));
        deployment.insert(
            "ConfigurationVersion".to_string(),
            json!(configuration_version),
        );
        insert_opt(&mut deployment, "Description", &description);
        deployment.insert("DeploymentDurationInMinutes".to_string(), duration);
        deployment.insert("GrowthType".to_string(), growth_type);
        deployment.insert("GrowthFactor".to_string(), growth_factor);
        deployment.insert("FinalBakeTimeInMinutes".to_string(), final_bake);
        deployment.insert("State".to_string(), json!("COMPLETE"));
        deployment.insert("EventLog".to_string(), event_log);
        deployment.insert("PercentageComplete".to_string(), json!(100.0));
        deployment.insert("StartedAt".to_string(), json!(iso(now)));
        deployment.insert("CompletedAt".to_string(), json!(iso(now)));
        deployment.insert("AppliedExtensions".to_string(), json!([]));
        insert_opt(&mut deployment, "KmsKeyIdentifier", &kms_key_identifier);
        let out = Value::Object(deployment);

        env.state = "READY_FOR_DEPLOYMENT".to_string();
        env.deployments.insert(
            deployment_number,
            DeploymentRecord {
                deployment_number,
                deployment: out.clone(),
            },
        );
        Ok(resp(201, out))
    }

    fn get_deployment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        env_id: &str,
        number: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = number
            .parse()
            .map_err(|_| bad_request("DeploymentNumber must be an integer"))?;
        let accounts = self.state.read();
        let dep = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.environments.get(env_id))
            .and_then(|env| env.deployments.get(&n))
            .ok_or_else(|| not_found(format!("Deployment not found: {number}")))?;
        Ok(ok(dep.deployment.clone()))
    }

    fn stop_deployment(
        &self,
        req: &AwsRequest,
        app_id: &str,
        env_id: &str,
        number: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = number
            .parse()
            .map_err(|_| bad_request("DeploymentNumber must be an integer"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let dep = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.environments.get_mut(env_id))
            .and_then(|env| env.deployments.get_mut(&n))
            .ok_or_else(|| not_found(format!("Deployment not found: {number}")))?;
        if let Some(obj) = dep.deployment.as_object_mut() {
            obj.insert("State".to_string(), json!("ROLLED_BACK"));
            obj.insert(
                "CompletedAt".to_string(),
                json!(iso(Utc::now())),
            );
        }
        Ok(resp(202, dep.deployment.clone()))
    }

    fn list_deployments(
        &self,
        req: &AwsRequest,
        app_id: &str,
        env_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let env = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.environments.get(env_id))
            .ok_or_else(|| not_found(format!("Environment not found: {env_id}")))?;
        let items: Vec<Value> = env
            .deployments
            .values()
            .rev()
            .map(|d| {
                let dep = &d.deployment;
                json!({
                    "DeploymentNumber": dep.get("DeploymentNumber"),
                    "ConfigurationName": dep.get("ConfigurationName"),
                    "ConfigurationVersion": dep.get("ConfigurationVersion"),
                    "DeploymentDurationInMinutes": dep.get("DeploymentDurationInMinutes"),
                    "GrowthType": dep.get("GrowthType"),
                    "GrowthFactor": dep.get("GrowthFactor"),
                    "FinalBakeTimeInMinutes": dep.get("FinalBakeTimeInMinutes"),
                    "State": dep.get("State"),
                    "PercentageComplete": dep.get("PercentageComplete"),
                    "StartedAt": dep.get("StartedAt"),
                    "CompletedAt": dep.get("CompletedAt"),
                })
            })
            .collect();
        page_response(items, req)
    }

    /// Legacy `GetConfiguration`: resolve the latest deployed hosted-config
    /// bytes for an app/env/profile and return them with a version header.
    fn get_configuration(
        &self,
        req: &AwsRequest,
        app_id: &str,
        env_id: &str,
        config_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        if !app.environments.contains_key(env_id) {
            return Err(not_found(format!("Environment not found: {env_id}")));
        }
        let profile = app
            .profiles
            .get(config_id)
            .or_else(|| app.profiles.values().find(|p| p.name == config_id))
            .ok_or_else(|| not_found(format!("Configuration not found: {config_id}")))?;
        let latest = profile
            .hosted_versions
            .values()
            .next_back()
            .ok_or_else(|| not_found("No configuration content deployed"))?;
        let mut resp =
            AwsResponse::json(StatusCode::OK, latest.content.clone());
        resp.content_type = latest.content_type.clone();
        set_header(&mut resp, "Content-Type", &latest.content_type);
        set_header(
            &mut resp,
            "Configuration-Version",
            &latest.version_number.to_string(),
        );
        Ok(resp)
    }

    // ---- deployment strategies -------------------------------------------

    fn create_deployment_strategy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let duration = body
            .get("DeploymentDurationInMinutes")
            .cloned()
            .ok_or_else(|| bad_request("DeploymentDurationInMinutes is required"))?;
        let growth_factor = body
            .get("GrowthFactor")
            .cloned()
            .ok_or_else(|| bad_request("GrowthFactor is required"))?;
        let id = gen_id7();
        let mut strategy = Map::new();
        strategy.insert("Id".to_string(), json!(id));
        strategy.insert("Name".to_string(), json!(name));
        if let Some(d) = str_field(&body, "Description") {
            strategy.insert("Description".to_string(), json!(d));
        }
        strategy.insert("DeploymentDurationInMinutes".to_string(), duration);
        strategy.insert(
            "GrowthType".to_string(),
            body.get("GrowthType").cloned().unwrap_or(json!("LINEAR")),
        );
        strategy.insert("GrowthFactor".to_string(), growth_factor);
        strategy.insert(
            "FinalBakeTimeInMinutes".to_string(),
            body.get("FinalBakeTimeInMinutes").cloned().unwrap_or(json!(0)),
        );
        strategy.insert(
            "ReplicateTo".to_string(),
            body.get("ReplicateTo").cloned().unwrap_or(json!("NONE")),
        );
        let out = Value::Object(strategy);
        let tags = parse_tags(body.get("Tags"));
        let arn = deployment_strategy_arn(&req.region, &req.account_id, &id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.deployment_strategies.insert(id, out.clone());
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_deployment_strategy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(p) = predefined_strategy(id) {
            return Ok(ok(p));
        }
        let accounts = self.state.read();
        let s = accounts
            .get(&req.account_id)
            .and_then(|st| st.deployment_strategies.get(id))
            .cloned()
            .ok_or_else(|| not_found(format!("DeploymentStrategy not found: {id}")))?;
        Ok(ok(s))
    }

    fn update_deployment_strategy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let s = st
            .deployment_strategies
            .get_mut(id)
            .ok_or_else(|| not_found(format!("DeploymentStrategy not found: {id}")))?;
        if let Some(obj) = s.as_object_mut() {
            for key in [
                "Description",
                "DeploymentDurationInMinutes",
                "FinalBakeTimeInMinutes",
                "GrowthFactor",
                "GrowthType",
            ] {
                if let Some(v) = body.get(key) {
                    obj.insert(key.to_string(), v.clone());
                }
            }
        }
        Ok(ok(s.clone()))
    }

    fn delete_deployment_strategy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.deployment_strategies.remove(id).is_none() {
            return Err(not_found(format!("DeploymentStrategy not found: {id}")));
        }
        let arn = deployment_strategy_arn(&req.region, &req.account_id, id);
        st.tags.remove(&arn);
        Ok(empty(204))
    }

    fn list_deployment_strategies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let mut items: Vec<Value> = ["AppConfig.AllAtOnce",
            "AppConfig.Linear50PercentEvery30Seconds",
            "AppConfig.Canary10Percent20Minutes",
            "AppConfig.Linear20PercentEvery6Minutes"]
        .iter()
        .filter_map(|id| predefined_strategy(id))
        .collect();
        if let Some(st) = accounts.get(&req.account_id) {
            items.extend(st.deployment_strategies.values().cloned());
        }
        page_response(items, req)
    }

    // ---- extensions ------------------------------------------------------

    fn create_extension(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let actions = body
            .get("Actions")
            .cloned()
            .ok_or_else(|| bad_request("Actions is required"))?;
        let id = gen_id7();
        let version = 1i64;
        let arn = extension_arn(&req.region, &req.account_id, &id, version);
        let mut ext = Map::new();
        ext.insert("Id".to_string(), json!(id));
        ext.insert("Name".to_string(), json!(name));
        ext.insert("VersionNumber".to_string(), json!(version));
        ext.insert("Arn".to_string(), json!(arn));
        if let Some(d) = str_field(&body, "Description") {
            ext.insert("Description".to_string(), json!(d));
        }
        ext.insert("Actions".to_string(), actions);
        if let Some(p) = body.get("Parameters") {
            ext.insert("Parameters".to_string(), p.clone());
        }
        let out = Value::Object(ext);
        let tags = parse_tags(body.get("Tags"));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.extensions.insert(id, out.clone());
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_extension(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|st| {
                st.extensions
                    .get(id)
                    .or_else(|| st.referenced_extensions.get(id))
            })
            .cloned()
            .ok_or_else(|| not_found(format!("Extension not found: {id}")))?;
        Ok(ok(e))
    }

    fn update_extension(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let e = st
            .extensions
            .get_mut(id)
            .ok_or_else(|| not_found(format!("Extension not found: {id}")))?;
        if let Some(obj) = e.as_object_mut() {
            for key in ["Description", "Actions", "Parameters"] {
                if let Some(v) = body.get(key) {
                    obj.insert(key.to_string(), v.clone());
                }
            }
        }
        Ok(ok(e.clone()))
    }

    fn delete_extension(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.extensions.remove(id).is_none() {
            return Err(not_found(format!("Extension not found: {id}")));
        }
        Ok(empty(204))
    }

    fn list_extensions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.extensions
                    .values()
                    .map(|e| {
                        json!({
                            "Id": e.get("Id"),
                            "Name": e.get("Name"),
                            "VersionNumber": e.get("VersionNumber"),
                            "Arn": e.get("Arn"),
                            "Description": e.get("Description"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, req)
    }

    // ---- extension associations ------------------------------------------

    fn create_extension_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let extension_identifier = req_str(&body, "ExtensionIdentifier")?;
        let resource_identifier = req_str(&body, "ResourceIdentifier")?;
        let id = gen_id7();
        let arn = extension_association_arn(&req.region, &req.account_id, &id);
        let ext_arn = if extension_identifier.starts_with("arn:") {
            extension_identifier.clone()
        } else {
            extension_arn(&req.region, &req.account_id, &extension_identifier, 1)
        };
        let mut assoc = Map::new();
        assoc.insert("Id".to_string(), json!(id));
        assoc.insert("ExtensionArn".to_string(), json!(ext_arn));
        assoc.insert("ResourceArn".to_string(), json!(resource_identifier));
        assoc.insert("Arn".to_string(), json!(arn));
        if let Some(p) = body.get("Parameters") {
            assoc.insert("Parameters".to_string(), p.clone());
        }
        if let Some(v) = body.get("ExtensionVersionNumber") {
            assoc.insert("ExtensionVersionNumber".to_string(), v.clone());
        }
        let out = Value::Object(assoc);
        let tags = parse_tags(body.get("Tags"));
        let ext_version = body
            .get("ExtensionVersionNumber")
            .cloned()
            .unwrap_or(json!(1));
        let params = body.get("Parameters").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Record the referenced extension so GetExtension can resolve it when
        // the association points at an extension not created in this account
        // (e.g. an AWS-authored extension referenced by name).
        if !st.extensions.contains_key(&extension_identifier) {
            let mut ext = Map::new();
            ext.insert("Id".to_string(), json!(extension_identifier));
            ext.insert("Name".to_string(), json!(extension_identifier));
            ext.insert("VersionNumber".to_string(), ext_version);
            ext.insert("Arn".to_string(), json!(ext_arn));
            if let Some(p) = params {
                ext.insert("Parameters".to_string(), p);
            }
            st.referenced_extensions
                .insert(extension_identifier.clone(), Value::Object(ext));
        }
        st.extension_associations.insert(id, out.clone());
        if !tags.is_empty() {
            st.tags.insert(arn, tags);
        }
        Ok(resp(201, out))
    }

    fn get_extension_association(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let a = accounts
            .get(&req.account_id)
            .and_then(|st| st.extension_associations.get(id))
            .cloned()
            .ok_or_else(|| not_found(format!("ExtensionAssociation not found: {id}")))?;
        Ok(ok(a))
    }

    fn update_extension_association(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let a = st
            .extension_associations
            .get_mut(id)
            .ok_or_else(|| not_found(format!("ExtensionAssociation not found: {id}")))?;
        if let (Some(obj), Some(p)) = (a.as_object_mut(), body.get("Parameters")) {
            obj.insert("Parameters".to_string(), p.clone());
        }
        Ok(ok(a.clone()))
    }

    fn delete_extension_association(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.extension_associations.remove(id).is_none() {
            return Err(not_found(format!("ExtensionAssociation not found: {id}")));
        }
        Ok(empty(204))
    }

    fn list_extension_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let resource = req.query_params.get("resource_identifier").cloned();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.extension_associations
                    .values()
                    .filter(|a| {
                        resource.as_ref().is_none_or(|r| {
                            a.get("ResourceArn").and_then(|v| v.as_str()) == Some(r.as_str())
                        })
                    })
                    .map(|a| {
                        json!({
                            "Id": a.get("Id"),
                            "ExtensionArn": a.get("ExtensionArn"),
                            "ResourceArn": a.get("ResourceArn"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, req)
    }

    // ---- experiments -----------------------------------------------------

    fn create_experiment_definition(
        &self,
        req: &AwsRequest,
        app_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let name = req_str(&body, "Name")?;
        let now = Utc::now();
        let id = gen_id7();
        let mut def = Map::new();
        def.insert("ApplicationId".to_string(), json!(app_id));
        def.insert("Id".to_string(), json!(id));
        def.insert("Name".to_string(), json!(name));
        for key in [
            "Hypothesis",
            "ConfigurationProfileIdentifier",
            "EnvironmentIdentifier",
            "FlagKey",
            "AudienceRule",
            "AudienceDescription",
            "LaunchCriteria",
            "Treatments",
            "Control",
            "KmsKeyIdentifier",
        ] {
            if let Some(v) = body.get(key) {
                let out_key = match key {
                    "ConfigurationProfileIdentifier" => "ConfigurationProfileId",
                    "EnvironmentIdentifier" => "EnvironmentId",
                    other => other,
                };
                def.insert(out_key.to_string(), v.clone());
            }
        }
        def.insert("Status".to_string(), json!("IDLE"));
        def.insert("CreatedAt".to_string(), json!(iso(now)));
        def.insert("UpdatedAt".to_string(), json!(iso(now)));
        let out = Value::Object(def);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        app.experiments.insert(
            id.clone(),
            ExperimentRecord {
                id,
                definition: out.clone(),
                runs: Default::default(),
                next_run_number: 1,
            },
        );
        Ok(resp(201, out))
    }

    fn get_experiment_definition(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.experiments.get(id))
            .ok_or_else(|| not_found(format!("ExperimentDefinition not found: {id}")))?;
        Ok(ok(e.definition.clone()))
    }

    fn update_experiment_definition(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let e = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.experiments.get_mut(id))
            .ok_or_else(|| not_found(format!("ExperimentDefinition not found: {id}")))?;
        if let Some(obj) = e.definition.as_object_mut() {
            for key in [
                "Treatments",
                "Control",
                "Hypothesis",
                "AudienceRule",
                "AudienceDescription",
                "LaunchCriteria",
            ] {
                if let Some(v) = body.get(key) {
                    obj.insert(key.to_string(), v.clone());
                }
            }
            obj.insert("UpdatedAt".to_string(), json!(iso(Utc::now())));
        }
        Ok(ok(e.definition.clone()))
    }

    fn delete_experiment_definition(
        &self,
        req: &AwsRequest,
        app_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let app = st
            .applications
            .get_mut(app_id)
            .ok_or_else(|| not_found(format!("Application not found: {app_id}")))?;
        if app.experiments.remove(id).is_none() {
            return Err(not_found(format!("ExperimentDefinition not found: {id}")));
        }
        Ok(empty(204))
    }

    fn list_experiment_definitions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let app_filter = req.query_params.get("application_identifier").cloned();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.applications
                    .values()
                    .filter(|app| app_filter.as_ref().is_none_or(|a| &app.id == a))
                    .flat_map(|app| {
                        app.experiments.values().map(|e| {
                            json!({
                                "ApplicationId": e.definition.get("ApplicationId"),
                                "Id": e.definition.get("Id"),
                                "Name": e.definition.get("Name"),
                                "Status": e.definition.get("Status"),
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, req)
    }

    fn experiment_run_json(app_id: &str, exp_id: &str, run: i64, status: &str) -> Value {
        let now = Utc::now();
        json!({
            "ApplicationId": app_id,
            "ExperimentDefinitionId": exp_id,
            "Run": run,
            "Status": status,
            "StartedAt": iso(now),
            "UpdatedAt": iso(now),
        })
    }

    fn start_experiment_run(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let e = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.experiments.get_mut(exp_id))
            .ok_or_else(|| not_found(format!("ExperimentDefinition not found: {exp_id}")))?;
        let run = e.next_run_number.max(1);
        e.next_run_number = run + 1;
        let mut out = Self::experiment_run_json(app_id, exp_id, run, "RUNNING");
        if let (Some(obj), Some(v)) = (out.as_object_mut(), body.get("Description")) {
            obj.insert("Description".to_string(), v.clone());
        }
        if let (Some(obj), Some(v)) = (out.as_object_mut(), body.get("ExposurePercentage")) {
            obj.insert("ExposurePercentage".to_string(), v.clone());
        }
        e.runs.insert(run, out.clone());
        if let Some(obj) = e.definition.as_object_mut() {
            obj.insert("Status".to_string(), json!("ACTIVE"));
        }
        Ok(resp(201, out))
    }

    fn get_experiment_run(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
        run: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = run
            .parse()
            .map_err(|_| bad_request("Run must be an integer"))?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.experiments.get(exp_id))
            .and_then(|e| e.runs.get(&n))
            .cloned()
            .ok_or_else(|| not_found(format!("ExperimentRun not found: {run}")))?;
        Ok(ok(r))
    }

    fn update_experiment_run(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
        run: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = run
            .parse()
            .map_err(|_| bad_request("Run must be an integer"))?;
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let r = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.experiments.get_mut(exp_id))
            .and_then(|e| e.runs.get_mut(&n))
            .ok_or_else(|| not_found(format!("ExperimentRun not found: {run}")))?;
        if let Some(obj) = r.as_object_mut() {
            for key in ["Description", "ExposurePercentage", "TreatmentOverrides"] {
                if let Some(v) = body.get(key) {
                    obj.insert(key.to_string(), v.clone());
                }
            }
            obj.insert("UpdatedAt".to_string(), json!(iso(Utc::now())));
        }
        Ok(ok(r.clone()))
    }

    fn stop_experiment_run(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
        run: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = run
            .parse()
            .map_err(|_| bad_request("Run must be an integer"))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let e = st
            .applications
            .get_mut(app_id)
            .and_then(|app| app.experiments.get_mut(exp_id))
            .ok_or_else(|| not_found(format!("ExperimentDefinition not found: {exp_id}")))?;
        let r = e
            .runs
            .get_mut(&n)
            .ok_or_else(|| not_found(format!("ExperimentRun not found: {run}")))?;
        if let Some(obj) = r.as_object_mut() {
            obj.insert("Status".to_string(), json!("DONE"));
            obj.insert("EndedAt".to_string(), json!(iso(Utc::now())));
        }
        let out = r.clone();
        if let Some(obj) = e.definition.as_object_mut() {
            obj.insert("Status".to_string(), json!("IDLE"));
        }
        Ok(ok(out))
    }

    fn list_experiment_runs(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.experiments.get(exp_id))
            .ok_or_else(|| not_found(format!("ExperimentDefinition not found: {exp_id}")))?;
        let items: Vec<Value> = e
            .runs
            .values()
            .rev()
            .map(|r| {
                json!({
                    "Run": r.get("Run"),
                    "Status": r.get("Status"),
                    "StartedAt": r.get("StartedAt"),
                    "EndedAt": r.get("EndedAt"),
                })
            })
            .collect();
        page_response(items, req)
    }

    fn list_experiment_run_events(
        &self,
        req: &AwsRequest,
        app_id: &str,
        exp_id: &str,
        run: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let n: i64 = run
            .parse()
            .map_err(|_| bad_request("Run must be an integer"))?;
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|st| st.applications.get(app_id))
            .and_then(|app| app.experiments.get(exp_id))
            .and_then(|e| e.runs.get(&n))
            .ok_or_else(|| not_found(format!("ExperimentRun not found: {run}")))?;
        let started = r.get("StartedAt").cloned().unwrap_or(json!(iso(Utc::now())));
        let items = vec![json!({
            "EventType": "RUN_STARTED",
            "Description": "Experiment run started",
            "OccurredAt": started,
        })];
        page_response(items, req)
    }

    // ---- account settings ------------------------------------------------

    fn default_account_settings() -> Value {
        json!({
            "DeletionProtection": {
                "Enabled": false,
                "ProtectionPeriodInMinutes": 60,
            },
            "VendedMetrics": {
                "Enabled": false,
            },
        })
    }

    fn get_account_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let settings = accounts
            .get(&req.account_id)
            .and_then(|st| st.account_settings.clone())
            .unwrap_or_else(Self::default_account_settings);
        Ok(ok(settings))
    }

    fn update_account_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let mut settings = st
            .account_settings
            .clone()
            .unwrap_or_else(Self::default_account_settings);
        if let Some(obj) = settings.as_object_mut() {
            if let Some(v) = body.get("DeletionProtection") {
                obj.insert("DeletionProtection".to_string(), v.clone());
            }
            if let Some(v) = body.get("VendedMetrics") {
                obj.insert("VendedMetrics".to_string(), v.clone());
            }
        }
        st.account_settings = Some(settings.clone());
        Ok(ok(settings))
    }

    // ---- tags ------------------------------------------------------------

    fn list_tags(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let tags = accounts
            .get(&req.account_id)
            .and_then(|st| st.tags.get(arn).cloned())
            .unwrap_or_default();
        Ok(ok(json!({ "Tags": tag_map_to_value(&tags) })))
    }

    fn tag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let new = parse_tags(body.get("Tags"));
        if new.is_empty() {
            return Err(bad_request("Tags is required"));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let entry = st.tags.entry(arn.to_string()).or_default();
        for (k, v) in new {
            entry.insert(k, v);
        }
        Ok(empty(204))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let keys = query_all(req, "tagKeys");
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(entry) = st.tags.get_mut(arn) {
            for k in &keys {
                entry.remove(k);
            }
        }
        Ok(empty(204))
    }

    // ---- appconfigdata (data plane) --------------------------------------

    fn start_configuration_session(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let app_ident = req_str(&body, "ApplicationIdentifier")?;
        let env_ident = req_str(&body, "EnvironmentIdentifier")?;
        let profile_ident = req_str(&body, "ConfigurationProfileIdentifier")?;

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Resolve identifiers (id or name) to concrete ids so the token binds
        // to real resources; the data plane rejects unknown ones.
        let app = st
            .applications
            .values()
            .find(|a| a.id == app_ident || a.name == app_ident)
            .ok_or_else(|| not_found(format!("Application not found: {app_ident}")))?;
        let env = app
            .environments
            .values()
            .find(|e| e.id == env_ident || e.name == env_ident)
            .ok_or_else(|| not_found(format!("Environment not found: {env_ident}")))?;
        let profile = app
            .profiles
            .values()
            .find(|p| p.id == profile_ident || p.name == profile_ident)
            .ok_or_else(|| not_found(format!("ConfigurationProfile not found: {profile_ident}")))?;
        let token = gen_token();
        let record = SessionRecord {
            token: token.clone(),
            application_id: app.id.clone(),
            environment_id: env.id.clone(),
            configuration_profile_id: profile.id.clone(),
        };
        st.sessions.insert(token.clone(), record);
        Ok(resp(201, json!({ "InitialConfigurationToken": token })))
    }

    fn get_latest_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = req
            .query_params
            .get("configuration_token")
            .cloned()
            .ok_or_else(|| bad_request("ConfigurationToken is required"))?;
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .ok_or_else(|| bad_request("Invalid ConfigurationToken"))?;
        let session = st
            .sessions
            .get(&token)
            .ok_or_else(|| bad_request("Invalid ConfigurationToken"))?;
        let app = st
            .applications
            .get(&session.application_id)
            .ok_or_else(|| not_found("Application not found"))?;
        let profile = app
            .profiles
            .get(&session.configuration_profile_id)
            .ok_or_else(|| not_found("ConfigurationProfile not found"))?;
        // Resolve the latest deployed hosted-config version's bytes (highest
        // version number wins) for this app/env/profile.
        let latest = profile.hosted_versions.values().next_back();
        let (content, content_type, version) = match latest {
            Some(v) => (
                v.content.clone(),
                v.content_type.clone(),
                v.version_number.to_string(),
            ),
            None => (Vec::new(), "application/json".to_string(), "null".to_string()),
        };
        let next_token = gen_token();
        let mut resp = AwsResponse::json(StatusCode::OK, content);
        resp.content_type = content_type.clone();
        set_header(&mut resp, "Content-Type", &content_type);
        set_header(&mut resp, "Next-Poll-Configuration-Token", &next_token);
        set_header(&mut resp, "Next-Poll-Interval-In-Seconds", "60");
        set_header(&mut resp, "Version-Label", &version);
        Ok(resp)
    }
}
