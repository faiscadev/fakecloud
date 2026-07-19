//! `StepFunctionsService` `state_machines` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    // ─── State Machine CRUD ─────────────────────────────────────────────

    pub(super) fn create_state_machine(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_required("name", &body["name"])?;
        let name = body["name"].as_str().ok_or_else(|| missing("name"))?;
        validate_name(name)?;

        validate_required("definition", &body["definition"])?;
        let definition = body["definition"]
            .as_str()
            .ok_or_else(|| missing("definition"))?;
        validate_definition(definition)?;

        validate_required("roleArn", &body["roleArn"])?;
        let role_arn = body["roleArn"].as_str().ok_or_else(|| missing("roleArn"))?;
        validate_arn(role_arn)?;

        let machine_type = if let Some(t) = body["type"].as_str() {
            StateMachineType::parse(t).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "Value '{t}' at 'type' failed to satisfy constraint: \
                         Member must satisfy enum value set: [STANDARD, EXPRESS]"
                    ),
                )
            })?
        } else {
            StateMachineType::Standard
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let arn = state.state_machine_arn(name);

        // Check if name already exists
        if state.state_machines.values().any(|sm| sm.name == name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "StateMachineAlreadyExists",
                format!("State Machine Already Exists: '{arn}'"),
            ));
        }

        let now = Utc::now();
        let revision_id = uuid::Uuid::new_v4().to_string();

        let mut tags = BTreeMap::new();
        if !body["tags"].is_null() {
            fakecloud_core::tags::apply_tags(&mut tags, &body, "tags", "key", "value").map_err(
                |f| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!("{f} must be a list"),
                    )
                },
            )?;
        }

        let sm = StateMachine {
            name: name.to_string(),
            arn: arn.clone(),
            definition: definition.to_string(),
            role_arn: role_arn.to_string(),
            machine_type,
            status: StateMachineStatus::Active,
            creation_date: now,
            update_date: now,
            tags,
            revision_id: revision_id.clone(),
            logging_configuration: body.get("loggingConfiguration").cloned(),
            tracing_configuration: body.get("tracingConfiguration").cloned(),
            description: body["description"].as_str().unwrap_or("").to_string(),
            encryption_configuration: body.get("encryptionConfiguration").cloned(),
        };

        state.state_machines.insert(arn.clone(), sm);

        let mut resp = json!({
            "stateMachineArn": arn,
            "creationDate": now.timestamp() as f64,
        });
        // `publish: true` creates version 1 and returns its version-qualified ARN
        // (which ListStateMachineVersions resolves). Without publish, no version
        // exists and no `stateMachineVersionArn` is returned.
        if body["publish"].as_bool() == Some(true) {
            let version_description = body["versionDescription"].as_str().unwrap_or("");
            let (version_arn, _) = publish_version(state, &arn, version_description);
            resp["stateMachineVersionArn"] = json!(version_arn);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_state_machine(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let sm = state
            .state_machines
            .get(arn)
            .ok_or_else(|| state_machine_not_found(arn))?;

        Ok(AwsResponse::ok_json(state_machine_to_json(sm)))
    }

    pub(super) fn list_state_machines(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let raw_max_results = body["maxResults"].as_i64();
        if let Some(mr) = raw_max_results {
            validate_max_results(mr)?;
        }
        // Smithy default: maxResults=0 means "use the service default" (100).
        let max_results = match raw_max_results.unwrap_or(0) {
            0 => 100,
            n => n as usize,
        };
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut machines: Vec<&StateMachine> = state.state_machines.values().collect();
        machines.sort_by(|a, b| a.name.cmp(&b.name));

        let items: Vec<Value> = machines
            .iter()
            .map(|sm| {
                json!({
                    "name": sm.name,
                    "stateMachineArn": sm.arn,
                    "type": sm.machine_type.as_str(),
                    "creationDate": sm.creation_date.timestamp() as f64,
                })
            })
            .collect();

        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;

        let mut resp = json!({ "stateMachines": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_state_machine(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // AWS returns success even if it doesn't exist
        state.state_machines.remove(arn);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_state_machine(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sm = state
            .state_machines
            .get_mut(arn)
            .ok_or_else(|| state_machine_not_found(arn))?;

        if let Some(definition) = body["definition"].as_str() {
            validate_definition(definition)?;
            sm.definition = definition.to_string();
        }

        if let Some(role_arn) = body["roleArn"].as_str() {
            validate_arn(role_arn)?;
            sm.role_arn = role_arn.to_string();
        }

        if let Some(logging) = body.get("loggingConfiguration") {
            sm.logging_configuration = Some(logging.clone());
        }

        if let Some(tracing) = body.get("tracingConfiguration") {
            sm.tracing_configuration = Some(tracing.clone());
        }

        if let Some(description) = body["description"].as_str() {
            sm.description = description.to_string();
        }

        if let Some(enc) = body.get("encryptionConfiguration") {
            sm.encryption_configuration = Some(enc.clone());
        }

        let now = Utc::now();
        sm.update_date = now;
        sm.revision_id = uuid::Uuid::new_v4().to_string();

        let revision_id = sm.revision_id.clone();
        let sm_arn = sm.arn.clone();

        let mut resp = json!({
            "updateDate": now.timestamp() as f64,
            "revisionId": revision_id,
        });
        // `publish: true` publishes a new version reflecting this update and
        // returns its version-qualified ARN. Without publish, no version ARN is
        // returned (a bare update does not create a version).
        if body["publish"].as_bool() == Some(true) {
            let version_description = body["versionDescription"].as_str().unwrap_or("");
            let (version_arn, _) = publish_version(state, &sm_arn, version_description);
            resp["stateMachineVersionArn"] = json!(version_arn);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_state_machine_for_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        let sm = state
            .state_machines
            .get(&exec.state_machine_arn)
            .ok_or_else(|| state_machine_not_found(&exec.state_machine_arn))?;

        Ok(AwsResponse::ok_json(state_machine_to_json(sm)))
    }

    // ─── State machine versions / aliases ───────────────────────────────

    pub(super) fn publish_state_machine_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?
            .to_string();
        let description = body["description"].as_str().unwrap_or("");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.state_machines.contains_key(&arn) {
            return Err(state_machine_not_found(&arn));
        }
        let (version_arn, creation_date) = publish_version(state, &arn, description);
        Ok(AwsResponse::ok_json(json!({
            "stateMachineVersionArn": version_arn,
            "creationDate": creation_date.timestamp(),
        })))
    }

    pub(super) fn delete_state_machine_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineVersionArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineVersionArn"))?
            .to_string();
        validate_arn_length("stateMachineVersionArn", &arn, 2000)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.state_machine_versions.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_state_machine_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?
            .to_string();
        validate_arn_length("stateMachineArn", &arn, 256)?;
        let raw_max_results = body["maxResults"].as_i64();
        if let Some(mr) = raw_max_results {
            validate_max_results(mr)?;
        }
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }
        let max_results = match raw_max_results.unwrap_or(0) {
            0 => 100,
            n => n as usize,
        };
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut versions: Vec<&crate::state::StateMachineVersion> = state
            .state_machine_versions
            .values()
            .filter(|v| v.state_machine_arn == arn)
            .collect();
        versions.sort_by_key(|v| std::cmp::Reverse(v.version));
        let items: Vec<Value> = versions
            .iter()
            .map(|v| {
                json!({
                    "stateMachineVersionArn": format!("{}:{}", v.state_machine_arn, v.version),
                    "creationDate": v.creation_date.timestamp(),
                })
            })
            .collect();
        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;
        let mut resp = json!({ "stateMachineVersions": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn create_state_machine_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["name"]
            .as_str()
            .ok_or_else(|| missing("name"))?
            .to_string();
        validate_name(&name)?;
        let routing_cfg = body["routingConfiguration"]
            .as_array()
            .ok_or_else(|| missing("routingConfiguration"))?;
        let routes = parse_routing_configuration(routing_cfg)?;
        let parent_arn = routes[0]
            .state_machine_version_arn
            .rsplit_once(':')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default();
        // All routed versions must belong to the same state machine.
        // Otherwise an alias could mix versions of two different
        // workflows, which AWS rejects with InvalidArn.
        for route in &routes[1..] {
            let other_parent = route
                .state_machine_version_arn
                .rsplit_once(':')
                .map(|(p, _)| p.to_string())
                .unwrap_or_default();
            if other_parent != parent_arn {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArn",
                    "routingConfiguration entries must all reference versions of the same state machine",
                ));
            }
        }
        let alias_arn = format!("{parent_arn}:{name}");
        let now = chrono::Utc::now();
        let alias = crate::state::StateMachineAlias {
            name,
            arn: alias_arn.clone(),
            description: body["description"].as_str().unwrap_or("").to_string(),
            routing_configuration: routes,
            creation_date: now,
            update_date: now,
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.state_machine_aliases.insert(alias_arn.clone(), alias);
        Ok(AwsResponse::ok_json(json!({
            "stateMachineAliasArn": alias_arn,
            "creationDate": now.timestamp(),
        })))
    }

    pub(super) fn delete_state_machine_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineAliasArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineAliasArn"))?
            .to_string();
        validate_arn_length("stateMachineAliasArn", &arn, 256)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.state_machine_aliases.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_state_machine_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineAliasArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineAliasArn"))?
            .to_string();
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let alias = state
            .state_machine_aliases
            .get(&arn)
            .ok_or_else(|| resource_not_found(&arn))?;
        Ok(AwsResponse::ok_json(state_machine_alias_to_json(alias)))
    }

    pub(super) fn list_state_machine_aliases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let parent = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?
            .to_string();
        validate_arn_length("stateMachineArn", &parent, 256)?;
        let raw_max_results = body["maxResults"].as_i64();
        if let Some(mr) = raw_max_results {
            validate_max_results(mr)?;
        }
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }
        let max_results = match raw_max_results.unwrap_or(0) {
            0 => 100,
            n => n as usize,
        };
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // Anchor the prefix on the alias separator so a state machine
        // named `foo` doesn't pull in aliases for `foobar`.
        let parent_prefix = format!("{parent}:");
        let mut aliases: Vec<&crate::state::StateMachineAlias> = state
            .state_machine_aliases
            .values()
            .filter(|a| a.arn.starts_with(&parent_prefix))
            .collect();
        aliases.sort_by(|a, b| a.name.cmp(&b.name));
        let items: Vec<Value> = aliases
            .iter()
            .map(|a| {
                json!({
                    "stateMachineAliasArn": a.arn,
                    "creationDate": a.creation_date.timestamp(),
                })
            })
            .collect();
        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;
        let mut resp = json!({ "stateMachineAliases": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_state_machine_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["stateMachineAliasArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineAliasArn"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let alias = state
            .state_machine_aliases
            .get_mut(&arn)
            .ok_or_else(|| resource_not_found(&arn))?;
        if let Some(d) = body["description"].as_str() {
            alias.description = d.to_string();
        }
        if let Some(routes) = body["routingConfiguration"].as_array() {
            alias.routing_configuration = parse_routing_configuration(routes)?;
        }
        alias.update_date = chrono::Utc::now();
        Ok(AwsResponse::ok_json(json!({
            "updateDate": alias.update_date.timestamp(),
        })))
    }

    pub(super) fn validate_state_machine_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let definition = body["definition"]
            .as_str()
            .ok_or_else(|| missing("definition"))?;
        if definition.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "definition must be 1..=1048576 characters",
            ));
        }
        if let Some(mr) = body["maxResults"].as_i64() {
            if !(0..=100).contains(&mr) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("maxResults '{mr}' is outside 0..=100"),
                ));
            }
        }
        if let Some(sev) = body["severity"].as_str() {
            if !matches!(sev, "ERROR" | "WARNING") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("severity '{sev}' must be ERROR or WARNING"),
                ));
            }
        }
        if let Some(ty) = body["type"].as_str() {
            if !matches!(ty, "STANDARD" | "EXPRESS") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("type '{ty}' must be STANDARD or EXPRESS"),
                ));
            }
        }
        match validate_definition(definition) {
            Ok(()) => Ok(AwsResponse::ok_json(json!({
                "result": "OK",
                "diagnostics": [],
            }))),
            Err(e) => Ok(AwsResponse::ok_json(json!({
                "result": "FAIL",
                "diagnostics": [{
                    "severity": "ERROR",
                    "code": "INVALID_DEFINITION",
                    "message": e.to_string(),
                }],
            }))),
        }
    }
}

/// Publish a new state-machine version and return its version-qualified ARN
/// plus its creation timestamp. The version number is the current max for the
/// state machine plus one (starting at 1). Callers must hold the state write
/// lock and have verified the state machine exists.
fn publish_version(
    state: &mut crate::state::StepFunctionsState,
    arn: &str,
    description: &str,
) -> (String, chrono::DateTime<chrono::Utc>) {
    let version = state
        .state_machine_versions
        .values()
        .filter(|v| v.state_machine_arn == arn)
        .map(|v| v.version)
        .max()
        .unwrap_or(0)
        + 1;
    let version_arn = format!("{arn}:{version}");
    let v = crate::state::StateMachineVersion {
        state_machine_arn: arn.to_string(),
        version,
        revision_id: format!("rev-{version}"),
        description: description.to_string(),
        creation_date: chrono::Utc::now(),
    };
    let creation_date = v.creation_date;
    state.state_machine_versions.insert(version_arn.clone(), v);
    (version_arn, creation_date)
}
