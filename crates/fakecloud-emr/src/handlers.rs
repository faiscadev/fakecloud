// Operation handlers for Amazon EMR, `include!`d into `service.rs` so they
// share its module scope (`invalid_request`, `ok`, `now_epoch`, id helpers,
// and the `EmrState` accessors).

/// Fetch a string body field.
fn sf<'a>(b: &'a Value, k: &str) -> Option<&'a str> {
    b.get(k).and_then(Value::as_str)
}

/// A `ClusterStatus` in a running-but-idle (WAITING) state.
fn waiting_status(created: f64) -> Value {
    json!({
        "State": "WAITING",
        "StateChangeReason": { "Code": "USER_REQUEST", "Message": "Waiting for steps to run" },
        "Timeline": { "CreationDateTime": created, "ReadyDateTime": created },
    })
}

/// Convert an input `KeyValueList` (`[{Key,Value}]`) into a `StringMap`.
fn key_values_to_map(list: Option<&Value>) -> Value {
    let mut map = serde_json::Map::new();
    if let Some(arr) = list.and_then(Value::as_array) {
        for kv in arr {
            if let (Some(k), Some(v)) = (sf(kv, "Key"), sf(kv, "Value")) {
                map.insert(k.to_string(), Value::String(v.to_string()));
            }
        }
    }
    Value::Object(map)
}

impl EmrService {
    // ---- clusters / job flows --------------------------------------------

    pub(crate) fn run_job_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "Name").unwrap_or("cluster").to_string();
        let id = format!("j-{}", rand_suffix(13));
        let arn = cluster_arn(&req.region, &req.account_id, &id);
        let created = now_epoch();
        let instances = body.get("Instances").cloned().unwrap_or(json!({}));
        let keep_alive = instances
            .get("KeepJobFlowAliveWhenNoSteps")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let termination_protected = instances
            .get("TerminationProtected")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let collection_type = if instances
            .get("InstanceFleets")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty())
        {
            "INSTANCE_FLEET"
        } else {
            "INSTANCE_GROUP"
        };

        let mut cluster = serde_json::Map::new();
        cluster.insert("Id".into(), json!(id));
        cluster.insert("Name".into(), json!(name));
        cluster.insert("Status".into(), waiting_status(created));
        cluster.insert("InstanceCollectionType".into(), json!(collection_type));
        cluster.insert("AutoTerminate".into(), json!(!keep_alive));
        cluster.insert("TerminationProtected".into(), json!(termination_protected));
        cluster.insert(
            "VisibleToAllUsers".into(),
            json!(body.get("VisibleToAllUsers").and_then(Value::as_bool).unwrap_or(true)),
        );
        cluster.insert("NormalizedInstanceHours".into(), json!(0));
        cluster.insert(
            "MasterPublicDnsName".into(),
            json!(format!("ec2-master.{}.compute.amazonaws.com", req.region)),
        );
        cluster.insert("ClusterArn".into(), json!(arn));
        cluster.insert(
            "StepConcurrencyLevel".into(),
            json!(body.get("StepConcurrencyLevel").and_then(Value::as_i64).unwrap_or(1)),
        );
        for (in_key, out_key) in [
            ("ReleaseLabel", "ReleaseLabel"),
            ("LogUri", "LogUri"),
            ("LogEncryptionKmsKeyId", "LogEncryptionKmsKeyId"),
            ("ServiceRole", "ServiceRole"),
            ("AutoScalingRole", "AutoScalingRole"),
            ("ScaleDownBehavior", "ScaleDownBehavior"),
            ("SecurityConfiguration", "SecurityConfiguration"),
            ("CustomAmiId", "CustomAmiId"),
            ("OSReleaseLabel", "OSReleaseLabel"),
        ] {
            if let Some(v) = sf(&body, in_key) {
                cluster.insert(out_key.into(), json!(v));
            }
        }
        for key in ["Applications", "Tags", "Configurations", "PlacementGroups"] {
            if let Some(arr) = body.get(key).filter(|v| v.is_array()) {
                cluster.insert(key.into(), arr.clone());
            }
        }
        let cluster = Value::Object(cluster);

        // Sub-resources derived from the RunJobFlow request.
        let groups = self.build_instance_groups(&instances, created);
        let fleets = self.build_instance_fleets(&instances, created);
        let cluster_instances = self.build_instances(&groups, created);
        let steps = self.build_steps(body.get("Steps"), created);
        let bootstrap = build_bootstrap_actions(body.get("BootstrapActions"));
        let job_flow = build_job_flow_detail(&id, &name, &body, &instances, created);

        self.with_account_mut(req, |acct| {
            acct.clusters.insert(id.clone(), cluster);
            acct.cluster_order.push(id.clone());
            acct.job_flows.insert(id.clone(), job_flow);
            if !groups.is_empty() {
                acct.instance_groups.insert(id.clone(), groups);
            }
            if !fleets.is_empty() {
                acct.instance_fleets.insert(id.clone(), fleets);
            }
            if !cluster_instances.is_empty() {
                acct.instances.insert(id.clone(), cluster_instances);
            }
            if !steps.is_empty() {
                acct.steps.insert(id.clone(), steps);
            }
            if !bootstrap.is_empty() {
                acct.bootstrap_actions.insert(id.clone(), bootstrap);
            }
            if let Some(tags) = body.get("Tags").and_then(Value::as_array) {
                if !tags.is_empty() {
                    acct.tags.insert(id.clone(), tags.clone());
                }
            }
        });

        ok(json!({ "JobFlowId": id, "ClusterArn": arn }))
    }

    fn build_instance_groups(&self, instances: &Value, created: f64) -> Vec<Value> {
        let Some(configs) = instances.get("InstanceGroups").and_then(Value::as_array) else {
            return Vec::new();
        };
        configs
            .iter()
            .map(|c| {
                let count = c.get("InstanceCount").and_then(Value::as_i64).unwrap_or(1);
                let mut g = serde_json::Map::new();
                g.insert("Id".into(), json!(format!("ig-{}", rand_suffix(13))));
                if let Some(n) = sf(c, "Name") {
                    g.insert("Name".into(), json!(n));
                }
                g.insert(
                    "Market".into(),
                    json!(sf(c, "Market").unwrap_or("ON_DEMAND")),
                );
                g.insert(
                    "InstanceGroupType".into(),
                    json!(sf(c, "InstanceRole").unwrap_or("CORE")),
                );
                if let Some(bid) = sf(c, "BidPrice") {
                    g.insert("BidPrice".into(), json!(bid));
                }
                g.insert(
                    "InstanceType".into(),
                    json!(sf(c, "InstanceType").unwrap_or("m5.xlarge")),
                );
                g.insert("RequestedInstanceCount".into(), json!(count));
                g.insert("RunningInstanceCount".into(), json!(count));
                g.insert(
                    "Status".into(),
                    json!({
                        "State": "RUNNING",
                        "StateChangeReason": { "Message": "" },
                        "Timeline": { "CreationDateTime": created, "ReadyDateTime": created },
                    }),
                );
                if let Some(cfgs) = c.get("Configurations").filter(|v| v.is_array()) {
                    g.insert("Configurations".into(), cfgs.clone());
                }
                Value::Object(g)
            })
            .collect()
    }

    fn build_instance_fleets(&self, instances: &Value, created: f64) -> Vec<Value> {
        let Some(configs) = instances.get("InstanceFleets").and_then(Value::as_array) else {
            return Vec::new();
        };
        configs
            .iter()
            .map(|c| {
                let on_demand = c
                    .get("TargetOnDemandCapacity")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let spot = c.get("TargetSpotCapacity").and_then(Value::as_i64).unwrap_or(0);
                let mut f = serde_json::Map::new();
                f.insert("Id".into(), json!(format!("if-{}", rand_suffix(13))));
                if let Some(n) = sf(c, "Name") {
                    f.insert("Name".into(), json!(n));
                }
                f.insert(
                    "InstanceFleetType".into(),
                    json!(sf(c, "InstanceFleetType").unwrap_or("CORE")),
                );
                f.insert("TargetOnDemandCapacity".into(), json!(on_demand));
                f.insert("TargetSpotCapacity".into(), json!(spot));
                f.insert("ProvisionedOnDemandCapacity".into(), json!(on_demand));
                f.insert("ProvisionedSpotCapacity".into(), json!(spot));
                f.insert(
                    "Status".into(),
                    json!({
                        "State": "RUNNING",
                        "StateChangeReason": { "Message": "" },
                        "Timeline": { "CreationDateTime": created, "ReadyDateTime": created },
                    }),
                );
                Value::Object(f)
            })
            .collect()
    }

    fn build_instances(&self, groups: &[Value], created: f64) -> Vec<Value> {
        let mut out = Vec::new();
        for g in groups {
            let count = g
                .get("RunningInstanceCount")
                .and_then(Value::as_i64)
                .unwrap_or(1)
                .max(1);
            let group_id = sf(g, "Id").unwrap_or_default().to_string();
            let instance_type = sf(g, "InstanceType").unwrap_or("m5.xlarge").to_string();
            let market = sf(g, "Market").unwrap_or("ON_DEMAND").to_string();
            for _ in 0..count {
                out.push(json!({
                    "Id": format!("ci-{}", rand_suffix(13)),
                    "Ec2InstanceId": format!("i-{}", rand_suffix(17).to_ascii_lowercase()),
                    "PublicDnsName": "",
                    "PrivateDnsName": "ip-10-0-0-1.ec2.internal",
                    "PrivateIpAddress": "10.0.0.1",
                    "Status": {
                        "State": "RUNNING",
                        "StateChangeReason": { "Message": "" },
                        "Timeline": { "CreationDateTime": created, "ReadyDateTime": created },
                    },
                    "InstanceGroupId": group_id,
                    "Market": market,
                    "InstanceType": instance_type,
                }));
            }
        }
        out
    }

    fn build_steps(&self, steps_in: Option<&Value>, created: f64) -> Vec<Value> {
        let Some(arr) = steps_in.and_then(Value::as_array) else {
            return Vec::new();
        };
        arr.iter().map(|s| build_step(s, created)).collect()
    }

    pub(crate) fn describe_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| match acct.clusters.get(&id) {
            Some(c) => ok(json!({ "Cluster": c })),
            None => Err(invalid_request(format!("Cluster id '{id}' is not valid."))),
        })
    }

    pub(crate) fn list_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let states: Vec<String> = body
            .get("ClusterStates")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        self.with_account(req, |acct| {
            let mut summaries = Vec::new();
            for id in &acct.cluster_order {
                let Some(c) = acct.clusters.get(id) else { continue };
                let state = c.pointer("/Status/State").and_then(Value::as_str).unwrap_or("");
                if !states.is_empty() && !states.iter().any(|s| s == state) {
                    continue;
                }
                summaries.push(cluster_summary(c));
            }
            ok(json!({ "Clusters": summaries }))
        })
    }

    pub(crate) fn terminate_job_flows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids: Vec<String> = string_list(&body, "JobFlowIds");
        let now = now_epoch();
        self.with_account_mut(req, |acct| {
            for id in &ids {
                if let Some(c) = acct.clusters.get_mut(id).and_then(Value::as_object_mut) {
                    c.insert(
                        "Status".into(),
                        json!({
                            "State": "TERMINATED",
                            "StateChangeReason": { "Code": "USER_REQUEST", "Message": "Terminated by user request" },
                            "Timeline": { "CreationDateTime": now, "ReadyDateTime": now, "EndDateTime": now },
                        }),
                    );
                }
                if let Some(jf) = acct.job_flows.get_mut(id).and_then(Value::as_object_mut) {
                    if let Some(esd) = jf.get_mut("ExecutionStatusDetail").and_then(Value::as_object_mut) {
                        esd.insert("State".into(), json!("TERMINATED"));
                        esd.insert("EndDateTime".into(), json!(now));
                    }
                }
            }
        });
        ok(json!({}))
    }

    pub(crate) fn modify_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let level = body.get("StepConcurrencyLevel").and_then(Value::as_i64);
        let extended = body.get("ExtendedSupport").and_then(Value::as_bool);
        self.with_account_mut(req, |acct| {
            let Some(c) = acct.clusters.get_mut(&id).and_then(Value::as_object_mut) else {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            };
            if let Some(l) = level {
                c.insert("StepConcurrencyLevel".into(), json!(l));
            }
            if let Some(e) = extended {
                c.insert("ExtendedSupport".into(), json!(e));
            }
            let out_level = c
                .get("StepConcurrencyLevel")
                .and_then(Value::as_i64)
                .unwrap_or(1);
            let mut out = serde_json::Map::new();
            out.insert("StepConcurrencyLevel".into(), json!(out_level));
            if let Some(e) = c.get("ExtendedSupport").cloned() {
                out.insert("ExtendedSupport".into(), e);
            }
            ok(Value::Object(out))
        })
    }

    pub(crate) fn set_termination_protection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_cluster_bool(req, "TerminationProtected", "TerminationProtected")
    }

    pub(crate) fn set_visible_to_all_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_cluster_bool(req, "VisibleToAllUsers", "VisibleToAllUsers")
    }

    pub(crate) fn set_unhealthy_node_replacement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.set_cluster_bool(req, "UnhealthyNodeReplacement", "UnhealthyNodeReplacement")
    }

    pub(crate) fn set_keep_alive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids = string_list(&body, "JobFlowIds");
        let keep = body
            .get("KeepJobFlowAliveWhenNoSteps")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.with_account_mut(req, |acct| {
            for id in &ids {
                if let Some(c) = acct.clusters.get_mut(id).and_then(Value::as_object_mut) {
                    c.insert("AutoTerminate".into(), json!(!keep));
                }
            }
        });
        ok(json!({}))
    }

    fn set_cluster_bool(
        &self,
        req: &AwsRequest,
        in_field: &str,
        out_field: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids = string_list(&body, "JobFlowIds");
        let value = body.get(in_field).and_then(Value::as_bool).unwrap_or(false);
        self.with_account_mut(req, |acct| {
            for id in &ids {
                if let Some(c) = acct.clusters.get_mut(id).and_then(Value::as_object_mut) {
                    c.insert(out_field.into(), json!(value));
                }
            }
        });
        ok(json!({}))
    }

    pub(crate) fn describe_job_flows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filter: Vec<String> = string_list(&body, "JobFlowIds");
        self.with_account(req, |acct| {
            let mut flows = Vec::new();
            for id in &acct.cluster_order {
                if !filter.is_empty() && !filter.contains(id) {
                    continue;
                }
                if let Some(jf) = acct.job_flows.get(id) {
                    flows.push(jf.clone());
                }
            }
            ok(json!({ "JobFlows": flows }))
        })
    }

    // ---- steps -----------------------------------------------------------

    pub(crate) fn add_job_flow_steps(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "JobFlowId").unwrap_or_default().to_string();
        let created = now_epoch();
        let new_steps: Vec<Value> = body
            .get("Steps")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(|s| build_step(s, created)).collect())
            .unwrap_or_default();
        let step_ids: Vec<String> = new_steps
            .iter()
            .filter_map(|s| sf(s, "Id").map(String::from))
            .collect();
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            acct.steps.entry(id.clone()).or_default().extend(new_steps);
            ok(json!({ "StepIds": step_ids }))
        })
    }

    pub(crate) fn list_steps(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let states: Vec<String> = string_list(&body, "StepStates");
        let step_ids: Vec<String> = string_list(&body, "StepIds");
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let steps = acct.steps.get(&id).cloned().unwrap_or_default();
            let summaries: Vec<Value> = steps
                .iter()
                .filter(|s| {
                    let state_ok = states.is_empty()
                        || s.pointer("/Status/State")
                            .and_then(Value::as_str)
                            .is_some_and(|st| states.iter().any(|x| x == st));
                    let id_ok = step_ids.is_empty()
                        || sf(s, "Id").is_some_and(|sid| step_ids.iter().any(|x| x == sid));
                    state_ok && id_ok
                })
                .map(step_summary)
                .collect();
            ok(json!({ "Steps": summaries }))
        })
    }

    pub(crate) fn describe_step(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let step_id = sf(&body, "StepId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            let step = acct
                .steps
                .get(&id)
                .and_then(|steps| steps.iter().find(|s| sf(s, "Id") == Some(step_id.as_str())));
            match step {
                Some(s) => ok(json!({ "Step": s })),
                None => Err(invalid_request(format!("Step id '{step_id}' is not valid."))),
            }
        })
    }

    pub(crate) fn cancel_steps(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let step_ids: Vec<String> = string_list(&body, "StepIds");
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let steps = acct.steps.entry(id.clone()).or_default();
            let mut info = Vec::new();
            for sid in &step_ids {
                let mut status = "FAILED";
                if let Some(step) = steps.iter_mut().find(|s| sf(s, "Id") == Some(sid.as_str())) {
                    let state = step.pointer("/Status/State").and_then(Value::as_str).unwrap_or("");
                    if state == "PENDING" || state == "RUNNING" {
                        if let Some(st) = step.pointer_mut("/Status/State") {
                            *st = json!("CANCELLED");
                        }
                        status = "SUBMITTED";
                    }
                }
                info.push(json!({
                    "StepId": sid,
                    "Status": status,
                    "Reason": "User requested cancellation",
                }));
            }
            ok(json!({ "CancelStepsInfoList": info }))
        })
    }

    // ---- instance groups -------------------------------------------------

    pub(crate) fn add_instance_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "JobFlowId").unwrap_or_default().to_string();
        let created = now_epoch();
        let wrapper = json!({ "InstanceGroups": body.get("InstanceGroups").cloned().unwrap_or(json!([])) });
        let groups = self.build_instance_groups(&wrapper, created);
        let group_ids: Vec<String> = groups.iter().filter_map(|g| sf(g, "Id").map(String::from)).collect();
        let arn = cluster_arn(&req.region, &req.account_id, &id);
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            acct.instance_groups.entry(id.clone()).or_default().extend(groups.clone());
            let new_instances = self.build_instances(&groups, created);
            acct.instances.entry(id.clone()).or_default().extend(new_instances);
            ok(json!({ "JobFlowId": id, "InstanceGroupIds": group_ids, "ClusterArn": arn }))
        })
    }

    pub(crate) fn list_instance_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let groups = acct.instance_groups.get(&id).cloned().unwrap_or_default();
            ok(json!({ "InstanceGroups": groups }))
        })
    }

    pub(crate) fn modify_instance_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let configs = body.get("InstanceGroups").and_then(Value::as_array).cloned().unwrap_or_default();
        self.with_account_mut(req, |acct| {
            for cfg in &configs {
                let gid = sf(cfg, "InstanceGroupId").unwrap_or_default();
                let new_count = cfg.get("InstanceCount").and_then(Value::as_i64);
                for groups in acct.instance_groups.values_mut() {
                    if let Some(g) = groups
                        .iter_mut()
                        .find(|g| sf(g, "Id") == Some(gid))
                        .and_then(Value::as_object_mut)
                    {
                        if let Some(c) = new_count {
                            g.insert("RequestedInstanceCount".into(), json!(c));
                            g.insert("RunningInstanceCount".into(), json!(c));
                        }
                    }
                }
            }
        });
        ok(json!({}))
    }

    pub(crate) fn put_auto_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let group_id = sf(&body, "InstanceGroupId").unwrap_or_default().to_string();
        let policy_in = body.get("AutoScalingPolicy").cloned().unwrap_or(json!({}));
        let arn = cluster_arn(&req.region, &req.account_id, &cluster_id);
        let mut description = serde_json::Map::new();
        description.insert(
            "Status".into(),
            json!({ "State": "ATTACHED", "StateChangeReason": { "Message": "" } }),
        );
        if let Some(c) = policy_in.get("Constraints") {
            description.insert("Constraints".into(), c.clone());
        }
        if let Some(r) = policy_in.get("Rules") {
            description.insert("Rules".into(), r.clone());
        }
        let description = Value::Object(description);
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&cluster_id) {
                return Err(invalid_request(format!("Cluster id '{cluster_id}' is not valid.")));
            }
            let key = format!("{cluster_id}\u{1}{group_id}");
            acct.auto_scaling_policies.insert(key, description.clone());
            for g in acct.instance_groups.entry(cluster_id.clone()).or_default().iter_mut() {
                if sf(g, "Id") == Some(group_id.as_str()) {
                    if let Some(obj) = g.as_object_mut() {
                        obj.insert("AutoScalingPolicy".into(), description.clone());
                    }
                }
            }
            ok(json!({
                "ClusterId": cluster_id,
                "InstanceGroupId": group_id,
                "AutoScalingPolicy": description,
                "ClusterArn": arn,
            }))
        })
    }

    pub(crate) fn remove_auto_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let group_id = sf(&body, "InstanceGroupId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            acct.auto_scaling_policies.remove(&format!("{cluster_id}\u{1}{group_id}"));
        });
        ok(json!({}))
    }

    // ---- instance fleets -------------------------------------------------

    pub(crate) fn add_instance_fleet(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let created = now_epoch();
        let wrapper = json!({ "InstanceFleets": [ body.get("InstanceFleet").cloned().unwrap_or(json!({})) ] });
        let fleets = self.build_instance_fleets(&wrapper, created);
        let fleet_id = fleets.first().and_then(|f| sf(f, "Id")).unwrap_or_default().to_string();
        let arn = cluster_arn(&req.region, &req.account_id, &id);
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            acct.instance_fleets.entry(id.clone()).or_default().extend(fleets);
            ok(json!({ "ClusterId": id, "InstanceFleetId": fleet_id, "ClusterArn": arn }))
        })
    }

    pub(crate) fn list_instance_fleets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let fleets = acct.instance_fleets.get(&id).cloned().unwrap_or_default();
            ok(json!({ "InstanceFleets": fleets }))
        })
    }

    pub(crate) fn modify_instance_fleet(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let cfg = body.get("InstanceFleet").cloned().unwrap_or(json!({}));
        let fleet_id = sf(&cfg, "InstanceFleetId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            if let Some(fleets) = acct.instance_fleets.get_mut(&id) {
                if let Some(f) = fleets
                    .iter_mut()
                    .find(|f| sf(f, "Id") == Some(fleet_id.as_str()))
                    .and_then(Value::as_object_mut)
                {
                    if let Some(v) = cfg.get("TargetOnDemandCapacity").and_then(Value::as_i64) {
                        f.insert("TargetOnDemandCapacity".into(), json!(v));
                        f.insert("ProvisionedOnDemandCapacity".into(), json!(v));
                    }
                    if let Some(v) = cfg.get("TargetSpotCapacity").and_then(Value::as_i64) {
                        f.insert("TargetSpotCapacity".into(), json!(v));
                        f.insert("ProvisionedSpotCapacity".into(), json!(v));
                    }
                }
            }
            ok(json!({}))
        })
    }

    // ---- instances / bootstrap actions -----------------------------------

    pub(crate) fn list_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let group_id = sf(&body, "InstanceGroupId").map(String::from);
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let mut instances = acct.instances.get(&id).cloned().unwrap_or_default();
            if let Some(gid) = &group_id {
                instances.retain(|i| sf(i, "InstanceGroupId") == Some(gid.as_str()));
            }
            ok(json!({ "Instances": instances }))
        })
    }

    pub(crate) fn list_bootstrap_actions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            let actions = acct.bootstrap_actions.get(&id).cloned().unwrap_or_default();
            ok(json!({ "BootstrapActions": actions }))
        })
    }

    // ---- security configurations -----------------------------------------

    pub(crate) fn create_security_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "Name").unwrap_or_default().to_string();
        let config = sf(&body, "SecurityConfiguration").unwrap_or_default().to_string();
        let created = now_epoch();
        self.with_account_mut(req, |acct| {
            if acct.security_configurations.contains_key(&name) {
                return Err(invalid_request(format!(
                    "Security configuration with name '{name}' already exists."
                )));
            }
            acct.security_configurations.insert(
                name.clone(),
                json!({ "Name": name, "SecurityConfiguration": config, "CreationDateTime": created }),
            );
            acct.security_config_order.push(name.clone());
            ok(json!({ "Name": name, "CreationDateTime": created }))
        })
    }

    pub(crate) fn describe_security_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "Name").unwrap_or_default().to_string();
        self.with_account(req, |acct| match acct.security_configurations.get(&name) {
            Some(c) => ok(c.clone()),
            None => Err(invalid_request(format!(
                "Security configuration with name '{name}' does not exist."
            ))),
        })
    }

    pub(crate) fn delete_security_configuration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "Name").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            if acct.security_configurations.remove(&name).is_none() {
                return Err(invalid_request(format!(
                    "Security configuration with name '{name}' does not exist."
                )));
            }
            acct.security_config_order.retain(|n| n != &name);
            ok(json!({}))
        })
    }

    pub(crate) fn list_security_configurations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.with_account(req, |acct| {
            let list: Vec<Value> = acct
                .security_config_order
                .iter()
                .filter_map(|n| acct.security_configurations.get(n))
                .map(|c| {
                    json!({
                        "Name": c.get("Name").cloned().unwrap_or(json!("")),
                        "CreationDateTime": c.get("CreationDateTime").cloned().unwrap_or(json!(0)),
                    })
                })
                .collect();
            ok(json!({ "SecurityConfigurations": list }))
        })
    }

    // ---- studios ---------------------------------------------------------

    pub(crate) fn create_studio(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = format!("es-{}", rand_suffix(13));
        let arn = studio_arn(&req.region, &req.account_id, &id);
        let created = now_epoch();
        let url = format!("https://{id}.emrstudio-prod.{}.amazonaws.com", req.region);
        let mut studio = serde_json::Map::new();
        studio.insert("StudioId".into(), json!(id));
        studio.insert("StudioArn".into(), json!(arn));
        studio.insert("Url".into(), json!(url));
        studio.insert("CreationTime".into(), json!(created));
        for key in [
            "Name",
            "Description",
            "AuthMode",
            "VpcId",
            "ServiceRole",
            "UserRole",
            "WorkspaceSecurityGroupId",
            "EngineSecurityGroupId",
            "DefaultS3Location",
            "IdpAuthUrl",
            "IdpRelayStateParameterName",
            "IdcInstanceArn",
            "IdcUserAssignment",
            "EncryptionKeyArn",
        ] {
            if let Some(v) = sf(&body, key) {
                studio.insert(key.into(), json!(v));
            }
        }
        if let Some(v) = body.get("SubnetIds").filter(|v| v.is_array()) {
            studio.insert("SubnetIds".into(), v.clone());
        }
        if let Some(v) = body.get("Tags").filter(|v| v.is_array()) {
            studio.insert("Tags".into(), v.clone());
        }
        if let Some(v) = body.get("TrustedIdentityPropagationEnabled").and_then(Value::as_bool) {
            studio.insert("TrustedIdentityPropagationEnabled".into(), json!(v));
        }
        let studio = Value::Object(studio);
        self.with_account_mut(req, |acct| {
            acct.studios.insert(id.clone(), studio);
            acct.studio_order.push(id.clone());
        });
        ok(json!({ "StudioId": id, "Url": url }))
    }

    pub(crate) fn describe_studio(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "StudioId").unwrap_or_default().to_string();
        self.with_account(req, |acct| match acct.studios.get(&id) {
            Some(s) => ok(json!({ "Studio": s })),
            None => Err(invalid_request(format!("Studio {id} does not exist."))),
        })
    }

    pub(crate) fn update_studio(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "StudioId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            let Some(s) = acct.studios.get_mut(&id).and_then(Value::as_object_mut) else {
                return Err(invalid_request(format!("Studio {id} does not exist.")));
            };
            for key in ["Name", "Description", "DefaultS3Location", "EncryptionKeyArn"] {
                if let Some(v) = sf(&body, key) {
                    s.insert(key.into(), json!(v));
                }
            }
            if let Some(v) = body.get("SubnetIds").filter(|v| v.is_array()) {
                s.insert("SubnetIds".into(), v.clone());
            }
            ok(json!({}))
        })
    }

    pub(crate) fn delete_studio(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "StudioId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            if acct.studios.remove(&id).is_none() {
                return Err(invalid_request(format!("Studio {id} does not exist.")));
            }
            acct.studio_order.retain(|s| s != &id);
            acct.studio_session_mappings
                .retain(|k, _| !k.starts_with(&format!("{id}\u{1}")));
            ok(json!({}))
        })
    }

    pub(crate) fn list_studios(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.with_account(req, |acct| {
            let list: Vec<Value> = acct
                .studio_order
                .iter()
                .filter_map(|id| acct.studios.get(id))
                .map(studio_summary)
                .collect();
            ok(json!({ "Studios": list }))
        })
    }

    // ---- studio session mappings -----------------------------------------

    pub(crate) fn create_studio_session_mapping(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let studio_id = sf(&body, "StudioId").unwrap_or_default().to_string();
        let identity_type = sf(&body, "IdentityType").unwrap_or_default().to_string();
        let identity_id = sf(&body, "IdentityId").unwrap_or_default().to_string();
        let identity_name = sf(&body, "IdentityName").unwrap_or_default().to_string();
        let policy = sf(&body, "SessionPolicyArn").unwrap_or_default().to_string();
        let key = format!("{studio_id}\u{1}{identity_type}\u{1}{}", identity_key(&identity_id, &identity_name));
        let now = now_epoch();
        self.with_account_mut(req, |acct| {
            acct.studio_session_mappings.insert(
                key,
                json!({
                    "StudioId": studio_id,
                    "IdentityId": if identity_id.is_empty() { format!("id-{}", rand_suffix(12)) } else { identity_id },
                    "IdentityName": identity_name,
                    "IdentityType": identity_type,
                    "SessionPolicyArn": policy,
                    "CreationTime": now,
                    "LastModifiedTime": now,
                }),
            );
        });
        ok(json!({}))
    }

    pub(crate) fn get_studio_session_mapping(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let studio_id = sf(&body, "StudioId").unwrap_or_default();
        let identity_type = sf(&body, "IdentityType").unwrap_or_default();
        let identity_id = sf(&body, "IdentityId").unwrap_or_default();
        let identity_name = sf(&body, "IdentityName").unwrap_or_default();
        let key = format!("{studio_id}\u{1}{identity_type}\u{1}{}", identity_key(identity_id, identity_name));
        self.with_account(req, |acct| match acct.studio_session_mappings.get(&key) {
            Some(m) => ok(json!({ "SessionMapping": m })),
            None => Err(invalid_request("Session mapping does not exist.")),
        })
    }

    pub(crate) fn update_studio_session_mapping(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let studio_id = sf(&body, "StudioId").unwrap_or_default();
        let identity_type = sf(&body, "IdentityType").unwrap_or_default();
        let identity_id = sf(&body, "IdentityId").unwrap_or_default();
        let identity_name = sf(&body, "IdentityName").unwrap_or_default();
        let policy = sf(&body, "SessionPolicyArn").unwrap_or_default().to_string();
        let key = format!("{studio_id}\u{1}{identity_type}\u{1}{}", identity_key(identity_id, identity_name));
        self.with_account_mut(req, |acct| {
            let Some(m) = acct.studio_session_mappings.get_mut(&key).and_then(Value::as_object_mut) else {
                return Err(invalid_request("Session mapping does not exist."));
            };
            m.insert("SessionPolicyArn".into(), json!(policy));
            m.insert("LastModifiedTime".into(), json!(now_epoch()));
            ok(json!({}))
        })
    }

    pub(crate) fn delete_studio_session_mapping(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let studio_id = sf(&body, "StudioId").unwrap_or_default();
        let identity_type = sf(&body, "IdentityType").unwrap_or_default();
        let identity_id = sf(&body, "IdentityId").unwrap_or_default();
        let identity_name = sf(&body, "IdentityName").unwrap_or_default();
        let key = format!("{studio_id}\u{1}{identity_type}\u{1}{}", identity_key(identity_id, identity_name));
        self.with_account_mut(req, |acct| {
            acct.studio_session_mappings.remove(&key);
        });
        ok(json!({}))
    }

    pub(crate) fn list_studio_session_mappings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let studio_id = sf(&body, "StudioId").map(String::from);
        let identity_type = sf(&body, "IdentityType").map(String::from);
        self.with_account(req, |acct| {
            let list: Vec<Value> = acct
                .studio_session_mappings
                .values()
                .filter(|m| {
                    studio_id.as_deref().is_none_or(|s| sf(m, "StudioId") == Some(s))
                        && identity_type.as_deref().is_none_or(|t| sf(m, "IdentityType") == Some(t))
                })
                .map(session_mapping_summary)
                .collect();
            ok(json!({ "SessionMappings": list }))
        })
    }

    // ---- tags ------------------------------------------------------------

    pub(crate) fn add_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_id = sf(&body, "ResourceId").unwrap_or_default().to_string();
        let new_tags = body.get("Tags").and_then(Value::as_array).cloned().unwrap_or_default();
        self.with_account_mut(req, |acct| {
            let tags = acct.tags.entry(resource_id).or_default();
            for t in new_tags {
                let key = sf(&t, "Key").map(String::from);
                tags.retain(|e| sf(e, "Key").map(String::from) != key);
                tags.push(t);
            }
        });
        ok(json!({}))
    }

    pub(crate) fn remove_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_id = sf(&body, "ResourceId").unwrap_or_default().to_string();
        let keys: Vec<String> = string_list(&body, "TagKeys");
        self.with_account_mut(req, |acct| {
            if let Some(tags) = acct.tags.get_mut(&resource_id) {
                tags.retain(|t| sf(t, "Key").is_none_or(|k| !keys.iter().any(|x| x == k)));
            }
        });
        ok(json!({}))
    }

    // ---- block public access ---------------------------------------------

    pub(crate) fn get_block_public_access(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let created = now_epoch();
        let arn = format!("arn:aws:iam::{}:root", req.account_id);
        self.with_account(req, |acct| {
            let config = acct.block_public_access.clone().unwrap_or_else(|| {
                json!({
                    "BlockPublicSecurityGroupRules": true,
                    "PermittedPublicSecurityGroupRuleRanges": [ { "MinRange": 22, "MaxRange": 22 } ],
                })
            });
            let metadata = acct
                .block_public_access_metadata
                .clone()
                .unwrap_or_else(|| json!({ "CreationDateTime": created, "CreatedByArn": arn }));
            ok(json!({
                "BlockPublicAccessConfiguration": config,
                "BlockPublicAccessConfigurationMetadata": metadata,
            }))
        })
    }

    pub(crate) fn put_block_public_access(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let config = body.get("BlockPublicAccessConfiguration").cloned().unwrap_or(json!({}));
        let metadata = json!({
            "CreationDateTime": now_epoch(),
            "CreatedByArn": format!("arn:aws:iam::{}:root", req.account_id),
        });
        self.with_account_mut(req, |acct| {
            acct.block_public_access = Some(config);
            acct.block_public_access_metadata = Some(metadata);
        });
        ok(json!({}))
    }

    // ---- auto-termination policy -----------------------------------------

    pub(crate) fn put_auto_termination_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let policy = body.get("AutoTerminationPolicy").cloned().unwrap_or(json!({}));
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            acct.auto_termination_policies.insert(id, policy);
            ok(json!({}))
        })
    }

    pub(crate) fn get_auto_termination_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            match acct.auto_termination_policies.get(&id) {
                Some(p) => ok(json!({ "AutoTerminationPolicy": p })),
                None => ok(json!({})),
            }
        })
    }

    pub(crate) fn remove_auto_termination_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            acct.auto_termination_policies.remove(&id);
        });
        ok(json!({}))
    }

    // ---- managed-scaling policy ------------------------------------------

    pub(crate) fn put_managed_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let policy = body.get("ManagedScalingPolicy").cloned().unwrap_or(json!({}));
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            acct.managed_scaling_policies.insert(id, policy);
            ok(json!({}))
        })
    }

    pub(crate) fn get_managed_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            match acct.managed_scaling_policies.get(&id) {
                Some(p) => ok(json!({ "ManagedScalingPolicy": p })),
                None => ok(json!({})),
            }
        })
    }

    pub(crate) fn remove_managed_scaling_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            acct.managed_scaling_policies.remove(&id);
        });
        ok(json!({}))
    }

    // ---- notebook executions ---------------------------------------------

    pub(crate) fn start_notebook_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let engine = body.get("ExecutionEngine").cloned().unwrap_or(json!({}));
        let cluster_id = sf(&engine, "Id").unwrap_or_default().to_string();
        let id = format!("ex-{}", rand_suffix(26));
        let arn = format!(
            "arn:aws:elasticmapreduce:{}:{}:notebook-execution/{id}",
            req.region, req.account_id
        );
        let now = now_epoch();
        let mut exec = serde_json::Map::new();
        exec.insert("NotebookExecutionId".into(), json!(id));
        exec.insert("Arn".into(), json!(arn));
        exec.insert("ExecutionEngine".into(), engine.clone());
        exec.insert(
            "Status".into(),
            json!("RUNNING"),
        );
        exec.insert("StartTime".into(), json!(now));
        for key in ["EditorId", "NotebookExecutionName", "NotebookParams"] {
            if let Some(v) = sf(&body, key) {
                exec.insert(key.into(), json!(v));
            }
        }
        if let Some(v) = body.get("Tags").filter(|v| v.is_array()) {
            exec.insert("Tags".into(), v.clone());
        }
        let exec = Value::Object(exec);
        self.with_account_mut(req, |acct| {
            if !cluster_id.is_empty() && !acct.clusters.contains_key(&cluster_id) {
                return Err(invalid_request(format!(
                    "Cluster id '{cluster_id}' is not valid."
                )));
            }
            acct.notebook_executions.insert(id.clone(), exec);
            acct.notebook_order.push(id.clone());
            ok(json!({ "NotebookExecutionId": id }))
        })
    }

    pub(crate) fn describe_notebook_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "NotebookExecutionId").unwrap_or_default().to_string();
        self.with_account(req, |acct| match acct.notebook_executions.get(&id) {
            Some(e) => ok(json!({ "NotebookExecution": e })),
            None => Err(invalid_request(format!(
                "Notebook execution '{id}' does not exist."
            ))),
        })
    }

    pub(crate) fn list_notebook_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let status = sf(&body, "Status").map(String::from);
        self.with_account(req, |acct| {
            let list: Vec<Value> = acct
                .notebook_order
                .iter()
                .filter_map(|id| acct.notebook_executions.get(id))
                .filter(|e| status.as_deref().is_none_or(|s| sf(e, "Status") == Some(s)))
                .map(notebook_summary)
                .collect();
            ok(json!({ "NotebookExecutions": list }))
        })
    }

    pub(crate) fn stop_notebook_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "NotebookExecutionId").unwrap_or_default().to_string();
        self.with_account_mut(req, |acct| {
            let Some(e) = acct.notebook_executions.get_mut(&id).and_then(Value::as_object_mut) else {
                return Err(invalid_request(format!(
                    "Notebook execution '{id}' does not exist."
                )));
            };
            e.insert("Status".into(), json!("STOPPED"));
            e.insert("EndTime".into(), json!(now_epoch()));
            ok(json!({}))
        })
    }

    // ---- persistent app UIs ----------------------------------------------

    pub(crate) fn create_persistent_app_ui(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = format!("p-{}", rand_suffix(26));
        let now = now_epoch();
        let mut ui = serde_json::Map::new();
        ui.insert("PersistentAppUIId".into(), json!(id));
        ui.insert("PersistentAppUIStatus".into(), json!("ATTACHED"));
        ui.insert("CreationTime".into(), json!(now));
        ui.insert("LastModifiedTime".into(), json!(now));
        if let Some(v) = body.get("Tags").filter(|v| v.is_array()) {
            ui.insert("Tags".into(), v.clone());
        }
        let ui = Value::Object(ui);
        self.with_account_mut(req, |acct| {
            acct.persistent_app_uis.insert(id.clone(), ui);
        });
        ok(json!({ "PersistentAppUIId": id, "RuntimeRoleEnabledCluster": false }))
    }

    pub(crate) fn describe_persistent_app_ui(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "PersistentAppUIId").unwrap_or_default().to_string();
        self.with_account(req, |acct| match acct.persistent_app_uis.get(&id) {
            Some(u) => ok(json!({ "PersistentAppUI": u })),
            None => Err(invalid_request(format!(
                "Persistent app UI '{id}' does not exist."
            ))),
        })
    }

    pub(crate) fn get_persistent_app_ui_presigned_url(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "PersistentAppUIId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.persistent_app_uis.contains_key(&id) {
                return Err(invalid_request(format!(
                    "Persistent app UI '{id}' does not exist."
                )));
            }
            ok(json!({
                "PresignedURLReady": true,
                "PresignedURL": format!("https://{}.emrappui-prod.{}.amazonaws.com/{}", id, req.region, "shs"),
            }))
        })
    }

    pub(crate) fn get_on_cluster_app_ui_presigned_url(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&id) {
                return Err(invalid_request(format!("Cluster id '{id}' is not valid.")));
            }
            ok(json!({
                "PresignedURLReady": true,
                "PresignedURL": format!("https://{}.emrappui-prod.{}.amazonaws.com/yarn", id, req.region),
            }))
        })
    }

    // ---- interactive sessions --------------------------------------------

    pub(crate) fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let session_id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:elasticmapreduce:{}:{}:cluster/{}/session/{}",
            req.region, req.account_id, cluster_id, session_id
        );
        let now = now_epoch();
        let mut session = serde_json::Map::new();
        session.insert("Id".into(), json!(session_id));
        session.insert("ClusterId".into(), json!(cluster_id));
        session.insert("Arn".into(), json!(arn));
        session.insert("State".into(), json!("STARTING"));
        session.insert("AccountId".into(), json!(req.account_id));
        session.insert("CreatedAt".into(), json!(now));
        session.insert("UpdatedAt".into(), json!(now));
        if let Some(v) = sf(&body, "ExecutionRoleArn") {
            session.insert("ExecutionRoleArn".into(), json!(v));
        }
        if let Some(v) = sf(&body, "ReleaseLabel") {
            session.insert("ReleaseLabel".into(), json!(v));
        }
        if let Some(v) = body.get("Tags").filter(|v| v.is_array()) {
            session.insert("Tags".into(), v.clone());
        }
        let session = Value::Object(session);
        self.with_account_mut(req, |acct| {
            if !acct.clusters.contains_key(&cluster_id) {
                return Err(invalid_request(format!("Cluster id '{cluster_id}' is not valid.")));
            }
            acct.sessions.insert(format!("{cluster_id}\u{1}{session_id}"), session);
            ok(json!({
                "Id": session_id,
                "ClusterId": cluster_id,
                "Arn": arn,
                "AccountId": req.account_id,
                "State": "STARTING",
            }))
        })
    }

    pub(crate) fn get_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default();
        let session_id = sf(&body, "SessionId").unwrap_or_default();
        let key = format!("{cluster_id}\u{1}{session_id}");
        self.with_account(req, |acct| match acct.sessions.get(&key) {
            Some(s) => ok(json!({ "Session": s })),
            None => Err(invalid_request(format!("Session '{session_id}' is not valid."))),
        })
    }

    pub(crate) fn get_session_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default();
        let session_id = sf(&body, "SessionId").unwrap_or_default();
        let key = format!("{cluster_id}\u{1}{session_id}");
        self.with_account(req, |acct| {
            if !acct.sessions.contains_key(&key) {
                return Err(invalid_request(format!("Session '{session_id}' is not valid.")));
            }
            ok(json!({
                "Endpoint": format!("https://{}.session.{}.amazonaws.com", session_id, req.region),
            }))
        })
    }

    pub(crate) fn terminate_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let session_id = sf(&body, "SessionId").unwrap_or_default().to_string();
        let key = format!("{cluster_id}\u{1}{session_id}");
        self.with_account_mut(req, |acct| {
            let Some(s) = acct.sessions.get_mut(&key).and_then(Value::as_object_mut) else {
                return Err(invalid_request(format!("Session '{session_id}' is not valid.")));
            };
            s.insert("State".into(), json!("TERMINATING"));
            s.insert("UpdatedAt".into(), json!(now_epoch()));
            ok(json!({
                "ClusterId": cluster_id,
                "SessionId": session_id,
                "State": "TERMINATING",
            }))
        })
    }

    pub(crate) fn list_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        let states: Vec<String> = string_list(&body, "SessionStates");
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&cluster_id) {
                return Err(invalid_request(format!("Cluster id '{cluster_id}' is not valid.")));
            }
            let prefix = format!("{cluster_id}\u{1}");
            let list: Vec<Value> = acct
                .sessions
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.clone())
                .filter(|s| {
                    states.is_empty()
                        || sf(s, "State").is_some_and(|st| states.iter().any(|x| x == st))
                })
                .collect();
            ok(json!({ "Sessions": list }))
        })
    }

    pub(crate) fn get_cluster_session_credentials(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cluster_id = sf(&body, "ClusterId").unwrap_or_default().to_string();
        self.with_account(req, |acct| {
            if !acct.clusters.contains_key(&cluster_id) {
                return Err(invalid_request(format!("Cluster id '{cluster_id}' is not valid.")));
            }
            // Session credentials require a runtime-role-enabled cluster with an
            // active execution role; without one EMR returns InvalidRequestException.
            Err(invalid_request(
                "The cluster is not enabled for runtime roles; session credentials are unavailable.",
            ))
        })
    }

    // ---- release labels / instance types ---------------------------------

    pub(crate) fn list_release_labels(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "ReleaseLabels": release_labels() }))
    }

    pub(crate) fn describe_release_label(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let label = sf(&body, "ReleaseLabel").unwrap_or("emr-7.1.0").to_string();
        ok(json!({
            "ReleaseLabel": label,
            "Applications": [
                { "Name": "Hadoop", "Version": "3.3.6" },
                { "Name": "Spark", "Version": "3.5.0" },
                { "Name": "Hive", "Version": "3.1.3" },
            ],
            "AvailableOSReleases": [ { "Label": "2.0.20240202.0" } ],
        }))
    }

    pub(crate) fn list_supported_instance_types(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _ = req;
        ok(json!({
            "SupportedInstanceTypes": [
                { "Type": "m5.xlarge", "MemoryGB": 16.0, "StorageGB": 0, "VCPU": 4, "Is64BitsOnly": true, "InstanceFamilyId": "m5", "EbsOptimizedAvailable": true, "EbsOptimizedByDefault": true, "NumberOfDisks": 0, "EbsStorageOnly": true, "Architecture": "X86_64" },
                { "Type": "m5.2xlarge", "MemoryGB": 32.0, "StorageGB": 0, "VCPU": 8, "Is64BitsOnly": true, "InstanceFamilyId": "m5", "EbsOptimizedAvailable": true, "EbsOptimizedByDefault": true, "NumberOfDisks": 0, "EbsStorageOnly": true, "Architecture": "X86_64" },
                { "Type": "c5.xlarge", "MemoryGB": 8.0, "StorageGB": 0, "VCPU": 4, "Is64BitsOnly": true, "InstanceFamilyId": "c5", "EbsOptimizedAvailable": true, "EbsOptimizedByDefault": true, "NumberOfDisks": 0, "EbsStorageOnly": true, "Architecture": "X86_64" },
            ],
        }))
    }
}

// ---- free functions: response-shape builders -----------------------------

fn string_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

fn identity_key(identity_id: &str, identity_name: &str) -> String {
    if !identity_id.is_empty() {
        identity_id.to_string()
    } else {
        identity_name.to_string()
    }
}

fn build_step(cfg: &Value, created: f64) -> Value {
    let hadoop = cfg.get("HadoopJarStep").cloned().unwrap_or(json!({}));
    let mut config = serde_json::Map::new();
    if let Some(jar) = sf(&hadoop, "Jar") {
        config.insert("Jar".into(), json!(jar));
    }
    if let Some(main) = sf(&hadoop, "MainClass") {
        config.insert("MainClass".into(), json!(main));
    }
    if let Some(args) = hadoop.get("Args").filter(|v| v.is_array()) {
        config.insert("Args".into(), args.clone());
    }
    let props = key_values_to_map(hadoop.get("Properties"));
    if props.as_object().is_some_and(|m| !m.is_empty()) {
        config.insert("Properties".into(), props);
    }
    json!({
        "Id": format!("s-{}", rand_suffix(13)),
        "Name": sf(cfg, "Name").unwrap_or("Step"),
        "Config": Value::Object(config),
        "ActionOnFailure": sf(cfg, "ActionOnFailure").unwrap_or("CONTINUE"),
        "Status": {
            "State": "COMPLETED",
            "StateChangeReason": { "Message": "" },
            "Timeline": { "CreationDateTime": created, "StartDateTime": created, "EndDateTime": created },
        },
    })
}

fn build_bootstrap_actions(actions: Option<&Value>) -> Vec<Value> {
    let Some(arr) = actions.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .map(|a| {
            let script = a.get("ScriptBootstrapAction").cloned().unwrap_or(json!({}));
            json!({
                "Name": sf(a, "Name").unwrap_or("BootstrapAction"),
                "ScriptPath": sf(&script, "Path").unwrap_or(""),
                "Args": script.get("Args").cloned().unwrap_or(json!([])),
            })
        })
        .collect()
}

fn build_job_flow_detail(
    id: &str,
    name: &str,
    body: &Value,
    instances: &Value,
    created: f64,
) -> Value {
    let master = instances
        .get("MasterInstanceType")
        .and_then(Value::as_str)
        .or_else(|| {
            instances
                .get("InstanceGroups")
                .and_then(Value::as_array)
                .and_then(|g| g.iter().find(|c| sf(c, "InstanceRole") == Some("MASTER")))
                .and_then(|c| sf(c, "InstanceType"))
        })
        .unwrap_or("m5.xlarge")
        .to_string();
    let slave = instances
        .get("SlaveInstanceType")
        .and_then(Value::as_str)
        .unwrap_or("m5.xlarge")
        .to_string();
    let count = instances
        .get("InstanceCount")
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let mut detail = serde_json::Map::new();
    detail.insert("JobFlowId".into(), json!(id));
    detail.insert("Name".into(), json!(name));
    detail.insert(
        "ExecutionStatusDetail".into(),
        json!({
            "State": "WAITING",
            "CreationDateTime": created,
            "ReadyDateTime": created,
        }),
    );
    detail.insert(
        "Instances".into(),
        json!({
            "MasterInstanceType": master,
            "SlaveInstanceType": slave,
            "InstanceCount": count,
            "KeepJobFlowAliveWhenNoSteps": instances
                .get("KeepJobFlowAliveWhenNoSteps")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            "TerminationProtected": instances
                .get("TerminationProtected")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
    );
    detail.insert(
        "VisibleToAllUsers".into(),
        json!(body.get("VisibleToAllUsers").and_then(Value::as_bool).unwrap_or(true)),
    );
    for key in ["LogUri", "ServiceRole", "AutoScalingRole", "ScaleDownBehavior"] {
        if let Some(v) = sf(body, key) {
            detail.insert(key.into(), json!(v));
        }
    }
    Value::Object(detail)
}

fn cluster_summary(c: &Value) -> Value {
    let mut s = serde_json::Map::new();
    for key in ["Id", "Name", "Status", "NormalizedInstanceHours", "ClusterArn", "OutpostArn"] {
        if let Some(v) = c.get(key) {
            s.insert(key.into(), v.clone());
        }
    }
    Value::Object(s)
}

fn step_summary(s: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in ["Id", "Name", "Config", "ActionOnFailure", "Status", "LogUri", "EncryptionKeyArn"] {
        if let Some(v) = s.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    Value::Object(out)
}

fn studio_summary(s: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in ["StudioId", "Name", "VpcId", "Description", "Url", "AuthMode", "CreationTime"] {
        if let Some(v) = s.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    Value::Object(out)
}

fn session_mapping_summary(m: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in ["StudioId", "IdentityId", "IdentityName", "IdentityType", "SessionPolicyArn", "CreationTime"] {
        if let Some(v) = m.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    Value::Object(out)
}

fn notebook_summary(e: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in ["NotebookExecutionId", "EditorId", "NotebookExecutionName", "Status", "StartTime", "EndTime"] {
        if let Some(v) = e.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    Value::Object(out)
}

fn release_labels() -> Vec<String> {
    vec![
        "emr-7.1.0".into(),
        "emr-7.0.0".into(),
        "emr-6.15.0".into(),
        "emr-6.14.0".into(),
        "emr-5.36.1".into(),
    ]
}
