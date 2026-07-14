// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EcsService {
    pub(super) fn create_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_name = opt_str(&body, "clusterName")
            .unwrap_or("default")
            .to_string();
        let tags = parse_tags(&body);
        let settings: Vec<Value> = body
            .get("settings")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let configuration = body.get("configuration").cloned();
        let capacity_providers: Vec<String> = body
            .get("capacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let default_strategy: Vec<Value> = body
            .get("defaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let service_connect = body.get("serviceConnectDefaults").cloned();

        let account = request.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let arn = state.cluster_arn(&cluster_name);
        let mut cluster = Cluster::new(&cluster_name, arn);
        cluster.tags = tags;
        cluster.settings = settings;
        cluster.configuration = configuration;
        cluster.capacity_providers = capacity_providers;
        cluster.default_capacity_provider_strategy = default_strategy;
        cluster.service_connect_defaults = service_connect;
        // CreateCluster on an existing cluster is idempotent-ish — AWS
        // returns the existing cluster, potentially with merged settings.
        // We keep it simple and overwrite on recreate.
        state.clusters.insert(cluster_name.clone(), cluster.clone());

        Ok(AwsResponse::ok_json(json!({
            "cluster": cluster_to_json(&cluster),
        })))
    }

    pub(super) fn describe_clusters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let names: Vec<String> = body
            .get("clusters")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| EcsState::resolve_cluster_name(Some(s))))
                    .collect()
            })
            .unwrap_or_else(|| vec!["default".to_string()]);

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        if let Some(state) = accounts.get(&account) {
            for name in &names {
                match state.clusters.get(name) {
                    Some(c) => found.push(cluster_to_json(c)),
                    None => failures.push(json!({
                        "arn": state.cluster_arn(name),
                        "reason": "MISSING",
                    })),
                }
            }
        } else {
            for name in &names {
                failures.push(json!({
                    "arn": format!(
                        "arn:aws:ecs:{}:{}:cluster/{}",
                        accounts.region(),
                        account,
                        name
                    ),
                    "reason": "MISSING",
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "clusters": found,
            "failures": failures,
        })))
    }

    pub(super) fn delete_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = Some(req_str(&body, "cluster")?);
        let name = EcsState::resolve_cluster_name(cluster_ref);
        let account = target_account_for_cluster(request, cluster_ref);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let active_services = state
            .clusters
            .get(&name)
            .ok_or_else(|| cluster_not_found(&name))?
            .active_services_count;
        if active_services > 0 {
            return Err(cluster_contains_services());
        }
        // Scan live task records rather than the cluster's cached running/
        // pending counters, which can drift under rapid task churn (a
        // container that exits immediately may skip a clean RUNNING->STOPPED
        // counter decrement). STOPPED tasks are history and don't block delete.
        if cluster_active_task_count(state, &name) > 0 {
            return Err(cluster_contains_tasks());
        }
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(|| cluster_not_found(&name))?;
        cluster.status = "INACTIVE".to_string();
        let snapshot = cluster.clone();
        // Real ECS keeps the cluster visible as INACTIVE for about an
        // hour before garbage-collecting it. We drop it immediately to
        // keep state bounded — callers that try to describe it by name
        // will get a MISSING failure, matching the long-tail behaviour.
        state.clusters.remove(&name);
        Ok(AwsResponse::ok_json(json!({
            "cluster": cluster_to_json(&snapshot),
        })))
    }

    pub(super) fn list_clusters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .filter(|n| (1..=100).contains(n))
            .map(|n| n as usize)
            .unwrap_or(100);
        let next_token = opt_str(&body, "nextToken").unwrap_or("");

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let arns: Vec<String> = match accounts.get(&account) {
            Some(state) => state
                .clusters
                .values()
                .map(|c| c.cluster_arn.clone())
                .collect(),
            None => Vec::new(),
        };
        let (page, next) = paginate_arns(arns, next_token, max_results);
        let mut out = json!({ "clusterArns": page });
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(n));
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn update_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = req_str(&body, "cluster")?;
        let name = EcsState::resolve_cluster_name(Some(cluster_ref));
        let account = target_account_for_cluster(request, Some(cluster_ref));

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(|| cluster_not_found(&name))?;
        if let Some(settings) = body.get("settings").and_then(|v| v.as_array()) {
            cluster.settings = settings.clone();
        }
        if let Some(cfg) = body.get("configuration") {
            cluster.configuration = Some(cfg.clone());
        }
        if let Some(sc) = body.get("serviceConnectDefaults") {
            cluster.service_connect_defaults = Some(sc.clone());
        }
        let snapshot = cluster.clone();
        Ok(AwsResponse::ok_json(json!({
            "cluster": cluster_to_json(&snapshot),
        })))
    }

    pub(super) fn update_cluster_settings(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = req_str(&body, "cluster")?;
        let name = EcsState::resolve_cluster_name(Some(cluster_ref));
        let account = target_account_for_cluster(request, Some(cluster_ref));
        let settings: Vec<Value> = body
            .get("settings")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(|| cluster_not_found(&name))?;
        cluster.settings = settings;
        let snapshot = cluster.clone();
        Ok(AwsResponse::ok_json(json!({
            "cluster": cluster_to_json(&snapshot),
        })))
    }

    pub(super) fn put_cluster_capacity_providers(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = req_str(&body, "cluster")?;
        let name = EcsState::resolve_cluster_name(Some(cluster_ref));
        let account = target_account_for_cluster(request, Some(cluster_ref));
        let capacity_providers: Vec<String> = body
            .get("capacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .ok_or_else(|| client_exception("Missing required field: capacityProviders"))?;
        let default_strategy: Vec<Value> = body
            .get("defaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| {
                client_exception("Missing required field: defaultCapacityProviderStrategy")
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(|| cluster_not_found(&name))?;
        cluster.capacity_providers = capacity_providers;
        cluster.default_capacity_provider_strategy = default_strategy;
        let snapshot = cluster.clone();
        Ok(AwsResponse::ok_json(json!({
            "cluster": cluster_to_json(&snapshot),
        })))
    }
}
