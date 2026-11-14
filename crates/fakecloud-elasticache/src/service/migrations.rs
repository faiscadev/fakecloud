//! ElastiCache online-migration handlers: replication-group migrations
//! (StartMigration / TestMigration / CompleteMigration by
//! `ReplicationGroupId`) and serverless-cache migrations
//! (PrepareMigration -> StartMigration -> StopMigration / CompleteMigration
//! by `ServerlessCacheName`). The serverless lifecycle is documented on
//! [`crate::state::migration_status`].

use super::*;
use crate::state::{migration_status, ServerlessCacheMigration};

/// The resource a StartMigration / CompleteMigration addresses: exactly one
/// of `ReplicationGroupId` or `ServerlessCacheName`.
enum MigrationTarget {
    ReplicationGroup(String),
    ServerlessCache(String),
}

fn migration_target(request: &AwsRequest) -> Result<MigrationTarget, AwsServiceError> {
    match (
        optional_query_param(request, "ReplicationGroupId"),
        optional_query_param(request, "ServerlessCacheName"),
    ) {
        (Some(id), None) => Ok(MigrationTarget::ReplicationGroup(id)),
        (None, Some(name)) => Ok(MigrationTarget::ServerlessCache(name)),
        (Some(_), Some(_)) => Err(invalid_combination(
            "Specify either ReplicationGroupId or ServerlessCacheName, not both.",
        )),
        (None, None) => Err(invalid_combination(
            "Either ReplicationGroupId or ServerlessCacheName must be specified.",
        )),
    }
}

fn invalid_combination(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterCombination",
        message.to_string(),
    )
}

fn invalid_value(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterValue", message)
}

fn serverless_cache_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ServerlessCacheNotFoundFault",
        format!("ServerlessCache {name} not found."),
    )
}

fn serverless_cache_not_under_migration(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ServerlessCacheNotUnderMigrationFault",
        format!("ServerlessCache {name} is not currently being migrated."),
    )
}

fn invalid_serverless_cache_state(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidServerlessCacheStateFault",
        message,
    )
}

fn replication_group_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ReplicationGroupNotFoundFault",
        format!("ReplicationGroup {id} not found."),
    )
}

/// `CustomerNodeEndpointList.member.N.{Address,Port}` as (address, port)
/// pairs, in index order. A port that is not a valid TCP port is rejected.
fn customer_node_endpoints(
    request: &AwsRequest,
) -> Result<Vec<(String, Option<i32>)>, AwsServiceError> {
    let mut out = Vec::new();
    for i in 1..=64 {
        let address = optional_query_param(
            request,
            &format!("CustomerNodeEndpointList.member.{i}.Address"),
        );
        let port = optional_query_param(
            request,
            &format!("CustomerNodeEndpointList.member.{i}.Port"),
        );
        if address.is_none() && port.is_none() {
            break;
        }
        let port = port
            .map(|p| match p.parse::<i32>() {
                Ok(v) if (1..=65535).contains(&v) => Ok(v),
                _ => Err(invalid_value(format!(
                    "Invalid port '{p}' in CustomerNodeEndpointList."
                ))),
            })
            .transpose()?;
        out.push((address.unwrap_or_default(), port));
    }
    Ok(out)
}

fn response(action: &str, inner: String, request: &AwsRequest) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(action, ELASTICACHE_NS, &inner, &request.request_id),
    )
}

impl ElastiCacheService {
    /// PrepareMigration: validate the source configuration and open a
    /// migration on an `available` serverless cache.
    pub(super) fn prepare_migration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "ServerlessCacheName")?;
        let endpoints = customer_node_endpoints(request)?;
        let (address, port) = match endpoints.as_slice() {
            [(address, port)] if !address.is_empty() => (address.clone(), port.unwrap_or(6379)),
            [_] => {
                return Err(invalid_value(
                    "CustomerNodeEndpointList.member.1.Address is required.".to_string(),
                ))
            }
            [] => {
                return Err(invalid_value(
                    "CustomerNodeEndpointList must contain exactly one endpoint.".to_string(),
                ))
            }
            _ => {
                return Err(invalid_value(format!(
                    "CustomerNodeEndpointList must contain exactly one endpoint for a serverless cache migration, got {}.",
                    endpoints.len()
                )))
            }
        };
        let secret_arn = optional_query_param(
            request,
            "SourceAuthenticationConfiguration.SecretManagerConfig.SecretArn",
        );
        if let Some(ref arn) = secret_arn {
            if !(arn.starts_with("arn:")
                && arn.contains(":secretsmanager:")
                && arn.contains(":secret:"))
            {
                return Err(invalid_value(format!(
                    "SecretArn '{arn}' is not a valid Secrets Manager secret ARN."
                )));
            }
        }
        let tls_mode = optional_query_param(request, "SourceTlsConfiguration.SourceTlsMode");
        let tls_certificate =
            optional_query_param(request, "SourceTlsConfiguration.SourceTlsCertificate");
        if let Some(ref mode) = tls_mode {
            if mode != "none" && mode != "verify-ca" {
                return Err(invalid_value(format!(
                    "Invalid value for SourceTlsMode: '{mode}'. Valid values: none, verify-ca."
                )));
            }
            if mode == "verify-ca" && tls_certificate.is_none() {
                return Err(invalid_value(
                    "SourceTlsCertificate is required when SourceTlsMode is verify-ca.".to_string(),
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let cache = state
            .serverless_caches
            .get_mut(&name)
            .ok_or_else(|| serverless_cache_not_found(&name))?;
        if cache.migration.as_ref().is_some_and(|m| m.is_active()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ServerlessCacheAlreadyUnderMigrationFault",
                format!("ServerlessCache {name} is already under migration."),
            ));
        }
        if cache.status != "available" {
            return Err(invalid_serverless_cache_state(format!(
                "ServerlessCache {name} is not in available state (current state: {}).",
                cache.status
            )));
        }
        cache.migration = Some(ServerlessCacheMigration {
            status: migration_status::PREPARED.to_string(),
            cutover_mode: None,
            source_address: address,
            source_port: port,
            source_secret_arn: secret_arn,
            source_tls_mode: tls_mode,
            source_tls_certificate: tls_certificate,
            prepared_at: chrono::Utc::now().to_rfc3339(),
        });
        Ok(response(
            "PrepareMigration",
            format!(
                "<ServerlessCache>{}</ServerlessCache>",
                serverless_cache_xml(cache)
            ),
            request,
        ))
    }

    pub(super) async fn start_migration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let auto_cutover =
            parse_optional_bool(optional_query_param(request, "AutoCutover").as_deref())?
                .unwrap_or(false);
        let clear_target =
            parse_optional_bool(optional_query_param(request, "ClearTargetCacheData").as_deref())?
                .unwrap_or(false);
        match migration_target(request)? {
            MigrationTarget::ReplicationGroup(id) => {
                if clear_target {
                    return Err(invalid_combination(
                        "ClearTargetCacheData applies only to serverless cache migrations.",
                    ));
                }
                let status = if auto_cutover { "complete" } else { "queued" };
                self.replication_group_migration(request, "StartMigration", &id, status, true)
            }
            MigrationTarget::ServerlessCache(name) => {
                self.start_serverless_migration(request, &name, auto_cutover, clear_target)
                    .await
            }
        }
    }

    async fn start_serverless_migration(
        &self,
        request: &AwsRequest,
        name: &str,
        auto_cutover: bool,
        clear_target: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !customer_node_endpoints(request)?.is_empty() {
            return Err(invalid_combination(
                "CustomerNodeEndpointList is not accepted for a serverless cache migration; \
                 the endpoint given to PrepareMigration is used.",
            ));
        }
        let (xml, flush) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let cache = state
                .serverless_caches
                .get_mut(name)
                .ok_or_else(|| serverless_cache_not_found(name))?;
            if cache.status != "available" {
                return Err(invalid_serverless_cache_state(format!(
                    "ServerlessCache {name} is not in available state (current state: {}).",
                    cache.status
                )));
            }
            let engine = cache.engine.clone();
            let incarnation = cache.incarnation();
            let migration = cache
                .migration
                .as_mut()
                .filter(|m| m.is_active())
                .ok_or_else(|| serverless_cache_not_under_migration(name))?;
            match migration.status.as_str() {
                migration_status::PREPARED => {}
                migration_status::STOPPED if !clear_target => {
                    return Err(invalid_combination(
                        "ClearTargetCacheData must be true to restart a stopped migration.",
                    ));
                }
                migration_status::STOPPED => {}
                other => {
                    return Err(invalid_serverless_cache_state(format!(
                        "Migration for ServerlessCache {name} is {other}; it cannot be started."
                    )));
                }
            }
            // Data sync is established as soon as it starts here, so an
            // automatic cutover completes the migration immediately.
            migration.status = if auto_cutover {
                migration_status::COMPLETED
            } else {
                migration_status::SYNCING
            }
            .to_string();
            migration.cutover_mode = Some(if auto_cutover { "auto" } else { "manual" }.to_string());
            let flush = clear_target && (engine == ENGINE_REDIS || engine == ENGINE_VALKEY);
            (serverless_cache_xml(cache), flush.then_some(incarnation))
        };
        if let (Some(incarnation), Some(runtime)) = (flush, self.runtime.as_ref()) {
            // ClearTargetCacheData: empty the target before the sync begins.
            match runtime
                .exec_redis(&incarnation, &["FLUSHALL".to_string()])
                .await
            {
                Ok(output) if !output.success => tracing::warn!(
                    serverless_cache = %name,
                    stderr = %String::from_utf8_lossy(&output.stderr),
                    "FLUSHALL for ClearTargetCacheData failed"
                ),
                Err(e) => tracing::warn!(
                    serverless_cache = %name,
                    %e,
                    "FLUSHALL for ClearTargetCacheData failed"
                ),
                _ => {}
            }
        }
        Ok(response(
            "StartMigration",
            format!("<ServerlessCache>{xml}</ServerlessCache>"),
            request,
        ))
    }

    /// StopMigration: pause a syncing serverless-cache migration. The
    /// migration stays open; StartMigration with `ClearTargetCacheData`
    /// restarts it.
    pub(super) fn stop_migration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(request, "ServerlessCacheName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let cache = state
            .serverless_caches
            .get_mut(&name)
            .ok_or_else(|| serverless_cache_not_found(&name))?;
        let migration = cache
            .migration
            .as_mut()
            .filter(|m| m.is_active())
            .ok_or_else(|| serverless_cache_not_under_migration(&name))?;
        if migration.status != migration_status::SYNCING {
            return Err(invalid_serverless_cache_state(format!(
                "Migration for ServerlessCache {name} is {}; only a syncing migration can be stopped.",
                migration.status
            )));
        }
        migration.status = migration_status::STOPPED.to_string();
        Ok(response(
            "StopMigration",
            format!(
                "<ServerlessCache>{}</ServerlessCache>",
                serverless_cache_xml(cache)
            ),
            request,
        ))
    }

    pub(super) fn complete_migration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let force = parse_optional_bool(optional_query_param(request, "Force").as_deref())?
            .unwrap_or(false);
        match migration_target(request)? {
            MigrationTarget::ReplicationGroup(id) => {
                self.complete_replication_group_migration(request, &id)
            }
            MigrationTarget::ServerlessCache(name) => {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&request.account_id);
                let cache = state
                    .serverless_caches
                    .get_mut(&name)
                    .ok_or_else(|| serverless_cache_not_found(&name))?;
                let migration = cache
                    .migration
                    .as_mut()
                    .filter(|m| m.is_active())
                    .ok_or_else(|| serverless_cache_not_under_migration(&name))?;
                migration.status = if migration.status == migration_status::SYNCING {
                    migration_status::COMPLETED
                } else if force {
                    // Forced without an established sync: the migration is
                    // abandoned rather than cut over.
                    migration_status::CANCELLED
                } else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MigrationHealthFault",
                        format!(
                            "Data sync for ServerlessCache {name} is not established (migration is {}); \
                             start the migration or use Force to abort it.",
                            migration.status
                        ),
                    ));
                }
                .to_string();
                Ok(response(
                    "CompleteMigration",
                    format!(
                        "<ServerlessCache>{}</ServerlessCache>",
                        serverless_cache_xml(cache)
                    ),
                    request,
                ))
            }
        }
    }

    fn complete_replication_group_migration(
        &self,
        request: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        // Validate all prerequisites BEFORE mutating migration.status, so a
        // missing group never leaves a stale "complete" status behind.
        if !state.migrations.contains_key(id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationGroupNotUnderMigrationFault",
                format!("ReplicationGroup {id} is not currently being migrated."),
            ));
        }
        let group = state
            .replication_groups
            .get(id)
            .ok_or_else(|| replication_group_not_found(id))?;
        let xml = replication_group_xml(group, &request.region);
        state.migrations.get_mut(id).expect("checked above").status = "complete".to_string();
        Ok(response(
            "CompleteMigration",
            format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
            request,
        ))
    }

    pub(super) fn test_migration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "ReplicationGroupId")?;
        self.replication_group_migration(request, "TestMigration", &id, "test-passed", false)
    }

    /// Record a replication-group migration (or connection test) against
    /// the first `CustomerNodeEndpointList` entry. `exclusive` refuses a
    /// group that already has a queued migration.
    fn replication_group_migration(
        &self,
        request: &AwsRequest,
        action: &str,
        id: &str,
        status: &str,
        exclusive: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (endpoint_addr, endpoint_port) = customer_node_endpoints(request)?
            .into_iter()
            .next()
            .map(|(a, p)| (a, p.unwrap_or(6379)))
            .filter(|(a, _)| !a.is_empty())
            .unwrap_or_else(|| ("127.0.0.1".to_string(), 6379));

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let group = state
            .replication_groups
            .get(id)
            .ok_or_else(|| replication_group_not_found(id))?;
        if exclusive
            && state
                .migrations
                .get(id)
                .is_some_and(|m| m.status == "queued")
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ReplicationGroupAlreadyUnderMigrationFault",
                format!("ReplicationGroup {id} is already under migration."),
            ));
        }
        let xml = replication_group_xml(group, &request.region);
        state.migrations.insert(
            id.to_string(),
            crate::state::Migration {
                replication_group_id: id.to_string(),
                customer_node_endpoint_address: endpoint_addr,
                customer_node_endpoint_port: endpoint_port,
                status: status.to_string(),
                started_at: chrono::Utc::now().to_rfc3339(),
            },
        );
        Ok(response(
            action,
            format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
            request,
        ))
    }
}
