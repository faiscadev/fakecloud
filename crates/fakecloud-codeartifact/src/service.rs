//! AWS CodeArtifact restJson1 dispatch + operation handlers.
//!
//! Requests are routed to an operation by HTTP method + `@http` URI path
//! (there are no path labels -- every input parameter is a query-string value,
//! a JSON body field, an `@httpPayload` blob, or a header). Query parameters
//! are read from the raw query string so repeated multi-value keys (for example
//! `?versions=a&versions=b` do not appear here, but list filters that could)
//! survive intact, sidestepping the collapsing `HashMap` used elsewhere.
//! Everything is real, persisted, account-partitioned CRUD.

use async_trait::async_trait;
use http::{Method, StatusCode};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::SharedCodeArtifactState;

pub const CODEARTIFACT_ACTIONS: &[&str] = &[
    "AssociateExternalConnection",
    "CopyPackageVersions",
    "CreateDomain",
    "CreatePackageGroup",
    "CreateRepository",
    "DeleteDomain",
    "DeleteDomainPermissionsPolicy",
    "DeletePackage",
    "DeletePackageGroup",
    "DeletePackageVersions",
    "DeleteRepository",
    "DeleteRepositoryPermissionsPolicy",
    "DescribeDomain",
    "DescribePackage",
    "DescribePackageGroup",
    "DescribePackageVersion",
    "DescribeRepository",
    "DisassociateExternalConnection",
    "DisposePackageVersions",
    "GetAssociatedPackageGroup",
    "GetAuthorizationToken",
    "GetDomainPermissionsPolicy",
    "GetPackageVersionAsset",
    "GetPackageVersionReadme",
    "GetRepositoryEndpoint",
    "GetRepositoryPermissionsPolicy",
    "ListAllowedRepositoriesForGroup",
    "ListAssociatedPackages",
    "ListDomains",
    "ListPackageGroups",
    "ListPackageVersionAssets",
    "ListPackageVersionDependencies",
    "ListPackageVersions",
    "ListPackages",
    "ListRepositories",
    "ListRepositoriesInDomain",
    "ListSubPackageGroups",
    "ListTagsForResource",
    "PublishPackageVersion",
    "PutDomainPermissionsPolicy",
    "PutPackageOriginConfiguration",
    "PutRepositoryPermissionsPolicy",
    "TagResource",
    "UntagResource",
    "UpdatePackageGroup",
    "UpdatePackageGroupOriginConfiguration",
    "UpdatePackageVersionsStatus",
    "UpdateRepository",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "AssociateExternalConnection",
    "CopyPackageVersions",
    "CreateDomain",
    "CreatePackageGroup",
    "CreateRepository",
    "DeleteDomain",
    "DeleteDomainPermissionsPolicy",
    "DeletePackage",
    "DeletePackageGroup",
    "DeletePackageVersions",
    "DeleteRepository",
    "DeleteRepositoryPermissionsPolicy",
    "DisassociateExternalConnection",
    "DisposePackageVersions",
    "PublishPackageVersion",
    "PutDomainPermissionsPolicy",
    "PutPackageOriginConfiguration",
    "PutRepositoryPermissionsPolicy",
    "TagResource",
    "UntagResource",
    "UpdatePackageGroup",
    "UpdatePackageGroupOriginConfiguration",
    "UpdatePackageVersionsStatus",
    "UpdateRepository",
];

pub struct CodeArtifactService {
    state: SharedCodeArtifactState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CodeArtifactService {
    pub fn new(state: SharedCodeArtifactState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an operation name by HTTP method + `@http` URI path.
    /// Returns `None` when no route matches (a 404 routing miss).
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        // Drop a single trailing slash so a trailing-slash variant (`/v1/domain/`)
        // routes like its canonical form instead of yielding an empty final
        // segment that matches nothing. No CodeArtifact route relies on a trailing
        // empty segment.
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        let m = &req.method;
        let post = m == Method::POST;
        let get = m == Method::GET;
        let del = m == Method::DELETE;
        let put = m == Method::PUT;
        let action = match segs.as_slice() {
            ["v1", "domain"] if post => "CreateDomain",
            ["v1", "domain"] if get => "DescribeDomain",
            ["v1", "domain"] if del => "DeleteDomain",
            ["v1", "domains"] if post => "ListDomains",
            ["v1", "domain", "permissions", "policy"] if put => "PutDomainPermissionsPolicy",
            ["v1", "domain", "permissions", "policy"] if get => "GetDomainPermissionsPolicy",
            ["v1", "domain", "permissions", "policy"] if del => "DeleteDomainPermissionsPolicy",
            ["v1", "domain", "repositories"] if post => "ListRepositoriesInDomain",
            ["v1", "repository"] if post => "CreateRepository",
            ["v1", "repository"] if get => "DescribeRepository",
            ["v1", "repository"] if del => "DeleteRepository",
            ["v1", "repository"] if put => "UpdateRepository",
            ["v1", "repositories"] if post => "ListRepositories",
            ["v1", "repository", "external-connection"] if post => "AssociateExternalConnection",
            ["v1", "repository", "external-connection"] if del => "DisassociateExternalConnection",
            ["v1", "repository", "permissions", "policy"] if get => {
                "GetRepositoryPermissionsPolicy"
            }
            ["v1", "repository", "permissions", "policy"] if put => {
                "PutRepositoryPermissionsPolicy"
            }
            // The model spells the DELETE path `policies` (plural).
            ["v1", "repository", "permissions", "policies"] if del => {
                "DeleteRepositoryPermissionsPolicy"
            }
            ["v1", "repository", "endpoint"] if get => "GetRepositoryEndpoint",
            ["v1", "package"] if get => "DescribePackage",
            ["v1", "package"] if del => "DeletePackage",
            ["v1", "package"] if post => "PutPackageOriginConfiguration",
            ["v1", "package", "version"] if get => "DescribePackageVersion",
            ["v1", "package", "version", "asset"] if get => "GetPackageVersionAsset",
            ["v1", "package", "version", "readme"] if get => "GetPackageVersionReadme",
            ["v1", "package", "version", "publish"] if post => "PublishPackageVersion",
            ["v1", "package", "version", "assets"] if post => "ListPackageVersionAssets",
            ["v1", "package", "version", "dependencies"] if post => {
                "ListPackageVersionDependencies"
            }
            ["v1", "package", "versions"] if post => "ListPackageVersions",
            ["v1", "package", "versions", "copy"] if post => "CopyPackageVersions",
            ["v1", "package", "versions", "delete"] if post => "DeletePackageVersions",
            ["v1", "package", "versions", "dispose"] if post => "DisposePackageVersions",
            ["v1", "package", "versions", "update_status"] if post => "UpdatePackageVersionsStatus",
            ["v1", "packages"] if post => "ListPackages",
            ["v1", "package-group"] if post => "CreatePackageGroup",
            ["v1", "package-group"] if get => "DescribePackageGroup",
            ["v1", "package-group"] if del => "DeletePackageGroup",
            ["v1", "package-group"] if put => "UpdatePackageGroup",
            ["v1", "package-groups"] if post => "ListPackageGroups",
            ["v1", "package-groups", "sub-groups"] if post => "ListSubPackageGroups",
            ["v1", "package-group-allowed-repositories"] if get => {
                "ListAllowedRepositoriesForGroup"
            }
            ["v1", "package-group-origin-configuration"] if put => {
                "UpdatePackageGroupOriginConfiguration"
            }
            ["v1", "get-associated-package-group"] if get => "GetAssociatedPackageGroup",
            ["v1", "list-associated-packages"] if get => "ListAssociatedPackages",
            ["v1", "authorization-token"] if post => "GetAuthorizationToken",
            ["v1", "tags"] if post => "ListTagsForResource",
            ["v1", "tag"] if post => "TagResource",
            ["v1", "untag"] if post => "UntagResource",
            _ => return None,
        };
        Some(action)
    }
}

#[async_trait]
impl AwsService for CodeArtifactService {
    fn service_name(&self) -> &str {
        "codeartifact"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(action) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &req);
        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        CODEARTIFACT_ACTIONS
    }
}

impl CodeArtifactService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            // domains
            "CreateDomain" => self.create_domain(req),
            "DescribeDomain" => self.describe_domain(req),
            "DeleteDomain" => self.delete_domain(req),
            "ListDomains" => self.list_domains(req),
            "PutDomainPermissionsPolicy" => self.put_domain_policy(req),
            "GetDomainPermissionsPolicy" => self.get_domain_policy(req),
            "DeleteDomainPermissionsPolicy" => self.delete_domain_policy(req),
            // repositories
            "CreateRepository" => self.create_repository(req),
            "DescribeRepository" => self.describe_repository(req),
            "UpdateRepository" => self.update_repository(req),
            "DeleteRepository" => self.delete_repository(req),
            "ListRepositories" => self.list_repositories(req),
            "ListRepositoriesInDomain" => self.list_repositories_in_domain(req),
            "GetRepositoryEndpoint" => self.get_repository_endpoint(req),
            "AssociateExternalConnection" => self.associate_external_connection(req),
            "DisassociateExternalConnection" => self.disassociate_external_connection(req),
            "GetRepositoryPermissionsPolicy" => self.get_repository_policy(req),
            "PutRepositoryPermissionsPolicy" => self.put_repository_policy(req),
            "DeleteRepositoryPermissionsPolicy" => self.delete_repository_policy(req),
            // packages
            "ListPackages" => self.list_packages(req),
            "DescribePackage" => self.describe_package(req),
            "DeletePackage" => self.delete_package(req),
            "PutPackageOriginConfiguration" => self.put_package_origin(req),
            // package versions
            "ListPackageVersions" => self.list_package_versions(req),
            "DescribePackageVersion" => self.describe_package_version(req),
            "DeletePackageVersions" => self.delete_package_versions(req),
            "DisposePackageVersions" => self.dispose_package_versions(req),
            "UpdatePackageVersionsStatus" => self.update_package_versions_status(req),
            "CopyPackageVersions" => self.copy_package_versions(req),
            "PublishPackageVersion" => self.publish_package_version(req),
            "GetPackageVersionReadme" => self.get_package_version_readme(req),
            "GetPackageVersionAsset" => self.get_package_version_asset(req),
            "ListPackageVersionAssets" => self.list_package_version_assets(req),
            "ListPackageVersionDependencies" => self.list_package_version_dependencies(req),
            // package groups
            "CreatePackageGroup" => self.create_package_group(req),
            "DescribePackageGroup" => self.describe_package_group(req),
            "UpdatePackageGroup" => self.update_package_group(req),
            "DeletePackageGroup" => self.delete_package_group(req),
            "ListPackageGroups" => self.list_package_groups(req),
            "ListSubPackageGroups" => self.list_sub_package_groups(req),
            "ListAllowedRepositoriesForGroup" => self.list_allowed_repositories_for_group(req),
            "GetAssociatedPackageGroup" => self.get_associated_package_group(req),
            "ListAssociatedPackages" => self.list_associated_packages(req),
            "UpdatePackageGroupOriginConfiguration" => self.update_package_group_origin(req),
            // auth + tags
            "GetAuthorizationToken" => self.get_authorization_token(req),
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

include!("handlers.rs");
