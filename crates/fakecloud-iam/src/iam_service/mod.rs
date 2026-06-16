mod account;
mod extras;
mod groups;
mod instance_profiles;
mod oidc;
mod policies;
mod roles;
mod users;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
// NOTE: The shared validation helpers use ValidationException error codes, but real IAM
// typically returns InvalidInput or ValidationError for input validation failures. This is
// a known simplification — the validators are reused across services for consistency.
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use crate::persistence::{save_iam_snapshot, IamSnapshotLock};
use crate::state::{AccessKeyLastUsed, SharedIamState, Tag};

pub struct IamService {
    state: SharedIamState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: IamSnapshotLock,
}

impl IamService {
    pub fn new(state: SharedIamState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: crate::persistence::new_snapshot_lock(),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Share the snapshot lock with another service (e.g. `StsService`)
    /// so writes from both services are mutually serialized. Without
    /// this, STS-issued credentials and IAM mutations could race and
    /// leave stale bytes on disk.
    pub fn with_snapshot_lock(mut self, lock: IamSnapshotLock) -> Self {
        self.snapshot_lock = lock;
        self
    }

    pub fn snapshot_lock(&self) -> IamSnapshotLock {
        self.snapshot_lock.clone()
    }

    pub fn snapshot_store(&self) -> Option<Arc<dyn SnapshotStore>> {
        self.snapshot_store.clone()
    }
}

#[async_trait]
impl AwsService for IamService {
    fn service_name(&self) -> &str {
        "iam"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Track access key usage for GetAccessKeyLastUsed. This is itself
        // a state mutation, so we have to flag it for snapshot persistence
        // even though the action itself may be read-only (e.g. `ListUsers`).
        // Without this, `access_key_last_used` drifts between in-memory
        // and on-disk state and reads after a restart lose the timestamp.
        let mut last_used_updated = false;
        if let Some(ref key_id) = req.access_key_id {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let is_known = state
                .access_keys
                .values()
                .any(|keys| keys.iter().any(|k| k.access_key_id == *key_id));
            if is_known {
                state.access_key_last_used.insert(
                    key_id.clone(),
                    AccessKeyLastUsed {
                        last_used_date: Utc::now(),
                        service_name: "iam".to_string(),
                        region: req.region.clone(),
                    },
                );
                last_used_updated = true;
            }
            drop(accounts);
        }

        let mutates = is_mutating_action(req.action.as_str()) || last_used_updated;
        let result = match req.action.as_str() {
            // Users
            "CreateUser" => self.create_user(&req),
            "GetUser" => self.get_user(&req),
            "DeleteUser" => self.delete_user(&req),
            "ListUsers" => self.list_users(&req),
            "UpdateUser" => self.update_user(&req),
            "TagUser" => self.tag_user(&req),
            "UntagUser" => self.untag_user(&req),
            "ListUserTags" => self.list_user_tags(&req),

            // Access Keys
            "CreateAccessKey" => self.create_access_key(&req),
            "DeleteAccessKey" => self.delete_access_key(&req),
            "ListAccessKeys" => self.list_access_keys(&req),
            "UpdateAccessKey" => self.update_access_key(&req),
            "GetAccessKeyLastUsed" => self.get_access_key_last_used(&req),

            // Roles
            "CreateRole" => self.create_role(&req),
            "GetRole" => self.get_role(&req),
            "DeleteRole" => self.delete_role(&req),
            "ListRoles" => self.list_roles(&req),
            "UpdateRole" => self.update_role(&req),
            "UpdateRoleDescription" => self.update_role_description(&req),
            "UpdateAssumeRolePolicy" => self.update_assume_role_policy(&req),
            "TagRole" => self.tag_role(&req),
            "UntagRole" => self.untag_role(&req),
            "ListRoleTags" => self.list_role_tags(&req),
            "PutRolePermissionsBoundary" => self.put_role_permissions_boundary(&req),
            "DeleteRolePermissionsBoundary" => self.delete_role_permissions_boundary(&req),

            // Policies (managed)
            "CreatePolicy" => self.create_policy(&req),
            "GetPolicy" => self.get_policy(&req),
            "DeletePolicy" => self.delete_policy(&req),
            "ListPolicies" => self.list_policies(&req),
            "TagPolicy" => self.tag_policy(&req),
            "UntagPolicy" => self.untag_policy(&req),
            "ListPolicyTags" => self.list_policy_tags(&req),

            // Policy Versions
            "CreatePolicyVersion" => self.create_policy_version(&req),
            "GetPolicyVersion" => self.get_policy_version(&req),
            "ListPolicyVersions" => self.list_policy_versions(&req),
            "DeletePolicyVersion" => self.delete_policy_version(&req),
            "SetDefaultPolicyVersion" => self.set_default_policy_version(&req),

            // Role policy attachments (managed)
            "AttachRolePolicy" => self.attach_role_policy(&req),
            "DetachRolePolicy" => self.detach_role_policy(&req),
            "ListAttachedRolePolicies" => self.list_attached_role_policies(&req),

            // Role inline policies
            "PutRolePolicy" => self.put_role_policy(&req),
            "GetRolePolicy" => self.get_role_policy(&req),
            "DeleteRolePolicy" => self.delete_role_policy(&req),
            "ListRolePolicies" => self.list_role_policies(&req),

            // User policy attachments (managed)
            "AttachUserPolicy" => self.attach_user_policy(&req),
            "DetachUserPolicy" => self.detach_user_policy(&req),
            "ListAttachedUserPolicies" => self.list_attached_user_policies(&req),

            // User inline policies
            "PutUserPolicy" => self.put_user_policy(&req),
            "GetUserPolicy" => self.get_user_policy(&req),
            "DeleteUserPolicy" => self.delete_user_policy(&req),
            "ListUserPolicies" => self.list_user_policies(&req),
            "PutUserPermissionsBoundary" => self.put_user_permissions_boundary(&req),
            "DeleteUserPermissionsBoundary" => self.delete_user_permissions_boundary(&req),

            // Groups
            "CreateGroup" => self.create_group(&req),
            "GetGroup" => self.get_group(&req),
            "DeleteGroup" => self.delete_group(&req),
            "ListGroups" => self.list_groups(&req),
            "UpdateGroup" => self.update_group(&req),
            "AddUserToGroup" => self.add_user_to_group(&req),
            "RemoveUserFromGroup" => self.remove_user_from_group(&req),
            "ListGroupsForUser" => self.list_groups_for_user(&req),

            // Group policies
            "PutGroupPolicy" => self.put_group_policy(&req),
            "GetGroupPolicy" => self.get_group_policy(&req),
            "DeleteGroupPolicy" => self.delete_group_policy(&req),
            "ListGroupPolicies" => self.list_group_policies(&req),
            "AttachGroupPolicy" => self.attach_group_policy(&req),
            "DetachGroupPolicy" => self.detach_group_policy(&req),
            "ListAttachedGroupPolicies" => self.list_attached_group_policies(&req),

            // Instance Profiles
            "CreateInstanceProfile" => self.create_instance_profile(&req),
            "GetInstanceProfile" => self.get_instance_profile(&req),
            "DeleteInstanceProfile" => self.delete_instance_profile(&req),
            "ListInstanceProfiles" => self.list_instance_profiles(&req),
            "AddRoleToInstanceProfile" => self.add_role_to_instance_profile(&req),
            "RemoveRoleFromInstanceProfile" => self.remove_role_from_instance_profile(&req),
            "ListInstanceProfilesForRole" => self.list_instance_profiles_for_role(&req),
            "TagInstanceProfile" => self.tag_instance_profile(&req),
            "UntagInstanceProfile" => self.untag_instance_profile(&req),
            "ListInstanceProfileTags" => self.list_instance_profile_tags(&req),

            // Login Profiles
            "CreateLoginProfile" => self.create_login_profile(&req),
            "GetLoginProfile" => self.get_login_profile(&req),
            "UpdateLoginProfile" => self.update_login_profile(&req),
            "DeleteLoginProfile" => self.delete_login_profile(&req),

            // SAML Providers
            "CreateSAMLProvider" => self.create_saml_provider(&req),
            "GetSAMLProvider" => self.get_saml_provider(&req),
            "DeleteSAMLProvider" => self.delete_saml_provider(&req),
            "ListSAMLProviders" => self.list_saml_providers(&req),
            "UpdateSAMLProvider" => self.update_saml_provider(&req),

            // OIDC Providers
            "CreateOpenIDConnectProvider" => self.create_oidc_provider(&req),
            "GetOpenIDConnectProvider" => self.get_oidc_provider(&req),
            "DeleteOpenIDConnectProvider" => self.delete_oidc_provider(&req),
            "ListOpenIDConnectProviders" => self.list_oidc_providers(&req),
            "UpdateOpenIDConnectProviderThumbprint" => self.update_oidc_thumbprint(&req),
            "AddClientIDToOpenIDConnectProvider" => self.add_client_id_to_oidc(&req),
            "RemoveClientIDFromOpenIDConnectProvider" => self.remove_client_id_from_oidc(&req),
            "TagOpenIDConnectProvider" => self.tag_oidc_provider(&req),
            "UntagOpenIDConnectProvider" => self.untag_oidc_provider(&req),
            "ListOpenIDConnectProviderTags" => self.list_oidc_provider_tags(&req),

            // Server Certificates
            "UploadServerCertificate" => self.upload_server_certificate(&req),
            "GetServerCertificate" => self.get_server_certificate(&req),
            "DeleteServerCertificate" => self.delete_server_certificate(&req),
            "ListServerCertificates" => self.list_server_certificates(&req),

            // Signing Certificates
            "UploadSigningCertificate" => self.upload_signing_certificate(&req),
            "ListSigningCertificates" => self.list_signing_certificates(&req),
            "UpdateSigningCertificate" => self.update_signing_certificate(&req),
            "DeleteSigningCertificate" => self.delete_signing_certificate(&req),

            // SSH Public Keys
            "UploadSSHPublicKey" => self.upload_ssh_public_key(&req),
            "GetSSHPublicKey" => self.get_ssh_public_key(&req),
            "ListSSHPublicKeys" => self.list_ssh_public_keys(&req),
            "UpdateSSHPublicKey" => self.update_ssh_public_key(&req),
            "DeleteSSHPublicKey" => self.delete_ssh_public_key(&req),

            // Service Linked Roles
            "CreateServiceLinkedRole" => self.create_service_linked_role(&req),
            "DeleteServiceLinkedRole" => self.delete_service_linked_role(&req),
            "GetServiceLinkedRoleDeletionStatus" => {
                self.get_service_linked_role_deletion_status(&req)
            }

            // Account
            "GetAccountSummary" => self.get_account_summary(&req),
            "GetAccountAuthorizationDetails" => self.get_account_authorization_details(&req),
            "CreateAccountAlias" => self.create_account_alias(&req),
            "DeleteAccountAlias" => self.delete_account_alias(&req),
            "ListAccountAliases" => self.list_account_aliases(&req),
            "UpdateAccountPasswordPolicy" => self.update_account_password_policy(&req),
            "GetAccountPasswordPolicy" => self.get_account_password_policy(&req),
            "DeleteAccountPasswordPolicy" => self.delete_account_password_policy(&req),

            // Credential Report
            "GenerateCredentialReport" => self.generate_credential_report(&req),
            "GetCredentialReport" => self.get_credential_report(&req),

            // Virtual MFA Devices
            "CreateVirtualMFADevice" => self.create_virtual_mfa_device(&req),
            "DeleteVirtualMFADevice" => self.delete_virtual_mfa_device(&req),
            "ListVirtualMFADevices" => self.list_virtual_mfa_devices(&req),
            "EnableMFADevice" => self.enable_mfa_device(&req),
            "DeactivateMFADevice" => self.deactivate_mfa_device(&req),
            "ListMFADevices" => self.list_mfa_devices(&req),

            // Entities for policy
            "ListEntitiesForPolicy" => self.list_entities_for_policy(&req),

            // Service-specific credentials
            "CreateServiceSpecificCredential" => self.create_service_specific_credential(&req),
            "DeleteServiceSpecificCredential" => self.delete_service_specific_credential(&req),
            "ListServiceSpecificCredentials" => self.list_service_specific_credentials(&req),
            "ResetServiceSpecificCredential" => self.reset_service_specific_credential(&req),
            "UpdateServiceSpecificCredential" => self.update_service_specific_credential(&req),

            // Organizations integration
            "EnableOrganizationsRootCredentialsManagement" => {
                self.enable_organizations_root_credentials_management(&req)
            }
            "DisableOrganizationsRootCredentialsManagement" => {
                self.disable_organizations_root_credentials_management(&req)
            }
            "EnableOrganizationsRootSessions" => self.enable_organizations_root_sessions(&req),
            "DisableOrganizationsRootSessions" => self.disable_organizations_root_sessions(&req),
            "GenerateOrganizationsAccessReport" => self.generate_organizations_access_report(&req),
            "GetOrganizationsAccessReport" => self.get_organizations_access_report(&req),
            "ListOrganizationsFeatures" => self.list_organizations_features(&req),

            // Service last accessed details
            "GenerateServiceLastAccessedDetails" => {
                self.generate_service_last_accessed_details(&req)
            }
            "GetServiceLastAccessedDetails" => self.get_service_last_accessed_details(&req),
            "GetServiceLastAccessedDetailsWithEntities" => {
                self.get_service_last_accessed_details_with_entities(&req)
            }

            // Tags on more resource types
            "TagSAMLProvider" => self.tag_saml_provider(&req),
            "UntagSAMLProvider" => self.untag_saml_provider(&req),
            "ListSAMLProviderTags" => self.list_saml_provider_tags(&req),
            "TagServerCertificate" => self.tag_server_certificate(&req),
            "UntagServerCertificate" => self.untag_server_certificate(&req),
            "ListServerCertificateTags" => self.list_server_certificate_tags(&req),
            "TagMFADevice" => self.tag_mfa_device(&req),
            "UntagMFADevice" => self.untag_mfa_device(&req),
            "ListMFADeviceTags" => self.list_mfa_device_tags(&req),

            // Policy simulation
            "SimulateCustomPolicy" => self.simulate_custom_policy(&req),
            "SimulatePrincipalPolicy" => self.simulate_principal_policy(&req),
            "GetContextKeysForCustomPolicy" => self.get_context_keys_for_custom_policy(&req),
            "GetContextKeysForPrincipalPolicy" => self.get_context_keys_for_principal_policy(&req),
            "ListPoliciesGrantingServiceAccess" => self.list_policies_granting_service_access(&req),

            // Misc
            "ChangePassword" => self.change_password(&req),
            "GetMFADevice" => self.get_mfa_device(&req),
            "ResyncMFADevice" => self.resync_mfa_device(&req),
            "SetSecurityTokenServicePreferences" => {
                self.set_security_token_service_preferences(&req)
            }
            "GetSecurityTokenServicePreferences" => {
                self.get_security_token_service_preferences(&req)
            }
            "UpdateServerCertificate" => self.update_server_certificate(&req),

            // Delegation requests + outbound web identity federation
            "CreateDelegationRequest" => self.create_delegation_request(&req),
            "GetDelegationRequest" => self.get_delegation_request(&req),
            "ListDelegationRequests" => self.list_delegation_requests(&req),
            "AcceptDelegationRequest" => self.accept_delegation_request(&req),
            "RejectDelegationRequest" => self.reject_delegation_request(&req),
            "UpdateDelegationRequest" => self.update_delegation_request(&req),
            "AssociateDelegationRequest" => self.associate_delegation_request(&req),
            "SendDelegationToken" => self.send_delegation_token(&req),
            "EnableOutboundWebIdentityFederation" => {
                self.enable_outbound_web_identity_federation(&req)
            }
            "DisableOutboundWebIdentityFederation" => {
                self.disable_outbound_web_identity_federation(&req)
            }
            "GetOutboundWebIdentityFederationInfo" => {
                self.get_outbound_web_identity_federation_info(&req)
            }
            "GetHumanReadableSummary" => self.get_human_readable_summary(&req),

            _ => Err(AwsServiceError::action_not_implemented("iam", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            save_iam_snapshot(
                &self.state,
                self.snapshot_store.clone(),
                &self.snapshot_lock,
            )
            .await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    /// IAM opts into Phase 1 enforcement. Every action in
    /// [`SUPPORTED_ACTIONS`] maps to a concrete `IamAction` — see
    /// [`iam_action_resource`] for the resource-ARN extraction logic.
    fn iam_enforceable(&self) -> bool {
        true
    }

    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        // The action list is small enough to reuse SUPPORTED_ACTIONS as
        // the whitelist: anything we didn't declare support for isn't
        // enforced (and would be handled as action_not_implemented by
        // the match in `handle()`).
        let static_action = SUPPORTED_ACTIONS
            .iter()
            .copied()
            .find(|a| *a == request.action)?;
        let account = request
            .principal
            .as_ref()
            .map(|p| p.account_id.as_str())
            .unwrap_or(request.account_id.as_str());
        let partition = partition_for_region(&request.region);
        Some(fakecloud_core::auth::IamAction {
            service: "iam",
            action: static_action,
            resource: iam_action_resource(static_action, partition, account, request),
        })
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        iam_resource_tags(&self.state, resource_arn)
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        iam_request_tags(request, action)
    }
}

/// All IAM actions fakecloud handles. Promoted to a file-level const so
/// the `supported_actions()` method body is one line and the action
/// list is easy to grep / diff over time. Same shape as the logs
/// equivalent introduced in PR #336.
const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateUser",
    "GetUser",
    "DeleteUser",
    "ListUsers",
    "UpdateUser",
    "TagUser",
    "UntagUser",
    "ListUserTags",
    "CreateAccessKey",
    "DeleteAccessKey",
    "ListAccessKeys",
    "UpdateAccessKey",
    "GetAccessKeyLastUsed",
    "CreateRole",
    "GetRole",
    "DeleteRole",
    "ListRoles",
    "UpdateRole",
    "UpdateRoleDescription",
    "UpdateAssumeRolePolicy",
    "TagRole",
    "UntagRole",
    "ListRoleTags",
    "PutRolePermissionsBoundary",
    "DeleteRolePermissionsBoundary",
    "CreatePolicy",
    "GetPolicy",
    "DeletePolicy",
    "ListPolicies",
    "TagPolicy",
    "UntagPolicy",
    "ListPolicyTags",
    "CreatePolicyVersion",
    "GetPolicyVersion",
    "ListPolicyVersions",
    "DeletePolicyVersion",
    "SetDefaultPolicyVersion",
    "AttachRolePolicy",
    "DetachRolePolicy",
    "ListAttachedRolePolicies",
    "PutRolePolicy",
    "GetRolePolicy",
    "DeleteRolePolicy",
    "ListRolePolicies",
    "AttachUserPolicy",
    "DetachUserPolicy",
    "ListAttachedUserPolicies",
    "PutUserPolicy",
    "GetUserPolicy",
    "DeleteUserPolicy",
    "ListUserPolicies",
    "PutUserPermissionsBoundary",
    "DeleteUserPermissionsBoundary",
    "CreateGroup",
    "GetGroup",
    "DeleteGroup",
    "ListGroups",
    "UpdateGroup",
    "AddUserToGroup",
    "RemoveUserFromGroup",
    "ListGroupsForUser",
    "PutGroupPolicy",
    "GetGroupPolicy",
    "DeleteGroupPolicy",
    "ListGroupPolicies",
    "AttachGroupPolicy",
    "DetachGroupPolicy",
    "ListAttachedGroupPolicies",
    "CreateInstanceProfile",
    "GetInstanceProfile",
    "DeleteInstanceProfile",
    "ListInstanceProfiles",
    "AddRoleToInstanceProfile",
    "RemoveRoleFromInstanceProfile",
    "ListInstanceProfilesForRole",
    "TagInstanceProfile",
    "UntagInstanceProfile",
    "ListInstanceProfileTags",
    "CreateLoginProfile",
    "GetLoginProfile",
    "UpdateLoginProfile",
    "DeleteLoginProfile",
    "CreateSAMLProvider",
    "GetSAMLProvider",
    "DeleteSAMLProvider",
    "ListSAMLProviders",
    "UpdateSAMLProvider",
    "CreateOpenIDConnectProvider",
    "GetOpenIDConnectProvider",
    "DeleteOpenIDConnectProvider",
    "ListOpenIDConnectProviders",
    "UpdateOpenIDConnectProviderThumbprint",
    "AddClientIDToOpenIDConnectProvider",
    "RemoveClientIDFromOpenIDConnectProvider",
    "TagOpenIDConnectProvider",
    "UntagOpenIDConnectProvider",
    "ListOpenIDConnectProviderTags",
    "UploadServerCertificate",
    "GetServerCertificate",
    "DeleteServerCertificate",
    "ListServerCertificates",
    "UploadSigningCertificate",
    "ListSigningCertificates",
    "UpdateSigningCertificate",
    "DeleteSigningCertificate",
    "CreateServiceLinkedRole",
    "DeleteServiceLinkedRole",
    "GetServiceLinkedRoleDeletionStatus",
    "GetAccountSummary",
    "GetAccountAuthorizationDetails",
    "CreateAccountAlias",
    "DeleteAccountAlias",
    "ListAccountAliases",
    "UpdateAccountPasswordPolicy",
    "GetAccountPasswordPolicy",
    "DeleteAccountPasswordPolicy",
    "GenerateCredentialReport",
    "GetCredentialReport",
    "CreateVirtualMFADevice",
    "DeleteVirtualMFADevice",
    "ListVirtualMFADevices",
    "EnableMFADevice",
    "DeactivateMFADevice",
    "ListMFADevices",
    "ListEntitiesForPolicy",
    "UploadSSHPublicKey",
    "GetSSHPublicKey",
    "ListSSHPublicKeys",
    "UpdateSSHPublicKey",
    "DeleteSSHPublicKey",
    "CreateServiceSpecificCredential",
    "DeleteServiceSpecificCredential",
    "ListServiceSpecificCredentials",
    "ResetServiceSpecificCredential",
    "UpdateServiceSpecificCredential",
    "EnableOrganizationsRootCredentialsManagement",
    "DisableOrganizationsRootCredentialsManagement",
    "EnableOrganizationsRootSessions",
    "DisableOrganizationsRootSessions",
    "GenerateOrganizationsAccessReport",
    "GetOrganizationsAccessReport",
    "ListOrganizationsFeatures",
    "GenerateServiceLastAccessedDetails",
    "GetServiceLastAccessedDetails",
    "GetServiceLastAccessedDetailsWithEntities",
    "TagSAMLProvider",
    "UntagSAMLProvider",
    "ListSAMLProviderTags",
    "TagServerCertificate",
    "UntagServerCertificate",
    "ListServerCertificateTags",
    "TagMFADevice",
    "UntagMFADevice",
    "ListMFADeviceTags",
    "SimulateCustomPolicy",
    "SimulatePrincipalPolicy",
    "GetContextKeysForCustomPolicy",
    "GetContextKeysForPrincipalPolicy",
    "ListPoliciesGrantingServiceAccess",
    "ChangePassword",
    "GetMFADevice",
    "ResyncMFADevice",
    "SetSecurityTokenServicePreferences",
    "UpdateServerCertificate",
    "CreateDelegationRequest",
    "GetDelegationRequest",
    "ListDelegationRequests",
    "AcceptDelegationRequest",
    "RejectDelegationRequest",
    "UpdateDelegationRequest",
    "AssociateDelegationRequest",
    "SendDelegationToken",
    "EnableOutboundWebIdentityFederation",
    "DisableOutboundWebIdentityFederation",
    "GetOutboundWebIdentityFederationInfo",
    "GetHumanReadableSummary",
];

// ========= Helper functions =========

use fakecloud_aws::xml::xml_escape;

mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod tests;
