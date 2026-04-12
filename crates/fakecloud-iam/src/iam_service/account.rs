use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{AccountPasswordPolicy, VirtualMfaDevice};

use super::{empty_response, parse_tags, required_param, tags_xml, url_encode, IamService};

use fakecloud_aws::xml::xml_escape;

impl IamService {
    pub(super) fn get_account_summary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountSummaryResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountSummaryResult>
    <SummaryMap>
      <entry><key>Users</key><value>{}</value></entry>
      <entry><key>UsersQuota</key><value>5000</value></entry>
      <entry><key>Groups</key><value>{}</value></entry>
      <entry><key>GroupsQuota</key><value>300</value></entry>
      <entry><key>ServerCertificates</key><value>{}</value></entry>
      <entry><key>ServerCertificatesQuota</key><value>20</value></entry>
      <entry><key>UserPolicySizeQuota</key><value>2048</value></entry>
      <entry><key>GroupPolicySizeQuota</key><value>5120</value></entry>
      <entry><key>GroupsPerUserQuota</key><value>10</value></entry>
      <entry><key>SigningCertificatesPerUserQuota</key><value>2</value></entry>
      <entry><key>AccessKeysPerUserQuota</key><value>2</value></entry>
      <entry><key>MFADevices</key><value>{}</value></entry>
      <entry><key>MFADevicesInUse</key><value>{}</value></entry>
      <entry><key>AccountMFAEnabled</key><value>0</value></entry>
      <entry><key>AccountAccessKeysPresent</key><value>0</value></entry>
      <entry><key>AccountSigningCertificatesPresent</key><value>0</value></entry>
      <entry><key>Policies</key><value>{}</value></entry>
      <entry><key>PoliciesQuota</key><value>1500</value></entry>
      <entry><key>PolicySizeQuota</key><value>6144</value></entry>
      <entry><key>PolicyVersionsInUse</key><value>{}</value></entry>
      <entry><key>PolicyVersionsInUseQuota</key><value>10000</value></entry>
      <entry><key>VersionsPerPolicyQuota</key><value>5</value></entry>
      <entry><key>Roles</key><value>{}</value></entry>
      <entry><key>RolesQuota</key><value>1000</value></entry>
      <entry><key>RolePolicySizeQuota</key><value>10240</value></entry>
      <entry><key>InstanceProfiles</key><value>{}</value></entry>
      <entry><key>InstanceProfilesQuota</key><value>1000</value></entry>
      <entry><key>Providers</key><value>{}</value></entry>
      <entry><key>AttachedPoliciesPerGroupQuota</key><value>10</value></entry>
      <entry><key>AttachedPoliciesPerRoleQuota</key><value>10</value></entry>
      <entry><key>AttachedPoliciesPerUserQuota</key><value>10</value></entry>
      <entry><key>GlobalEndpointTokenVersion</key><value>1</value></entry>
      <entry><key>AssumeRolePolicySizeQuota</key><value>2048</value></entry>
    </SummaryMap>
  </GetAccountSummaryResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountSummaryResponse>"#,
            state.users.len(),
            state.groups.len(),
            state.server_certificates.len(),
            // MFADevices: count all devices with an assigned user (hardware + virtual enabled)
            state
                .virtual_mfa_devices
                .values()
                .filter(|d| d.user.is_some())
                .count(),
            // MFADevicesInUse: count enabled devices
            state
                .virtual_mfa_devices
                .values()
                .filter(|d| d.user.is_some() && d.enable_date.is_some())
                .count(),
            state.policies.len(),
            // PolicyVersionsInUse: sum of all versions across all policies
            state
                .policies
                .values()
                .map(|p| p.versions.len())
                .sum::<usize>(),
            state.roles.len(),
            state.instance_profiles.len(),
            state.saml_providers.len() + state.oidc_providers.len(),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_account_authorization_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        // Build user details
        let user_details: String = state
            .users
            .values()
            .map(|u| {
                let inline_policies: String = state
                    .user_inline_policies
                    .get(&u.user_name)
                    .map(|policies| {
                        policies
                            .iter()
                            .map(|(name, doc)| {
                                format!(
                                    "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                                    xml_escape(name),
                                    url_encode(doc)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let attached: String = state
                    .user_policies
                    .get(&u.user_name)
                    .map(|arns| {
                        arns.iter()
                            .filter_map(|arn| {
                                state.policies.get(arn).map(|p| {
                                    format!(
                                        "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                        p.policy_name, p.arn
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let group_list: String = state
                    .groups
                    .values()
                    .filter(|g| g.members.contains(&u.user_name))
                    .map(|g| format!("          <member>{}</member>", g.group_name))
                    .collect::<Vec<_>>()
                    .join("\n");

                let tags_members = tags_xml(&u.tags);

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <UserName>{}</UserName>\n        <UserId>{}</UserId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <UserPolicyList>\n{inline_policies}\n        </UserPolicyList>\n        <GroupList>\n{group_list}\n        </GroupList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n        <Tags>\n{tags_members}\n        </Tags>\n      </member>",
                    u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build role details
        let role_details: String = state
            .roles
            .values()
            .map(|r| {
                let inline_policies: String = state
                    .role_inline_policies
                    .get(&r.role_name)
                    .map(|policies| {
                        policies
                            .iter()
                            .map(|(name, doc)| {
                                format!(
                                    "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                                    xml_escape(name),
                                    url_encode(doc)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let attached: String = state
                    .role_policies
                    .get(&r.role_name)
                    .map(|arns| {
                        arns.iter()
                            .filter_map(|arn| {
                                state.policies.get(arn).map(|p| {
                                    format!(
                                        "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                        p.policy_name, p.arn
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let instance_profiles: String = state
                    .instance_profiles
                    .values()
                    .filter(|ip| ip.roles.contains(&r.role_name))
                    .map(|ip| {
                        format!(
                            "          <member>\n            <InstanceProfileName>{}</InstanceProfileName>\n            <InstanceProfileId>{}</InstanceProfileId>\n            <Arn>{}</Arn>\n            <Path>{}</Path>\n            <CreateDate>{}</CreateDate>\n          </member>",
                            ip.instance_profile_name, ip.instance_profile_id, ip.arn, ip.path, ip.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let tags_members = tags_xml(&r.tags);

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <RoleName>{}</RoleName>\n        <RoleId>{}</RoleId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <AssumeRolePolicyDocument>{}</AssumeRolePolicyDocument>\n        <RolePolicyList>\n{inline_policies}\n        </RolePolicyList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n        <InstanceProfileList>\n{instance_profiles}\n        </InstanceProfileList>\n        <Tags>\n{tags_members}\n        </Tags>\n      </member>",
                    r.path, r.role_name, r.role_id, r.arn, r.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                    url_encode(&r.assume_role_policy_document),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build group details
        let group_details: String = state
            .groups
            .values()
            .map(|g| {
                let inline_policies: String = g
                    .inline_policies
                    .iter()
                    .map(|(name, doc)| {
                        format!(
                            "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                            xml_escape(name),
                            url_encode(doc)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let attached: String = g
                    .attached_policies
                    .iter()
                    .filter_map(|arn| {
                        state.policies.get(arn).map(|p| {
                            format!(
                                "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                p.policy_name, p.arn
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <GroupPolicyList>\n{inline_policies}\n        </GroupPolicyList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n      </member>",
                    g.path, g.group_name, g.group_id, g.arn, g.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build policy details
        let policy_details: String = state
            .policies
            .values()
            .map(|p| {
                let versions: String = p
                    .versions
                    .iter()
                    .map(|v| {
                        format!(
                            "            <member>\n              <VersionId>{}</VersionId>\n              <IsDefaultVersion>{}</IsDefaultVersion>\n              <Document>{}</Document>\n              <CreateDate>{}</CreateDate>\n            </member>",
                            v.version_id, v.is_default, url_encode(&v.document), v.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!(
                    "      <member>\n        <PolicyName>{}</PolicyName>\n        <PolicyId>{}</PolicyId>\n        <Arn>{}</Arn>\n        <Path>{}</Path>\n        <DefaultVersionId>{}</DefaultVersionId>\n        <AttachmentCount>{}</AttachmentCount>\n        <IsAttachable>true</IsAttachable>\n        <CreateDate>{}</CreateDate>\n        <PolicyVersionList>\n{versions}\n        </PolicyVersionList>\n      </member>",
                    p.policy_name, p.policy_id, p.arn, p.path, p.default_version_id,
                    p.attachment_count, p.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountAuthorizationDetailsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountAuthorizationDetailsResult>
    <IsTruncated>false</IsTruncated>
    <UserDetailList>
{user_details}
    </UserDetailList>
    <RoleDetailList>
{role_details}
    </RoleDetailList>
    <GroupDetailList>
{group_details}
    </GroupDetailList>
    <Policies>
{policy_details}
    </Policies>
  </GetAccountAuthorizationDetailsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountAuthorizationDetailsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn create_account_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias = required_param(&req.query_params, "AccountAlias")?;
        let mut state = self.state.write();

        if !state.account_aliases.contains(&alias) {
            state.account_aliases.push(alias);
        }

        let xml = empty_response("CreateAccountAlias", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_account_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias = required_param(&req.query_params, "AccountAlias")?;
        let mut state = self.state.write();
        state.account_aliases.retain(|a| a != &alias);

        let xml = empty_response("DeleteAccountAlias", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_account_aliases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let members: String = state
            .account_aliases
            .iter()
            .map(|a| format!("      <member>{a}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAccountAliasesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAccountAliasesResult>
    <IsTruncated>false</IsTruncated>
    <AccountAliases>
{members}
    </AccountAliases>
  </ListAccountAliasesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListAccountAliasesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate constraints
        let min_len: Option<i64> = req
            .query_params
            .get("MinimumPasswordLength")
            .and_then(|v| v.parse().ok());
        let max_age: Option<i64> = req
            .query_params
            .get("MaxPasswordAge")
            .and_then(|v| v.parse().ok());
        let reuse_prevention: Option<i64> = req
            .query_params
            .get("PasswordReusePrevention")
            .and_then(|v| v.parse().ok());

        let mut errors = Vec::new();
        if let Some(v) = min_len {
            if v > 128 {
                errors.push(format!("Value \"{v}\" at \"minimumPasswordLength\" failed to satisfy constraint: Member must have value less than or equal to 128"));
            }
        }
        if let Some(v) = reuse_prevention {
            if v > 24 {
                errors.push(format!("Value \"{v}\" at \"passwordReusePrevention\" failed to satisfy constraint: Member must have value less than or equal to 24"));
            }
        }
        if let Some(v) = max_age {
            if v > 1095 {
                errors.push(format!("Value \"{v}\" at \"maxPasswordAge\" failed to satisfy constraint: Member must have value less than or equal to 1095"));
            }
        }
        if !errors.is_empty() {
            let n = errors.len();
            let msg = format!(
                "{n} validation error{} detected: {}",
                if n > 1 { "s" } else { "" },
                errors.join("; ")
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                msg,
            ));
        }

        let mut state = self.state.write();

        let policy = state
            .account_password_policy
            .get_or_insert(AccountPasswordPolicy::default());

        if let Some(v) = req
            .query_params
            .get("MinimumPasswordLength")
            .and_then(|v| v.parse().ok())
        {
            policy.minimum_password_length = v;
        }
        if let Some(v) = req.query_params.get("RequireSymbols") {
            policy.require_symbols = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireNumbers") {
            policy.require_numbers = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireUppercaseCharacters") {
            policy.require_uppercase_characters = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireLowercaseCharacters") {
            policy.require_lowercase_characters = v == "true";
        }
        if let Some(v) = req.query_params.get("AllowUsersToChangePassword") {
            policy.allow_users_to_change_password = v == "true";
        }
        if let Some(v) = req
            .query_params
            .get("MaxPasswordAge")
            .and_then(|v| v.parse().ok())
        {
            policy.max_password_age = v;
        }
        if let Some(v) = req
            .query_params
            .get("PasswordReusePrevention")
            .and_then(|v| v.parse().ok())
        {
            policy.password_reuse_prevention = v;
        }
        if let Some(v) = req.query_params.get("HardExpiry") {
            policy.hard_expiry = v == "true";
        }

        let xml = empty_response("UpdateAccountPasswordPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let policy = state.account_password_policy.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!(
                    "The Password Policy with domain name {} cannot be found.",
                    state.account_id
                ),
            )
        })?;

        let max_age_xml = format!(
            "\n      <MaxPasswordAge>{}</MaxPasswordAge>",
            policy.max_password_age
        );

        let reuse_prevention_xml = if policy.password_reuse_prevention > 0 {
            format!(
                "\n      <PasswordReusePrevention>{}</PasswordReusePrevention>",
                policy.password_reuse_prevention
            )
        } else {
            String::new()
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountPasswordPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountPasswordPolicyResult>
    <PasswordPolicy>
      <MinimumPasswordLength>{}</MinimumPasswordLength>
      <RequireSymbols>{}</RequireSymbols>
      <RequireNumbers>{}</RequireNumbers>
      <RequireUppercaseCharacters>{}</RequireUppercaseCharacters>
      <RequireLowercaseCharacters>{}</RequireLowercaseCharacters>
      <AllowUsersToChangePassword>{}</AllowUsersToChangePassword>{max_age_xml}{reuse_prevention_xml}
      <HardExpiry>{}</HardExpiry>
      <ExpirePasswords>{}</ExpirePasswords>
    </PasswordPolicy>
  </GetAccountPasswordPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountPasswordPolicyResponse>"#,
            policy.minimum_password_length,
            policy.require_symbols,
            policy.require_numbers,
            policy.require_uppercase_characters,
            policy.require_lowercase_characters,
            policy.allow_users_to_change_password,
            policy.hard_expiry,
            policy.max_password_age > 0,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.account_password_policy.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                "The account policy with name PasswordPolicy cannot be found.".to_string(),
            ));
        }

        state.account_password_policy = None;

        let xml = empty_response("DeleteAccountPasswordPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn generate_credential_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let (report_state, description) = if state.credential_report_generated {
            ("COMPLETE", "Report generated")
        } else {
            state.credential_report_generated = true;
            (
                "STARTED",
                "No report exists. Starting a new report generation.",
            )
        };
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GenerateCredentialReportResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GenerateCredentialReportResult>
    <State>{report_state}</State>
    <Description>{description}</Description>
  </GenerateCredentialReportResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GenerateCredentialReportResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_credential_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use base64::Engine;
        let state = self.state.read();

        if !state.credential_report_generated {
            return Err(AwsServiceError::aws_error(
                StatusCode::GONE,
                "ReportNotPresent",
                "Credential report does not exist. Use GenerateCredentialReport to generate one.",
            ));
        }

        let mut csv = String::from(
            "user,arn,user_creation_time,password_enabled,password_last_used,password_last_changed,password_next_rotation,mfa_active,access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date,access_key_1_last_used_region,access_key_1_last_used_service,access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date,access_key_2_last_used_region,access_key_2_last_used_service,cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated\n"
        );

        // User rows first (sorted), then root account
        let mut sorted_users: Vec<&crate::state::IamUser> = state.users.values().collect();
        sorted_users.sort_by(|a, b| a.user_name.cmp(&b.user_name));

        for user in &sorted_users {
            let has_password = state.login_profiles.contains_key(&user.user_name);
            let password_last_used = if has_password {
                "no_information".to_string()
            } else {
                "not_supported".to_string()
            };
            let keys = state
                .access_keys
                .get(&user.user_name)
                .cloned()
                .unwrap_or_default();
            let key1_active = keys.first().map(|k| k.status == "Active").unwrap_or(false);
            let key1_last_rotated = keys
                .first()
                .map(|k| k.created_at.format("%Y-%m-%dT%H:%M:%S+00:00").to_string())
                .unwrap_or_else(|| "N/A".to_string());
            let key2_active = keys.get(1).map(|k| k.status == "Active").unwrap_or(false);
            let key2_last_rotated = keys
                .get(1)
                .map(|k| k.created_at.format("%Y-%m-%dT%H:%M:%S+00:00").to_string())
                .unwrap_or_else(|| "N/A".to_string());
            let mfa_active = state
                .virtual_mfa_devices
                .values()
                .any(|d| d.user.as_deref() == Some(&user.user_name) && d.enable_date.is_some());
            let certs = state
                .signing_certificates
                .get(&user.user_name)
                .cloned()
                .unwrap_or_default();
            let cert1_active = certs.first().map(|c| c.status == "Active").unwrap_or(false);
            let cert2_active = certs.get(1).map(|c| c.status == "Active").unwrap_or(false);

            csv.push_str(&format!(
                "{},{},{},{},{},N/A,N/A,{},{},{},N/A,N/A,N/A,{},{},N/A,N/A,N/A,{},N/A,{},N/A\n",
                user.user_name,
                user.arn,
                user.created_at.format("%Y-%m-%dT%H:%M:%S+00:00"),
                has_password,
                password_last_used,
                mfa_active,
                key1_active,
                key1_last_rotated,
                key2_active,
                key2_last_rotated,
                cert1_active,
                cert2_active,
            ));
        }

        // Root account row (after users)
        csv.push_str(&format!(
            "<root_account>,arn:aws:iam::{}:root,{},not_supported,not_supported,not_supported,not_supported,false,false,N/A,N/A,N/A,N/A,false,N/A,N/A,N/A,N/A,false,N/A,false,N/A\n",
            state.account_id,
            Utc::now().format("%Y-%m-%dT%H:%M:%S+00:00")
        ));

        let encoded = base64::engine::general_purpose::STANDARD.encode(csv.as_bytes());

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetCredentialReportResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetCredentialReportResult>
    <Content>{encoded}</Content>
    <GeneratedTime>{}</GeneratedTime>
    <ReportFormat>text/csv</ReportFormat>
  </GetCredentialReportResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetCredentialReportResponse>"#,
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn create_virtual_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let virtual_mfa_device_name = required_param(&req.query_params, "VirtualMFADeviceName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let tags = parse_tags(&req.query_params);

        // Validate path length first (different error message than format)
        if path.len() > 512 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!(
                    "1 validation error detected: Value \"{}\" at \"path\" failed to satisfy constraint: Member must have length less than or equal to 512",
                    path
                ),
            ));
        }

        // Validate path format
        if !is_valid_iam_path(&path) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "The specified value for path is invalid. It must begin and end with / and contain only alphanumeric characters and/or / characters.",
            ));
        }

        let mut state = self.state.write();

        // Include path in serial number
        let path_part = path.trim_start_matches('/');
        let serial_number = format!(
            "arn:aws:iam::{}:mfa/{}{}",
            state.account_id, path_part, virtual_mfa_device_name
        );

        if state.virtual_mfa_devices.contains_key(&serial_number) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                "MFADevice entity at the same path and name already exists.".to_string(),
            ));
        }

        use base64::Engine;
        let seed = uuid::Uuid::new_v4().to_string();
        let seed_b32 = base64::engine::general_purpose::STANDARD.encode(seed.as_bytes());
        let qr_png = base64::engine::general_purpose::STANDARD
            .encode(format!("fake-qr-{}", virtual_mfa_device_name).as_bytes());

        let device = VirtualMfaDevice {
            serial_number: serial_number.clone(),
            base32_string_seed: seed_b32.clone(),
            qr_code_png: qr_png.clone(),
            enable_date: None,
            user: None,
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateVirtualMFADeviceResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateVirtualMFADeviceResult>
    <VirtualMFADevice>
      <SerialNumber>{serial_number}</SerialNumber>
      <Base32StringSeed>{seed_b32}</Base32StringSeed>
      <QRCodePNG>{qr_png}</QRCodePNG>
    </VirtualMFADevice>
  </CreateVirtualMFADeviceResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateVirtualMFADeviceResponse>"#,
            req.request_id
        );

        state.virtual_mfa_devices.insert(serial_number, device);
        let _ = path; // path is used in serial_number for MFA devices

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_virtual_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serial_number = required_param(&req.query_params, "SerialNumber")?;
        let mut state = self.state.write();

        if state.virtual_mfa_devices.remove(&serial_number).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("VirtualMFADevice with serial number {serial_number} doesn't exist."),
            ));
        }

        let xml = empty_response("DeleteVirtualMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_virtual_mfa_devices(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let assignment_status = req.query_params.get("AssignmentStatus").cloned();
        let max_items_i64 = parse_optional_i64_param(
            "maxItems",
            req.query_params.get("MaxItems").map(|s| s.as_str()),
        )?;
        validate_optional_range_i64("maxItems", max_items_i64, 1, 1000)?;
        let max_items: Option<usize> = max_items_i64.map(|v| v as usize);
        let marker: Option<usize> = req
            .query_params
            .get("Marker")
            .map(|v| {
                v.parse::<usize>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationError",
                        "Invalid Marker.",
                    )
                })
            })
            .transpose()?;

        let mut devices: Vec<&VirtualMfaDevice> = state
            .virtual_mfa_devices
            .values()
            // Exclude hardware MFA placeholders (created by EnableMFADevice with
            // non-virtual serial numbers); they have empty base32_string_seed
            .filter(|d| !d.base32_string_seed.is_empty())
            .filter(|d| match assignment_status.as_deref() {
                Some("Assigned") => d.user.is_some(),
                Some("Unassigned") => d.user.is_none(),
                _ => true,
            })
            .collect();
        devices.sort_by(|a, b| a.serial_number.cmp(&b.serial_number));

        let start = marker.unwrap_or(0);
        if start > devices.len() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "Invalid Marker.",
            ));
        }
        let (page, is_truncated, next_marker) = if let Some(max) = max_items {
            let end = (start + max).min(devices.len());
            let truncated = end < devices.len();
            let nm = if truncated {
                Some(end.to_string())
            } else {
                None
            };
            (&devices[start..end], truncated, nm)
        } else {
            (&devices[start..], false, None)
        };

        let members: String = page
            .iter()
            .map(|d| {
                let user_xml = d
                    .user
                    .as_ref()
                    .and_then(|uname| {
                        state.users.get(uname).map(|u| {
                            let tags_xml = if u.tags.is_empty() { String::new() } else {
                                let tm: String = u.tags.iter().map(|t| format!(
                                    "\n              <member>\n                <Key>{}</Key>\n                <Value>{}</Value>\n              </member>", t.key, t.value
                                )).collect::<Vec<_>>().join("");
                                format!("\n          <Tags>{}\n          </Tags>", tm)
                            };
                            format!(
                                "\n        <User>\n          <Path>{}</Path>\n          <UserName>{}</UserName>\n          <UserId>{}</UserId>\n          <Arn>{}</Arn>\n          <CreateDate>{}</CreateDate>{}\n        </User>",
                                u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ"), tags_xml
                            )
                        })
                    })
                    .unwrap_or_default();
                let enable_date = d
                    .enable_date
                    .map(|dt| {
                        format!(
                            "\n        <EnableDate>{}</EnableDate>",
                            dt.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .unwrap_or_default();
                format!(
                    "      <member>\n        <SerialNumber>{}</SerialNumber>{user_xml}{enable_date}\n      </member>",
                    d.serial_number,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let marker_xml = next_marker
            .map(|m| format!("\n    <Marker>{}</Marker>", m))
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVirtualMFADevicesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListVirtualMFADevicesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_xml}
    <VirtualMFADevices>
{members}
    </VirtualMFADevices>
  </ListVirtualMFADevicesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListVirtualMFADevicesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn enable_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let serial_number = required_param(&req.query_params, "SerialNumber")?;
        let _code1 = required_param(&req.query_params, "AuthenticationCode1")?;
        let _code2 = required_param(&req.query_params, "AuthenticationCode2")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        // Support both virtual MFA devices and hardware/arbitrary serial numbers
        if let Some(device) = state.virtual_mfa_devices.get_mut(&serial_number) {
            device.user = Some(user_name);
            device.enable_date = Some(Utc::now());
        } else {
            let device = VirtualMfaDevice {
                serial_number: serial_number.clone(),
                base32_string_seed: String::new(),
                qr_code_png: String::new(),
                enable_date: Some(Utc::now()),
                user: Some(user_name),
                tags: Vec::new(),
            };
            state.virtual_mfa_devices.insert(serial_number, device);
        }

        let xml = empty_response("EnableMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn deactivate_mfa_device(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _user_name = required_param(&req.query_params, "UserName")?;
        let serial_number = required_param(&req.query_params, "SerialNumber")?;

        let mut state = self.state.write();

        if let Some(device) = state.virtual_mfa_devices.get_mut(&serial_number) {
            device.user = None;
            device.enable_date = None;
        }

        let xml = empty_response("DeactivateMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_mfa_devices(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        let devices: Vec<&VirtualMfaDevice> = state
            .virtual_mfa_devices
            .values()
            .filter(|d| d.user.as_deref() == Some(&user_name))
            .collect();

        let members: String = devices
            .iter()
            .map(|d| {
                let enable_date = d
                    .enable_date
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_default();
                format!(
                    "      <member>\n        <SerialNumber>{}</SerialNumber>\n        <UserName>{}</UserName>\n        <EnableDate>{}</EnableDate>\n      </member>",
                    d.serial_number, user_name, enable_date,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMFADevicesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListMFADevicesResult>
    <IsTruncated>false</IsTruncated>
    <MFADevices>
{members}
    </MFADevices>
  </ListMFADevicesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListMFADevicesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

fn is_valid_iam_path(path: &str) -> bool {
    if !path.starts_with('/') || !path.ends_with('/') {
        return false;
    }
    if path.contains("//") {
        return false;
    }
    if path.len() > 512 {
        return false;
    }
    path.chars().all(|c| {
        c.is_alphanumeric()
            || c == '/'
            || c == '-'
            || c == '_'
            || c == '.'
            || c == '+'
            || c == '='
            || c == '@'
            || c == ','
    })
}
