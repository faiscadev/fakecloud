use base64::Engine;

use crate::state::{IamAccessKey, IamPolicy, IamRole, IamUser};

pub fn create_user_response(user: &IamUser, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateUserResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateUserResult>
    <User>
      <Path>{path}</Path>
      <UserName>{name}</UserName>
      <UserId>{id}</UserId>
      <Arn>{arn}</Arn>
      <CreateDate>{date}</CreateDate>
    </User>
  </CreateUserResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</CreateUserResponse>"#,
        path = user.path,
        name = user.user_name,
        id = user.user_id,
        arn = user.arn,
        date = user.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        request_id = request_id,
    )
}

pub fn get_user_response(user: &IamUser, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetUserResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetUserResult>
    <User>
      <Path>{path}</Path>
      <UserName>{name}</UserName>
      <UserId>{id}</UserId>
      <Arn>{arn}</Arn>
      <CreateDate>{date}</CreateDate>
    </User>
  </GetUserResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetUserResponse>"#,
        path = user.path,
        name = user.user_name,
        id = user.user_id,
        arn = user.arn,
        date = user.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        request_id = request_id,
    )
}

pub fn delete_user_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteUserResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DeleteUserResponse>"#,
        request_id = request_id,
    )
}

pub fn list_users_response(users: &[IamUser], request_id: &str) -> String {
    let members: String = users
        .iter()
        .map(|u| {
            format!(
                r#"      <member>
        <Path>{path}</Path>
        <UserName>{name}</UserName>
        <UserId>{id}</UserId>
        <Arn>{arn}</Arn>
        <CreateDate>{date}</CreateDate>
      </member>"#,
                path = u.path,
                name = u.user_name,
                id = u.user_id,
                arn = u.arn,
                date = u.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListUsersResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListUsersResult>
    <IsTruncated>false</IsTruncated>
    <Users>
{members}
    </Users>
  </ListUsersResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListUsersResponse>"#,
        members = members,
        request_id = request_id,
    )
}

pub fn create_access_key_response(key: &IamAccessKey, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateAccessKeyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateAccessKeyResult>
    <AccessKey>
      <UserName>{user}</UserName>
      <AccessKeyId>{key_id}</AccessKeyId>
      <Status>{status}</Status>
      <SecretAccessKey>{secret}</SecretAccessKey>
      <CreateDate>{date}</CreateDate>
    </AccessKey>
  </CreateAccessKeyResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</CreateAccessKeyResponse>"#,
        user = key.user_name,
        key_id = key.access_key_id,
        status = key.status,
        secret = key.secret_access_key,
        date = key.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        request_id = request_id,
    )
}

pub fn delete_access_key_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteAccessKeyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DeleteAccessKeyResponse>"#,
        request_id = request_id,
    )
}

pub fn list_access_keys_response(
    keys: &[IamAccessKey],
    user_name: &str,
    request_id: &str,
) -> String {
    let members: String = keys
        .iter()
        .map(|k| {
            format!(
                r#"      <member>
        <UserName>{user}</UserName>
        <AccessKeyId>{key_id}</AccessKeyId>
        <Status>{status}</Status>
        <CreateDate>{date}</CreateDate>
      </member>"#,
                user = k.user_name,
                key_id = k.access_key_id,
                status = k.status,
                date = k.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAccessKeysResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAccessKeysResult>
    <UserName>{user_name}</UserName>
    <IsTruncated>false</IsTruncated>
    <AccessKeyMetadata>
{members}
    </AccessKeyMetadata>
  </ListAccessKeysResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListAccessKeysResponse>"#,
        user_name = user_name,
        members = members,
        request_id = request_id,
    )
}

pub fn create_role_response(role: &IamRole, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateRoleResult>
    <Role>
      <Path>{path}</Path>
      <RoleName>{name}</RoleName>
      <RoleId>{id}</RoleId>
      <Arn>{arn}</Arn>
      <CreateDate>{date}</CreateDate>
      <AssumeRolePolicyDocument>{policy}</AssumeRolePolicyDocument>
    </Role>
  </CreateRoleResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</CreateRoleResponse>"#,
        path = role.path,
        name = role.role_name,
        id = role.role_id,
        arn = role.arn,
        date = role.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        policy = xml_escape(&role.assume_role_policy_document),
        request_id = request_id,
    )
}

pub fn get_role_response(role: &IamRole, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetRoleResult>
    <Role>
      <Path>{path}</Path>
      <RoleName>{name}</RoleName>
      <RoleId>{id}</RoleId>
      <Arn>{arn}</Arn>
      <CreateDate>{date}</CreateDate>
      <AssumeRolePolicyDocument>{policy}</AssumeRolePolicyDocument>
    </Role>
  </GetRoleResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetRoleResponse>"#,
        path = role.path,
        name = role.role_name,
        id = role.role_id,
        arn = role.arn,
        date = role.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        policy = xml_escape(&role.assume_role_policy_document),
        request_id = request_id,
    )
}

pub fn delete_role_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DeleteRoleResponse>"#,
        request_id = request_id,
    )
}

pub fn list_roles_response(roles: &[IamRole], request_id: &str) -> String {
    let members: String = roles
        .iter()
        .map(|r| {
            format!(
                r#"      <member>
        <Path>{path}</Path>
        <RoleName>{name}</RoleName>
        <RoleId>{id}</RoleId>
        <Arn>{arn}</Arn>
        <CreateDate>{date}</CreateDate>
        <AssumeRolePolicyDocument>{policy}</AssumeRolePolicyDocument>
      </member>"#,
                path = r.path,
                name = r.role_name,
                id = r.role_id,
                arn = r.arn,
                date = r.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                policy = xml_escape(&r.assume_role_policy_document),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListRolesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListRolesResult>
    <IsTruncated>false</IsTruncated>
    <Roles>
{members}
    </Roles>
  </ListRolesResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListRolesResponse>"#,
        members = members,
        request_id = request_id,
    )
}

pub fn create_policy_response(policy: &IamPolicy, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreatePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreatePolicyResult>
    <Policy>
      <PolicyName>{name}</PolicyName>
      <PolicyId>{id}</PolicyId>
      <Arn>{arn}</Arn>
      <Path>{path}</Path>
      <CreateDate>{date}</CreateDate>
    </Policy>
  </CreatePolicyResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</CreatePolicyResponse>"#,
        name = policy.policy_name,
        id = policy.policy_id,
        arn = policy.arn,
        path = policy.path,
        date = policy.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        request_id = request_id,
    )
}

pub fn list_policies_response(policies: &[IamPolicy], request_id: &str) -> String {
    let members: String = policies
        .iter()
        .map(|p| {
            format!(
                r#"      <member>
        <PolicyName>{name}</PolicyName>
        <PolicyId>{id}</PolicyId>
        <Arn>{arn}</Arn>
        <Path>{path}</Path>
        <CreateDate>{date}</CreateDate>
      </member>"#,
                name = p.policy_name,
                id = p.policy_id,
                arn = p.arn,
                path = p.path,
                date = p.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListPoliciesResult>
    <IsTruncated>false</IsTruncated>
    <Policies>
{members}
    </Policies>
  </ListPoliciesResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListPoliciesResponse>"#,
        members = members,
        request_id = request_id,
    )
}

pub fn attach_role_policy_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AttachRolePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AttachRolePolicyResponse>"#,
        request_id = request_id,
    )
}

pub fn get_policy_response(policy: &IamPolicy, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetPolicyResult>
    <Policy>
      <PolicyName>{name}</PolicyName>
      <PolicyId>{id}</PolicyId>
      <Arn>{arn}</Arn>
      <Path>{path}</Path>
      <CreateDate>{date}</CreateDate>
      <AttachmentCount>0</AttachmentCount>
      <IsAttachable>true</IsAttachable>
      <DefaultVersionId>v1</DefaultVersionId>
    </Policy>
  </GetPolicyResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetPolicyResponse>"#,
        name = policy.policy_name,
        id = policy.policy_id,
        arn = policy.arn,
        path = policy.path,
        date = policy.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        request_id = request_id,
    )
}

pub fn delete_policy_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeletePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DeletePolicyResponse>"#,
        request_id = request_id,
    )
}

pub fn list_role_policies_response(policy_names: &[String], request_id: &str) -> String {
    let members: String = policy_names
        .iter()
        .map(|name| format!("      <member>{name}</member>"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListRolePoliciesResult>
    <IsTruncated>false</IsTruncated>
    <PolicyNames>
{members}
    </PolicyNames>
  </ListRolePoliciesResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</ListRolePoliciesResponse>"#,
        members = members,
        request_id = request_id,
    )
}

pub fn detach_role_policy_response(request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DetachRolePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</DetachRolePolicyResponse>"#,
        request_id = request_id,
    )
}

pub fn get_caller_identity_response(
    account_id: &str,
    arn: &str,
    user_id: &str,
    request_id: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>{arn}</Arn>
    <UserId>{user_id}</UserId>
    <Account>{account_id}</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>"#,
        arn = arn,
        user_id = user_id,
        account_id = account_id,
        request_id = request_id,
    )
}

/// Pre-generated STS credentials to be returned in XML responses.
pub struct StsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

impl StsCredentials {
    pub fn generate() -> Self {
        Self {
            access_key_id: generate_access_key_id(),
            secret_access_key: generate_secret_access_key(),
            session_token: generate_session_token(),
        }
    }
}

pub fn assume_role_response(
    role_arn: &str,
    role_session_name: &str,
    role_id: &str,
    account_id: &str,
    partition: &str,
    creds: &StsCredentials,
    request_id: &str,
) -> String {
    // Extract role name from ARN
    let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
    let assumed_role_arn = format!(
        "arn:{}:sts::{}:assumed-role/{}/{}",
        partition, account_id, role_name, role_session_name
    );

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>{access_key_id}</AccessKeyId>
      <SecretAccessKey>{secret_access_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2099-12-31T23:59:59Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <AssumedRoleId>{role_id}:{session}</AssumedRoleId>
      <Arn>{assumed_role_arn}</Arn>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>"#,
        access_key_id = creds.access_key_id,
        secret_access_key = creds.secret_access_key,
        session_token = creds.session_token,
        role_id = role_id,
        assumed_role_arn = assumed_role_arn,
        session = role_session_name,
        request_id = request_id,
    )
}

pub fn assume_role_with_web_identity_response(
    role_arn: &str,
    role_session_name: &str,
    account_id: &str,
    partition: &str,
    creds: &StsCredentials,
    assumed_role_id: &str,
    request_id: &str,
) -> String {
    let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
    let assumed_role_arn = format!(
        "arn:{}:sts::{}:assumed-role/{}/{}",
        partition, account_id, role_name, role_session_name
    );

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>{access_key_id}</AccessKeyId>
      <SecretAccessKey>{secret_access_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2099-12-31T23:59:59Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <AssumedRoleId>{assumed_role_id}:{session}</AssumedRoleId>
      <Arn>{assumed_role_arn}</Arn>
    </AssumedRoleUser>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>"#,
        access_key_id = creds.access_key_id,
        secret_access_key = creds.secret_access_key,
        session_token = creds.session_token,
        assumed_role_id = assumed_role_id,
        assumed_role_arn = assumed_role_arn,
        session = role_session_name,
        request_id = request_id,
    )
}

pub fn assume_role_with_saml_response(
    role_arn: &str,
    role_session_name: &str,
    account_id: &str,
    partition: &str,
    creds: &StsCredentials,
    assumed_role_id: &str,
    request_id: &str,
) -> String {
    let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
    let assumed_role_arn = format!(
        "arn:{}:sts::{}:assumed-role/{}/{}",
        partition, account_id, role_name, role_session_name
    );

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithSAMLResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithSAMLResult>
    <Credentials>
      <AccessKeyId>{access_key_id}</AccessKeyId>
      <SecretAccessKey>{secret_access_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2099-12-31T23:59:59Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <AssumedRoleId>{assumed_role_id}:{session}</AssumedRoleId>
      <Arn>{assumed_role_arn}</Arn>
    </AssumedRoleUser>
  </AssumeRoleWithSAMLResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleWithSAMLResponse>"#,
        access_key_id = creds.access_key_id,
        secret_access_key = creds.secret_access_key,
        session_token = creds.session_token,
        assumed_role_id = assumed_role_id,
        assumed_role_arn = assumed_role_arn,
        session = role_session_name,
        request_id = request_id,
    )
}

pub fn get_session_token_response(request_id: &str) -> String {
    // AWS docs example credentials (deterministic for local testing)
    let access_key_id = "ASIAIOSFODNN7EXAMPLE";
    let secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY";
    let session_token = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3zrkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtpZ3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE";

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetSessionTokenResult>
    <Credentials>
      <AccessKeyId>{access_key_id}</AccessKeyId>
      <SecretAccessKey>{secret_access_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2099-12-31T23:59:59Z</Expiration>
    </Credentials>
  </GetSessionTokenResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetSessionTokenResponse>"#,
        access_key_id = access_key_id,
        secret_access_key = secret_access_key,
        session_token = session_token,
        request_id = request_id,
    )
}

pub fn get_federation_token_response(
    name: &str,
    account_id: &str,
    partition: &str,
    request_id: &str,
) -> String {
    // AWS docs example credentials (deterministic for local testing)
    let access_key_id = "ASIAIOSFODNN7EXAMPLE";
    let secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY";
    let session_token = "AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQWLWsKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGdQrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==";

    let name = xml_escape(name);
    let federated_user_arn = format!(
        "arn:{}:sts::{}:federated-user/{}",
        partition, account_id, name
    );
    let federated_user_id = format!("{}:{}", account_id, name);

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetFederationTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetFederationTokenResult>
    <Credentials>
      <AccessKeyId>{access_key_id}</AccessKeyId>
      <SecretAccessKey>{secret_access_key}</SecretAccessKey>
      <SessionToken>{session_token}</SessionToken>
      <Expiration>2099-12-31T23:59:59Z</Expiration>
    </Credentials>
    <FederatedUser>
      <FederatedUserId>{federated_user_id}</FederatedUserId>
      <Arn>{federated_user_arn}</Arn>
    </FederatedUser>
  </GetFederationTokenResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetFederationTokenResponse>"#,
        access_key_id = access_key_id,
        secret_access_key = secret_access_key,
        session_token = session_token,
        federated_user_arn = federated_user_arn,
        federated_user_id = federated_user_id,
        request_id = request_id,
    )
}

pub fn get_access_key_info_response(account_id: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccessKeyInfoResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetAccessKeyInfoResult>
    <Account>{account_id}</Account>
  </GetAccessKeyInfoResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</GetAccessKeyInfoResponse>"#,
        account_id = account_id,
        request_id = request_id,
    )
}

/// Generate an ASIA-prefixed access key ID (20 chars total).
pub fn generate_access_key_id() -> String {
    let id = generate_alphanum_id(16);
    format!("ASIA{}", id)
}

/// Generate a 40-character secret access key.
pub fn generate_secret_access_key() -> String {
    generate_alphanum_id(40)
}

/// Generate an AROA-prefixed role ID (21 chars total).
pub fn generate_role_id() -> String {
    let id = generate_alphanum_id(17);
    format!("AROA{}", id)
}

/// Generate a session token that is exactly 356 characters starting with "FQoGZXIvYXdzE".
pub fn generate_session_token() -> String {
    // AWS session tokens are typically 356 chars and start with "FQoGZXIvYXdzE"
    let prefix = "FQoGZXIvYXdzE";
    let remaining = 356 - prefix.len(); // 343 chars needed
                                        // Generate enough random bytes: we need at least ceil(343*3/4) = 258 bytes
                                        // 18 UUIDs * 16 bytes = 288 bytes -> base64 = 384 chars (plenty)
    let mut raw = Vec::with_capacity(288);
    for _ in 0..18 {
        raw.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    // Take exactly what we need from the encoded data
    let suffix = &encoded[..remaining];
    format!("{}{}", prefix, suffix)
}

/// Generate alphanumeric ID of given length.
fn generate_alphanum_id(len: usize) -> String {
    let raw = format!(
        "{}{}{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    );
    raw.replace('-', "")
        .chars()
        .filter(|c| c.is_alphanumeric())
        .take(len)
        .collect()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
