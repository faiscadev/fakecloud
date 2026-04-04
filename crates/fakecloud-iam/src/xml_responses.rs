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

pub fn assume_role_response(arn: &str, role_session_name: &str, request_id: &str) -> String {
    let access_key_id = format!("ASIA{}", generate_id());
    let secret_access_key = format!("fakecloud/{}", uuid::Uuid::new_v4());
    let session_token = generate_session_token();

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
      <AssumedRoleId>AROA3XFRBF23EXAMPLE:{session}</AssumedRoleId>
      <Arn>{arn}</Arn>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>"#,
        access_key_id = access_key_id,
        secret_access_key = secret_access_key,
        session_token = session_token,
        arn = arn,
        session = role_session_name,
        request_id = request_id,
    )
}

fn generate_id() -> String {
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..16]
        .to_string()
}

fn generate_session_token() -> String {
    let raw = format!(
        "{}{}{}{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    );
    base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
