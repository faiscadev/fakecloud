mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ==========================================================================
// Users
// ==========================================================================

#[test_action("iam", "CreateUser", checksum = "f44a86b8")]
#[test_action("iam", "GetUser", checksum = "9f274efe")]
#[test_action("iam", "ListUsers", checksum = "646fd37f")]
#[test_action("iam", "UpdateUser", checksum = "feb72967")]
#[test_action("iam", "DeleteUser", checksum = "eb9be363")]
#[tokio::test]
async fn iam_user_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-user")
        .send()
        .await
        .unwrap();
    let get = client
        .get_user()
        .user_name("conf-user")
        .send()
        .await
        .unwrap();
    assert_eq!(get.user().unwrap().user_name(), "conf-user");

    let list = client.list_users().send().await.unwrap();
    assert!(!list.users().is_empty());

    client
        .update_user()
        .user_name("conf-user")
        .new_path("/updated/")
        .send()
        .await
        .unwrap();

    client
        .delete_user()
        .user_name("conf-user")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "TagUser", checksum = "91309f3d")]
#[test_action("iam", "UntagUser", checksum = "2c1fc62d")]
#[test_action("iam", "ListUserTags", checksum = "ae73fe03")]
#[tokio::test]
async fn iam_user_tags() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-utag")
        .send()
        .await
        .unwrap();

    client
        .tag_user()
        .user_name("conf-utag")
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_user_tags()
        .user_name("conf-utag")
        .send()
        .await
        .unwrap();
    assert!(!resp.tags().is_empty());

    client
        .untag_user()
        .user_name("conf-utag")
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Access keys
// ==========================================================================

#[test_action("iam", "CreateAccessKey", checksum = "079ca956")]
#[test_action("iam", "ListAccessKeys", checksum = "35b71bcf")]
#[test_action("iam", "UpdateAccessKey", checksum = "c8cf3d9f")]
#[test_action("iam", "GetAccessKeyLastUsed", checksum = "8470b24f")]
#[test_action("iam", "DeleteAccessKey", checksum = "25b278a4")]
#[tokio::test]
async fn iam_access_key_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-ak")
        .send()
        .await
        .unwrap();

    let create = client
        .create_access_key()
        .user_name("conf-ak")
        .send()
        .await
        .unwrap();
    let key_id = create.access_key().unwrap().access_key_id().to_string();

    let list = client
        .list_access_keys()
        .user_name("conf-ak")
        .send()
        .await
        .unwrap();
    assert!(!list.access_key_metadata().is_empty());

    client
        .update_access_key()
        .user_name("conf-ak")
        .access_key_id(&key_id)
        .status(aws_sdk_iam::types::StatusType::Inactive)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_access_key_last_used()
        .access_key_id(&key_id)
        .send()
        .await
        .unwrap();

    client
        .delete_access_key()
        .user_name("conf-ak")
        .access_key_id(&key_id)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Roles
// ==========================================================================

const ASSUME_ROLE_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

#[test_action("iam", "CreateRole", checksum = "873f52f2")]
#[test_action("iam", "GetRole", checksum = "eb87506d")]
#[test_action("iam", "ListRoles", checksum = "65174afc")]
#[test_action("iam", "UpdateRole", checksum = "4ef4a056")]
#[test_action("iam", "UpdateRoleDescription", checksum = "b7ded596")]
#[test_action("iam", "UpdateAssumeRolePolicy", checksum = "2097f40b")]
#[test_action("iam", "DeleteRole", checksum = "13b863d4")]
#[tokio::test]
async fn iam_role_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-role")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    let get = client
        .get_role()
        .role_name("conf-role")
        .send()
        .await
        .unwrap();
    assert_eq!(get.role().unwrap().role_name(), "conf-role");

    let list = client.list_roles().send().await.unwrap();
    assert!(!list.roles().is_empty());

    client
        .update_role()
        .role_name("conf-role")
        .max_session_duration(7200)
        .send()
        .await
        .unwrap();

    client
        .update_role_description()
        .role_name("conf-role")
        .description("updated desc")
        .send()
        .await
        .unwrap();

    client
        .update_assume_role_policy()
        .role_name("conf-role")
        .policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    client
        .delete_role()
        .role_name("conf-role")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "TagRole", checksum = "28966142")]
#[test_action("iam", "UntagRole", checksum = "58291cdb")]
#[test_action("iam", "ListRoleTags", checksum = "61151908")]
#[tokio::test]
async fn iam_role_tags() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-rtag")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    client
        .tag_role()
        .role_name("conf-rtag")
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_role_tags()
        .role_name("conf-rtag")
        .send()
        .await
        .unwrap();
    assert!(!resp.tags().is_empty());

    client
        .untag_role()
        .role_name("conf-rtag")
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "PutRolePermissionsBoundary", checksum = "02a1078a")]
#[test_action("iam", "DeleteRolePermissionsBoundary", checksum = "a718c0a3")]
#[tokio::test]
async fn iam_role_permissions_boundary() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-rpb")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    client
        .put_role_permissions_boundary()
        .role_name("conf-rpb")
        .permissions_boundary("arn:aws:iam::aws:policy/PowerUserAccess")
        .send()
        .await
        .unwrap();

    client
        .delete_role_permissions_boundary()
        .role_name("conf-rpb")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Policies
// ==========================================================================

const POLICY_DOC: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

#[test_action("iam", "CreatePolicy", checksum = "e2b8e9ad")]
#[test_action("iam", "GetPolicy", checksum = "070be7a5")]
#[test_action("iam", "ListPolicies", checksum = "b374d17a")]
#[test_action("iam", "DeletePolicy", checksum = "64b85f27")]
#[tokio::test]
async fn iam_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_policy()
        .policy_name("conf-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = create.policy().unwrap().arn().unwrap().to_string();

    let _ = client.get_policy().policy_arn(&arn).send().await.unwrap();
    let _ = client.list_policies().send().await.unwrap();

    client
        .delete_policy()
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "TagPolicy", checksum = "0847d985")]
#[test_action("iam", "UntagPolicy", checksum = "1640c997")]
#[test_action("iam", "ListPolicyTags", checksum = "80031082")]
#[tokio::test]
async fn iam_policy_tags() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_policy()
        .policy_name("conf-ptag")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = create.policy().unwrap().arn().unwrap().to_string();

    client
        .tag_policy()
        .policy_arn(&arn)
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_policy_tags()
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.tags().is_empty());

    client
        .untag_policy()
        .policy_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Policy versions
// ==========================================================================

#[test_action("iam", "CreatePolicyVersion", checksum = "ae5732df")]
#[test_action("iam", "GetPolicyVersion", checksum = "c753f09f")]
#[test_action("iam", "ListPolicyVersions", checksum = "e55b368d")]
#[test_action("iam", "SetDefaultPolicyVersion", checksum = "af99b113")]
#[test_action("iam", "DeletePolicyVersion", checksum = "f1edba4b")]
#[tokio::test]
async fn iam_policy_versions() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_policy()
        .policy_name("conf-pver")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = create.policy().unwrap().arn().unwrap().to_string();

    let v2_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
    let v2 = client
        .create_policy_version()
        .policy_arn(&arn)
        .policy_document(v2_doc)
        .send()
        .await
        .unwrap();
    let v2_id = v2
        .policy_version()
        .unwrap()
        .version_id()
        .unwrap()
        .to_string();

    let _ = client
        .get_policy_version()
        .policy_arn(&arn)
        .version_id(&v2_id)
        .send()
        .await
        .unwrap();

    let _ = client
        .list_policy_versions()
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();

    client
        .set_default_policy_version()
        .policy_arn(&arn)
        .version_id(&v2_id)
        .send()
        .await
        .unwrap();

    client
        .delete_policy_version()
        .policy_arn(&arn)
        .version_id("v1")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Role policy attachments (managed)
// ==========================================================================

#[test_action("iam", "AttachRolePolicy", checksum = "e0fb047c")]
#[test_action("iam", "ListAttachedRolePolicies", checksum = "f1e6276f")]
#[test_action("iam", "DetachRolePolicy", checksum = "07cfd4d3")]
#[tokio::test]
async fn iam_role_managed_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-rmp")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    let pol = client
        .create_policy()
        .policy_name("conf-rmp-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = pol.policy().unwrap().arn().unwrap().to_string();

    client
        .attach_role_policy()
        .role_name("conf-rmp")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();

    let list = client
        .list_attached_role_policies()
        .role_name("conf-rmp")
        .send()
        .await
        .unwrap();
    assert!(!list.attached_policies().is_empty());

    client
        .detach_role_policy()
        .role_name("conf-rmp")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Role inline policies
// ==========================================================================

#[test_action("iam", "PutRolePolicy", checksum = "3791b2d7")]
#[test_action("iam", "GetRolePolicy", checksum = "2063170e")]
#[test_action("iam", "ListRolePolicies", checksum = "24b7aa94")]
#[test_action("iam", "DeleteRolePolicy", checksum = "af6cd576")]
#[tokio::test]
async fn iam_role_inline_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-rip")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();

    client
        .put_role_policy()
        .role_name("conf-rip")
        .policy_name("inline1")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_role_policy()
        .role_name("conf-rip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();

    let list = client
        .list_role_policies()
        .role_name("conf-rip")
        .send()
        .await
        .unwrap();
    assert!(!list.policy_names().is_empty());

    client
        .delete_role_policy()
        .role_name("conf-rip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// User policy attachments (managed)
// ==========================================================================

#[test_action("iam", "AttachUserPolicy", checksum = "a1b9fc5e")]
#[test_action("iam", "ListAttachedUserPolicies", checksum = "dad611b0")]
#[test_action("iam", "DetachUserPolicy", checksum = "1f18da48")]
#[tokio::test]
async fn iam_user_managed_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-ump")
        .send()
        .await
        .unwrap();
    let pol = client
        .create_policy()
        .policy_name("conf-ump-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = pol.policy().unwrap().arn().unwrap().to_string();

    client
        .attach_user_policy()
        .user_name("conf-ump")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();

    let list = client
        .list_attached_user_policies()
        .user_name("conf-ump")
        .send()
        .await
        .unwrap();
    assert!(!list.attached_policies().is_empty());

    client
        .detach_user_policy()
        .user_name("conf-ump")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// User inline policies
// ==========================================================================

#[test_action("iam", "PutUserPolicy", checksum = "245e5162")]
#[test_action("iam", "GetUserPolicy", checksum = "f938baca")]
#[test_action("iam", "ListUserPolicies", checksum = "17893ece")]
#[test_action("iam", "DeleteUserPolicy", checksum = "45fbae53")]
#[tokio::test]
async fn iam_user_inline_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-uip")
        .send()
        .await
        .unwrap();

    client
        .put_user_policy()
        .user_name("conf-uip")
        .policy_name("inline1")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_user_policy()
        .user_name("conf-uip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();

    let list = client
        .list_user_policies()
        .user_name("conf-uip")
        .send()
        .await
        .unwrap();
    assert!(!list.policy_names().is_empty());

    client
        .delete_user_policy()
        .user_name("conf-uip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Groups
// ==========================================================================

#[test_action("iam", "CreateGroup", checksum = "b121af2a")]
#[test_action("iam", "GetGroup", checksum = "b9ba9cba")]
#[test_action("iam", "ListGroups", checksum = "4bbbd522")]
#[test_action("iam", "UpdateGroup", checksum = "3e229237")]
#[test_action("iam", "DeleteGroup", checksum = "1beb602c")]
#[tokio::test]
async fn iam_group_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("conf-grp")
        .send()
        .await
        .unwrap();

    let get = client
        .get_group()
        .group_name("conf-grp")
        .send()
        .await
        .unwrap();
    assert_eq!(get.group().unwrap().group_name(), "conf-grp");

    let list = client.list_groups().send().await.unwrap();
    assert!(!list.groups().is_empty());

    client
        .update_group()
        .group_name("conf-grp")
        .new_group_name("conf-grp-new")
        .send()
        .await
        .unwrap();

    client
        .delete_group()
        .group_name("conf-grp-new")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Group membership
// ==========================================================================

#[test_action("iam", "AddUserToGroup", checksum = "d0cb9ba4")]
#[test_action("iam", "RemoveUserFromGroup", checksum = "a7074802")]
#[test_action("iam", "ListGroupsForUser", checksum = "8d424afe")]
#[tokio::test]
async fn iam_group_membership() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("conf-gm")
        .send()
        .await
        .unwrap();
    client
        .create_user()
        .user_name("conf-gm-u")
        .send()
        .await
        .unwrap();

    client
        .add_user_to_group()
        .group_name("conf-gm")
        .user_name("conf-gm-u")
        .send()
        .await
        .unwrap();

    let list = client
        .list_groups_for_user()
        .user_name("conf-gm-u")
        .send()
        .await
        .unwrap();
    assert!(!list.groups().is_empty());

    client
        .remove_user_from_group()
        .group_name("conf-gm")
        .user_name("conf-gm-u")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Group inline policies
// ==========================================================================

#[test_action("iam", "PutGroupPolicy", checksum = "8b0be12d")]
#[test_action("iam", "GetGroupPolicy", checksum = "ec2e696a")]
#[test_action("iam", "ListGroupPolicies", checksum = "f25fa3be")]
#[test_action("iam", "DeleteGroupPolicy", checksum = "3cc368db")]
#[tokio::test]
async fn iam_group_inline_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("conf-gip")
        .send()
        .await
        .unwrap();

    client
        .put_group_policy()
        .group_name("conf-gip")
        .policy_name("inline1")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_group_policy()
        .group_name("conf-gip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();

    let list = client
        .list_group_policies()
        .group_name("conf-gip")
        .send()
        .await
        .unwrap();
    assert!(!list.policy_names().is_empty());

    client
        .delete_group_policy()
        .group_name("conf-gip")
        .policy_name("inline1")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Group managed policies
// ==========================================================================

#[test_action("iam", "AttachGroupPolicy", checksum = "a8bf637b")]
#[test_action("iam", "ListAttachedGroupPolicies", checksum = "2deb2525")]
#[test_action("iam", "DetachGroupPolicy", checksum = "01cb55f3")]
#[tokio::test]
async fn iam_group_managed_policies() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_group()
        .group_name("conf-gmp")
        .send()
        .await
        .unwrap();
    let pol = client
        .create_policy()
        .policy_name("conf-gmp-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = pol.policy().unwrap().arn().unwrap().to_string();

    client
        .attach_group_policy()
        .group_name("conf-gmp")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();

    let list = client
        .list_attached_group_policies()
        .group_name("conf-gmp")
        .send()
        .await
        .unwrap();
    assert!(!list.attached_policies().is_empty());

    client
        .detach_group_policy()
        .group_name("conf-gmp")
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Instance profiles
// ==========================================================================

#[test_action("iam", "CreateInstanceProfile", checksum = "55d4f12f")]
#[test_action("iam", "GetInstanceProfile", checksum = "dc894f55")]
#[test_action("iam", "ListInstanceProfiles", checksum = "73fb3093")]
#[test_action("iam", "DeleteInstanceProfile", checksum = "0bcced85")]
#[tokio::test]
async fn iam_instance_profile_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_instance_profile()
        .instance_profile_name("conf-ip")
        .send()
        .await
        .unwrap();

    let _ = client
        .get_instance_profile()
        .instance_profile_name("conf-ip")
        .send()
        .await
        .unwrap();

    let _ = client.list_instance_profiles().send().await.unwrap();

    client
        .delete_instance_profile()
        .instance_profile_name("conf-ip")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "AddRoleToInstanceProfile", checksum = "d91b8859")]
#[test_action("iam", "RemoveRoleFromInstanceProfile", checksum = "db70911c")]
#[test_action("iam", "ListInstanceProfilesForRole", checksum = "62799439")]
#[tokio::test]
async fn iam_instance_profile_role() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_role()
        .role_name("conf-ipr")
        .assume_role_policy_document(ASSUME_ROLE_POLICY)
        .send()
        .await
        .unwrap();
    client
        .create_instance_profile()
        .instance_profile_name("conf-ipr")
        .send()
        .await
        .unwrap();

    client
        .add_role_to_instance_profile()
        .instance_profile_name("conf-ipr")
        .role_name("conf-ipr")
        .send()
        .await
        .unwrap();

    let list = client
        .list_instance_profiles_for_role()
        .role_name("conf-ipr")
        .send()
        .await
        .unwrap();
    assert!(!list.instance_profiles().is_empty());

    client
        .remove_role_from_instance_profile()
        .instance_profile_name("conf-ipr")
        .role_name("conf-ipr")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "TagInstanceProfile", checksum = "76a884be")]
#[test_action("iam", "UntagInstanceProfile", checksum = "b851bbcb")]
#[test_action("iam", "ListInstanceProfileTags", checksum = "b40cbfd1")]
#[tokio::test]
async fn iam_instance_profile_tags() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_instance_profile()
        .instance_profile_name("conf-ipt")
        .send()
        .await
        .unwrap();

    client
        .tag_instance_profile()
        .instance_profile_name("conf-ipt")
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_instance_profile_tags()
        .instance_profile_name("conf-ipt")
        .send()
        .await
        .unwrap();
    assert!(!resp.tags().is_empty());

    client
        .untag_instance_profile()
        .instance_profile_name("conf-ipt")
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Login profiles
// ==========================================================================

#[test_action("iam", "CreateLoginProfile", checksum = "781c029f")]
#[test_action("iam", "GetLoginProfile", checksum = "a7696b03")]
#[test_action("iam", "UpdateLoginProfile", checksum = "04b34262")]
#[test_action("iam", "DeleteLoginProfile", checksum = "0f968393")]
#[tokio::test]
async fn iam_login_profile() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-lp")
        .send()
        .await
        .unwrap();

    client
        .create_login_profile()
        .user_name("conf-lp")
        .password("P@ssw0rd123!")
        .send()
        .await
        .unwrap();

    let _ = client
        .get_login_profile()
        .user_name("conf-lp")
        .send()
        .await
        .unwrap();

    client
        .update_login_profile()
        .user_name("conf-lp")
        .password("NewP@ss456!")
        .send()
        .await
        .unwrap();

    client
        .delete_login_profile()
        .user_name("conf-lp")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// SAML providers
// ==========================================================================

const SAML_METADATA: &str = r#"<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com"><IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/></IDPSSODescriptor></EntityDescriptor>"#;

#[test_action("iam", "CreateSAMLProvider", checksum = "62baff49")]
#[test_action("iam", "GetSAMLProvider", checksum = "25286183")]
#[test_action("iam", "ListSAMLProviders", checksum = "8fc561ba")]
#[test_action("iam", "UpdateSAMLProvider", checksum = "818db9ce")]
#[test_action("iam", "DeleteSAMLProvider", checksum = "c3eca04c")]
#[tokio::test]
async fn iam_saml_provider() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_saml_provider()
        .name("conf-saml")
        .saml_metadata_document(SAML_METADATA)
        .send()
        .await
        .unwrap();
    let arn = create.saml_provider_arn().unwrap().to_string();

    let _ = client
        .get_saml_provider()
        .saml_provider_arn(&arn)
        .send()
        .await
        .unwrap();

    let _ = client.list_saml_providers().send().await.unwrap();

    client
        .update_saml_provider()
        .saml_provider_arn(&arn)
        .saml_metadata_document(SAML_METADATA)
        .send()
        .await
        .unwrap();

    client
        .delete_saml_provider()
        .saml_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// OIDC providers
// ==========================================================================

#[test_action("iam", "CreateOpenIDConnectProvider", checksum = "4c2d6af3")]
#[test_action("iam", "GetOpenIDConnectProvider", checksum = "3496136f")]
#[test_action("iam", "ListOpenIDConnectProviders", checksum = "9b08e4b0")]
#[test_action("iam", "UpdateOpenIDConnectProviderThumbprint", checksum = "bdb2d121")]
#[test_action("iam", "AddClientIDToOpenIDConnectProvider", checksum = "e511cddf")]
#[test_action(
    "iam",
    "RemoveClientIDFromOpenIDConnectProvider",
    checksum = "3e1e5e4b"
)]
#[test_action("iam", "DeleteOpenIDConnectProvider", checksum = "a7564079")]
#[tokio::test]
async fn iam_oidc_provider() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_open_id_connect_provider()
        .url("https://oidc.example.com")
        .thumbprint_list("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .send()
        .await
        .unwrap();
    let arn = create.open_id_connect_provider_arn().unwrap().to_string();

    let _ = client
        .get_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();

    let _ = client
        .list_open_id_connect_providers()
        .send()
        .await
        .unwrap();

    client
        .update_open_id_connect_provider_thumbprint()
        .open_id_connect_provider_arn(&arn)
        .thumbprint_list("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        .send()
        .await
        .unwrap();

    client
        .add_client_id_to_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .client_id("my-client-id")
        .send()
        .await
        .unwrap();

    client
        .remove_client_id_from_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .client_id("my-client-id")
        .send()
        .await
        .unwrap();

    client
        .delete_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "TagOpenIDConnectProvider", checksum = "121a1ce6")]
#[test_action("iam", "UntagOpenIDConnectProvider", checksum = "84448e48")]
#[test_action("iam", "ListOpenIDConnectProviderTags", checksum = "23053130")]
#[tokio::test]
async fn iam_oidc_provider_tags() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_open_id_connect_provider()
        .url("https://oidc-tag.example.com")
        .thumbprint_list("cccccccccccccccccccccccccccccccccccccccc")
        .send()
        .await
        .unwrap();
    let arn = create.open_id_connect_provider_arn().unwrap().to_string();

    client
        .tag_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_open_id_connect_provider_tags()
        .open_id_connect_provider_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(!resp.tags().is_empty());

    client
        .untag_open_id_connect_provider()
        .open_id_connect_provider_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Server certificates
// ==========================================================================

const CERT_BODY: &str = "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJALRiMLAh6TbcMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnNl\ncnZlcjAeFw0yNTAxMDEwMDAwMDBaFw0zNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM\nBnNlcnZlcjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96HtiGPOnLZikGSMBFP\n0VHaFjmsy7NJ8L8GKwyWIqFNcGdEB4q6GMXFF+jlSmlbbQ0RGNFwyA9sVHT0x3mr\nAgMBAAEwDQYJKoZIhvcNAQELBQADQQBOWM1ZRPW0JfE4Cq5VXQEY26+gKaLOMVP\nT6fB2g90aaKrE/rnWLFBuEFLjDeRlpRH3hWsnKGG+GBnK5GSXLJN\n-----END CERTIFICATE-----\n";
const PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIBogIBAAJBALuj3oe2IY86ctmKQZIwEU/RUdoWOazLs0nwvwYrDJYioU1wZ0QH\niroYxcUX6OVKaVttDREY0XDID2xUdPTHeasCAwEAAQJABmjb3LyOY9cM6sMbCOnF\nOkEVCU4rIBaHjMxP+9RIiAt/4qDFzVQKGZ1CwnPZ5jym89b4KDQNF31FOqXvfDYQ\ngQIhAPXA3FIcfFMHRLG2QqB0cHB8LOkMJfYfEQ6H8iAWFMmjAiEAw7W/Yz7F1jCH\nfNIVHHQ1ZPdE1IsfXYPnT2MWxJAH0BECIHdq7JmA3MmGkMODAPzJ9SKVxbLTKTud\nV27zS9uIZZF1AiEArQn8GpOeSIh0noNoKHMXzkGSBflAPWc/9j7wEKAHADECIGPZ\neWRV0MyfpGMJVB5VKIFeLfp4lhXijf9MJSOY2wfc\n-----END RSA PRIVATE KEY-----\n";

#[test_action("iam", "UploadServerCertificate", checksum = "81d10b1a")]
#[test_action("iam", "GetServerCertificate", checksum = "3cd3d33d")]
#[test_action("iam", "ListServerCertificates", checksum = "3d412a65")]
#[test_action("iam", "DeleteServerCertificate", checksum = "b8623f01")]
#[tokio::test]
async fn iam_server_certificate() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .upload_server_certificate()
        .server_certificate_name("conf-cert")
        .certificate_body(CERT_BODY)
        .private_key(PRIVATE_KEY)
        .send()
        .await
        .unwrap();

    let _ = client
        .get_server_certificate()
        .server_certificate_name("conf-cert")
        .send()
        .await
        .unwrap();

    let _ = client.list_server_certificates().send().await.unwrap();

    client
        .delete_server_certificate()
        .server_certificate_name("conf-cert")
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Signing certificates
// ==========================================================================

#[test_action("iam", "UploadSigningCertificate", checksum = "297ae345")]
#[test_action("iam", "ListSigningCertificates", checksum = "d63ae181")]
#[test_action("iam", "UpdateSigningCertificate", checksum = "afb0dc00")]
#[test_action("iam", "DeleteSigningCertificate", checksum = "a1321c10")]
#[tokio::test]
async fn iam_signing_certificate() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-sc")
        .send()
        .await
        .unwrap();

    let upload = client
        .upload_signing_certificate()
        .user_name("conf-sc")
        .certificate_body(CERT_BODY)
        .send()
        .await
        .unwrap();
    let cert_id = upload.certificate().unwrap().certificate_id().to_string();

    let _ = client
        .list_signing_certificates()
        .user_name("conf-sc")
        .send()
        .await
        .unwrap();

    client
        .update_signing_certificate()
        .user_name("conf-sc")
        .certificate_id(&cert_id)
        .status(aws_sdk_iam::types::StatusType::Inactive)
        .send()
        .await
        .unwrap();

    client
        .delete_signing_certificate()
        .user_name("conf-sc")
        .certificate_id(&cert_id)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Service-linked roles
// ==========================================================================

#[test_action("iam", "CreateServiceLinkedRole", checksum = "7e8f9e97")]
#[test_action("iam", "DeleteServiceLinkedRole", checksum = "8ac7f160")]
#[test_action("iam", "GetServiceLinkedRoleDeletionStatus", checksum = "506cf566")]
#[tokio::test]
async fn iam_service_linked_role() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_service_linked_role()
        .aws_service_name("elasticloadbalancing.amazonaws.com")
        .send()
        .await
        .unwrap();

    let del = client
        .delete_service_linked_role()
        .role_name("AWSServiceRoleForElasticLoadBalancing")
        .send()
        .await
        .unwrap();
    let task_id = del.deletion_task_id().to_string();

    let _ = client
        .get_service_linked_role_deletion_status()
        .deletion_task_id(&task_id)
        .send()
        .await;
}

// ==========================================================================
// Account management
// ==========================================================================

#[test_action("iam", "GetAccountSummary", checksum = "e23c8072")]
#[test_action("iam", "GetAccountAuthorizationDetails", checksum = "a939671b")]
#[tokio::test]
async fn iam_account_info() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let _ = client.get_account_summary().send().await.unwrap();
    let _ = client
        .get_account_authorization_details()
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "CreateAccountAlias", checksum = "63d28a61")]
#[test_action("iam", "ListAccountAliases", checksum = "711a5c9f")]
#[test_action("iam", "DeleteAccountAlias", checksum = "ee61360e")]
#[tokio::test]
async fn iam_account_alias() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_account_alias()
        .account_alias("conf-alias")
        .send()
        .await
        .unwrap();

    let list = client.list_account_aliases().send().await.unwrap();
    assert!(!list.account_aliases().is_empty());

    client
        .delete_account_alias()
        .account_alias("conf-alias")
        .send()
        .await
        .unwrap();
}

#[test_action("iam", "UpdateAccountPasswordPolicy", checksum = "e8353a9a")]
#[test_action("iam", "GetAccountPasswordPolicy", checksum = "ee84923c")]
#[test_action("iam", "DeleteAccountPasswordPolicy", checksum = "2682d07c")]
#[tokio::test]
async fn iam_password_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .update_account_password_policy()
        .minimum_password_length(12)
        .require_uppercase_characters(true)
        .send()
        .await
        .unwrap();

    let _ = client.get_account_password_policy().send().await.unwrap();

    client
        .delete_account_password_policy()
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// Credential reports
// ==========================================================================

#[test_action("iam", "GenerateCredentialReport", checksum = "4795a9b9")]
#[test_action("iam", "GetCredentialReport", checksum = "3f777bd4")]
#[tokio::test]
async fn iam_credential_report() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client.generate_credential_report().send().await.unwrap();
    let _ = client.get_credential_report().send().await;
}

// ==========================================================================
// Virtual MFA devices
// ==========================================================================

#[test_action("iam", "CreateVirtualMFADevice", checksum = "f3a8685f")]
#[test_action("iam", "ListVirtualMFADevices", checksum = "62efcff7")]
#[test_action("iam", "DeleteVirtualMFADevice", checksum = "a9101f94")]
#[tokio::test]
async fn iam_virtual_mfa_device() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let create = client
        .create_virtual_mfa_device()
        .virtual_mfa_device_name("conf-mfa")
        .send()
        .await
        .unwrap();
    let serial = create
        .virtual_mfa_device()
        .unwrap()
        .serial_number()
        .to_string();

    let _ = client.list_virtual_mfa_devices().send().await.unwrap();

    client
        .delete_virtual_mfa_device()
        .serial_number(&serial)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// MFA devices (enable/deactivate/list)
// ==========================================================================

#[test_action("iam", "EnableMFADevice", checksum = "d342b0fb")]
#[test_action("iam", "ListMFADevices", checksum = "0a91d26a")]
#[test_action("iam", "DeactivateMFADevice", checksum = "4b99fc49")]
#[tokio::test]
async fn iam_mfa_device_lifecycle() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-mfa-u")
        .send()
        .await
        .unwrap();
    let create = client
        .create_virtual_mfa_device()
        .virtual_mfa_device_name("conf-mfa-en")
        .send()
        .await
        .unwrap();
    let serial = create
        .virtual_mfa_device()
        .unwrap()
        .serial_number()
        .to_string();

    client
        .enable_mfa_device()
        .user_name("conf-mfa-u")
        .serial_number(&serial)
        .authentication_code1("123456")
        .authentication_code2("654321")
        .send()
        .await
        .unwrap();

    let list = client
        .list_mfa_devices()
        .user_name("conf-mfa-u")
        .send()
        .await
        .unwrap();
    assert!(!list.mfa_devices().is_empty());

    client
        .deactivate_mfa_device()
        .user_name("conf-mfa-u")
        .serial_number(&serial)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// ListEntitiesForPolicy
// ==========================================================================

#[test_action("iam", "ListEntitiesForPolicy", checksum = "d4f92d63")]
#[tokio::test]
async fn iam_list_entities_for_policy() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    let pol = client
        .create_policy()
        .policy_name("conf-lefp")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .unwrap();
    let arn = pol.policy().unwrap().arn().unwrap().to_string();

    let _ = client
        .list_entities_for_policy()
        .policy_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ==========================================================================
// SSH public keys
// ==========================================================================

const SSH_PUB_KEY: &str = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCxO38tUfq4Gqmkq1Hrmx0d+5aVPzBR8cQH4PiPeFrM5JhK0U3hKpNVQNzLigCrjYgHQXlu6jTjJk4JQiF8iB2nmb1RJFq3QlMTHQq766CUr1OQrP2g8GzqMzfJMSHJJ4Y//5Itxb5XAGaD5C0NDNxadB7B5GvFT8qqhC1mJZ1FeX8BkeK7Hpwii1P4y7qNB3Pj5xDQ8J9G3DxS5s8N7K4bH3PrYVLGYvHn5R0j2m3K6JaB7F3dN4A7K3pB6YxzhQ2L8PAFDuOi4gBnK+aTfTnFSRNFnKRhjE7RD3CWabMrZ3s6PiKXO6VBM7Wl+R13D0i1lPNbQIEz2xITZ7xBnZ test@conformance";

#[test_action("iam", "UploadSSHPublicKey", checksum = "080a214e")]
#[test_action("iam", "GetSSHPublicKey", checksum = "943f188a")]
#[test_action("iam", "ListSSHPublicKeys", checksum = "f292a035")]
#[test_action("iam", "UpdateSSHPublicKey", checksum = "95eb9f00")]
#[test_action("iam", "DeleteSSHPublicKey", checksum = "cdfffd7e")]
#[tokio::test]
async fn iam_ssh_public_keys() {
    let server = TestServer::start().await;
    let client = server.iam_client().await;

    client
        .create_user()
        .user_name("conf-ssh")
        .send()
        .await
        .unwrap();

    let upload = client
        .upload_ssh_public_key()
        .user_name("conf-ssh")
        .ssh_public_key_body(SSH_PUB_KEY)
        .send()
        .await
        .unwrap();
    let key_id = upload
        .ssh_public_key()
        .unwrap()
        .ssh_public_key_id()
        .to_string();

    let _ = client
        .get_ssh_public_key()
        .user_name("conf-ssh")
        .ssh_public_key_id(&key_id)
        .encoding(aws_sdk_iam::types::EncodingType::Ssh)
        .send()
        .await
        .unwrap();

    let _ = client
        .list_ssh_public_keys()
        .user_name("conf-ssh")
        .send()
        .await
        .unwrap();

    client
        .update_ssh_public_key()
        .user_name("conf-ssh")
        .ssh_public_key_id(&key_id)
        .status(aws_sdk_iam::types::StatusType::Inactive)
        .send()
        .await
        .unwrap();

    client
        .delete_ssh_public_key()
        .user_name("conf-ssh")
        .ssh_public_key_id(&key_id)
        .send()
        .await
        .unwrap();
}
