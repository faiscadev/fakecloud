//! Service Quotas E2E.
//!
//! Drives the Service Quotas API with the AWS Rust SDK (awsJson1.1,
//! x-amz-target `ServiceQuotasV20190624.<Op>`) and checks that the applied
//! values it reports are the ones EC2 enforces once enforcement is switched
//! on: raising "Inbound or outbound rules per security group" or "Security
//! groups per network interface" with `RequestServiceQuotaIncrease` changes
//! what `AuthorizeSecurityGroupIngress` and `CreateNetworkInterface` accept,
//! and that IAM, DynamoDB, KMS, S3 and Lambda refuse the create that would
//! take an enforced count quota past its applied value. Enforcement is opt-in (`--enforce-quotas`, `--enforce-quota`, or the
//! `/_fakecloud/service-quotas/*` introspection API, driven here through the
//! Rust `fakecloud-sdk`).

mod helpers;

use aws_sdk_ec2::types::{IpPermission, IpRange};
use aws_sdk_servicequotas::error::ProvideErrorMetadata;
use aws_sdk_servicequotas::types::{RequestStatus, Tag};
use fakecloud_sdk::types::{
    PutQuotaEnforcementRequest, PutServiceQuotaRequest, QuotaEnforcement, QuotaOverrideChange,
};
use fakecloud_sdk::FakeCloud;
use helpers::TestServer;

const VPC: &str = "vpc";
const SGS_PER_ENI: &str = "L-2AFB9258";
const RULES_PER_SG: &str = "L-0EA8095F";

async fn clients(server: &TestServer) -> (aws_sdk_servicequotas::Client, aws_sdk_ec2::Client) {
    let config = server.aws_config().await;
    (
        aws_sdk_servicequotas::Client::new(&config),
        aws_sdk_ec2::Client::new(&config),
    )
}

async fn applied(sq: &aws_sdk_servicequotas::Client, code: &str) -> f64 {
    sq.get_service_quota()
        .service_code(VPC)
        .quota_code(code)
        .send()
        .await
        .unwrap()
        .quota()
        .unwrap()
        .value()
        .unwrap()
}

async fn request_increase(sq: &aws_sdk_servicequotas::Client, code: &str, value: f64) -> String {
    let resp = sq
        .request_service_quota_increase()
        .service_code(VPC)
        .quota_code(code)
        .desired_value(value)
        .send()
        .await
        .unwrap();
    let req = resp.requested_quota().unwrap();
    assert_eq!(req.status(), Some(&RequestStatus::Pending));
    req.id().unwrap().to_string()
}

async fn status(sq: &aws_sdk_servicequotas::Client, id: &str) -> RequestStatus {
    sq.get_requested_service_quota_change()
        .request_id(id)
        .send()
        .await
        .unwrap()
        .requested_quota()
        .unwrap()
        .status()
        .unwrap()
        .clone()
}

fn cidr_rule(n: usize) -> IpPermission {
    IpPermission::builder()
        .ip_protocol("tcp")
        .from_port(1000 + n as i32)
        .to_port(1000 + n as i32)
        .ip_ranges(IpRange::builder().cidr_ip("10.0.0.0/8").build())
        .build()
}

#[tokio::test]
async fn catalog_lookups() {
    let server = TestServer::start().await;
    let (sq, _) = clients(&server).await;

    // Every service AWS lists, over several pages of at most 100.
    let mut codes = Vec::new();
    let mut token = None;
    loop {
        let page = sq
            .list_services()
            .set_next_token(token)
            .send()
            .await
            .unwrap();
        assert!(page.services().len() <= 100);
        codes.extend(
            page.services()
                .iter()
                .filter_map(|s| s.service_code().map(str::to_string)),
        );
        token = page.next_token().map(str::to_string);
        if token.is_none() {
            break;
        }
    }
    // Every service AWS lists: well over 300.
    assert!(codes.len() > 300, "{}", codes.len());
    assert!(codes.iter().any(|c| c == VPC));
    assert!(codes.iter().any(|c| c == "AWSCloudMap"));

    let default = sq
        .get_aws_default_service_quota()
        .service_code(VPC)
        .quota_code(SGS_PER_ENI)
        .send()
        .await
        .unwrap();
    let q = default.quota().unwrap();
    assert_eq!(q.value(), Some(5.0));
    assert_eq!(
        q.quota_name(),
        Some("Security groups per network interface")
    );
    assert_eq!(
        q.quota_arn(),
        Some("arn:aws:servicequotas:us-east-1::vpc/L-2AFB9258")
    );

    let all = sq
        .list_service_quotas()
        .service_code(VPC)
        .send()
        .await
        .unwrap();
    assert!(all
        .quotas()
        .iter()
        .any(|q| q.quota_code() == Some(RULES_PER_SG) && q.value() == Some(60.0)));

    let err = sq
        .get_service_quota()
        .service_code(VPC)
        .quota_code("L-00000000")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchResourceException"));
}

#[tokio::test]
async fn raised_rule_quota_is_enforced_by_ec2() {
    let server = TestServer::start_full(&[], &["--enforce-quota", "vpc/L-0EA8095F"]).await;
    let (sq, ec2) = clients(&server).await;

    let vpc = ec2
        .create_vpc()
        .cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let sg = ec2
        .create_security_group()
        .group_name("quota")
        .description("quota")
        .vpc_id(&vpc_id)
        .send()
        .await
        .unwrap();
    let sg_id = sg.group_id().unwrap().to_string();

    // The AWS default allows 60 inbound rules.
    ec2.authorize_security_group_ingress()
        .group_id(&sg_id)
        .set_ip_permissions(Some((0..60).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();
    let err = ec2
        .authorize_security_group_ingress()
        .group_id(&sg_id)
        .ip_permissions(cidr_rule(60))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_ec2::error::ProvideErrorMetadata::code(&err),
        Some("RulesPerSecurityGroupLimitExceeded")
    );

    // 5 groups per interface x 100 rules = 500 <= 1000: approved.
    let id = request_increase(&sq, RULES_PER_SG, 100.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
    assert_eq!(applied(&sq, RULES_PER_SG).await, 100.0);

    ec2.authorize_security_group_ingress()
        .group_id(&sg_id)
        .ip_permissions(cidr_rule(60))
        .send()
        .await
        .unwrap();

    // Terraform updates an existing quota by submitting another increase,
    // then reading both the applied quota and the new request's status.
    let id = request_increase(&sq, RULES_PER_SG, 200.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
    assert_eq!(applied(&sq, RULES_PER_SG).await, 200.0);
    ec2.authorize_security_group_ingress()
        .group_id(&sg_id)
        .set_ip_permissions(Some((61..200).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();
    let err = ec2
        .authorize_security_group_ingress()
        .group_id(&sg_id)
        .ip_permissions(cidr_rule(200))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_ec2::error::ProvideErrorMetadata::code(&err),
        Some("RulesPerSecurityGroupLimitExceeded")
    );
    // The same applied quota independently allows 200 outbound rules (one
    // default outbound rule already exists); inbound usage is not added.
    ec2.authorize_security_group_egress()
        .group_id(&sg_id)
        .set_ip_permissions(Some((0..199).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();
    let err = ec2
        .authorize_security_group_egress()
        .group_id(&sg_id)
        .ip_permissions(cidr_rule(199))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_ec2::error::ProvideErrorMetadata::code(&err),
        Some("RulesPerSecurityGroupLimitExceeded")
    );

    let history = sq
        .list_requested_service_quota_change_history_by_quota()
        .service_code(VPC)
        .quota_code(RULES_PER_SG)
        .send()
        .await
        .unwrap();
    assert_eq!(history.requested_quotas().len(), 2);
}

#[tokio::test]
async fn raised_groups_per_interface_quota_is_enforced_by_ec2() {
    let server = TestServer::start_full(&[], &["--enforce-quotas"]).await;
    let (sq, ec2) = clients(&server).await;

    let vpc = ec2
        .create_vpc()
        .cidr_block("10.1.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = ec2
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.1.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet_id = subnet.subnet().unwrap().subnet_id().unwrap().to_string();
    let mut groups = Vec::new();
    for i in 0..6 {
        let sg = ec2
            .create_security_group()
            .group_name(format!("g{i}"))
            .description("g")
            .vpc_id(&vpc_id)
            .send()
            .await
            .unwrap();
        groups.push(sg.group_id().unwrap().to_string());
    }

    let err = ec2
        .create_network_interface()
        .subnet_id(&subnet_id)
        .set_groups(Some(groups.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_ec2::error::ProvideErrorMetadata::code(&err),
        Some("SecurityGroupsPerInterfaceLimitExceeded")
    );

    // 8 groups x 60 rules = 480: approved.
    let id = request_increase(&sq, SGS_PER_ENI, 8.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
    ec2.create_network_interface()
        .subnet_id(&subnet_id)
        .set_groups(Some(groups))
        .send()
        .await
        .unwrap();

    // 8 groups x 200 rules = 1600 > 1000: AWS does not approve it, and the
    // applied value stays.
    let id = request_increase(&sq, RULES_PER_SG, 200.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::NotApproved);
    assert_eq!(applied(&sq, RULES_PER_SG).await, 60.0);
}

#[tokio::test]
async fn applied_quota_tags_and_utilization_report() {
    let server = TestServer::start().await;
    let (sq, ec2) = clients(&server).await;

    let arn = sq
        .get_service_quota()
        .service_code(VPC)
        .quota_code(SGS_PER_ENI)
        .send()
        .await
        .unwrap()
        .quota()
        .unwrap()
        .quota_arn()
        .unwrap()
        .to_string();
    sq.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("team").value("network").build().unwrap())
        .send()
        .await
        .unwrap();
    let tags = sq
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].value(), "network");

    let before = ec2.describe_vpcs().send().await.unwrap().vpcs().len();
    ec2.create_vpc()
        .cidr_block("10.2.0.0/16")
        .send()
        .await
        .unwrap();
    let report = sq.start_quota_utilization_report().send().await.unwrap();
    let got = sq
        .get_quota_utilization_report()
        .report_id(report.report_id().unwrap())
        .send()
        .await
        .unwrap();
    let vpcs = got
        .quotas()
        .iter()
        .find(|q| q.quota_code() == Some("L-F678F1CE"))
        .expect("VPCs per Region is measured");
    let expected = (before + 1) as f64 / 5.0 * 100.0;
    assert_eq!(vpcs.utilization(), Some(expected));
}

/// A VPC, a subnet and `n` security groups in it.
async fn vpc_with_groups(ec2: &aws_sdk_ec2::Client, n: usize) -> (String, String, Vec<String>) {
    let vpc = ec2
        .create_vpc()
        .cidr_block("10.9.0.0/16")
        .send()
        .await
        .unwrap();
    let vpc_id = vpc.vpc().unwrap().vpc_id().unwrap().to_string();
    let subnet = ec2
        .create_subnet()
        .vpc_id(&vpc_id)
        .cidr_block("10.9.1.0/24")
        .send()
        .await
        .unwrap();
    let subnet_id = subnet.subnet().unwrap().subnet_id().unwrap().to_string();
    let mut groups = Vec::new();
    for i in 0..n {
        let sg = ec2
            .create_security_group()
            .group_name(format!("g{i}"))
            .description("g")
            .vpc_id(&vpc_id)
            .send()
            .await
            .unwrap();
        groups.push(sg.group_id().unwrap().to_string());
    }
    (vpc_id, subnet_id, groups)
}

async fn eni_error(
    ec2: &aws_sdk_ec2::Client,
    subnet_id: &str,
    groups: &[String],
) -> Option<String> {
    ec2.create_network_interface()
        .subnet_id(subnet_id)
        .set_groups(Some(groups.to_vec()))
        .send()
        .await
        .err()
        .and_then(|e| aws_sdk_ec2::error::ProvideErrorMetadata::code(&e).map(str::to_string))
}

#[tokio::test]
async fn quotas_are_not_enforced_by_default() {
    let server = TestServer::start().await;
    let (_, ec2) = clients(&server).await;
    let (_, subnet_id, groups) = vpc_with_groups(&ec2, 6).await;
    // Six groups on one interface: past the AWS default of 5, accepted.
    assert_eq!(eni_error(&ec2, &subnet_id, &groups).await, None);
    // 61 inbound rules: past the AWS default of 60, accepted.
    ec2.authorize_security_group_ingress()
        .group_id(&groups[0])
        .set_ip_permissions(Some((0..61).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();

    let fc = FakeCloud::new(server.endpoint());
    let quotas = fc
        .service_quotas()
        .get_quotas(None, None, Some(VPC))
        .await
        .unwrap();
    let rules = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == RULES_PER_SG)
        .unwrap();
    assert!(rules.enforceable);
    assert!(!rules.enforced);
    assert_eq!(rules.enforcement_source, "global");
    let vpcs = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == "L-F678F1CE")
        .unwrap();
    assert!(vpcs.usage.is_some_and(|u| u >= 1.0), "{:?}", vpcs.usage);
}

#[tokio::test]
async fn introspection_lowers_and_enforces_a_quota() {
    let server = TestServer::start().await;
    let (sq, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    let (_, subnet_id, groups) = vpc_with_groups(&ec2, 2).await;

    // One group per interface, below the AWS default of 5 (AWS never lowers
    // a quota; a test can, to hit the limit without five groups).
    let quota = fc
        .service_quotas()
        .put_quota(
            VPC,
            SGS_PER_ENI,
            &PutServiceQuotaRequest {
                value: Some(1.0),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(quota.applied_value, 1.0);
    assert!(quota.enforced);
    assert_eq!(quota.enforcement_source, "override");
    // The Service Quotas API reports the same applied value.
    assert_eq!(applied(&sq, SGS_PER_ENI).await, 1.0);
    assert_eq!(
        eni_error(&ec2, &subnet_id, &groups).await.as_deref(),
        Some("SecurityGroupsPerInterfaceLimitExceeded")
    );
    assert_eq!(eni_error(&ec2, &subnet_id, &groups[..1]).await, None);

    // Back to the default, and no longer enforced.
    let quota = fc
        .service_quotas()
        .delete_quota(VPC, SGS_PER_ENI, None, None)
        .await
        .unwrap();
    assert_eq!(quota.applied_value, 5.0);
    assert!(!quota.enforced);
    assert_eq!(eni_error(&ec2, &subnet_id, &groups).await, None);

    // Unknown quotas and quotas no service checks are refused.
    let err = fc
        .service_quotas()
        .put_quota(
            VPC,
            "L-NOPE",
            &PutServiceQuotaRequest {
                value: Some(1.0),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, fakecloud_sdk::Error::Api { status: 404, .. }),
        "{err}"
    );
    let err = fc
        .service_quotas()
        .put_quota(
            "lambda",
            "L-B99A9384",
            &PutServiceQuotaRequest {
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, fakecloud_sdk::Error::Api { status: 400, .. }),
        "{err}"
    );
}

#[tokio::test]
async fn global_switch_with_a_per_quota_ignore() {
    let server = TestServer::start().await;
    let (_, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    let enforcement = fc
        .service_quotas()
        .put_enforcement(&PutQuotaEnforcementRequest {
            enforce_all: Some(true),
            overrides: vec![QuotaOverrideChange {
                service_code: VPC.into(),
                quota_code: RULES_PER_SG.into(),
                account_id: None,
                enforce: QuotaEnforcement::Ignore,
            }],
        })
        .await
        .unwrap();
    assert!(enforcement.enforce_all);
    assert_eq!(enforcement.overrides.len(), 1);
    assert!(!enforcement.overrides[0].enforce);

    let (_, subnet_id, groups) = vpc_with_groups(&ec2, 6).await;
    assert_eq!(
        eni_error(&ec2, &subnet_id, &groups).await.as_deref(),
        Some("SecurityGroupsPerInterfaceLimitExceeded")
    );
    ec2.authorize_security_group_ingress()
        .group_id(&groups[0])
        .set_ip_permissions(Some((0..61).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();

    // An account-scoped override applies to that account only.
    let other = "210987654321";
    let enforcement = fc
        .service_quotas()
        .put_enforcement(&PutQuotaEnforcementRequest {
            enforce_all: None,
            overrides: vec![QuotaOverrideChange {
                service_code: VPC.into(),
                quota_code: SGS_PER_ENI.into(),
                account_id: Some(other.into()),
                enforce: QuotaEnforcement::Ignore,
            }],
        })
        .await
        .unwrap();
    assert_eq!(
        enforcement.account_overrides[0].account_id.as_deref(),
        Some(other)
    );
    let quotas = fc
        .service_quotas()
        .get_quotas(Some(other), None, Some(VPC))
        .await
        .unwrap();
    let q = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == SGS_PER_ENI)
        .unwrap();
    assert!(!q.enforced);
    assert_eq!(q.enforcement_source, "account_override");
}

#[tokio::test]
async fn manual_approval_holds_requests_until_decided() {
    let server = TestServer::start_full(&[], &["--quota-requests", "manual"]).await;
    let (sq, _) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    assert_eq!(
        fc.service_quotas()
            .get_request_approval()
            .await
            .unwrap()
            .mode,
        "manual"
    );

    let id = request_increase(&sq, RULES_PER_SG, 100.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::Pending);
    assert_eq!(applied(&sq, RULES_PER_SG).await, 60.0);
    // A pending request can open a support case, as on AWS.
    sq.create_support_case()
        .request_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(status(&sq, &id).await, RequestStatus::CaseOpened);

    let open = fc
        .service_quotas()
        .get_requests(None, Some("CASE_OPENED"))
        .await
        .unwrap();
    assert_eq!(open.requests.len(), 1);
    assert_eq!(open.requests[0].request_id, id);
    assert!(open.requests[0].case_id.is_some());

    let decided = fc.service_quotas().approve_request(&id).await.unwrap();
    assert_eq!(decided.status, "APPROVED");
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
    assert_eq!(applied(&sq, RULES_PER_SG).await, 100.0);

    let id = request_increase(&sq, SGS_PER_ENI, 8.0).await;
    let decided = fc
        .service_quotas()
        .deny_request(&id, Some("DENIED"))
        .await
        .unwrap();
    assert_eq!(decided.status, "DENIED");
    assert_eq!(status(&sq, &id).await, RequestStatus::Denied);
    assert_eq!(applied(&sq, SGS_PER_ENI).await, 5.0);
    let err = fc.service_quotas().approve_request(&id).await.unwrap_err();
    assert!(
        matches!(err, fakecloud_sdk::Error::Api { status: 409, .. }),
        "{err}"
    );

    // Back to automatic approval: the next request is decided on submission.
    fc.service_quotas()
        .set_request_approval("auto")
        .await
        .unwrap();
    let id = request_increase(&sq, SGS_PER_ENI, 8.0).await;
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
}

#[tokio::test]
async fn reset_restores_the_startup_quota_settings() {
    let server = TestServer::start_full(&[], &["--enforce-quotas"]).await;
    let fc = FakeCloud::new(server.endpoint());
    fc.service_quotas()
        .put_enforcement(&PutQuotaEnforcementRequest {
            enforce_all: Some(false),
            overrides: vec![],
        })
        .await
        .unwrap();
    fc.service_quotas()
        .put_quota(
            VPC,
            SGS_PER_ENI,
            &PutServiceQuotaRequest {
                value: Some(1.0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    fc.reset().await.unwrap();
    assert!(
        fc.service_quotas()
            .get_enforcement()
            .await
            .unwrap()
            .enforce_all
    );
    let quotas = fc
        .service_quotas()
        .get_quotas(None, None, Some(VPC))
        .await
        .unwrap();
    let q = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == SGS_PER_ENI)
        .unwrap();
    assert_eq!(q.applied_value, 5.0);
}

#[tokio::test]
async fn account_quota_data_survives_a_restart_but_server_settings_follow_the_flags() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().display().to_string();
    let mut server = TestServer::start_full(
        &[("FAKECLOUD_CONTAINER_CLI", "false")],
        &["--storage-mode", "persistent", "--data-path", &data],
    )
    .await;
    let fc = FakeCloud::new(server.endpoint());
    let account = fc
        .service_quotas()
        .get_quotas(None, None, Some(VPC))
        .await
        .unwrap()
        .account_id;
    fc.service_quotas()
        .put_quota(
            VPC,
            SGS_PER_ENI,
            &PutServiceQuotaRequest {
                account_id: Some(account.clone()),
                value: Some(2.0),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    fc.service_quotas()
        .put_enforcement(&PutQuotaEnforcementRequest {
            enforce_all: Some(true),
            overrides: vec![],
        })
        .await
        .unwrap();
    fc.service_quotas()
        .set_request_approval("manual")
        .await
        .unwrap();

    server.restart().await;
    let fc = FakeCloud::new(server.endpoint());
    let quotas = fc
        .service_quotas()
        .get_quotas(None, None, Some(VPC))
        .await
        .unwrap();
    let q = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == SGS_PER_ENI)
        .unwrap();
    // The applied value and the account's override are account data.
    assert_eq!(q.applied_value, 2.0);
    assert!(q.enforced);
    assert_eq!(q.enforcement_source, "account_override");
    // The server-wide settings come from the (absent) flags again.
    assert!(
        !fc.service_quotas()
            .get_enforcement()
            .await
            .unwrap()
            .enforce_all
    );
    assert_eq!(
        fc.service_quotas()
            .get_request_approval()
            .await
            .unwrap()
            .mode,
        "auto"
    );
}

async fn default_quota(
    sq: &aws_sdk_servicequotas::Client,
    service: &str,
    code: &str,
) -> aws_sdk_servicequotas::types::ServiceQuota {
    sq.get_aws_default_service_quota()
        .service_code(service)
        .quota_code(code)
        .send()
        .await
        .unwrap()
        .quota()
        .unwrap()
        .clone()
}

/// Request an increase of `service`/`code` to `value` and return how it was
/// decided.
async fn decide(
    sq: &aws_sdk_servicequotas::Client,
    service: &str,
    code: &str,
    value: f64,
) -> RequestStatus {
    let id = sq
        .request_service_quota_increase()
        .service_code(service)
        .quota_code(code)
        .desired_value(value)
        .send()
        .await
        .unwrap()
        .requested_quota()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    status(sq, &id).await
}

/// Defaults, units and adjustability as AWS publishes them.
#[tokio::test]
async fn published_defaults() {
    let server = TestServer::start().await;
    let (sq, _) = clients(&server).await;

    for (service, code, value) in [
        (VPC, "L-93826ACB", 500.0),
        ("iam", "L-0DA4ABF3", 20.0),
        ("iam", "L-6E65F664", 1000.0),
        ("iam", "L-C07B4B0D", 2048.0),
        ("lambda", "L-2ACBD22F", 300.0),
    ] {
        assert_eq!(
            default_quota(&sq, service, code).await.value(),
            Some(value),
            "{code}"
        );
    }
    let storage = default_quota(&sq, "lambda", "L-2ACBD22F").await;
    assert_eq!(storage.unit(), Some("Gigabytes"));
    let concurrency = default_quota(&sq, "lambda", "L-B99A9384").await;
    assert_eq!(concurrency.unit(), Some("Count"));
    let per_group = default_quota(&sq, "iam", "L-384571C4").await;
    assert!(!per_group.adjustable());

    let err = sq
        .request_service_quota_increase()
        .service_code("iam")
        .quota_code("L-384571C4")
        .desired_value(11.0)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("IllegalArgumentException"));
}

/// An increase past AWS's documented maximum is not approved; one at the
/// maximum is.
#[tokio::test]
async fn increases_past_the_documented_maximum_are_not_approved() {
    let server = TestServer::start().await;
    let (sq, _) = clients(&server).await;

    for (service, code, max) in [
        ("iam", "L-0DA4ABF3", 25.0),
        ("iam", "L-858F3967", 700.0),
        ("iam", "L-FE177D64", 10000.0),
        (VPC, "L-085A6257", 50.0),
        (VPC, "L-BB24F6E5", 256000.0),
        (VPC, "L-CD17FD4B", 512000.0),
    ] {
        assert_eq!(
            decide(&sq, service, code, max + 1.0).await,
            RequestStatus::NotApproved,
            "{code} above max"
        );
        assert_eq!(
            decide(&sq, service, code, max).await,
            RequestStatus::Approved,
            "{code} at max"
        );
    }
    // Server certificates per account cannot go past the default of 20.
    assert_eq!(
        decide(&sq, "iam", "L-BF35879D", 21.0).await,
        RequestStatus::NotApproved
    );
}

/// A raised IAM quota shows up in IAM's own `GetAccountSummary`.
#[tokio::test]
async fn raised_iam_quota_shows_in_account_summary() {
    use aws_sdk_iam::types::SummaryKeyType;

    let server = TestServer::start().await;
    let (sq, _) = clients(&server).await;
    let iam = server.iam_client().await;

    let summary = iam.get_account_summary().send().await.unwrap();
    let map = summary.summary_map().unwrap();
    assert_eq!(
        map.get(&SummaryKeyType::AttachedPoliciesPerRoleQuota),
        Some(&20)
    );
    assert_eq!(map.get(&SummaryKeyType::RolesQuota), Some(&1000));

    assert_eq!(
        decide(&sq, "iam", "L-0DA4ABF3", 25.0).await,
        RequestStatus::Approved
    );
    assert_eq!(
        decide(&sq, "iam", "L-FE177D64", 4000.0).await,
        RequestStatus::Approved
    );

    let summary = iam.get_account_summary().send().await.unwrap();
    let map = summary.summary_map().unwrap();
    assert_eq!(
        map.get(&SummaryKeyType::AttachedPoliciesPerRoleQuota),
        Some(&25)
    );
    assert_eq!(map.get(&SummaryKeyType::RolesQuota), Some(&4000));
    assert_eq!(map.get(&SummaryKeyType::GroupsQuota), Some(&300));
}

/// A rule that references the CloudFront origin-facing AWS-managed prefix
/// list counts as its published weight of 55 rules, leaving 5 of the default
/// 60.
#[tokio::test]
async fn aws_managed_prefix_list_rule_counts_its_weight() {
    use aws_sdk_ec2::types::{Filter, PrefixListId};

    let server = TestServer::start_full(&[], &["--enforce-quota", "vpc/L-0EA8095F"]).await;
    let (_, ec2) = clients(&server).await;

    let lists = ec2
        .describe_managed_prefix_lists()
        .filters(
            Filter::builder()
                .name("prefix-list-name")
                .values("com.amazonaws.global.cloudfront.origin-facing")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(lists.prefix_lists().len(), 1);
    let cf = &lists.prefix_lists()[0];
    assert_eq!(cf.owner_id(), Some("AWS"));
    let pl_id = cf.prefix_list_id().unwrap().to_string();

    // The list holds CloudFront's published origin-facing ranges, no more
    // than its weight.
    let entries = ec2
        .get_managed_prefix_list_entries()
        .prefix_list_id(&pl_id)
        .send()
        .await
        .unwrap();
    let n = entries.entries().len();
    assert!(n > 0 && n <= 55, "{n} entries");
    assert!(entries
        .entries()
        .iter()
        .all(|e| e.cidr().is_some_and(|c| c.contains('.'))));

    // An unknown filter name matches nothing, as in the other EC2 describes.
    let none = ec2
        .describe_managed_prefix_lists()
        .filters(Filter::builder().name("no-such-filter").values("x").build())
        .send()
        .await
        .unwrap();
    assert!(none.prefix_lists().is_empty());

    let vpc = ec2
        .create_vpc()
        .cidr_block("10.0.0.0/16")
        .send()
        .await
        .unwrap();
    let sg_id = ec2
        .create_security_group()
        .group_name("cloudfront")
        .description("cloudfront")
        .vpc_id(vpc.vpc().unwrap().vpc_id().unwrap())
        .send()
        .await
        .unwrap()
        .group_id()
        .unwrap()
        .to_string();

    ec2.authorize_security_group_ingress()
        .group_id(&sg_id)
        .ip_permissions(
            IpPermission::builder()
                .ip_protocol("tcp")
                .from_port(443)
                .to_port(443)
                .prefix_list_ids(PrefixListId::builder().prefix_list_id(&pl_id).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    // 55 + 5 = 60: at the quota.
    ec2.authorize_security_group_ingress()
        .group_id(&sg_id)
        .set_ip_permissions(Some((0..5).map(cidr_rule).collect()))
        .send()
        .await
        .unwrap();
    let err = ec2
        .authorize_security_group_ingress()
        .group_id(&sg_id)
        .ip_permissions(cidr_rule(5))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        aws_sdk_ec2::error::ProvideErrorMetadata::code(&err),
        Some("RulesPerSecurityGroupLimitExceeded")
    );
}

// ---- IAM, DynamoDB, KMS, S3 and Lambda enforcement ----

/// Lower `service`/`code` to one above its current usage and switch its
/// enforcement on through the introspection API. Returns that usage, which
/// must be reported (the service counts it).
async fn cap_one_above_usage(fc: &FakeCloud, service: &str, code: &str) -> f64 {
    let quotas = fc
        .service_quotas()
        .get_quotas(None, None, Some(service))
        .await
        .unwrap();
    let usage = quotas
        .quotas
        .iter()
        .find(|q| q.quota_code == code)
        .unwrap_or_else(|| panic!("{service}/{code} missing"))
        .usage
        .unwrap_or_else(|| panic!("{service}/{code} reports no usage"));
    let quota = fc
        .service_quotas()
        .put_quota(
            service,
            code,
            &PutServiceQuotaRequest {
                value: Some(usage + 1.0),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(quota.enforceable && quota.enforced, "{service}/{code}");
    usage
}

const TRUST: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

#[tokio::test]
async fn enforced_iam_roles_quota_refuses_the_next_role() {
    use aws_sdk_iam::error::ProvideErrorMetadata;
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let iam = server.iam_client().await;

    let create = |name: &'static str| {
        iam.create_role()
            .role_name(name)
            .assume_role_policy_document(TRUST)
            .send()
    };
    // Unenforced by default: no limit applies.
    create("before-1").await.unwrap();

    let usage = cap_one_above_usage(&fc, "iam", "L-FE177D64").await;
    create("fits").await.unwrap();
    assert_eq!(usage_of(&fc, "iam", "L-FE177D64").await, usage + 1.0);
    let err = create("over").await.unwrap_err();
    assert_eq!(err.code(), Some("LimitExceeded"));
    assert_eq!(
        err.message(),
        Some(format!("Cannot exceed quota for RolesPerAccount: {}", usage + 1.0).as_str())
    );
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 409);
}

#[tokio::test]
async fn enforced_managed_policies_per_role_quota_refuses_the_next_attachment() {
    use aws_sdk_iam::error::ProvideErrorMetadata;
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let iam = server.iam_client().await;
    iam.create_role()
        .role_name("app")
        .assume_role_policy_document(TRUST)
        .send()
        .await
        .unwrap();

    fc.service_quotas()
        .put_quota(
            "iam",
            "L-0DA4ABF3",
            &PutServiceQuotaRequest {
                value: Some(1.0),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let attach = |arn: &'static str| {
        iam.attach_role_policy()
            .role_name("app")
            .policy_arn(arn)
            .send()
    };
    attach("arn:aws:iam::aws:policy/ReadOnlyAccess")
        .await
        .unwrap();
    let err = attach("arn:aws:iam::aws:policy/AdministratorAccess")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("LimitExceeded"));
    assert_eq!(
        err.message(),
        Some("Cannot exceed quota for PoliciesPerRole: 1")
    );
}

#[tokio::test]
async fn enforced_dynamodb_table_quota_refuses_the_next_table() {
    use aws_sdk_dynamodb::error::ProvideErrorMetadata;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let ddb = aws_sdk_dynamodb::Client::new(&server.aws_config().await);
    let create = |name: &'static str| {
        ddb.create_table()
            .table_name(name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
    };
    create("before").await.unwrap();

    let usage = cap_one_above_usage(&fc, "dynamodb", "L-F98FE922").await;
    assert_eq!(usage, 1.0);
    create("fits").await.unwrap();
    let err = create("over").await.unwrap_err();
    assert_eq!(err.code(), Some("LimitExceededException"));
    assert_eq!(
        err.message(),
        Some("Subscriber limit exceeded: There is a limit of 2 tables per subscriber")
    );
}

#[tokio::test]
async fn enforced_kms_key_quota_refuses_the_next_key() {
    use aws_sdk_kms::error::ProvideErrorMetadata;
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let kms = aws_sdk_kms::Client::new(&server.aws_config().await);
    kms.create_key().send().await.unwrap();

    let usage = cap_one_above_usage(&fc, "kms", "L-C2F1777E").await;
    assert_eq!(usage, 1.0);
    let fits = kms.create_key().send().await.unwrap();
    // A key pending deletion still counts.
    kms.schedule_key_deletion()
        .key_id(fits.key_metadata().unwrap().key_id())
        .pending_window_in_days(7)
        .send()
        .await
        .unwrap();
    let err = kms.create_key().send().await.unwrap_err();
    assert_eq!(err.code(), Some("LimitExceededException"));
}

#[tokio::test]
async fn enforced_s3_bucket_quota_refuses_the_next_bucket() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("quota-before")
        .send()
        .await
        .unwrap();

    let usage = cap_one_above_usage(&fc, "s3", "L-DC2B2D3D").await;
    assert_eq!(usage, 1.0);
    s3.create_bucket()
        .bucket("quota-fits")
        .send()
        .await
        .unwrap();
    let err = s3
        .create_bucket()
        .bucket("quota-over")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("TooManyBuckets"));
    assert_eq!(
        err.message(),
        Some("You have attempted to create more buckets than allowed")
    );
}

#[tokio::test]
async fn enforced_lambda_code_storage_quota_refuses_the_next_deploy() {
    use aws_sdk_lambda::error::ProvideErrorMetadata;
    use aws_sdk_lambda::primitives::Blob;
    use aws_sdk_lambda::types::{FunctionCode, Runtime};
    let server = TestServer::start().await;
    let fc = FakeCloud::new(server.endpoint());
    let lambda = aws_sdk_lambda::Client::new(&server.aws_config().await);

    // 100 bytes of storage, in the quota's gigabytes.
    let limit_gb = 100.5 / (1024.0 * 1024.0 * 1024.0);
    fc.service_quotas()
        .put_quota(
            "lambda",
            "L-2ACBD22F",
            &PutServiceQuotaRequest {
                value: Some(limit_gb),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let create = |name: &'static str| {
        lambda
            .create_function()
            .function_name(name)
            .runtime(Runtime::Python312)
            .role("arn:aws:iam::123456789012:role/test-role")
            .handler("index.handler")
            .code(
                FunctionCode::builder()
                    .zip_file(Blob::new(vec![b'z'; 60]))
                    .build(),
            )
            .send()
    };
    create("fits").await.unwrap();
    let err = create("over").await.unwrap_err();
    assert_eq!(err.code(), Some("CodeStorageExceededException"));
    assert_eq!(err.message(), Some("Code storage limit exceeded."));

    let settings = lambda.get_account_settings().send().await.unwrap();
    assert_eq!(settings.account_limit().unwrap().total_code_size(), 100);
    assert_eq!(settings.account_usage().unwrap().total_code_size(), 60);
    let usage = usage_of(&fc, "lambda", "L-2ACBD22F").await;
    assert_eq!((usage * 1024.0 * 1024.0 * 1024.0).round(), 60.0);
}

/// Usage of a quota as introspection reports it.
async fn usage_of(fc: &FakeCloud, service_code: &str, quota_code: &str) -> f64 {
    fc.service_quotas()
        .get_quotas(None, None, Some(service_code))
        .await
        .unwrap()
        .quotas
        .into_iter()
        .find(|q| q.quota_code == quota_code)
        .unwrap()
        .usage
        .unwrap_or_else(|| panic!("{service_code}/{quota_code} is measured"))
}

/// Set a quota's applied value and switch its enforcement on.
async fn enforce_at(fc: &FakeCloud, service_code: &str, quota_code: &str, value: f64) {
    let q = fc
        .service_quotas()
        .put_quota(
            service_code,
            quota_code,
            &PutServiceQuotaRequest {
                value: Some(value),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(q.enforced);
    assert_eq!(q.applied_value, value);
}

fn ec2_code<E: aws_sdk_ec2::error::ProvideErrorMetadata>(e: &E) -> Option<&str> {
    aws_sdk_ec2::error::ProvideErrorMetadata::code(e)
}

/// VPCs per Region, lowered to one more than the account holds (its default
/// VPC included): one more VPC fits, the next is refused.
#[tokio::test]
async fn vpcs_per_region_is_enforced() {
    let server = TestServer::start().await;
    let (_, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    let held = usage_of(&fc, VPC, "L-F678F1CE").await;
    assert!(held >= 1.0, "the default VPC counts");
    enforce_at(&fc, VPC, "L-F678F1CE", held + 1.0).await;

    ec2.create_vpc()
        .cidr_block("10.20.0.0/16")
        .send()
        .await
        .unwrap();
    let err = ec2
        .create_vpc()
        .cidr_block("10.21.0.0/16")
        .send()
        .await
        .unwrap_err();
    assert_eq!(ec2_code(&err), Some("VpcLimitExceeded"));
    assert_eq!(usage_of(&fc, VPC, "L-F678F1CE").await, held + 1.0);
}

#[tokio::test]
async fn elastic_ips_are_enforced() {
    let server = TestServer::start().await;
    let (_, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    let held = usage_of(&fc, "ec2", "L-0263D0A3").await;
    enforce_at(&fc, "ec2", "L-0263D0A3", held + 2.0).await;

    for _ in 0..2 {
        ec2.allocate_address().send().await.unwrap();
    }
    let err = ec2.allocate_address().send().await.unwrap_err();
    assert_eq!(ec2_code(&err), Some("AddressLimitExceeded"));
    assert_eq!(usage_of(&fc, "ec2", "L-0263D0A3").await, held + 2.0);
}

/// Subnets per VPC is counted per VPC: a full VPC refuses another subnet
/// while a second VPC still takes one.
#[tokio::test]
async fn subnets_per_vpc_is_enforced_per_vpc() {
    let server = TestServer::start().await;
    let (_, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    enforce_at(&fc, VPC, "L-407747CB", 1.0).await;

    let mut vpcs = Vec::new();
    for cidr in ["10.30.0.0/16", "10.31.0.0/16"] {
        let vpc = ec2.create_vpc().cidr_block(cidr).send().await.unwrap();
        vpcs.push(vpc.vpc().unwrap().vpc_id().unwrap().to_string());
    }
    let subnet =
        |vpc_id: &str, cidr: &str| ec2.create_subnet().vpc_id(vpc_id).cidr_block(cidr).send();
    subnet(&vpcs[0], "10.30.1.0/24").await.unwrap();
    let err = subnet(&vpcs[0], "10.30.2.0/24").await.unwrap_err();
    assert_eq!(ec2_code(&err), Some("SubnetLimitExceeded"));
    subnet(&vpcs[1], "10.31.1.0/24").await.unwrap();
}

/// With every quota enforced at the AWS defaults, a new account runs at most
/// 5 vCPUs of Standard On-Demand instances; an approved increase raises what
/// RunInstances accepts.
#[tokio::test]
async fn on_demand_vcpus_are_enforced_at_the_aws_default() {
    let server = TestServer::start_full(
        &[("FAKECLOUD_CONTAINER_CLI", "false")],
        &["--enforce-quotas"],
    )
    .await;
    let (sq, ec2) = clients(&server).await;
    let fc = FakeCloud::new(server.endpoint());
    let run = || {
        ec2.run_instances()
            .image_id("ami-12345678")
            .instance_type(aws_sdk_ec2::types::InstanceType::T3Micro)
            .min_count(1)
            .max_count(1)
            .send()
    };
    // t3.micro has 2 vCPUs: two fit in 5, a third does not.
    run().await.unwrap();
    run().await.unwrap();
    let err = run().await.unwrap_err();
    assert_eq!(ec2_code(&err), Some("VcpuLimitExceeded"));
    assert_eq!(usage_of(&fc, "ec2", "L-1216C47A").await, 4.0);

    let id = sq
        .request_service_quota_increase()
        .service_code("ec2")
        .quota_code("L-1216C47A")
        .desired_value(8.0)
        .send()
        .await
        .unwrap()
        .requested_quota()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    assert_eq!(status(&sq, &id).await, RequestStatus::Approved);
    run().await.unwrap();
    assert_eq!(usage_of(&fc, "ec2", "L-1216C47A").await, 6.0);

    // Other families have their own quota: G instances start at 0 vCPUs.
    let err = ec2
        .run_instances()
        .image_id("ami-12345678")
        .instance_type(aws_sdk_ec2::types::InstanceType::G5Xlarge)
        .min_count(1)
        .max_count(1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(ec2_code(&err), Some("VcpuLimitExceeded"));
}
