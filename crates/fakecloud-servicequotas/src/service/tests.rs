use super::*;
use crate::settings::QuotaSettings;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_organizations::{MemberAccount, OrganizationState, OrganizationsRegistry};
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

const MGMT: &str = "111111111111";
const MEMBER: &str = "222222222222";

fn orgs() -> SharedOrganizationsState {
    Arc::new(RwLock::new(OrganizationsRegistry::default()))
}

fn svc_with(orgs: SharedOrganizationsState) -> ServiceQuotasService {
    ServiceQuotasService::new(
        Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))),
        orgs,
        Arc::new(RwLock::new(QuotaSettings::default())),
    )
}

fn svc() -> ServiceQuotasService {
    svc_with(orgs())
}

fn req_in(account: &str, region: &str, action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "servicequotas".into(),
        action: action.into(),
        region: region.into(),
        account_id: account.into(),
        request_id: "req".into(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn run(
    s: &ServiceQuotasService,
    account: &str,
    action: &str,
    body: Value,
) -> Result<Value, AwsServiceError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(s.handle(req_in(account, "us-east-1", action, body)))
        .map(|resp| serde_json::from_slice(resp.body.expect_bytes()).unwrap())
}

fn call(s: &ServiceQuotasService, action: &str, body: Value) -> Value {
    run(s, "000000000000", action, body).expect("op ok")
}

fn call_err(s: &ServiceQuotasService, action: &str, body: Value) -> AwsServiceError {
    run(s, "000000000000", action, body).expect_err("op should fail")
}

fn sg_quota(code: &str) -> Value {
    json!({ "ServiceCode": "vpc", "QuotaCode": code })
}

#[test]
fn default_and_applied_quota_shapes() {
    let s = svc();
    let def = call(&s, "GetAWSDefaultServiceQuota", sg_quota("L-2AFB9258"));
    assert_eq!(def["Quota"]["Value"], 5.0);
    assert_eq!(
        def["Quota"]["QuotaName"],
        "Security groups per network interface"
    );
    assert_eq!(
        def["Quota"]["QuotaArn"],
        "arn:aws:servicequotas:us-east-1::vpc/L-2AFB9258"
    );
    let applied = call(&s, "GetServiceQuota", sg_quota("L-2AFB9258"));
    assert_eq!(
        applied["Quota"]["QuotaArn"],
        "arn:aws:servicequotas:us-east-1:000000000000:vpc/L-2AFB9258"
    );
    assert_eq!(applied["Quota"]["Adjustable"], true);
    assert_eq!(applied["Quota"]["GlobalQuota"], false);

    let iam = call(
        &s,
        "GetServiceQuota",
        json!({ "ServiceCode": "iam", "QuotaCode": "L-FE177D64" }),
    );
    assert_eq!(
        iam["Quota"]["QuotaArn"],
        "arn:aws:servicequotas::000000000000:iam/L-FE177D64"
    );
    assert_eq!(iam["Quota"]["GlobalQuota"], true);

    let vcpu = call(
        &s,
        "GetServiceQuota",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-1216C47A" }),
    );
    assert_eq!(vcpu["Quota"]["UsageMetric"]["MetricNamespace"], "AWS/Usage");
    assert_eq!(
        vcpu["Quota"]["UsageMetric"]["MetricDimensions"]["Class"],
        "Standard/OnDemand"
    );
}

/// `Description`, `Period` and `QuotaContext` come from AWS's published
/// defaults, verbatim.
#[test]
fn quota_description_period_and_context() {
    let s = svc();
    let rate = call(
        &s,
        "GetAWSDefaultServiceQuota",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-2394664B" }),
    );
    assert_eq!(
        rate["Quota"]["QuotaName"],
        "ModifySnapshotTier request bucket refill rate"
    );
    assert_eq!(
        rate["Quota"]["Description"],
        "The refill rate per second for the ModifySnapshotTier API request bucket"
    );
    assert_eq!(
        rate["Quota"]["Period"],
        json!({ "PeriodValue": 1, "PeriodUnit": "SECOND" })
    );
    assert!(rate["Quota"].get("QuotaContext").is_none());

    let per_tgw = call(
        &s,
        "GetServiceQuota",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-43872EB7" }),
    );
    assert_eq!(
        per_tgw["Quota"]["Description"],
        "Number of transit gateway route tables per transit gateway."
    );
    assert_eq!(
        per_tgw["Quota"]["Period"],
        json!({ "PeriodValue": 5, "PeriodUnit": "MINUTE" })
    );
    assert_eq!(
        per_tgw["Quota"]["QuotaContext"],
        json!({ "ContextScope": "RESOURCE", "ContextScopeType": "AWS::EC2::TransitGateway" })
    );

    // A quota without a period has none.
    let expiry = call(&s, "GetAWSDefaultServiceQuota", sg_quota("L-8312C5BB"));
    assert!(expiry["Quota"].get("Period").is_none());
    assert!(expiry["Quota"]["Description"].as_str().is_some());
}

#[test]
fn context_id_refusal_describes_the_quota_truthfully() {
    let s = svc();
    // An account-only quota never takes a ContextId.
    let mut body = sg_quota("L-F678F1CE");
    body["ContextId"] = json!("vpc-123");
    let e = call_err(&s, "GetServiceQuota", body.clone());
    assert_eq!(e.code(), "IllegalArgumentException");
    assert!(
        e.message().contains("applied at the account level"),
        "{}",
        e.message()
    );
    body["DesiredValue"] = json!(10.0);
    let e = call_err(&s, "RequestServiceQuotaIncrease", body);
    assert!(e.message().contains("applied at the account level"));

    // A quota AWS scopes per transit gateway: resource-level values are what
    // fakecloud lacks.
    let mut per_tgw = json!({
        "ServiceCode": "ec2",
        "QuotaCode": "L-43872EB7",
        "ContextId": "tgw-123",
    });
    let e = call_err(&s, "GetServiceQuota", per_tgw.clone());
    assert_eq!(e.code(), "IllegalArgumentException");
    assert!(
        e.message()
            .contains("does not support resource-level applied values"),
        "{}",
        e.message()
    );
    per_tgw["DesiredValue"] = json!(30.0);
    let e = call_err(&s, "RequestServiceQuotaIncrease", per_tgw);
    assert!(e
        .message()
        .contains("does not support resource-level applied values"));
    // Its account-level value is still readable, at the ACCOUNT level.
    let q = call(
        &s,
        "GetServiceQuota",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-43872EB7" }),
    );
    assert_eq!(q["Quota"]["QuotaAppliedAtLevel"], "ACCOUNT");
}

#[test]
fn list_service_quotas_quota_code_filter() {
    let s = svc();
    let one = call(
        &s,
        "ListServiceQuotas",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-1216C47A" }),
    );
    let quotas = one["Quotas"].as_array().unwrap();
    assert_eq!(quotas.len(), 1);
    assert_eq!(quotas[0]["QuotaCode"], "L-1216C47A");
    assert!(one.get("NextToken").is_none());
    let none = call(
        &s,
        "ListServiceQuotas",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-00000000" }),
    );
    assert_eq!(none["Quotas"], json!([]));
    let resource = call(
        &s,
        "ListServiceQuotas",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-43872EB7", "QuotaAppliedAtLevel": "RESOURCE" }),
    );
    assert_eq!(resource["Quotas"], json!([]));
}

/// Walk every page of a list operation and return the items under `key`.
fn all_pages(s: &ServiceQuotasService, action: &str, body: Value, key: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut body = body;
    loop {
        let page = call(s, action, body.clone());
        let items = page[key].as_array().unwrap();
        assert!(items.len() <= 100);
        out.extend(items.iter().cloned());
        match page.get("NextToken").and_then(Value::as_str) {
            Some(t) => body["NextToken"] = json!(t),
            None => return out,
        }
    }
}

#[test]
fn list_operations_page_through_the_whole_catalog() {
    let s = svc();
    let services = all_pages(&s, "ListServices", json!({}), "Services");
    assert_eq!(services.len(), catalog::services().len());
    assert!(services.len() > 300);
    let codes: Vec<&str> = services
        .iter()
        .map(|v| v["ServiceCode"].as_str().unwrap())
        .collect();
    assert!(codes.windows(2).all(|w| w[0] < w[1]));
    assert!(codes.contains(&"AWSCloudMap") && codes.contains(&"vpc"));

    let ec2 = catalog::quotas_of("ec2");
    for action in ["ListAWSDefaultServiceQuotas", "ListServiceQuotas"] {
        let quotas = all_pages(&s, action, json!({ "ServiceCode": "ec2" }), "Quotas");
        assert_eq!(quotas.len(), ec2.len(), "{action}");
        let got: Vec<&str> = quotas
            .iter()
            .map(|q| q["QuotaCode"].as_str().unwrap())
            .collect();
        let want: Vec<&str> = ec2.iter().map(|d| d.quota_code).collect();
        assert_eq!(got, want, "{action}");
    }
    let small = all_pages(
        &s,
        "ListAWSDefaultServiceQuotas",
        json!({ "ServiceCode": "ec2", "MaxResults": 7 }),
        "Quotas",
    );
    assert_eq!(small.len(), ec2.len());
    // A service AWS lists with no quotas has an empty list.
    let none = call(
        &s,
        "ListAWSDefaultServiceQuotas",
        json!({ "ServiceCode": "health-agent" }),
    );
    assert_eq!(none["Quotas"], json!([]));
}

#[test]
fn unknown_service_and_quota_are_no_such_resource() {
    let s = svc();
    let e = call_err(&s, "ListServiceQuotas", json!({ "ServiceCode": "nope" }));
    assert_eq!(e.code(), "NoSuchResourceException");
    assert_eq!(e.status(), StatusCode::NOT_FOUND);
    let e = call_err(&s, "GetServiceQuota", sg_quota("L-00000000"));
    assert_eq!(e.code(), "NoSuchResourceException");
    let e = call_err(&s, "GetServiceQuota", json!({ "ServiceCode": "vpc" }));
    assert_eq!(e.code(), "IllegalArgumentException");
}

#[test]
fn approved_increase_raises_applied_value_and_is_in_history() {
    let s = svc();
    let mut body = sg_quota("L-2AFB9258");
    body["DesiredValue"] = json!(10.0);
    let resp = call(&s, "RequestServiceQuotaIncrease", body);
    assert_eq!(resp["RequestedQuota"]["Status"], "PENDING");
    let id = resp["RequestedQuota"]["Id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 40);

    let got = call(
        &s,
        "GetRequestedServiceQuotaChange",
        json!({ "RequestId": id }),
    );
    assert_eq!(got["RequestedQuota"]["Status"], "APPROVED");
    let requester: Value =
        serde_json::from_str(got["RequestedQuota"]["Requester"].as_str().unwrap()).unwrap();
    assert_eq!(requester["accountId"], "000000000000");

    let applied = call(&s, "GetServiceQuota", sg_quota("L-2AFB9258"));
    assert_eq!(applied["Quota"]["Value"], 10.0);
    // The AWS default is unchanged.
    let def = call(&s, "GetAWSDefaultServiceQuota", sg_quota("L-2AFB9258"));
    assert_eq!(def["Quota"]["Value"], 5.0);

    let hist = call(
        &s,
        "ListRequestedServiceQuotaChangeHistoryByQuota",
        sg_quota("L-2AFB9258"),
    );
    assert_eq!(hist["RequestedQuotas"].as_array().unwrap().len(), 1);
    let none = call(
        &s,
        "ListRequestedServiceQuotaChangeHistory",
        json!({ "Status": "PENDING" }),
    );
    assert!(none["RequestedQuotas"].as_array().unwrap().is_empty());

    // Requests are regional: another region sees neither the request nor the
    // raised value.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let other: Value = rt
        .block_on(s.handle(req_in(
            "000000000000",
            "eu-west-1",
            "GetServiceQuota",
            sg_quota("L-2AFB9258"),
        )))
        .map(|r| serde_json::from_slice(r.body.expect_bytes()).unwrap())
        .unwrap();
    assert_eq!(other["Quota"]["Value"], 5.0);
}

#[test]
fn increase_must_exceed_current_value_and_quota_must_be_adjustable() {
    let s = svc();
    let mut body = sg_quota("L-2AFB9258");
    body["DesiredValue"] = json!(5.0);
    assert_eq!(
        call_err(&s, "RequestServiceQuotaIncrease", body).code(),
        "IllegalArgumentException"
    );
    let mut fixed = sg_quota("L-8312C5BB");
    fixed["DesiredValue"] = json!(200.0);
    assert_eq!(
        call_err(&s, "RequestServiceQuotaIncrease", fixed).code(),
        "IllegalArgumentException"
    );
    // The Lambda function timeout is not increasable on AWS.
    let timeout = json!({
        "ServiceCode": "lambda",
        "QuotaCode": "L-9FEEFFC0",
        "DesiredValue": 1000.0,
    });
    assert_eq!(
        call_err(&s, "RequestServiceQuotaIncrease", timeout).code(),
        "IllegalArgumentException"
    );
    // Function and layer storage is.
    let storage = json!({
        "ServiceCode": "lambda",
        "QuotaCode": "L-2ACBD22F",
        "DesiredValue": 400.0,
    });
    assert_eq!(
        call(&s, "RequestServiceQuotaIncrease", storage)["RequestedQuota"]["Status"],
        "PENDING"
    );
}

#[test]
fn security_group_product_limit_is_not_approved() {
    let s = svc();
    // 16 groups x 60 rules = 960 <= 1000: approved.
    let mut groups = sg_quota("L-2AFB9258");
    groups["DesiredValue"] = json!(16.0);
    call(&s, "RequestServiceQuotaIncrease", groups);
    // 16 x 100 = 1600 > 1000: not approved, value unchanged.
    let mut rules = sg_quota("L-0EA8095F");
    rules["DesiredValue"] = json!(100.0);
    let id = call(&s, "RequestServiceQuotaIncrease", rules)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let got = call(
        &s,
        "GetRequestedServiceQuotaChange",
        json!({ "RequestId": id }),
    );
    assert_eq!(got["RequestedQuota"]["Status"], "NOT_APPROVED");
    let applied = call(&s, "GetServiceQuota", sg_quota("L-0EA8095F"));
    assert_eq!(applied["Quota"]["Value"], 60.0);
    // Above the documented maximum of 16 groups: not approved.
    let s = svc();
    let mut groups = sg_quota("L-2AFB9258");
    groups["DesiredValue"] = json!(17.0);
    let id = call(&s, "RequestServiceQuotaIncrease", groups)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let got = call(
        &s,
        "GetRequestedServiceQuotaChange",
        json!({ "RequestId": id }),
    );
    assert_eq!(got["RequestedQuota"]["Status"], "NOT_APPROVED");
}

#[test]
fn support_case_only_for_pending_requests() {
    let s = svc();
    let e = call_err(&s, "CreateSupportCase", json!({ "RequestId": "abc123" }));
    assert_eq!(e.code(), "NoSuchResourceException");
    let mut body = sg_quota("L-F678F1CE");
    body["DesiredValue"] = json!(10.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let e = call_err(&s, "CreateSupportCase", json!({ "RequestId": id }));
    assert_eq!(e.code(), "InvalidResourceStateException");
    assert_eq!(e.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn list_paginates_with_checked_tokens() {
    let s = svc();
    let first = call(
        &s,
        "ListAWSDefaultServiceQuotas",
        json!({ "ServiceCode": "vpc", "MaxResults": 10 }),
    );
    assert_eq!(first["Quotas"].as_array().unwrap().len(), 10);
    let token = first["NextToken"].as_str().unwrap().to_string();
    let second = call(
        &s,
        "ListAWSDefaultServiceQuotas",
        json!({ "ServiceCode": "vpc", "MaxResults": 100, "NextToken": token }),
    );
    assert_eq!(
        second["Quotas"].as_array().unwrap().len(),
        catalog::quotas_of("vpc").len() - 10
    );
    assert!(second.get("NextToken").is_none());
    let e = call_err(&s, "ListServices", json!({ "NextToken": "garbage" }));
    assert_eq!(e.code(), "InvalidPaginationTokenException");
    let resource_level = call(
        &s,
        "ListServiceQuotas",
        json!({ "ServiceCode": "vpc", "QuotaAppliedAtLevel": "RESOURCE" }),
    );
    assert!(resource_level["Quotas"].as_array().unwrap().is_empty());
}

#[test]
fn tags_on_applied_quotas() {
    let s = svc();
    let arn = "arn:aws:servicequotas:us-east-1:000000000000:vpc/L-2AFB9258";
    call(
        &s,
        "TagResource",
        json!({ "ResourceARN": arn, "Tags": [{ "Key": "team", "Value": "net" }, { "Key": "env", "Value": "dev" }] }),
    );
    call(
        &s,
        "UntagResource",
        json!({ "ResourceARN": arn, "TagKeys": ["env"] }),
    );
    let tags = call(&s, "ListTagsForResource", json!({ "ResourceARN": arn }));
    assert_eq!(tags["Tags"], json!([{ "Key": "team", "Value": "net" }]));

    let foreign = "arn:aws:servicequotas:us-east-1:999999999999:vpc/L-2AFB9258";
    let e = call_err(&s, "ListTagsForResource", json!({ "ResourceARN": foreign }));
    assert_eq!(e.code(), "NoSuchResourceException");
    let e = call_err(
        &s,
        "TagResource",
        json!({ "ResourceARN": arn, "Tags": [{ "Key": "aws:x", "Value": "y" }] }),
    );
    assert_eq!(e.code(), "IllegalArgumentException");
    let many: Vec<Value> = (0..51)
        .map(|i| json!({ "Key": format!("k{i}"), "Value": "v" }))
        .collect();
    let e = call_err(
        &s,
        "TagResource",
        json!({ "ResourceARN": arn, "Tags": many }),
    );
    assert_eq!(e.code(), "TooManyTagsException");
}

#[test]
fn auto_management_lifecycle() {
    let s = svc();
    assert_eq!(
        call(&s, "GetAutoManagementConfiguration", json!({}))["OptInStatus"],
        "DISABLED"
    );
    assert_eq!(
        call_err(
            &s,
            "UpdateAutoManagement",
            json!({ "OptInType": "NotifyOnly" })
        )
        .code(),
        "NoSuchResourceException"
    );
    call(
        &s,
        "StartAutoManagement",
        json!({ "OptInLevel": "ACCOUNT", "OptInType": "NotifyOnly", "ExclusionList": { "vpc": ["L-2AFB9258"] } }),
    );
    call(
        &s,
        "UpdateAutoManagement",
        json!({ "OptInType": "NotifyAndAdjust" }),
    );
    let cfg = call(&s, "GetAutoManagementConfiguration", json!({}));
    assert_eq!(cfg["OptInStatus"], "ENABLED");
    assert_eq!(cfg["OptInType"], "NotifyAndAdjust");
    assert_eq!(
        cfg["ExclusionList"]["vpc"][0]["QuotaName"],
        "Security groups per network interface"
    );
    call(&s, "StopAutoManagement", json!({}));
    assert_eq!(
        call(&s, "GetAutoManagementConfiguration", json!({}))["OptInStatus"],
        "DISABLED"
    );
    assert_eq!(
        call_err(
            &s,
            "StartAutoManagement",
            json!({ "OptInLevel": "ACCOUNT", "OptInType": "NotifyOnly", "ExclusionList": { "vpc": ["L-00000000"] } }),
        )
        .code(),
        "NoSuchResourceException"
    );
}

struct FixedUsage;

impl QuotaUsageSource for FixedUsage {
    fn service_codes(&self) -> &[&str] {
        &["vpc"]
    }
    fn usage(&self, _: &str, _: &str, _: &str, quota_code: &str) -> Option<f64> {
        (quota_code == "L-F678F1CE").then_some(2.0)
    }
}

#[test]
fn utilization_report_uses_measured_usage() {
    let s = svc().with_usage_source(Arc::new(FixedUsage));
    let started = call(&s, "StartQuotaUtilizationReport", json!({}));
    let id = started["ReportId"].as_str().unwrap().to_string();
    let report = call(&s, "GetQuotaUtilizationReport", json!({ "ReportId": id }));
    assert_eq!(report["Status"], "COMPLETED");
    assert_eq!(report["TotalCount"], 1);
    assert_eq!(report["Quotas"][0]["QuotaCode"], "L-F678F1CE");
    assert_eq!(report["Quotas"][0]["Utilization"], 40.0);
    let e = call_err(
        &s,
        "GetQuotaUtilizationReport",
        json!({ "ReportId": "missing1" }),
    );
    assert_eq!(e.code(), "NoSuchResourceException");
}

fn org_with_member(joined_offset_secs: i64) -> SharedOrganizationsState {
    let o = orgs();
    let mut org = OrganizationState::bootstrap_in("us-east-1", MGMT);
    let joined = Utc::now() + chrono::Duration::seconds(joined_offset_secs);
    org.accounts.insert(
        MEMBER.to_string(),
        MemberAccount {
            id: MEMBER.to_string(),
            arn: org.account_arn(MEMBER),
            email: format!("{MEMBER}@example.com"),
            name: "member".into(),
            status: "ACTIVE".into(),
            joined_method: "CREATED".into(),
            joined_timestamp: joined,
            parent_id: org.root_id.clone(),
            gov_cloud_mirror: false,
        },
    );
    o.write().insert(org);
    o
}

#[test]
fn template_requires_management_account_in_us_east_1() {
    let s = svc();
    let e = call_err(&s, "AssociateServiceQuotaTemplate", json!({}));
    assert_eq!(e.code(), "NoAvailableOrganizationException");

    let o = org_with_member(0);
    let s = svc_with(o);
    let e = run(&s, MEMBER, "AssociateServiceQuotaTemplate", json!({})).unwrap_err();
    assert_eq!(e.code(), "AccessDeniedException");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let Err(e) = rt.block_on(s.handle(req_in(
        MGMT,
        "eu-west-1",
        "AssociateServiceQuotaTemplate",
        json!({}),
    ))) else {
        panic!("template ops are us-east-1 only");
    };
    assert_eq!(e.code(), "TemplatesNotAvailableInRegionException");
    let e = run(&s, MGMT, "GetAssociationForServiceQuotaTemplate", json!({})).unwrap_err();
    assert_eq!(e.code(), "ServiceQuotaTemplateNotInUseException");
}

#[test]
fn template_entries_crud_and_limit() {
    let o = org_with_member(0);
    let s = svc_with(o);
    let entry = |code: &str| json!({ "ServiceCode": "vpc", "QuotaCode": code, "AwsRegion": "us-east-1", "DesiredValue": 10.0 });
    let put = run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        entry("L-2AFB9258"),
    )
    .unwrap();
    assert_eq!(
        put["ServiceQuotaIncreaseRequestInTemplate"]["QuotaName"],
        "Security groups per network interface"
    );
    let key = json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1" });
    let got = run(
        &s,
        MGMT,
        "GetServiceQuotaIncreaseRequestFromTemplate",
        key.clone(),
    )
    .unwrap();
    assert_eq!(
        got["ServiceQuotaIncreaseRequestInTemplate"]["DesiredValue"],
        10.0
    );
    let codes: Vec<&str> = catalog::quotas_of("vpc")
        .iter()
        .filter(|d| d.adjustable && d.quota_code != "L-2AFB9258")
        .map(|d| d.quota_code)
        .take(9)
        .collect();
    for c in &codes {
        run(
            &s,
            MGMT,
            "PutServiceQuotaIncreaseRequestIntoTemplate",
            entry(c),
        )
        .unwrap();
    }
    let list = run(
        &s,
        MGMT,
        "ListServiceQuotaIncreaseRequestsInTemplate",
        json!({}),
    )
    .unwrap();
    assert_eq!(
        list["ServiceQuotaIncreaseRequestInTemplateList"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
    let e = run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "ec2", "QuotaCode": "L-0263D0A3", "AwsRegion": "us-east-1", "DesiredValue": 10.0 }),
    )
    .unwrap_err();
    assert_eq!(e.code(), "QuotaExceededException");
    run(
        &s,
        MGMT,
        "DeleteServiceQuotaIncreaseRequestFromTemplate",
        key.clone(),
    )
    .unwrap();
    let e = run(&s, MGMT, "GetServiceQuotaIncreaseRequestFromTemplate", key).unwrap_err();
    assert_eq!(e.code(), "NoSuchResourceException");
}

#[test]
fn associated_template_applies_to_accounts_created_afterwards() {
    // The member joined an hour after the association below.
    let o = org_with_member(3600);
    let s = svc_with(o.clone());
    run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    assert_eq!(
        run(&s, MGMT, "GetAssociationForServiceQuotaTemplate", json!({})).unwrap()
            ["ServiceQuotaTemplateAssociationStatus"],
        "ASSOCIATED"
    );
    let org_id = o.read().org_of_account(MGMT).unwrap().org_id.clone();
    assert!(o
        .read()
        .org_by_id(&org_id)
        .unwrap()
        .trusted_services
        .contains_key("servicequotas.amazonaws.com"));

    let member_quota = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(member_quota["Quota"]["Value"], 8.0);
    let hist = run(
        &s,
        MEMBER,
        "ListRequestedServiceQuotaChangeHistory",
        json!({}),
    )
    .unwrap();
    assert_eq!(hist["RequestedQuotas"][0]["Status"], "APPROVED");
    // The management account's own quota is untouched.
    let mgmt_quota = run(&s, MGMT, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(mgmt_quota["Quota"]["Value"], 5.0);

    run(&s, MGMT, "DisassociateServiceQuotaTemplate", json!({})).unwrap();
    assert_eq!(
        run(&s, MGMT, "GetAssociationForServiceQuotaTemplate", json!({})).unwrap()
            ["ServiceQuotaTemplateAssociationStatus"],
        "DISASSOCIATED"
    );
    let e = run(&s, MGMT, "DisassociateServiceQuotaTemplate", json!({})).unwrap_err();
    assert_eq!(e.code(), "ServiceQuotaTemplateNotInUseException");
}

#[test]
fn template_skips_accounts_that_joined_before_association() {
    let o = org_with_member(-3600);
    let s = svc_with(o);
    run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    let member_quota = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(member_quota["Quota"]["Value"], 5.0);
}

#[test]
fn provider_reports_applied_values() {
    use fakecloud_core::quota::QuotaProvider;
    let s = svc();
    let mut body = sg_quota("L-0EA8095F");
    body["DesiredValue"] = json!(100.0);
    call(&s, "RequestServiceQuotaIncrease", body);
    let p = provider(&s);
    assert_eq!(
        p.applied_value("000000000000", "us-east-1", "vpc", "L-0EA8095F"),
        Some(100.0)
    );
    assert_eq!(
        p.applied_value("000000000000", "us-east-1", "vpc", "L-2AFB9258"),
        Some(5.0)
    );
    assert_eq!(
        p.applied_value("000000000000", "us-east-1", "vpc", "L-0"),
        None
    );
}

#[test]
fn template_entries_are_held_to_the_approval_rules() {
    let o = org_with_member(3600);
    let s = svc_with(o);
    for (code, value) in [("L-2AFB9258", 16.0), ("L-0EA8095F", 200.0)] {
        run(
            &s,
            MGMT,
            "PutServiceQuotaIncreaseRequestIntoTemplate",
            json!({ "ServiceCode": "vpc", "QuotaCode": code, "AwsRegion": "us-east-1", "DesiredValue": value }),
        )
        .unwrap();
    }
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    let groups = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap()["Quota"]
        ["Value"]
        .as_f64()
        .unwrap();
    let rules = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-0EA8095F")).unwrap()["Quota"]
        ["Value"]
        .as_f64()
        .unwrap();
    assert!(
        groups * rules <= catalog::SG_RULES_PRODUCT_LIMIT,
        "{groups} x {rules}"
    );
    let hist = run(
        &s,
        MEMBER,
        "ListRequestedServiceQuotaChangeHistory",
        json!({ "Status": "NOT_APPROVED" }),
    )
    .unwrap();
    assert_eq!(hist["RequestedQuotas"].as_array().unwrap().len(), 1);
}

#[test]
fn template_applied_on_membership_change_survives_disassociation() {
    let o = org_with_member(3600);
    let s = svc_with(o);
    run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    // The account is created (Organizations fires its change hooks) before it
    // ever calls Service Quotas, and the template is disassociated after.
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(s.apply_templates_to_org_members());
    run(&s, MGMT, "DisassociateServiceQuotaTemplate", json!({})).unwrap();
    let member_quota = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(member_quota["Quota"]["Value"], 8.0);
}

fn provider(s: &ServiceQuotasService) -> crate::ServiceQuotasProvider {
    crate::ServiceQuotasProvider::new(s.state.clone(), s.orgs.clone(), s.settings.clone())
}

const ACCT: &str = "000000000000";

fn firehose_call(
    s: &ServiceQuotasService,
    account: &str,
    region: &str,
    action: &str,
    body: Value,
) -> Value {
    let resp = dispatch(s, &req_in(account, region, action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[test]
fn firehose_quota_gets_lists_and_introspection_use_regional_defaults() {
    let s = svc();
    for (region, expected) in [
        ("us-east-1", [2000.0, 500_000.0, 5.0]),
        ("us-west-2", [2000.0, 500_000.0, 5.0]),
        ("eu-west-1", [2000.0, 500_000.0, 5.0]),
        ("us-east-2", [1000.0, 100_000.0, 1.0]),
    ] {
        for (code, expected) in ["L-7681C790", "L-17D4EADE", "L-937C879B"]
            .into_iter()
            .zip(expected)
        {
            let body = json!({"ServiceCode": "firehose", "QuotaCode": code});
            for action in ["GetAWSDefaultServiceQuota", "GetServiceQuota"] {
                let q = firehose_call(&s, ACCT, region, action, body.clone());
                assert_eq!(q["Quota"]["Value"], expected, "{action}/{region}/{code}");
                assert_eq!(q["Quota"]["Adjustable"], false);
                assert_eq!(q["Quota"]["GlobalQuota"], false);
            }
            for action in ["ListAWSDefaultServiceQuotas", "ListServiceQuotas"] {
                let list =
                    firehose_call(&s, ACCT, region, action, json!({"ServiceCode": "firehose"}));
                let q = list["Quotas"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|q| q["QuotaCode"] == code)
                    .unwrap();
                assert_eq!(q["Value"], expected, "{action}/{region}/{code}");
                assert_eq!(q["Adjustable"], false);
            }
            let list = s
                .introspect_quotas(None, Some(region), Some("firehose"))
                .unwrap();
            let q = list["quotas"]
                .as_array()
                .unwrap()
                .iter()
                .find(|q| q["quotaCode"] == code)
                .unwrap();
            assert_eq!(q["defaultValue"], expected, "{region}/{code}");
            assert_eq!(q["appliedValue"], expected);
            assert_eq!(q["enforceable"], true);
            assert_eq!(q["enforced"], false);
        }
        // Nearby quotas must not acquire Firehose throughput defaults.
        for (service, code, expected) in [
            ("firehose", "L-14BB0BE7", 5000.0),
            ("vpc", "L-F678F1CE", 5.0),
        ] {
            for action in ["GetAWSDefaultServiceQuota", "GetServiceQuota"] {
                let q = firehose_call(
                    &s,
                    ACCT,
                    region,
                    action,
                    json!({"ServiceCode": service, "QuotaCode": code}),
                );
                assert_eq!(q["Quota"]["Value"], expected);
            }
        }
    }
}

#[test]
fn firehose_local_values_are_unrestricted_scoped_and_deleted_to_regional_defaults() {
    use fakecloud_core::quota::QuotaProvider;
    let s = svc();
    let p = provider(&s);
    for (code, high, low) in [
        ("L-7681C790", 2000.0, 1000.0),
        ("L-17D4EADE", 500_000.0, 100_000.0),
        ("L-937C879B", 5.0, 1.0),
    ] {
        for (region, default) in [("us-east-1", high), ("us-east-2", low)] {
            for value in [0.25, 0.0, 1e12, f64::MAX] {
                let view = s
                    .introspect_put_quota(
                        "firehose",
                        code,
                        &PutQuotaRequest {
                            account_id: Some(ACCT.into()),
                            region: Some(region.into()),
                            value: Some(value),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                assert_eq!(view["appliedValue"], value);
                assert_eq!(view["defaultValue"], default);
                assert_eq!(view["enforced"], false);
                assert_eq!(p.applied_value(ACCT, region, "firehose", code), Some(value));
                assert_eq!(p.enforced_limit(ACCT, region, "firehose", code), None);
                for action in ["GetServiceQuota", "ListServiceQuotas"] {
                    let q = firehose_call(
                        &s,
                        ACCT,
                        region,
                        action,
                        json!({"ServiceCode": "firehose", "QuotaCode": code}),
                    );
                    let q = if action == "GetServiceQuota" {
                        &q["Quota"]
                    } else {
                        &q["Quotas"][0]
                    };
                    assert_eq!(q["Value"].as_f64(), Some(value));
                }
                let aws_default = firehose_call(
                    &s,
                    ACCT,
                    region,
                    "GetAWSDefaultServiceQuota",
                    json!({"ServiceCode": "firehose", "QuotaCode": code}),
                );
                assert_eq!(aws_default["Quota"]["Value"], default);
                assert_eq!(
                    p.applied_value(MEMBER, region, "firehose", code),
                    Some(default)
                );
                let other_region = if region == "us-east-1" {
                    "us-east-2"
                } else {
                    "us-east-1"
                };
                let other_default = if region == "us-east-1" { low } else { high };
                assert_eq!(
                    p.applied_value(ACCT, other_region, "firehose", code),
                    Some(other_default)
                );
            }
            s.introspect_put_quota(
                "firehose",
                code,
                &PutQuotaRequest {
                    account_id: Some(ACCT.into()),
                    region: Some(region.into()),
                    enforce: Some(Some(true)),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                p.enforced_limit(ACCT, region, "firehose", code),
                Some(f64::MAX)
            );
            assert_eq!(p.enforced_limit(MEMBER, region, "firehose", code), None);
            let deleted = s
                .introspect_delete_quota("firehose", code, Some(ACCT), Some(region))
                .unwrap();
            assert_eq!(deleted["defaultValue"], default);
            assert_eq!(deleted["appliedValue"], default);
            assert_eq!(deleted["enforced"], false);
            assert_eq!(
                p.applied_value(ACCT, region, "firehose", code),
                Some(default)
            );
            assert_eq!(p.enforced_limit(ACCT, region, "firehose", code), None);
        }
    }
}

#[test]
fn firehose_throughput_quotas_remain_non_adjustable() {
    let s = svc();
    for code in ["L-7681C790", "L-17D4EADE", "L-937C879B"] {
        let err = call_err(
            &s,
            "RequestServiceQuotaIncrease",
            json!({
                "ServiceCode": "firehose", "QuotaCode": code, "DesiredValue": 1_000_000.0,
            }),
        );
        assert_eq!(err.code(), "IllegalArgumentException");
        assert!(err.message().contains("not adjustable"));
        for value in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = s
                .introspect_put_quota(
                    "firehose",
                    code,
                    &PutQuotaRequest {
                        value: Some(value),
                        ..Default::default()
                    },
                )
                .unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
        }
    }
}

#[test]
fn firehose_utilization_reports_use_regional_defaults() {
    struct FirehoseUsage;
    impl QuotaUsageSource for FirehoseUsage {
        fn service_codes(&self) -> &[&str] {
            &["firehose"]
        }
        fn usage(&self, _: &str, _: &str, _: &str, code: &str) -> Option<f64> {
            (code == "L-937C879B").then_some(0.5)
        }
    }
    let s = svc().with_usage_source(Arc::new(FirehoseUsage));
    let started = firehose_call(
        &s,
        ACCT,
        "us-east-2",
        "StartQuotaUtilizationReport",
        json!({}),
    );
    let report = firehose_call(
        &s,
        ACCT,
        "us-east-2",
        "GetQuotaUtilizationReport",
        json!({"ReportId": started["ReportId"]}),
    );
    assert_eq!(report["Quotas"][0]["DefaultValue"], 1.0);
    assert_eq!(report["Quotas"][0]["AppliedValue"], 1.0);
    assert_eq!(report["Quotas"][0]["Utilization"], 50.0);
}

const FIREHOSE_THROUGHPUT: [&str; 3] = ["L-7681C790", "L-17D4EADE", "L-937C879B"];

fn put_value(s: &ServiceQuotasService, account: &str, region: &str, code: &str, v: f64) -> Value {
    s.introspect_put_quota(
        "firehose",
        code,
        &PutQuotaRequest {
            account_id: Some(account.into()),
            region: Some(region.into()),
            value: Some(v),
            ..Default::default()
        },
    )
    .unwrap()
}

fn set_override(s: &ServiceQuotasService, code: &str, account: Option<&str>, on: Option<bool>) {
    s.introspect_put_enforcement(&PutEnforcementRequest {
        enforce_all: None,
        overrides: vec![OverrideChange {
            service_code: "firehose".into(),
            quota_code: code.into(),
            account_id: account.map(Into::into),
            enforce: on,
        }],
    })
    .unwrap();
}

fn enforce_all(s: &ServiceQuotasService, on: bool) {
    s.introspect_put_enforcement(&PutEnforcementRequest {
        enforce_all: Some(on),
        overrides: vec![],
    })
    .unwrap();
}

/// AWS scales Firehose throughput automatically, so switching enforcement on
/// is not enough: only a value the account set explicitly in the region is a
/// limit.
#[test]
fn firehose_throughput_is_enforced_only_at_explicit_values() {
    use fakecloud_core::quota::QuotaProvider;
    let s = svc();
    let p = provider(&s);
    let limit = |account: &str, region: &str, code: &str| {
        p.enforced_limit(account, region, "firehose", code)
    };

    enforce_all(&s, true);
    for code in FIREHOSE_THROUGHPUT {
        for region in ["us-east-1", "us-east-2"] {
            assert_eq!(limit(ACCT, region, code), None, "{region}/{code}");
        }
    }
    // Other enforceable quotas are enforced at their applied value as before.
    assert_eq!(
        p.enforced_limit(ACCT, "us-east-1", "vpc", "L-0EA8095F"),
        Some(60.0)
    );

    enforce_all(&s, false);
    let values = [10.0, 200.0, 0.5];
    for (code, v) in FIREHOSE_THROUGHPUT.into_iter().zip(values) {
        put_value(&s, ACCT, "us-east-2", code, v);
        assert_eq!(limit(ACCT, "us-east-2", code), None, "not switched on");
    }

    enforce_all(&s, true);
    for (code, v) in FIREHOSE_THROUGHPUT.into_iter().zip(values) {
        assert_eq!(limit(ACCT, "us-east-2", code), Some(v), "{code}");
        // The value belongs to its account and region only.
        assert_eq!(limit(ACCT, "us-east-1", code), None);
        assert_eq!(limit(MEMBER, "us-east-2", code), None);
    }
    // A value equal to the AWS default is explicit too.
    put_value(&s, ACCT, "us-east-1", "L-7681C790", 2000.0);
    assert_eq!(limit(ACCT, "us-east-1", "L-7681C790"), Some(2000.0));

    // Overrides switch it on and off as for any enforceable quota.
    enforce_all(&s, false);
    set_override(&s, "L-17D4EADE", None, Some(true));
    assert_eq!(limit(ACCT, "us-east-2", "L-17D4EADE"), Some(200.0));
    assert_eq!(limit(ACCT, "us-east-2", "L-7681C790"), None);
    put_value(&s, MEMBER, "us-east-2", "L-937C879B", 3.0);
    assert_eq!(limit(MEMBER, "us-east-2", "L-937C879B"), None);
    set_override(&s, "L-937C879B", Some(MEMBER), Some(true));
    assert_eq!(limit(MEMBER, "us-east-2", "L-937C879B"), Some(3.0));
    assert_eq!(limit(ACCT, "us-east-2", "L-937C879B"), None);
    set_override(&s, "L-17D4EADE", Some(ACCT), Some(false));
    assert_eq!(limit(ACCT, "us-east-2", "L-17D4EADE"), None);

    // Deleting the explicit value stops enforcement, even when switched on.
    set_override(&s, "L-17D4EADE", Some(ACCT), None);
    enforce_all(&s, true);
    for (code, v) in FIREHOSE_THROUGHPUT.into_iter().zip(values) {
        assert_eq!(limit(ACCT, "us-east-2", code), Some(v));
        s.introspect_delete_quota("firehose", code, None, Some("us-east-2"))
            .unwrap();
        assert_eq!(limit(ACCT, "us-east-2", code), None, "{code} deleted");
    }
}

#[test]
fn firehose_introspection_reports_auto_scaling_defaults_as_unenforced() {
    let s = svc();
    let view = |account: &str, region: &str, code: &str| -> Value {
        s.introspect_quotas(Some(account), Some(region), Some("firehose"))
            .unwrap()["quotas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["quotaCode"] == code)
            .unwrap()
            .clone()
    };
    let state = |v: &Value| (v["enforced"].clone(), v["enforcementSource"].clone());
    let off = |source: &str| (json!(false), json!(source));
    let on = |source: &str| (json!(true), json!(source));

    for code in FIREHOSE_THROUGHPUT {
        assert_eq!(state(&view(ACCT, "us-east-2", code)), off("global"));
    }
    enforce_all(&s, true);
    for code in FIREHOSE_THROUGHPUT {
        assert_eq!(
            state(&view(ACCT, "us-east-2", code)),
            off("auto_scaling_default"),
            "{code}"
        );
    }
    let vpc = s
        .introspect_quotas(Some(ACCT), Some("us-east-2"), Some("vpc"))
        .unwrap();
    let rules = vpc["quotas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["quotaCode"] == "L-0EA8095F")
        .unwrap();
    assert_eq!(state(rules), on("global"));

    let code = "L-937C879B";
    let put = put_value(&s, ACCT, "us-east-2", code, 2.0);
    assert_eq!(state(&put), on("global"));
    assert_eq!(state(&view(ACCT, "us-east-2", code)), on("global"));
    assert_eq!(
        state(&view(ACCT, "us-east-1", code)),
        off("auto_scaling_default")
    );
    assert_eq!(
        state(&view(MEMBER, "us-east-2", code)),
        off("auto_scaling_default")
    );
    set_override(&s, code, None, Some(true));
    assert_eq!(state(&view(ACCT, "us-east-2", code)), on("override"));
    assert_eq!(
        state(&view(ACCT, "us-east-1", code)),
        off("auto_scaling_default")
    );
    set_override(&s, code, Some(ACCT), Some(true));
    assert_eq!(
        state(&view(ACCT, "us-east-2", code)),
        on("account_override")
    );
    set_override(&s, code, Some(ACCT), Some(false));
    assert_eq!(
        state(&view(ACCT, "us-east-2", code)),
        off("account_override")
    );
    set_override(&s, code, Some(ACCT), None);
    set_override(&s, code, None, None);
    enforce_all(&s, false);
    assert_eq!(state(&view(ACCT, "us-east-2", code)), off("global"));

    enforce_all(&s, true);
    let deleted = s
        .introspect_delete_quota("firehose", code, Some(ACCT), Some("us-east-2"))
        .unwrap();
    assert_eq!(state(&deleted), off("auto_scaling_default"));
}

/// Another service's limit lookup of an enforceable quota applies a due
/// Organizations template, whether or not it enforces anything, and applies
/// it once.
#[test]
fn enforced_limit_applies_a_due_template_even_when_unenforced() {
    use fakecloud_core::quota::QuotaProvider;
    let o = org_with_member(3600);
    let s = svc_with(o);
    run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    let p = provider(&s);
    assert_eq!(
        p.enforced_limit(MEMBER, "us-east-1", "vpc", "L-2AFB9258"),
        None
    );
    assert_eq!(
        s.state.read().get(MEMBER).unwrap().requests.len(),
        1,
        "template not applied"
    );
    // A quota no service checks never touches the account's quotas.
    let s2 = svc_with(org_with_member(3600));
    run(
        &s2,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s2, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    assert_eq!(
        provider(&s2).enforced_limit(MEMBER, "us-east-1", "lambda", "L-B99A9384"),
        None
    );
    assert!(s2.state.read().get(MEMBER).is_none(), "template applied");

    enforce_all(&s, true);
    assert_eq!(
        p.enforced_limits(MEMBER, "us-east-1", "vpc", &["L-2AFB9258", "L-0EA8095F"]),
        [Some(8.0), Some(60.0)]
    );
    assert_eq!(s.state.read().get(MEMBER).unwrap().requests.len(), 1);
    assert_eq!(
        p.enforced_limit(MEMBER, "us-east-1", "vpc", "L-2AFB9258"),
        Some(8.0)
    );
    assert_eq!(s.state.read().get(MEMBER).unwrap().requests.len(), 1);
}

#[test]
fn nothing_is_enforced_until_switched_on() {
    use fakecloud_core::quota::QuotaProvider;
    let s = svc();
    let p = provider(&s);
    assert_eq!(
        p.enforced_limit(ACCT, "us-east-1", "vpc", "L-0EA8095F"),
        None
    );

    // The global switch turns on every enforceable quota.
    s.introspect_put_enforcement(&PutEnforcementRequest {
        enforce_all: Some(true),
        overrides: vec![],
    })
    .unwrap();
    assert_eq!(
        p.enforced_limit(ACCT, "us-east-1", "vpc", "L-0EA8095F"),
        Some(60.0)
    );
    // A quota no service checks stays unenforced.
    assert_eq!(
        p.enforced_limit(ACCT, "us-east-1", "lambda", "L-B99A9384"),
        None
    );

    // A server-wide ignore beats the global switch; an account override
    // beats both.
    s.introspect_put_enforcement(&PutEnforcementRequest {
        enforce_all: None,
        overrides: vec![
            OverrideChange {
                service_code: "vpc".into(),
                quota_code: "L-0EA8095F".into(),
                account_id: None,
                enforce: Some(false),
            },
            OverrideChange {
                service_code: "vpc".into(),
                quota_code: "L-0EA8095F".into(),
                account_id: Some(MEMBER.into()),
                enforce: Some(true),
            },
        ],
    })
    .unwrap();
    assert_eq!(
        p.enforced_limit(ACCT, "us-east-1", "vpc", "L-0EA8095F"),
        None
    );
    assert_eq!(
        p.enforced_limit(MEMBER, "us-east-1", "vpc", "L-0EA8095F"),
        Some(60.0)
    );
    let view = s.introspect_enforcement();
    assert_eq!(view["enforceAll"], true);
    assert_eq!(view["overrides"][0]["enforce"], false);
    assert_eq!(view["accountOverrides"][0]["accountId"], MEMBER);
}

#[test]
fn put_quota_sets_values_below_the_default_and_enforces() {
    use fakecloud_core::quota::QuotaProvider;
    let s = svc();
    let body: PutQuotaRequest =
        serde_json::from_value(json!({ "value": 2.0, "enforce": true })).unwrap();
    let view = s.introspect_put_quota("vpc", "L-2AFB9258", &body).unwrap();
    assert_eq!(view["appliedValue"], 2.0);
    assert_eq!(view["defaultValue"], 5.0);
    assert_eq!(view["enforced"], true);
    assert_eq!(view["enforcementSource"], "override");
    assert_eq!(
        provider(&s).enforced_limit(ACCT, "us-east-1", "vpc", "L-2AFB9258"),
        Some(2.0)
    );
    // The Service Quotas API reports the same applied value.
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(q["Quota"]["Value"], 2.0);

    // An explicit null clears the override but keeps the value.
    let body: PutQuotaRequest = serde_json::from_value(json!({ "enforce": null })).unwrap();
    let view = s.introspect_put_quota("vpc", "L-2AFB9258", &body).unwrap();
    assert_eq!(view["enforced"], false);
    assert_eq!(view["appliedValue"], 2.0);

    // DELETE restores the AWS default.
    let view = s
        .introspect_delete_quota("vpc", "L-2AFB9258", None, None)
        .unwrap();
    assert_eq!(view["appliedValue"], 5.0);
}

#[test]
fn put_quota_rejects_bad_input() {
    let s = svc();
    let put = |svc_code: &str, code: &str, body: Value| {
        let body: PutQuotaRequest = serde_json::from_value(body).unwrap();
        s.introspect_put_quota(svc_code, code, &body).unwrap_err()
    };
    assert_eq!(
        put("vpc", "L-NOPE", json!({"value": 1.0})).status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        put("vpc", "L-2AFB9258", json!({})).status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        put("vpc", "L-2AFB9258", json!({"value": -1.0})).status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        put(
            "vpc",
            "L-2AFB9258",
            json!({"value": 1.0, "accountId": "abc"})
        )
        .status,
        StatusCode::BAD_REQUEST
    );
    // Switching on a quota no service checks would silently do nothing.
    let e = put("lambda", "L-B99A9384", json!({"enforce": true}));
    assert_eq!(e.status, StatusCode::BAD_REQUEST);
    assert!(e.message.contains("not enforceable"), "{}", e.message);
    assert!(serde_json::from_value::<PutQuotaRequest>(json!({"bogus": 1})).is_err());
}

#[test]
fn global_quota_values_ignore_the_region() {
    let s = svc();
    let body: PutQuotaRequest =
        serde_json::from_value(json!({ "value": 3.0, "region": "eu-west-1" })).unwrap();
    s.introspect_put_quota("iam", "L-FE177D64", &body).unwrap();
    let all = s
        .introspect_quotas(None, Some("us-west-2"), Some("iam"))
        .unwrap();
    let roles = all["quotas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["quotaCode"] == "L-FE177D64")
        .unwrap();
    assert_eq!(roles["appliedValue"], 3.0);
    assert_eq!(roles["global"], true);
}

#[test]
fn introspect_quotas_lists_the_catalog_with_usage() {
    struct Count;
    impl QuotaUsageSource for Count {
        fn service_codes(&self) -> &[&str] {
            &["vpc"]
        }
        fn usage(&self, _: &str, _: &str, _: &str, quota_code: &str) -> Option<f64> {
            (quota_code == "L-F678F1CE").then_some(3.0)
        }
    }
    let s = svc().with_usage_source(Arc::new(Count));
    let all = s.introspect_quotas(None, None, None).unwrap();
    assert_eq!(all["accountId"], ACCT);
    assert_eq!(all["region"], "us-east-1");
    // Without a service code: the enforceable quotas, plus the measured one
    // and the ones the account changed, not the whole catalog.
    let codes = |v: &Value| -> Vec<String> {
        v["quotas"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| {
                format!(
                    "{}/{}",
                    q["serviceCode"].as_str().unwrap(),
                    q["quotaCode"].as_str().unwrap()
                )
            })
            .collect()
    };
    let listed = codes(&all);
    assert_eq!(listed.len(), catalog::ENFORCEABLE.len());
    for (svc, code) in catalog::ENFORCEABLE {
        assert!(listed.contains(&format!("{svc}/{code}")), "{svc}/{code}");
    }
    s.introspect_put_quota(
        "lambda",
        "L-B99A9384",
        &PutQuotaRequest {
            value: Some(2000.0),
            ..Default::default()
        },
    )
    .unwrap();
    let listed = codes(&s.introspect_quotas(None, None, None).unwrap());
    assert_eq!(listed.len(), catalog::ENFORCEABLE.len() + 1);
    assert!(listed.contains(&"lambda/L-B99A9384".to_string()));
    // An applied value set in another region counts as a change too.
    s.introspect_put_quota(
        "vpc",
        "L-45FE3B85",
        &PutQuotaRequest {
            region: Some("eu-west-1".into()),
            value: Some(9.0),
            ..Default::default()
        },
    )
    .unwrap();
    // So does an open increase request.
    s.introspect_set_request_approval("manual").unwrap();
    let mut open = sg_quota("L-2C462E13");
    open["DesiredValue"] = json!(150.0);
    call(&s, "RequestServiceQuotaIncrease", open);
    let listed = codes(&s.introspect_quotas(None, None, None).unwrap());
    assert!(listed.contains(&"vpc/L-45FE3B85".to_string()));
    assert!(listed.contains(&"vpc/L-2C462E13".to_string()));
    assert_eq!(listed.len(), catalog::ENFORCEABLE.len() + 3);
    // A measured quota that is not enforceable shows up too.
    struct Eigw;
    impl QuotaUsageSource for Eigw {
        fn service_codes(&self) -> &[&str] {
            &["vpc"]
        }
        fn usage(&self, _: &str, _: &str, _: &str, quota_code: &str) -> Option<f64> {
            (quota_code == "L-45FE3B85").then_some(1.0)
        }
    }
    let measured = svc().with_usage_source(Arc::new(Eigw));
    let listed = codes(&measured.introspect_quotas(None, None, None).unwrap());
    assert!(listed.contains(&"vpc/L-45FE3B85".to_string()));
    assert_eq!(listed.len(), catalog::ENFORCEABLE.len() + 1);
    // With a service code, every quota of the service.
    assert_eq!(
        s.introspect_quotas(None, None, Some("ec2")).unwrap()["quotas"]
            .as_array()
            .unwrap()
            .len(),
        catalog::quotas_of("ec2").len()
    );
    let vpc = s.introspect_quotas(None, None, Some("vpc")).unwrap();
    let vpcs = vpc["quotas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["quotaCode"] == "L-F678F1CE")
        .unwrap()
        .clone();
    assert_eq!(vpcs["usage"], 3.0);
    assert_eq!(vpcs["enforced"], false);
    assert_eq!(vpcs["enforcementSource"], "global");
    let eigws = vpc["quotas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["quotaCode"] == "L-45FE3B85")
        .unwrap()
        .clone();
    assert_eq!(eigws["enforcementSource"], "not_enforceable");
    assert_eq!(
        s.introspect_quotas(None, None, Some("nope"))
            .unwrap_err()
            .status,
        StatusCode::NOT_FOUND
    );
}

#[test]
fn manual_approval_holds_requests_pending_until_decided() {
    let s = svc();
    s.introspect_set_request_approval("manual").unwrap();
    assert_eq!(s.introspect_request_approval()["mode"], "manual");
    let mut body = sg_quota("L-0EA8095F");
    body["DesiredValue"] = json!(100.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let status = |s: &ServiceQuotasService| {
        run(
            s,
            ACCT,
            "GetRequestedServiceQuotaChange",
            json!({ "RequestId": id }),
        )
        .unwrap()["RequestedQuota"]["Status"]
            .clone()
    };
    assert_eq!(status(&s), "PENDING");
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-0EA8095F")).unwrap();
    assert_eq!(q["Quota"]["Value"], 60.0);

    // A pending request can open a support case now.
    run(&s, ACCT, "CreateSupportCase", json!({ "RequestId": id })).unwrap();
    assert_eq!(status(&s), "CASE_OPENED");

    let pending = s.introspect_requests(None, Some("CASE_OPENED")).unwrap();
    assert_eq!(pending["requests"][0]["requestId"], id.as_str());

    let decided = s.introspect_decide_request(&id, Decision::Approve).unwrap();
    assert_eq!(decided["status"], "APPROVED");
    assert_eq!(status(&s), "APPROVED");
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-0EA8095F")).unwrap();
    assert_eq!(q["Quota"]["Value"], 100.0);

    // A decided request cannot be decided again.
    let e = s
        .introspect_decide_request(&id, Decision::deny(None).unwrap())
        .unwrap_err();
    assert_eq!(e.status, StatusCode::CONFLICT);
}

#[test]
fn denying_a_request_keeps_the_applied_value() {
    let s = svc();
    s.introspect_set_request_approval("manual").unwrap();
    let mut body = sg_quota("L-0EA8095F");
    body["DesiredValue"] = json!(100.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(Decision::deny(Some("APPROVED")).is_err());
    let decided = s
        .introspect_decide_request(&id, Decision::deny(Some("CASE_CLOSED")).unwrap())
        .unwrap();
    assert_eq!(decided["status"], "CASE_CLOSED");
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-0EA8095F")).unwrap();
    assert_eq!(q["Quota"]["Value"], 60.0);
    assert_eq!(
        s.introspect_decide_request("nope", Decision::Approve)
            .unwrap_err()
            .status,
        StatusCode::NOT_FOUND
    );
    assert!(s.introspect_set_request_approval("sometimes").is_err());
}

#[test]
fn manual_approval_leaves_template_entries_pending() {
    let o = org_with_member(3600);
    let s = svc_with(o);
    s.introspect_set_request_approval("manual").unwrap();
    run(
        &s,
        MGMT,
        "PutServiceQuotaIncreaseRequestIntoTemplate",
        json!({ "ServiceCode": "vpc", "QuotaCode": "L-2AFB9258", "AwsRegion": "us-east-1", "DesiredValue": 8.0 }),
    )
    .unwrap();
    run(&s, MGMT, "AssociateServiceQuotaTemplate", json!({})).unwrap();
    let member_quota = run(&s, MEMBER, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(member_quota["Quota"]["Value"], 5.0);
    let hist = run(
        &s,
        MEMBER,
        "ListRequestedServiceQuotaChangeHistory",
        json!({ "Status": "PENDING" }),
    )
    .unwrap();
    assert_eq!(hist["RequestedQuotas"].as_array().unwrap().len(), 1);
}

#[test]
fn approving_never_lowers_a_quota_set_higher_meanwhile() {
    let s = svc();
    s.introspect_set_request_approval("manual").unwrap();
    let mut body = sg_quota("L-0EA8095F");
    body["DesiredValue"] = json!(100.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let raise: PutQuotaRequest = serde_json::from_value(json!({ "value": 150.0 })).unwrap();
    s.introspect_put_quota("vpc", "L-0EA8095F", &raise).unwrap();
    s.introspect_decide_request(&id, Decision::Approve).unwrap();
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-0EA8095F")).unwrap();
    assert_eq!(q["Quota"]["Value"], 150.0);
}

#[test]
fn manual_approval_still_refuses_values_aws_would_not_approve() {
    let s = svc();
    s.introspect_set_request_approval("manual").unwrap();
    // 100 groups per interface is past the documented maximum of 16.
    let mut body = sg_quota("L-2AFB9258");
    body["DesiredValue"] = json!(100.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = run(
        &s,
        ACCT,
        "GetRequestedServiceQuotaChange",
        json!({ "RequestId": id }),
    )
    .unwrap();
    assert_eq!(r["RequestedQuota"]["Status"], "NOT_APPROVED");

    // An approvable request that stops being approvable while it waits
    // (rules per group raised so groups x rules would pass 1000) cannot be
    // approved.
    let mut body = sg_quota("L-2AFB9258");
    body["DesiredValue"] = json!(10.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    let rules: PutQuotaRequest = serde_json::from_value(json!({ "value": 200.0 })).unwrap();
    s.introspect_put_quota("vpc", "L-0EA8095F", &rules).unwrap();
    let e = s
        .introspect_decide_request(&id, Decision::Approve)
        .unwrap_err();
    assert_eq!(e.status, StatusCode::CONFLICT);
    assert!(s
        .introspect_decide_request(&id, Decision::deny(None).unwrap())
        .is_ok());
}

#[test]
fn an_override_on_a_quota_no_service_checks_is_refused() {
    let s = svc();
    for enforce in [json!(true), json!(false)] {
        let body: PutQuotaRequest = serde_json::from_value(json!({ "enforce": enforce })).unwrap();
        let e = s
            .introspect_put_quota("lambda", "L-B99A9384", &body)
            .unwrap_err();
        assert_eq!(e.status, StatusCode::BAD_REQUEST);
    }
    // Clearing is always allowed.
    let body: PutQuotaRequest = serde_json::from_value(json!({ "enforce": null })).unwrap();
    assert!(s
        .introspect_put_quota("lambda", "L-B99A9384", &body)
        .is_ok());
}

#[test]
fn approving_a_request_already_met_is_a_no_op_not_a_conflict() {
    let s = svc();
    s.introspect_set_request_approval("manual").unwrap();
    let mut body = sg_quota("L-2AFB9258");
    body["DesiredValue"] = json!(10.0);
    let id = call(&s, "RequestServiceQuotaIncrease", body)["RequestedQuota"]["Id"]
        .as_str()
        .unwrap()
        .to_string();
    // Meanwhile groups go to 12 and rules to 120: 10 x 120 would pass the
    // product limit, but approving changes nothing.
    for (code, v) in [("L-2AFB9258", 12.0), ("L-0EA8095F", 120.0)] {
        let b: PutQuotaRequest = serde_json::from_value(json!({ "value": v })).unwrap();
        s.introspect_put_quota("vpc", code, &b).unwrap();
    }
    let decided = s.introspect_decide_request(&id, Decision::Approve).unwrap();
    assert_eq!(decided["status"], "APPROVED");
    let q = run(&s, ACCT, "GetServiceQuota", sg_quota("L-2AFB9258")).unwrap();
    assert_eq!(q["Quota"]["Value"], 12.0);
}
