use super::*;
use fakecloud_core::multi_account::MultiAccountState;
use parking_lot::RwLock;

fn service() -> PinpointService {
    let state: SharedPinpointState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    PinpointService::new(state)
}

fn ctx() -> Ctx {
    Ctx {
        account: "000000000000".to_string(),
        region: "us-east-1".to_string(),
    }
}

fn body_of(resp: &AwsResponse) -> Value {
    let bytes = resp.body.expect_bytes();
    if bytes.is_empty() {
        return json!({});
    }
    serde_json::from_slice(bytes).unwrap()
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected an error, got a response"),
        Err(e) => e,
    }
}

fn new_app(svc: &PinpointService, ctx: &Ctx, name: &str) -> String {
    let resp = svc.create_app(ctx, &json!({ "Name": name })).unwrap();
    assert_eq!(resp.status, StatusCode::CREATED);
    body_of(&resp)["Id"].as_str().unwrap().to_string()
}

#[test]
fn app_lifecycle() {
    let svc = service();
    let ctx = ctx();
    let create = svc
        .create_app(
            &ctx,
            &json!({ "Name": "demo", "tags": { "team": "growth" } }),
        )
        .unwrap();
    let created = body_of(&create);
    let id = created["Id"].as_str().unwrap().to_string();
    assert_eq!(created["Name"], "demo");
    assert!(created["Arn"].as_str().unwrap().contains(":apps/"));

    let got = body_of(&svc.get_app(&ctx, &id).unwrap());
    assert_eq!(got["Id"], json!(id));

    let apps = body_of(&svc.get_apps(&ctx, &[]).unwrap());
    assert_eq!(apps["Item"].as_array().unwrap().len(), 1);

    svc.delete_app(&ctx, &id).unwrap();
    assert!(svc.get_app(&ctx, &id).is_err());
}

#[test]
fn get_missing_app_is_not_found() {
    let svc = service();
    let err = expect_err(svc.get_app(&ctx(), "nope"));
    assert!(
        matches!(err, AwsServiceError::AwsError { ref code, .. } if code == "NotFoundException")
    );
}

#[test]
fn application_settings_round_trip() {
    let svc = service();
    let ctx = ctx();
    let id = new_app(&svc, &ctx, "s");
    let updated = body_of(
        &svc.update_application_settings(&ctx, &id, &json!({ "QuietTime": { "Start": "22:00" } }))
            .unwrap(),
    );
    assert_eq!(updated["ApplicationId"], json!(id));
    let got = body_of(&svc.get_application_settings(&ctx, &id).unwrap());
    assert_eq!(got["ApplicationId"], json!(id));
    assert!(got.get("QuietTime").is_some());
}

#[test]
fn campaign_versioning() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "c");
    let create = body_of(
        &svc.create_campaign(&ctx, &app, &json!({ "Name": "c1", "SegmentId": "seg" }))
            .unwrap(),
    );
    let cid = create["Id"].as_str().unwrap().to_string();
    assert_eq!(create["Version"], json!(1));
    assert_eq!(create["SegmentId"], "seg");

    svc.update_campaign(&ctx, &app, &cid, &json!({ "Name": "c2" }))
        .unwrap();
    let got = body_of(&svc.get_campaign(&ctx, &app, &cid).unwrap());
    assert_eq!(got["Version"], json!(2));

    let versions = body_of(&svc.get_campaign_versions(&ctx, &app, &cid, &[]).unwrap());
    assert_eq!(versions["Item"].as_array().unwrap().len(), 2);

    let v1 = body_of(&svc.get_campaign_version(&ctx, &app, &cid, "1").unwrap());
    assert_eq!(v1["Version"], json!(1));

    assert!(svc.get_campaign_version(&ctx, &app, &cid, "9").is_err());
}

#[test]
fn segment_type_inference() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "seg");
    let dim = body_of(
        &svc.create_segment(&ctx, &app, &json!({ "Name": "d" }))
            .unwrap(),
    );
    assert_eq!(dim["SegmentType"], "DIMENSIONAL");
    let imp = body_of(
        &svc.create_segment(
            &ctx,
            &app,
            &json!({ "Name": "i", "ImportDefinition": { "S3Url": "s3://x" } }),
        )
        .unwrap(),
    );
    assert_eq!(imp["SegmentType"], "IMPORT");
}

#[test]
fn endpoint_and_user_endpoints() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "e");
    svc.update_endpoint(
        &ctx,
        &app,
        "ep1",
        &json!({ "Address": "a@b.com", "ChannelType": "EMAIL", "User": { "UserId": "u1" } }),
    )
    .unwrap();
    let got = body_of(&svc.get_endpoint(&ctx, &app, "ep1").unwrap());
    assert_eq!(got["Address"], "a@b.com");

    let user = body_of(&svc.get_user_endpoints(&ctx, &app, "u1").unwrap());
    assert_eq!(user["Item"].as_array().unwrap().len(), 1);

    svc.delete_endpoint(&ctx, &app, "ep1").unwrap();
    assert!(svc.get_endpoint(&ctx, &app, "ep1").is_err());
}

#[test]
fn channel_lifecycle() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "ch");
    assert!(svc.get_channel(&ctx, &app, "sms").is_err());
    let updated = body_of(
        &svc.update_channel(&ctx, &app, "sms", &json!({ "Enabled": true }))
            .unwrap(),
    );
    assert_eq!(updated["Platform"], "SMS");
    assert_eq!(updated["Enabled"], json!(true));
    let got = body_of(&svc.get_channel(&ctx, &app, "sms").unwrap());
    assert_eq!(got["Platform"], "SMS");
    let channels = body_of(&svc.get_channels(&ctx, &app).unwrap());
    assert!(channels["Channels"].get("SMS").is_some());
    svc.delete_channel(&ctx, &app, "sms").unwrap();
    assert!(svc.get_channel(&ctx, &app, "sms").is_err());
}

#[test]
fn journey_state_machine() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "j");
    let create = body_of(
        &svc.create_journey(&ctx, &app, &json!({ "Name": "j1" }))
            .unwrap(),
    );
    let jid = create["Id"].as_str().unwrap().to_string();
    assert_eq!(create["State"], "DRAFT");
    let active = body_of(
        &svc.update_journey_state(&ctx, &app, &jid, &json!({ "State": "ACTIVE" }))
            .unwrap(),
    );
    assert_eq!(active["State"], "ACTIVE");
    let list = body_of(&svc.list_journeys(&ctx, &app, &[]).unwrap());
    assert_eq!(list["Item"].as_array().unwrap().len(), 1);
}

#[test]
fn template_versioning() {
    let svc = service();
    let ctx = ctx();
    svc.create_template(&ctx, "tmpl", "EMAIL", &json!({ "Subject": "hi" }))
        .unwrap();
    let got = body_of(&svc.get_template(&ctx, "tmpl", "EMAIL", &[]).unwrap());
    assert_eq!(got["TemplateName"], "tmpl");
    assert_eq!(got["TemplateType"], "EMAIL");
    svc.update_template(
        &ctx,
        "tmpl",
        "EMAIL",
        &json!({ "Subject": "hi2" }),
        &[("CreateNewVersion".to_string(), "true".to_string())],
    )
    .unwrap();
    let versions = body_of(
        &svc.list_template_versions(&ctx, "tmpl", "EMAIL", &[])
            .unwrap(),
    );
    assert_eq!(versions["Item"].as_array().unwrap().len(), 2);
    // Wrong type must not resolve.
    assert!(svc.get_template(&ctx, "tmpl", "SMS", &[]).is_err());
    let list = body_of(&svc.list_templates(&ctx, &[]).unwrap());
    assert_eq!(list["Item"].as_array().unwrap().len(), 1);
}

#[test]
fn import_job_completes() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "job");
    let create = body_of(
        &svc.create_import_job(
            &ctx,
            &app,
            &json!({ "Format": "CSV", "RoleArn": "arn:role", "S3Url": "s3://b/k" }),
        )
        .unwrap(),
    );
    let jid = create["Id"].as_str().unwrap().to_string();
    assert_eq!(create["JobStatus"], "COMPLETED");
    assert_eq!(create["Definition"]["Format"], "CSV");
    let got = body_of(&svc.get_import_job(&ctx, &app, &jid).unwrap());
    assert_eq!(got["Id"], json!(jid));
    let jobs = body_of(&svc.get_import_jobs(&ctx, &app, &[]).unwrap());
    assert_eq!(jobs["Item"].as_array().unwrap().len(), 1);
}

#[test]
fn event_stream_lifecycle() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "es");
    assert!(svc.get_event_stream(&ctx, &app).is_err());
    let put = body_of(
        &svc.put_event_stream(
            &ctx,
            &app,
            &json!({ "DestinationStreamArn": "arn:stream", "RoleArn": "arn:role" }),
        )
        .unwrap(),
    );
    assert_eq!(put["DestinationStreamArn"], "arn:stream");
    let got = body_of(&svc.get_event_stream(&ctx, &app).unwrap());
    assert_eq!(got["RoleArn"], "arn:role");
    svc.delete_event_stream(&ctx, &app).unwrap();
    assert!(svc.get_event_stream(&ctx, &app).is_err());
}

#[test]
fn recommender_lifecycle() {
    let svc = service();
    let ctx = ctx();
    let create = body_of(
        &svc.create_recommender(
            &ctx,
            &json!({ "RecommendationProviderRoleArn": "arn:role", "RecommendationProviderUri": "arn:uri" }),
        )
        .unwrap(),
    );
    let rid = create["Id"].as_str().unwrap().to_string();
    let got = body_of(&svc.get_recommender(&ctx, &rid).unwrap());
    assert_eq!(got["RecommendationProviderRoleArn"], "arn:role");
    let list = body_of(&svc.get_recommenders(&ctx, &[]).unwrap());
    assert_eq!(list["Item"].as_array().unwrap().len(), 1);
    svc.delete_recommender(&ctx, &rid).unwrap();
    assert!(svc.get_recommender(&ctx, &rid).is_err());
}

#[test]
fn tags_never_error() {
    let svc = service();
    let ctx = ctx();
    let arn = "arn:aws:mobiletargeting:us-east-1:000000000000:apps/x";
    svc.tag_resource(&ctx, arn, &json!({ "tags": { "k": "v" } }))
        .unwrap();
    let got = body_of(&svc.list_tags_for_resource(&ctx, arn).unwrap());
    assert_eq!(got["tags"]["k"], "v");
    svc.untag_resource(&ctx, arn, &[("tagKeys".to_string(), "k".to_string())])
        .unwrap();
    let after = body_of(&svc.list_tags_for_resource(&ctx, arn).unwrap());
    assert!(after["tags"].as_object().unwrap().is_empty());
    // Unknown ARN still succeeds with an empty tag set.
    let unknown = body_of(&svc.list_tags_for_resource(&ctx, "arn:unknown").unwrap());
    assert!(unknown["tags"].as_object().unwrap().is_empty());
}

#[test]
fn send_messages_returns_result_per_address() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "m");
    let resp = body_of(
        &svc.send_messages(
            &ctx,
            &app,
            &json!({
                "MessageConfiguration": {},
                "Addresses": { "+15555550100": { "ChannelType": "SMS" } }
            }),
        )
        .unwrap(),
    );
    assert_eq!(resp["ApplicationId"], json!(app));
    assert_eq!(
        resp["Result"]["+15555550100"]["DeliveryStatus"],
        "SUCCESSFUL"
    );
}

#[test]
fn pagination_limits_page() {
    let svc = service();
    let ctx = ctx();
    let app = new_app(&svc, &ctx, "p");
    for _ in 0..3 {
        svc.create_segment(&ctx, &app, &json!({ "Name": "s" }))
            .unwrap();
    }
    let page = body_of(
        &svc.get_segments(&ctx, &app, &[("PageSize".to_string(), "2".to_string())])
            .unwrap(),
    );
    assert_eq!(page["Item"].as_array().unwrap().len(), 2);
    assert!(page.get("NextToken").is_some());
}

#[test]
fn resolve_action_routes_key_paths() {
    let cases = [
        ("POST", "/v1/apps", "CreateApp"),
        ("GET", "/v1/apps/abc", "GetApp"),
        (
            "PUT",
            "/v1/apps/abc/channels/apns_voip_sandbox",
            "UpdateApnsVoipSandboxChannel",
        ),
        (
            "GET",
            "/v1/apps/abc/campaigns/c/versions/2",
            "GetCampaignVersion",
        ),
        ("POST", "/v1/templates/t/email", "CreateEmailTemplate"),
        (
            "GET",
            "/v1/templates/t/EMAIL/versions",
            "ListTemplateVersions",
        ),
        ("GET", "/v1/tags/arn", "ListTagsForResource"),
        ("POST", "/v1/phone/number/validate", "PhoneNumberValidate"),
    ];
    for (method, path, expected) in cases {
        let req = AwsRequest {
            method: Method::from_bytes(method.as_bytes()).unwrap(),
            raw_path: path.to_string(),
            ..test_request()
        };
        let (action, _) = PinpointService::resolve_action(&req).expect(path);
        assert_eq!(action, expected, "path {path}");
    }
}

fn test_request() -> AwsRequest {
    AwsRequest {
        service: "pinpoint".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}
