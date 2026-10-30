//! W2: WAFv2 inspection wired into ELBv2 ALB + API Gateway v1 + v2
//! dataplanes. Each test associates a WebACL with the resource, then
//! drives a real HTTP request through the dataplane to assert that
//! Block / Count actions produce the expected effect.

mod helpers;

use std::time::Duration;

use aws_sdk_elasticloadbalancingv2::types::{
    Action, ActionTypeEnum, FixedResponseActionConfig, ProtocolEnum,
};
use aws_sdk_wafv2::types::{
    ByteMatchStatement, DefaultAction, FieldToMatch, PositionalConstraint, Rule, RuleAction, Scope,
    Statement, TextTransformation, TextTransformationType, UriPath, VisibilityConfig,
};
use helpers::TestServer;

fn vis(name: &str) -> VisibilityConfig {
    VisibilityConfig::builder()
        .sampled_requests_enabled(false)
        .cloud_watch_metrics_enabled(false)
        .metric_name(name)
        .build()
        .unwrap()
}

fn allow_default() -> DefaultAction {
    DefaultAction::builder()
        .allow(aws_sdk_wafv2::types::AllowAction::builder().build())
        .build()
}

/// Build a `ByteMatchStatement` that matches when the URI path
/// starts with `needle`. Used as the bedrock match shape for both
/// Block and Count rules in the W2 tests below.
fn uri_starts_with(needle: &str) -> Statement {
    Statement::builder()
        .byte_match_statement(
            ByteMatchStatement::builder()
                .search_string(aws_smithy_types::Blob::new(needle.as_bytes()))
                .field_to_match(
                    FieldToMatch::builder()
                        .uri_path(UriPath::builder().build())
                        .build(),
                )
                .text_transformations(
                    TextTransformation::builder()
                        .priority(0)
                        .r#type(TextTransformationType::None)
                        .build()
                        .unwrap(),
                )
                .positional_constraint(PositionalConstraint::StartsWith)
                .build()
                .unwrap(),
        )
        .build()
}

fn block_uri_rule(name: &str, needle: &str) -> Rule {
    Rule::builder()
        .name(name)
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis(name))
        .statement(uri_starts_with(needle))
        .build()
        .expect("rule")
}

fn count_uri_rule(name: &str, needle: &str) -> Rule {
    Rule::builder()
        .name(name)
        .priority(0)
        .action(
            RuleAction::builder()
                .count(aws_sdk_wafv2::types::CountAction::builder().build())
                .build(),
        )
        .visibility_config(vis(name))
        .statement(uri_starts_with(needle))
        .build()
        .expect("rule")
}

/// Wait for an ALB's data-plane port to be bound + reported via the
/// admin endpoint. ELBv2 supervisor binds on a 1s tick, so a few-
/// second deadline is plenty.
async fn wait_for_bound_port(server: &TestServer, lb_arn: &str, deadline: Duration) -> Option<u16> {
    let url = format!("{}/_fakecloud/elbv2/load-balancers", server.endpoint());
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client.get(&url).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(arr) = v.get("loadBalancers").and_then(|x| x.as_array()) {
                    for lb in arr {
                        let arn = lb.get("arn").and_then(|x| x.as_str()).unwrap_or("");
                        if arn == lb_arn {
                            if let Some(p) = lb.get("boundPort").and_then(|x| x.as_u64()) {
                                return Some(p as u16);
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Provision an ALB with a fixed-response listener so any request
/// that makes it past the WAF returns 200 + "ok". Returns the LB ARN
/// and the bound TCP port the dataplane allocated.
async fn provision_alb_with_fixed_response(server: &TestServer, lb_name: &str) -> (String, u16) {
    let elbv2 = server.elbv2_client().await;
    let lb = elbv2
        .create_load_balancer()
        .name(lb_name)
        .scheme(aws_sdk_elasticloadbalancingv2::types::LoadBalancerSchemeEnum::Internal)
        .send()
        .await
        .unwrap();
    let lb_arn = lb
        .load_balancers()
        .first()
        .unwrap()
        .load_balancer_arn()
        .unwrap()
        .to_string();
    elbv2
        .create_listener()
        .load_balancer_arn(&lb_arn)
        .protocol(ProtocolEnum::Http)
        .port(80)
        .default_actions(
            Action::builder()
                .r#type(ActionTypeEnum::FixedResponse)
                .fixed_response_config(
                    FixedResponseActionConfig::builder()
                        .status_code("200")
                        .content_type("text/plain")
                        .message_body("ok")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let port = wait_for_bound_port(server, &lb_arn, Duration::from_secs(8))
        .await
        .expect("data plane should bind a port for the active LB");
    (lb_arn, port)
}

#[tokio::test]
async fn alb_waf_blocks_admin_path() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    // ALB serving 200/"ok" on every request.
    let (lb_arn, port) = provision_alb_with_fixed_response(&server, "waf-block-lb").await;

    // WebACL with a single Block-on-/admin rule.
    let acl_arn = waf
        .create_web_acl()
        .name("alb-block-admin")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("alb-block-admin"))
        .rules(block_uri_rule("block-admin", "/admin"))
        .send()
        .await
        .expect("create acl")
        .summary
        .unwrap()
        .arn
        .expect("acl arn");

    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(&lb_arn)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();

    // /admin/x is blocked with 403.
    let blocked = http
        .get(format!("http://127.0.0.1:{port}/admin/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status().as_u16(), 403);

    // /public falls through to the listener fixed-response (200/ok).
    let allowed = http
        .get(format!("http://127.0.0.1:{port}/public"))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status().as_u16(), 200);
    assert_eq!(allowed.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn alb_waf_count_rule_lets_request_through_and_increments_metric() {
    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;

    let (lb_arn, port) = provision_alb_with_fixed_response(&server, "waf-count-lb").await;

    let acl_arn = waf
        .create_web_acl()
        .name("alb-count-admin")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("alb-count-admin"))
        .rules(count_uri_rule("count-admin", "/admin"))
        .send()
        .await
        .expect("create acl")
        .summary
        .unwrap()
        .arn
        .expect("acl arn");

    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(&lb_arn)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://127.0.0.1:{port}/admin/x"))
        .send()
        .await
        .unwrap();
    // Count is non-terminal: request still hits the listener and
    // returns its fixed-response 200.
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // Verify the Count metric incremented via the ELBv2 admin
    // endpoint, which exposes per-(WebACL ARN, rule) counters.
    let admin: serde_json::Value = http
        .get(format!("{}/_fakecloud/elbv2/waf-counts", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        admin,
        serde_json::json!({
            "counts": [{
                "webAclArn": acl_arn,
                "ruleName": "count-admin",
                "count": 1,
            }]
        }),
        "expected one count-admin entry"
    );

    let typed = fakecloud_sdk::FakeCloud::new(server.endpoint())
        .elbv2()
        .waf_counts()
        .await
        .expect("sdk waf counts");
    assert_eq!(typed.counts.len(), 1);
    assert_eq!(typed.counts[0].web_acl_arn, acl_arn);
    assert_eq!(typed.counts[0].rule_name, "count-admin");
    assert_eq!(typed.counts[0].count, 1);
}

#[tokio::test]
async fn apigw_v1_waf_blocks_user_agent_substring() {
    use aws_sdk_apigateway::types::IntegrationType;

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let apigw = server.apigateway_client().await;

    // Provision a minimal API with a MOCK integration so allowed
    // requests succeed without needing a backend Lambda.
    let api = apigw.create_rest_api().name("waf-v1").send().await.unwrap();
    let api_id = api.id().unwrap().to_string();
    let root = api.root_resource_id().unwrap().to_string();
    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root)
        .path_part("items")
        .send()
        .await
        .unwrap();
    let res_id = resource.id().unwrap().to_string();
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .unwrap();
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&res_id)
        .http_method("GET")
        .r#type(IntegrationType::Mock)
        .send()
        .await
        .unwrap();
    apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .stage_name("prod")
        .send()
        .await
        .unwrap();

    // Block any request whose User-Agent header contains `BadBot`.
    let block_user_agent = Statement::builder()
        .byte_match_statement(
            ByteMatchStatement::builder()
                .search_string(aws_smithy_types::Blob::new(b"BadBot".as_slice()))
                .field_to_match(
                    FieldToMatch::builder()
                        .single_header(
                            aws_sdk_wafv2::types::SingleHeader::builder()
                                .name("user-agent")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .text_transformations(
                    TextTransformation::builder()
                        .priority(0)
                        .r#type(TextTransformationType::None)
                        .build()
                        .unwrap(),
                )
                .positional_constraint(PositionalConstraint::Contains)
                .build()
                .unwrap(),
        )
        .build();
    let rule = Rule::builder()
        .name("block-badbot")
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("block-badbot"))
        .statement(block_user_agent)
        .build()
        .unwrap();
    let acl_arn = waf
        .create_web_acl()
        .name("apigw-v1-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("apigw-v1-acl"))
        .rules(rule)
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .arn
        .unwrap();

    // API GW stage ARN format: arn:aws:apigateway:<region>::/restapis/<api>/stages/<stage>.
    let stage_arn = format!("arn:aws:apigateway:us-east-1::/restapis/{api_id}/stages/prod");
    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(&stage_arn)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let host = format!("{api_id}.execute-api.us-east-1.amazonaws.com");

    // Bad UA -> blocked.
    let blocked = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", &host)
        .header("user-agent", "Mozilla/5.0 BadBot/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status().as_u16(), 403);

    // Different UA -> default Allow path; MOCK returns 200.
    let allowed = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("host", &host)
        .header("user-agent", "GoodBot/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status().as_u16(), 200);
}

#[tokio::test]
async fn apigw_v2_waf_blocks_user_agent_substring() {
    use aws_sdk_apigatewayv2::types::{IntegrationType, ProtocolType};

    let server = TestServer::start().await;
    let waf = server.wafv2_client().await;
    let apigw = server.apigatewayv2_client().await;

    // Provision a minimal HTTP API with a MOCK integration.
    let api = apigw
        .create_api()
        .name("waf-v2")
        .protocol_type(ProtocolType::Http)
        .send()
        .await
        .unwrap();
    let api_id = api.api_id().unwrap().to_string();
    let integ = apigw
        .create_integration()
        .api_id(&api_id)
        .integration_type(IntegrationType::Mock)
        .send()
        .await
        .unwrap();
    let integ_id = integ.integration_id().unwrap().to_string();
    apigw
        .create_route()
        .api_id(&api_id)
        .route_key("GET /items")
        .target(format!("integrations/{integ_id}"))
        .send()
        .await
        .unwrap();
    apigw
        .create_stage()
        .api_id(&api_id)
        .stage_name("prod")
        .auto_deploy(true)
        .send()
        .await
        .unwrap();

    // Block any request whose User-Agent header contains `BadBot`.
    let block_user_agent = Statement::builder()
        .byte_match_statement(
            ByteMatchStatement::builder()
                .search_string(aws_smithy_types::Blob::new(b"BadBot".as_slice()))
                .field_to_match(
                    FieldToMatch::builder()
                        .single_header(
                            aws_sdk_wafv2::types::SingleHeader::builder()
                                .name("user-agent")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .text_transformations(
                    TextTransformation::builder()
                        .priority(0)
                        .r#type(TextTransformationType::None)
                        .build()
                        .unwrap(),
                )
                .positional_constraint(PositionalConstraint::Contains)
                .build()
                .unwrap(),
        )
        .build();
    let rule = Rule::builder()
        .name("block-badbot")
        .priority(0)
        .action(
            RuleAction::builder()
                .block(aws_sdk_wafv2::types::BlockAction::builder().build())
                .build(),
        )
        .visibility_config(vis("block-badbot"))
        .statement(block_user_agent)
        .build()
        .unwrap();
    let acl_arn = waf
        .create_web_acl()
        .name("apigw-v2-acl")
        .scope(Scope::Regional)
        .default_action(allow_default())
        .visibility_config(vis("apigw-v2-acl"))
        .rules(rule)
        .send()
        .await
        .unwrap()
        .summary
        .unwrap()
        .arn
        .unwrap();

    // API GW v2 stage ARN: arn:aws:apigateway:<region>::/apis/<api>/stages/<stage>.
    let stage_arn = format!("arn:aws:apigateway:us-east-1::/apis/{api_id}/stages/prod");
    waf.associate_web_acl()
        .web_acl_arn(&acl_arn)
        .resource_arn(&stage_arn)
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();

    // Bad UA -> blocked.
    let blocked = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("user-agent", "Mozilla/5.0 BadBot/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status().as_u16(), 403);

    // Allowed UA -> MOCK 200.
    let allowed = http
        .get(format!("{}/prod/items", server.endpoint()))
        .header("user-agent", "GoodBot/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status().as_u16(), 200);
}
