//! Unsigned (anonymous) requests to services other than S3, under
//! `--iam strict`, `soft` and `off`.
//!
//! Recorded against real AWS (us-east-1), see
//! `crates/fakecloud-core/tests/fixtures/anonymous-requests/README.md`: an
//! unsigned API request is refused
//! for its missing credentials before anything looks at it, in a shape that
//! depends on the protocol and the service; SQS instead authorizes the
//! anonymous principal against the queue policy; the operations the Smithy
//! models mark unauthenticated (Cognito InitiateAuth, STS
//! AssumeRoleWithWebIdentity, ...) are served.

mod helpers;

use helpers::TestServer;
use serde_json::{json, Value};

async fn start_strict() -> TestServer {
    TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await
}

struct Unsigned {
    status: u16,
    error_type: Option<String>,
    body: String,
}

impl Unsigned {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
    }
}

/// Send a request with no `Authorization` header and no query
/// authentication. `host`, when given, names the AWS endpoint, as a client
/// pointed at the real hostname does.
async fn unsigned(
    server: &TestServer,
    method: reqwest::Method,
    path: &str,
    host: Option<&str>,
    headers: &[(&str, &str)],
    body: &str,
) -> Unsigned {
    let mut req = reqwest::Client::new().request(method, format!("{}{path}", server.endpoint()));
    if let Some(host) = host {
        req = req.header("host", host);
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = req.body(body.to_string()).send().await.unwrap();
    let status = resp.status().as_u16();
    let error_type = resp
        .headers()
        .get("x-amzn-errortype")
        .map(|v| v.to_str().unwrap().to_string());
    Unsigned {
        status,
        error_type,
        body: resp.text().await.unwrap(),
    }
}

async fn query(server: &TestServer, host: Option<&str>, body: &str) -> Unsigned {
    unsigned(
        server,
        reqwest::Method::POST,
        "/",
        host,
        &[("content-type", "application/x-www-form-urlencoded")],
        body,
    )
    .await
}

async fn json_target(server: &TestServer, target: &str, content_type: &str) -> Unsigned {
    unsigned(
        server,
        reqwest::Method::POST,
        "/",
        None,
        &[("x-amz-target", target), ("content-type", content_type)],
        "{}",
    )
    .await
}

/// Percent-encode a form or query value.
fn encoded(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

const STS_NS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";

/// The recorded awsQuery refusal, in the service's namespaced envelope.
fn assert_query_missing_token(resp: &Unsigned, namespace: &str) {
    assert_eq!(resp.status, 403, "{}", resp.body);
    assert!(
        resp.body
            .contains(&format!(r#"<ErrorResponse xmlns="{namespace}">"#)),
        "{}",
        resp.body
    );
    assert!(resp.body.contains("<Type>Sender</Type>"), "{}", resp.body);
    assert!(
        resp.body
            .contains("<Code>MissingAuthenticationToken</Code>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body
            .contains("<Message>Request is missing Authentication Token</Message>"),
        "{}",
        resp.body
    );
}

#[tokio::test]
async fn unsigned_sts_requests_on_the_plain_host_are_refused_before_validation() {
    let server = start_strict().await;
    // A form POST to the bare endpoint reaches STS by its Action.
    let resp = query(&server, None, "Action=GetCallerIdentity&Version=2011-06-15").await;
    assert_query_missing_token(&resp, STS_NS);
    // Recorded: the missing token comes before the TaskPolicyArn check.
    let resp = query(
        &server,
        None,
        "Action=AssumeRoot&Version=2011-06-15&TargetPrincipal=111122223333\
         &TaskPolicyArn.arn=bogus",
    )
    .await;
    assert_query_missing_token(&resp, STS_NS);
    assert!(!resp.body.contains("ValidationError"), "{}", resp.body);
}

#[tokio::test]
async fn unsigned_iam_and_sns_requests_are_refused() {
    let server = start_strict().await;
    let resp = query(
        &server,
        Some("iam.amazonaws.com"),
        "Action=ListUsers&Version=2010-05-08",
    )
    .await;
    assert_query_missing_token(&resp, "https://iam.amazonaws.com/doc/2010-05-08/");
    let resp = query(
        &server,
        Some("sns.us-east-1.amazonaws.com"),
        "Action=ListTopics&Version=2010-03-31",
    )
    .await;
    assert_query_missing_token(&resp, "http://sns.amazonaws.com/doc/2010-03-31/");
}

#[tokio::test]
async fn unsigned_ec2_request_is_missing_its_access_key_parameter() {
    let server = start_strict().await;
    let resp = query(
        &server,
        Some("ec2.us-east-1.amazonaws.com"),
        "Action=DescribeRegions&Version=2016-11-15",
    )
    .await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    assert!(
        resp.body.contains("<Response><Errors><Error>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body.contains("<Code>MissingParameter</Code>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body
            .contains("<Message>The request must contain the parameter AWSAccessKeyId</Message>"),
        "{}",
        resp.body
    );
}

#[tokio::test]
async fn unsigned_json_requests_are_refused_in_each_services_shape() {
    let server = start_strict().await;
    for target in [
        "TrentService.ListKeys",
        "AmazonSSM.DescribeParameters",
        "Logs_20140328.DescribeLogGroups",
    ] {
        let resp = json_target(&server, target, "application/x-amz-json-1.1").await;
        assert_eq!(resp.status, 400, "{target}: {}", resp.body);
        assert_eq!(
            resp.json(),
            json!({
                "__type": "MissingAuthenticationTokenException",
                "message": "Missing Authentication Token"
            }),
            "{target}"
        );
    }
    let resp = json_target(
        &server,
        "DynamoDB_20120810.ListTables",
        "application/x-amz-json-1.0",
    )
    .await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    assert_eq!(
        resp.json(),
        json!({
            "__type": "com.amazon.coral.service#MissingAuthenticationTokenException",
            "message": "Request is missing Authentication Token"
        })
    );
    let resp = json_target(
        &server,
        "secretsmanager.ListSecrets",
        "application/x-amz-json-1.1",
    )
    .await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    assert_eq!(
        resp.json(),
        json!({
            "__type": "UnrecognizedClientException",
            "message": "The security token included in the request is invalid."
        })
    );
}

#[tokio::test]
async fn unsigned_rest_requests_to_an_api_host_are_refused() {
    let server = start_strict().await;
    let resp = unsigned(
        &server,
        reqwest::Method::GET,
        "/2015-03-31/functions/",
        Some("lambda.us-east-1.amazonaws.com"),
        &[],
        "",
    )
    .await;
    assert_eq!(resp.status, 403, "{}", resp.body);
    assert_eq!(
        resp.json(),
        json!({"message": "Missing Authentication Token"})
    );
    assert_eq!(
        resp.error_type.as_deref(),
        Some("MissingAuthenticationTokenException")
    );

    let resp = unsigned(
        &server,
        reqwest::Method::GET,
        "/2013-04-01/hostedzone",
        Some("route53.amazonaws.com"),
        &[],
        "",
    )
    .await;
    assert_eq!(resp.status, 403, "{}", resp.body);
    assert!(
        resp.body
            .contains(r#"<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">"#),
        "{}",
        resp.body
    );
    assert!(
        resp.body
            .contains("<Code>MissingAuthenticationToken</Code>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body
            .contains("<Message>Request is missing Authentication Token</Message>"),
        "{}",
        resp.body
    );
}

#[tokio::test]
async fn unsigned_sqs_requests_are_authorized_by_the_queue_policy() {
    let server = start_strict().await;
    let host = Some("sqs.us-east-1.amazonaws.com");
    // Recorded: no policy can grant ListQueues to anyone, so it is denied
    // naming the endpoint.
    let resp = query(&server, host, "Action=ListQueues&Version=2012-11-05").await;
    assert_eq!(resp.status, 403, "{}", resp.body);
    assert!(
        resp.body.contains("<Code>AccessDenied</Code>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body.contains(
            "<Message>Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.</Message>"
        ),
        "{}",
        resp.body
    );
    assert!(
        resp.body
            .contains(r#"<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">"#),
        "{}",
        resp.body
    );
    assert!(resp.body.contains("<Detail/>"), "{}", resp.body);

    let sqs = aws_sdk_sqs::Client::new(&helpers::root_config(&server).await);
    let open = sqs
        .create_queue()
        .queue_name("open-to-all")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&open)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .clone();
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "sqs:SendMessage",
            "Resource": arn,
        }]
    })
    .to_string();
    sqs.set_queue_attributes()
        .queue_url(&open)
        .attributes(aws_sdk_sqs::types::QueueAttributeName::Policy, policy)
        .send()
        .await
        .unwrap();

    // The policy lets the anonymous caller send...
    let resp = query(
        &server,
        host,
        &format!(
            "Action=SendMessage&Version=2012-11-05&QueueUrl={}&MessageBody=hi",
            encoded(&open)
        ),
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert!(resp.body.contains("<MessageId>"), "{}", resp.body);
    // ...and nothing else, refused naming the queue.
    let resp = query(
        &server,
        host,
        &format!(
            "Action=ReceiveMessage&Version=2012-11-05&QueueUrl={}",
            encoded(&open)
        ),
    )
    .await;
    assert_sqs_anonymous_denied(&resp, "receivemessage", &arn);

    // A queue with no policy refuses every queue operation, naming the
    // lowercased action and the queue ARN (recorded).
    let closed = sqs
        .create_queue()
        .queue_name("closed")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let closed_arn = arn.replace("open-to-all", "closed");
    for (action, extra, iam) in [
        ("ReceiveMessage", "", "receivemessage"),
        ("SendMessage", "&MessageBody=hi", "sendmessage"),
        (
            "GetQueueAttributes",
            "&AttributeName.1=All",
            "getqueueattributes",
        ),
    ] {
        let resp = query(
            &server,
            host,
            &format!(
                "Action={action}&Version=2012-11-05&QueueUrl={}{extra}",
                encoded(&closed)
            ),
        )
        .await;
        assert_sqs_anonymous_denied(&resp, iam, &closed_arn);
    }
}

/// The recorded SQS anonymous denial of a queue operation, in SQS's
/// namespaced envelope with its `<Detail/>`.
fn assert_sqs_anonymous_denied(resp: &Unsigned, iam_action: &str, queue_arn: &str) {
    assert_eq!(resp.status, 403, "{}", resp.body);
    assert!(
        resp.body
            .contains(r#"<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">"#),
        "{}",
        resp.body
    );
    assert!(
        resp.body.contains("<Code>AccessDenied</Code>"),
        "{}",
        resp.body
    );
    assert!(
        resp.body.contains(&format!(
            "<Message>User: anonymous is not authorized to perform: sqs:{iam_action} on \
             resource: {queue_arn} because no resource-based policy allows the \
             sqs:{iam_action} action</Message>"
        )),
        "{}",
        resp.body
    );
    assert!(resp.body.contains("<Detail/>"), "{}", resp.body);
}

#[tokio::test]
async fn unsigned_cognito_auth_flows_are_served() {
    use aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType;
    let server = start_strict().await;
    let target = |op: &str| format!("AWSCognitoIdentityProviderService.{op}");
    let call = |op: String, body: Value| {
        let server = &server;
        async move {
            unsigned(
                server,
                reqwest::Method::POST,
                "/",
                None,
                &[
                    ("x-amz-target", op.as_str()),
                    ("content-type", "application/x-amz-json-1.1"),
                ],
                &body.to_string(),
            )
            .await
        }
    };
    // Recorded: InitiateAuth is processed, so an unknown client is the
    // answer, not a missing token.
    let resp = call(
        target("InitiateAuth"),
        json!({
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": "1example23456789",
            "AuthParameters": {"USERNAME": "u", "PASSWORD": "p"}
        }),
    )
    .await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    assert_eq!(resp.json()["__type"], "ResourceNotFoundException");
    assert_eq!(
        resp.json()["message"],
        "User pool client 1example23456789 does not exist."
    );

    let cognito = server.cognito_client().await;
    let pool = cognito
        .create_user_pool()
        .pool_name("anon")
        .send()
        .await
        .unwrap()
        .user_pool()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    let client_id = cognito
        .create_user_pool_client()
        .user_pool_id(&pool)
        .client_name("web")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .unwrap()
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    let resp = call(
        target("SignUp"),
        json!({"ClientId": client_id, "Username": "alice", "Password": "Passw0rd!Long"}),
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    cognito
        .admin_confirm_sign_up()
        .user_pool_id(&pool)
        .username("alice")
        .send()
        .await
        .unwrap();
    let resp = call(
        target("InitiateAuth"),
        json!({
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": client_id,
            "AuthParameters": {"USERNAME": "alice", "PASSWORD": "Passw0rd!Long"}
        }),
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert!(
        resp.json()["AuthenticationResult"]["AccessToken"].is_string(),
        "{}",
        resp.body
    );
    // An admin operation is not one of them.
    let resp = call(
        target("AdminGetUser"),
        json!({"UserPoolId": pool, "Username": "alice"}),
    )
    .await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    assert_eq!(resp.json()["__type"], "MissingAuthenticationTokenException");
}

#[tokio::test]
async fn unsigned_assume_role_with_web_identity_is_served() {
    let server = start_strict().await;
    aws_sdk_iam::Client::new(&helpers::root_config(&server).await)
        .create_role()
        .role_name("web-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let resp = query(
        &server,
        None,
        "Action=AssumeRoleWithWebIdentity&Version=2011-06-15\
         &RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fweb-role\
         &RoleSessionName=web&WebIdentityToken=fake-token",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert!(resp.body.contains("<AccessKeyId>"), "{}", resp.body);
    assert!(
        resp.body.contains("assumed-role/web-role/web"),
        "{}",
        resp.body
    );
}

/// SNS's subscription confirmation and unsubscribe links are plain unsigned
/// GETs (documented as needing no signature), so they keep working.
#[tokio::test]
async fn unsigned_sns_unsubscribe_link_is_served() {
    let server = start_strict().await;
    let root = helpers::root_config(&server).await;
    let sns = aws_sdk_sns::Client::new(&root);
    let sqs = aws_sdk_sqs::Client::new(&root);
    let topic = sns
        .create_topic()
        .name("links")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();
    let queue = sqs
        .create_queue()
        .queue_name("links-q")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(&queue)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .clone();
    let sub = sns
        .subscribe()
        .topic_arn(&topic)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap()
        .subscription_arn()
        .unwrap()
        .to_string();
    let resp = unsigned(
        &server,
        reqwest::Method::GET,
        &format!("/?Action=Unsubscribe&SubscriptionArn={}", encoded(&sub)),
        Some("sns.us-east-1.amazonaws.com"),
        &[],
        "",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let subs = sns
        .list_subscriptions_by_topic()
        .topic_arn(&topic)
        .send()
        .await
        .unwrap();
    assert!(subs.subscriptions().is_empty());
}

/// Soft mode logs the missing token and serves the request.
#[tokio::test]
async fn soft_mode_serves_unsigned_requests() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "soft")]).await;
    let resp = json_target(
        &server,
        "TrentService.ListKeys",
        "application/x-amz-json-1.1",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let resp = query(&server, None, "Action=ListUsers&Version=2010-05-08").await;
    assert_eq!(resp.status, 200, "{}", resp.body);
}

/// With enforcement off (the default) unsigned requests are served, as
/// before. (`GetCallerIdentity` has no identity to report for one, and is
/// refused as `MissingAuthenticationToken` in every mode.)
#[tokio::test]
async fn off_mode_serves_unsigned_requests() {
    let server = TestServer::start().await;
    for (target, content_type) in [
        ("TrentService.ListKeys", "application/x-amz-json-1.1"),
        ("DynamoDB_20120810.ListTables", "application/x-amz-json-1.0"),
        ("secretsmanager.ListSecrets", "application/x-amz-json-1.1"),
    ] {
        let resp = json_target(&server, target, content_type).await;
        assert_eq!(resp.status, 200, "{target}: {}", resp.body);
    }
    let resp = query(&server, None, "Action=ListUsers&Version=2010-05-08").await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let resp = query(&server, None, "Action=GetCallerIdentity&Version=2011-06-15").await;
    assert_query_missing_token(&resp, STS_NS);
    let resp = query(
        &server,
        Some("ec2.us-east-1.amazonaws.com"),
        "Action=DescribeRegions&Version=2016-11-15",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let resp = unsigned(
        &server,
        reqwest::Method::GET,
        "/2015-03-31/functions/",
        Some("lambda.us-east-1.amazonaws.com"),
        &[],
        "",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    let resp = unsigned(
        &server,
        reqwest::Method::GET,
        "/2013-04-01/hostedzone",
        Some("route53.amazonaws.com"),
        &[],
        "",
    )
    .await;
    assert_eq!(resp.status, 200, "{}", resp.body);
}
