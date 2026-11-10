//! WebSocket-to-Lambda dispatch for API Gateway v2 WebSocket APIs.
//!
//! Constructs the WebSocket proxy integration event and invokes the target
//! Lambda. The event format follows AWS's documented shape for `$connect`,
//! `$disconnect`, `$default`, and custom routes.

use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use crate::state::{ApiGatewayV2State, SharedApiGatewayV2State};

/// Resolve the route key for an inbound WebSocket message using the API's
/// `routeSelectionExpression`.
///
/// AWS supports expressions like `$request.body.action` and `$default`.
/// We parse `$request.body.<field>` out of the JSON body; anything else
/// (including `$default`) is returned as a literal route key.
pub fn resolve_route_key(expression: &str, body: &str) -> String {
    if expression == "$default" {
        return "$default".to_string();
    }

    // Strip `$request.body.` prefix and look up the field.
    let field = expression.strip_prefix("$request.body.");
    let Some(field) = field else {
        return expression.to_string();
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) else {
        return "$default".to_string();
    };

    parsed
        .get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "$default".to_string())
}

/// Construct the WebSocket proxy integration event.
#[allow(clippy::too_many_arguments)]
fn construct_event(
    api_id: &str,
    region: &str,
    stage: &str,
    connection_id: &str,
    route_key: &str,
    event_type: &str,
    body: Option<&str>,
    source_ip: &str,
    user_agent: Option<&str>,
    connected_at: chrono::DateTime<chrono::Utc>,
    headers: Option<&std::collections::HashMap<String, String>>,
    query_params: Option<&std::collections::HashMap<String, String>>,
) -> serde_json::Value {
    let now = Utc::now();
    let request_id = uuid::Uuid::new_v4().to_string();
    let extended_request_id = uuid::Uuid::new_v4().to_string();
    let message_id = if event_type == "MESSAGE" {
        Some(uuid::Uuid::new_v4().to_string())
    } else {
        None
    };

    let mut identity = serde_json::Map::new();
    identity.insert("sourceIp".to_string(), json!(source_ip));
    if let Some(ua) = user_agent {
        identity.insert("userAgent".to_string(), json!(ua));
    }

    let mut request_context = serde_json::Map::new();
    request_context.insert("apiId".to_string(), json!(api_id));
    request_context.insert(
        "connectedAt".to_string(),
        json!(connected_at.timestamp_millis()),
    );
    request_context.insert("connectionId".to_string(), json!(connection_id));
    request_context.insert(
        "domainName".to_string(),
        json!(format!("{api_id}.execute-api.{region}.amazonaws.com")),
    );
    request_context.insert("eventType".to_string(), json!(event_type));
    request_context.insert("extendedRequestId".to_string(), json!(extended_request_id));
    request_context.insert("identity".to_string(), serde_json::Value::Object(identity));
    request_context.insert("messageDirection".to_string(), json!("IN"));
    request_context.insert("messageId".to_string(), json!(message_id));
    request_context.insert("requestId".to_string(), json!(request_id));
    request_context.insert(
        "requestTime".to_string(),
        json!(now.format("%d/%b/%Y:%H:%M:%S %z").to_string()),
    );
    request_context.insert(
        "requestTimeEpoch".to_string(),
        json!(now.timestamp_millis()),
    );
    request_context.insert("routeKey".to_string(), json!(route_key));
    request_context.insert("stage".to_string(), json!(stage));

    let mut event = serde_json::Map::new();
    event.insert(
        "requestContext".to_string(),
        serde_json::Value::Object(request_context),
    );

    if let Some(headers) = headers {
        event.insert("headers".to_string(), json!(headers));
    }
    if let Some(query_params) = query_params {
        event.insert("queryStringParameters".to_string(), json!(query_params));
    }

    if let Some(body_str) = body {
        event.insert("body".to_string(), json!(body_str));
        event.insert("isBase64Encoded".to_string(), json!(false));
    } else {
        event.insert("body".to_string(), serde_json::Value::Null);
        event.insert("isBase64Encoded".to_string(), json!(false));
    }

    serde_json::Value::Object(event)
}

/// What dispatching a WebSocket event to its route's integration produced.
/// Only `$connect` acts on it: a rejected `$connect` refuses the
/// connection with that HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketDispatchOutcome {
    /// No route / integration handled the event (or no Lambda is wired).
    NotRouted,
    /// The integration answered successfully.
    Accepted,
    /// The integration answered with a failure: refuse with this response
    /// (`Content-Type: application/json; charset=UTF-8`).
    Rejected { status: u16, body: String },
}

/// The outcome of a Lambda proxy integration answer on a WebSocket route,
/// as recorded on AWS for `$connect`
/// (`apigateway_websocket_connect` in
/// fakecloud-lambda/tests/fixtures/function-errors/aws-recordings.json): a
/// 2xx `statusCode` accepts; another `statusCode` is answered with that
/// status and `body`; a function error, a failed invocation or a response
/// without `statusCode` is a 502 naming the connection and request.
pub fn integration_outcome(
    result: &Result<fakecloud_core::delivery::LambdaInvocationOutput, String>,
    connection_id: &str,
    request_id: &str,
) -> WebSocketDispatchOutcome {
    let internal_error = || {
        WebSocketDispatchOutcome::Rejected {
        status: 502,
        body: format!(
            "{{\"message\": \"Internal server error\", \"connectionId\":\"{connection_id}\", \"requestId\":\"{request_id}\"}}"
        ),
    }
    };
    let output = match result {
        Ok(output) if !output.is_function_error() => output,
        _ => return internal_error(),
    };
    let Some(response) = serde_json::from_slice::<serde_json::Value>(&output.payload)
        .ok()
        .filter(serde_json::Value::is_object)
    else {
        return internal_error();
    };
    let Some(status) = response
        .get("statusCode")
        .and_then(serde_json::Value::as_u64)
        .and_then(|s| u16::try_from(s).ok())
    else {
        return internal_error();
    };
    if (200..300).contains(&status) {
        WebSocketDispatchOutcome::Accepted
    } else {
        WebSocketDispatchOutcome::Rejected {
            status,
            body: response
                .get("body")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }
    }
}

/// Dispatch a WebSocket event to the Lambda integration for the given route.
///
/// Silently returns when:
/// - the API or route has no matching integration
/// - Lambda delivery is not configured
/// - the integration is not `AWS_PROXY`
///
/// AWS never fails the WebSocket connection if the Lambda errors; we mirror
/// that by swallowing invocation failures.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_websocket_event(
    state: &SharedApiGatewayV2State,
    lambda_delivery: Option<&Arc<dyn fakecloud_core::delivery::LambdaDelivery>>,
    account_id: &str,
    region: &str,
    api_id: &str,
    stage: &str,
    connection_id: &str,
    route_key: &str,
    event_type: &str,
    body: Option<&str>,
    source_ip: &str,
    user_agent: Option<&str>,
    connected_at: chrono::DateTime<chrono::Utc>,
    headers: Option<&std::collections::HashMap<String, String>>,
    query_params: Option<&std::collections::HashMap<String, String>>,
) -> WebSocketDispatchOutcome {
    let (integration_id, function_arn, matched_route_key) = {
        let accounts = state.read();
        let empty = ApiGatewayV2State::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);

        let Some(_api) = state.apis.get(api_id) else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        // WebSocket APIs don't have a `$default` route automatically;
        // if the resolved route doesn't exist and there's no `$default`,
        // AWS silently drops the message.
        let routes = state.routes.get(api_id);
        let matched = routes.and_then(|rs| {
            rs.values()
                .find(|r| r.route_key == route_key)
                .map(|r| (r, route_key.to_string()))
        });
        let matched = matched.or_else(|| {
            routes
                .and_then(|rs| rs.values().find(|r| r.route_key == "$default"))
                .map(|r| (r, "$default".to_string()))
        });
        let Some((route, matched_route_key)) = matched else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        let Some(target) = route.target.as_ref() else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        let Some(integration_id) = target.strip_prefix("integrations/") else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        let Some(integration) = state
            .integrations
            .get(api_id)
            .and_then(|ints| ints.get(integration_id))
        else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        if integration.integration_type != "AWS_PROXY" {
            return WebSocketDispatchOutcome::NotRouted;
        }

        let Some(function_arn) = integration.integration_uri.clone() else {
            return WebSocketDispatchOutcome::NotRouted;
        };

        (integration_id.to_string(), function_arn, matched_route_key)
    };

    let lambda_delivery = match lambda_delivery {
        Some(d) => d,
        None => return WebSocketDispatchOutcome::NotRouted,
    };

    let event = construct_event(
        api_id,
        region,
        stage,
        connection_id,
        &matched_route_key,
        event_type,
        body,
        source_ip,
        user_agent,
        connected_at,
        headers,
        query_params,
    );

    let payload = match serde_json::to_string(&event) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(%e, "failed to serialize websocket event");
            return WebSocketDispatchOutcome::NotRouted;
        }
    };

    let request_id = event["requestContext"]["requestId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let result = lambda_delivery.invoke_lambda(&function_arn, &payload).await;
    match &result {
        Ok(output) if output.is_function_error() => {
            tracing::warn!(
                route_key,
                integration_id,
                error = %String::from_utf8_lossy(&output.payload),
                "websocket lambda returned a function error"
            );
        }
        Ok(_) => {
            tracing::debug!(route_key, integration_id, "websocket lambda invoked");
        }
        Err(e) => {
            tracing::warn!(%e, route_key, "websocket lambda invocation failed");
        }
    }
    integration_outcome(&result, connection_id, &request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use fakecloud_core::delivery::LambdaInvocationOutput;

    fn ok(v: serde_json::Value) -> Result<LambdaInvocationOutput, String> {
        Ok(LambdaInvocationOutput::success(v.to_string().into_bytes()))
    }

    // Recorded: `apigateway_websocket_connect` in
    // fakecloud-lambda/tests/fixtures/function-errors/aws-recordings.json.
    #[test]
    fn connect_outcome_matches_aws_recordings() {
        let internal = WebSocketDispatchOutcome::Rejected {
            status: 502,
            body: r#"{"message": "Internal server error", "connectionId":"c1", "requestId":"r1"}"#
                .to_string(),
        };
        let failed = Ok(LambdaInvocationOutput::unhandled(
            br#"{"errorType":"ValueError","errorMessage":"no"}"#.to_vec(),
        ));
        assert_eq!(integration_outcome(&failed, "c1", "r1"), internal);
        assert_eq!(
            integration_outcome(&ok(serde_json::json!({"hello": "x"})), "c1", "r1"),
            internal
        );
        assert_eq!(
            integration_outcome(&Err("boom".into()), "c1", "r1"),
            internal
        );
        assert_eq!(
            integration_outcome(
                &ok(serde_json::json!({"statusCode": 403, "body": "nope"})),
                "c1",
                "r1"
            ),
            WebSocketDispatchOutcome::Rejected {
                status: 403,
                body: "nope".to_string()
            }
        );
        assert_eq!(
            integration_outcome(&ok(serde_json::json!({"statusCode": 200})), "c1", "r1"),
            WebSocketDispatchOutcome::Accepted
        );
    }

    #[test]
    fn resolve_route_key_default() {
        assert_eq!(
            resolve_route_key("$default", r#"{"action":"x"}"#),
            "$default"
        );
    }

    #[test]
    fn resolve_route_key_from_body_action() {
        assert_eq!(
            resolve_route_key("$request.body.action", r#"{"action":"sendmessage"}"#),
            "sendmessage"
        );
    }

    #[test]
    fn resolve_route_key_falls_back_to_default_when_field_missing() {
        assert_eq!(
            resolve_route_key("$request.body.action", r#"{"foo":"bar"}"#),
            "$default"
        );
    }

    #[test]
    fn resolve_route_key_falls_back_to_default_when_not_json() {
        assert_eq!(
            resolve_route_key("$request.body.action", "not-json"),
            "$default"
        );
    }

    #[test]
    fn resolve_route_key_literal_expression() {
        assert_eq!(
            resolve_route_key("$request.body.messageType", r#"{"messageType":"chat"}"#),
            "chat"
        );
    }

    #[test]
    fn construct_connect_event_has_null_body() {
        let event = construct_event(
            "api-1",
            "eu-west-1",
            "dev",
            "conn-id=",
            "$connect",
            "CONNECT",
            None,
            "127.0.0.1",
            Some("test-agent"),
            chrono::Utc::now(),
            Some(&std::collections::HashMap::from([(
                "X-Custom".to_string(),
                "val".to_string(),
            )])),
            Some(&std::collections::HashMap::from([(
                "token".to_string(),
                "abc".to_string(),
            )])),
        );
        assert_eq!(event["requestContext"]["routeKey"], "$connect");
        assert_eq!(event["requestContext"]["eventType"], "CONNECT");
        assert!(event["body"].is_null());
        assert_eq!(event["isBase64Encoded"], false);
        assert_eq!(event["headers"]["X-Custom"], "val");
        assert_eq!(event["queryStringParameters"]["token"], "abc");
        // The proxy event's domainName carries the request region, not a
        // hardcoded us-east-1, so the Lambda sees a region-consistent host.
        assert_eq!(
            event["requestContext"]["domainName"],
            "api-1.execute-api.eu-west-1.amazonaws.com"
        );
    }

    #[test]
    fn construct_message_event_has_message_id() {
        let event = construct_event(
            "api-1",
            "us-east-1",
            "dev",
            "conn-id=",
            "$default",
            "MESSAGE",
            Some(r#"hello"#),
            "127.0.0.1",
            None,
            chrono::Utc::now(),
            None,
            None,
        );
        assert_eq!(event["requestContext"]["routeKey"], "$default");
        assert_eq!(event["requestContext"]["eventType"], "MESSAGE");
        assert!(event["requestContext"]["messageId"].as_str().is_some());
        assert_eq!(event["body"], "hello");
    }
}
