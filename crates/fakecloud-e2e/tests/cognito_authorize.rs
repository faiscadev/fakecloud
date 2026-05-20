//! E2E tests for the Cognito Hosted UI `/oauth2/authorize` endpoint
//! (Y4). The endpoint is the first step of the Authorization Code and
//! Implicit OAuth 2.0 flows; in real Cognito it serves an HTML login
//! form, but our fakecloud variant accepts username/password directly
//! on the query string so scripted E2E tests don't have to scrape HTML.

mod helpers;
use helpers::TestServer;

use aws_sdk_cognitoidentityprovider::types::{PasswordPolicyType, UserPoolPolicyType};

async fn fetch_confirmation_code(server: &TestServer, pool_id: &str, username: &str) -> String {
    let url = format!(
        "{}/_fakecloud/cognito/confirmation-codes/{}/{}",
        server.endpoint(),
        pool_id,
        username,
    );
    reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["confirmationCode"]
        .as_str()
        .unwrap_or_else(|| panic!("no confirmationCode for {pool_id}/{username}"))
        .to_string()
}

/// Percent-encode a URL component for inline query-string assembly.
/// We don't pull in the `urlencoding` crate just for tests — RFC 3986
/// unreserved set is enough for the values we care about (URLs, JWTs,
/// usernames).
fn encode_uri(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.as_bytes() {
        let c = *byte;
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~') {
            out.push(c as char);
        } else {
            out.push_str(&format!("%{c:02X}"));
        }
    }
    out
}

/// Decode a percent-encoded fragment value. We only need to handle
/// the ASCII subset we know our handler emits.
fn decode_uri(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16).unwrap_or(0) as u8;
            let lo = (bytes[i + 2] as char).to_digit(16).unwrap_or(0) as u8;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Walk the full code grant: GET /oauth2/authorize -> follow the 302
/// -> exchange the resulting `code` at /oauth2/token. This is the
/// happy path real apps drive against a Cognito Hosted UI.
#[tokio::test]
async fn cognito_oauth2_authorize_code_flow_round_trip() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("y4-code-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-code-client")
        .callback_urls("https://app.test/cb")
        .allowed_o_auth_flows("code".into())
        .allowed_o_auth_scopes("openid")
        .allowed_o_auth_scopes("email")
        .allowed_o_auth_flows_user_pool_client(true)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("alice")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("alice")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "alice").await)
        .send()
        .await
        .expect("confirm");

    // Build a non-redirect-following client so we can inspect the 302
    // location the way a browser would.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let authorize_url = format!(
        "{}/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}&scope=openid&state=xyz123&username=alice&password=hunter22",
        server.endpoint(),
        client_id,
        encode_uri("https://app.test/cb"),
    );
    let resp = http.get(&authorize_url).send().await.unwrap();
    assert_eq!(resp.status(), 302, "expected redirect");
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("Location header present")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.starts_with("https://app.test/cb?"),
        "unexpected redirect target: {location}"
    );
    assert!(
        location.contains("state=xyz123"),
        "state must round-trip: {location}"
    );

    // Pull out the `code` query param.
    let url = reqwest::Url::parse(&location).unwrap();
    let code = url
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("code param present");
    assert!(!code.is_empty());

    // Exchange the code at /oauth2/token.
    let token_url = format!("{}/oauth2/token", server.endpoint());
    let body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://app.test/cb"),
    ])
    .unwrap();
    let token_resp = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(token_resp.status(), 200);
    let json: serde_json::Value = token_resp.json().await.unwrap();
    assert_eq!(json["token_type"], "Bearer");
    assert_eq!(json["expires_in"], 3600);
    let id_token = json["id_token"].as_str().unwrap();
    assert_eq!(id_token.split('.').count(), 3);
    assert!(json["access_token"].as_str().unwrap().split('.').count() == 3);
    assert!(!json["refresh_token"].as_str().unwrap().is_empty());
}

/// `response_type=token` (Implicit) returns id_token + access_token
/// in the URL fragment, no refresh_token, per RFC 6749 §4.2.2.
#[tokio::test]
async fn cognito_oauth2_authorize_implicit_flow_returns_fragment_tokens() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("y4-implicit-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-implicit-client")
        .callback_urls("https://app.test/cb")
        .allowed_o_auth_flows("implicit".into())
        .allowed_o_auth_scopes("openid")
        .allowed_o_auth_flows_user_pool_client(true)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("bob")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("bob")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "bob").await)
        .send()
        .await
        .expect("confirm");

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let authorize_url = format!(
        "{}/oauth2/authorize?response_type=token&client_id={}&redirect_uri={}&scope=openid&state=frag1&nonce=nonce123&username=bob&password=hunter22",
        server.endpoint(),
        client_id,
        encode_uri("https://app.test/cb"),
    );
    let resp = http.get(&authorize_url).send().await.unwrap();
    assert_eq!(resp.status(), 302);
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("Location header present")
        .to_str()
        .unwrap()
        .to_string();

    // Implicit flow uses the URL fragment, not the query.
    let (base, fragment) = location
        .split_once('#')
        .expect("implicit response uses fragment");
    assert_eq!(base, "https://app.test/cb");
    let pairs: std::collections::BTreeMap<String, String> = fragment
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (decode_uri(k), decode_uri(v)))
        .collect();
    assert_eq!(pairs.get("token_type").map(String::as_str), Some("Bearer"));
    assert_eq!(pairs.get("expires_in").map(String::as_str), Some("3600"));
    assert_eq!(pairs.get("state").map(String::as_str), Some("frag1"));
    let id_token = pairs.get("id_token").expect("id_token in fragment");
    let access_token = pairs.get("access_token").expect("access_token in fragment");
    assert_eq!(id_token.split('.').count(), 3);
    assert_eq!(access_token.split('.').count(), 3);
    // Implicit flow MUST NOT issue a refresh_token (RFC 6749 §4.2.2).
    assert!(!pairs.contains_key("refresh_token"));

    // The minted access_token must round-trip via /oauth2/userInfo so
    // downstream code can introspect the user.
    let userinfo_url = format!("{}/oauth2/userInfo", server.endpoint());
    let info = http
        .get(&userinfo_url)
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();
    assert_eq!(info.status(), 200);
    let info_json: serde_json::Value = info.json().await.unwrap();
    assert_eq!(info_json["username"], "bob");
}

/// A redirect_uri that isn't on the client's CallbackURLs MUST be
/// rejected with a 400 — RFC 6749 §3.1.2.4 forbids redirecting to an
/// untrusted URL even to surface an error.
#[tokio::test]
async fn cognito_oauth2_authorize_unknown_redirect_uri_rejected() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let pool = client
        .create_user_pool()
        .pool_name("y4-bad-redirect-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-bad-redirect-client")
        .callback_urls("https://app.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!(
        "{}/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}",
        server.endpoint(),
        client_id,
        encode_uri("https://attacker.test/cb"),
    );
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_request");
}

/// An unknown client_id is also a 400 (no redirect target to trust).
#[tokio::test]
async fn cognito_oauth2_authorize_unknown_client_rejected() {
    let server = TestServer::start().await;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!(
        "{}/oauth2/authorize?response_type=code&client_id=ghost&redirect_uri={}",
        server.endpoint(),
        encode_uri("https://app.test/cb"),
    );
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_client");
}

/// Without username/password we serve an HTML login page (200) rather
/// than a redirect — keeps the door open for manual smoke tests.
#[tokio::test]
async fn cognito_oauth2_authorize_no_credentials_serves_login_form() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let pool = client
        .create_user_pool()
        .pool_name("y4-loginform-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-loginform-client")
        .callback_urls("https://app.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!(
        "{}/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}",
        server.endpoint(),
        client_id,
        encode_uri("https://app.test/cb"),
    );
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ctype = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(ctype.starts_with("text/html"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("<form"), "missing form: {body}");
    assert!(body.contains("name=\"username\""));
    assert!(body.contains("name=\"password\""));
}

/// Bad credentials must redirect back with `error=access_denied` so
/// the SPA can render its own error UI. Per RFC 6749 §4.1.2.1, the
/// state must round-trip on the error path too.
#[tokio::test]
async fn cognito_oauth2_authorize_bad_password_redirects_access_denied() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let pool = client
        .create_user_pool()
        .pool_name("y4-baddpw-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-baddpw-client")
        .callback_urls("https://app.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    client
        .sign_up()
        .client_id(&client_id)
        .username("carol")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("carol")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "carol").await)
        .send()
        .await
        .expect("confirm");

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!(
        "{}/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}&state=preserve&username=carol&password=wrong",
        server.endpoint(),
        client_id,
        encode_uri("https://app.test/cb"),
    );
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 302);
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let parsed = reqwest::Url::parse(&location).unwrap();
    let pairs: std::collections::BTreeMap<_, _> = parsed.query_pairs().into_owned().collect();
    assert_eq!(
        pairs.get("error").map(String::as_str),
        Some("access_denied")
    );
    assert_eq!(pairs.get("state").map(String::as_str), Some("preserve"));
}

/// PKCE: a code minted with a `code_challenge` must require the
/// matching `code_verifier` at /oauth2/token. This wires the Y4
/// authorize endpoint up to the Y3 token endpoint's PKCE check.
#[tokio::test]
async fn cognito_oauth2_authorize_with_pkce_round_trips_to_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let pool = client
        .create_user_pool()
        .pool_name("y4-pkce-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("y4-pkce-client")
        .callback_urls("https://app.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    client
        .sign_up()
        .client_id(&client_id)
        .username("dave")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("dave")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "dave").await)
        .send()
        .await
        .expect("confirm");

    // Verifier/challenge from RFC 7636 Appendix B.
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let authorize_url = format!(
        "{}/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}&code_challenge={}&code_challenge_method=S256&username=dave&password=hunter22",
        server.endpoint(),
        client_id,
        encode_uri("https://app.test/cb"),
        challenge,
    );
    let resp = http.get(&authorize_url).send().await.unwrap();
    assert_eq!(resp.status(), 302);
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let url = reqwest::Url::parse(&location).unwrap();
    let code = url
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("code param");

    let token_url = format!("{}/oauth2/token", server.endpoint());
    // Without verifier: /token MUST refuse (invalid_grant).
    let body_bad = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://app.test/cb"),
    ])
    .unwrap();
    let resp_bad = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body_bad)
        .send()
        .await
        .unwrap();
    assert_eq!(resp_bad.status(), 400);
    let bad_json: serde_json::Value = resp_bad.json().await.unwrap();
    assert_eq!(bad_json["error"], "invalid_grant");

    // /authorize must mint a fresh code; codes are single-use even
    // when the previous redemption failed PKCE. Re-mint and retry
    // with the correct verifier.
    let resp2 = http.get(&authorize_url).send().await.unwrap();
    let location2 = resp2
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let code2 = reqwest::Url::parse(&location2)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .expect("second code");
    let body_ok = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code2.as_str()),
        ("redirect_uri", "https://app.test/cb"),
        ("code_verifier", verifier),
    ])
    .unwrap();
    let resp_ok = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body_ok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp_ok.status(), 200);
}
