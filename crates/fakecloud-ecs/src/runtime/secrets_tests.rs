use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsService};
use fakecloud_secretsmanager::SecretsManagerService;
use fakecloud_ssm::SsmService;
use serde_json::{json, Value};

use super::*;

const ACCOUNT: &str = "123456789012";
const OTHER_ACCOUNT: &str = "210987654321";
const REGION: &str = "us-east-1";

/// "Encrypts" by prefixing `ENC:`, "decrypts" by stripping it: proves the
/// resolver hands the container plaintext, not the stored ciphertext.
struct PrefixKmsHook;
impl fakecloud_core::delivery::KmsHook for PrefixKmsHook {
    fn encrypt(
        &self,
        _account_id: &str,
        _region: &str,
        _key_id: &str,
        plaintext: &[u8],
        _service_principal: &str,
        _ctx: HashMap<String, String>,
    ) -> Result<String, String> {
        Ok(format!("ENC:{}", String::from_utf8_lossy(plaintext)))
    }
    fn decrypt(
        &self,
        _account_id: &str,
        ciphertext: &str,
        _service_principal: &str,
        _ctx: HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        ciphertext
            .strip_prefix("ENC:")
            .map(|p| p.as_bytes().to_vec())
            .ok_or_else(|| "not ciphertext".to_string())
    }
}

fn request(service: &str, action: &str, account: &str, region: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: service.to_string(),
        action: action.to_string(),
        region: region.to_string(),
        account_id: account.to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: serde_json::to_vec(&body).unwrap().into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

/// Secrets Manager + SSM services sharing state with an ECS runtime, all
/// wired to the same KMS hook the server wires.
struct Fixture {
    sm: SecretsManagerService,
    ssm: SsmService,
    runtime: EcsRuntime,
}

impl Fixture {
    fn new() -> Self {
        let hook: Arc<dyn fakecloud_core::delivery::KmsHook> = Arc::new(PrefixKmsHook);
        let sm_state: SharedSecretsManagerState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, REGION, ""),
        ));
        let ssm_state: SharedSsmState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, REGION, ""),
        ));
        Self {
            sm: SecretsManagerService::new(sm_state.clone()).with_kms_hook(hook.clone()),
            ssm: SsmService::new(ssm_state.clone()).with_kms_hook(hook.clone()),
            runtime: EcsRuntime::bare_for_tests()
                .with_secretsmanager(sm_state)
                .with_ssm(ssm_state)
                .with_kms_hook(hook),
        }
    }

    async fn sm_call(&self, action: &str, account: &str, region: &str, body: Value) -> Value {
        let resp = self
            .sm
            .handle(request("secretsmanager", action, account, region, body))
            .await
            .unwrap_or_else(|e| panic!("{action} failed: {e}"));
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    async fn ssm_call(&self, action: &str, account: &str, body: Value) -> Value {
        let resp = self
            .ssm
            .handle(request("ssm", action, account, REGION, body))
            .await
            .unwrap_or_else(|e| panic!("{action} failed: {e}"));
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// Create a secret in `ACCOUNT`/`REGION`, returning its full ARN.
    async fn create_secret(&self, name: &str, value: &str) -> String {
        self.sm_call(
            "CreateSecret",
            ACCOUNT,
            REGION,
            json!({"Name": name, "SecretString": value}),
        )
        .await["ARN"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn resolve(&self, value_from: &str) -> Result<String, String> {
        self.runtime
            .resolve_secret(ACCOUNT, REGION, value_from)
            .map_err(|e| e.to_string())
    }
}

fn partial(arn: &str) -> &str {
    // Drop the `-XXXXXX` random suffix.
    &arn[..arn.len() - 7]
}

fn assert_err_contains(result: Result<String, String>, needle: &str) {
    match result {
        Ok(v) => panic!("expected an error containing {needle:?}, resolved {v:?}"),
        Err(e) => assert!(e.contains(needle), "error {e:?} lacks {needle:?}"),
    }
}

#[tokio::test]
async fn full_arn_resolves_the_current_value() {
    let f = Fixture::new();
    let arn = f.create_secret("db/password", "hunter2").await;
    assert_eq!(f.resolve(&arn).unwrap(), "hunter2");
}

#[tokio::test]
async fn partial_arn_resolves_only_its_own_secret() {
    let f = Fixture::new();
    let app = f.create_secret("app", "app-value").await;
    // Only `app` exists: the partial ARN of `app-db` must not match it,
    // although `app-db` minus a dash-suffix looks like `app`.
    let app_db_partial = format!("{}-db", partial(&app));
    assert_err_contains(f.resolve(&app_db_partial), "ResourceNotFoundException");

    let app_db = f.create_secret("app-db", "app-db-value").await;
    assert_eq!(f.resolve(partial(&app_db)).unwrap(), "app-db-value");
    assert_eq!(f.resolve(partial(&app)).unwrap(), "app-value");
}

#[tokio::test]
async fn json_key_extracts_the_field() {
    let f = Fixture::new();
    let arn = f
        .create_secret(
            "db",
            r#"{"username":"admin","password":"p@ss","port":5432,"ratio":1.5,"big":100000000,"tls":true,"none":null,"opts":{"z":1,"a":["x",false]}}"#,
        )
        .await;
    let field = |key: &str| f.resolve(&format!("{arn}:{key}::"));
    assert_eq!(field("password").unwrap(), "p@ss");
    assert_eq!(field("username").unwrap(), "admin");
    // Non-string values render as the agent's Go `%v` does.
    assert_eq!(field("port").unwrap(), "5432");
    assert_eq!(field("ratio").unwrap(), "1.5");
    assert_eq!(field("big").unwrap(), "1e+08");
    assert_eq!(field("tls").unwrap(), "true");
    assert_eq!(field("none").unwrap(), "<nil>");
    assert_eq!(field("opts").unwrap(), "map[a:[x false] z:1]");
    // Also through a partial ARN.
    assert_eq!(
        f.resolve(&format!("{}:password::", partial(&arn))).unwrap(),
        "p@ss"
    );
}

#[tokio::test]
async fn json_key_missing_or_non_json_secret_fails() {
    let f = Fixture::new();
    let arn = f.create_secret("db", r#"{"username":"admin"}"#).await;
    let err = f.resolve(&format!("{arn}:password::")).unwrap_err();
    assert_eq!(
        err,
        "ResourceInitializationError: unable to pull secrets or registry auth: execution \
         resource retrieval failed: unable to retrieve secret from asm: retrieved secret from \
         Secrets Manager did not contain json key password"
    );

    let plain = f.create_secret("plain", "not-json").await;
    assert_err_contains(
        f.resolve(&format!("{plain}:password::")),
        "secret value is not a JSON object",
    );
}

#[tokio::test]
async fn version_stage_and_version_id_select_the_version() {
    let f = Fixture::new();
    let arn = f.create_secret("rotating", r#"{"pw":"one"}"#).await;
    let first_version = f
        .sm_call(
            "GetSecretValue",
            ACCOUNT,
            REGION,
            json!({"SecretId": arn.clone()}),
        )
        .await["VersionId"]
        .as_str()
        .unwrap()
        .to_string();
    f.sm_call(
        "PutSecretValue",
        ACCOUNT,
        REGION,
        json!({"SecretId": arn.clone(), "SecretString": r#"{"pw":"two"}"#}),
    )
    .await;

    assert_eq!(f.resolve(&arn).unwrap(), r#"{"pw":"two"}"#);
    assert_eq!(
        f.resolve(&format!("{arn}::AWSPREVIOUS:")).unwrap(),
        r#"{"pw":"one"}"#
    );
    assert_eq!(f.resolve(&format!("{arn}:pw:AWSPREVIOUS:")).unwrap(), "one");
    assert_eq!(f.resolve(&format!("{arn}:pw:AWSCURRENT:")).unwrap(), "two");
    assert_eq!(
        f.resolve(&format!("{arn}:pw::{first_version}")).unwrap(),
        "one"
    );
    assert_eq!(
        f.resolve(&format!("{arn}:pw:AWSPREVIOUS:{first_version}"))
            .unwrap(),
        "one"
    );

    // A stage no version carries, a version id that does not exist, and a
    // stage the named version does not carry all fail.
    assert_err_contains(
        f.resolve(&format!("{arn}::AWSPENDING:")),
        "can't find the specified secret value for staging label: AWSPENDING",
    );
    assert_err_contains(
        f.resolve(&format!("{arn}:::00000000-0000-0000-0000-000000000000")),
        "ResourceNotFoundException",
    );
    assert_err_contains(
        f.resolve(&format!("{arn}::AWSCURRENT:{first_version}")),
        "VersionStage that is not associated to the provided VersionId",
    );
}

#[tokio::test]
async fn arn_is_resolved_in_its_own_region_and_account() {
    let f = Fixture::new();
    let arn = f.create_secret("regional", "east").await;

    // The same secret name addressed in another region does not exist.
    let west = arn.replace(":us-east-1:", ":us-west-2:");
    assert_err_contains(f.resolve(&west), "ResourceNotFoundException");
    assert_err_contains(f.resolve(partial(&west)), "ResourceNotFoundException");

    // A secret owned by another account: not found in the task's account,
    // and readable only once the owner's resource policy allows it.
    let foreign = f
        .sm_call(
            "CreateSecret",
            OTHER_ACCOUNT,
            REGION,
            json!({"Name": "shared", "SecretString": "from-other-account"}),
        )
        .await["ARN"]
        .as_str()
        .unwrap()
        .to_string();
    assert_err_contains(f.resolve(&foreign), "AccessDeniedException");
    let wrong_account = arn.replace(ACCOUNT, OTHER_ACCOUNT);
    assert_err_contains(f.resolve(&wrong_account), "ResourceNotFoundException");

    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{ACCOUNT}:root")},
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "*"
        }]
    });
    f.sm_call(
        "PutResourcePolicy",
        OTHER_ACCOUNT,
        REGION,
        json!({"SecretId": foreign.clone(), "ResourcePolicy": policy.to_string()}),
    )
    .await;
    assert_eq!(f.resolve(&foreign).unwrap(), "from-other-account");
}

#[tokio::test]
async fn missing_secret_fails_with_the_agent_reason() {
    let f = Fixture::new();
    let arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:nope-AbCdEf";
    assert_eq!(
        f.resolve(arn).unwrap_err(),
        format!(
            "ResourceInitializationError: unable to pull secrets or registry auth: execution \
             resource retrieval failed: unable to retrieve secret from asm: \
             ResourceNotFoundException: The task can't retrieve the secret with ARN '{arn}' \
             from AWS Secrets Manager. Check whether the secret exists in the specified Region: \
             ResourceNotFoundException: Secrets Manager can't find the specified secret."
        )
    );
}

#[tokio::test]
async fn malformed_selector_arn_is_rejected() {
    let f = Fixture::new();
    let arn = f.create_secret("db", r#"{"pw":"x"}"#).await;
    // The agent accepts `secret:<id>` or all three selectors, nothing between.
    for bad in [format!("{arn}:pw"), format!("{arn}:pw:AWSCURRENT")] {
        assert_err_contains(f.resolve(&bad), "an invalid ARN format");
    }
}

#[tokio::test]
async fn kms_encrypted_secret_is_decrypted() {
    let f = Fixture::new();
    let arn = f
        .sm_call(
            "CreateSecret",
            ACCOUNT,
            REGION,
            json!({"Name": "kms", "SecretString": r#"{"k":"plain"}"#, "KmsKeyId": "alias/app"}),
        )
        .await["ARN"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(f.resolve(&arn).unwrap(), r#"{"k":"plain"}"#);
    assert_eq!(f.resolve(&format!("{arn}:k::")).unwrap(), "plain");
}

#[tokio::test]
async fn binary_secret_injects_the_empty_string() {
    let f = Fixture::new();
    let arn = f
        .sm_call(
            "CreateSecret",
            ACCOUNT,
            REGION,
            json!({"Name": "bin", "SecretBinary": "AAEC"}),
        )
        .await["ARN"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(f.resolve(&arn).unwrap(), "");
}

#[tokio::test]
async fn ssm_parameter_by_name_arn_and_selector() {
    let f = Fixture::new();
    f.ssm_call(
        "PutParameter",
        ACCOUNT,
        json!({"Name": "/app/api-key", "Value": "v1", "Type": "String"}),
    )
    .await;
    f.ssm_call(
        "PutParameter",
        ACCOUNT,
        json!({"Name": "/app/api-key", "Value": "v2", "Type": "String", "Overwrite": true}),
    )
    .await;
    f.ssm_call(
        "LabelParameterVersion",
        ACCOUNT,
        json!({"Name": "/app/api-key", "ParameterVersion": 1, "Labels": ["stable"]}),
    )
    .await;
    let arn = "arn:aws:ssm:us-east-1:123456789012:parameter/app/api-key";

    assert_eq!(f.resolve("/app/api-key").unwrap(), "v2");
    assert_eq!(f.resolve(arn).unwrap(), "v2");
    assert_eq!(f.resolve("/app/api-key:1").unwrap(), "v1");
    assert_eq!(f.resolve("/app/api-key:stable").unwrap(), "v1");
    assert_eq!(f.resolve(&format!("{arn}:1")).unwrap(), "v1");
    assert_err_contains(
        f.resolve("/app/api-key:9"),
        "invalid parameters: /app/api-key:9",
    );
}

#[tokio::test]
async fn ssm_secure_string_is_decrypted() {
    let f = Fixture::new();
    f.ssm_call(
        "PutParameter",
        ACCOUNT,
        json!({"Name": "/app/db-password", "Value": "s3cret", "Type": "SecureString"}),
    )
    .await;
    // Stored KMS-encrypted...
    let masked = f
        .ssm_call("GetParameter", ACCOUNT, json!({"Name": "/app/db-password"}))
        .await;
    assert!(
        masked["Parameter"]["Value"]
            .as_str()
            .unwrap()
            .contains("ENC:s3cret"),
        "{masked}"
    );
    // ...injected decrypted.
    assert_eq!(f.resolve("/app/db-password").unwrap(), "s3cret");
    assert_eq!(
        f.resolve("arn:aws:ssm:us-east-1:123456789012:parameter/app/db-password")
            .unwrap(),
        "s3cret"
    );
}

#[tokio::test]
async fn missing_ssm_parameter_fails_with_the_agent_reason() {
    let f = Fixture::new();
    assert_eq!(
        f.resolve("/nope").unwrap_err(),
        "ResourceInitializationError: unable to pull secrets or registry auth: execution \
         resource retrieval failed: unable to retrieve secrets from ssm: fetching secret data \
         from SSM Parameter Store: invalid parameters: /nope"
    );
}

#[tokio::test]
async fn ssm_parameter_arn_is_read_in_its_own_account() {
    let f = Fixture::new();
    f.ssm_call(
        "PutParameter",
        OTHER_ACCOUNT,
        json!({"Name": "/shared/key", "Value": "theirs", "Type": "String", "Tier": "Advanced"}),
    )
    .await;
    let foreign = format!("arn:aws:ssm:us-east-1:{OTHER_ACCOUNT}:parameter/shared/key");
    // Not in the task's own account under that name...
    assert_err_contains(f.resolve("/shared/key"), "invalid parameters");
    // ...and not readable across accounts until shared.
    assert_err_contains(f.resolve(&foreign), "AccessDeniedException");

    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{ACCOUNT}:root")},
            "Action": ["ssm:GetParameters", "ssm:GetParameter"],
            "Resource": foreign.clone()
        }]
    });
    f.ssm_call(
        "PutResourcePolicy",
        OTHER_ACCOUNT,
        json!({"ResourceArn": foreign.clone(), "Policy": policy.to_string()}),
    )
    .await;
    assert_eq!(f.resolve(&foreign).unwrap(), "theirs");
}

#[test]
fn value_from_parsing() {
    let base = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";
    assert_eq!(
        parse_value_from(base).unwrap(),
        SecretReference::SecretsManager {
            secret_id: base.to_string(),
            json_key: None,
            version_stage: None,
            version_id: None,
        }
    );
    assert_eq!(
        parse_value_from(&format!("{base}:pw:AWSPREVIOUS:v-1")).unwrap(),
        SecretReference::SecretsManager {
            secret_id: base.to_string(),
            json_key: Some("pw"),
            version_stage: Some("AWSPREVIOUS"),
            version_id: Some("v-1"),
        }
    );
    assert_eq!(
        parse_value_from("/app/key").unwrap(),
        SecretReference::Parameter("/app/key")
    );
    assert!(parse_value_from(&format!("{base}:a:b:c:d")).is_err());
    assert!(parse_value_from("arn:aws:secretsmanager:us-east-1:123456789012:secret").is_err());
}

#[test]
fn go_float_rendering_matches_go_percent_v() {
    // Expected values produced by Go's fmt.Sprintf("%v", float64(x)).
    for (f, want) in [
        (100000.0, "100000"),
        (1000000.0, "1e+06"),
        (1200000.0, "1.2e+06"),
        (123456789.0, "1.23456789e+08"),
        (12345678901234567890.0, "1.2345678901234567e+19"),
        (100.0, "100"),
        (1e20, "1e+20"),
        (1e21, "1e+21"),
        (0.0001, "0.0001"),
        (0.00001, "1e-05"),
        (1.5e-7, "1.5e-07"),
        (123456.5, "123456.5"),
        (1234567.5, "1.2345675e+06"),
        (-0.0001, "-0.0001"),
        (0.0, "0"),
        (-42.0, "-42"),
    ] {
        assert_eq!(go_float_text(f), want, "{f}");
    }
}
